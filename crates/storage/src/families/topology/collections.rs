//! The resolver `/aliases`, `/links` and `/roles` collections over the aliases
//! (`project_resolver_alias`, and the alias-path bindings through `project_resource_pointer`),
//! the resolver links (`project_resolver_link`) and the grants (`project_grant`). Each page is
//! keyed `(key1, key2)` as the served collections are, and its total is an exact count over the
//! same relation in the same statement (apps/api/src/v2/resolvers/collections/reads.rs), ordered
//! and paged in SQL so the database's collation orders both alike. The readers take a keyset
//! position and nothing else, no publication token, generation or height: each reads in one
//! read-only REPEATABLE READ snapshot at the block the family marker names, and fails with
//! [`FamilyPublicationUnavailable`] when the marker is not servable. Under the publication
//! switch the resolver routes serve them (TYR-36 step 7b slice 4).
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::families::{
    control::{
        lifecycle::Clock,
        permissions::{ResourceInput, load_shadow_permissions_on, resolver_grant_evidence},
        position::EventOrder,
    },
    name::{
        CoverageShape, FamilyPublication, FamilyPublicationUnavailable, load_names_on,
        publication_on, read_snapshot,
    },
};

/// One page of a resolver collection: `(key1, key2, item)` rows in key order, and the total.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FamilyCollectionPage {
    pub rows: Vec<(String, String, Value)>,
    pub total_count: u64,
}

/// The servable publication of `chain_id`, or the unavailable error.
async fn marker(conn: &mut PgConnection, chain_id: &str) -> Result<FamilyPublication> {
    publication_on(conn, chain_id).await?.ok_or_else(|| {
        FamilyPublicationUnavailable {
            chain_id: chain_id.to_owned(),
        }
        .into()
    })
}

/// `/aliases`: the binding arm (names whose selected binding is an alias-path binding whose
/// resource's current pointer is this resolver) then the event arm (active
/// `project_resolver_alias` rows). A name's selected binding, raw name and namehash, and whether
/// it is served at all, come from its composed row (`families::name`), which stands where the
/// served statement reads `name_current` under the current-name read filter.
pub async fn load_resolver_aliases_shadow(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    after: Option<&(String, String)>,
    limit: i64,
) -> Result<FamilyCollectionPage> {
    let address = resolver_address.to_ascii_lowercase();
    let mut snapshot = read_snapshot(pool).await?;
    marker(&mut snapshot, chain_id).await?;
    let candidates: Vec<(String, Uuid)> = sqlx::query_as(
        "/* storage:families.topology.alias_binding_candidates */
         SELECT DISTINCT candidate.logical_name_id, candidate.surface_binding_id
         FROM bigname_phase.project_binding_candidate candidate
         JOIN bigname_phase.project_resource_pointer pointer
           ON pointer.chain_id = $1 AND pointer.resource_id = candidate.resource_id
          AND pointer.resolver_address = $2
         WHERE candidate.binding_kind = 'resolver_alias_path'",
    )
    .bind(chain_id)
    .bind(&address)
    .fetch_all(&mut *snapshot)
    .await
    .with_context(|| format!("failed to load the alias bindings of {address}"))?;
    let mut bindings: BTreeMap<String, BTreeSet<Uuid>> = BTreeMap::new();
    for (name, binding) in candidates {
        bindings.entry(name).or_default().insert(binding);
    }
    let names: Vec<String> = bindings.keys().cloned().collect();
    let rows = load_names_on(&mut snapshot, &names, CoverageShape::Plain).await?;
    let bound: Vec<Value> = rows
        .values()
        .filter(|row| {
            row.surface_binding_id.is_some_and(|binding| {
                bindings
                    .get(&row.logical_name_id)
                    .is_some_and(|candidates| candidates.contains(&binding))
            })
        })
        .map(|row| {
            json!({"logical_name_id": row.logical_name_id,
                   "normalized_name": row.normalized_name, "namehash": row.namehash})
        })
        .collect();
    let items = "WITH items AS (
            SELECT 'binding'::text AS key1, bound ->> 'logical_name_id' AS key2,
                jsonb_build_object('logical_name_id', bound -> 'logical_name_id',
                    'normalized_name', bound -> 'normalized_name',
                    'namehash', bound -> 'namehash') AS item
            FROM jsonb_array_elements($7::jsonb) bound
            UNION ALL
            SELECT 'event', alias.alias_identity,
                jsonb_strip_nulls(jsonb_build_object('logical_name_id', alias.logical_name_id,
                    'alias_state', COALESCE(to_jsonb(alias.alias_state), '\"active\"'::jsonb),
                    'chain_id', alias.chain_id, 'resolver_address', $2::text,
                    'from_name', alias.from_name, 'to_name', alias.to_name,
                    'from_dns_encoded_name', alias.from_dns_encoded_name,
                    'to_dns_encoded_name', alias.to_dns_encoded_name,
                    'to_logical_name_id', alias.to_logical_name_id,
                    'to_resource_id', alias.to_resource_id))
            FROM bigname_phase.project_resolver_alias alias
            WHERE alias.chain_id = $1 AND alias.resolver_address = $2 AND alias.active
        )";
    let page = page(
        &mut snapshot,
        items,
        (chain_id, &address, None),
        after,
        limit,
        &Value::Array(bound),
    )
    .await?;
    snapshot.commit().await?;
    Ok(page)
}

/// `/links`: the latest link per node at this resolver with a non-zero record, names attached
/// from the readable surfaces in `namespace` at the family marker's block. The newest link per
/// (resolver, node) wins (Tate, 2026-09-26), as on chain, where the resolver keeps one record id
/// per node (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L96-L97 @
/// ens_v2@a971bd64) and each `Linked` overwrites it (upstream:
/// .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L363-L367 @ ens_v2@a971bd64).
/// `storage_model` is an annotation and plays no part (see `load_family_link_selection`). Today's
/// `/links` drops a link not annotated `resolver_record_id` and serves an older one; no producer
/// writes such a link, so on real data that filter keeps every link.
pub async fn load_resolver_links_shadow(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    namespace: &str,
    after: Option<&(String, String)>,
    limit: i64,
) -> Result<FamilyCollectionPage> {
    let items = "WITH clock AS (
            SELECT current_block_number AS block_number
            FROM bigname_phase.project_family_marker WHERE chain_id = $1
        ), items AS (
            SELECT lpad(link.record_id, 78, '0') AS key1, link.node AS key2,
                jsonb_strip_nulls(jsonb_build_object(
                    'record_id', link.record_id, 'namehash', link.node,
                    'default', link.node =
                        '0x0000000000000000000000000000000000000000000000000000000000000000',
                    'logical_name_id', named.logical_name_id, 'name', named.raw_name,
                    'namespace', named.namespace, 'normalized_event_id', link.normalized_event_id,
                    'chain_position', jsonb_strip_nulls(jsonb_build_object(
                        'chain_id', link.chain_id, 'block_number', link.block_number,
                        'block_hash', event.block_hash, 'transaction_hash', event.transaction_hash,
                        'log_index', link.log_index,
                        'timestamp', to_char(lineage.block_timestamp AT TIME ZONE 'UTC',
                                             'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'))))) AS item
            FROM bigname_phase.project_resolver_link link
            CROSS JOIN clock
            LEFT JOIN bigname_phase.normalized_events event
              ON event.event_identity = link.event_identity
            LEFT JOIN bigname_phase.chain_lineage lineage
              ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
             AND lineage.block_number = event.block_number
            LEFT JOIN LATERAL (
                -- Only an active readable surface is a name; the default node's is the root.
                SELECT surface.logical_name_id, surface.raw_name, surface.namespace
                FROM bigname_phase.name_surfaces surface
                JOIN bigname_phase.chain_lineage surface_lineage
                  ON surface_lineage.chain_id = surface.chain_id
                 AND surface_lineage.block_hash = surface.block_hash
                 AND surface_lineage.block_number = surface.block_number
                 AND surface_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                WHERE link.node <>
                      '0x0000000000000000000000000000000000000000000000000000000000000000'
                  AND surface.logical_name_id = $3 || ':' || link.node
                  AND surface.chain_id = $1 AND surface.block_number <= clock.block_number
                  AND surface.visibility_state = 'active'
                  AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
            ) named ON TRUE
            WHERE link.chain_id = $1 AND link.resolver_address = $2
              AND link.record_id <> '0'
        )";
    let address = resolver_address.to_ascii_lowercase();
    let mut snapshot = read_snapshot(pool).await?;
    marker(&mut snapshot, chain_id).await?;
    let page = page(
        &mut snapshot,
        items,
        (chain_id, &address, Some(namespace)),
        after,
        limit,
        &Value::Null,
    )
    .await?;
    snapshot.commit().await?;
    Ok(page)
}

/// `/roles`: the served permission rows of this resolver's scope with non-empty powers, each
/// resource's rows computed from its F8 grants as `GET /v1/permissions` computes them
/// (`load_shadow_permissions_on`: the wrapper fuse and grace masks at the publication's block
/// time, the ENSv2 path-expiry drop and the empty-row drop), on resources that are readable, the
/// resource predicate of the served read filter (`DEFAULT_PERMISSIONS_CURRENT_READ_FILTER`).
/// The filter's other two predicates, the row's own canonicality and its publication lineage,
/// are not checked: the family undo removes the grants of a dropped block (ruling J13).
/// `event_ids` are the grant's evidence events read by key from `normalized_events`
/// (`resolver_grant_evidence`), the served row's `provenance.normalized_event_ids` permission
/// events, from which the route picks the `grant_event` it attaches.
pub async fn load_resolver_roles_shadow(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    after: Option<&(String, String)>,
    limit: i64,
) -> Result<FamilyCollectionPage> {
    let address = resolver_address.to_ascii_lowercase();
    let scope = format!("resolver:{chain_id}:{address}");
    let mut snapshot = read_snapshot(pool).await?;
    let publication = marker(&mut snapshot, chain_id).await?;
    let resources: Vec<Uuid> = sqlx::query_scalar(
        "/* storage:families.topology.role_resources */
         SELECT DISTINCT grant_row.resource_id
         FROM bigname_phase.project_grant grant_row
         WHERE grant_row.chain_id = $1 AND grant_row.scope = $2
           AND EXISTS (
               SELECT 1
               FROM bigname_phase.resources resource
               JOIN bigname_phase.chain_lineage resource_lineage
                 ON resource_lineage.chain_id = resource.chain_id
                AND resource_lineage.block_hash = resource.block_hash
                AND resource_lineage.block_number = resource.block_number
               WHERE resource.resource_id = grant_row.resource_id
                 AND resource.canonicality_state IN ('canonical', 'safe', 'finalized')
                 AND resource_lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))",
    )
    .bind(chain_id)
    .bind(&scope)
    .fetch_all(&mut *snapshot)
    .await
    .with_context(|| format!("failed to load the resources granted on {scope}"))?;
    let inputs: Vec<ResourceInput> = resources
        .iter()
        .map(|resource| ResourceInput {
            resource_id: resource.to_string(),
            ..ResourceInput::default()
        })
        .collect();
    let clock = Clock {
        block_number: publication.block_number,
        timestamp_seconds: publication.timestamp_seconds(),
    };
    let shadows = load_shadow_permissions_on(
        &mut snapshot,
        chain_id,
        &clock,
        &inputs,
        &EventOrder::Canonical,
    )
    .await?;
    let evidence = resolver_grant_evidence(
        &mut snapshot,
        chain_id,
        publication.block_number,
        &scope,
        &resources,
    )
    .await?;
    let mut granted = Vec::new();
    for shadow in shadows.values() {
        for grant in shadow.grants.iter().filter(|grant| grant.scope == scope) {
            if !grant
                .effective_powers
                .as_array()
                .is_some_and(|powers| !powers.is_empty())
            {
                continue;
            }
            let resource: Uuid = grant.resource_id.parse()?;
            let ids = evidence
                .get(&(resource, grant.subject.to_ascii_lowercase()))
                .cloned()
                .unwrap_or_default();
            granted.push(json!({
                "subject": grant.subject, "resource_id": grant.resource_id,
                "powers": grant.effective_powers,
                "selector": grant.scope_detail.get("resource_selector"),
                "event_ids": ids,
            }));
        }
    }
    let items = "WITH items AS (
            SELECT granted ->> 'subject' AS key1, granted ->> 'resource_id' AS key2,
                jsonb_strip_nulls(jsonb_build_object('address', granted -> 'subject',
                    'registration_id', granted -> 'resource_id', 'powers', granted -> 'powers',
                    'record_resource_selector', granted -> 'selector',
                    'event_ids', granted -> 'event_ids')) AS item
            FROM jsonb_array_elements($7::jsonb) granted
        )";
    let page = page(
        &mut snapshot,
        items,
        (chain_id, &address, None),
        after,
        limit,
        &Value::Array(granted),
    )
    .await?;
    snapshot.commit().await?;
    Ok(page)
}

/// Page `items` (a `WITH items AS (...)` of `(key1, key2, item)`) after `after`, with its exact
/// total. `$1` is the chain, `$2` the lower-cased resolver, `$3` the namespace (links only) and
/// `$7` a JSON array the items may read (the arms computed outside SQL); every statement binds
/// all of them.
async fn page(
    conn: &mut PgConnection,
    items: &str,
    (chain_id, resolver_address, namespace): (&str, &str, Option<&str>),
    after: Option<&(String, String)>,
    limit: i64,
    computed: &Value,
) -> Result<FamilyCollectionPage> {
    let query = format!(
        "{items}, selected_page AS (
            SELECT key1, key2, item FROM items
            WHERE $4::text IS NULL OR (key1, key2) > ($4, $5)
            ORDER BY key1, key2 LIMIT $6
        ) SELECT (SELECT count(*) FROM items) AS total,
            COALESCE((SELECT jsonb_agg(jsonb_build_object('key1', key1, 'key2', key2,
                'item', item) ORDER BY key1, key2) FROM selected_page), '[]'::jsonb) AS rows,
            $3::text IS NULL AS unused_namespace, $7::jsonb IS NULL AS unused_computed"
    );
    let row = sqlx::query(&query)
        .bind(chain_id)
        .bind(resolver_address)
        .bind(namespace)
        .bind(after.map(|key| key.0.as_str()))
        .bind(after.map(|key| key.1.as_str()))
        .bind(limit)
        .bind(computed)
        .fetch_one(conn)
        .await
        .with_context(|| format!("failed to load a resolver collection of {resolver_address}"))?;
    let total: i64 = row.try_get("total")?;
    let rows: Value = row.try_get("rows")?;
    let rows = rows
        .as_array()
        .context("resolver collection rows must be an array")?
        .iter()
        .map(|row| {
            Ok((
                row["key1"].as_str().context("key1")?.to_owned(),
                row["key2"].as_str().context("key2")?.to_owned(),
                row["item"].clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(FamilyCollectionPage {
        rows,
        total_count: u64::try_from(total).context("negative collection total")?,
    })
}
