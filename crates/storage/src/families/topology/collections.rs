//! The resolver `/aliases`, `/links` and `/roles` collections over F10, F7 and F8. Each page is
//! keyed `(key1, key2)` as the served collections are, and its total is an exact count over the
//! same relation in the same statement (collections/reads.rs). The readers take a keyset
//! position and nothing else: no publication token, generation or height (D10); they read the
//! families at the publication the family marker names.
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::name_current::{DEFAULT_NAME_CURRENT_LINEAGE_JOINS, DEFAULT_NAME_CURRENT_READ_FILTER};

/// One page of a resolver collection: `(key1, key2, item)` rows in key order, and the total.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FamilyCollectionPage {
    pub rows: Vec<(String, String, Value)>,
    pub total_count: u64,
}

/// `/aliases`: the binding arm (names whose selected alias-path binding's current F5 pointer is
/// this resolver) then the event arm (F10 per-resolver rows that are active).
pub async fn load_resolver_aliases_shadow(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    after: Option<&(String, String)>,
    limit: i64,
) -> Result<FamilyCollectionPage> {
    let items = format!(
        "WITH items AS (
            SELECT 'binding'::text AS key1, nc.logical_name_id AS key2,
                jsonb_build_object('logical_name_id', nc.logical_name_id,
                    'normalized_name', nc.raw_name, 'namehash', nc.namehash) AS item
            FROM bigname_phase.name_current nc
            JOIN bigname_phase.project_binding_candidate candidate
              ON candidate.surface_binding_id = nc.surface_binding_id
             AND candidate.binding_kind = 'resolver_alias_path'
            JOIN bigname_phase.project_resource_pointer pointer
              ON pointer.chain_id = $1 AND pointer.resource_id = candidate.resource_id
             AND pointer.resolver_address = $2
            JOIN bigname_phase.name_surfaces surface ON surface.logical_name_id = nc.logical_name_id
            LEFT JOIN bigname_phase.resources resource ON resource.resource_id = nc.resource_id
            LEFT JOIN bigname_phase.surface_bindings binding
              ON binding.surface_binding_id = nc.surface_binding_id
            LEFT JOIN bigname_phase.token_lineages token_lineage
              ON token_lineage.token_lineage_id = nc.token_lineage_id
            {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
            WHERE TRUE {DEFAULT_NAME_CURRENT_READ_FILTER}
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
        )"
    );
    page(pool, &items, chain_id, resolver_address, None, after, limit).await
}

/// `/links`: the latest link per node at this resolver with a non-zero record, names attached
/// from the readable surfaces in `namespace` at the family marker's block.
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
              AND link.record_id <> '0' AND link.storage_model = 'resolver_record_id'
        )";
    page(
        pool,
        items,
        chain_id,
        resolver_address,
        Some(namespace),
        after,
        limit,
    )
    .await
}

/// `/roles`: resolver-scoped F8 grants whose effective powers are non-empty. Step 3 applies the
/// wrapper, grace and expiry-retirement masks at read; until then the powers are the stored,
/// unmasked ones, which resolver-scoped ENSv2 grants carry unmasked today. `event_ids` holds the
/// grant's last event only: the per-key evidence arrays are dropped from the row (design F8).
pub async fn load_resolver_roles_shadow(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    after: Option<&(String, String)>,
    limit: i64,
) -> Result<FamilyCollectionPage> {
    let items = "WITH items AS (
            SELECT grant_row.subject AS key1, grant_row.resource_id::text AS key2,
                jsonb_strip_nulls(jsonb_build_object('address', grant_row.subject,
                    'registration_id', grant_row.resource_id,
                    'powers', grant_row.effective_powers,
                    'record_resource_selector', grant_row.scope_detail -> 'resource_selector',
                    'event_ids', jsonb_build_array(grant_row.normalized_event_id))) AS item
            FROM bigname_phase.project_grant grant_row
            WHERE grant_row.chain_id = $1
              AND grant_row.scope = 'resolver:' || $1 || ':' || $2
              AND jsonb_array_length(grant_row.effective_powers) > 0
        )";
    page(pool, items, chain_id, resolver_address, None, after, limit).await
}

async fn page(
    pool: &PgPool,
    items: &str,
    chain_id: &str,
    resolver_address: &str,
    namespace: Option<&str>,
    after: Option<&(String, String)>,
    limit: i64,
) -> Result<FamilyCollectionPage> {
    // `$3` is the namespace for links and unused elsewhere, so every statement binds it.
    let query = format!(
        "{items}, selected_page AS (
            SELECT key1, key2, item FROM items
            WHERE $4::text IS NULL OR (key1, key2) > ($4, $5)
            ORDER BY key1, key2 LIMIT $6
        ) SELECT (SELECT count(*) FROM items) AS total,
            COALESCE((SELECT jsonb_agg(jsonb_build_object('key1', key1, 'key2', key2,
                'item', item) ORDER BY key1, key2) FROM selected_page), '[]'::jsonb) AS rows,
            $3::text IS NULL AS unused_namespace"
    );
    let row = sqlx::query(&query)
        .bind(chain_id)
        .bind(resolver_address.to_ascii_lowercase())
        .bind(namespace)
        .bind(after.map(|key| key.0.as_str()))
        .bind(after.map(|key| key.1.as_str()))
        .bind(limit)
        .fetch_one(pool)
        .await
        .with_context(|| {
            format!("failed to load a resolver collection shadow of {resolver_address}")
        })?;
    let total: i64 = row.try_get("total")?;
    let rows: Value = row.try_get("rows")?;
    let rows = rows
        .as_array()
        .context("resolver collection shadow rows must be an array")?
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
