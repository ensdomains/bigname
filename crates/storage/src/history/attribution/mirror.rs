//! A resource whose latest pointer is a declared ENSv1 mirror resolver: Project re-points it at the
//! ENSv1 resolver the mirror would call for the queried name when the registry walk selects that
//! resolver at the queried node itself, and attributes that resolver's node-keyed writes for the
//! queried node. The walk reads each registry pointer by the node the event addresses
//! (`child_node`, then `namehash`, then `node`) and never consults the root. The mirror keeps a
//! nearest resolver found on an ancestor only when it supports `IExtendedResolver`, and Project
//! derives through neither kind of ancestor, so a nearest ancestor attributes nothing and the walk
//! does not continue past it. When the mirror cannot be followed, Project publishes the resource's
//! inventory row with no attributed writes at all
//! (`crates/project/src/builders/record_inventory/mirror.rs`).
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L39-L48 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v1/contracts/universalResolver/RegistryUtils.sol:L25-L38 @ ens_v1@91c966f)

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{B256, keccak256};
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use super::sql::{
    CLEARED, ENS_V1_POINTER_FAMILIES, ENS_V2_POINTER_FAMILIES, push_declaration_manifest,
    push_pointer_ctes, push_readable_event, push_readable_surface,
};

struct MirrorPointer {
    resource_id: Uuid,
    chain_id: String,
    namespace: String,
    raw_labels: Vec<String>,
    labelhashes: Vec<String>,
    namehash: String,
    followable: bool,
}

/// For each resource whose latest pointer at the bound is an ENSv2 pointer to a resolver
/// classified as an ENSv1 mirror: `Some(writes)` when Project would follow the mirror, `None` when
/// it would publish the resource with no attributed writes.
pub(super) async fn load_mirror_attribution(
    connection: &mut PgConnection,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<BTreeMap<Uuid, Option<BTreeSet<i64>>>> {
    let mirrors = load_mirror_pointers(connection, resource_ids, published).await?;
    let mut attribution = mirrors
        .iter()
        .map(|mirror| (mirror.resource_id, None))
        .collect::<BTreeMap<_, _>>();
    let followable = mirrors
        .into_iter()
        .filter(|mirror| mirror.followable)
        .collect::<Vec<_>>();
    if followable.is_empty() {
        return Ok(attribution);
    }

    let walk = MirrorWalk::new(&followable);
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_mirror_writes(&mut builder, &walk, published);
    for row in builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context("failed to load mirrored resolver record writes")?
    {
        let resource_id: Uuid = row.try_get("resource_id")?;
        let writes = attribution
            .entry(resource_id)
            .or_default()
            .get_or_insert_with(BTreeSet::new);
        if let Some(event_id) = row.try_get::<Option<i64>, _>("normalized_event_id")? {
            writes.insert(event_id);
        }
    }
    Ok(attribution)
}

async fn load_mirror_pointers(
    connection: &mut PgConnection,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<Vec<MirrorPointer>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_pointer_ctes(&mut builder, resource_ids, published);
    builder.push(
        "
        SELECT latest.resource_id, latest.chain_id, latest.surface_namespace, latest.raw_labels,
               latest.labelhashes, latest.namehash,
               COALESCE(resolver.support_status = 'supported' AND declaration.active
                        AND declaration.namespace = latest.pointer_namespace, FALSE) AS followable
        FROM pointer_windows latest
        JOIN bigname_phase.resolver_current resolver
          ON resolver.chain_id = latest.chain_id
         AND resolver.resolver_address = latest.resolver_address
         AND resolver.declared_summary #>> '{classification,source_family}' = 'ens_v2_resolver_l1'
         AND resolver.declared_summary #>> '{classification,role}' = 'ensv1_mirror_resolver'
        LEFT JOIN",
    );
    push_declaration_manifest(
        &mut builder,
        "(resolver.provenance ->> 'manifest_id')::bigint",
        "latest.chain_id",
        published,
    );
    builder.push(format!(
        " ON TRUE
        WHERE latest.end_position IS NULL
          AND latest.pointer_source_family IN {ENS_V2_POINTER_FAMILIES}
          AND latest.resolver_address IS NOT NULL
          AND latest.resolver_address NOT IN {CLEARED}"
    ));
    builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context("failed to load mirror resolver pointers")?
        .into_iter()
        .map(|row| {
            Ok(MirrorPointer {
                resource_id: row.try_get("resource_id")?,
                chain_id: row.try_get("chain_id")?,
                namespace: row.try_get("surface_namespace")?,
                raw_labels: row.try_get("raw_labels")?,
                labelhashes: row.try_get("labelhashes")?,
                namehash: row.try_get("namehash")?,
                followable: row.try_get("followable")?,
            })
        })
        .collect()
}

/// Every registry node the mirror's walk consults for each queried name: the name itself and each
/// proper ancestor below the root, deepest first, as parallel arrays.
#[derive(Default)]
struct MirrorWalk {
    resource_ids: Vec<Uuid>,
    chain_ids: Vec<String>,
    namespaces: Vec<String>,
    depths: Vec<i32>,
    nodes: Vec<String>,
    labels: Vec<Value>,
    queried_nodes: Vec<String>,
}

impl MirrorWalk {
    fn new(mirrors: &[MirrorPointer]) -> Self {
        let mut walk = Self::default();
        for mirror in mirrors {
            for depth in 0..mirror.raw_labels.len() {
                walk.resource_ids.push(mirror.resource_id);
                walk.chain_ids.push(mirror.chain_id.clone());
                walk.namespaces.push(mirror.namespace.clone());
                walk.depths.push(i32::try_from(depth).unwrap_or(i32::MAX));
                walk.nodes.push(suffix_namehash(
                    &mirror.raw_labels[depth..],
                    mirror.labelhashes.get(depth..).unwrap_or_default(),
                ));
                walk.labels
                    .push(Value::from(mirror.raw_labels[depth..].to_vec()));
                walk.queried_nodes.push(mirror.namehash.clone());
            }
        }
        walk
    }
}

/// The namehash of a name suffix. The stored labelhashes are used when each is a 32-byte hash, so
/// labels known only by their hash still resolve; otherwise the raw labels are hashed.
fn suffix_namehash(raw_labels: &[String], labelhashes: &[String]) -> String {
    let parsed = (labelhashes.len() == raw_labels.len())
        .then(|| {
            labelhashes
                .iter()
                .map(|labelhash| labelhash.parse::<B256>().ok())
                .collect::<Option<Vec<_>>>()
        })
        .flatten();
    let labelhashes = parsed.unwrap_or_else(|| {
        raw_labels
            .iter()
            .map(|label| keccak256(label.as_bytes()))
            .collect()
    });
    let node = labelhashes
        .iter()
        .rev()
        .fold(B256::ZERO, |parent, labelhash| {
            let mut input = [0_u8; 64];
            input[..32].copy_from_slice(parent.as_slice());
            input[32..].copy_from_slice(labelhash.as_slice());
            keccak256(input)
        });
    format!("{node:#x}")
}

#[cfg(test)]
pub(super) fn push_empty_mirror_writes_for_test(
    builder: &mut QueryBuilder<'static, Postgres>,
    published: Option<&BTreeMap<String, i64>>,
) {
    let walk: &'static MirrorWalk = Box::leak(Box::default());
    push_mirror_writes(builder, walk, published);
}

/// `SELECT resource_id, normalized_event_id`: one row with a null id for every followed mirror,
/// and one per node-keyed write of the ENSv1 resolver it follows for the queried node.
fn push_mirror_writes<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    walk: &'a MirrorWalk,
    published: Option<&BTreeMap<String, i64>>,
) {
    builder.push("WITH walk AS (SELECT * FROM unnest(");
    builder.push_bind(&walk.resource_ids);
    builder.push("::uuid[], ");
    builder.push_bind(&walk.chain_ids);
    builder.push("::text[], ");
    builder.push_bind(&walk.namespaces);
    builder.push("::text[], ");
    builder.push_bind(&walk.depths);
    builder.push("::int[], ");
    builder.push_bind(&walk.nodes);
    builder.push("::text[], ");
    builder.push_bind(&walk.labels);
    builder.push("::jsonb[], ");
    builder.push_bind(&walk.queried_nodes);
    builder.push(
        "::text[]) AS walk(resource_id, chain_id, namespace, ancestor_depth, node, labels,
                           queried_node)
        ),
        candidates AS (
            SELECT walk.resource_id, walk.chain_id, walk.ancestor_depth, walk.queried_node,
                   registry.mirrored_pointer_namespace, registry.mirrored_resolver_address,
                   registry.mirrored_pointer_event_id
            FROM walk
            JOIN bigname_phase.name_surfaces surface
              ON surface.namespace = walk.namespace
             AND surface.namehash = walk.node
             AND surface.chain_id = walk.chain_id
             AND to_jsonb(surface.raw_labels) = walk.labels",
    );
    push_readable_surface(builder, "surface", published);
    // The ENSv1 registry's resolver for the node at the bound: its latest registry-side pointer,
    // clears included, so a cleared node falls through to its ancestors.
    builder.push(format!(
        "
            JOIN LATERAL (
                SELECT registry.namespace AS mirrored_pointer_namespace,
                       lower(registry.after_state ->> 'resolver') AS mirrored_resolver_address,
                       registry.normalized_event_id AS mirrored_pointer_event_id
                FROM bigname_phase.normalized_events registry
                WHERE registry.chain_id = walk.chain_id
                  AND registry.event_kind = 'ResolverChanged'
                  AND registry.source_family IN {ENS_V1_POINTER_FAMILIES}
                  AND COALESCE(registry.after_state ->> 'child_node', registry.after_state ->> 'namehash',
                               registry.after_state ->> 'node') IS NOT NULL
                  AND lower(COALESCE(registry.after_state ->> 'child_node', registry.after_state ->> 'namehash',
                                     registry.after_state ->> 'node')) = lower(surface.namehash)
                  AND registry.namespace = surface.namespace"
    ));
    push_readable_event(builder, "registry", published);
    builder.push(format!(
        "
                ORDER BY registry.block_number DESC NULLS LAST,
                         registry.transaction_index DESC NULLS LAST,
                         registry.log_index DESC NULLS LAST,
                         registry.normalized_event_id DESC
                LIMIT 1
            ) registry
              ON registry.mirrored_resolver_address IS NOT NULL
             AND registry.mirrored_resolver_address NOT IN {CLEARED}
        ),
        nearest AS (
            SELECT DISTINCT ON (resource_id) *
            FROM candidates
            ORDER BY resource_id, ancestor_depth ASC, mirrored_pointer_event_id DESC
        ),
        followed AS (
            SELECT nearest.*
            FROM nearest
            JOIN bigname_phase.resolver_current resolver
              ON resolver.chain_id = nearest.chain_id
             AND resolver.resolver_address = nearest.mirrored_resolver_address
            JOIN"
    ));
    push_declaration_manifest(
        builder,
        "(resolver.provenance ->> 'manifest_id')::bigint",
        "nearest.chain_id",
        published,
    );
    // The mirror keeps a resolver found on an ancestor only when it is an ENSIP-10 extended
    // resolver, and calls an extended resolver's `resolve(name, data)`, so an ancestor's answer
    // for a descendant is that resolver's own logic, not node-keyed storage bigname can read.
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L39-L48 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/AbstractMirrorResolver.sol:L67-L68 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L66-L87 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L108-L117 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/universalResolver/ResolverCaller.sol:L175-L187 @ ens_v1@91c966f)
    // A non-extended ancestor is rejected by the mirror outright. Following only the exact node is
    // bigname's attribution rule, mirroring the Project producer that marks an ancestor row
    // `ensip10_extended_resolver` or `ancestor_resolver_not_extended`
    // (bigname: `crates/project/src/builders/record_inventory/mirror.rs:169-179`).
    builder.push(
        "
              ON declaration.active
             AND declaration.namespace = nearest.mirrored_pointer_namespace
            WHERE resolver.declared_summary #>> '{classification,role}'
                      IS DISTINCT FROM 'ensv1_mirror_resolver'
              AND resolver.support_status = 'supported'
              AND resolver.declared_summary #>> '{classification,source_family}' =
                  'ens_v1_resolver_l1'
              AND nearest.ancestor_depth = 0
        )
        SELECT followed.resource_id, NULL::bigint AS normalized_event_id FROM followed
        UNION ALL
        SELECT followed.resource_id, record.normalized_event_id
        FROM followed
        JOIN bigname_phase.normalized_events record
          ON record.chain_id = followed.chain_id
         AND record.logical_name_id IS NULL
         AND record.source_family = 'ens_v1_resolver_l1'
         AND record.event_kind IN ('RecordChanged', 'RecordVersionChanged')
         AND lower(record.after_state ->> 'node') = followed.queried_node
         AND lower(COALESCE(
                 NULLIF(record.after_state ->> 'resolver', ''),
                 NULLIF(record.raw_fact_ref ->> 'emitting_address', '')
             )) = followed.mirrored_resolver_address
        WHERE TRUE",
    );
    push_readable_event(builder, "record", published);
}
