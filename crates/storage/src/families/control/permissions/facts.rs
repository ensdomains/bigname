//! The resource facts the served permission reads take beside the F8 and F9 rows, read from the
//! identity and event input tables because no family stores them: whether a resource is
//! readable, the authority kind the resource summary derives from the resource's whole event
//! history, its ENSv2 registry root (resource 0 of the registry contract instance the resource's
//! own events name, checked readable by `registry_roots`), namespace membership and the evidence
//! events of a resolver-scoped grant. Every read is by key.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L54 @ ens_v2_sepolia_20261001@07e55a05)
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::identity::ens_v2_registry_root_resource_id;

const READABLE: &str = "('canonical', 'safe', 'finalized')";

/// A resource whose identity row and its block are readable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReadableResource {
    pub chain_id: String,
    pub block_number: i64,
}

/// The readable resources of `ids`: the resource predicate of the served read filters
/// (`DEFAULT_PERMISSIONS_CURRENT_READ_FILTER`, `CURRENT_PERMISSION_SUMMARY_READ_FILTER`).
pub(super) async fn readable_resources(
    conn: &mut PgConnection,
    ids: &[Uuid],
) -> Result<BTreeMap<Uuid, ReadableResource>> {
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query(&format!(
        "/* storage:families.control.permissions.readable_resources */
         SELECT resource.resource_id, resource.chain_id, resource.block_number
         FROM bigname_phase.resources resource
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = resource.chain_id AND lineage.block_hash = resource.block_hash
          AND lineage.block_number = resource.block_number
         WHERE resource.resource_id = ANY($1::uuid[])
           AND resource.canonicality_state IN {READABLE}
           AND lineage.canonicality_state IN {READABLE}"
    ))
    .bind(ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the readable permission resources")?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("resource_id")?,
                ReadableResource {
                    chain_id: row.try_get("chain_id")?,
                    block_number: row.try_get("block_number")?,
                },
            ))
        })
        .collect()
}

/// One resource's summary facts: its authority kind and, for an ENSv2 registry resource, its
/// registry root.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct AuthorityFacts {
    pub authority_kind: Option<String>,
    pub root_resource_id: Option<Uuid>,
    /// The registry contract instance an `ens_v2_registry` resource's latest event names.
    pub registry_instance: Option<Uuid>,
}

/// The authority kind and registry root of each of `ids` on `chain_id` at `block_number`. The
/// authority kind is the latest readable event of the resource carrying one, else the latest
/// resource-scoped permission event whose grant or revocation source carries one. The registry
/// root of an `ens_v2_registry` resource is resource 0 of the registry contract instance its
/// latest readable event names (`registry_contract_instance_id`), kept only when that root's
/// identity row is readable at `block_number`. The latest is by block, transaction index, log
/// index, then generated event id, each descending with nulls last.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L54 @ ens_v2_sepolia_20261001@07e55a05)
pub(super) async fn authority_facts(
    conn: &mut PgConnection,
    chain_id: &str,
    block_number: i64,
    ids: &[Uuid],
) -> Result<BTreeMap<Uuid, AuthorityFacts>> {
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let latest = "ORDER BY event.block_number DESC NULLS LAST,
                           event.transaction_index DESC NULLS LAST,
                           event.log_index DESC NULLS LAST, event.normalized_event_id DESC
                  LIMIT 1";
    let scoped = "COALESCE(event.after_state -> 'grant_source' ->> 'authority_kind',
                           event.after_state -> 'revocation_source' ->> 'authority_kind')";
    let rows = sqlx::query(&format!(
        "/* storage:families.control.permissions.authority_facts */
         WITH readable_events AS (
             SELECT event.resource_id, event.after_state, event.block_number,
                    event.transaction_index, event.log_index, event.normalized_event_id
             FROM bigname_phase.normalized_events event
             LEFT JOIN bigname_phase.chain_lineage lineage
               ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             WHERE event.resource_id = ANY($3::uuid[]) AND event.chain_id = $1
               AND event.consumer_visibility = 'activated'
               AND event.canonicality_state IN {READABLE}
               AND ((event.block_number IS NULL AND event.block_hash IS NULL)
                    OR (event.block_number <= $2
                        AND lineage.canonicality_state IN {READABLE}))
         ), kinds AS (
             SELECT resource.resource_id,
                    COALESCE(
                        (SELECT event.after_state ->> 'authority_kind' FROM readable_events event
                         WHERE event.resource_id = resource.resource_id
                           AND event.after_state ->> 'authority_kind' IS NOT NULL
                         {latest}),
                        (SELECT {scoped} FROM readable_events event
                         WHERE event.resource_id = resource.resource_id
                           AND event.after_state -> 'scope' ->> 'kind' = 'resource'
                           AND {scoped} IS NOT NULL
                         {latest})
                    ) AS authority_kind,
                    (SELECT event.after_state ->> 'registry_contract_instance_id'
                     FROM readable_events event
                     WHERE event.resource_id = resource.resource_id
                       AND event.after_state ->> 'registry_contract_instance_id' IS NOT NULL
                     {latest}) AS registry_instance
             FROM bigname_phase.resources resource
             WHERE resource.resource_id = ANY($3::uuid[])
         )
         SELECT resource_id, authority_kind,
                CASE WHEN authority_kind = 'ens_v2_registry' THEN registry_instance END
                    AS registry_instance
         FROM kinds"
    ))
    .bind(chain_id)
    .bind(block_number)
    .bind(ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the permission resources' authority kinds")?;
    let mut facts = BTreeMap::new();
    let mut roots = BTreeMap::new();
    for row in rows {
        let resource: Uuid = row.try_get("resource_id")?;
        let instance: Option<String> = row.try_get("registry_instance")?;
        // A malformed instance only drops the root's admins; it must not fail the serving read.
        let instance = instance.and_then(|instance| instance.parse::<Uuid>().ok());
        if let Some(instance) = instance {
            roots.insert(
                resource,
                ens_v2_registry_root_resource_id(chain_id, instance),
            );
        }
        facts.insert(
            resource,
            AuthorityFacts {
                authority_kind: row.try_get("authority_kind")?,
                root_resource_id: None,
                registry_instance: instance,
            },
        );
    }
    let candidates: Vec<Uuid> = roots
        .values()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let readable = readable_registry_roots(conn, chain_id, block_number, &candidates).await?;
    for (resource, root) in roots {
        if readable.contains(&root)
            && let Some(facts) = facts.get_mut(&resource)
        {
            facts.root_resource_id = Some(root);
        }
    }
    Ok(facts)
}

/// The registry roots of `ids` whose identity row and its block are readable on `chain_id` at
/// or below `block_number`. A registry whose root never had a role change has no root row.
async fn readable_registry_roots(
    conn: &mut PgConnection,
    chain_id: &str,
    block_number: i64,
    ids: &[Uuid],
) -> Result<BTreeSet<Uuid>> {
    if ids.is_empty() {
        return Ok(BTreeSet::new());
    }
    let roots = sqlx::query_scalar::<_, Uuid>(&format!(
        "/* storage:families.control.permissions.registry_roots */
         SELECT root.resource_id
         FROM bigname_phase.resources root
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = root.chain_id AND lineage.block_hash = root.block_hash
          AND lineage.block_number = root.block_number
         WHERE root.resource_id = ANY($3::uuid[])
           AND root.chain_id = $1 AND root.block_number <= $2
           AND root.canonicality_state IN {READABLE}
           AND lineage.canonicality_state IN {READABLE}"
    ))
    .bind(chain_id)
    .bind(block_number)
    .bind(ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the permission resources' registry roots")?;
    Ok(roots.into_iter().collect())
}

/// The resources of `ids` that a readable activated event places in `namespace`, the served
/// page's namespace filter, which reads interpreted events rather than name bindings so that
/// unnamed and superseded registrations are included.
pub(super) async fn in_namespace(
    conn: &mut PgConnection,
    ids: &[Uuid],
    namespace: &str,
) -> Result<BTreeSet<Uuid>> {
    if ids.is_empty() {
        return Ok(BTreeSet::new());
    }
    let rows: Vec<Uuid> = sqlx::query_scalar(&format!(
        "/* storage:families.control.permissions.in_namespace */
         SELECT DISTINCT event.resource_id
         FROM bigname_phase.normalized_events event
         JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
         WHERE event.resource_id = ANY($1::uuid[]) AND event.namespace = $2
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN {READABLE}
           AND lineage.canonicality_state IN {READABLE}"
    ))
    .bind(ids)
    .bind(namespace)
    .fetch_all(&mut *conn)
    .await
    .context("failed to check the permission resources' namespace")?;
    Ok(rows.into_iter().collect())
}

/// The evidence events of every resolver-scoped grant on `resources` at `resolver_scope`
/// (`resolver:<chain>:<address>`), keyed by resource and subject: the readable permission
/// events of that key at or below `block_number`, in generated-id order, as the served row's
/// `provenance.normalized_event_ids` collects them. The `/roles` route picks its `grant_event`
/// from these.
pub(crate) async fn resolver_grant_evidence(
    conn: &mut PgConnection,
    chain_id: &str,
    block_number: i64,
    resolver_scope: &str,
    resources: &[Uuid],
) -> Result<BTreeMap<(Uuid, String), Vec<i64>>> {
    if resources.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query(&format!(
        "/* storage:families.control.permissions.resolver_grant_evidence */
         SELECT event.resource_id, lower(event.after_state ->> 'subject') AS subject,
                array_agg(event.normalized_event_id ORDER BY event.normalized_event_id) AS ids
         FROM bigname_phase.normalized_events event
         LEFT JOIN bigname_phase.chain_lineage lineage
           ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.resource_id = ANY($3::uuid[]) AND event.chain_id = $1
           AND event.event_kind IN ('PermissionChanged', 'RootPermissionChanged')
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN {READABLE}
           AND ((event.block_number IS NULL AND event.block_hash IS NULL)
                OR (event.block_number <= $2 AND lineage.canonicality_state IN {READABLE}))
           AND btrim(COALESCE(event.after_state ->> 'subject', '')) <> ''
           AND jsonb_typeof(event.after_state -> 'effective_powers') = 'array'
           AND event.after_state -> 'scope' ->> 'kind' = 'resolver'
           AND concat('resolver:', event.after_state -> 'scope' ->> 'chain_id', ':',
                      lower(event.after_state -> 'scope' ->> 'resolver_address')) = $4
         GROUP BY 1, 2"
    ))
    .bind(chain_id)
    .bind(block_number)
    .bind(resources)
    .bind(resolver_scope)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the resolver grants' evidence events")?;
    rows.into_iter()
        .map(|row| {
            Ok((
                (row.try_get("resource_id")?, row.try_get("subject")?),
                row.try_get("ids")?,
            ))
        })
        .collect()
}
