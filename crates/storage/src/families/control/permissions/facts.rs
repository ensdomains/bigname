//! The resource facts the served permission reads take beside the F8 and F9 rows, read from the
//! identity and event input tables because no family stores them: whether a resource is
//! readable, the authority kind the resource summary derives from the resource's whole event
//! history (builders/permissions/resource_summary.rs, `resource_event_summaries` and
//! `resource_authority`), its ENSv2 registry root (`registry_roots`), namespace membership
//! (permissions/effective.rs `push_namespace_filter`) and the evidence events of a
//! resolver-scoped grant (builders/permissions.rs, the `evidence` window). Every read is by key.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

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
}

/// The authority kind and registry root of each of `ids` on `chain_id` at `block_number`, by the
/// resource summary's rule: the latest readable event of the resource carrying an authority kind,
/// else the latest resource-scoped permission event whose grant or revocation source carries
/// one, else the identity row's, else `ens_v2_registry` for an ENSv2 root or registry resource;
/// `name_wrapper` reads as `wrapper`. The latest is by block, transaction index, log index, then
/// generated event id, each descending with nulls last, as the served summary orders them.
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
             SELECT resource.resource_id, resource.provenance,
                    COALESCE(
                        (SELECT event.after_state ->> 'authority_kind' FROM readable_events event
                         WHERE event.resource_id = resource.resource_id
                           AND event.after_state ->> 'authority_kind' IS NOT NULL
                         {latest}),
                        (SELECT {scoped} FROM readable_events event
                         WHERE event.resource_id = resource.resource_id
                           AND event.after_state -> 'scope' ->> 'kind' = 'resource'
                           AND {scoped} IS NOT NULL
                         {latest}),
                        resource.provenance ->> 'authority_kind',
                        CASE WHEN COALESCE(resource.provenance ->> 'source_family',
                                           resource.provenance ->> 'binding_source_family')
                                  IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                             THEN 'ens_v2_registry' END
                    ) AS raw_kind
             FROM bigname_phase.resources resource
             WHERE resource.resource_id = ANY($3::uuid[])
         )
         SELECT kinds.resource_id,
                CASE kinds.raw_kind WHEN 'name_wrapper' THEN 'wrapper' ELSE kinds.raw_kind END
                    AS authority_kind,
                root.resource_id AS root_resource_id
         FROM kinds
         LEFT JOIN LATERAL (
             SELECT root.resource_id
             FROM bigname_phase.resources root
             JOIN bigname_phase.chain_lineage lineage
               ON lineage.chain_id = root.chain_id AND lineage.block_hash = root.block_hash
              AND lineage.block_number = root.block_number
             WHERE kinds.raw_kind = 'ens_v2_registry'
               AND root.chain_id = $1 AND root.block_number <= $2
               AND root.canonicality_state IN {READABLE}
               AND lineage.canonicality_state IN {READABLE}
               AND root.provenance ->> 'upstream_resource' =
                   '0x0000000000000000000000000000000000000000000000000000000000000000'
               AND root.provenance ->> 'registry_contract_instance_id' =
                   kinds.provenance ->> 'registry_contract_instance_id'
             ORDER BY root.resource_id
             LIMIT 1
         ) root ON TRUE"
    ))
    .bind(chain_id)
    .bind(block_number)
    .bind(ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the permission resources' authority kinds")?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("resource_id")?,
                AuthorityFacts {
                    authority_kind: row.try_get("authority_kind")?,
                    root_resource_id: row.try_get("root_resource_id")?,
                },
            ))
        })
        .collect()
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
