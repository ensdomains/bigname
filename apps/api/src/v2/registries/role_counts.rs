use std::collections::BTreeMap;

use sqlx::{PgPool, types::Uuid};

use super::super::{V2Error, V2Result};

// Count declared assignments, not individual role bits or inherited effective powers.
// Keep zero transitions while ranking so revocations cannot reveal older grants.
const CURRENT_ROLES: &str = r#"
WITH latest AS (
    SELECT DISTINCT ON (ne.resource_id, lower(ne.after_state ->> 'subject'))
           ne.resource_id, lower(ne.after_state ->> 'subject') AS subject,
           lower(ne.after_state ->> 'upstream_resource') AS upstream_resource,
           lower(ne.after_state ->> 'role_bitmap') AS bitmap
    FROM bigname_phase.normalized_events ne
    JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id = ne.chain_id AND lineage.block_hash = ne.block_hash
    WHERE ne.chain_id = $1
      AND lower(ne.raw_fact_ref ->> 'emitting_address') = $2
      AND ne.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
      AND ne.event_kind IN ('PermissionChanged', 'RootPermissionChanged')
      AND ne.after_state ->> 'source_event' = 'EACRolesChanged'
      AND ne.resource_id IS NOT NULL
      AND ne.after_state ->> 'subject' IS NOT NULL
      AND ne.consumer_visibility = 'activated'
      AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND ($3::bigint IS NULL OR ne.block_number <= $3)
    ORDER BY ne.resource_id, lower(ne.after_state ->> 'subject'),
             ne.block_number DESC, ne.transaction_index DESC NULLS LAST,
             ne.log_index DESC NULLS LAST, ne.normalized_event_id DESC
), versioned AS (
    SELECT *, max(upstream_resource) OVER (
        PARTITION BY left(upstream_resource, 58)
    ) AS latest_resource
    FROM latest
), current_roles AS (
    SELECT * FROM versioned
    WHERE bitmap ~ '^0x[0-9a-f]{64}$' AND bitmap <> ('0x' || repeat('0', 64))
      AND (upstream_resource = ('0x' || repeat('0', 64))
           OR upstream_resource = latest_resource)
)
"#;

pub(super) async fn registry_role_count(
    pool: &PgPool,
    chain: &str,
    registry: &str,
    at: Option<i64>,
) -> V2Result<u64> {
    let count: i64 = sqlx::query_scalar(&format!(
        "{CURRENT_ROLES} SELECT count(*) FROM current_roles"
    ))
    .bind(chain)
    .bind(registry)
    .bind(at)
    .fetch_one(pool)
    .await
    .map_err(|error| count_error(&error))?;
    Ok(count as u64)
}

pub(super) async fn label_role_counts(
    pool: &PgPool,
    chain: &str,
    registry: &str,
    resources: &[Uuid],
    at: Option<i64>,
) -> V2Result<BTreeMap<Uuid, u64>> {
    if resources.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows: Vec<(Uuid, i64)> = sqlx::query_as(&format!(
        "{CURRENT_ROLES} SELECT resource_id, count(DISTINCT subject) \
         FROM current_roles WHERE resource_id = ANY($4) \
         AND upstream_resource <> ('0x' || repeat('0', 64)) GROUP BY resource_id"
    ))
    .bind(chain)
    .bind(registry)
    .bind(at)
    .bind(resources)
    .fetch_all(pool)
    .await
    .map_err(|error| count_error(&error))?;
    Ok(rows
        .into_iter()
        .map(|(id, count)| (id, count as u64))
        .collect())
}

fn count_error(error: &sqlx::Error) -> V2Error {
    tracing::error!(?error, "failed to count registry roles");
    V2Error::internal_error("failed to count registry roles")
}
