use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row, types::time::OffsetDateTime};

/// `provenance.authority_selection.proof_kind` written by Project when the name has an activated
/// `MigrationApplied` boundary. It is migration history; it does not select authority.
pub const MIGRATION_AUTHORITY_TRANSITION_PROOF_KIND: &str = "migration_authority_transition";

/// The `authority_arm` Project selected for a `name_current` row (`ens_v1`, `ens_v2`, ...).
pub fn name_current_authority_arm(provenance: &Value) -> Option<&str> {
    provenance
        .pointer("/authority_selection/authority_arm")
        .and_then(Value::as_str)
        .filter(|arm| !arm.trim().is_empty())
}

/// Block timestamps of the `MigrationApplied` event for every requested name whose current
/// authority is the ENSv2 arm and that has an activated ENSv1→ENSv2 migration. Names without a
/// migration, or on the ENSv1 arm, are absent from the map.
pub async fn load_name_migration_transition_timestamps(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, OffsetDateTime>> {
    if logical_name_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    // The CASE keeps the bigint cast behind the digit check so a malformed proof id cannot fail
    // the whole page.
    let rows = sqlx::query(
        r#"
        SELECT nc.logical_name_id, lineage.block_timestamp
        FROM bigname_phase.name_current nc
        JOIN bigname_phase.normalized_events proof
          ON proof.normalized_event_id = CASE
                 WHEN nc.provenance #>> '{authority_selection,proof_event_id}' ~ '^[0-9]+$'
                     THEN (nc.provenance #>> '{authority_selection,proof_event_id}')::bigint
             END
        JOIN bigname_phase.chain_lineage lineage
          ON lineage.chain_id = proof.chain_id
         AND lineage.block_hash = proof.block_hash
        WHERE nc.logical_name_id = ANY($1::text[])
          AND nc.provenance #>> '{authority_selection,authority_arm}' = 'ens_v2'
          AND nc.provenance #>> '{authority_selection,proof_kind}' = $2
        "#,
    )
    .bind(logical_name_ids)
    .bind(MIGRATION_AUTHORITY_TRANSITION_PROOF_KIND)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load migration transition timestamps for {} logical_name_id values",
            logical_name_ids.len()
        )
    })?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>("logical_name_id")?,
                row.try_get::<OffsetDateTime, _>("block_timestamp")?,
            ))
        })
        .collect()
}
