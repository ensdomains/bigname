use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use crate::SourceManifest;

use super::types::ActiveManifestAbiEvent;

pub async fn load_active_manifest_abi_events(
    pool: &PgPool,
    manifest_ids: &[i64],
) -> Result<Vec<ActiveManifestAbiEvent>> {
    if manifest_ids.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT manifest_id, manifest_payload
        FROM manifest_versions
        WHERE rollout_status = 'active'
          AND manifest_id = ANY($1::BIGINT[])
        ORDER BY manifest_id
        "#,
    )
    .bind(manifest_ids)
    .fetch_all(pool)
    .await
    .context("failed to load active manifest ABI events")?;

    let mut events = Vec::new();
    for row in rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("failed to read ABI manifest_id")?;
        let payload = row
            .try_get("manifest_payload")
            .context("failed to read ABI manifest_payload")?;
        let manifest: SourceManifest = serde_json::from_value(payload)
            .with_context(|| format!("failed to decode manifest payload for {manifest_id}"))?;

        for event in &manifest.abi.events {
            let parsed = event.parsed_event_view().with_context(|| {
                format!(
                    "failed to derive ABI event view for manifest_id {manifest_id} event {}",
                    event.name
                )
            })?;
            events.push(ActiveManifestAbiEvent {
                manifest_id,
                manifest_version: manifest.manifest_version,
                namespace: manifest.namespace.clone(),
                source_family: manifest.source_family.clone(),
                chain: manifest.chain.clone(),
                deployment_epoch: manifest.deployment_epoch.clone(),
                name: event.name.clone(),
                canonical_signature: parsed.canonical_signature(),
                topic0: parsed.topic0(),
                emitter_roles: event.emitter_roles.clone(),
                normalized_events: event.normalized_events.clone(),
            });
        }
    }

    Ok(events)
}
