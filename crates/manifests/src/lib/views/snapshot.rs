use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use crate::{ActiveManifestVersion, NamespaceManifestSnapshot};

async fn load_active_manifests_for_namespace(
    pool: &PgPool,
    namespace: &str,
) -> Result<Vec<ActiveManifestVersion>> {
    let manifest_rows = sqlx::query(
        r#"
        SELECT manifest_version, source_family, chain_id, deployment_label,
               normalizer_version, manifest_payload
        FROM bigname_phase.manifest_versions
        WHERE rollout_status = 'active'
          AND namespace = $1
        ORDER BY source_family, chain_id, deployment_label, manifest_version
        "#,
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .context("failed to load active manifests")?;

    manifest_rows
        .into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read manifest_version from active manifest row")?;
            Ok(ActiveManifestVersion {
                manifest_version: u64::try_from(manifest_version)
                    .context("manifest_version must be non-negative")?,
                source_family: row
                    .try_get("source_family")
                    .context("failed to read source_family from active manifest row")?,
                chain: row
                    .try_get("chain_id")
                    .context("failed to read chain from active manifest row")?,
                deployment_epoch: row
                    .try_get("deployment_label")
                    .context("failed to read deployment_epoch from active manifest row")?,
                normalizer_version: row
                    .try_get("normalizer_version")
                    .context("failed to read normalizer_version from active manifest row")?,
                capability_flags: serde_json::from_value(
                    row.try_get::<serde_json::Value, _>("manifest_payload")?
                        .get("capability_flags")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({})),
                )
                .context("failed to decode phase manifest capability flags")?,
            })
        })
        .collect()
}

pub async fn load_namespace_manifest_snapshot(
    pool: &PgPool,
    namespace: &str,
) -> Result<NamespaceManifestSnapshot> {
    let manifests = load_active_manifests_for_namespace(pool, namespace).await?;
    let last_updated = sqlx::query_scalar::<_, String>(
        r#"
        SELECT COALESCE(
            TO_CHAR(MAX(loaded_at AT TIME ZONE 'UTC'), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        )
        FROM bigname_phase.manifest_versions
        WHERE namespace = $1
        "#,
    )
    .bind(namespace)
    .fetch_one(pool)
    .await
    .context("failed to load namespace manifest freshness timestamp")?;

    Ok(NamespaceManifestSnapshot {
        manifests,
        last_updated,
    })
}
