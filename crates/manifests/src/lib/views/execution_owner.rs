use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use crate::ExecutionOwnerManifestVersion;

/// Loads the manifest identity used by a route-level execution producer.
/// An active version wins; when the family has no active version, the newest
/// shadow version still supplies execution ownership without promoting its
/// capability flag to general public support.
pub async fn load_execution_owner_manifest_version(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
    chain: &str,
    deployment_epoch: &str,
) -> Result<Option<ExecutionOwnerManifestVersion>> {
    let row = sqlx::query(
        r#"
        SELECT manifest_version, source_family
        FROM manifest_versions
        WHERE namespace = $1
          AND source_family = $2
          AND chain = $3
          AND deployment_epoch = $4
          AND rollout_status IN (
              'active'::manifest_rollout_status,
              'shadow'::manifest_rollout_status
          )
        ORDER BY
            CASE rollout_status
                WHEN 'active'::manifest_rollout_status THEN 0
                ELSE 1
            END,
            manifest_version DESC
        LIMIT 1
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(deployment_epoch)
    .fetch_optional(pool)
    .await
    .context("failed to load route execution owner manifest version")?;

    row.map(|row| {
        let manifest_version = row
            .try_get::<i64, _>("manifest_version")
            .context("failed to read route execution owner manifest_version")?;
        Ok(ExecutionOwnerManifestVersion {
            manifest_version: u64::try_from(manifest_version)
                .context("route execution owner manifest_version must be non-negative")?,
            source_family: row
                .try_get("source_family")
                .context("failed to read route execution owner source_family")?,
        })
    })
    .transpose()
}
