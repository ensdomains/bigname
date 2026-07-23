use bigname_storage::sql_row;
use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

use super::types::ActiveCapabilityRow;

pub(super) async fn load_active_capabilities(
    pool: &PgPool,
) -> Result<HashMap<i64, Vec<ActiveCapabilityRow>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id AS manifest_id,
            mcf.capability_name AS capability_name,
            mcf.status::text AS status,
            mcf.notes AS notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        ORDER BY mv.namespace, mv.source_family, mv.chain, mv.deployment_epoch, mv.manifest_version, mcf.capability_name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active capability flags for normalized-event sync")?;

    let mut grouped = HashMap::<i64, Vec<ActiveCapabilityRow>>::new();
    for row in rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("missing capability manifest_id")?;
        grouped
            .entry(manifest_id)
            .or_default()
            .push(ActiveCapabilityRow {
                capability_name: sql_row::get(&row, "capability_name")?,
                status: sql_row::get(&row, "status")?,
                notes: sql_row::get(&row, "notes")?,
            });
    }

    Ok(grouped)
}
