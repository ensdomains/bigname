//! Capture affected keys before due name clocks or old inventory dependencies are replaced.
use super::super::{derived::SUMMARY_WORK_LIST, input::BlockHeader};
use crate::{ProjectError, Result};
use sqlx::{Postgres, Transaction};

pub(in crate::families) async fn prepare(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    after: i64,
) -> Result<()> {
    sqlx::query("/* project:families.lookup.create_name_work */ CREATE TEMP TABLE bigname_lookup_name_work (logical_name_id text COLLATE \"C\" PRIMARY KEY) ON COMMIT DROP")
        .execute(&mut **transaction).await.map_err(|e| ProjectError::database("failed to create lookup name work", e))?;
    sqlx::query("/* project:families.lookup.create_inventory_work */ CREATE TEMP TABLE bigname_lookup_inventory_work (resource_id uuid PRIMARY KEY, refresh boolean NOT NULL) ON COMMIT DROP")
        .execute(&mut **transaction).await.map_err(|e| ProjectError::database("failed to create lookup inventory work", e))?;
    sqlx::query(&format!("/* project:families.lookup.name_work */ INSERT INTO pg_temp.bigname_lookup_name_work {SUMMARY_WORK_LIST} ON CONFLICT DO NOTHING"))
        .bind(chain).bind(block.number).bind(block.timestamp_seconds).bind(after)
        .execute(&mut **transaction).await.map_err(|e| ProjectError::database("failed to capture lookup name work", e))?;
    sqlx::query(
        "/* project:families.lookup.path_relation_work */
        INSERT INTO pg_temp.bigname_lookup_name_work
        SELECT logical_name_id FROM pg_temp.bigname_resolution_path_work
        UNION SELECT key::jsonb ->> 1 FROM project_family_undo
          WHERE chain_id=$1 AND block_number=$2
            AND family IN ('project_address_name_fold', 'project_address_controller_candidate')
        UNION SELECT key::jsonb ->> 2 FROM project_family_undo
          WHERE chain_id=$1 AND block_number=$2 AND family='project_named_resource_pointer'
        ON CONFLICT DO NOTHING",
    )
    .bind(chain)
    .bind(block.number)
    .execute(&mut **transaction)
    .await
    .map_err(|e| ProjectError::database("failed to capture lookup path and relation work", e))?;
    sqlx::query(include_str!("inventory_work.sql"))
        .bind(chain)
        .bind(block.number)
        .bind(after)
        .execute(&mut **transaction)
        .await
        .map_err(|e| {
            ProjectError::database("failed to capture lookup inventory dependencies", e)
        })?;
    Ok(())
}
