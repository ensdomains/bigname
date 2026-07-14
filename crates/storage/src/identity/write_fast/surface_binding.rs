use std::collections::HashSet;

use anyhow::{Context, Result};
use sqlx::Postgres;
use uuid::Uuid;

use crate::identity::types::SurfaceBinding;

pub(super) const ANCHOR_REFRESH_ASSIGNMENTS: &str = r#"
    chain_id = CASE
        WHEN surface_bindings.canonicality_state = 'orphaned'::canonicality_state
            THEN EXCLUDED.chain_id
        ELSE surface_bindings.chain_id
    END,
    block_hash = CASE
        WHEN surface_bindings.canonicality_state = 'orphaned'::canonicality_state
            THEN EXCLUDED.block_hash
        ELSE surface_bindings.block_hash
    END,
    block_number = CASE
        WHEN surface_bindings.canonicality_state = 'orphaned'::canonicality_state
            THEN EXCLUDED.block_number
        ELSE surface_bindings.block_number
    END,
"#;

pub(super) const ANCHOR_REFRESH_COMPATIBILITY: &str = r#"
    (
        surface_bindings.canonicality_state = 'orphaned'::canonicality_state
        OR (
            surface_bindings.chain_id = EXCLUDED.chain_id
            AND surface_bindings.block_hash = EXCLUDED.block_hash
            AND surface_bindings.block_number = EXCLUDED.block_number
        )
    )
"#;

pub(super) const ANCHOR_REFRESH_CHANGED: &str = r#"
    (
        surface_bindings.canonicality_state = 'orphaned'::canonicality_state
        AND (
            surface_bindings.chain_id IS DISTINCT FROM EXCLUDED.chain_id
            OR surface_bindings.block_hash IS DISTINCT FROM EXCLUDED.block_hash
            OR surface_bindings.block_number IS DISTINCT FROM EXCLUDED.block_number
        )
    )
"#;

pub(in crate::identity) async fn load_existing_surface_binding_ids(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    bindings: &[SurfaceBinding],
) -> Result<HashSet<Uuid>> {
    let surface_binding_ids = bindings
        .iter()
        .map(|binding| binding.surface_binding_id)
        .collect::<Vec<_>>();
    if surface_binding_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let rows = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT surface_binding_id
        FROM surface_bindings
        WHERE surface_binding_id = ANY($1::UUID[])
        "#,
    )
    .bind(&surface_binding_ids)
    .fetch_all(&mut **executor)
    .await
    .context("failed to load existing surface binding ids for batch upsert")?;

    Ok(rows.into_iter().collect())
}
