use std::collections::HashMap;

use anyhow::{Context, Result};
use bigname_storage::sql_row;
use sqlx::PgPool;

/// Resolve attributed nodes to known name surfaces:
/// `lower(namehash)` → `logical_name_id` for one namespace, canonical-branch
/// rows only. Unknown nodes stay unresolved (the write event keeps the node).
pub(super) async fn load_logical_name_ids_by_namehash(
    pool: &PgPool,
    namespace: &str,
    namehashes: &[String],
) -> Result<HashMap<String, String>> {
    if namehashes.is_empty() {
        return Ok(HashMap::new());
    }
    let lowered = namehashes
        .iter()
        .map(|namehash| namehash.to_ascii_lowercase())
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (LOWER(namehash))
            LOWER(namehash) AS namehash,
            logical_name_id
        FROM name_surfaces
        WHERE namespace = $1
          AND LOWER(namehash) = ANY($2::TEXT[])
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY LOWER(namehash), block_number DESC
        "#,
    )
    .bind(namespace)
    .bind(&lowered)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to resolve name surfaces by namehash for namespace {namespace}")
    })?;

    rows.into_iter()
        .map(|row| {
            Ok((
                sql_row::get::<String>(&row, "namehash")?,
                sql_row::get::<String>(&row, "logical_name_id")?,
            ))
        })
        .collect()
}
