mod list;
mod replacement_publish;
mod row;
mod snapshot;
mod write;

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgPool, types::Uuid};

pub use list::{
    NameCurrentAddressFilter, NameCurrentAddressRelationFilter, NameCurrentListCursor,
    NameCurrentListCursorValue, NameCurrentListFilter, NameCurrentListOrder, NameCurrentListPage,
    NameCurrentListRow, NameCurrentListSort, count_name_current_list, load_name_current_list_page,
    load_name_current_list_page_offset, load_name_current_list_row_by_name,
    load_name_current_list_row_by_namehash, name_current_list_cursor_from_row,
};
pub use row::NameCurrentRow;
use row::decode_name_current_row;
pub use snapshot::load_name_current_for_snapshot;
pub use write::{
    NameCurrentReplacement, clear_name_current, delete_name_current,
    publish_name_current_replacement_table, replace_name_current_rows,
    stage_name_current_replacement_rows_in_transaction, upsert_name_current_rows,
};

pub(crate) const DEFAULT_NAME_CURRENT_READ_FILTER: &str = r#"
  AND surface.canonicality_state IN (
      'canonical'::canonicality_state,
      'safe'::canonicality_state,
      'finalized'::canonicality_state
  )
  AND (
      nc.surface_binding_id IS NULL
      OR (
          resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND binding.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND binding.active_to IS NULL
          AND (
              nc.token_lineage_id IS NULL
              OR token_lineage.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
          )
      )
  )
"#;

/// Load one current exact-name projection row by deterministic logical name identity.
pub async fn load_name_current(
    pool: &PgPool,
    logical_name_id: &str,
) -> Result<Option<NameCurrentRow>> {
    let row = sqlx::query(&format!(
        r#"
        SELECT
            nc.logical_name_id,
            nc.namespace,
            nc.canonical_display_name,
            nc.normalized_name,
            nc.namehash,
            nc.surface_binding_id,
            nc.resource_id,
            nc.token_lineage_id,
            nc.binding_kind,
            nc.declared_summary,
            nc.provenance,
            nc.coverage,
            nc.chain_positions,
            nc.canonicality_summary,
            nc.manifest_version,
            nc.last_recomputed_at
        FROM name_current nc
        JOIN name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        WHERE nc.logical_name_id = $1
        {DEFAULT_NAME_CURRENT_READ_FILTER}
        "#,
    ))
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load name_current row for logical_name_id {logical_name_id}")
    })?;

    row.map(decode_name_current_row).transpose()
}

/// Load current exact-name projection rows for a set of logical name identities.
///
/// The returned map is keyed by `logical_name_id`, so duplicate requested ids collapse into one
/// found row and missing rows are omitted. Iteration order is deterministic `BTreeMap` key order;
/// callers that need request or page order should iterate their original ids and look up into the
/// map.
pub async fn load_name_current_by_logical_name_ids(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    if logical_name_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(&format!(
        r#"
        SELECT
            nc.logical_name_id,
            nc.namespace,
            nc.canonical_display_name,
            nc.normalized_name,
            nc.namehash,
            nc.surface_binding_id,
            nc.resource_id,
            nc.token_lineage_id,
            nc.binding_kind,
            nc.declared_summary,
            nc.provenance,
            nc.coverage,
            nc.chain_positions,
            nc.canonicality_summary,
            nc.manifest_version,
            nc.last_recomputed_at
        FROM name_current nc
        JOIN name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        WHERE nc.logical_name_id = ANY($1::TEXT[])
        {DEFAULT_NAME_CURRENT_READ_FILTER}
        ORDER BY nc.logical_name_id
        "#,
    ))
    .bind(logical_name_ids)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load name_current rows for {} logical_name_id values",
            logical_name_ids.len()
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let row = decode_name_current_row(row)?;
            Ok((row.logical_name_id.clone(), row))
        })
        .collect()
}

/// Load the canonical representative current name for each resource (registration).
///
/// `name_current.resource_id` is 1:many; this picks one representative per resource using the
/// `canonical_display_name ASC` tie-break the rest of v2 uses, and returns that picked row's
/// `normalized_name`.
pub async fn load_current_names_by_resource_ids(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, String>> {
    if resource_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query_as::<_, (Uuid, String)>(&format!(
        r#"
        SELECT DISTINCT ON (nc.resource_id)
            nc.resource_id,
            nc.normalized_name
        FROM name_current nc
        JOIN name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        WHERE nc.resource_id = ANY($1::UUID[])
        {DEFAULT_NAME_CURRENT_READ_FILTER}
        ORDER BY nc.resource_id ASC, nc.canonical_display_name ASC, nc.logical_name_id ASC
        "#,
    ))
    .bind(resource_ids)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load current representative names for {} resource_id values",
            resource_ids.len()
        )
    })?;

    Ok(rows.into_iter().collect())
}

#[cfg(test)]
mod tests;
