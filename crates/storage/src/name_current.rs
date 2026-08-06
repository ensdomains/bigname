mod list;
mod row;
mod snapshot;

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

pub(crate) const DEFAULT_NAME_CURRENT_READ_FILTER: &str = r#"
  AND nc.canonicality_summary ->> 'state' = 'canonical_lineage'
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.chain_lineage projection_lineage
      WHERE projection_lineage.chain_id = nc.provenance ->> 'chain_id'
        AND projection_lineage.block_hash = nc.canonicality_summary ->> 'target_block_hash'
        AND projection_lineage.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
  )
  AND surface.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND surface_lineage.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND (
      nc.surface_binding_id IS NULL
      OR (
          resource.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND resource_lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND binding.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND binding_lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND binding.active_to IS NULL
          AND (
              nc.token_lineage_id IS NULL
              OR (
                  token_lineage.canonicality_state IN (
                      'canonical'::bigname_phase.canonicality_state,
                      'safe'::bigname_phase.canonicality_state,
                      'finalized'::bigname_phase.canonicality_state
                  )
                  AND token_lineage_lineage.canonicality_state IN (
                      'canonical'::bigname_phase.canonicality_state,
                      'safe'::bigname_phase.canonicality_state,
                      'finalized'::bigname_phase.canonicality_state
                  )
              )
          )
      )
  )
"#;

pub(crate) const DEFAULT_NAME_CURRENT_LINEAGE_JOINS: &str = r#"
  JOIN bigname_phase.chain_lineage surface_lineage
    ON surface_lineage.chain_id = surface.chain_id
   AND surface_lineage.block_hash = surface.block_hash
  LEFT JOIN bigname_phase.chain_lineage resource_lineage
    ON resource_lineage.chain_id = resource.chain_id
   AND resource_lineage.block_hash = resource.block_hash
  LEFT JOIN bigname_phase.chain_lineage binding_lineage
    ON binding_lineage.chain_id = binding.chain_id
   AND binding_lineage.block_hash = binding.block_hash
  LEFT JOIN bigname_phase.chain_lineage token_lineage_lineage
    ON token_lineage_lineage.chain_id = token_lineage.chain_id
   AND token_lineage_lineage.block_hash = token_lineage.block_hash
"#;

pub(crate) const DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER: &str = r#"
  AND anc.canonicality_summary ->> 'state' = 'canonical_lineage'
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.chain_lineage membership_projection_lineage
      WHERE membership_projection_lineage.chain_id = anc.provenance ->> 'chain_id'
        AND membership_projection_lineage.block_hash =
            anc.chain_positions ->> 'target_block_hash'
        AND membership_projection_lineage.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
  )
  AND membership_surface.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND membership_surface_lineage.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND membership_resource.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND membership_resource_lineage.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND membership_binding.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND membership_binding_lineage.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND membership_binding.active_to IS NULL
  AND (
      anc.token_lineage_id IS NULL
      OR (
          membership_token_lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND membership_token_lineage_lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
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
            nc.raw_name AS canonical_display_name,
            lower(nc.raw_name) AS normalized_name,
            nc.namehash,
            nc.surface_binding_id,
            nc.resource_id,
            nc.token_lineage_id,
            nc.binding_kind,
            nc.declared_summary,
            nc.provenance,
            CASE WHEN nc.support_status = 'supported'
                 THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted')
                 ELSE jsonb_build_object(
                     'status', 'unsupported', 'exhaustiveness', 'not_asserted',
                     'unsupported_reason', nc.unsupported_reason
                 ) END AS coverage,
            nc.chain_positions,
            nc.canonicality_summary,
            nc.manifest_version,
            nc.last_recomputed_at
        FROM bigname_phase.name_current nc
        JOIN bigname_phase.name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN bigname_phase.resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN bigname_phase.surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN bigname_phase.token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
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
            nc.raw_name AS canonical_display_name,
            lower(nc.raw_name) AS normalized_name,
            nc.namehash,
            nc.surface_binding_id,
            nc.resource_id,
            nc.token_lineage_id,
            nc.binding_kind,
            nc.declared_summary,
            nc.provenance,
            CASE WHEN nc.support_status = 'supported'
                 THEN jsonb_build_object('status', 'projected', 'exhaustiveness', 'not_asserted')
                 ELSE jsonb_build_object(
                     'status', 'unsupported', 'exhaustiveness', 'not_asserted',
                     'unsupported_reason', nc.unsupported_reason
                 ) END AS coverage,
            nc.chain_positions,
            nc.canonicality_summary,
            nc.manifest_version,
            nc.last_recomputed_at
        FROM bigname_phase.name_current nc
        JOIN bigname_phase.name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN bigname_phase.resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN bigname_phase.surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN bigname_phase.token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
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
            lower(nc.raw_name)
        FROM bigname_phase.name_current nc
        JOIN bigname_phase.name_surfaces surface
          ON surface.logical_name_id = nc.logical_name_id
        LEFT JOIN bigname_phase.resources resource
          ON resource.resource_id = nc.resource_id
        LEFT JOIN bigname_phase.surface_bindings binding
          ON binding.surface_binding_id = nc.surface_binding_id
        LEFT JOIN bigname_phase.token_lineages token_lineage
          ON token_lineage.token_lineage_id = nc.token_lineage_id
        {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
        WHERE nc.resource_id = ANY($1::UUID[])
        {DEFAULT_NAME_CURRENT_READ_FILTER}
        ORDER BY nc.resource_id ASC, nc.raw_name ASC, nc.logical_name_id ASC
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
