use anyhow::{Context, Result, bail};
use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow, SurfaceBindingKind};
use serde_json::Value;
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::json_helpers::{json_field, json_string_field};

pub(super) async fn load_supported_record_inventory_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    row: &NameCurrentRow,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let Some((resource_id, record_version_boundary)) =
        bigname_storage::resolution_record_inventory_lookup_key_for_revalidation(row)?
    else {
        return Ok(None);
    };

    if let Some(record_inventory_row) = load_record_inventory_current_for_revalidation(
        transaction,
        resource_id,
        &record_version_boundary,
    )
    .await?
    {
        return Ok(Some(record_inventory_row));
    }

    if record_version_boundary_has_pointer(&record_version_boundary) {
        return Ok(None);
    }

    let Some(persisted_boundary) = find_supported_record_inventory_boundary_for_revalidation(
        transaction,
        resource_id,
        &record_version_boundary,
    )
    .await?
    else {
        return Ok(None);
    };

    load_record_inventory_current_for_revalidation(transaction, resource_id, &persisted_boundary)
        .await?
        .with_context(|| {
            format!(
                "matched record_inventory_current boundary for resource_id {resource_id} but the projection row was not loadable"
            )
        })
        .map(Some)
}

async fn find_supported_record_inventory_boundary_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<Value>> {
    let logical_name_id =
        json_string_field(json_field(record_version_boundary, "logical_name_id")).with_context(
            || {
                format!(
                    "supported record version boundary for resource_id {resource_id} must include logical_name_id"
                )
            },
        )?;
    let chain_position = json_field(record_version_boundary, "chain_position").with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position"
        )
    })?;
    let chain_id = json_string_field(json_field(chain_position, "chain_id")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.chain_id"
        )
    })?;
    let block_number = json_field(chain_position, "block_number")
        .and_then(Value::as_i64)
        .with_context(|| {
            format!(
                "supported record version boundary for resource_id {resource_id} must include chain_position.block_number"
            )
        })?;
    let block_hash = json_string_field(json_field(chain_position, "block_hash")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.block_hash"
        )
    })?;
    let timestamp = json_string_field(json_field(chain_position, "timestamp")).with_context(|| {
        format!(
            "supported record version boundary for resource_id {resource_id} must include chain_position.timestamp"
        )
    })?;

    let boundaries = sqlx::query(
        r#"
        SELECT record_version_boundary
        FROM record_inventory_current
        WHERE resource_id = $1
          AND record_version_boundary ->> 'logical_name_id' = $2
          AND record_version_boundary -> 'chain_position' ->> 'chain_id' = $3
          AND (record_version_boundary -> 'chain_position' ->> 'block_number')::bigint = $4
          AND record_version_boundary -> 'chain_position' ->> 'block_hash' = $5
          AND record_version_boundary -> 'chain_position' ->> 'timestamp' = $6
        ORDER BY
          (record_version_boundary ->> 'normalized_event_id') IS NULL ASC,
          (record_version_boundary ->> 'normalized_event_id')::bigint DESC NULLS LAST
        LIMIT 2
        "#,
    )
    .bind(resource_id)
    .bind(logical_name_id)
    .bind(chain_id)
    .bind(block_number)
    .bind(block_hash)
    .bind(timestamp)
    .fetch_all(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to locate supported record_inventory_current boundary for resource_id {resource_id}"
        )
    })?
    .into_iter()
    .map(|row| {
        row.try_get("record_version_boundary").with_context(|| {
            format!(
                "supported record_inventory_current lookup for resource_id {resource_id} returned a row without record_version_boundary"
            )
        })
    })
    .collect::<Result<Vec<Value>, _>>()?;

    let Some(first_boundary) = boundaries.first().cloned() else {
        return Ok(None);
    };
    if let Some(second_boundary) = boundaries.get(1)
        && (!record_version_boundary_has_pointer(&first_boundary)
            || record_version_boundary_has_pointer(second_boundary))
    {
        bail!(
            "supported record_inventory_current lookup for resource_id {} found multiple projection rows for the same boundary anchor",
            resource_id
        );
    }

    Ok(Some(first_boundary))
}

fn record_version_boundary_has_pointer(record_version_boundary: &Value) -> bool {
    bigname_storage::record_version_boundary_has_pointer(record_version_boundary)
}

pub(super) async fn load_name_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    logical_name_id: &str,
) -> Result<Option<NameCurrentRow>> {
    let row = sqlx::query(
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
        "#,
    )
    .bind(logical_name_id)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to load name_current row for logical_name_id {logical_name_id}")
    })?;

    row.map(decode_name_current_row_for_revalidation)
        .transpose()
}

fn decode_name_current_row_for_revalidation(row: PgRow) -> Result<NameCurrentRow> {
    Ok(NameCurrentRow {
        logical_name_id: row
            .try_get("logical_name_id")
            .context("missing logical_name_id")?,
        namespace: row.try_get("namespace").context("missing namespace")?,
        canonical_display_name: row
            .try_get("canonical_display_name")
            .context("missing canonical_display_name")?,
        normalized_name: row
            .try_get("normalized_name")
            .context("missing normalized_name")?,
        namehash: row.try_get("namehash").context("missing namehash")?,
        surface_binding_id: row
            .try_get("surface_binding_id")
            .context("missing surface_binding_id")?,
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        token_lineage_id: row
            .try_get("token_lineage_id")
            .context("missing token_lineage_id")?,
        binding_kind: row
            .try_get::<Option<String>, _>("binding_kind")
            .context("missing binding_kind")?
            .map(|value| parse_surface_binding_kind_for_revalidation(&value))
            .transpose()?,
        declared_summary: row
            .try_get("declared_summary")
            .context("missing declared_summary")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}

fn parse_surface_binding_kind_for_revalidation(value: &str) -> Result<SurfaceBindingKind> {
    match value {
        "declared_registry_path" => Ok(SurfaceBindingKind::DeclaredRegistryPath),
        "linked_subregistry_path" => Ok(SurfaceBindingKind::LinkedSubregistryPath),
        "resolver_alias_path" => Ok(SurfaceBindingKind::ResolverAliasPath),
        "observed_wildcard_path" => Ok(SurfaceBindingKind::ObservedWildcardPath),
        "migration_rebind" => Ok(SurfaceBindingKind::MigrationRebind),
        "observed_only" => Ok(SurfaceBindingKind::ObservedOnly),
        _ => bail!("unknown surface binding kind {value}"),
    }
}

async fn load_record_inventory_current_for_revalidation(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    record_version_boundary: &Value,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let record_version_boundary_key = serde_json::to_string(record_version_boundary)
        .context("failed to serialize revalidation record_version_boundary")?;

    let row = sqlx::query(
        r#"
        SELECT
            ric.resource_id,
            ric.record_version_boundary,
            ric.enumeration_basis,
            ric.selectors,
            ric.explicit_gaps,
            ric.unsupported_families,
            ric.last_change,
            ric.entries,
            ric.provenance,
            ric.coverage,
            ric.chain_positions,
            ric.canonicality_summary,
            ric.manifest_version,
            ric.last_recomputed_at
        FROM record_inventory_current ric
        JOIN resources resource
          ON resource.resource_id = ric.resource_id
        WHERE ric.resource_id = $1
          AND ric.record_version_boundary = $2::JSONB
          AND resource.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(resource_id)
    .bind(record_version_boundary_key)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to load record_inventory_current row for resource_id {resource_id}")
    })?;

    row.map(decode_record_inventory_current_row_for_revalidation)
        .transpose()
}

fn decode_record_inventory_current_row_for_revalidation(
    row: PgRow,
) -> Result<RecordInventoryCurrentRow> {
    Ok(RecordInventoryCurrentRow {
        resource_id: row.try_get("resource_id").context("missing resource_id")?,
        record_version_boundary: row
            .try_get("record_version_boundary")
            .context("missing record_version_boundary")?,
        enumeration_basis: row
            .try_get("enumeration_basis")
            .context("missing enumeration_basis")?,
        selectors: row.try_get("selectors").context("missing selectors")?,
        explicit_gaps: row
            .try_get("explicit_gaps")
            .context("missing explicit_gaps")?,
        unsupported_families: row
            .try_get("unsupported_families")
            .context("missing unsupported_families")?,
        last_change: row.try_get("last_change").context("missing last_change")?,
        entries: row.try_get("entries").context("missing entries")?,
        provenance: row.try_get("provenance").context("missing provenance")?,
        coverage: row.try_get("coverage").context("missing coverage")?,
        chain_positions: row
            .try_get("chain_positions")
            .context("missing chain_positions")?,
        canonicality_summary: row
            .try_get("canonicality_summary")
            .context("missing canonicality_summary")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        last_recomputed_at: row
            .try_get("last_recomputed_at")
            .context("missing last_recomputed_at")?,
    })
}
