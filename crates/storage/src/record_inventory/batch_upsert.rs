use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, Postgres, QueryBuilder, postgres::PgRow};
use uuid::Uuid;

use crate::projection_helpers::{
    POSTGRES_MAX_BIND_PARAMETERS, serialize_jsonb_field, serialize_optional_jsonb_field,
};

use super::{
    boundary_key::record_version_boundary_storage_key,
    row_decode::{RecordInventoryCurrentRow, decode_record_inventory_current_row},
    validation::validate_record_inventory_current_row,
};

const RECORD_INVENTORY_CURRENT_UPSERT_COLUMN_COUNT: usize = 15;
const RECORD_INVENTORY_CURRENT_UPSERT_MAX_ROWS: usize =
    (POSTGRES_MAX_BIND_PARAMETERS - 1) / RECORD_INVENTORY_CURRENT_UPSERT_COLUMN_COUNT;

#[derive(Clone, Debug)]
struct RecordInventoryCurrentUpsertRow {
    input_index: usize,
    resource_id: Uuid,
    record_version_boundary_key: String,
    record_version_boundary: String,
    enumeration_basis: String,
    selectors: String,
    explicit_gaps: String,
    unsupported_families: String,
    last_change: Option<String>,
    entries: String,
    provenance: String,
    coverage: String,
    chain_positions: String,
    canonicality_summary: String,
    manifest_version: i64,
    last_recomputed_at: OffsetDateTime,
}

impl RecordInventoryCurrentUpsertRow {
    fn storage_key(&self) -> (Uuid, String) {
        (self.resource_id, self.record_version_boundary_key.clone())
    }
}

/// Insert or replace record-inventory projection rows for one or more resource and boundary keys.
pub async fn upsert_record_inventory_current_rows(
    pool: &PgPool,
    rows: &[RecordInventoryCurrentRow],
) -> Result<Vec<RecordInventoryCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let prepared_rows = prepare_record_inventory_current_upsert_rows(rows)?;
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for record_inventory_current upsert")?;

    let mut snapshots = Vec::with_capacity(prepared_rows.len());
    let mut batch = Vec::with_capacity(
        prepared_rows
            .len()
            .min(RECORD_INVENTORY_CURRENT_UPSERT_MAX_ROWS),
    );
    let mut batch_keys = BTreeSet::new();

    for row in &prepared_rows {
        let key = row.storage_key();
        if batch.len() == RECORD_INVENTORY_CURRENT_UPSERT_MAX_ROWS || batch_keys.contains(&key) {
            snapshots
                .extend(upsert_record_inventory_current_row_batch(&mut transaction, &batch).await?);
            batch.clear();
            batch_keys.clear();
        }

        batch_keys.insert(key);
        batch.push(row);
    }

    if !batch.is_empty() {
        snapshots
            .extend(upsert_record_inventory_current_row_batch(&mut transaction, &batch).await?);
    }

    transaction
        .commit()
        .await
        .context("failed to commit record_inventory_current upsert")?;

    Ok(snapshots)
}

fn prepare_record_inventory_current_upsert_rows(
    rows: &[RecordInventoryCurrentRow],
) -> Result<Vec<RecordInventoryCurrentUpsertRow>> {
    rows.iter()
        .enumerate()
        .map(|(input_index, row)| prepare_record_inventory_current_upsert_row(input_index, row))
        .collect()
}

fn prepare_record_inventory_current_upsert_row(
    input_index: usize,
    row: &RecordInventoryCurrentRow,
) -> Result<RecordInventoryCurrentUpsertRow> {
    validate_record_inventory_current_row(row)?;

    let record_version_boundary_key =
        record_version_boundary_storage_key(&row.record_version_boundary, row.resource_id)
            .with_context(|| {
                format!(
                    "failed to derive record_inventory_current boundary key for resource_id {}",
                    row.resource_id
                )
            })?;
    let record_version_boundary = serialize_jsonb_field(
        &row.record_version_boundary,
        "failed to serialize record_inventory_current record_version_boundary",
    )?;
    let enumeration_basis = serialize_jsonb_field(
        &row.enumeration_basis,
        "failed to serialize record_inventory_current enumeration_basis",
    )?;
    let selectors = serialize_jsonb_field(
        &row.selectors,
        "failed to serialize record_inventory_current selectors",
    )?;
    let explicit_gaps = serialize_jsonb_field(
        &row.explicit_gaps,
        "failed to serialize record_inventory_current explicit_gaps",
    )?;
    let unsupported_families = serialize_jsonb_field(
        &row.unsupported_families,
        "failed to serialize record_inventory_current unsupported_families",
    )?;
    let last_change = serialize_optional_jsonb_field(
        row.last_change.as_ref(),
        "failed to serialize record_inventory_current last_change",
    )?;
    let entries = serialize_jsonb_field(
        &row.entries,
        "failed to serialize record_inventory_current entries",
    )?;
    let provenance = serialize_jsonb_field(
        &row.provenance,
        "failed to serialize record_inventory_current provenance",
    )?;
    let coverage = serialize_jsonb_field(
        &row.coverage,
        "failed to serialize record_inventory_current coverage",
    )?;
    let chain_positions = serialize_jsonb_field(
        &row.chain_positions,
        "failed to serialize record_inventory_current chain_positions",
    )?;
    let canonicality_summary = serialize_jsonb_field(
        &row.canonicality_summary,
        "failed to serialize record_inventory_current canonicality_summary",
    )?;

    Ok(RecordInventoryCurrentUpsertRow {
        input_index,
        resource_id: row.resource_id,
        record_version_boundary_key,
        record_version_boundary,
        enumeration_basis,
        selectors,
        explicit_gaps,
        unsupported_families,
        last_change,
        entries,
        provenance,
        coverage,
        chain_positions,
        canonicality_summary,
        manifest_version: row.manifest_version,
        last_recomputed_at: row.last_recomputed_at,
    })
}

async fn upsert_record_inventory_current_row_batch(
    executor: &mut sqlx::Transaction<'_, Postgres>,
    rows: &[&RecordInventoryCurrentUpsertRow],
) -> Result<Vec<RecordInventoryCurrentRow>> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO record_inventory_current (
            resource_id,
            record_version_boundary_key,
            record_version_boundary,
            enumeration_basis,
            selectors,
            explicit_gaps,
            unsupported_families,
            last_change,
            entries,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        "#,
    );

    builder.push_values(rows.iter().copied(), |mut values, row| {
        values.push_bind(row.resource_id);
        values.push_bind(&row.record_version_boundary_key);
        values
            .push_bind(&row.record_version_boundary)
            .push_unseparated("::jsonb");
        values
            .push_bind(&row.enumeration_basis)
            .push_unseparated("::jsonb");
        values.push_bind(&row.selectors).push_unseparated("::jsonb");
        values
            .push_bind(&row.explicit_gaps)
            .push_unseparated("::jsonb");
        values
            .push_bind(&row.unsupported_families)
            .push_unseparated("::jsonb");
        values
            .push_bind(row.last_change.as_deref())
            .push_unseparated("::jsonb");
        values.push_bind(&row.entries).push_unseparated("::jsonb");
        values
            .push_bind(&row.provenance)
            .push_unseparated("::jsonb");
        values.push_bind(&row.coverage).push_unseparated("::jsonb");
        values
            .push_bind(&row.chain_positions)
            .push_unseparated("::jsonb");
        values
            .push_bind(&row.canonicality_summary)
            .push_unseparated("::jsonb");
        values.push_bind(row.manifest_version);
        values.push_bind(row.last_recomputed_at);
    });

    builder.push(
        r#"
        ON CONFLICT (resource_id, record_version_boundary_key) DO UPDATE
        SET
            record_version_boundary = EXCLUDED.record_version_boundary,
            enumeration_basis = EXCLUDED.enumeration_basis,
            selectors = EXCLUDED.selectors,
            explicit_gaps = EXCLUDED.explicit_gaps,
            unsupported_families = EXCLUDED.unsupported_families,
            last_change = EXCLUDED.last_change,
            entries = EXCLUDED.entries,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            resource_id,
            record_version_boundary_key,
            record_version_boundary,
            enumeration_basis,
            selectors,
            explicit_gaps,
            unsupported_families,
            last_change,
            entries,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    );

    let returned_rows = builder
        .build()
        .fetch_all(&mut **executor)
        .await
        .with_context(|| {
            let first_input_index = rows.first().map(|row| row.input_index).unwrap_or_default();
            let last_input_index = rows
                .last()
                .map(|row| row.input_index)
                .unwrap_or(first_input_index);
            format!(
                "failed to upsert record_inventory_current rows for input indexes {first_input_index}..={last_input_index}"
            )
        })?;

    remap_record_inventory_current_snapshots(rows, returned_rows)
}

fn remap_record_inventory_current_snapshots(
    rows: &[&RecordInventoryCurrentUpsertRow],
    returned_rows: Vec<PgRow>,
) -> Result<Vec<RecordInventoryCurrentRow>> {
    if returned_rows.len() != rows.len() {
        bail!(
            "record_inventory_current upsert returned {} snapshots for {} input rows",
            returned_rows.len(),
            rows.len()
        );
    }

    let mut snapshots_by_key = BTreeMap::new();
    for returned_row in returned_rows {
        let snapshot = decode_record_inventory_current_row(returned_row)?;
        let key = (
            snapshot.resource_id,
            record_version_boundary_storage_key(
                &snapshot.record_version_boundary,
                snapshot.resource_id,
            )?,
        );
        if snapshots_by_key.insert(key, snapshot).is_some() {
            bail!("record_inventory_current upsert returned duplicate snapshots for one key");
        }
    }

    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        let key = row.storage_key();
        let snapshot = snapshots_by_key.remove(&key).with_context(|| {
            format!(
                "record_inventory_current upsert did not return snapshot for resource_id {}",
                row.resource_id
            )
        })?;
        snapshots.push(snapshot);
    }

    if !snapshots_by_key.is_empty() {
        bail!("record_inventory_current upsert returned snapshots for unexpected keys");
    }

    Ok(snapshots)
}
