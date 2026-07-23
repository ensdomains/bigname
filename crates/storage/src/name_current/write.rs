use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};

use crate::SurfaceBindingKind;
use crate::address_names::rebuild_address_names_current_identity_sidecars_in_transaction;
use crate::projection_staging::NAME_CURRENT_STAGING_COLUMNS;

use super::replacement_publish::publish_name_current_replacement_rows;
use super::row::{NameCurrentRow, decode_name_current_row, validate_name_current_row};

const NAME_CURRENT_REPLACEMENT_BATCH_SIZE: usize = 2_000;

/// Transaction-scoped staging area for atomically replacing `name_current`.
///
/// Callers can stage bounded batches as they rebuild rows, then publish the staged replacement in
/// one transaction. If the caller is dropped before `publish`, Postgres rolls back the temp table
/// and the public projection is left untouched.
pub struct NameCurrentReplacement {
    transaction: Transaction<'static, Postgres>,
    staged_row_count: usize,
}

impl NameCurrentReplacement {
    pub async fn begin(pool: &PgPool) -> Result<Self> {
        let mut transaction = pool
            .begin()
            .await
            .context("failed to open transaction for name_current replacement")?;
        create_name_current_replacement_table(&mut transaction).await?;

        Ok(Self {
            transaction,
            staged_row_count: 0,
        })
    }

    pub async fn stage_rows(&mut self, rows: &[NameCurrentRow]) -> Result<()> {
        for chunk in rows.chunks(NAME_CURRENT_REPLACEMENT_BATCH_SIZE) {
            insert_name_current_replacement_chunk(
                &mut self.transaction,
                "name_current_replacement",
                chunk,
            )
            .await?;
        }
        self.staged_row_count += rows.len();
        Ok(())
    }

    pub fn staged_row_count(&self) -> usize {
        self.staged_row_count
    }

    pub async fn publish(mut self) -> Result<(usize, u64)> {
        index_name_current_replacement_rows(&mut self.transaction).await?;
        set_name_current_sidecar_triggers(&mut self.transaction, false).await?;
        let upserted_row_count = publish_name_current_replacement_rows(
            &mut self.transaction,
            "name_current_replacement",
        )
        .await?;
        let deleted_row_count = delete_stale_name_current_rows_from_replacement(
            &mut self.transaction,
            "name_current_replacement",
        )
        .await?;
        set_name_current_sidecar_triggers(&mut self.transaction, true).await?;
        rebuild_address_names_current_identity_sidecars_in_transaction(&mut self.transaction)
            .await?;
        self.transaction
            .commit()
            .await
            .context("failed to commit name_current replacement")?;

        Ok((upserted_row_count, deleted_row_count))
    }
}

/// Stage exact-name rows into a worker-owned durable replacement table.
pub async fn stage_name_current_replacement_rows_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    replacement_table: &str,
    rows: &[NameCurrentRow],
) -> Result<u64> {
    let replacement_table = checked_staging_table(replacement_table)?;
    let mut inserted = 0_u64;
    for chunk in rows.chunks(NAME_CURRENT_REPLACEMENT_BATCH_SIZE) {
        inserted +=
            insert_name_current_replacement_chunk(transaction, &replacement_table, chunk).await?;
    }
    Ok(inserted)
}

/// Analyze a worker-owned durable exact-name replacement table before publication.
pub async fn analyze_name_current_replacement_table(
    pool: &PgPool,
    replacement_table: &str,
) -> Result<()> {
    let replacement_table = checked_staging_table(replacement_table)?;
    sqlx::query(&format!("ANALYZE {replacement_table}"))
        .execute(pool)
        .await
        .context("failed to analyze durable name_current replacement table")?;
    Ok(())
}

/// Publish a durable exact-name replacement inside the caller's fenced transaction.
pub async fn publish_name_current_replacement_table_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    replacement_table: &str,
) -> Result<(usize, u64)> {
    let replacement_table = checked_staging_table(replacement_table)?;
    set_name_current_sidecar_triggers(transaction, false).await?;
    let upserted_row_count =
        publish_name_current_replacement_rows(transaction, &replacement_table).await?;
    let deleted_row_count =
        delete_stale_name_current_rows_from_replacement(transaction, &replacement_table).await?;
    set_name_current_sidecar_triggers(transaction, true).await?;
    rebuild_address_names_current_identity_sidecars_in_transaction(transaction).await?;
    Ok((upserted_row_count, deleted_row_count))
}

/// Insert or replace projection rows for exact-name current reads.
pub async fn upsert_name_current_rows(
    pool: &PgPool,
    rows: &[NameCurrentRow],
) -> Result<Vec<NameCurrentRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for name_current upsert")?;
    let snapshots = upsert_name_current_rows_in_transaction(&mut transaction, rows).await?;
    transaction
        .commit()
        .await
        .context("failed to commit name_current upsert")?;

    Ok(snapshots)
}

/// Atomically publish a full replacement set for the exact-name current projection.
pub async fn replace_name_current_rows(
    pool: &PgPool,
    rows: &[NameCurrentRow],
    _logical_name_ids: &[String],
) -> Result<(usize, u64)> {
    let mut replacement = NameCurrentReplacement::begin(pool).await?;
    replacement.stage_rows(rows).await?;
    replacement.publish().await
}

async fn create_name_current_replacement_table(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE name_current_replacement (
            LIKE name_current INCLUDING DEFAULTS
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut **executor)
    .await
    .context("failed to create temporary name_current replacement table")?;

    Ok(())
}

async fn index_name_current_replacement_rows(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<()> {
    sqlx::query(
        "CREATE UNIQUE INDEX name_current_replacement_logical_name_id_idx
         ON name_current_replacement (logical_name_id)",
    )
    .execute(&mut **executor)
    .await
    .context("failed to index temporary name_current replacement table")?;

    sqlx::query("ANALYZE name_current_replacement")
        .execute(&mut **executor)
        .await
        .context("failed to analyze temporary name_current replacement table")?;

    Ok(())
}

struct EncodedNameCurrentRow<'a> {
    row: &'a NameCurrentRow,
    declared_summary: String,
    provenance: String,
    coverage: String,
    chain_positions: String,
    canonicality_summary: String,
}

fn encode_name_current_row(row: &NameCurrentRow) -> Result<EncodedNameCurrentRow<'_>> {
    validate_name_current_row(row)?;
    Ok(EncodedNameCurrentRow {
        row,
        declared_summary: serde_json::to_string(&row.declared_summary)
            .context("failed to serialize name_current declared_summary")?,
        provenance: serde_json::to_string(&row.provenance)
            .context("failed to serialize name_current provenance")?,
        coverage: serde_json::to_string(&row.coverage)
            .context("failed to serialize name_current coverage")?,
        chain_positions: serde_json::to_string(&row.chain_positions)
            .context("failed to serialize name_current chain_positions")?,
        canonicality_summary: serde_json::to_string(&row.canonicality_summary)
            .context("failed to serialize name_current canonicality_summary")?,
    })
}

async fn insert_name_current_replacement_chunk(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replacement_table: &str,
    rows: &[NameCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let encoded_rows = rows
        .iter()
        .map(encode_name_current_row)
        .collect::<Result<Vec<_>>>()?;
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "INSERT INTO {replacement_table} ({}) ",
        NAME_CURRENT_STAGING_COLUMNS.join(", ")
    ));

    builder.push_values(encoded_rows.iter(), |mut row_builder, encoded| {
        row_builder
            .push_bind(&encoded.row.logical_name_id)
            .push_bind(&encoded.row.namespace)
            .push_bind(&encoded.row.canonical_display_name)
            .push_bind(&encoded.row.normalized_name)
            .push_bind(&encoded.row.namehash)
            .push_bind(encoded.row.surface_binding_id)
            .push_bind(encoded.row.resource_id)
            .push_bind(encoded.row.token_lineage_id)
            .push_bind(encoded.row.binding_kind.map(SurfaceBindingKind::as_str))
            .push_bind(&encoded.declared_summary)
            .push_unseparated("::jsonb")
            .push_bind(&encoded.provenance)
            .push_unseparated("::jsonb")
            .push_bind(&encoded.coverage)
            .push_unseparated("::jsonb")
            .push_bind(&encoded.chain_positions)
            .push_unseparated("::jsonb")
            .push_bind(&encoded.canonicality_summary)
            .push_unseparated("::jsonb")
            .push_bind(encoded.row.manifest_version)
            .push_bind(encoded.row.last_recomputed_at);
    });

    let inserted = builder
        .build()
        .execute(&mut **executor)
        .await
        .context("failed to stage name_current replacement chunk")?
        .rows_affected();

    Ok(inserted)
}

async fn upsert_name_current_rows_in_transaction(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[NameCurrentRow],
) -> Result<Vec<NameCurrentRow>> {
    let mut snapshots = Vec::with_capacity(rows.len());
    for row in rows {
        validate_name_current_row(row)?;
        snapshots.push(upsert_name_current_row(executor, row).await?);
    }
    Ok(snapshots)
}

/// Delete one current exact-name projection row so a worker can rebuild the key.
pub async fn delete_name_current(pool: &PgPool, logical_name_id: &str) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM name_current
        WHERE logical_name_id = $1
        "#,
    )
    .bind(logical_name_id)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete name_current row for logical_name_id {logical_name_id}")
    })
    .map(|result| result.rows_affected())
}

/// Clear the exact-name current projection so a worker can perform a one-shot rebuild.
pub async fn clear_name_current(pool: &PgPool) -> Result<u64> {
    sqlx::query("DELETE FROM name_current")
        .execute(pool)
        .await
        .context("failed to clear name_current rows")
        .map(|result| result.rows_affected())
}

const NAME_CURRENT_SIDECAR_TRIGGERS: &[&str] = &[
    "address_names_current_identity_counts_name_current_insert_delete",
    "address_names_current_identity_counts_name_current_update",
    "name_current_identity_feed_after_insert_delete",
    "name_current_identity_feed_after_anchor_update",
];

async fn set_name_current_sidecar_triggers(
    transaction: &mut Transaction<'_, Postgres>,
    enabled: bool,
) -> Result<()> {
    let action = if enabled { "ENABLE" } else { "DISABLE" };
    for trigger in NAME_CURRENT_SIDECAR_TRIGGERS {
        let sql = format!("ALTER TABLE name_current {action} TRIGGER {trigger}");
        sqlx::query(&sql)
            .execute(&mut **transaction)
            .await
            .with_context(|| {
                format!(
                    "failed to {} name_current sidecar trigger {}",
                    action.to_ascii_lowercase(),
                    trigger
                )
            })?;
    }
    Ok(())
}

async fn delete_stale_name_current_rows_from_replacement(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    replacement_table: &str,
) -> Result<u64> {
    sqlx::query(&format!(
        r#"
        DELETE FROM name_current current
        WHERE NOT EXISTS (
            SELECT 1 FROM {replacement_table} replacement
            WHERE replacement.logical_name_id = current.logical_name_id
        )
        "#
    ))
    .execute(&mut **executor)
    .await
    .context("failed to delete stale name_current rows during replacement")
    .map(|result| result.rows_affected())
}

fn checked_staging_table(table: &str) -> Result<String> {
    anyhow::ensure!(
        !table.is_empty()
            && table
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "unsafe name_current replacement table {table:?}"
    );
    Ok(format!("\"{table}\""))
}

async fn upsert_name_current_row(
    executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &NameCurrentRow,
) -> Result<NameCurrentRow> {
    let declared_summary = serde_json::to_string(&row.declared_summary)
        .context("failed to serialize name_current declared_summary")?;
    let provenance = serde_json::to_string(&row.provenance)
        .context("failed to serialize name_current provenance")?;
    let coverage = serde_json::to_string(&row.coverage)
        .context("failed to serialize name_current coverage")?;
    let chain_positions = serde_json::to_string(&row.chain_positions)
        .context("failed to serialize name_current chain_positions")?;
    let canonicality_summary = serde_json::to_string(&row.canonicality_summary)
        .context("failed to serialize name_current canonicality_summary")?;

    let snapshot = sqlx::query(
        r#"
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb, $15, $16
        )
        ON CONFLICT (logical_name_id) DO UPDATE
        SET
            namespace = EXCLUDED.namespace,
            canonical_display_name = EXCLUDED.canonical_display_name,
            normalized_name = EXCLUDED.normalized_name,
            namehash = EXCLUDED.namehash,
            surface_binding_id = EXCLUDED.surface_binding_id,
            resource_id = EXCLUDED.resource_id,
            token_lineage_id = EXCLUDED.token_lineage_id,
            binding_kind = EXCLUDED.binding_kind,
            declared_summary = EXCLUDED.declared_summary,
            provenance = EXCLUDED.provenance,
            coverage = EXCLUDED.coverage,
            chain_positions = EXCLUDED.chain_positions,
            canonicality_summary = EXCLUDED.canonicality_summary,
            manifest_version = EXCLUDED.manifest_version,
            last_recomputed_at = EXCLUDED.last_recomputed_at
        RETURNING
            logical_name_id,
            namespace,
            canonical_display_name,
            normalized_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            declared_summary,
            provenance,
            coverage,
            chain_positions,
            canonicality_summary,
            manifest_version,
            last_recomputed_at
        "#,
    )
    .bind(&row.logical_name_id)
    .bind(&row.namespace)
    .bind(&row.canonical_display_name)
    .bind(&row.normalized_name)
    .bind(&row.namehash)
    .bind(row.surface_binding_id)
    .bind(row.resource_id)
    .bind(row.token_lineage_id)
    .bind(row.binding_kind.map(SurfaceBindingKind::as_str))
    .bind(declared_summary)
    .bind(provenance)
    .bind(coverage)
    .bind(chain_positions)
    .bind(canonicality_summary)
    .bind(row.manifest_version)
    .bind(row.last_recomputed_at)
    .fetch_one(&mut **executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert name_current row for logical_name_id {}",
            row.logical_name_id
        )
    })?;

    decode_name_current_row(snapshot)
}
