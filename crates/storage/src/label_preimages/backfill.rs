use anyhow::{Context, Result};
use sqlx::{PgPool, Row, postgres::PgRow};

use crate::{identity::NameSurface, normalized_events::NormalizedEvent};

use super::{
    IMPORT_BATCH_SIZE, LabelPreimageImportSummary,
    enqueue_children_invalidations_for_existing_label_preimages, label_preimages_from_name_surface,
    label_preimages_from_normalized_event, upsert_label_preimages_without_invalidations,
};

const RETAINED_FACT_BACKFILL_RUN_KEY: &str = "label_preimages:retained_facts:v2";

pub async fn backfill_label_preimages_from_existing_facts(
    pool: &PgPool,
    batch_size: Option<i64>,
) -> Result<LabelPreimageImportSummary> {
    if label_preimage_backfill_completed(pool).await? {
        return Ok(LabelPreimageImportSummary::default());
    }

    let batch_size = batch_size.unwrap_or(IMPORT_BATCH_SIZE).max(1);
    let mut summary = LabelPreimageImportSummary::default();

    backfill_label_preimages_from_existing_name_surfaces(pool, batch_size, &mut summary).await?;
    backfill_label_preimages_from_existing_normalized_events(pool, batch_size, &mut summary)
        .await?;
    summary.invalidated_parent_count +=
        enqueue_children_invalidations_for_existing_label_preimages(pool).await?;
    record_label_preimage_backfill_completion(pool, &summary).await?;

    Ok(summary)
}

async fn label_preimage_backfill_completed(pool: &PgPool) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM label_preimage_backfill_runs
            WHERE run_key = $1
        )
        "#,
    )
    .bind(RETAINED_FACT_BACKFILL_RUN_KEY)
    .fetch_one(pool)
    .await
    .context("failed to check retained label preimage backfill completion")
}

async fn record_label_preimage_backfill_completion(
    pool: &PgPool,
    summary: &LabelPreimageImportSummary,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO label_preimage_backfill_runs (
            run_key,
            scanned_row_count,
            retained_row_count,
            invalidated_parent_count
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (run_key) DO UPDATE SET
            completed_at = now(),
            scanned_row_count = EXCLUDED.scanned_row_count,
            retained_row_count = EXCLUDED.retained_row_count,
            invalidated_parent_count = EXCLUDED.invalidated_parent_count
        "#,
    )
    .bind(RETAINED_FACT_BACKFILL_RUN_KEY)
    .bind(i64::try_from(summary.scanned_row_count).unwrap_or(i64::MAX))
    .bind(i64::try_from(summary.retained_row_count).unwrap_or(i64::MAX))
    .bind(i64::try_from(summary.invalidated_parent_count).unwrap_or(i64::MAX))
    .execute(pool)
    .await
    .context("failed to record retained label preimage backfill completion")?;
    Ok(())
}

async fn backfill_label_preimages_from_existing_name_surfaces(
    pool: &PgPool,
    batch_size: i64,
    summary: &mut LabelPreimageImportSummary,
) -> Result<()> {
    let mut last_logical_name_id = String::new();

    loop {
        let rows = sqlx::query(
            r#"
            SELECT
                logical_name_id,
                namespace,
                input_name,
                canonical_display_name,
                normalized_name,
                dns_encoded_name,
                namehash,
                labelhashes,
                normalizer_version,
                normalization_warnings,
                normalization_errors,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state::TEXT AS canonicality_state
            FROM name_surfaces
            WHERE logical_name_id > $1
            ORDER BY logical_name_id ASC
            LIMIT $2
            "#,
        )
        .bind(&last_logical_name_id)
        .bind(batch_size)
        .fetch_all(pool)
        .await
        .context("failed to load name surfaces for retained label preimage backfill")?;

        if rows.is_empty() {
            break;
        }

        let mut preimages = Vec::new();
        for row in &rows {
            last_logical_name_id = row.try_get("logical_name_id")?;
            summary.scanned_row_count += 1;
            let surface = decode_label_preimage_name_surface(row)?;
            preimages.extend(label_preimages_from_name_surface(&surface)?);
        }

        let changed = upsert_label_preimages_without_invalidations(pool, &preimages).await?;
        summary.retained_row_count += changed.len() as u64;
    }

    Ok(())
}

async fn backfill_label_preimages_from_existing_normalized_events(
    pool: &PgPool,
    batch_size: i64,
    summary: &mut LabelPreimageImportSummary,
) -> Result<()> {
    let mut last_normalized_event_id = 0_i64;

    loop {
        let rows = sqlx::query(
            r#"
            SELECT
                normalized_event_id,
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state::TEXT AS canonicality_state,
                before_state,
                after_state
            FROM normalized_events
            WHERE normalized_event_id > $1
              AND event_kind = 'PreimageObserved'
            ORDER BY normalized_event_id ASC
            LIMIT $2
            "#,
        )
        .bind(last_normalized_event_id)
        .bind(batch_size)
        .fetch_all(pool)
        .await
        .context("failed to load normalized events for retained label preimage backfill")?;

        if rows.is_empty() {
            break;
        }

        let mut preimages = Vec::new();
        for row in &rows {
            last_normalized_event_id = row.try_get("normalized_event_id")?;
            summary.scanned_row_count += 1;
            let event = decode_label_preimage_normalized_event(row)?;
            if let Some(event_preimages) = label_preimages_from_normalized_event(&event) {
                preimages.extend(event_preimages?);
            }
        }

        let changed = upsert_label_preimages_without_invalidations(pool, &preimages).await?;
        summary.retained_row_count += changed.len() as u64;
    }

    Ok(())
}

fn decode_label_preimage_name_surface(row: &PgRow) -> Result<NameSurface> {
    Ok(NameSurface {
        logical_name_id: row.try_get("logical_name_id")?,
        namespace: row.try_get("namespace")?,
        input_name: row.try_get("input_name")?,
        canonical_display_name: row.try_get("canonical_display_name")?,
        normalized_name: row.try_get("normalized_name")?,
        dns_encoded_name: row.try_get("dns_encoded_name")?,
        namehash: row.try_get("namehash")?,
        labelhashes: row.try_get("labelhashes")?,
        normalizer_version: row.try_get("normalizer_version")?,
        normalization_warnings: row.try_get("normalization_warnings")?,
        normalization_errors: row.try_get("normalization_errors")?,
        chain_id: row.try_get("chain_id")?,
        block_hash: row.try_get("block_hash")?,
        block_number: row.try_get("block_number")?,
        provenance: row.try_get("provenance")?,
        canonicality_state: row.try_get("canonicality_state")?,
    })
}

fn decode_label_preimage_normalized_event(row: &PgRow) -> Result<NormalizedEvent> {
    Ok(NormalizedEvent {
        event_identity: row.try_get("event_identity")?,
        namespace: row.try_get("namespace")?,
        logical_name_id: row.try_get("logical_name_id")?,
        resource_id: row.try_get("resource_id")?,
        event_kind: row.try_get("event_kind")?,
        source_family: row.try_get("source_family")?,
        manifest_version: row.try_get("manifest_version")?,
        source_manifest_id: row.try_get("source_manifest_id")?,
        chain_id: row.try_get("chain_id")?,
        block_number: row.try_get("block_number")?,
        block_hash: row.try_get("block_hash")?,
        transaction_hash: row.try_get("transaction_hash")?,
        log_index: row.try_get("log_index")?,
        raw_fact_ref: row.try_get("raw_fact_ref")?,
        derivation_kind: row.try_get("derivation_kind")?,
        canonicality_state: row.try_get("canonicality_state")?,
        before_state: row.try_get("before_state")?,
        after_state: row.try_get("after_state")?,
    })
}
