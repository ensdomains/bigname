use anyhow::{Context, Result};
use sqlx::{Connection, PgConnection, Postgres, Transaction};

use super::lock::{
    invalidate_all_verified_primary_name_outcomes_in_transaction,
    lock_primary_names_current_replacement_in_transaction,
};
use crate::address_names::rebuild_address_names_current_identity_sidecars_in_transaction;

/// Per-row triggers disabled during fenced bulk publication. The identity-feed
/// triggers take one transaction-scoped advisory lock per address, while the
/// compatibility triggers otherwise run once per replaced row. A full-table
/// delete or insert can therefore exhaust shared memory or spend millions of
/// unnecessary trigger calls (observed 2026-07-09: ~3.8M rows vs a ~220k-entry
/// lock table).
const PRIMARY_NAMES_CURRENT_BULK_DISABLED_TRIGGERS: &[&str] = &[
    "primary_names_current_identity_feed_after_claim_update",
    "primary_names_current_identity_feed_after_insert_delete",
    // The replacement holds the global advisory fence and marks the
    // transaction for the compatibility functions before disabling these.
    // Avoid invoking row-level compatibility triggers millions of times.
    "primary_names_current_tuple_fence_before_write",
    "primary_names_current_cache_invalidation_after_write",
];

const PRIMARY_NAMES_CURRENT_COLUMNS: &str = "address, coin_type, namespace, claim_status, raw_claim_name, normalized_claim_name, claim_name_is_normalized, claim_provenance";

/// Replace primary_names_current from a staged rebuild table with the per-row
/// sidecar triggers disabled, then rebuild the identity sidecars in bulk —
/// the same discipline the address_names_current full rebuild uses.
pub async fn publish_primary_names_current_full_rebuild(
    conn: &mut PgConnection,
    stage_table: &str,
) -> Result<(u64, u64)> {
    let mut transaction = conn
        .begin()
        .await
        .context("failed to open primary_names_current replacement transaction")?;

    lock_primary_names_current_replacement_in_transaction(&mut transaction).await?;
    // This compatibility wrapper has no caller-supplied staged diff. Evict the
    // whole verified-primary cache before publication; the worker's normal
    // full rebuild uses the transaction-owned entry point and its exact diff.
    invalidate_all_verified_primary_name_outcomes_in_transaction(&mut transaction).await?;

    let published =
        publish_primary_names_current_full_rebuild_in_transaction(&mut transaction, stage_table)
            .await?;

    transaction
        .commit()
        .await
        .context("failed to commit primary_names_current replacement")?;

    Ok(published)
}

/// Replace `primary_names_current` inside a caller-owned transaction.
///
/// The replacement lock is reentrant, so callers may acquire it before related
/// cache invalidation and keep invalidation plus publication under one fence.
pub async fn publish_primary_names_current_full_rebuild_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    stage_table: &str,
) -> Result<(u64, u64)> {
    lock_primary_names_current_replacement_in_transaction(transaction).await?;

    set_primary_names_current_bulk_triggers(transaction, false).await?;

    let deleted = sqlx::query("DELETE FROM primary_names_current")
        .execute(&mut **transaction)
        .await
        .context("failed to delete old primary_names_current rows")?
        .rows_affected();
    let inserted = sqlx::query(&format!(
        "INSERT INTO primary_names_current ({PRIMARY_NAMES_CURRENT_COLUMNS}) SELECT {PRIMARY_NAMES_CURRENT_COLUMNS} FROM {stage_table}"
    ))
    .execute(&mut **transaction)
    .await
    .context("failed to publish staged primary_names_current rows")?
    .rows_affected();

    set_primary_names_current_bulk_triggers(transaction, true).await?;
    rebuild_address_names_current_identity_sidecars_in_transaction(transaction).await?;

    Ok((deleted, inserted))
}

async fn set_primary_names_current_bulk_triggers(
    transaction: &mut Transaction<'_, Postgres>,
    enabled: bool,
) -> Result<()> {
    let action = if enabled { "ENABLE" } else { "DISABLE" };
    for trigger in PRIMARY_NAMES_CURRENT_BULK_DISABLED_TRIGGERS {
        sqlx::query(&format!(
            "ALTER TABLE primary_names_current {action} TRIGGER {trigger}"
        ))
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!(
                "failed to {} primary_names_current sidecar trigger {}",
                action.to_ascii_lowercase(),
                trigger
            )
        })?;
    }
    Ok(())
}
