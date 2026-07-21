use anyhow::{Context, Result};
use sqlx::{Connection, PgConnection, Postgres, Transaction};

use crate::address_names::rebuild_address_names_current_identity_sidecars_in_transaction;

/// Per-row sidecar triggers on primary_names_current. Each fires the identity
/// feed recompute, which takes one transaction-scoped advisory lock per
/// address — a full-table delete or insert therefore consumes one lock-table
/// entry per distinct address and exhausts shared memory at rebuild scale
/// (observed 2026-07-09: ~3.8M-row replacement vs a ~220k-entry lock table).
const PRIMARY_NAMES_CURRENT_SIDECAR_TRIGGERS: &[&str] = &[
    "primary_names_current_identity_feed_after_claim_update",
    "primary_names_current_identity_feed_after_insert_delete",
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

    set_primary_names_current_sidecar_triggers(&mut transaction, false).await?;

    let deleted = sqlx::query("DELETE FROM primary_names_current")
        .execute(&mut *transaction)
        .await
        .context("failed to delete old primary_names_current rows")?
        .rows_affected();
    let inserted = sqlx::query(&format!(
        "INSERT INTO primary_names_current ({PRIMARY_NAMES_CURRENT_COLUMNS}) SELECT {PRIMARY_NAMES_CURRENT_COLUMNS} FROM {stage_table}"
    ))
    .execute(&mut *transaction)
    .await
    .context("failed to publish staged primary_names_current rows")?
    .rows_affected();

    set_primary_names_current_sidecar_triggers(&mut transaction, true).await?;
    rebuild_address_names_current_identity_sidecars_in_transaction(&mut transaction).await?;

    transaction
        .commit()
        .await
        .context("failed to commit primary_names_current replacement")?;

    Ok((deleted, inserted))
}

async fn set_primary_names_current_sidecar_triggers(
    transaction: &mut Transaction<'_, Postgres>,
    enabled: bool,
) -> Result<()> {
    let action = if enabled { "ENABLE" } else { "DISABLE" };
    for trigger in PRIMARY_NAMES_CURRENT_SIDECAR_TRIGGERS {
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
