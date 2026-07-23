//! Durable full-projection staging primitives used by worker replay.

use anyhow::{Context, Result, ensure};
use sqlx::{Postgres, Transaction};

pub use crate::address_names::{
    insert_address_names_current_full_rebuild_rows_in_transaction,
    publish_address_names_current_full_rebuild_in_transaction,
};
pub use crate::children::stream_canonical_declared_child_sources_after;
pub use crate::name_current::{
    analyze_name_current_replacement_table, publish_name_current_replacement_table_in_transaction,
    stage_name_current_replacement_rows_in_transaction,
};

pub const NAME_CURRENT_STAGING_COLUMNS: &[&str] = &[
    "logical_name_id",
    "namespace",
    "canonical_display_name",
    "normalized_name",
    "namehash",
    "surface_binding_id",
    "resource_id",
    "token_lineage_id",
    "binding_kind",
    "declared_summary",
    "provenance",
    "coverage",
    "chain_positions",
    "canonicality_summary",
    "manifest_version",
    "last_recomputed_at",
];

pub const ADDRESS_NAMES_CURRENT_STAGING_COLUMNS: &[&str] = &[
    "address",
    "logical_name_id",
    "relation",
    "namespace",
    "canonical_display_name",
    "normalized_name",
    "namehash",
    "surface_binding_id",
    "resource_id",
    "token_lineage_id",
    "binding_kind",
    "provenance",
    "coverage",
    "chain_positions",
    "canonicality_summary",
    "manifest_version",
    "last_recomputed_at",
];

/// Lock and load the revision for direct source changes that require a full projection replay.
pub async fn load_current_projection_full_replay_input_revision_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT revision
        FROM current_projection_full_replay_input_revision
        WHERE singleton
        FOR SHARE
        "#,
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to lock current-projection full-replay input revision")
}

/// Fail closed unless publication still targets the direct-input revision used for staging.
pub async fn ensure_current_projection_full_replay_input_revision_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    expected_revision: i64,
) -> Result<()> {
    let observed_revision =
        load_current_projection_full_replay_input_revision_in_transaction(transaction).await?;
    ensure!(
        observed_revision == expected_revision,
        "current-projection full-replay input revision changed from {expected_revision} to {observed_revision}; durable staging must be discarded"
    );
    Ok(())
}

/// Invalidate every reusable full-projection stage after a direct, non-event source repair.
pub async fn advance_current_projection_full_replay_input_revision_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<i64> {
    let revision = sqlx::query_scalar(
        r#"
        UPDATE current_projection_full_replay_input_revision
        SET revision = revision + 1, updated_at = now()
        WHERE singleton
        RETURNING revision
        "#,
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to advance current-projection full-replay input revision")?;
    sqlx::query("DELETE FROM current_projection_replay_status")
        .execute(&mut **transaction)
        .await
        .context("failed to invalidate current-projection replay markers")?;
    sqlx::query("DELETE FROM current_projection_replay_attempt")
        .execute(&mut **transaction)
        .await
        .context("failed to invalidate the current-projection replay attempt")?;
    Ok(revision)
}
