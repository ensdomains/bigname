//! Durable full-projection staging primitives used by worker replay.

use std::fmt;

use anyhow::{Context, Result, ensure};
use sqlx::{Postgres, Transaction};

const FATAL_PROJECTION_REPLAY_VERSION_FENCE: &str = "fatal projection replay version fence";
const UNFENCED_REPLAY_ADMISSION: &str =
    "fatal projection replay version fence: unfenced writer crossed in-progress replay admission";
const UNSTAMPED_REPLAY_VERSION: &str =
    "fatal projection replay version fence: process replay version is unstamped";
const OUTDATED_REPLAY_VERSION_COMPARISON: &str = " is older than persisted replay version ";
const MISSING_REPLAY_VERSION_FENCE_SINGLETON: &str = "fatal projection replay version fence: \
    singleton state is missing; refusing projection-owned write";

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

/// Fatal error returned when an outdated process reaches projection-owned write state.
#[derive(Debug)]
pub struct OutdatedProjectionReplayVersionError {
    process_replay_version: i32,
    persisted_replay_version: i32,
}

impl fmt::Display for OutdatedProjectionReplayVersionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{FATAL_PROJECTION_REPLAY_VERSION_FENCE}: process replay version {} is older than \
             persisted replay version {}; refusing projection writes from the outdated process",
            self.process_replay_version, self.persisted_replay_version
        )
    }
}

impl std::error::Error for OutdatedProjectionReplayVersionError {}

/// Whether an error chain says this process is too old to write projection-owned state.
pub fn is_outdated_projection_replay_version_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<OutdatedProjectionReplayVersionError>()
        .is_some()
        || error.chain().any(|cause| {
            let message = cause.to_string();
            message.contains(UNFENCED_REPLAY_ADMISSION)
                || message.contains(UNSTAMPED_REPLAY_VERSION)
                || (message.contains(FATAL_PROJECTION_REPLAY_VERSION_FENCE)
                    && message.contains(OUTDATED_REPLAY_VERSION_COMPARISON))
        })
}

/// Whether an error chain reports a replay-version fence failure that must stop the process.
///
/// The current-version admission-race error deliberately lacks the fatal prefix and remains
/// retryable. Missing singleton state and invalid stamps are fatal without being classified as an
/// outdated process.
pub fn is_fatal_projection_replay_version_fence_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<OutdatedProjectionReplayVersionError>()
        .is_some()
        || error.chain().any(|cause| {
            cause
                .to_string()
                .contains(FATAL_PROJECTION_REPLAY_VERSION_FENCE)
        })
}

/// Hold the shared replay-version fence for a projection write transaction.
///
/// See `docs/glossary.md#projection-replay-version-fence`.
///
/// New replay-state writers take the conflicting row lock before raising the durable version
/// floor. A projection writer that was admitted first therefore finishes before cutover, while a
/// writer arriving after cutover fails instead of publishing older semantics.
pub async fn lock_current_projection_replay_version_for_projection_write_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    stamp_current_projection_replay_version_in_transaction(transaction).await?;
    let floor = lock_projection_replay_version_floor(transaction, "FOR SHARE").await?;
    ensure_process_replay_version_is_current(transaction, floor).await
}

/// Hold the exclusive replay-version fence and admit this binary's replay-state write.
///
/// The first fence-aware replay owner activates enforcement; a newer owner also advances the
/// floor. Applying the migration alone therefore installs the writer triggers without cutting off
/// a transaction that began under a pre-fence binary.
pub async fn lock_current_projection_replay_version_for_replay_write_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    stamp_current_projection_replay_version_in_transaction(transaction).await?;
    let floor = lock_projection_replay_version_floor(transaction, "FOR UPDATE").await?;
    ensure_process_replay_version_is_current(transaction, floor).await?;
    let fence_active = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT projection_replay_version_fence_active
        FROM current_projection_full_replay_input_revision
        WHERE singleton
        "#,
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to inspect current-projection replay-version fence activation")?;
    if !fence_active || floor < crate::CURRENT_PROJECTION_REPLAY_VERSION {
        sqlx::query(
            r#"
            UPDATE current_projection_full_replay_input_revision
            SET
                projection_replay_version_floor = GREATEST(
                    projection_replay_version_floor,
                    $1
                ),
                projection_replay_version_fence_active = true
            WHERE singleton
            "#,
        )
        .bind(crate::CURRENT_PROJECTION_REPLAY_VERSION)
        .execute(&mut **transaction)
        .await
        .context("failed to advance the current-projection replay-version fence")?;
    }
    Ok(())
}

async fn stamp_current_projection_replay_version_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query("SELECT set_config($1, $2, true)")
        .bind(crate::PROJECTION_REPLAY_VERSION_SETTING)
        .bind(crate::CURRENT_PROJECTION_REPLAY_VERSION.to_string())
        .execute(&mut **transaction)
        .await
        .context("failed to stamp the process projection replay version")
        .map(|_| ())
}

async fn lock_projection_replay_version_floor(
    transaction: &mut Transaction<'_, Postgres>,
    lock_clause: &str,
) -> Result<i32> {
    let query = format!(
        r#"
        SELECT projection_replay_version_floor
        FROM current_projection_full_replay_input_revision
        WHERE singleton
        {lock_clause}
        "#
    );
    let floor = sqlx::query_scalar(&query)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to lock the current-projection replay-version fence")?;
    floor.ok_or_else(|| anyhow::Error::msg(MISSING_REPLAY_VERSION_FENCE_SINGLETON))
}

async fn ensure_process_replay_version_is_current(
    transaction: &mut Transaction<'_, Postgres>,
    floor: i32,
) -> Result<()> {
    let persisted_version = sqlx::query_scalar::<_, Option<i32>>(
        r#"
        SELECT MAX(replay_version)
        FROM (
            SELECT replay_version
            FROM current_projection_replay_status
            UNION ALL
            SELECT replay_version
            FROM current_projection_replay_attempt
            UNION ALL
            SELECT replay_version
            FROM current_projection_staging_checkpoints
        ) AS persisted_replay_versions
        "#,
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to inspect persisted current-projection replay versions")?
    .unwrap_or(floor)
    .max(floor);
    if crate::CURRENT_PROJECTION_REPLAY_VERSION < persisted_version {
        return Err(OutdatedProjectionReplayVersionError {
            process_replay_version: crate::CURRENT_PROJECTION_REPLAY_VERSION,
            persisted_replay_version: persisted_version,
        }
        .into());
    }
    Ok(())
}

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
    lock_current_projection_replay_version_for_replay_write_in_transaction(transaction).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outdated_replay_version_classifier_excludes_other_fence_failures() {
        for message in [
            "projection replay admission is in progress; retry protected write",
            "fatal projection replay version fence: singleton state is missing",
            "fatal projection replay version fence: process replay version stamp 'bad' is invalid",
        ] {
            let error = anyhow::anyhow!("{message}");
            assert!(
                !is_outdated_projection_replay_version_error(&error),
                "unexpected outdated-process classification for {message}"
            );
        }

        for message in [
            UNFENCED_REPLAY_ADMISSION,
            "fatal projection replay version fence: process replay version is unstamped and predates the fence",
            "fatal projection replay version fence: process replay version 9 is older than persisted replay version 10",
        ] {
            let error = anyhow::anyhow!("{message}");
            assert!(
                is_outdated_projection_replay_version_error(&error),
                "missing outdated-process classification for {message}"
            );
        }
    }

    #[test]
    fn fatal_replay_version_fence_classifier_excludes_only_retryable_admission_races() {
        let retryable =
            anyhow::anyhow!("projection replay admission is in progress; retry protected write");
        assert!(
            !is_fatal_projection_replay_version_fence_error(&retryable),
            "a current stamped admission race must remain retryable"
        );

        for message in [
            "fatal projection replay version fence: singleton state is missing",
            "fatal projection replay version fence: process replay version stamp 'bad' is invalid",
            UNFENCED_REPLAY_ADMISSION,
            "fatal projection replay version fence: process replay version is unstamped and predates the fence",
            "fatal projection replay version fence: process replay version 9 is older than persisted replay version 10",
        ] {
            let error = anyhow::anyhow!("{message}");
            assert!(
                is_fatal_projection_replay_version_fence_error(&error),
                "missing fatal-fence classification for {message}"
            );
        }
    }
}
