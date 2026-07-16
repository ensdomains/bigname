use anyhow::{Context, Result, ensure};
use bigname_storage::{
    ChainCheckpoint, ChainCheckpointUpdate, CheckpointBlockRef, advance_chain_checkpoints,
    advance_chain_checkpoints_rejecting_non_orphaned_lineage_forks_in_transaction,
};

use crate::reconciliation::guard_release::prioritize_operation_error;

/// Briefly locks the chain's admission-epoch row while a previously observed
/// epoch is revalidated. Stored-lineage promotion verifies coverage and does
/// provider/storage preparation without this lock, then requires the same
/// epoch immediately before checkpoint persistence.
pub(crate) struct StoredLineageAdmissionEpochFence {
    transaction: sqlx::Transaction<'static, sqlx::Postgres>,
}

impl StoredLineageAdmissionEpochFence {
    pub(super) async fn read_epoch(pool: &sqlx::PgPool, chain: &str) -> Result<i64> {
        sqlx::query(
            r#"
            INSERT INTO discovery_admission_epochs (chain_id, epoch)
            VALUES ($1, 0)
            ON CONFLICT (chain_id) DO NOTHING
            "#,
        )
        .bind(chain)
        .execute(pool)
        .await
        .with_context(|| format!("failed to ensure the admission-epoch row for chain {chain}"))?;
        sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to read the admission epoch for chain {chain}"))
    }

    pub(super) async fn acquire_for_epoch(
        pool: &sqlx::PgPool,
        chain: &str,
        expected_epoch: i64,
    ) -> Result<Self> {
        let mut transaction = pool
            .begin()
            .await
            .context("failed to begin stored-lineage admission-epoch fence")?;
        sqlx::query(
            r#"
            INSERT INTO discovery_admission_epochs (chain_id, epoch)
            VALUES ($1, 0)
            ON CONFLICT (chain_id) DO NOTHING
            "#,
        )
        .bind(chain)
        .execute(&mut *transaction)
        .await
        .with_context(|| {
            format!("failed to ensure the admission-epoch fence row for chain {chain}")
        })?;
        let epoch = sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1 FOR SHARE",
        )
        .bind(chain)
        .fetch_one(&mut *transaction)
        .await
        .with_context(|| format!("failed to lock the admission epoch for chain {chain}"))?;
        ensure!(
            epoch == expected_epoch,
            "discovery admission epoch for chain {chain} changed from {expected_epoch} to {epoch}; refusing stored-lineage checkpoint promotion until raw-fact coverage is reverified"
        );
        Ok(Self { transaction })
    }

    pub(crate) async fn release(self) -> Result<()> {
        self.transaction
            .commit()
            .await
            .context("failed to release stored-lineage admission-epoch fence")
    }

    async fn advance_checkpoint(
        mut self,
        update: &ChainCheckpointUpdate,
    ) -> Result<ChainCheckpoint> {
        let checkpoint =
            advance_chain_checkpoints_rejecting_non_orphaned_lineage_forks_in_transaction(
                &mut self.transaction,
                update,
            )
            .await;
        let release = if checkpoint.is_ok() {
            self.transaction
                .commit()
                .await
                .context("failed to commit checkpoint advancement under admission-epoch fence")
        } else {
            self.transaction
                .rollback()
                .await
                .context("failed to roll back checkpoint advancement under admission-epoch fence")
        };
        prioritize_operation_error(checkpoint, release)
    }
}

impl super::coverage::ChainCoverageFrontiers {
    pub(crate) async fn reacquire_promotion_fence(
        pool: &sqlx::PgPool,
        chain: &str,
        expected_epoch: Option<i64>,
    ) -> Result<Option<StoredLineageAdmissionEpochFence>> {
        let Some(expected_epoch) = expected_epoch else {
            return Ok(None);
        };
        StoredLineageAdmissionEpochFence::acquire_for_epoch(pool, chain, expected_epoch)
            .await
            .map(Some)
    }

    pub(crate) async fn advance_checkpoint_with_promotion_epoch(
        pool: &sqlx::PgPool,
        chain: &str,
        expected_epoch: Option<i64>,
        canonical: Option<CheckpointBlockRef>,
        safe: Option<CheckpointBlockRef>,
        finalized: Option<CheckpointBlockRef>,
    ) -> Result<ChainCheckpoint> {
        let update = ChainCheckpointUpdate {
            chain_id: chain.to_owned(),
            canonical,
            safe,
            finalized,
        };
        if let Some(fence) = Self::reacquire_promotion_fence(pool, chain, expected_epoch).await? {
            fence.advance_checkpoint(&update).await
        } else {
            advance_chain_checkpoints(pool, &update).await
        }
    }
}

#[cfg(test)]
pub(crate) use test_hook::AdmissionEpochFenceTestHook;
#[cfg(test)]
pub(crate) use test_hook::install as install_admission_epoch_verification_test_hook;
#[cfg(test)]
pub(super) use test_hook::pause as pause_after_admission_epoch_verification_for_tests;

#[cfg(test)]
mod test_hook {
    use std::sync::Arc;

    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Notify;

    pub(crate) struct AdmissionEpochFenceTestHook {
        state: AdmissionEpochFenceTestHookState,
        _registration: ScopedTestHookGuard<HookKey, AdmissionEpochFenceTestHookState>,
    }

    #[derive(Clone)]
    struct AdmissionEpochFenceTestHookState {
        acquired: Arc<Notify>,
        resume: Arc<Notify>,
    }

    impl AdmissionEpochFenceTestHook {
        pub(crate) async fn wait_until_verified(&self) {
            self.state.acquired.notified().await;
        }

        pub(crate) fn resume(&self) {
            self.state.resume.notify_one();
        }
    }

    impl Drop for AdmissionEpochFenceTestHook {
        fn drop(&mut self) {
            self.state.resume.notify_one();
        }
    }

    type HookKey = (String, String);

    static HOOKS: ScopedTestHookRegistry<HookKey, AdmissionEpochFenceTestHookState> =
        ScopedTestHookRegistry::new();

    pub(crate) async fn install(pool: &PgPool, chain: &str) -> AdmissionEpochFenceTestHook {
        let database = current_test_database(pool)
            .await
            .expect("admission epoch fence test hook must identify its database");
        let state = AdmissionEpochFenceTestHookState {
            acquired: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
        };
        let registration = HOOKS.install((database, chain.to_owned()), state.clone());
        AdmissionEpochFenceTestHook {
            state,
            _registration: registration,
        }
    }

    pub(crate) async fn pause(pool: &PgPool, chain: &str) {
        let database = current_test_database(pool)
            .await
            .expect("admission epoch fence test hook must identify its database");
        let hook = HOOKS.take(&(database, chain.to_owned()));
        if let Some(hook) = hook {
            hook.acquired.notify_one();
            hook.resume.notified().await;
        }
    }
}
