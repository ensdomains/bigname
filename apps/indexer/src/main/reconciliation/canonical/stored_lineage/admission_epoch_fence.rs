use anyhow::{Context, Result};

/// Locks the chain's admission-epoch row through stored-lineage coverage and
/// checkpoint advancement. Manifest sync and discovery admission bump this
/// row in the same transaction as watched-surface changes, so their commit
/// cannot pass this fence between verification and checkpoint persistence.
pub(crate) struct StoredLineageAdmissionEpochFence {
    transaction: sqlx::Transaction<'static, sqlx::Postgres>,
    epoch: i64,
}

impl StoredLineageAdmissionEpochFence {
    pub(super) async fn acquire(pool: &sqlx::PgPool, chain: &str) -> Result<Self> {
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
        Ok(Self { transaction, epoch })
    }

    pub(super) const fn epoch(&self) -> i64 {
        self.epoch
    }

    pub(crate) async fn release(self) -> Result<()> {
        self.transaction
            .commit()
            .await
            .context("failed to release stored-lineage admission-epoch fence")
    }
}

impl super::coverage::ChainCoverageFrontiers {
    pub(crate) async fn release_promotion_fence(
        fence: Option<StoredLineageAdmissionEpochFence>,
    ) -> Result<()> {
        if let Some(fence) = fence {
            fence.release().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) use test_hook::AdmissionEpochFenceTestHook;
#[cfg(test)]
pub(crate) use test_hook::install as install_admission_epoch_fence_test_hook;
#[cfg(test)]
pub(super) use test_hook::pause as pause_after_admission_epoch_fence_for_tests;

#[cfg(test)]
mod test_hook {
    use std::{
        collections::BTreeMap,
        sync::{Arc, LazyLock, Mutex},
    };

    use tokio::sync::Notify;

    #[derive(Clone)]
    pub(crate) struct AdmissionEpochFenceTestHook {
        acquired: Arc<Notify>,
        resume: Arc<Notify>,
    }

    impl AdmissionEpochFenceTestHook {
        pub(crate) async fn wait_until_acquired(&self) {
            self.acquired.notified().await;
        }

        pub(crate) fn resume(&self) {
            self.resume.notify_one();
        }
    }

    static HOOKS: LazyLock<Mutex<BTreeMap<String, AdmissionEpochFenceTestHook>>> =
        LazyLock::new(|| Mutex::new(BTreeMap::new()));

    pub(crate) fn install(chain: &str) -> AdmissionEpochFenceTestHook {
        let hook = AdmissionEpochFenceTestHook {
            acquired: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
        };
        HOOKS
            .lock()
            .expect("admission epoch fence test hook lock must not be poisoned")
            .insert(chain.to_owned(), hook.clone());
        hook
    }

    pub(crate) async fn pause(chain: &str) {
        let hook = HOOKS
            .lock()
            .expect("admission epoch fence test hook lock must not be poisoned")
            .remove(chain);
        if let Some(hook) = hook {
            hook.acquired.notify_one();
            hook.resume.notified().await;
        }
    }
}
