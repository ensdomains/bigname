use std::future::Future;

use anyhow::{Context, Result};
use sqlx::{Connection, Either, PgConnection, PgPool, postgres::PgAdvisoryLock};

pub(super) async fn with_full_closure_replay_lock<T, Operation, OperationFuture>(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    operation: Operation,
) -> Result<T>
where
    Operation: FnOnce() -> OperationFuture,
    OperationFuture: Future<Output = Result<T>>,
{
    // Use a dedicated connection rather than occupying a pool slot. Runtime
    // processes already retain one pool connection for the Base correction
    // writer guard, and full-closure adapters need the pool while this
    // cross-process ownership fence is held.
    let mut connection = PgConnection::connect_with(pool.connect_options().as_ref())
        .await
        .context("failed to connect the full-closure replay ownership fence")?;
    let lock_identity = format!("bigname:indexer:full-closure-replay:{deployment_profile}:{chain}");
    let lock = PgAdvisoryLock::new(lock_identity);
    let guard = loop {
        // A backend waiting inside pg_advisory_lock can retain a snapshot for
        // the entire competing replay. Polling leaves no long-lived statement
        // behind to hold back CREATE INDEX CONCURRENTLY or vacuum horizons.
        match lock.try_acquire(connection).await.with_context(|| {
            format!("failed to try full-closure replay ownership for {deployment_profile}/{chain}")
        })? {
            Either::Left(guard) => break guard,
            Either::Right(mut unlocked) => {
                sqlx::query("SELECT pg_sleep(0.05)")
                    .execute(&mut unlocked)
                    .await
                    .with_context(|| {
                        format!(
                            "failed while polling full-closure replay ownership for {deployment_profile}/{chain}"
                        )
                    })?;
                connection = unlocked;
            }
        }
    };

    let operation_result = operation().await;
    #[cfg(test)]
    test_hook::pause_before_release(pool, deployment_profile, chain).await;
    let release_result = guard
        .release_now()
        .await
        .with_context(|| {
            format!(
                "failed to release full-closure replay ownership for {deployment_profile}/{chain}"
            )
        })
        .map(|_| ());
    match (operation_result, release_result) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

#[cfg(test)]
pub(crate) use test_hook::install as install_ownership_release_test_hook;

#[cfg(test)]
mod test_hook {
    use std::{
        collections::BTreeMap,
        sync::{Arc, LazyLock, Mutex},
    };

    use sqlx::PgPool;
    use tokio::sync::Notify;

    #[derive(Clone)]
    pub(crate) struct FullClosureOwnershipReleaseTestHook {
        before_release: Arc<Notify>,
        resume: Arc<Notify>,
    }

    impl FullClosureOwnershipReleaseTestHook {
        pub(crate) async fn wait_until_before_release(&self) {
            self.before_release.notified().await;
        }

        pub(crate) fn resume(&self) {
            self.resume.notify_one();
        }
    }

    static HOOKS: LazyLock<
        Mutex<BTreeMap<(String, String, String), FullClosureOwnershipReleaseTestHook>>,
    > = LazyLock::new(|| Mutex::new(BTreeMap::new()));

    pub(crate) async fn install(
        pool: &PgPool,
        deployment_profile: &str,
        chain: &str,
    ) -> FullClosureOwnershipReleaseTestHook {
        let database = current_database(pool).await;
        let hook = FullClosureOwnershipReleaseTestHook {
            before_release: Arc::new(Notify::new()),
            resume: Arc::new(Notify::new()),
        };
        HOOKS
            .lock()
            .expect("full-closure ownership release hook lock must not be poisoned")
            .insert(
                (database, deployment_profile.to_owned(), chain.to_owned()),
                hook.clone(),
            );
        hook
    }

    pub(super) async fn pause_before_release(pool: &PgPool, deployment_profile: &str, chain: &str) {
        let database = current_database(pool).await;
        let hook = HOOKS
            .lock()
            .expect("full-closure ownership release hook lock must not be poisoned")
            .remove(&(database, deployment_profile.to_owned(), chain.to_owned()));
        if let Some(hook) = hook {
            hook.before_release.notify_one();
            hook.resume.notified().await;
        }
    }

    async fn current_database(pool: &PgPool) -> String {
        sqlx::query_scalar("SELECT current_database()")
            .fetch_one(pool)
            .await
            .expect("full-closure ownership test hook must identify its database")
    }
}
