#[cfg(test)]
pub(crate) mod relation_page_test_hooks {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    #[derive(Clone)]
    pub(crate) struct RelationPageHook {
        calls: Arc<AtomicUsize>,
        paused: Arc<AtomicBool>,
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct RelationPageControl {
        calls: Arc<AtomicUsize>,
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl RelationPageControl {
        pub(crate) fn page_loader_call_count(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<String, RelationPageHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, RelationPageHook>,
        RelationPageControl,
    )> {
        let database = current_test_database(pool).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            RelationPageHook {
                calls: Arc::clone(&calls),
                paused: Arc::new(AtomicBool::new(false)),
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((
            guard,
            RelationPageControl {
                calls,
                reached,
                resume,
            },
        ))
    }

    pub(in crate::v2::support::reverse_identity) async fn record_page_load(
        pool: &PgPool,
    ) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(hook) = HOOKS.get_cloned(&database) {
            hook.calls.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(crate) async fn pause_before_additional_scan(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(hook) = HOOKS.get_cloned(&database)
            && hook
                .paused
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;

    static COUNT_CALLS: ScopedTestHookRegistry<String, Arc<AtomicUsize>> =
        ScopedTestHookRegistry::new();

    pub(crate) struct CountCallControl(Arc<AtomicUsize>);

    impl CountCallControl {
        pub(crate) fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, Arc<AtomicUsize>>,
        CountCallControl,
    )> {
        let database = current_test_database(pool).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let guard = COUNT_CALLS.install(database, Arc::clone(&calls));
        Ok((guard, CountCallControl(calls)))
    }

    pub(in crate::v2::support::reverse_identity) async fn record(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(calls) = COUNT_CALLS.get_cloned(&database) {
            calls.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}
