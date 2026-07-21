use std::sync::{Arc, Mutex};

use anyhow::Result;
use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};

type HookKey = (String, String, String);

static FAILURES: ScopedTestHookRegistry<HookKey, ()> = ScopedTestHookRegistry::new();
type PageRanges = Arc<Mutex<Vec<(i64, i64)>>>;
static PAGE_OBSERVERS: ScopedTestHookRegistry<HookKey, PageRanges> = ScopedTestHookRegistry::new();

pub(crate) struct AutomaticStatelessPageTestHook {
    page_ranges: PageRanges,
    _registration: ScopedTestHookGuard<HookKey, PageRanges>,
}

impl AutomaticStatelessPageTestHook {
    pub(crate) fn page_ranges(&self) -> Vec<(i64, i64)> {
        self.page_ranges
            .lock()
            .expect("automatic stateless page observer mutex poisoned")
            .clone()
    }
}

pub(crate) async fn install_after_stateless_failure(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<ScopedTestHookGuard<HookKey, ()>> {
    let database = current_test_database(pool).await?;
    Ok(FAILURES.install(
        (database, deployment_profile.to_owned(), chain.to_owned()),
        (),
    ))
}

pub(crate) async fn install_stateless_page_observer(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<AutomaticStatelessPageTestHook> {
    let database = current_test_database(pool).await?;
    let key = (database, deployment_profile.to_owned(), chain.to_owned());
    let page_ranges = Arc::new(Mutex::new(Vec::new()));
    let registration = PAGE_OBSERVERS.install(key, Arc::clone(&page_ranges));
    Ok(AutomaticStatelessPageTestHook {
        page_ranges,
        _registration: registration,
    })
}

pub(super) async fn record_stateless_page(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    let database = current_test_database(pool).await?;
    if let Some(page_ranges) =
        PAGE_OBSERVERS.get_cloned(&(database, deployment_profile.to_owned(), chain.to_owned()))
    {
        page_ranges
            .lock()
            .expect("automatic stateless page observer mutex poisoned")
            .push((from_block, to_block));
    }
    Ok(())
}

pub(super) async fn fail_after_stateless(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<()> {
    let database = current_test_database(pool).await?;
    if FAILURES
        .take(&(database, deployment_profile.to_owned(), chain.to_owned()))
        .is_some()
    {
        anyhow::bail!("injected failure after automatic stateless replay phase");
    }
    Ok(())
}
