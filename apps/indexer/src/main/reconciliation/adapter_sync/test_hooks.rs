use anyhow::Result;
use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};

static POST_DISCOVERY_MUTATION_FAILURES: ScopedTestHookRegistry<String, ()> =
    ScopedTestHookRegistry::new();

pub(crate) async fn install_post_discovery_mutation_failure(
    pool: &sqlx::PgPool,
) -> Result<ScopedTestHookGuard<String, ()>> {
    let database = current_test_database(pool).await?;
    Ok(POST_DISCOVERY_MUTATION_FAILURES.install(database, ()))
}

pub(super) async fn fail_after_discovery_mutation(pool: &sqlx::PgPool) -> Result<()> {
    let database = current_test_database(pool).await?;
    if POST_DISCOVERY_MUTATION_FAILURES.take(&database).is_some() {
        anyhow::bail!("injected failure after committed discovery mutation");
    }
    Ok(())
}
