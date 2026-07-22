use std::sync::Arc;

use anyhow::Result;
use bigname_test_support::{ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database};
use sqlx::PgPool;

pub(crate) type TargetedRebuildAfterInvalidationHook =
    Arc<dyn Fn(&str, &str, &str) + Send + Sync + 'static>;
pub(crate) type FullRebuildAfterInvalidationHook = Arc<dyn Fn() + Send + Sync + 'static>;

static TARGETED_REBUILD_AFTER_INVALIDATION_HOOKS: ScopedTestHookRegistry<
    String,
    TargetedRebuildAfterInvalidationHook,
> = ScopedTestHookRegistry::new();
static FULL_REBUILD_AFTER_INVALIDATION_HOOKS: ScopedTestHookRegistry<
    String,
    FullRebuildAfterInvalidationHook,
> = ScopedTestHookRegistry::new();

pub(crate) async fn install_targeted_rebuild_after_invalidation_hook(
    pool: &PgPool,
    hook: TargetedRebuildAfterInvalidationHook,
) -> Result<ScopedTestHookGuard<String, TargetedRebuildAfterInvalidationHook>> {
    let database = current_database(pool).await?;
    Ok(TARGETED_REBUILD_AFTER_INVALIDATION_HOOKS.install(database, hook))
}

pub(crate) async fn install_full_rebuild_after_invalidation_hook(
    pool: &PgPool,
    hook: FullRebuildAfterInvalidationHook,
) -> Result<ScopedTestHookGuard<String, FullRebuildAfterInvalidationHook>> {
    let database = current_database(pool).await?;
    Ok(FULL_REBUILD_AFTER_INVALIDATION_HOOKS.install(database, hook))
}

pub(super) fn run_targeted_rebuild_after_invalidation_hook(
    database: &str,
    address: &str,
    namespace: &str,
    coin_type: &str,
) {
    let hook = TARGETED_REBUILD_AFTER_INVALIDATION_HOOKS.get_cloned(&database.to_owned());
    if let Some(hook) = hook {
        hook(address, namespace, coin_type);
    }
}

pub(super) fn run_full_rebuild_after_invalidation_hook(database: &str) {
    let hook = FULL_REBUILD_AFTER_INVALIDATION_HOOKS.get_cloned(&database.to_owned());
    if let Some(hook) = hook {
        hook();
    }
}

pub(super) async fn current_database(pool: &PgPool) -> Result<String> {
    current_test_database(pool).await
}
