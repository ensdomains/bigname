use super::*;
use crate::v2::collection_snapshot::CollectionSnapshot;

const MAX_INLINE_GRANT_ROWS: u64 = 1_000;

/// The grant read is not pinned to `snapshot`, so an overflow is only reported for a publication
/// that is still the captured one. The under-budget path relies on the caller's final fence.
pub(super) async fn load_rows(
    state: &AppState,
    snapshot: &CollectionSnapshot,
    ids: &[sqlx::types::Uuid],
    namespace: Option<&str>,
    returned_resources: impl Iterator<Item = sqlx::types::Uuid>,
) -> V2Result<Vec<EffectivePermissionRow>> {
    #[cfg(test)]
    grant_read_test_hooks::run(&state.pool).await?;
    let rows = bigname_storage::load_bounded_effective_permissions_by_resource_ids(
        &state.pool,
        ids,
        namespace,
        MAX_INLINE_GRANT_ROWS,
    )
    .await
    .map_err(|_| V2Error::internal_error("failed to load address-name role summaries"))?;
    let mut multiplicity = BTreeMap::<_, usize>::new();
    for resource_id in returned_resources {
        *multiplicity.entry(resource_id).or_default() += 1;
    }
    // Surface dedupe can repeat one resource's grants on several returned names. Count the
    // serialized expansion, not only distinct storage rows or permission subjects.
    let expanded_rows: usize = rows.iter().map(|row| multiplicity[&row.resource_id]).sum();
    if expanded_rows > MAX_INLINE_GRANT_ROWS as usize {
        snapshot.finish(state).await?;
        return Err(V2Error::unsupported(
            "inline role_summary exceeds 1000 total grant rows; omit include and paginate /v1/permissions using the returned permission handle as registration_id; preserve only an explicitly requested namespace and do not add name or address filters",
        ));
    }
    Ok(rows)
}

/// Pauses a request after membership selection and before the grant read.
#[cfg(test)]
pub(crate) mod grant_read_test_hooks {
    use std::sync::Arc;

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    use crate::v2::{V2Error, V2Result};

    #[derive(Clone)]
    pub(crate) struct GrantReadHook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct GrantReadControl {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl GrantReadControl {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<String, GrantReadHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(ScopedTestHookGuard<String, GrantReadHook>, GrantReadControl)> {
        let database = current_test_database(pool).await?;
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            GrantReadHook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, GrantReadControl { reached, resume }))
    }

    pub(super) async fn run(pool: &PgPool) -> V2Result<()> {
        let database = current_test_database(pool)
            .await
            .map_err(|_| V2Error::internal_error("failed to run role-summary read test hook"))?;
        if let Some(hook) = HOOKS.take(&database) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}
