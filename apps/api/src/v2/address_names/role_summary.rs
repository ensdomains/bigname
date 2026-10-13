use super::*;
use crate::v2::collection_snapshot::CollectionSnapshot;

const MAX_INLINE_GRANT_ROWS: u64 = 1_000;

/// The grants of the page's resources, read on the page's snapshot.
pub(super) async fn load_rows(
    state: &AppState,
    snapshot: &mut CollectionSnapshot,
    ids: &[sqlx::types::Uuid],
    namespace: Option<&str>,
    returned_resources: impl Iterator<Item = sqlx::types::Uuid>,
) -> V2Result<Vec<EffectivePermissionRow>> {
    #[cfg(test)]
    let per_resource = grant_read_test_hooks::run(&state.pool).await?;
    let conn = snapshot.conn().await?;
    #[cfg(test)]
    let rows = if per_resource {
        bigname_storage::families::control::permissions::page::load_family_bounded_permissions_per_resource(
            conn,
            ids,
            namespace,
            MAX_INLINE_GRANT_ROWS,
        )
        .await
    } else {
        bigname_storage::load_bounded_effective_permissions_by_resource_ids(
            conn,
            ids,
            namespace,
            MAX_INLINE_GRANT_ROWS,
        )
        .await
    };
    #[cfg(not(test))]
    let rows = bigname_storage::load_bounded_effective_permissions_by_resource_ids(
        conn,
        ids,
        namespace,
        MAX_INLINE_GRANT_ROWS,
    )
    .await;
    let rows = rows.map_err(crate::v2::name_rows_error(
        crate::v2::SnapshotReadResource::Resource,
        |_| V2Error::internal_error("failed to load address-name role summaries"),
    ))?;
    let mut multiplicity = BTreeMap::<_, usize>::new();
    for resource_id in returned_resources {
        *multiplicity.entry(resource_id).or_default() += 1;
    }
    // Surface dedupe can repeat one resource's grants on several returned names. Count the
    // serialized expansion, not only distinct storage rows or permission subjects.
    let expanded_rows: usize = rows.iter().map(|row| multiplicity[&row.resource_id]).sum();
    if expanded_rows > MAX_INLINE_GRANT_ROWS as usize {
        let unsupported = V2Error::unsupported(
            "inline role_summary exceeds 1000 total grant rows; omit include and paginate /v1/permissions using the returned permission handle as registration_id; preserve only an explicitly requested namespace and do not add name or address filters",
        );
        return Err(snapshot.refuse(state, unsupported).await);
    }
    Ok(rows)
}

/// Pauses a request after membership selection and before the grant read, or makes the grant
/// read the one that reads a resource at a time. Finding a hook takes a second pool connection
/// while the read snapshot holds one.
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

    static PER_RESOURCE: ScopedTestHookRegistry<String, ()> = ScopedTestHookRegistry::new();

    /// While the guard lives, role summaries of this database come from the read that takes
    /// one resource at a time, which tests compare the served read with.
    pub(crate) async fn read_per_resource(
        pool: &PgPool,
    ) -> Result<ScopedTestHookGuard<String, ()>> {
        Ok(PER_RESOURCE.install(current_test_database(pool).await?, ()))
    }

    /// Runs the pause, and returns whether the grant read is the per-resource one.
    pub(super) async fn run(pool: &PgPool) -> V2Result<bool> {
        let database = current_test_database(pool)
            .await
            .map_err(|_| V2Error::internal_error("failed to run role-summary read test hook"))?;
        if let Some(hook) = HOOKS.take(&database) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(PER_RESOURCE.get_cloned(&database).is_some())
    }
}
