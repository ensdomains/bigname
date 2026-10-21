use std::collections::BTreeMap;

use bigname_storage::{
    SelectedSnapshot, SnapshotConsistency, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, load_served_project_generation, resolve_exact_name_snapshot_selection,
    snapshot_chain_has_head,
};
use sqlx::{PgPool, Postgres, Transaction};

use crate::v2::{
    Meta, SnapshotReadResource, V2Error, V2Result, sanitized_snapshot_internal_error,
    snapshot_meta,
    support::{InterpretRedoCapture, load_selected_project_generations_on},
};

#[derive(Clone)]
pub(crate) struct ServedHead {
    selected: SelectedSnapshot,
    project_generations: BTreeMap<String, String>,
    interpret_redo_state: InterpretRedoCapture,
}

impl ServedHead {
    pub(crate) fn selected(&self) -> &SelectedSnapshot {
        &self.selected
    }

    pub(super) fn meta(&self) -> V2Result<Meta> {
        snapshot_meta(&self.selected)
    }

    /// Begins the read snapshot the whole batch reads on, once it still serves the captured
    /// publication of every selected chain. A block or rebuild after this cannot reach the batch.
    pub(crate) async fn pin(&self, pool: &PgPool) -> V2Result<Transaction<'static, Postgres>> {
        #[cfg(test)]
        served_head_revalidation_test_hooks::run_at(
            &mut pool.into(),
            served_head_revalidation_test_hooks::Stage::BeforeSnapshot,
        )
        .await?;
        let mut reads = bigname_storage::begin_read_snapshot(pool)
            .await
            .map_err(|_| V2Error::internal_error("failed to load lookup data"))?;
        let current = load_selected_project_generations_on(&mut reads, &self.selected, true)
            .await
            .map_err(|_| V2Error::internal_error("failed to validate lookup data"))?;
        if current.as_ref() != Some(&self.project_generations) {
            return Err(V2Error::stale(
                "served publication changed before the lookup read began",
            ));
        }
        Ok(reads)
    }
}

pub(crate) async fn load_served_head(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
) -> V2Result<Option<ServedHead>> {
    let input = crate::v2::snapshots::selector_input(None, None, SnapshotConsistency::Head)
        .map_err(|_| V2Error::internal_error("failed to select lookup data"))?;
    let selected = match resolve_exact_name_snapshot_selection(pool, scope, &input).await {
        Ok(selected) => selected,
        Err(error) if served_head_scope_conflict(&error) => {
            if served_head_absent_for_single_scope(pool, scope, &error).await? {
                return Ok(None);
            }
            return Err(V2Error::conflict(
                "served head is unavailable for snapshot scope",
            ));
        }
        Err(error) if error.kind() == SnapshotSelectionErrorKind::Stale => {
            return Err(V2Error::stale(
                "served data is not available at the current snapshot",
            ));
        }
        Err(error) if error.kind() == SnapshotSelectionErrorKind::InvalidInput => {
            return Err(V2Error::internal_error("failed to select lookup data"));
        }
        Err(error) => {
            return Err(sanitized_snapshot_internal_error(
                &error,
                SnapshotReadResource::Resource,
            ));
        }
    };
    #[cfg(test)]
    served_head_initial_validation_test_hooks::run(pool).await?;
    let project_generations = load_selected_project_generations(pool, &selected).await?;
    let chain_ids = selected
        .chain_positions
        .as_map()
        .values()
        .map(|position| position.chain_id.clone())
        .collect();
    let interpret_redo_state = InterpretRedoCapture::load(pool, chain_ids).await?;
    Ok(Some(ServedHead {
        selected,
        project_generations,
        interpret_redo_state,
    }))
}

/// After the batch's snapshot committed: `409 stale` when a selected chain began, ran or
/// finished an Interpret redo since the capture. The publication needs no recheck, because the
/// batch read it on a snapshot that `ServedHead::pin` verified.
pub(crate) async fn revalidate_served_head(pool: &PgPool, served: &ServedHead) -> V2Result<()> {
    served.interpret_redo_state.revalidate(pool).await
}

pub(crate) async fn load_project_generations(
    pool: &PgPool,
    selected: &SelectedSnapshot,
) -> V2Result<BTreeMap<String, String>> {
    let mut generations = BTreeMap::new();
    for position in selected.chain_positions.as_map().values() {
        let generation = load_served_project_generation(
            pool,
            &position.chain_id,
            position.block_number,
            &position.block_hash,
            false,
            false,
            crate::state::publication_lag_tolerance_blocks(),
        )
        .await
        .map_err(|_| V2Error::internal_error("failed to validate lookup data"))?
        .ok_or_else(|| V2Error::stale("served data is not available at the selected snapshot"))?;
        generations.insert(position.chain_id.clone(), generation);
    }
    Ok(generations)
}

pub(crate) async fn load_selected_project_generations(
    pool: &PgPool,
    selected: &SelectedSnapshot,
) -> V2Result<BTreeMap<String, String>> {
    crate::v2::support::load_selected_project_generations_for_read(pool, selected, true)
        .await
        .map_err(|_| V2Error::internal_error("failed to validate lookup data"))?
        .ok_or_else(|| V2Error::stale("served data is not available at the selected snapshot"))
}

async fn served_head_absent_for_single_scope(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
    error: &SnapshotSelectionError,
) -> V2Result<bool> {
    if scope.required_positions().len() != 1 || error.kind() != SnapshotSelectionErrorKind::Conflict
    {
        return Ok(false);
    }
    let chain_id = &scope.required_positions()[0].chain_id;
    snapshot_chain_has_head(pool, chain_id)
        .await
        .map(|has_head| !has_head)
        .map_err(|error| sanitized_snapshot_internal_error(&error, SnapshotReadResource::Resource))
}

fn served_head_scope_conflict(error: &SnapshotSelectionError) -> bool {
    error.kind() == SnapshotSelectionErrorKind::Conflict
        || error.message().contains("mismatched hash and number")
}

#[cfg(test)]
pub(crate) mod served_head_revalidation_test_hooks {
    use std::sync::Arc;

    use anyhow::Result;
    use bigname_storage::ReadDb;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    use crate::v2::{V2Error, V2Result};

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub(crate) enum Stage {
        /// After the served head was captured, before the batch's read snapshot begins.
        BeforeSnapshot,
        /// Once the snapshot serves the captured publication, before the batch's first read on
        /// it. A change committed here is one the batch must not see.
        MidRead,
    }

    type Key = (String, Stage);

    #[derive(Clone)]
    pub(crate) struct RevalidationHook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct RevalidationControl {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl RevalidationControl {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<Key, RevalidationHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<Key, RevalidationHook>,
        RevalidationControl,
    )> {
        install_at(pool, Stage::MidRead).await
    }

    pub(crate) async fn install_at(
        pool: &PgPool,
        stage: Stage,
    ) -> Result<(
        ScopedTestHookGuard<Key, RevalidationHook>,
        RevalidationControl,
    )> {
        let database = (current_test_database(pool).await?, stage);
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            RevalidationHook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, RevalidationControl { reached, resume }))
    }

    pub(crate) async fn run_at(db: &mut ReadDb<'_>, stage: Stage) -> V2Result<()> {
        let database = crate::v2::support::test_database_on(db)
            .await
            .map_err(|_| V2Error::internal_error("failed to run lookup served-head test hook"))?;
        if let Some(hook) = HOOKS.take(&(database, stage)) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod served_head_initial_validation_test_hooks {
    use std::sync::Arc;

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    use crate::v2::{V2Error, V2Result};

    #[derive(Clone)]
    pub(crate) struct ValidationHook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct ValidationControl {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl ValidationControl {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<String, ValidationHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, ValidationHook>,
        ValidationControl,
    )> {
        let database = current_test_database(pool).await?;
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            ValidationHook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, ValidationControl { reached, resume }))
    }

    pub(super) async fn run(pool: &PgPool) -> V2Result<()> {
        let database = current_test_database(pool).await.map_err(|_| {
            V2Error::internal_error("failed to resolve lookup initial-validation hook")
        })?;
        if let Some(hook) = HOOKS.get_cloned(&database) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}
