use std::collections::BTreeMap;

use bigname_storage::{
    SelectedSnapshot, SnapshotConsistency, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, SnapshotSelectorInput, resolve_exact_name_snapshot_selection,
    snapshot_chain_has_head,
};
use sqlx::PgPool;

use crate::v2::{
    Meta, SnapshotReadResource, V2Error, V2Result, sanitized_snapshot_internal_error, snapshot_meta,
};

pub(super) struct ServedHead {
    selected: SelectedSnapshot,
    project_generations: BTreeMap<String, String>,
}

impl ServedHead {
    pub(super) fn selected(&self) -> &SelectedSnapshot {
        &self.selected
    }

    pub(super) fn meta(&self) -> V2Result<Meta> {
        snapshot_meta(&self.selected)
    }
}

pub(super) async fn load_served_head(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
) -> V2Result<Option<ServedHead>> {
    let input = SnapshotSelectorInput::new(None, None, SnapshotConsistency::Head)
        .map_err(|_| V2Error::internal_error("failed to build lookup served head selector"))?;
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
                "served data is not available at the current phase head",
            ));
        }
        Err(error) if error.kind() == SnapshotSelectionErrorKind::InvalidInput => {
            return Err(V2Error::internal_error(
                "failed to build lookup served head selector",
            ));
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
    Ok(Some(ServedHead {
        selected,
        project_generations,
    }))
}

pub(super) async fn revalidate_served_head(pool: &PgPool, served: &ServedHead) -> V2Result<()> {
    #[cfg(test)]
    served_head_revalidation_test_hooks::run(pool).await?;
    let current = load_selected_project_generations(pool, &served.selected).await?;
    if current != served.project_generations {
        return Err(V2Error::stale(
            "served data changed while the lookup was being read",
        ));
    }
    Ok(())
}

pub(crate) async fn load_project_generations(
    pool: &PgPool,
    selected: &SelectedSnapshot,
) -> V2Result<BTreeMap<String, String>> {
    let mut generations = BTreeMap::new();
    for position in selected.chain_positions.as_map().values() {
        let generation = sqlx::query_scalar::<_, String>(
            r#"
            SELECT project.xmin::TEXT
            FROM chain_heads head
            JOIN chain_phase_state project
              ON project.chain_id = head.chain_id
             AND project.phase_name = 'project'
             AND project.phase_status = 'completed'
             AND project.current_block_number = head.latest_block_number
             AND project.current_block_hash = head.latest_block_hash
             AND project.input_content_hash = $2
            WHERE head.chain_id = $1
            "#,
        )
        .bind(&position.chain_id)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .fetch_optional(pool)
        .await
        .map_err(|_| V2Error::internal_error("failed to validate lookup project publication"))?
        .ok_or_else(|| V2Error::stale("served data is not available at the selected phase head"))?;
        generations.insert(position.chain_id.clone(), generation);
    }
    Ok(generations)
}

pub(crate) async fn load_selected_project_generations(
    pool: &PgPool,
    selected: &SelectedSnapshot,
) -> V2Result<BTreeMap<String, String>> {
    let mut generations = BTreeMap::new();
    for position in selected.chain_positions.as_map().values() {
        let generation = sqlx::query_scalar::<_, String>(
            r#"
            SELECT project.xmin::TEXT
            FROM chain_heads head
            JOIN chain_phase_state project
              ON project.chain_id = head.chain_id
             AND project.phase_name = 'project'
             AND project.phase_status = 'completed'
             AND project.current_block_number = head.latest_block_number
             AND project.current_block_hash = head.latest_block_hash
             AND project.input_content_hash = $4
            WHERE head.chain_id = $1
              AND head.latest_block_number = $2
              AND head.latest_block_hash = $3
            "#,
        )
        .bind(&position.chain_id)
        .bind(position.block_number)
        .bind(&position.block_hash)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .fetch_optional(pool)
        .await
        .map_err(|_| V2Error::internal_error("failed to validate lookup project publication"))?
        .ok_or_else(|| V2Error::stale("served data is not available at the selected phase head"))?;
        generations.insert(position.chain_id.clone(), generation);
    }
    Ok(generations)
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
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    use crate::v2::{V2Error, V2Result};

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

    static HOOKS: ScopedTestHookRegistry<String, RevalidationHook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(
        ScopedTestHookGuard<String, RevalidationHook>,
        RevalidationControl,
    )> {
        let database = current_test_database(pool).await?;
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

    pub(super) async fn run(pool: &PgPool) -> V2Result<()> {
        let database = current_test_database(pool)
            .await
            .map_err(|_| V2Error::internal_error("failed to run lookup served-head test hook"))?;
        if let Some(hook) = HOOKS.take(&database) {
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
