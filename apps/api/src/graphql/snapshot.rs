use async_graphql::{Context, MaybeUndefined, Result};
use bigname_storage::SelectedSnapshot;
use tokio::sync::OnceCell;

use crate::{
    AppState,
    v2::{
        lookup::{
            head::{ServedHead, load_served_head, revalidate_served_head},
            require_name_current_at_served_head, require_name_projection_at_served_head,
        },
        v2_exact_name_snapshot_scope,
    },
};

use super::enums::SubgraphErrorPolicy;
use super::error::internal_error;
use super::inputs::BlockHeight;
use super::name_queries::{PhaseGraphqlNameCount, PhaseGraphqlNameListRow};

const NAMESPACE: &str = "ens";

/// One [served head](../../../../docs/glossary.md#served-head) shared by every root field in an HTTP
/// GraphQL request.
#[derive(Default)]
pub(super) struct GraphqlRequestHead {
    selected: OnceCell<Option<ServedHead>>,
}

pub(super) enum BlockConstraint {
    Latest,
    Number,
}

impl BlockConstraint {
    pub(super) fn hides_hash(&self) -> bool {
        matches!(self, Self::Number)
    }
}

pub(super) async fn load_graphql_head(
    ctx: &Context<'_>,
    operation: &str,
) -> Result<Option<ServedHead>> {
    let state = ctx.data::<AppState>()?;
    let select = || async {
        let scope = v2_exact_name_snapshot_scope(state, NAMESPACE, None)
            .await
            .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))?;
        load_served_head(&state.pool, &scope)
            .await
            .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))
    };
    let Some(request_head) = ctx.data_opt::<GraphqlRequestHead>() else {
        return select().await;
    };
    request_head.selected.get_or_try_init(select).await.cloned()
}

pub(super) async fn load_graphql_entity_head(
    ctx: &Context<'_>,
    block: Option<&BlockHeight>,
    _subgraph_error: SubgraphErrorPolicy,
    operation: &str,
) -> Result<Option<ServedHead>> {
    let head = load_graphql_head(ctx, operation).await?;
    validate_block_constraint(head.as_ref(), block, operation)?;
    // Accept the policy argument without changing the existing Manager response path. The current
    // projections do not record an indexing error against individual names.
    Ok(head)
}

pub(super) fn validate_block_constraint(
    head: Option<&ServedHead>,
    block: Option<&BlockHeight>,
    operation: &str,
) -> Result<BlockConstraint> {
    let Some(block) = block else {
        return Ok(BlockConstraint::Latest);
    };
    if block.hash.is_undefined() && block.number.is_undefined() && block.number_gte.is_undefined() {
        return Err(async_graphql::Error::new(
            "block must contain hash, number, or number_gte",
        ));
    }
    let head = head.ok_or_else(|| {
        async_graphql::Error::new("served head is unavailable for the requested block")
    })?;
    let position = head
        .selected()
        .chain_positions
        .as_map()
        .values()
        .next()
        .ok_or_else(|| internal_error(operation, anyhow::anyhow!("served head has no block")))?;
    if !block.hash.is_undefined() {
        let MaybeUndefined::Value(hash) = &block.hash else {
            return Err(async_graphql::Error::new("block.hash must not be null"));
        };
        if !position.block_hash.eq_ignore_ascii_case(hash.as_str()) {
            return Err(async_graphql::Error::new(
                "the requested block hash is not the served head",
            ));
        }
        return Ok(BlockConstraint::Latest);
    }
    if !block.number.is_undefined() {
        let MaybeUndefined::Value(number) = block.number else {
            return Err(async_graphql::Error::new("block.number must not be null"));
        };
        if number < 0 {
            return Err(async_graphql::Error::new(
                "block number constraints must be non-negative",
            ));
        }
        if position.block_number != i64::from(number) {
            return Err(async_graphql::Error::new(
                "the requested block number is not the served head",
            ));
        }
        return Ok(BlockConstraint::Number);
    }
    let MaybeUndefined::Value(number_gte) = block.number_gte else {
        return Err(async_graphql::Error::new(
            "block.number_gte must not be null",
        ));
    };
    if number_gte < 0 {
        return Err(async_graphql::Error::new(
            "block number constraints must be non-negative",
        ));
    }
    if position.block_number < i64::from(number_gte) {
        return Err(async_graphql::Error::new(
            "the served head has not reached block.number_gte",
        ));
    }
    Ok(BlockConstraint::Latest)
}

pub(super) async fn load_graphql_indexing_errors(
    state: &AppState,
    selected: &SelectedSnapshot,
    operation: &str,
) -> Result<bool> {
    let status = bigname_storage::load_phase_indexing_status(&state.pool)
        .await
        .map_err(|error| internal_error(operation, error))?;
    Ok(selected.chain_positions.as_map().values().any(|position| {
        let Some(row) = status
            .chains
            .iter()
            .find(|row| row.chain_id == position.chain_id)
        else {
            return true;
        };
        row.canonical_block != Some(position.block_number)
            || row.latest_projected_block != Some(position.block_number)
            || !matches!(
                row.project_phase_status.as_deref(),
                Some("completed" | "running")
            )
            || !row.project_generation_current
            || row.project_redo_in_progress
            || row.any_phase_settled_while_unconfigured
            || required_verification_has_error(row)
    }))
}

fn required_verification_has_error(row: &bigname_storage::IndexingStatusChainRow) -> bool {
    if !row.provider_trusted_verification_required {
        return false;
    }
    row.ingest_phase_status.as_deref() != Some("completed")
        || row.verify_phase_status.as_deref() != Some("completed")
        || row.verify_settled_while_unconfigured
        || !matches!(
            row.verify_verification_level.as_deref(),
            Some("quick_synced" | "cross_checked" | "node_checked")
        )
}

pub(super) fn graphql_snapshot_chain_ids(head: Option<&ServedHead>) -> Vec<String> {
    head.into_iter()
        .flat_map(|head| head.selected().chain_positions.as_map().values())
        .map(|position| position.chain_id.clone())
        .collect()
}

pub(super) fn require_rows_at_head(
    rows: &[PhaseGraphqlNameListRow],
    head: Option<&ServedHead>,
    operation: &str,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let head = head.ok_or_else(|| {
        internal_error(
            operation,
            anyhow::anyhow!("schema-v2 GraphQL projection has rows without a served head"),
        )
    })?;
    for row in rows {
        require_name_current_at_served_head(&row.row.row, head.selected())
            .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))?;
        for target in &row.membership_targets {
            require_name_projection_at_served_head(target, NAMESPACE, head.selected())
                .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))?;
        }
    }
    Ok(())
}

pub(super) fn require_inventory_at_head(
    chain_positions: &serde_json::Value,
    chain_id: Option<&str>,
    head: Option<&ServedHead>,
    operation: &str,
) -> Result<()> {
    let head = head.ok_or_else(|| {
        internal_error(
            operation,
            anyhow::anyhow!("schema-v2 GraphQL inventory has no served head"),
        )
    })?;
    if let Some(target_block_number) = chain_positions
        .get("target_block_number")
        .and_then(serde_json::Value::as_i64)
    {
        let target_block_hash = chain_positions
            .get("target_block_hash")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                internal_error(
                    operation,
                    anyhow::anyhow!("record inventory target has no block hash"),
                )
            })?;
        let chain_id = chain_id.ok_or_else(|| {
            internal_error(
                operation,
                anyhow::anyhow!("record inventory target has no chain id"),
            )
        })?;
        let selected = head
            .selected()
            .chain_positions
            .as_map()
            .values()
            .find(|position| position.chain_id == chain_id)
            .ok_or_else(|| {
                internal_error(
                    operation,
                    anyhow::anyhow!("record inventory target is outside the served chain scope"),
                )
            })?;
        if target_block_number > selected.block_number
            || (target_block_number == selected.block_number
                && target_block_hash != selected.block_hash)
        {
            return Err(internal_error(
                operation,
                anyhow::anyhow!("record inventory target is ahead of the served head"),
            ));
        }
        return Ok(());
    }
    require_name_projection_at_served_head(chain_positions, NAMESPACE, head.selected())
        .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))
}

pub(super) fn require_count_at_head(
    count: &PhaseGraphqlNameCount,
    head: Option<&ServedHead>,
    operation: &str,
) -> Result<()> {
    if count.total_count == 0 {
        return Ok(());
    }
    let head = head.ok_or_else(|| {
        internal_error(
            operation,
            anyhow::anyhow!("schema-v2 GraphQL count has rows without a served head"),
        )
    })?;
    for target in &count.name_targets {
        require_name_projection_at_served_head(
            &target.chain_positions,
            &target.namespace,
            head.selected(),
        )
        .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))?;
    }
    for target in &count.membership_targets {
        require_name_projection_at_served_head(target, NAMESPACE, head.selected())
            .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))?;
    }
    Ok(())
}

pub(super) async fn revalidate_graphql_head(
    state: &AppState,
    head: Option<&ServedHead>,
    operation: &str,
) -> Result<()> {
    if let Some(head) = head {
        revalidate_served_head(&state.pool, head)
            .await
            .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod nested_inventory_test_hooks {
    use std::sync::Arc;

    use anyhow::Result;
    use bigname_test_support::{
        ScopedTestHookGuard, ScopedTestHookRegistry, current_test_database,
    };
    use sqlx::PgPool;
    use tokio::sync::Barrier;

    #[derive(Clone)]
    pub(crate) struct Hook {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    pub(crate) struct Control {
        reached: Arc<Barrier>,
        resume: Arc<Barrier>,
    }

    impl Control {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.wait().await;
        }

        pub(crate) async fn resume(&self) {
            self.resume.wait().await;
        }
    }

    static HOOKS: ScopedTestHookRegistry<String, Hook> = ScopedTestHookRegistry::new();

    pub(crate) async fn install(
        pool: &PgPool,
    ) -> Result<(ScopedTestHookGuard<String, Hook>, Control)> {
        let database = current_test_database(pool).await?;
        let reached = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let guard = HOOKS.install(
            database,
            Hook {
                reached: Arc::clone(&reached),
                resume: Arc::clone(&resume),
            },
        );
        Ok((guard, Control { reached, resume }))
    }

    pub(crate) async fn run(pool: &PgPool) -> Result<()> {
        let database = current_test_database(pool).await?;
        if let Some(hook) = HOOKS.take(&database) {
            hook.reached.wait().await;
            hook.resume.wait().await;
        }
        Ok(())
    }
}
