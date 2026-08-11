use async_graphql::Result;

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

use super::error::internal_error;
use super::name_queries::{PhaseGraphqlNameCount, PhaseGraphqlNameListRow};

const NAMESPACE: &str = "ens";

pub(super) async fn load_graphql_head(
    state: &AppState,
    operation: &str,
) -> Result<Option<ServedHead>> {
    let scope = v2_exact_name_snapshot_scope(state, NAMESPACE, None)
        .await
        .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))?;
    load_served_head(&state.pool, &scope)
        .await
        .map_err(|error| internal_error(operation, anyhow::anyhow!("{error:?}")))
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
