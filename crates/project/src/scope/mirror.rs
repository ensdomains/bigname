use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

// The graph and its independent seen keys live only for this canonical transaction.
// New scope keys discover affected mirror pointers; cached links serve later closure steps.
#[derive(Default)]
pub(super) struct Strategy {
    iterations: usize,
    evidence_only: bool,
    #[cfg(test)]
    audit: bool,
    #[cfg(test)]
    reference: Option<deployed_reference::Strategy>,
}

pub(super) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<Strategy> {
    crate::stage::mirror_evidence::create(transaction).await?;
    #[cfg(test)]
    if crate::engine::contract_compare::audit::enabled(transaction).await? {
        crate::engine::contract_compare::audit::mirror_stage(transaction, chain_id, target_block)
            .await?;
        return Ok(Strategy {
            audit: true,
            ..Strategy::default()
        });
    }
    #[cfg(test)]
    if crate::reference::enabled(transaction).await? {
        return Ok(Strategy {
            reference: Some(deployed_reference::stage(transaction, chain_id, target_block).await?),
            ..Strategy::default()
        });
    }
    execute(
        transaction,
        chain_id,
        target_block,
        include_str!("mirror.sql"),
    )
    .await?;
    Ok(Strategy::default())
}

pub(super) fn use_evidence_inputs(strategy: &mut Strategy, enabled: bool) {
    strategy.evidence_only = enabled;
}

pub(super) async fn include(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
    strategy: &mut Strategy,
) -> Result<()> {
    #[cfg(test)]
    if strategy.audit {
        return crate::engine::contract_compare::audit::mirror_include(transaction).await;
    }
    #[cfg(test)]
    if let Some(reference) = strategy.reference.as_mut() {
        return deployed_reference::include(transaction, chain_id, target_block, reference).await;
    }

    let started = std::time::Instant::now();
    // Materialized frontiers and deduplicated suffixes handle both isolated updates
    // and shared ancestors. No seed/history threshold constructs a chain-wide graph.
    execute(
        transaction,
        chain_id,
        target_block,
        include_str!("mirror_bulk.sql"),
    )
    .await?;
    execute(
        transaction,
        chain_id,
        target_block,
        if strategy.evidence_only {
            include_str!("mirror_evidence_include.sql")
        } else {
            include_str!("mirror_bulk_include.sql")
        },
    )
    .await?;
    if tracing::enabled!(tracing::Level::DEBUG) {
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM project_mirror_frontier_names),
                    (SELECT count(*) FROM project_mirror_frontier_resources),
                    (SELECT count(*) FROM project_mirror_new_pointers),
                    (SELECT count(*) FROM project_mirror_links)",
        )
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to measure mirror frontier", error))?;
        tracing::debug!(
            strategy = "affected_graph",
            reason = "new_scope_keys",
            iteration = strategy.iterations,
            new_names = counts.0,
            new_resources = counts.1,
            new_pointers = counts.2,
            cached_links = counts.3,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "Project mirror expansion completed"
        );
    }
    execute(
        transaction,
        chain_id,
        target_block,
        include_str!("mirror_batch_finish.sql"),
    )
    .await?;
    strategy.iterations += 1;
    Ok(())
}

async fn execute(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
    statements: &str,
) -> Result<()> {
    for statement in statements.split(';').filter(|sql| !sql.trim().is_empty()) {
        // Only the bounded, analyzed inputs to this statement select its plan. This
        // changes join freedom, never the affected graph or eligibility predicates.
        let mut planned = statement.to_owned();
        if statement.contains(" OFFSET 0") {
            let inputs = [
                "frontier_resources",
                "resource_nodes",
                "seeds",
                "queried_names",
                "suffixes",
                "wanted",
            ]
            .into_iter()
            .filter(|name| statement.contains(&format!("FROM project_mirror_{name} ")))
            .map(|name| format!("SELECT 1 FROM project_mirror_{name}"))
            .collect::<Vec<_>>();
            debug_assert!(!inputs.is_empty());
            let broad: bool = sqlx::query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM ({}) inputs OFFSET 256 LIMIT 1)",
                inputs.join(" UNION ALL ")
            ))
            .fetch_one(&mut **transaction)
            .await
            .map_err(|e| ProjectError::database("failed to assess mirror frontier", e))?;
            if broad {
                planned = planned.replace(" OFFSET 0", "");
            }
            tracing::debug!(
                strategy = if broad { "set_based" } else { "keyed" },
                "Project mirror history strategy selected"
            );
        }
        #[cfg(test)]
        if let Some(stage) = crate::profile::mirror_stage(&planned)
            && crate::profile::execute(transaction, chain_id, target_block, &planned, stage).await?
        {
            continue;
        }
        let query = sqlx::query(&planned);
        let query = if statement.contains("$1") {
            query.bind(chain_id).bind(target_block)
        } else {
            query
        };
        query.execute(&mut **transaction).await.map_err(|error| {
            ProjectError::database("failed to expand affected mirror graph", error)
        })?;
    }
    Ok(())
}

pub(super) async fn finish(
    transaction: &mut Transaction<'_, Postgres>,
    _strategy: Strategy,
) -> Result<()> {
    #[cfg(test)]
    if _strategy.audit {
        return crate::engine::contract_compare::audit::mirror_finish(transaction).await;
    }
    #[cfg(test)]
    if let Some(reference) = _strategy.reference {
        return deployed_reference::finish(transaction, reference).await;
    }
    sqlx::query(
        "DROP TABLE project_mirror_links, project_mirror_seen_seeds, project_mirror_seen_pointers, project_mirror_seen_nodes, project_mirror_cached_nodes",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to drop affected mirror graph", error))?;
    Ok(())
}

#[cfg(test)]
#[path = "mirror_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "mirror_reference.rs"]
mod deployed_reference;
