use anyhow::{Context, Result};
use bigname_manifests::WatchedChainPlan;
use bigname_storage::{ChainCheckpoint, sync_chain_checkpoints};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WatchedChainPlanState {
    pub(crate) chain_count: usize,
    pub(crate) address_count: usize,
    pub(crate) entry_count: usize,
}

pub(crate) fn watched_chain_plan_state(plan: &[WatchedChainPlan]) -> WatchedChainPlanState {
    WatchedChainPlanState {
        chain_count: plan.len(),
        address_count: plan.iter().map(|chain| chain.addresses.len()).sum(),
        entry_count: plan
            .iter()
            .map(|chain| {
                chain.manifest_root_entry_count
                    + chain.manifest_contract_entry_count
                    + chain.discovery_edge_entry_count
            })
            .sum(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IntakeChainTask {
    pub(crate) chain: String,
    pub(crate) addresses: Vec<String>,
    pub(crate) manifest_root_entry_count: usize,
    pub(crate) manifest_contract_entry_count: usize,
    pub(crate) discovery_edge_entry_count: usize,
    pub(crate) checkpoint: ChainCheckpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IntakeRuntimeState {
    pub(crate) chain_count: usize,
    pub(crate) address_count: usize,
    pub(crate) entry_count: usize,
    pub(crate) cold_start_chain_count: usize,
    pub(crate) resumable_chain_count: usize,
    pub(crate) safe_checkpoint_chain_count: usize,
    pub(crate) finalized_checkpoint_chain_count: usize,
}

pub(crate) fn checkpoint_mode(checkpoint: &ChainCheckpoint) -> &'static str {
    if checkpoint.canonical_block_hash.is_some() && checkpoint.canonical_block_number.is_some() {
        "resume"
    } else {
        "cold_start"
    }
}

pub(crate) fn intake_runtime_state(tasks: &[IntakeChainTask]) -> IntakeRuntimeState {
    IntakeRuntimeState {
        chain_count: tasks.len(),
        address_count: tasks.iter().map(|task| task.addresses.len()).sum(),
        entry_count: tasks
            .iter()
            .map(|task| {
                task.manifest_root_entry_count
                    + task.manifest_contract_entry_count
                    + task.discovery_edge_entry_count
            })
            .sum(),
        cold_start_chain_count: tasks
            .iter()
            .filter(|task| checkpoint_mode(&task.checkpoint) == "cold_start")
            .count(),
        resumable_chain_count: tasks
            .iter()
            .filter(|task| checkpoint_mode(&task.checkpoint) == "resume")
            .count(),
        safe_checkpoint_chain_count: tasks
            .iter()
            .filter(|task| {
                task.checkpoint.safe_block_hash.is_some()
                    && task.checkpoint.safe_block_number.is_some()
            })
            .count(),
        finalized_checkpoint_chain_count: tasks
            .iter()
            .filter(|task| {
                task.checkpoint.finalized_block_hash.is_some()
                    && task.checkpoint.finalized_block_number.is_some()
            })
            .count(),
    }
}

pub(crate) async fn sync_intake_chain_tasks(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<Vec<IntakeChainTask>> {
    let chain_ids = watched_chain_plan
        .iter()
        .map(|chain| chain.chain.clone())
        .collect::<Vec<_>>();
    let checkpoints = sync_chain_checkpoints(pool, &chain_ids).await?;
    let checkpoints = checkpoints
        .into_iter()
        .map(|checkpoint| (checkpoint.chain_id.clone(), checkpoint))
        .collect::<std::collections::BTreeMap<_, _>>();

    let mut tasks = Vec::with_capacity(watched_chain_plan.len());
    for chain in watched_chain_plan {
        let checkpoint = checkpoints.get(&chain.chain).cloned().with_context(|| {
            format!(
                "checkpoint sync did not return a persisted chain row for {}",
                chain.chain
            )
        })?;
        tasks.push(IntakeChainTask {
            chain: chain.chain.clone(),
            addresses: chain.addresses.clone(),
            manifest_root_entry_count: chain.manifest_root_entry_count,
            manifest_contract_entry_count: chain.manifest_contract_entry_count,
            discovery_edge_entry_count: chain.discovery_edge_entry_count,
            checkpoint,
        });
    }

    Ok(tasks)
}
