use std::{collections::BTreeMap, sync::Arc};

use tokio::task::{Id, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{
    config::RuntimeConfig,
    error::{RunnerError, RunnerResult},
    runner::{PhaseRunner, SupervisorReport},
};

pub(crate) async fn run(
    runner: Arc<PhaseRunner>,
    config: &RuntimeConfig,
    cancellation: CancellationToken,
) -> RunnerResult<SupervisorReport> {
    let mut supervisors = JoinSet::new();
    let mut chain_by_task = BTreeMap::<Id, String>::new();
    for chain in config.chains.iter().cloned() {
        let chain_id = chain.chain_id.clone();
        let chain_runner = Arc::clone(&runner);
        let chain_cancellation = cancellation.child_token();
        let task = supervisors
            .spawn(async move { chain_runner.run_chain(&chain, chain_cancellation).await });
        chain_by_task.insert(task.id(), chain_id);
    }

    let mut report = SupervisorReport::default();
    while let Some(result) = supervisors.join_next_with_id().await {
        match result {
            Ok((task_id, Ok(()))) => {
                let chain_id = take_chain_id(&mut chain_by_task, task_id);
                info!(chain_id, "chain supervisor stopped");
            }
            Ok((task_id, Err(runner_error))) => {
                let chain_id = take_chain_id(&mut chain_by_task, task_id);
                record_terminal_error(&mut report, chain_id, runner_error);
            }
            Err(join_error) => {
                let chain_id = take_chain_id(&mut chain_by_task, join_error.id());
                let runner_error = RunnerError::data_integrity(format!(
                    "chain supervisor task panicked or was cancelled: {join_error}"
                ));
                record_terminal_error(&mut report, chain_id, runner_error);
            }
        }
    }
    Ok(report)
}

fn take_chain_id(chain_by_task: &mut BTreeMap<Id, String>, task_id: Id) -> String {
    chain_by_task
        .remove(&task_id)
        .unwrap_or_else(|| format!("unknown-task-{task_id}"))
}

fn record_terminal_error(
    report: &mut SupervisorReport,
    chain_id: String,
    runner_error: RunnerError,
) {
    error!(
        chain_id,
        error_kind = ?runner_error.kind(),
        error = %runner_error,
        "chain supervisor stopped after a terminal error"
    );
    report.stopped_chains.push((chain_id, runner_error));
}
