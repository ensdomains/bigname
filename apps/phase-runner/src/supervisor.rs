use std::{collections::BTreeMap, sync::Arc, time::Instant};

use tokio::task::{Id, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    config::{ChainConfig, RuntimeConfig, TimingConfig},
    error::{RunnerError, RunnerResult},
    runner::PhaseRunner,
    runner_support::Backoff,
};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SupervisorReport {
    pub stopped_chains: Vec<(String, RunnerError)>,
    /// How many of `stopped_chains`, from the front, stopped before a stop was requested. Only
    /// a supervised run sets it.
    pub failed_before_stop: usize,
}

impl SupervisorReport {
    /// The chains that failed once a stop had been requested. These are the failures a
    /// supervised run still exits nonzero on.
    pub fn stop_time_failures(&self) -> &[(String, RunnerError)] {
        &self.stopped_chains[self.failed_before_stop..]
    }
}

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
        let timing = config.timing.clone();
        let task = supervisors.spawn(async move {
            supervise_chain(&chain_runner, &chain, &timing, chain_cancellation).await
        });
        chain_by_task.insert(task.id(), chain_id);
    }

    let mut report = SupervisorReport::default();
    while let Some(result) = supervisors.join_next_with_id().await {
        // Read at the join, not at the failure. A stop that lands between the two counts the
        // failure as a stop-time one, which errs toward a nonzero exit.
        let stop_requested = cancellation.is_cancelled();
        match result {
            Ok((task_id, Ok(()))) => {
                let chain_id = take_chain_id(&mut chain_by_task, task_id);
                info!(chain_id, "chain supervisor stopped");
            }
            Ok((task_id, Err(runner_error))) => {
                let chain_id = take_chain_id(&mut chain_by_task, task_id);
                record_terminal_error(&mut report, chain_id, runner_error, stop_requested);
            }
            Err(join_error) => {
                let chain_id = take_chain_id(&mut chain_by_task, join_error.id());
                let runner_error = RunnerError::data_integrity(format!(
                    "chain supervisor task panicked or was cancelled: {join_error}"
                ));
                record_terminal_error(&mut report, chain_id, runner_error, stop_requested);
            }
        }
    }
    if !report.stopped_chains.is_empty() && !cancellation.is_cancelled() {
        wait_for_the_stop(&report, &cancellation).await;
    }
    Ok(report)
}

/// Runs the chain again after an error a later attempt can outlive, as the process restart this
/// replaces did: a database or RPC blip outside a phase's own retry loop, or a phase lock still
/// held at start-up. Other errors stop the chain.
async fn supervise_chain(
    runner: &PhaseRunner,
    chain: &ChainConfig,
    timing: &TimingConfig,
    cancellation: CancellationToken,
) -> RunnerResult<()> {
    let mut backoff = Backoff::new(timing);
    let mut attempt: u64 = 0;
    loop {
        let started = Instant::now();
        let error = match runner.run_chain(chain, cancellation.clone()).await {
            Err(error) if error.is_retryable() && !cancellation.is_cancelled() => error,
            // Waiting for that lock is not required work, so a stop during it is a clean stop.
            Err(error) if error.is_held_at_start_up() => return Ok(()),
            result => return result,
        };
        // An attempt that ran past the longest delay was not a tight failure loop.
        if started.elapsed() >= timing.maximum_backoff {
            backoff.reset();
            attempt = 0;
        }
        attempt += 1;
        let delay = backoff.next_delay();
        warn!(
            chain_id = chain.chain_id,
            attempt,
            error_kind = ?error.kind(),
            error = %error,
            retry_delay_ms = delay.as_millis(),
            "chain supervisor runs the chain again after a retryable error"
        );
        tokio::select! {
            () = cancellation.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay) => {}
        }
    }
}

/// An exit here would leave the stopped chains' metrics unreadable, and a restart policy would
/// replay each chain into the same stop.
async fn wait_for_the_stop(report: &SupervisorReport, cancellation: &CancellationToken) {
    let stopped_chains = report
        .stopped_chains
        .iter()
        .map(|(chain_id, _)| chain_id.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    error!(
        stopped_chains,
        "no chain is running, and the runner serves its metrics until it is stopped"
    );
    cancellation.cancelled().await;
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
    stop_requested: bool,
) {
    error!(
        chain_id,
        error_kind = ?runner_error.kind(),
        error = %runner_error,
        "chain supervisor stopped after a terminal error"
    );
    report.stopped_chains.push((chain_id, runner_error));
    if !stop_requested {
        report.failed_before_stop += 1;
    }
}
