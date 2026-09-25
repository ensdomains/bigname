use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::Result;
use bigname_metrics::{IntGaugeVec, MetricsRegistry};
use bigname_project::PROJECT_STEPS;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::RunnerMetricsFeed;

/// Which step a full-rebuild or redo Project run is in, per chain. Each change wakes
/// the step worker, which applies the latest snapshot. That worker is separate from
/// the served-lag one, so a step change never runs the served-lag query and never
/// waits behind it. The feed keeps one value per chain and the last write wins, so it
/// needs at most one reporting run per chain at a time; the runner's per-chain phase
/// lock provides that, and the feed does not check it.
#[derive(Clone, Default)]
pub(super) struct ProjectStepFeed {
    changed: Arc<Notify>,
    steps: Arc<Mutex<BTreeMap<String, Option<&'static str>>>>,
}

impl ProjectStepFeed {
    /// Seeded chains export the step gauges at 0 before any run reports.
    pub(super) fn seed_chain(&self, chain: &str) {
        self.steps().entry(chain.to_owned()).or_default();
    }

    /// Waits for the next step change; a change made while nobody waits is kept.
    pub(super) async fn changed(&self) {
        self.changed.notified().await;
    }

    pub(super) fn snapshot(&self) -> Vec<(String, Option<&'static str>)> {
        self.steps()
            .iter()
            .map(|(chain, step)| (chain.clone(), *step))
            .collect()
    }

    fn record(&self, chain: &str, step: Option<&'static str>) {
        self.steps().insert(chain.to_owned(), step);
        self.changed.notify_one();
    }

    fn steps(&self) -> MutexGuard<'_, BTreeMap<String, Option<&'static str>>> {
        self.steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl bigname_project::StepObserver for RunnerMetricsFeed {
    fn project_step(&self, chain_id: &str, step: Option<&'static str>) {
        self.project_steps.record(chain_id, step);
    }
}

#[derive(Clone)]
pub(super) struct ProjectStepGauges {
    step: IntGaugeVec,
    index: IntGaugeVec,
    total: IntGaugeVec,
}

impl ProjectStepGauges {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            step: registry.int_gauge_vec(
                "phase_runner_project_step",
                "Active step of a full-rebuild or redo Project run as a one-hot gauge.",
                &["chain", "step"],
            )?,
            index: registry.int_gauge_vec(
                "phase_runner_project_step_index",
                "One-based position of the active full-rebuild or redo Project step, or 0 when none runs.",
                &["chain"],
            )?,
            total: registry.int_gauge_vec(
                "phase_runner_project_step_total",
                "Number of steps in the running full-rebuild or redo Project run, or 0 when none runs.",
                &["chain"],
            )?,
        })
    }

    pub(super) fn apply(&self, snapshot: &[(String, Option<&'static str>)]) {
        let total = i64::try_from(PROJECT_STEPS.len()).unwrap_or(i64::MAX);
        for (chain, active) in snapshot {
            let mut index = 0;
            for (position, step) in (1..).zip(PROJECT_STEPS) {
                let is_active = *active == Some(step);
                if is_active {
                    index = position;
                }
                self.step
                    .with_label_values(&[chain.as_str(), step])
                    .set(i64::from(is_active));
            }
            self.index.with_label_values(&[chain.as_str()]).set(index);
            self.total
                .with_label_values(&[chain.as_str()])
                .set(if index == 0 { 0 } else { total });
        }
    }
}

/// Applies Project step changes on their own, so a step change or the final idle
/// never waits behind a pending served-lag database refresh.
pub(super) async fn project_step_loop(
    gauges: ProjectStepGauges,
    steps: ProjectStepFeed,
    cancellation: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return,
            () = steps.changed() => gauges.apply(&steps.snapshot()),
        }
    }
}
