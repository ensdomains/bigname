use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};

use anyhow::Result;
use bigname_metrics::{IntGaugeVec, MetricsRegistry};
use bigname_project::PROJECT_STEPS;
use tokio::sync::Notify;

/// What the runner tells the metrics task between refresh ticks: that a batch
/// committed, and which step a long Project run is in. Each change wakes the
/// metrics task, which applies it from this one task, so an older periodic result
/// can never overwrite a newer one.
#[derive(Clone, Default)]
pub struct RunnerMetricsFeed {
    changed: Arc<Notify>,
    project_steps: Arc<Mutex<BTreeMap<String, Option<&'static str>>>>,
}

impl RunnerMetricsFeed {
    pub fn seed_chain(&self, chain: &str) {
        self.project_steps().entry(chain.to_owned()).or_default();
    }

    pub fn batch_committed(&self) {
        self.changed.notify_one();
    }

    pub(super) async fn changed(&self) {
        self.changed.notified().await;
    }

    pub(super) fn project_step_snapshot(&self) -> Vec<(String, Option<&'static str>)> {
        self.project_steps()
            .iter()
            .map(|(chain, step)| (chain.clone(), *step))
            .collect()
    }

    fn project_steps(&self) -> MutexGuard<'_, BTreeMap<String, Option<&'static str>>> {
        self.project_steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl bigname_project::StepObserver for RunnerMetricsFeed {
    fn project_step(&self, chain_id: &str, step: Option<&'static str>) {
        self.project_steps().insert(chain_id.to_owned(), step);
        self.changed.notify_one();
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
