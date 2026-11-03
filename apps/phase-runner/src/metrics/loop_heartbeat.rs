use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::Result;
use bigname_metrics::{IntGaugeVec, MetricsRegistry};

/// What the runner's loops report to the metrics listener: when each chain last made loop
/// progress, and which chains' walk index rebuild is waiting for storage capacity.
#[derive(Clone, Default)]
pub struct RunnerLoopHeartbeat {
    last_progress: Arc<Mutex<BTreeMap<String, Instant>>>,
    capacity_blocked: Arc<Mutex<BTreeSet<String>>>,
}

impl RunnerLoopHeartbeat {
    pub fn record_progress(&self, chain: &str) {
        self.last_progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(chain.to_owned(), Instant::now());
    }

    pub fn age_seconds(&self, chain: &str) -> Option<i64> {
        self.last_progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(chain)
            .map(elapsed_seconds)
    }

    pub fn remove_progress(&self, chain: &str) {
        self.last_progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(chain);
    }

    pub(super) fn ages_seconds(&self) -> BTreeMap<String, i64> {
        let last_progress = self
            .last_progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        last_progress
            .iter()
            .map(|(chain, instant)| (chain.clone(), elapsed_seconds(instant)))
            .collect()
    }

    /// Marks whether the chain's walk index rebuild is waiting for storage capacity.
    pub fn set_walk_index_capacity_blocked(&self, chain: &str, blocked: bool) {
        let mut chains = self
            .capacity_blocked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if blocked {
            chains.insert(chain.to_owned());
        } else {
            chains.remove(chain);
        }
    }

    pub fn walk_index_capacity_blocked(&self, chain: &str) -> bool {
        self.capacity_blocked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(chain)
    }
}

fn elapsed_seconds(instant: &Instant) -> i64 {
    i64::try_from(instant.elapsed().as_secs()).unwrap_or(i64::MAX)
}

/// `phase_runner_walk_index_capacity_blocked{chain}`: 1 while the chain's walk index rebuild
/// waits for storage capacity, else 0, for every chain with phase rows.
#[derive(Clone)]
pub(super) struct WalkIndexCapacityGauge {
    blocked: IntGaugeVec,
}

impl WalkIndexCapacityGauge {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            blocked: registry.int_gauge_vec(
                "phase_runner_walk_index_capacity_blocked",
                "Whether the chain's rebuild of the normalized_events indexes the walk index set \
                 scripts drop is waiting for storage capacity before Project.",
                &["chain"],
            )?,
        })
    }

    pub(super) fn apply(&self, chains: &BTreeSet<String>, heartbeat: &RunnerLoopHeartbeat) {
        for chain in chains {
            self.blocked
                .with_label_values(&[chain])
                .set(i64::from(heartbeat.walk_index_capacity_blocked(chain)));
        }
    }

    pub(super) fn remove(&self, chain: &str) {
        let _ = self.blocked.remove_label_values(&[chain]);
    }
}
