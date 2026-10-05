//! Bounded family publication timing, lag, duplicate-event and hydration counters.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use bigname_metrics::{GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec, MetricsRegistry};
use bigname_project::families::{FamilyOutcome, HydrationKindOutcome, HydrationOutcome};

/// Batches Project reported that the metrics task has not applied yet: the newest summary of each
/// chain, the rows written since the last apply, and up to [`MAX_PENDING_FAMILY_BLOCKS`] family
/// block times per chain. All stay bounded however long the task waits.
#[derive(Clone, Default)]
pub(super) struct PendingProjectWrites {
    inner: Arc<Mutex<Pending>>,
}

/// Family block times kept per chain until the metrics task applies them; further blocks are not
/// observed. The task wakes on every family run, so only a feed nothing drains reaches it.
const MAX_PENDING_FAMILY_BLOCKS: usize = 65_536;

#[derive(Default)]
pub(super) struct Pending {
    families: BTreeMap<String, (f64, u64)>,
    family_block_ms: BTreeMap<String, Vec<u64>>,
    family_anomalies: BTreeMap<String, u64>,
    /// Hydration since the last apply, summed; the age and time are the newest pass's.
    hydration: BTreeMap<String, HydrationOutcome>,
}

impl PendingProjectWrites {
    pub(super) fn record_families(&self, chain: &str, outcome: &FamilyOutcome) {
        let mut pending = lock(&self.inner);
        pending.families.insert(
            chain.to_owned(),
            (outcome.elapsed_ms as f64 / 1_000.0, outcome.lag_blocks()),
        );
        let block_ms = pending.family_block_ms.entry(chain.to_owned()).or_default();
        let room = MAX_PENDING_FAMILY_BLOCKS.saturating_sub(block_ms.len());
        block_ms.extend(outcome.block_ms.iter().take(room));
        let anomalies = pending
            .family_anomalies
            .entry(chain.to_owned())
            .or_default();
        *anomalies += outcome.duplicate_anomalies;
        // A run that prepared no hydration reports nothing, so the gauges keep the newest pass.
        if outcome.hydration.passes > 0 {
            add_hydration(
                pending.hydration.entry(chain.to_owned()).or_default(),
                &outcome.hydration,
            );
        }
    }

    pub(super) fn take(&self) -> Pending {
        std::mem::take(&mut *lock(&self.inner))
    }
}

#[derive(Clone)]
pub(super) struct ProjectWriteGauges {
    families_seconds: GaugeVec,
    family_lag: IntGaugeVec,
    family_block: HistogramVec,
    family_anomalies: IntCounterVec,
    hydration_passes: IntCounterVec,
    hydration_rpc_calls: IntCounterVec,
    hydration_rpc_failures: IntCounterVec,
    hydration_selectors: IntCounterVec,
    hydration_writes: IntCounterVec,
    hydration_rpc_seconds: GaugeVec,
    hydration_head_age: IntGaugeVec,
}

impl ProjectWriteGauges {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            families_seconds: registry.gauge_vec(
                "phase_runner_project_families_seconds",
                "Wall time of the newest Project family publication run.",
                &["chain"],
            )?,
            family_lag: registry.int_gauge_vec(
                "phase_runner_project_family_lag_blocks",
                "Blocks between the family publication and its Project target after the newest run.",
                &["chain"],
            )?,
            family_block: registry.histogram_vec(
                "phase_runner_project_family_block_seconds",
                "Wall time of each owned key family block applied in a transaction of its own, \
                 from its first read to its commit; a rebuild range is not a block and is not \
                 observed.",
                &["chain"],
            )?,
            family_anomalies: registry.int_counter_vec(
                "phase_runner_project_family_duplicate_anomalies_total",
                "Deliveries of one normalized event identity whose position or payload disagreed \
                 with the one the family loop kept.",
                &["chain"],
            )?,
            hydration_passes: registry.int_counter_vec(
                "phase_runner_project_hydration_passes_total",
                "Head blocks whose hydration reads were prepared, each under the one way its pass \
                 ended: timed_out (the pass spent its time before every selector was read), \
                 unserved (the endpoint did not serve the block) or served.",
                &["chain", "result"],
            )?,
            hydration_rpc_calls: registry.int_counter_vec(
                "phase_runner_project_hydration_rpc_calls_total",
                "Hydration Multicall3 aggregates sent, whether or not their block then \
                 published; kind probe is the one-call aggregate sent after a failed batch when \
                 the endpoint has answered nothing at the block yet.",
                &["chain", "kind"],
            )?,
            hydration_rpc_failures: registry.int_counter_vec(
                "phase_runner_project_hydration_rpc_failures_total",
                "Hydration aggregates that failed as a whole and so observed no selector.",
                &["chain", "kind"],
            )?,
            hydration_selectors: registry.int_counter_vec(
                "phase_runner_project_hydration_selectors_total",
                "Selectors a hydration pass read, by outcome: observed (a value or an empty \
                 answer), failed_call (its own call failed inside an answered aggregate), \
                 deferred (its aggregate failed while the endpoint answered other calls at the \
                 block) or not_observed (the endpoint did not serve the block, or the pass had \
                 no time or no call left).",
                &["chain", "kind", "outcome"],
            )?,
            hydration_writes: registry.int_counter_vec(
                "phase_runner_project_hydration_writes_total",
                "Rows hydration changed in committed blocks: value (the row's observation) or \
                 schedule (only its place in the queue, aggregate size limit or failure count).",
                &["chain", "kind", "write"],
            )?,
            hydration_rpc_seconds: registry.gauge_vec(
                "phase_runner_project_hydration_rpc_seconds",
                "Wall time of the RPC reads of the newest family run that hydrated.",
                &["chain"],
            )?,
            hydration_head_age: registry.int_gauge_vec(
                "phase_runner_project_hydration_head_age_seconds",
                "Age of the newest hydrated head block, by its timestamp, when its reads began.",
                &["chain"],
            )?,
        })
    }

    pub(super) fn apply(&self, pending: Pending) {
        for (chain, (seconds, lag)) in pending.families {
            self.families_seconds
                .with_label_values(&[&chain])
                .set(seconds);
            self.family_lag
                .with_label_values(&[&chain])
                .set(gauge_value(lag));
        }
        for (chain, block_ms) in pending.family_block_ms {
            let histogram = self.family_block.with_label_values(&[&chain]);
            for elapsed_ms in block_ms {
                histogram.observe(elapsed_ms as f64 / 1_000.0);
            }
        }
        for (chain, anomalies) in pending.family_anomalies {
            self.family_anomalies
                .with_label_values(&[&chain])
                .inc_by(anomalies);
        }
        for (chain, hydration) in pending.hydration {
            self.apply_hydration(&chain, &hydration);
        }
    }

    fn apply_hydration(&self, chain: &str, hydration: &HydrationOutcome) {
        let passes = |result: &str, count: u64| {
            self.hydration_passes
                .with_label_values(&[chain, result])
                .inc_by(count)
        };
        // Project counts a pass as timed out or as unserved, never both.
        let failed = hydration.unserved_passes + hydration.timed_out_passes;
        passes("served", hydration.passes.saturating_sub(failed));
        passes("unserved", hydration.unserved_passes);
        passes("timed_out", hydration.timed_out_passes);
        let calls = |kind: &str, calls: u64, failures: u64| {
            self.hydration_rpc_calls
                .with_label_values(&[chain, kind])
                .inc_by(calls);
            self.hydration_rpc_failures
                .with_label_values(&[chain, kind])
                .inc_by(failures);
        };
        calls("probe", hydration.probes, hydration.probe_failures);
        for (kind, outcome) in [("reverse", hydration.reverse), ("text", hydration.text)] {
            calls(kind, outcome.rpc_calls, outcome.rpc_failures);
            for (label, count) in [
                (
                    "observed",
                    outcome.answered.saturating_sub(outcome.failed_calls),
                ),
                ("failed_call", outcome.failed_calls),
                ("deferred", outcome.deferred),
                ("not_observed", outcome.not_observed),
            ] {
                self.hydration_selectors
                    .with_label_values(&[chain, kind, label])
                    .inc_by(count);
            }
            for (label, count) in [
                ("value", outcome.value_writes),
                ("schedule", outcome.schedule_writes),
            ] {
                self.hydration_writes
                    .with_label_values(&[chain, kind, label])
                    .inc_by(count);
            }
        }
        self.hydration_rpc_seconds
            .with_label_values(&[chain])
            .set(hydration.rpc_ms as f64 / 1_000.0);
        if let Some(age) = hydration.head_age_seconds {
            self.hydration_head_age
                .with_label_values(&[chain])
                .set(gauge_value(age));
        }
    }
}

/// Sum `outcome` into `total`; the head age and RPC time are the newest pass's.
fn add_hydration(total: &mut HydrationOutcome, outcome: &HydrationOutcome) {
    let add = |total: &mut HydrationKindOutcome, outcome: &HydrationKindOutcome| {
        total.rpc_calls += outcome.rpc_calls;
        total.rpc_failures += outcome.rpc_failures;
        total.answered += outcome.answered;
        total.failed_calls += outcome.failed_calls;
        total.deferred += outcome.deferred;
        total.not_observed += outcome.not_observed;
        total.value_writes += outcome.value_writes;
        total.schedule_writes += outcome.schedule_writes;
    };
    add(&mut total.reverse, &outcome.reverse);
    add(&mut total.text, &outcome.text);
    total.passes += outcome.passes;
    total.unserved_passes += outcome.unserved_passes;
    total.timed_out_passes += outcome.timed_out_passes;
    total.probes += outcome.probes;
    total.probe_failures += outcome.probe_failures;
    total.rpc_ms = outcome.rpc_ms;
    total.head_age_seconds = outcome.head_age_seconds;
}

fn gauge_value(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
