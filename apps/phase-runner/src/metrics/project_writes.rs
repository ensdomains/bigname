//! What each Project batch read and wrote. The gauges hold the newest committed batch of each
//! chain, so a scrape answers "what did the last block cost"; the counter adds up the rows every
//! batch wrote to each served table.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use bigname_metrics::{GaugeVec, IntCounterVec, IntGaugeVec, MetricsRegistry};
use bigname_project::WriteSummary;

/// Batches Project reported that the metrics task has not applied yet: the newest summary of each
/// chain, and the rows written since the last apply. Both stay bounded however long the task waits.
#[derive(Clone, Default)]
pub(super) struct PendingProjectWrites {
    inner: Arc<Mutex<Pending>>,
}

#[derive(Default)]
pub(super) struct Pending {
    latest: BTreeMap<String, WriteSummary>,
    rows: BTreeMap<(String, &'static str, &'static str), u64>,
}

impl PendingProjectWrites {
    pub(super) fn record(&self, chain: &str, summary: &WriteSummary) {
        let mut pending = lock(&self.inner);
        for (kind, tables) in [
            ("inserted", &summary.inserted),
            ("deleted", &summary.deleted),
        ] {
            for (table, rows) in tables {
                let total = pending
                    .rows
                    .entry((chain.to_owned(), *table, kind))
                    .or_default();
                *total = total.saturating_add(*rows);
            }
        }
        pending.latest.insert(chain.to_owned(), summary.clone());
    }

    pub(super) fn take(&self) -> Pending {
        std::mem::take(&mut *lock(&self.inner))
    }
}

#[derive(Clone)]
pub(super) struct ProjectWriteGauges {
    scope_keys: IntGaugeVec,
    changed_events: IntGaugeVec,
    staged_events: IntGaugeVec,
    batch_blocks: IntGaugeVec,
    rows_written: IntCounterVec,
    stage_duration: GaugeVec,
}

impl ProjectWriteGauges {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            scope_keys: registry.int_gauge_vec(
                "phase_runner_project_scope_keys",
                "Keys in each Project scope of the newest committed batch: the names, children, \
                 resources, account permissions, resolvers and primary names it rebuilt.",
                &["chain", "scope"],
            )?,
            changed_events: registry.int_gauge_vec(
                "phase_runner_project_changed_events",
                "Events in the affected blocks of the newest committed Project batch; 0 for a \
                 full rebuild.",
                &["chain"],
            )?,
            staged_events: registry.int_gauge_vec(
                "phase_runner_project_staged_events",
                "Events the newest committed Project batch staged for its builders.",
                &["chain"],
            )?,
            batch_blocks: registry.int_gauge_vec(
                "phase_runner_project_batch_blocks",
                "Blocks in the affected range of the newest committed Project batch.",
                &["chain"],
            )?,
            rows_written: registry.int_counter_vec(
                "phase_runner_project_rows_written_total",
                "Rows Project batches inserted into or deleted from each served table.",
                &["chain", "table", "kind"],
            )?,
            stage_duration: registry.gauge_vec(
                "phase_runner_project_stage_duration_seconds",
                "Elapsed time of each derivation stage in the newest committed Project batch.",
                &["chain", "stage"],
            )?,
        })
    }

    pub(super) fn apply(&self, pending: Pending) {
        for ((chain, table, kind), rows) in pending.rows {
            self.rows_written
                .with_label_values(&[&chain, table, kind])
                .inc_by(rows);
        }
        for (chain, summary) in pending.latest {
            let chain = chain.as_str();
            for (scope, keys) in &summary.scope_keys {
                self.scope_keys
                    .with_label_values(&[chain, scope])
                    .set(gauge_value(*keys));
            }
            self.changed_events
                .with_label_values(&[chain])
                .set(gauge_value(summary.changed_events));
            self.staged_events
                .with_label_values(&[chain])
                .set(gauge_value(summary.staged_events));
            self.batch_blocks
                .with_label_values(&[chain])
                .set(gauge_value(summary.blocks));
            for (stage, elapsed_ms) in &summary.stage_elapsed_ms {
                self.stage_duration
                    .with_label_values(&[chain, stage])
                    .set(*elapsed_ms as f64 / 1_000.0);
            }
        }
    }
}

fn gauge_value(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
