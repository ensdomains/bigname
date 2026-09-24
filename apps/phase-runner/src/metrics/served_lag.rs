use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use bigname_metrics::{IntGaugeVec, MetricsRegistry};
use sqlx::{FromRow, PgPool};
use tokio::sync::Notify;

/// Tells the metrics task that a batch committed, so it refreshes the served-lag
/// gauges soon after the commit instead of at the next refresh tick. This is a
/// notification, not sampling of every block: commits that arrive together share
/// one refresh. The metrics task answers with the same query the periodic refresh
/// runs, one query at a time, so an older result can never overwrite a newer one.
#[derive(Clone, Default)]
pub struct RunnerMetricsFeed {
    committed: Arc<Notify>,
    configured_chains: Arc<Mutex<BTreeSet<String>>>,
    project_writes: super::project_writes::PendingProjectWrites,
}

impl RunnerMetricsFeed {
    /// Configured chains always export both gauges, as -1 when nothing is available.
    pub fn seed_chain(&self, chain: &str) {
        self.configured_chains
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(chain.to_owned());
    }

    /// Records what a Project batch wrote; the metrics task exports it with the next refresh.
    pub fn project_batch(&self, chain: &str, summary: &bigname_project::WriteSummary) {
        self.project_writes.record(chain, summary);
    }

    pub(super) fn take_project_writes(&self) -> super::project_writes::Pending {
        self.project_writes.take()
    }

    pub fn batch_committed(&self) {
        self.committed.notify_one();
    }

    /// Waits for the next `batch_committed`; a signal sent while nobody waits is kept.
    pub async fn committed(&self) {
        self.committed.notified().await;
    }

    pub(super) fn configured_chains(&self) -> Vec<String> {
        self.configured_chains
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug, FromRow)]
pub(super) struct ServedLagRow {
    pub(super) chain_id: String,
    pub(super) observed_head_block_number: Option<i64>,
    pub(super) publication_block_number: Option<i64>,
}

#[derive(Clone)]
pub(super) struct ServedLagGauges {
    lag_blocks: IntGaugeVec,
    publication_block: IntGaugeVec,
    incoherent: Arc<Mutex<BTreeMap<String, (i64, i64)>>>,
    configured: Arc<Mutex<BTreeSet<String>>>,
    exported: Arc<Mutex<BTreeSet<String>>>,
}

impl ServedLagGauges {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            lag_blocks: registry.int_gauge_vec(
                "phase_runner_served_lag_blocks",
                "Newest observed execution-client head minus the block of the newest readable \
                 Project publication, or -1 when either is unavailable.",
                &["chain"],
            )?,
            publication_block: registry.int_gauge_vec(
                "phase_runner_served_publication_block",
                "Block of the newest readable Project publication, or -1 when there is none.",
                &["chain"],
            )?,
            incoherent: Arc::default(),
            configured: Arc::default(),
            exported: Arc::default(),
        })
    }

    /// Seeds -1 for configured chains before the first refresh and keeps their series
    /// for the life of the process.
    pub(super) fn configure(&self, chains: &[String]) {
        *lock(&self.configured) = chains.iter().cloned().collect();
        self.apply(&[]);
    }

    /// Applies one complete query result. Configured chains missing from the result
    /// retain both gauges at -1. Previously exported chains that are neither
    /// configured nor returned are removed.
    pub(super) fn apply(&self, rows: &[ServedLagRow]) {
        let returned: BTreeSet<&str> = rows.iter().map(|row| row.chain_id.as_str()).collect();
        let configured = lock(&self.configured).clone();
        let mut exported = lock(&self.exported);
        for chain in exported.iter().chain(&configured) {
            if returned.contains(chain.as_str()) {
                continue;
            }
            self.incoherent_changed(chain, None);
            if configured.contains(chain) {
                self.lag_blocks.with_label_values(&[chain]).set(-1);
                self.publication_block.with_label_values(&[chain]).set(-1);
            } else {
                let _ = self.lag_blocks.remove_label_values(&[chain]);
                let _ = self.publication_block.remove_label_values(&[chain]);
            }
        }
        *exported = returned
            .iter()
            .map(|chain| (*chain).to_owned())
            .chain(configured)
            .collect();
        drop(exported);
        for row in rows {
            let incoherent = row
                .observed_head_block_number
                .zip(row.publication_block_number)
                .filter(|(head, publication)| head < publication);
            if self.incoherent_changed(&row.chain_id, incoherent)
                && let Some((observed_head, publication)) = incoherent
            {
                tracing::warn!(
                    chain_id = row.chain_id,
                    observed_head,
                    publication,
                    "observed head is below the Project publication; served lag reports -1"
                );
            }
            let chain = &[row.chain_id.as_str()];
            self.lag_blocks.with_label_values(chain).set(served_lag(
                row.observed_head_block_number,
                row.publication_block_number,
            ));
            self.publication_block
                .with_label_values(chain)
                .set(row.publication_block_number.unwrap_or(-1));
        }
    }

    /// True the first time a chain reports this incoherent pair.
    pub(super) fn incoherent_changed(&self, chain: &str, pair: Option<(i64, i64)>) -> bool {
        let mut incoherent = lock(&self.incoherent);
        match pair {
            Some(pair) => incoherent.insert(chain.to_owned(), pair) != Some(pair),
            None => {
                incoherent.remove(chain);
                false
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// -1 means unavailable, never caught up: either side is missing, or the observed
/// head is below the publication, which the retained head cannot explain.
pub(super) fn served_lag(observed_head: Option<i64>, publication: Option<i64>) -> i64 {
    match (observed_head, publication) {
        (Some(head), Some(publication)) if head >= publication => head - publication,
        _ => -1,
    }
}

/// The observed head is the newer of the head the latest Live batch saw at the
/// execution client (the Live row's target) and the published chain head, which
/// Ingest moves while it catches up before Live runs.
///
/// The publication is the one `load_served_project_generation` in `bigname-storage`
/// would accept, with one relaxation and two gates set aside:
/// - it drops only the upper one-block lag fence, so large lags show; the stored
///   head must still exist and must not be below the publication;
/// - it ignores the requested-position gate, which for a per-chain gauge would only
///   compare the publication with itself;
/// - it ignores the Interpret-redo gate: the redo gauges already show that state,
///   and this gauge measures Project publication eligibility only.
pub(super) async fn load(pool: &PgPool) -> Result<Vec<ServedLagRow>> {
    sqlx::query_as(
        "SELECT project.chain_id,
                GREATEST(live.target_block_number, head.latest_block_number)
                    AS observed_head_block_number,
                lineage.block_number AS publication_block_number
         FROM chain_phase_state project
         LEFT JOIN chain_phase_state live
           ON live.chain_id = project.chain_id
          AND live.phase_name = 'live'
         LEFT JOIN chain_heads head ON head.chain_id = project.chain_id
         LEFT JOIN chain_lineage lineage
           ON project.phase_status IN ('completed', 'running')
          AND project.input_content_hash = $1
          AND project.current_block_number <= head.latest_block_number
          AND lineage.chain_id = project.chain_id
          AND lineage.block_number = project.current_block_number
          AND lineage.block_hash = project.current_block_hash
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         WHERE project.phase_name = 'project'
         ORDER BY project.chain_id",
    )
    .bind(crate::INTERPRETER_CONTENT_HASH)
    .fetch_all(pool)
    .await
    .context("failed to read served-lag state")
}
