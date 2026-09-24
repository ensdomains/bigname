use std::sync::Arc;

use anyhow::{Context, Result};
use bigname_metrics::{IntGaugeVec, MetricsRegistry};
use sqlx::{FromRow, PgPool};
use tokio::sync::Notify;

/// Tells the metrics task that a batch committed, so the served-lag gauges move at
/// block boundaries instead of waiting for the next refresh tick. The metrics task
/// answers with the same query the periodic refresh runs, one query at a time, so
/// an older result can never overwrite a newer one.
#[derive(Clone, Default)]
pub struct RunnerMetricsFeed {
    committed: Arc<Notify>,
}

impl RunnerMetricsFeed {
    pub fn batch_committed(&self) {
        self.committed.notify_one();
    }

    pub(super) async fn committed(&self) {
        self.committed.notified().await;
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
}

impl ServedLagGauges {
    pub(super) fn new(registry: &MetricsRegistry) -> Result<Self> {
        Ok(Self {
            lag_blocks: registry.int_gauge_vec(
                "phase_runner_served_lag_blocks",
                "Newest observed execution-client head minus the block of the Project \
                 publication the API can serve, or -1 when either is unavailable.",
                &["chain"],
            )?,
            publication_block: registry.int_gauge_vec(
                "phase_runner_served_publication_block",
                "Block of the Project publication the API can serve, or -1 when none is servable.",
                &["chain"],
            )?,
        })
    }

    pub(super) fn apply(&self, rows: &[ServedLagRow]) {
        for row in rows {
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

    pub(super) fn remove_chain(&self, chain: &str) {
        let _ = self.lag_blocks.remove_label_values(&[chain]);
        let _ = self.publication_block.remove_label_values(&[chain]);
    }
}

pub(super) fn served_lag(observed_head: Option<i64>, publication: Option<i64>) -> i64 {
    match (observed_head, publication) {
        (Some(head), Some(publication)) => head.saturating_sub(publication).max(0),
        _ => -1,
    }
}

/// The observed head is the newer of the head the latest Live batch saw at the
/// execution client (the Live row's target) and the published chain head, which
/// Ingest moves while it catches up before Live runs. The publication repeats the
/// conditions of `load_current_project_publication` in `bigname-storage`; it leaves
/// out the API's one-block lag tolerance, so large lags show, and the Interpret-redo
/// refusal some routes add.
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
