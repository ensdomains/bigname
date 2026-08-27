use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, ensure};
use bigname_metrics::{BuildInfo, IntGauge, IntGaugeVec, MetricsRegistry, MetricsServer};
use sqlx::{FromRow, PgPool};
use tokio_util::sync::CancellationToken;

use crate::progress_monitor::RunnerPhaseProgress;

const REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const PHASE_STATUSES: [&str; 5] = ["idle", "running", "paused", "completed", "failed"];
const VERIFICATION_LEVELS: [&str; 3] = ["quick_synced", "cross_checked", "node_checked"];
const REDO_MODES: [&str; 2] = ["redo", "recompute_flags"];

#[derive(Clone, Default)]
pub struct RunnerLoopHeartbeat {
    last_progress: Arc<Mutex<BTreeMap<String, Instant>>>,
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

    fn ages_seconds(&self) -> BTreeMap<String, i64> {
        let last_progress = self
            .last_progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        last_progress
            .iter()
            .map(|(chain, instant)| (chain.clone(), elapsed_seconds(instant)))
            .collect()
    }
}

fn elapsed_seconds(instant: &Instant) -> i64 {
    i64::try_from(instant.elapsed().as_secs()).unwrap_or(i64::MAX)
}

#[derive(Clone)]
struct PipelineMetrics {
    registry: MetricsRegistry,
    current_block: IntGaugeVec,
    target_block: IntGaugeVec,
    phase_status: IntGaugeVec,
    heartbeat_age_seconds: IntGaugeVec,
    loop_heartbeat_age_seconds: IntGaugeVec,
    verification_level: IntGaugeVec,
    redo_in_progress: IntGaugeVec,
    redo_mode: IntGaugeVec,
    redo_current_block: IntGaugeVec,
    redo_target_block: IntGaugeVec,
    reinterpretation_required: IntGaugeVec,
    chain_head_block: IntGaugeVec,
    head_lag_blocks: IntGaugeVec,
    batches_since_cursor_advance: IntGaugeVec,
    cursor_stall_age_seconds: IntGaugeVec,
    refresh_success: IntGauge,
    last_refresh_timestamp_seconds: IntGauge,
    loop_heartbeat: RunnerLoopHeartbeat,
    phase_progress: RunnerPhaseProgress,
    known: Arc<Mutex<KnownLabels>>,
}

#[derive(Default)]
struct KnownLabels {
    chains: BTreeSet<String>,
    phases: BTreeSet<(String, String)>,
    loop_chains: BTreeSet<String>,
}

#[derive(Clone, Debug, FromRow)]
struct PhaseMetricRow {
    chain_id: String,
    phase_name: String,
    phase_status: String,
    verification_level: Option<String>,
    current_block_number: Option<i64>,
    target_block_number: Option<i64>,
    input_content_hash: Option<String>,
    redo_in_progress: bool,
    redo_mode: Option<String>,
    redo_current_block_number: Option<i64>,
    redo_target_block_number: Option<i64>,
    heartbeat_age_seconds: Option<i64>,
    chain_head_block_number: Option<i64>,
}

impl PipelineMetrics {
    fn new(
        heartbeat_stale_after_secs: i64,
        loop_heartbeat: RunnerLoopHeartbeat,
        phase_progress: RunnerPhaseProgress,
    ) -> Result<Self> {
        ensure!(
            heartbeat_stale_after_secs > 0,
            "heartbeat stale threshold must be positive"
        );
        let registry = MetricsRegistry::new(BuildInfo {
            build_sha: crate::BUILD_SHA,
            interpreter_content_hash: crate::INTERPRETER_CONTENT_HASH,
        })?;
        let process_start_timestamp_milliseconds = registry.int_gauge(
            "phase_runner_process_start_timestamp_milliseconds",
            "Unix timestamp in milliseconds when this phase-runner process started.",
        )?;
        process_start_timestamp_milliseconds.set(unix_timestamp_milliseconds()?);
        let current_block = registry.int_gauge_vec(
            "phase_runner_phase_current_block",
            "Latest block processed by a phase, or -1 when no position has been recorded.",
            &["chain", "phase"],
        )?;
        let target_block = registry.int_gauge_vec(
            "phase_runner_phase_target_block",
            "Current target block for a phase, or -1 when no target has been recorded.",
            &["chain", "phase"],
        )?;
        let phase_status = registry.int_gauge_vec(
            "phase_runner_phase_status",
            "Phase lifecycle state as a one-hot gauge.",
            &["chain", "phase", "status"],
        )?;
        let heartbeat_age_seconds = registry.int_gauge_vec(
            "phase_runner_heartbeat_age_seconds",
            "Seconds since the newest runner heartbeat, or -1 when no heartbeat exists.",
            &["chain", "phase"],
        )?;
        let heartbeat_stale_threshold_seconds = registry.int_gauge_vec(
            "phase_runner_heartbeat_stale_threshold_seconds",
            "Configured heartbeat-age paging threshold in seconds.",
            &["threshold_seconds"],
        )?;
        let threshold_label = heartbeat_stale_after_secs.to_string();
        heartbeat_stale_threshold_seconds
            .with_label_values(&[&threshold_label])
            .set(heartbeat_stale_after_secs);
        let loop_heartbeat_age_seconds = registry.int_gauge_vec(
            "phase_runner_loop_heartbeat_age_seconds",
            "Seconds since the runner loop for a configured chain last made observable progress.",
            &["chain"],
        )?;
        let verification_level = registry.int_gauge_vec(
            "phase_runner_verification_level",
            "Stored verification level as a one-hot gauge.",
            &["chain", "level"],
        )?;
        let redo_in_progress = registry.int_gauge_vec(
            "phase_runner_redo_in_progress",
            "Whether a phase has an unfinished repair run.",
            &["chain", "phase"],
        )?;
        let redo_mode = registry.int_gauge_vec(
            "phase_runner_redo_mode",
            "Active repair mode as a one-hot gauge.",
            &["chain", "phase", "mode"],
        )?;
        let redo_current_block = registry.int_gauge_vec(
            "phase_runner_redo_current_block",
            "Latest block processed by an active repair run, or -1 when absent.",
            &["chain", "phase"],
        )?;
        let redo_target_block = registry.int_gauge_vec(
            "phase_runner_redo_target_block",
            "Target block for an active repair run, or -1 when absent.",
            &["chain", "phase"],
        )?;
        let reinterpretation_required = registry.int_gauge_vec(
            "phase_runner_reinterpretation_required",
            "Whether stored Interpret output uses a different interpreter content hash.",
            &["chain"],
        )?;
        let chain_head_block = registry.int_gauge_vec(
            "phase_runner_chain_head_block",
            "Latest published chain-head block, or -1 when no head exists.",
            &["chain"],
        )?;
        let head_lag_blocks = registry.int_gauge_vec(
            "phase_runner_head_lag_blocks",
            "Observed provider target minus processed phase progress in blocks, or -1 when unavailable.",
            &["chain", "phase"],
        )?;
        let batches_since_cursor_advance = registry.int_gauge_vec(
            "phase_runner_phase_batches_since_cursor_advance",
            "Consecutive successful work-bearing phase batch commits confirmed not to have changed the durable composite cursor.",
            &["chain", "phase", "mode"],
        )?;
        let cursor_stall_age_seconds = registry.int_gauge_vec(
            "phase_runner_phase_cursor_stall_age_seconds",
            "Seconds since the first confirmed unchanged-cursor batch commit in the current consecutive sequence, or zero when no sequence is active.",
            &["chain", "phase", "mode"],
        )?;
        let refresh_success = registry.int_gauge(
            "phase_runner_metrics_refresh_success",
            "Whether the latest database refresh succeeded.",
        )?;
        let last_refresh_timestamp_seconds = registry.int_gauge(
            "phase_runner_metrics_last_refresh_timestamp_seconds",
            "Unix timestamp of the latest successful database refresh.",
        )?;
        Ok(Self {
            registry,
            current_block,
            target_block,
            phase_status,
            heartbeat_age_seconds,
            loop_heartbeat_age_seconds,
            verification_level,
            redo_in_progress,
            redo_mode,
            redo_current_block,
            redo_target_block,
            reinterpretation_required,
            chain_head_block,
            head_lag_blocks,
            batches_since_cursor_advance,
            cursor_stall_age_seconds,
            refresh_success,
            last_refresh_timestamp_seconds,
            loop_heartbeat,
            phase_progress,
            known: Arc::new(Mutex::new(KnownLabels::default())),
        })
    }

    async fn refresh(&self, pool: &PgPool) -> Result<()> {
        self.apply_phase_progress();
        let rows = match load_rows(pool).await {
            Ok(rows) => rows,
            Err(error) => {
                self.refresh_success.set(0);
                return Err(error);
            }
        };
        if let Err(error) = self.apply_rows(&rows) {
            self.refresh_success.set(0);
            return Err(error);
        }
        let timestamp = match unix_timestamp() {
            Ok(timestamp) => timestamp,
            Err(error) => {
                self.refresh_success.set(0);
                return Err(error);
            }
        };
        self.last_refresh_timestamp_seconds.set(timestamp);
        self.refresh_success.set(1);
        Ok(())
    }

    fn apply_phase_progress(&self) {
        for sample in self.phase_progress.snapshot() {
            let labels = &[sample.chain.as_str(), sample.phase.as_str(), sample.mode];
            self.batches_since_cursor_advance
                .with_label_values(labels)
                .set(sample.batches);
            self.cursor_stall_age_seconds
                .with_label_values(labels)
                .set(sample.age_seconds);
        }
    }

    fn apply_rows(&self, rows: &[PhaseMetricRow]) -> Result<()> {
        let mut next = KnownLabels::default();
        for row in rows {
            validate_row(row)?;
            next.chains.insert(row.chain_id.clone());
            next.phases
                .insert((row.chain_id.clone(), row.phase_name.clone()));
            self.apply_row(row);
        }
        for (chain, age) in self.loop_heartbeat.ages_seconds() {
            self.loop_heartbeat_age_seconds
                .with_label_values(&[&chain])
                .set(age);
            next.loop_chains.insert(chain);
        }

        let mut known = self
            .known
            .lock()
            .map_err(|_| anyhow::anyhow!("phase metric label set lock was poisoned"))?;
        for (chain, phase) in known.phases.difference(&next.phases) {
            self.remove_phase(chain, phase);
        }
        for chain in known.chains.difference(&next.chains) {
            self.remove_chain(chain);
        }
        for chain in known.loop_chains.difference(&next.loop_chains) {
            let _ = self
                .loop_heartbeat_age_seconds
                .remove_label_values(&[chain]);
        }
        *known = next;
        Ok(())
    }

    fn apply_row(&self, row: &PhaseMetricRow) {
        let phase_labels = &[row.chain_id.as_str(), row.phase_name.as_str()];
        self.current_block
            .with_label_values(phase_labels)
            .set(row.current_block_number.unwrap_or(-1));
        self.target_block
            .with_label_values(phase_labels)
            .set(row.target_block_number.unwrap_or(-1));
        self.heartbeat_age_seconds
            .with_label_values(phase_labels)
            .set(row.heartbeat_age_seconds.unwrap_or(-1));
        self.redo_in_progress
            .with_label_values(phase_labels)
            .set(i64::from(row.redo_in_progress));
        self.redo_current_block
            .with_label_values(phase_labels)
            .set(row.redo_current_block_number.unwrap_or(-1));
        self.redo_target_block
            .with_label_values(phase_labels)
            .set(row.redo_target_block_number.unwrap_or(-1));
        self.head_lag_blocks
            .with_label_values(phase_labels)
            .set(head_lag(row));

        for status in PHASE_STATUSES {
            self.phase_status
                .with_label_values(&[row.chain_id.as_str(), row.phase_name.as_str(), status])
                .set(i64::from(row.phase_status == status));
        }
        for mode in REDO_MODES {
            self.redo_mode
                .with_label_values(&[row.chain_id.as_str(), row.phase_name.as_str(), mode])
                .set(i64::from(
                    row.redo_in_progress && row.redo_mode.as_deref() == Some(mode),
                ));
        }
        if row.phase_name == "verify" {
            for level in VERIFICATION_LEVELS {
                self.verification_level
                    .with_label_values(&[row.chain_id.as_str(), level])
                    .set(i64::from(row.verification_level.as_deref() == Some(level)));
            }
        }
        if row.phase_name == "interpret" {
            let required = row.phase_status != "idle"
                && row.input_content_hash.as_deref() != Some(crate::INTERPRETER_CONTENT_HASH);
            self.reinterpretation_required
                .with_label_values(&[&row.chain_id])
                .set(i64::from(required));
        }
        self.chain_head_block
            .with_label_values(&[&row.chain_id])
            .set(row.chain_head_block_number.unwrap_or(-1));
    }

    fn remove_phase(&self, chain: &str, phase: &str) {
        for metric in [
            &self.current_block,
            &self.target_block,
            &self.heartbeat_age_seconds,
            &self.redo_in_progress,
            &self.redo_current_block,
            &self.redo_target_block,
            &self.head_lag_blocks,
        ] {
            let _ = metric.remove_label_values(&[chain, phase]);
        }
        for status in PHASE_STATUSES {
            let _ = self
                .phase_status
                .remove_label_values(&[chain, phase, status]);
        }
        for mode in REDO_MODES {
            let _ = self.redo_mode.remove_label_values(&[chain, phase, mode]);
        }
    }

    fn remove_chain(&self, chain: &str) {
        let _ = self.chain_head_block.remove_label_values(&[chain]);
        let _ = self.reinterpretation_required.remove_label_values(&[chain]);
        for level in VERIFICATION_LEVELS {
            let _ = self.verification_level.remove_label_values(&[chain, level]);
        }
    }
}

pub async fn start(
    bind_addr: SocketAddr,
    pool: PgPool,
    cancellation: CancellationToken,
    heartbeat_stale_after_secs: i64,
    loop_heartbeat: RunnerLoopHeartbeat,
    phase_progress: RunnerPhaseProgress,
) -> Result<SocketAddr> {
    let metrics = PipelineMetrics::new(heartbeat_stale_after_secs, loop_heartbeat, phase_progress)?;
    metrics.refresh(&pool).await?;
    let server = MetricsServer::bind(bind_addr, metrics.registry.clone()).await?;
    let local_addr = server.local_addr()?;
    let server_cancellation = cancellation.clone();
    tokio::spawn(async move {
        tokio::select! {
            result = server.serve() => {
                if let Err(error) = result {
                    tracing::error!(error = %format!("{error:#}"), "phase metrics listener exited");
                }
            }
            () = server_cancellation.cancelled() => {}
        }
    });
    tokio::spawn(refresh_loop(metrics, pool, cancellation));
    Ok(local_addr)
}

async fn refresh_loop(metrics: PipelineMetrics, pool: PgPool, cancellation: CancellationToken) {
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return,
            () = tokio::time::sleep(REFRESH_INTERVAL) => {}
        }
        if let Err(error) = metrics.refresh(&pool).await {
            tracing::error!(error = %format!("{error:#}"), "phase metrics refresh failed");
        }
    }
}

async fn load_rows(pool: &PgPool) -> Result<Vec<PhaseMetricRow>> {
    sqlx::query_as(
        "WITH latest_heartbeats AS (
             SELECT chain_id, phase_name, MAX(heartbeat_at) AS heartbeat_at
             FROM service_heartbeats
             WHERE service_name = 'phase-runner'
             GROUP BY chain_id, phase_name
         )
         SELECT phase.chain_id,
                phase.phase_name,
                phase.phase_status,
                phase.verification_level,
                phase.current_block_number,
                phase.target_block_number,
                phase.input_content_hash,
                phase.redo_in_progress,
                phase.redo_mode,
                phase.redo_current_block_number,
                phase.redo_target_block_number,
                CASE
                    WHEN heartbeat.heartbeat_at IS NULL THEN NULL
                    ELSE FLOOR(EXTRACT(EPOCH FROM GREATEST(
                        clock_timestamp() - heartbeat.heartbeat_at,
                        interval '0 seconds'
                    )))::bigint
                END AS heartbeat_age_seconds,
                head.latest_block_number AS chain_head_block_number
         FROM chain_phase_state phase
         LEFT JOIN latest_heartbeats heartbeat
           ON heartbeat.chain_id = phase.chain_id
          AND heartbeat.phase_name = phase.phase_name
         LEFT JOIN chain_heads head ON head.chain_id = phase.chain_id
         ORDER BY phase.chain_id, phase.phase_name",
    )
    .fetch_all(pool)
    .await
    .context("failed to read phase metrics state")
}

fn validate_row(row: &PhaseMetricRow) -> Result<()> {
    ensure!(
        PHASE_STATUSES.contains(&row.phase_status.as_str()),
        "unknown phase status {:?}",
        row.phase_status
    );
    ensure!(
        row.verification_level
            .as_deref()
            .is_none_or(|level| VERIFICATION_LEVELS.contains(&level)),
        "unknown verification level {:?}",
        row.verification_level
    );
    ensure!(
        row.redo_mode
            .as_deref()
            .is_none_or(|mode| REDO_MODES.contains(&mode)),
        "unknown repair mode {:?}",
        row.redo_mode
    );
    Ok(())
}

fn head_lag(row: &PhaseMetricRow) -> i64 {
    match (row.target_block_number, row.current_block_number) {
        (Some(target), Some(current)) => target.saturating_sub(current).max(0),
        _ => -1,
    }
}

fn unix_timestamp() -> Result<i64> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs();
    i64::try_from(seconds).context("Unix timestamp does not fit in an i64")
}

fn unix_timestamp_milliseconds() -> Result<i64> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_millis();
    i64::try_from(milliseconds).context("Unix timestamp in milliseconds does not fit in an i64")
}

#[cfg(test)]
#[path = "metrics/tests.rs"]
mod tests;
