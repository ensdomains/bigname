use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use sqlx::PgPool;
use sqlx::types::time::OffsetDateTime;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::reconciliation::{
    RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
    replay_raw_fact_normalized_events,
};

#[path = "normalized_replay_catchup/cursors.rs"]
mod cursors;
#[path = "normalized_replay_catchup/indexes.rs"]
mod indexes;
#[path = "normalized_replay_catchup/sources.rs"]
mod sources;

use cursors::{
    advance_cursor, ensure_cursor, record_cursor_failure,
    rewind_cursor_for_newly_observed_older_logs,
};
use indexes::{
    ensure_projection_indexes_after_catchup, prepare_deferred_projection_indexes_for_fresh_replay,
};
use sources::{load_canonical_raw_log_bounds, select_log_bounded_replay_to_block};

pub(crate) const DEFAULT_NORMALIZED_REPLAY_CATCHUP_CHUNK_BLOCKS: i64 = 262_144;
pub(crate) const DEFAULT_NORMALIZED_REPLAY_CATCHUP_MAX_LOGS_PER_CHUNK: usize = 100_000;
pub(crate) const DEFAULT_NORMALIZED_REPLAY_CATCHUP_POLL_INTERVAL_SECS: u64 = 5;
pub(crate) const DEFAULT_NORMALIZED_REPLAY_DEFER_PROJECTION_INDEXES: bool = true;

const CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS: &str = "raw_fact_normalized_events";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedReplayCatchupConfig {
    pub(crate) deployment_profile: String,
    pub(crate) chains: Vec<String>,
    pub(crate) chunk_blocks: i64,
    pub(crate) max_raw_logs_per_chunk: usize,
    pub(crate) poll_interval_secs: u64,
    pub(crate) defer_projection_indexes: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatchupIterationStatus {
    Progressed,
    Idle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RawLogBounds {
    start_block: i64,
    target_block: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NormalizedReplayCursor {
    range_start_block_number: i64,
    next_block_number: i64,
    target_block_number: i64,
    last_replayed_at: Option<OffsetDateTime>,
}

impl NormalizedReplayCatchupConfig {
    pub(crate) fn new(
        deployment_profile: String,
        chains: impl IntoIterator<Item = String>,
        chunk_blocks: i64,
        max_raw_logs_per_chunk: usize,
        poll_interval_secs: u64,
    ) -> Result<Self> {
        if deployment_profile.trim().is_empty() {
            bail!("normalized replay catch-up deployment_profile must not be empty");
        }
        if chunk_blocks <= 0 {
            bail!("normalized replay catch-up chunk blocks must be positive, got {chunk_blocks}");
        }
        if max_raw_logs_per_chunk == 0 {
            bail!("normalized replay catch-up max logs per chunk must be positive");
        }
        if poll_interval_secs == 0 {
            bail!("normalized replay catch-up poll interval must be positive");
        }

        let chains = chains
            .into_iter()
            .filter(|chain| !chain.trim().is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        Ok(Self {
            deployment_profile,
            chains,
            chunk_blocks,
            max_raw_logs_per_chunk,
            poll_interval_secs,
            defer_projection_indexes: DEFAULT_NORMALIZED_REPLAY_DEFER_PROJECTION_INDEXES,
        })
    }

    pub(crate) fn with_defer_projection_indexes(mut self, defer_projection_indexes: bool) -> Self {
        self.defer_projection_indexes = defer_projection_indexes;
        self
    }
}

pub(crate) async fn run_normalized_replay_catchup(
    pool: PgPool,
    config: NormalizedReplayCatchupConfig,
) -> Result<()> {
    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        deployment_profile = %config.deployment_profile,
        chain_count = config.chains.len(),
        chunk_blocks = config.chunk_blocks,
        max_raw_logs_per_chunk = config.max_raw_logs_per_chunk,
        poll_interval_secs = config.poll_interval_secs,
        defer_projection_indexes = config.defer_projection_indexes,
        "automatic normalized-event replay catch-up started"
    );

    loop {
        let mut progressed = false;
        for chain in &config.chains {
            match run_normalized_replay_catchup_iteration(&pool, &config, chain).await {
                Ok(CatchupIterationStatus::Progressed) => {
                    progressed = true;
                }
                Ok(CatchupIterationStatus::Idle) => {}
                Err(error) => {
                    record_cursor_failure(&pool, &config.deployment_profile, chain, &error).await?;
                    warn!(
                        service = "indexer",
                        command = "run",
                        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
                        chain,
                        error = ?error,
                        "automatic normalized-event replay catch-up iteration failed"
                    );
                }
            }
        }

        if !progressed {
            sleep(Duration::from_secs(config.poll_interval_secs)).await;
        }
    }
}

pub(crate) async fn run_normalized_replay_catchup_iteration(
    pool: &PgPool,
    config: &NormalizedReplayCatchupConfig,
    chain: &str,
) -> Result<CatchupIterationStatus> {
    let Some(bounds) = load_canonical_raw_log_bounds(pool, chain).await? else {
        if config.defer_projection_indexes {
            ensure_projection_indexes_after_catchup(
                pool,
                &config.deployment_profile,
                &config.chains,
            )
            .await?;
        }
        return Ok(CatchupIterationStatus::Idle);
    };
    let cursor = ensure_cursor(
        pool,
        &config.deployment_profile,
        chain,
        CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        bounds,
    )
    .await?;
    let cursor = rewind_cursor_for_newly_observed_older_logs(
        pool,
        &config.deployment_profile,
        chain,
        cursor,
    )
    .await?;
    if config.defer_projection_indexes {
        prepare_deferred_projection_indexes_for_fresh_replay(pool, &cursor).await?;
    }
    if cursor.next_block_number > cursor.target_block_number {
        if config.defer_projection_indexes {
            ensure_projection_indexes_after_catchup(
                pool,
                &config.deployment_profile,
                &config.chains,
            )
            .await?;
        }
        return Ok(CatchupIterationStatus::Idle);
    }

    let from_block = cursor.next_block_number;
    let hard_to_block = from_block
        .checked_add(config.chunk_blocks - 1)
        .unwrap_or(cursor.target_block_number)
        .min(cursor.target_block_number);
    let to_block = select_log_bounded_replay_to_block(
        pool,
        chain,
        from_block,
        hard_to_block,
        config.max_raw_logs_per_chunk,
    )
    .await?;
    let started = Instant::now();
    let outcome = replay_raw_fact_normalized_events(
        pool,
        RawFactNormalizedEventReplayRequest {
            deployment_profile: config.deployment_profile.clone(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block,
                to_block,
            },
        },
    )
    .await?;

    advance_cursor(
        pool,
        &config.deployment_profile,
        chain,
        CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        bounds.target_block,
        to_block,
        &outcome,
    )
    .await?;
    if config.defer_projection_indexes {
        ensure_projection_indexes_after_catchup(pool, &config.deployment_profile, &config.chains)
            .await?;
    }

    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        chain,
        from_block,
        to_block,
        target_block = bounds.target_block,
        max_raw_logs_per_chunk = config.max_raw_logs_per_chunk,
        selected_block_count = outcome.selected_block_count,
        canonical_raw_log_count = outcome.canonical_raw_log_count,
        scanned_raw_log_count = outcome.scanned_raw_log_count,
        matched_raw_log_count = outcome.matched_raw_log_count,
        normalized_event_synced_count = outcome.normalized_event_synced_count,
        normalized_event_inserted_count = outcome.normalized_event_inserted_count,
        elapsed_ms = started.elapsed().as_millis(),
        "automatic normalized-event replay catch-up chunk completed"
    );

    Ok(CatchupIterationStatus::Progressed)
}
