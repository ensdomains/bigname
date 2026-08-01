use std::time::Duration;

use anyhow::Result;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{info, warn};

use super::{
    CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS, CatchupIterationStatus, NormalizedReplayCatchupConfig,
    ProjectionIndexCoordination, admission,
};
use crate::{
    backfill::{CoinbaseSqlBackfillConfig, CoinbaseSqlSourceRegistry},
    provider::ChainProvider,
    reconciliation::HeaderAuditMode,
    run::startup_heartbeat::{NormalizedReplayHeartbeat, RequiredSubtaskActivity},
};

#[expect(clippy::too_many_arguments)]
pub(crate) async fn run_normalized_replay_catchup_chain(
    pool: PgPool,
    config: NormalizedReplayCatchupConfig,
    chain: String,
    provider: Option<ChainProvider>,
    coinbase_sql_recovery: Option<(CoinbaseSqlSourceRegistry, CoinbaseSqlBackfillConfig)>,
    header_audit_mode: HeaderAuditMode,
    projection_index_coordination: ProjectionIndexCoordination,
    heartbeat: NormalizedReplayHeartbeat,
    activity: RequiredSubtaskActivity,
) -> Result<()> {
    info!(
        service = "indexer",
        command = "run",
        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
        deployment_profile = %config.deployment_profile,
        chain,
        chunk_blocks = config.chunk_blocks,
        max_raw_logs_per_chunk = config.max_raw_logs_per_chunk,
        poll_interval_secs = config.poll_interval_secs,
        defer_projection_indexes = config.defer_projection_indexes,
        "automatic normalized-event replay catch-up chain lane started"
    );

    loop {
        let mut progress = heartbeat.clone();
        let status = admission::run_required_normalized_replay_catchup_iteration(
            &pool,
            &config,
            &chain,
            provider.as_ref(),
            coinbase_sql_recovery
                .as_ref()
                .map(|(registry, config)| (registry, config)),
            header_audit_mode,
            &projection_index_coordination,
            &mut progress,
            &activity,
        )
        .await;
        let progressed = match status {
            Ok(status) => {
                if let Err(error) =
                    crate::StartupAdapterProgress::record(&mut progress, &pool).await
                {
                    warn!(
                        service = "indexer",
                        command = "run",
                        replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
                        chain,
                        error = ?error,
                        "failed to record successful normalized-event replay catch-up iteration"
                    );
                    false
                } else {
                    status == CatchupIterationStatus::Progressed
                }
            }
            Err(error) if admission::is_fatal_replay_fence(&error) => return Err(error),
            Err(error) => {
                warn!(
                    service = "indexer",
                    command = "run",
                    replay_cursor_kind = CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS,
                    chain,
                    error = ?error,
                    "automatic normalized-event replay catch-up iteration failed"
                );
                false
            }
        };

        if !progressed {
            sleep(Duration::from_secs(config.poll_interval_secs)).await;
        }
    }
}
