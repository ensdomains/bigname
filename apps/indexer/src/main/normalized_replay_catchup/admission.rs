use anyhow::Result;
use bigname_storage::projection_staging::wait_for_projection_replay_admission_retry as wait_for_admission_retry;
use sqlx::PgPool;

use super::{
    CatchupIterationStatus, NormalizedReplayCatchupConfig, cursors::record_cursor_failure,
    run_normalized_replay_catchup_iteration_with_provider,
};
use crate::{
    backfill::{CoinbaseSqlBackfillConfig, CoinbaseSqlSourceRegistry},
    provider::ChainProviderOps,
    reconciliation::HeaderAuditMode,
    run::startup_heartbeat::{NormalizedReplayHeartbeat, RequiredSubtaskActivity},
};

#[expect(clippy::too_many_arguments)]
pub(super) async fn run_required_normalized_replay_catchup_iteration(
    pool: &PgPool,
    config: &NormalizedReplayCatchupConfig,
    chain: &str,
    provider: Option<&(impl ChainProviderOps + ?Sized)>,
    coinbase_sql_recovery: Option<(&CoinbaseSqlSourceRegistry, &CoinbaseSqlBackfillConfig)>,
    header_audit_mode: HeaderAuditMode,
    progress: &mut NormalizedReplayHeartbeat,
    activity: &RequiredSubtaskActivity,
) -> Result<CatchupIterationStatus> {
    let _activity = activity.begin().await;
    let mut replay_admission_attempt = 1_usize;
    let result = loop {
        let result = run_normalized_replay_catchup_iteration_with_provider(
            pool,
            config,
            chain,
            provider,
            coinbase_sql_recovery,
            header_audit_mode,
            &mut Some(&mut *progress),
        )
        .await;
        let Err(error) = &result else {
            break result;
        };
        if !wait_for_admission_retry(error, replay_admission_attempt).await {
            break result;
        }
        replay_admission_attempt += 1;
    };
    if let Err(error) = &result {
        record_cursor_failure(pool, &config.deployment_profile, chain, error).await?;
    }
    result
}
