use std::{future::Future, time::Duration};

use anyhow::Result;
use bigname_adapters::StartupAdapterVersion;
use bigname_storage::{
    StartupAdapterSyncCompletion, StartupAdapterSyncDecision, StartupAdapterSyncKey,
    acquire_raw_log_staging_read_guard, complete_startup_adapter_sync,
    prepare_startup_adapter_sync,
};
use tracing::{info, warn};

use crate::run::startup_heartbeat::StartupHeartbeat;

#[cfg(not(test))]
const LIVE_ADAPTER_HEARTBEAT_TICK: Duration = Duration::from_secs(1);
#[cfg(test)]
const LIVE_ADAPTER_HEARTBEAT_TICK: Duration = Duration::from_millis(10);

pub(crate) struct StartupFamilySyncAttempt {
    deployment_profile: Option<String>,
    started_key: Option<StartupAdapterSyncKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupFamilySyncCompletion {
    Stable,
    Retry,
}

impl StartupFamilySyncAttempt {
    pub(crate) fn scanned_lineage_extent_block_number(&self) -> Option<i64> {
        self.started_key.as_ref().map(|key| {
            key.canonical_lineage_head
                .as_ref()
                .map_or(0, |head| head.block_number)
        })
    }

    pub(crate) async fn complete_or_retry(
        self,
        pool: &sqlx::PgPool,
        chain: &str,
        adapter: StartupAdapterVersion,
    ) -> Result<StartupFamilySyncCompletion> {
        let Some(deployment_profile) = self.deployment_profile else {
            return Ok(StartupFamilySyncCompletion::Stable);
        };
        match complete_startup_adapter_sync(
            pool,
            &deployment_profile,
            chain,
            adapter.adapter,
            adapter.semantic_version,
            self.started_key,
        )
        .await?
        {
            StartupAdapterSyncCompletion::Completed => {
                info!(
                    service = "indexer",
                    command = "startup-adapter-sync",
                    deployment_profile,
                    chain,
                    adapter = adapter.adapter,
                    adapter_semantic_version = adapter.semantic_version,
                    "startup adapter family completion checkpoint published"
                );
                Ok(StartupFamilySyncCompletion::Stable)
            }
            StartupAdapterSyncCompletion::KeyUnknown => {
                warn!(
                    service = "indexer",
                    command = "startup-adapter-sync",
                    deployment_profile,
                    chain,
                    adapter = adapter.adapter,
                    adapter_semantic_version = adapter.semantic_version,
                    "startup adapter family completed with an unknown checkpoint key; \
                     the next boot will run the full sync again"
                );
                Ok(StartupFamilySyncCompletion::Stable)
            }
            StartupAdapterSyncCompletion::InputChanged => {
                warn!(
                    service = "indexer",
                    command = "startup-adapter-sync",
                    deployment_profile,
                    chain,
                    adapter = adapter.adapter,
                    adapter_semantic_version = adapter.semantic_version,
                    "startup adapter input advanced during the pass; the retained checkpoint was \
                     invalidated and another full pass is required"
                );
                Ok(StartupFamilySyncCompletion::Retry)
            }
        }
    }
}

pub(crate) async fn prepare_startup_family_sync(
    pool: &sqlx::PgPool,
    deployment_profile: Option<&str>,
    chain: &str,
    adapter: StartupAdapterVersion,
) -> Result<Option<StartupFamilySyncAttempt>> {
    let Some(deployment_profile) = deployment_profile else {
        return Ok(Some(StartupFamilySyncAttempt {
            deployment_profile: None,
            started_key: None,
        }));
    };
    match prepare_startup_adapter_sync(
        pool,
        deployment_profile,
        chain,
        adapter.adapter,
        adapter.semantic_version,
    )
    .await?
    {
        StartupAdapterSyncDecision::ReuseCompleted => {
            info!(
                service = "indexer",
                command = "startup-adapter-sync",
                deployment_profile,
                chain,
                adapter = adapter.adapter,
                adapter_semantic_version = adapter.semantic_version,
                "startup adapter family full scan skipped after checkpoint verification"
            );
            Ok(None)
        }
        StartupAdapterSyncDecision::RunFullSync { started_key } => {
            Ok(Some(StartupFamilySyncAttempt {
                deployment_profile: Some(deployment_profile.to_owned()),
                started_key,
            }))
        }
    }
}

pub(crate) async fn await_live_adapter_sync_with_heartbeat<T, F>(
    pool: &sqlx::PgPool,
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
    sync: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    tokio::pin!(sync);
    let first_tick = tokio::time::Instant::now() + LIVE_ADAPTER_HEARTBEAT_TICK;
    let mut interval = tokio::time::interval_at(first_tick, LIVE_ADAPTER_HEARTBEAT_TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            result = &mut sync => return result,
            _ = interval.tick() => heartbeat.record_if_due(pool, heartbeat_chain_ids).await?,
        }
    }
}

pub(crate) async fn await_live_full_source_adapter_with_heartbeat<T, F>(
    pool: &sqlx::PgPool,
    chain: &str,
    heartbeat: &mut StartupHeartbeat,
    heartbeat_chain_ids: &[String],
    sync: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    // Full-source discovery treats missing observations as removals. Keep the
    // same-chain raw corpus stable until reconciliation has published, so live
    // intake cannot commit a new source fact between the scan and source lock.
    let guard = acquire_raw_log_staging_read_guard(pool, chain).await?;
    let sync_result =
        await_live_adapter_sync_with_heartbeat(pool, heartbeat, heartbeat_chain_ids, sync).await;
    let release_result = guard.release().await;
    match (sync_result, release_result) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}
