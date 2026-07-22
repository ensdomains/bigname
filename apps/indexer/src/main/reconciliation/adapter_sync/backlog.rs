use anyhow::{Context, Result};
use tracing::info;

use super::{
    sync_live_adapter_state_from_persisted_raw_payloads,
    sync_live_adapter_state_from_persisted_raw_payloads_with_progress,
};

#[path = "backlog/cursor.rs"]
mod cursor;
#[cfg(test)]
#[path = "backlog/test_hook.rs"]
mod test_hook;

use cursor::{
    BacklogCursorPreparation, BacklogCursorPublication, load_backlog_block_hash_page,
    prepare_backlog_cursor, publish_backlog_cursor_page, publish_empty_backlog_cursor_page,
};
pub(crate) use cursor::{BacklogHandoffStatus, validate_chain_handoff_while_guarded};
#[cfg(test)]
pub(crate) use test_hook::install_after_adapter_sync as install_backlog_after_adapter_sync_test_hook;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct LiveAdapterBacklogSyncSummary {
    pub(crate) chain_count: usize,
    pub(crate) awaiting_replay_chain_count: usize,
    pub(crate) selected_block_count: usize,
    pub(crate) scanned_log_count: usize,
    pub(crate) matched_log_count: usize,
    pub(crate) normalized_event_synced_count: usize,
    pub(crate) normalized_event_inserted_count: usize,
}

#[cfg(test)]
pub(crate) async fn sync_live_adapter_backlog_after_normalized_replay(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chains: &[String],
) -> Result<LiveAdapterBacklogSyncSummary> {
    sync_live_adapter_backlog_after_normalized_replay_inner(
        pool,
        deployment_profile,
        chains,
        &mut None,
    )
    .await
}

pub(crate) async fn sync_live_adapter_backlog_after_normalized_replay_with_progress(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chains: &[String],
    progress: &mut dyn bigname_adapters::StartupAdapterProgress,
) -> Result<LiveAdapterBacklogSyncSummary> {
    sync_live_adapter_backlog_after_normalized_replay_inner(
        pool,
        deployment_profile,
        chains,
        &mut Some(progress),
    )
    .await
}

async fn sync_live_adapter_backlog_after_normalized_replay_inner(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chains: &[String],
    progress: &mut Option<&mut dyn bigname_adapters::StartupAdapterProgress>,
) -> Result<LiveAdapterBacklogSyncSummary> {
    let mut aggregate = LiveAdapterBacklogSyncSummary::default();

    for chain in chains {
        let mut cursor = match prepare_backlog_cursor(pool, deployment_profile, chain).await? {
            BacklogCursorPreparation::Ready(cursor) => cursor,
            BacklogCursorPreparation::NoWork => continue,
            BacklogCursorPreparation::AwaitingReplay => {
                aggregate.awaiting_replay_chain_count += 1;
                continue;
            }
        };
        if cursor.next_block_number > cursor.target_block_number {
            continue;
        }

        aggregate.chain_count += 1;
        loop {
            let block_hashes = load_backlog_block_hash_page(
                pool,
                chain,
                cursor.next_block_number,
                cursor.target_block_number,
            )
            .await?;
            if block_hashes.is_empty() {
                match publish_empty_backlog_cursor_page(pool, deployment_profile, chain, cursor)
                    .await?
                {
                    BacklogCursorPublication::Advanced(next_cursor)
                    | BacklogCursorPublication::Retry(next_cursor) => {
                        cursor = next_cursor;
                        if cursor.next_block_number > cursor.target_block_number {
                            break;
                        }
                        continue;
                    }
                    BacklogCursorPublication::AwaitingReplay => {
                        aggregate.awaiting_replay_chain_count += 1;
                        break;
                    }
                }
            }

            let completed_to_block = block_hashes
                .last()
                .map(|block| block.block_number)
                .context("backlog block page unexpectedly empty")?;
            let hashes = block_hashes
                .into_iter()
                .map(|block| block.block_hash)
                .collect::<Vec<_>>();
            info!(
                service = "indexer",
                command = "poll",
                deployment_profile,
                chain,
                from_block = cursor.next_block_number,
                to_block = completed_to_block,
                target_block_number = cursor.target_block_number,
                block_hash_count = hashes.len(),
                "post-replay live raw payload adapter backlog page selected"
            );
            let summary = sync_backlog_page(
                pool,
                deployment_profile,
                chain,
                &hashes,
                progress,
            )
            .await
                .with_context(|| {
                    format!(
                        "failed to sync post-replay live adapter backlog for {deployment_profile}/{chain} through block {completed_to_block}"
                    )
                })?;

            #[cfg(test)]
            test_hook::pause_after_adapter_sync(pool, deployment_profile, chain).await;

            match publish_backlog_cursor_page(
                pool,
                deployment_profile,
                chain,
                cursor,
                completed_to_block,
                hashes.len(),
                &summary,
            )
            .await?
            {
                BacklogCursorPublication::Advanced(next_cursor) => {
                    cursor = next_cursor;
                    aggregate.selected_block_count += hashes.len();
                    aggregate.scanned_log_count += summary.scanned_log_count;
                    aggregate.matched_log_count += summary.matched_log_count;
                    aggregate.normalized_event_synced_count += summary.total_synced_count;
                    aggregate.normalized_event_inserted_count += summary.total_inserted_count;
                    record_backlog_progress(pool, progress).await?;
                }
                BacklogCursorPublication::Retry(next_cursor) => {
                    cursor = next_cursor;
                    continue;
                }
                BacklogCursorPublication::AwaitingReplay => {
                    aggregate.awaiting_replay_chain_count += 1;
                    break;
                }
            }
            if cursor.next_block_number > cursor.target_block_number {
                break;
            }
        }
    }

    if aggregate.selected_block_count > 0 {
        info!(
            service = "indexer",
            command = "poll",
            deployment_profile,
            chain_count = aggregate.chain_count,
            selected_block_count = aggregate.selected_block_count,
            scanned_log_count = aggregate.scanned_log_count,
            matched_log_count = aggregate.matched_log_count,
            normalized_event_synced_count = aggregate.normalized_event_synced_count,
            normalized_event_inserted_count = aggregate.normalized_event_inserted_count,
            "post-replay live raw payload adapter backlog synced"
        );
    }

    Ok(aggregate)
}

async fn sync_backlog_page(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    hashes: &[String],
    progress: &mut Option<&mut dyn bigname_adapters::StartupAdapterProgress>,
) -> Result<super::super::types::PersistedRawPayloadAdapterSyncSummary> {
    match progress.as_mut() {
        Some(progress) => {
            let mut page_progress =
                Some(&mut **progress as &mut dyn bigname_adapters::StartupAdapterProgress);
            sync_live_adapter_state_from_persisted_raw_payloads_with_progress(
                pool,
                deployment_profile,
                chain,
                hashes,
                &mut page_progress,
            )
            .await
        }
        None => {
            sync_live_adapter_state_from_persisted_raw_payloads(
                pool,
                deployment_profile,
                chain,
                hashes,
            )
            .await
        }
    }
}

async fn record_backlog_progress(
    pool: &sqlx::PgPool,
    progress: &mut Option<&mut dyn bigname_adapters::StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}
