use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, ensure};
use tracing::info;

use crate::reconciliation::{
    replay::{
        NormalizedEventReplayAdapter, replay_stateless_normalized_events_before_full_closure,
        select_log_bounded_replay_to_block,
    },
    types::{
        PersistedRawPayloadAdapterSyncSummary, RawFactNormalizedEventReplayOutcome,
        RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
    },
};

use super::{
    FullClosureCheckpointCompletion, sync_full_closure_with_checkpoint_completion_and_prelude,
};

#[cfg(test)]
#[path = "automatic/test_hook.rs"]
mod test_hook;
#[cfg(test)]
pub(crate) use test_hook::{install_after_stateless_failure, install_stateless_page_observer};

pub(crate) struct AutomaticTwoPhaseFullClosureSyncResult {
    pub(crate) stateless: RawFactNormalizedEventReplayOutcome,
    pub(crate) closure: PersistedRawPayloadAdapterSyncSummary,
}

#[derive(Debug)]
struct AutomaticStatelessReplayCompleted;

impl fmt::Display for AutomaticStatelessReplayCompleted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("automatic stateless replay phase completed before failure")
    }
}

impl std::error::Error for AutomaticStatelessReplayCompleted {}

pub(crate) fn automatic_stateless_replay_completed(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<AutomaticStatelessReplayCompleted>()
        .is_some()
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn sync_automatic_two_phase_full_closure_normalized_events(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    checkpoint_cursor_kind: &str,
    range_start_block_number: i64,
    target_block_number: i64,
    stateless_ranges: &[(i64, i64)],
    adapters: &[NormalizedEventReplayAdapter],
    max_raw_logs_per_page: usize,
) -> Result<AutomaticTwoPhaseFullClosureSyncResult> {
    info!(
        service = "indexer",
        replay_cursor_kind = checkpoint_cursor_kind,
        chain,
        range_start_block_number,
        target_block_number,
        stateless_range_count = stateless_ranges.len(),
        stateless_ranges = ?stateless_ranges,
        "automatic two-phase normalized-event replay session started"
    );

    let stateless_replay_completed = Arc::new(AtomicBool::new(false));
    let stateless_replay_completed_in_prelude = Arc::clone(&stateless_replay_completed);
    let sync = sync_full_closure_with_checkpoint_completion_and_prelude(
        pool,
        deployment_profile,
        chain,
        checkpoint_cursor_kind,
        range_start_block_number,
        target_block_number,
        adapters,
        max_raw_logs_per_page,
        FullClosureCheckpointCompletion::Retain,
        move || async move {
            let stateless = replay_stateless_normalized_events_for_ranges(
                pool,
                deployment_profile,
                chain,
                stateless_ranges,
                max_raw_logs_per_page,
            )
            .await?;
            stateless_replay_completed_in_prelude.store(true, Ordering::Release);
            #[cfg(test)]
            test_hook::fail_after_stateless(pool, deployment_profile, chain).await?;
            Ok(stateless)
        },
    )
    .await;
    let (closure, stateless) = match sync {
        Ok(result) => result,
        Err(error) if stateless_replay_completed.load(Ordering::Acquire) => {
            return Err(error.context(AutomaticStatelessReplayCompleted));
        }
        Err(error) => return Err(error),
    };

    Ok(AutomaticTwoPhaseFullClosureSyncResult {
        stateless,
        closure: closure.summary,
    })
}

async fn replay_stateless_normalized_events_for_ranges(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    stateless_ranges: &[(i64, i64)],
    max_raw_logs_per_page: usize,
) -> Result<RawFactNormalizedEventReplayOutcome> {
    ensure!(
        !stateless_ranges.is_empty(),
        "automatic stateless replay requires at least one block range"
    );
    let mut aggregate = RawFactNormalizedEventReplayOutcome {
        deployment_profile: deployment_profile.to_owned(),
        chain: chain.to_owned(),
        selection_kind: "block_range",
        source_scope_target_count: 0,
        selected_block_count: 0,
        canonical_raw_log_count: 0,
        scanned_raw_log_count: 0,
        matched_raw_log_count: 0,
        normalized_event_synced_count: 0,
        normalized_event_inserted_count: 0,
        stateless_replay_authority: bigname_storage::NormalizedEventReplayAuthoritySummary::default(
        ),
    };
    for &(range_start_block_number, target_block_number) in stateless_ranges {
        let range = replay_stateless_normalized_events_in_pages(
            pool,
            deployment_profile,
            chain,
            range_start_block_number,
            target_block_number,
            max_raw_logs_per_page,
        )
        .await?;
        aggregate.selected_block_count += range.selected_block_count;
        aggregate.canonical_raw_log_count += range.canonical_raw_log_count;
        aggregate.scanned_raw_log_count += range.scanned_raw_log_count;
        aggregate.matched_raw_log_count += range.matched_raw_log_count;
        aggregate.normalized_event_synced_count += range.normalized_event_synced_count;
        aggregate.normalized_event_inserted_count += range.normalized_event_inserted_count;
        aggregate
            .stateless_replay_authority
            .add(&range.stateless_replay_authority);
    }
    Ok(aggregate)
}

async fn replay_stateless_normalized_events_in_pages(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
    range_start_block_number: i64,
    target_block_number: i64,
    max_raw_logs_per_page: usize,
) -> Result<RawFactNormalizedEventReplayOutcome> {
    ensure!(
        range_start_block_number <= target_block_number,
        "automatic stateless replay range start {range_start_block_number} is after target {target_block_number}"
    );
    ensure!(
        max_raw_logs_per_page > 0,
        "automatic stateless replay max logs per page must be positive"
    );
    let mut aggregate = RawFactNormalizedEventReplayOutcome {
        deployment_profile: deployment_profile.to_owned(),
        chain: chain.to_owned(),
        selection_kind: "block_range",
        source_scope_target_count: 0,
        selected_block_count: 0,
        canonical_raw_log_count: 0,
        scanned_raw_log_count: 0,
        matched_raw_log_count: 0,
        normalized_event_synced_count: 0,
        normalized_event_inserted_count: 0,
        stateless_replay_authority: bigname_storage::NormalizedEventReplayAuthoritySummary::default(
        ),
    };
    let mut from_block = range_start_block_number;
    loop {
        let to_block = select_log_bounded_replay_to_block(
            pool,
            chain,
            from_block,
            target_block_number,
            max_raw_logs_per_page,
        )
        .await?;
        #[cfg(test)]
        test_hook::record_stateless_page(pool, deployment_profile, chain, from_block, to_block)
            .await?;
        let page = replay_stateless_normalized_events_before_full_closure(
            pool,
            RawFactNormalizedEventReplayRequest {
                deployment_profile: deployment_profile.to_owned(),
                chain: chain.to_owned(),
                selection: RawFactNormalizedEventReplaySelection::BlockRange {
                    from_block,
                    to_block,
                },
            },
        )
        .await?;
        aggregate.selected_block_count += page.selected_block_count;
        aggregate.canonical_raw_log_count += page.canonical_raw_log_count;
        aggregate.scanned_raw_log_count += page.scanned_raw_log_count;
        aggregate.matched_raw_log_count += page.matched_raw_log_count;
        aggregate.normalized_event_synced_count += page.normalized_event_synced_count;
        aggregate.normalized_event_inserted_count += page.normalized_event_inserted_count;
        aggregate
            .stateless_replay_authority
            .add(&page.stateless_replay_authority);

        if to_block == target_block_number {
            break;
        }
        from_block = to_block
            .checked_add(1)
            .context("automatic stateless replay page boundary overflowed")?;
    }
    Ok(aggregate)
}
