use std::time::Instant;

use anyhow::Result;
use tracing::info;

use crate::runtime::{
    log_block_derived_normalized_event_summary, log_ens_v1_reverse_claim_sync_summary,
};

use super::{
    mode::{PersistedRawPayloadAdapterSyncMode, ensure_raw_fact_adapter_allowed},
    sync_logging::log_adapter_call_timing,
};
use crate::reconciliation::{
    replay::NormalizedEventReplayAdapter, types::PersistedRawPayloadAdapterSyncSummary,
};

pub(super) async fn sync_block_derived_for_mode(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    mode: PersistedRawPayloadAdapterSyncMode,
    aggregate: &mut PersistedRawPayloadAdapterSyncSummary,
) -> Result<()> {
    ensure_raw_fact_adapter_allowed(
        mode,
        NormalizedEventReplayAdapter::BlockDerivedNormalizedEvents,
    )?;
    let adapter_started = Instant::now();
    let source_scope_target_count = source_scope.map_or(0, <[_]>::len);
    info!(
        service = "indexer",
        command = "adapter-sync",
        chain,
        adapter = "block_derived_normalized_events",
        block_hash_count = block_hashes.len(),
        source_scope_target_count,
        adapter_sync_mode = ?mode,
        "adapter sync call started"
    );

    let (summary, authority) = match mode {
        PersistedRawPayloadAdapterSyncMode::RawFactReplay {
            canonical_raw_log_count,
            ..
        } if mode.uses_stateless_replay_authority() => {
            bigname_adapters::sync_block_derived_normalized_events_with_stateless_replay_authority(
                pool,
                chain,
                block_hashes,
                source_scope,
                canonical_raw_log_count,
            )
            .await?
        }
        PersistedRawPayloadAdapterSyncMode::RawFactReplay {
            canonical_raw_log_count,
            ..
        } => (
            bigname_adapters::sync_block_derived_normalized_events_with_scanned_log_count(
                pool,
                chain,
                block_hashes,
                source_scope,
                canonical_raw_log_count,
            )
            .await?,
            bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
        ),
        PersistedRawPayloadAdapterSyncMode::LivePoll
        | PersistedRawPayloadAdapterSyncMode::LiveOrBackfill => (
            bigname_adapters::sync_block_derived_normalized_events(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?,
            bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
        ),
    };
    log_adapter_call_timing(
        chain,
        "block_derived_normalized_events",
        "sync_block_derived_normalized_events",
        block_hashes.len(),
        source_scope_target_count,
        summary.scanned_log_count,
        summary.matched_log_count,
        summary.total_synced_count,
        summary.total_inserted_count,
        adapter_started.elapsed().as_millis(),
    );
    log_block_derived_normalized_event_summary(chain, &summary);
    aggregate.add_counts(
        summary.scanned_log_count,
        summary.matched_log_count,
        summary.total_synced_count,
        summary.total_inserted_count,
    );
    aggregate.add_stateless_replay_authority(&authority);
    Ok(())
}

pub(super) async fn sync_reverse_claim_for_mode(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    mode: PersistedRawPayloadAdapterSyncMode,
    aggregate: &mut PersistedRawPayloadAdapterSyncSummary,
) -> Result<()> {
    if !mode.selects_adapter(
        source_scope,
        NormalizedEventReplayAdapter::EnsV1ReverseClaim,
    ) {
        info!(
            service = "indexer",
            chain, "ENSv1 reverse-claim adapter sync skipped outside selected source scope"
        );
        return Ok(());
    }

    ensure_raw_fact_adapter_allowed(mode, NormalizedEventReplayAdapter::EnsV1ReverseClaim)?;
    let adapter_started = Instant::now();
    let source_scope_target_count = source_scope.map_or(0, <[_]>::len);
    info!(
        service = "indexer",
        command = "adapter-sync",
        chain,
        adapter = "ens_v1_reverse_claim",
        block_hash_count = block_hashes.len(),
        source_scope_target_count,
        adapter_sync_mode = ?mode,
        "adapter sync call started"
    );

    let (summary, authority) = match (source_scope, mode.uses_stateless_replay_authority()) {
        (Some(source_scope), true) => {
            bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes_with_source_scope_and_stateless_replay_authority(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?
        }
        (None, true) => {
            bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes_with_stateless_replay_authority(
                pool,
                chain,
                block_hashes,
            )
            .await?
        }
        (Some(source_scope), false) => (
            bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?,
            bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
        ),
        (None, false) => (
            bigname_adapters::EnsV1ReverseClaimSyncSummary::sync_for_block_hashes(
                pool,
                chain,
                block_hashes,
            )
            .await?,
            bigname_storage::NormalizedEventReplayAuthoritySummary::default(),
        ),
    };
    log_adapter_call_timing(
        chain,
        "ens_v1_reverse_claim",
        "sync_for_block_hashes",
        block_hashes.len(),
        source_scope_target_count,
        summary.scanned_log_count,
        summary.matched_log_count,
        summary.total_synced_count,
        summary.total_inserted_count,
        adapter_started.elapsed().as_millis(),
    );
    log_ens_v1_reverse_claim_sync_summary(chain, &summary);
    aggregate.add_counts(
        summary.scanned_log_count,
        summary.matched_log_count,
        summary.total_synced_count,
        summary.total_inserted_count,
    );
    aggregate.add_stateless_replay_authority(&authority);
    Ok(())
}
