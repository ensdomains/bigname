use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use bigname_storage::NormalizedEventReplayAuthoritySummary;
use sqlx::PgPool;

use crate::{
    checkpoint_context::StartupAdapterProgress,
    normalized_event_support::{
        upsert_normalized_events_in_chunks_with_counts,
        upsert_normalized_events_in_chunks_with_counts_and_progress,
        upsert_normalized_events_in_chunks_with_stateless_replay_authority_counts,
        upsert_normalized_events_in_chunks_with_stateless_replay_authority_counts_and_progress,
    },
    startup_progress::{STARTUP_ADAPTER_PROGRESS_PAGE_ROWS, record_processed_row_progress},
};

mod active_emitters;
mod events;
mod helpers;
mod raw_logs;

use active_emitters::load_active_emitters;
use events::build_reverse_changed_events;
use raw_logs::load_reverse_raw_logs;

#[cfg(test)]
use anyhow::Context;
#[cfg(test)]
use bigname_storage::CanonicalityState;
#[cfg(test)]
use helpers::{
    hex_string, name_for_addr_changed_topic0, reverse_claimed_topic0_for_source_family,
    reverse_name_for_source_family, reverse_node_for_source_family,
};

const SOURCE_FAMILY_ENS_V1_REVERSE_L1: &str = "ens_v1_reverse_l1";
const SOURCE_FAMILY_BASENAMES_BASE_PRIMARY: &str = "basenames_base_primary";
const SOURCE_EVENT_REVERSE_CLAIMED: &str = "ReverseClaimed";
const SOURCE_EVENT_NAME_FOR_ADDR_CHANGED: &str = "NameForAddrChanged";
const DERIVATION_KIND_ENS_V1_REVERSE_CLAIM: &str = "ens_v1_reverse_claim";
const EVENT_KIND_REVERSE_CHANGED: &str = "ReverseChanged";
const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
const ENS_NATIVE_COIN_TYPE: &str = "60";
const BASE_NATIVE_COIN_TYPE: &str = "2147492101";
const CONTRACT_ROLE_REVERSE_REGISTRAR: &str = "reverse_registrar";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV1ReverseClaimSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EnsV1ReverseClaimKindSyncSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsV1ReverseClaimKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

impl EnsV1ReverseClaimSyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_reverse_claim_with_scope(pool, chain, true, block_hashes, None, false, None)
            .await
            .map(|(summary, _)| summary)
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v1_reverse_claim_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            false,
            None,
        )
        .await
        .map(|(summary, _)| summary)
    }

    pub async fn sync_for_block_hashes_with_stateless_replay_authority(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<(Self, NormalizedEventReplayAuthoritySummary)> {
        sync_ens_v1_reverse_claim_with_scope(pool, chain, true, block_hashes, None, true, None)
            .await
    }

    pub async fn sync_for_block_hashes_with_source_scope_and_stateless_replay_authority(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<(Self, NormalizedEventReplayAuthoritySummary)> {
        sync_ens_v1_reverse_claim_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            true,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_stateless_replay_authority_and_progress(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        progress: &mut dyn StartupAdapterProgress,
    ) -> Result<(Self, NormalizedEventReplayAuthoritySummary)> {
        sync_ens_v1_reverse_claim_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            true,
            Some(progress),
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope_and_stateless_replay_authority_and_progress(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
        progress: &mut dyn StartupAdapterProgress,
    ) -> Result<(Self, NormalizedEventReplayAuthoritySummary)> {
        sync_ens_v1_reverse_claim_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            true,
            Some(progress),
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_progress(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        progress: &mut dyn StartupAdapterProgress,
    ) -> Result<Self> {
        sync_ens_v1_reverse_claim_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            false,
            Some(progress),
        )
        .await
        .map(|(summary, _)| summary)
    }

    pub async fn sync_for_block_hashes_with_source_scope_and_progress(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
        progress: &mut dyn StartupAdapterProgress,
    ) -> Result<Self> {
        sync_ens_v1_reverse_claim_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            false,
            Some(progress),
        )
        .await
        .map(|(summary, _)| summary)
    }
}

pub async fn sync_ens_v1_reverse_claim(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1ReverseClaimSyncSummary> {
    sync_ens_v1_reverse_claim_with_scope(pool, chain, false, &[], None, false, None)
        .await
        .map(|(summary, _)| summary)
}

pub async fn sync_ens_v1_reverse_claim_with_progress(
    pool: &PgPool,
    chain: &str,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<EnsV1ReverseClaimSyncSummary> {
    sync_ens_v1_reverse_claim_with_scope(pool, chain, false, &[], None, false, Some(progress))
        .await
        .map(|(summary, _)| summary)
}

pub async fn sync_ens_v1_reverse_claim_range(
    pool: &PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
) -> Result<EnsV1ReverseClaimSyncSummary> {
    let source_scope = load_active_emitters(pool, chain)
        .await?
        .into_iter()
        .map(|emitter| (emitter.source_family, emitter.address, from_block, to_block))
        .collect::<Vec<_>>();
    sync_ens_v1_reverse_claim_with_scope(pool, chain, false, &[], Some(&source_scope), false, None)
        .await
        .map(|(summary, _)| summary)
}

async fn sync_ens_v1_reverse_claim_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    stateless_replay_authority: bool,
    mut progress: Option<&mut dyn StartupAdapterProgress>,
) -> Result<(
    EnsV1ReverseClaimSyncSummary,
    NormalizedEventReplayAuthoritySummary,
)> {
    let mut active_emitters = load_active_emitters(pool, chain).await?;
    if let Some(source_scope) = source_scope {
        active_emitters.retain(|emitter| reverse_scope_includes_emitter(source_scope, emitter));
    }
    if active_emitters.is_empty() {
        return Ok((
            empty_summary(0),
            NormalizedEventReplayAuthoritySummary::default(),
        ));
    }

    let raw_logs = load_reverse_raw_logs(
        pool,
        chain,
        &active_emitters,
        restrict_to_block_hashes,
        block_hashes,
        source_scope,
        &mut progress,
    )
    .await?;
    let scanned_log_count = raw_logs.len();
    if raw_logs.is_empty() {
        return Ok((
            empty_summary(scanned_log_count),
            NormalizedEventReplayAuthoritySummary::default(),
        ));
    }

    let mut matched_log_refs = HashSet::new();
    let mut events = Vec::new();
    for (index, raw_log) in raw_logs.iter().enumerate() {
        let built_events = build_reverse_changed_events(raw_log)?;
        if !built_events.is_empty() {
            matched_log_refs.insert((
                raw_log.chain_id.clone(),
                raw_log.block_hash.clone(),
                raw_log.transaction_hash.clone(),
                raw_log.log_index,
            ));
            events.extend(built_events);
        }
        record_processed_row_progress(pool, &mut progress, index + 1, raw_logs.len()).await?;
    }

    if events.is_empty() {
        return Ok((
            empty_summary(scanned_log_count),
            NormalizedEventReplayAuthoritySummary::default(),
        ));
    }

    let (counts, authority) = if stateless_replay_authority {
        match progress {
            Some(progress) => {
                upsert_normalized_events_in_chunks_with_stateless_replay_authority_counts_and_progress(
                    pool,
                    &events,
                    "ENSv1 reverse normalized-event",
                    STARTUP_ADAPTER_PROGRESS_PAGE_ROWS,
                    Some(progress),
                )
                .await?
            }
            None => {
                upsert_normalized_events_in_chunks_with_stateless_replay_authority_counts(
                    pool,
                    &events,
                    "ENSv1 reverse normalized-event",
                    10_000,
                )
                .await?
            }
        }
    } else {
        let counts = match progress {
            Some(progress) => {
                upsert_normalized_events_in_chunks_with_counts_and_progress(
                    pool,
                    &events,
                    "ENSv1 reverse normalized-event",
                    STARTUP_ADAPTER_PROGRESS_PAGE_ROWS,
                    Some(progress),
                )
                .await?
            }
            None => {
                upsert_normalized_events_in_chunks_with_counts(
                    pool,
                    &events,
                    "ENSv1 reverse normalized-event",
                    10_000,
                )
                .await?
            }
        };
        (counts, NormalizedEventReplayAuthoritySummary::default())
    };
    let (total_synced_count, total_inserted_count, by_kind) =
        counts.into_parts_by_kind(|synced_count, inserted_count| {
            EnsV1ReverseClaimKindSyncSummary {
                synced_count,
                inserted_count,
            }
        });

    Ok((
        EnsV1ReverseClaimSyncSummary {
            scanned_log_count,
            matched_log_count: matched_log_refs.len(),
            total_synced_count,
            total_inserted_count,
            by_kind,
        },
        authority,
    ))
}

fn empty_summary(scanned_log_count: usize) -> EnsV1ReverseClaimSyncSummary {
    EnsV1ReverseClaimSyncSummary {
        scanned_log_count,
        matched_log_count: 0,
        total_synced_count: 0,
        total_inserted_count: 0,
        by_kind: BTreeMap::new(),
    }
}

fn reverse_scope_includes_emitter(
    source_scope: &[(String, String, i64, i64)],
    emitter: &active_emitters::ActiveEmitter,
) -> bool {
    source_scope
        .iter()
        .any(|(source_family, address, from_block, to_block)| {
            source_family == &emitter.source_family
                && address.eq_ignore_ascii_case(&emitter.address)
                && from_block <= to_block
        })
}

#[cfg(test)]
mod tests;
