use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_adapters::StartupAdapterProgress;
use bigname_storage::{RawBlock, RawCodeHash, load_raw_blocks_by_hashes, upsert_raw_code_hashes};
use sqlx::Row;

use crate::{
    provider::{ChainProviderOps, ProviderBlockCodeObservationRequest, ProviderHeadSnapshot},
    runtime::IntakeChainTask,
};

use super::super::{
    canonical::ChainCoverageFrontiers,
    payload::{
        SelectedAddressSet, provider_code_observation_to_raw_code_hash,
        raw_code_hash_candidate_hashes, raw_payload_candidate_hashes,
    },
    types::{CanonicalReconciliation, CanonicalReconciliationStatus, HeadChangeSet},
};
use super::load_live_generic_resolver_topic0s;

#[path = "code_hashes/config.rs"]
mod config;
use config::*;

/// Provider-fetch chunk size; successful rounds persist before the sweep advances.
const RAW_CODE_BASELINE_FETCH_CHUNK_ADDRESSES: usize = 256;
const LIVE_CODE_HASH_PROGRESS_ROWS: usize = 1_000;
const LIVE_CODE_OBSERVATION_PROGRESS_BLOCKS: usize = 32;

#[expect(clippy::too_many_arguments)]
#[allow(dead_code)]
pub(crate) async fn persist_reconciled_raw_code_hashes(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
    loaded_plan_admission_epoch: i64,
    coverage_frontiers: &ChainCoverageFrontiers,
) -> Result<()> {
    persist_reconciled_raw_code_hashes_inner(
        pool,
        task,
        provider,
        heads,
        canonical,
        head_change_set,
        loaded_plan_admission_epoch,
        coverage_frontiers,
        &mut None,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
pub(crate) async fn persist_reconciled_raw_code_hashes_with_progress(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
    loaded_plan_admission_epoch: i64,
    coverage_frontiers: &ChainCoverageFrontiers,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    persist_reconciled_raw_code_hashes_inner(
        pool,
        task,
        provider,
        heads,
        canonical,
        head_change_set,
        loaded_plan_admission_epoch,
        coverage_frontiers,
        progress,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn persist_reconciled_raw_code_hashes_inner(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
    loaded_plan_admission_epoch: i64,
    coverage_frontiers: &ChainCoverageFrontiers,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if canonical.status == CanonicalReconciliationStatus::StoredLineagePromoted {
        return Ok(());
    }

    let refreshed_block_hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let candidate_hashes = raw_code_hash_candidate_hashes(heads, canonical, head_change_set);
    if candidate_hashes.is_empty() {
        return Ok(());
    }

    let mut raw_blocks_by_order = BTreeMap::new();
    for block_page in candidate_hashes.chunks(LIVE_CODE_OBSERVATION_PROGRESS_BLOCKS) {
        for raw_block in load_raw_blocks_by_hashes(pool, &task.chain, block_page).await? {
            raw_blocks_by_order.insert(
                (raw_block.block_number, raw_block.block_hash.clone()),
                raw_block,
            );
        }
        record_progress(pool, progress).await?;
    }
    let raw_blocks = raw_blocks_by_order.into_values().collect::<Vec<_>>();
    if raw_blocks.len() != candidate_hashes.len() {
        bail!(
            "stored raw block count {} does not match the raw code-hash fetch plan size {} for chain {}",
            raw_blocks.len(),
            candidate_hashes.len(),
            task.chain
        );
    }

    let watched_addresses = SelectedAddressSet::from_plan_addresses(&task.addresses);
    let generic_resolver_topic0s = load_live_generic_resolver_topic0s(pool, &task.chain)
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    // Candidate blocks hold a bounded number of distinct emitters, so the
    // watched-membership filter runs client-side against the already-sorted
    // plan surface instead of binding the multi-million-address watch set
    // back into Postgres on every head-changed tick.
    let mut loaded_emitters_by_block_hash = BTreeMap::new();
    for block_page in candidate_hashes.chunks(LIVE_CODE_OBSERVATION_PROGRESS_BLOCKS) {
        loaded_emitters_by_block_hash.extend(
            load_raw_log_emitter_addresses_by_block_hashes(
                pool,
                &task.chain,
                block_page,
                &generic_resolver_topic0s,
            )
            .await?,
        );
        record_progress(pool, progress).await?;
    }
    let mut emitter_addresses_by_block_hash = BTreeMap::new();
    let loaded_emitter_count = loaded_emitters_by_block_hash.len();
    for (index, (block_hash, emitters)) in loaded_emitters_by_block_hash.into_iter().enumerate() {
        let selected = emitters
            .into_iter()
            .filter(|(address, topic0_selected)| {
                *topic0_selected || watched_addresses.contains(address)
            })
            .map(|(address, _)| address)
            .collect::<BTreeSet<_>>();
        if !selected.is_empty() {
            emitter_addresses_by_block_hash.insert(block_hash, selected);
        }
        if index + 1 == loaded_emitter_count
            || (index + 1).is_multiple_of(LIVE_CODE_HASH_PROGRESS_ROWS)
        {
            record_progress(pool, progress).await?;
        }
    }
    let mut stored_code_addresses_by_block_hash = BTreeMap::new();
    for block_page in candidate_hashes.chunks(LIVE_CODE_OBSERVATION_PROGRESS_BLOCKS) {
        let code_observation_addresses = block_page
            .iter()
            .filter_map(|block_hash| emitter_addresses_by_block_hash.get(block_hash))
            .flat_map(|addresses| addresses.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        stored_code_addresses_by_block_hash.extend(
            load_raw_code_addresses_by_block_hashes(
                pool,
                &task.chain,
                block_page,
                &code_observation_addresses,
            )
            .await?,
        );
        record_progress(pool, progress).await?;
    }
    let mut emitter_raw_blocks = Vec::new();
    for (index, raw_block) in raw_blocks.iter().enumerate() {
        if let Some(emitter_addresses) = emitter_addresses_by_block_hash.get(&raw_block.block_hash)
        {
            let stored_code_addresses = stored_code_addresses_by_block_hash
                .get(&raw_block.block_hash)
                .cloned()
                .unwrap_or_default();
            let block_refreshed = refreshed_block_hashes.contains(&raw_block.block_hash);
            let addresses = emitter_addresses
                .iter()
                .filter(|address| block_refreshed || !stored_code_addresses.contains(*address))
                .cloned()
                .collect::<Vec<_>>();
            if !addresses.is_empty() {
                emitter_raw_blocks.push((raw_block, addresses));
            }
        }
        if index + 1 == raw_blocks.len() || (index + 1).is_multiple_of(LIVE_CODE_HASH_PROGRESS_ROWS)
        {
            record_progress(pool, progress).await?;
        }
    }

    let emitter_raw_blocks_by_hash = emitter_raw_blocks
        .iter()
        .map(|(raw_block, _)| (raw_block.block_hash.to_ascii_lowercase(), *raw_block))
        .collect::<BTreeMap<_, _>>();
    let code_observation_requests = emitter_raw_blocks
        .iter()
        .map(
            |(raw_block, addresses)| ProviderBlockCodeObservationRequest {
                block_number: raw_block.block_number,
                block_hash: raw_block.block_hash.clone(),
                addresses: addresses.clone(),
            },
        )
        .collect::<Vec<_>>();
    let mut fetched_observations = Vec::with_capacity(code_observation_requests.len());
    for request_chunk in code_observation_requests.chunks(LIVE_CODE_OBSERVATION_PROGRESS_BLOCKS) {
        fetched_observations.extend(
            provider
                .fetch_code_observations_at_block_hashes(request_chunk)
                .await?,
        );
        record_progress(pool, progress).await?;
    }
    if fetched_observations.len() != code_observation_requests.len() {
        bail!(
            "provider returned {} code-observation block groups for {} requested blocks on chain {}",
            fetched_observations.len(),
            code_observation_requests.len(),
            task.chain
        );
    }
    let mut code_hashes = Vec::<RawCodeHash>::new();
    for block_observations in &fetched_observations {
        let raw_block = emitter_raw_blocks_by_hash
            .get(&block_observations.block_hash.to_ascii_lowercase())
            .with_context(|| {
                format!(
                    "provider returned code observations for unrequested block {} on chain {}",
                    block_observations.block_hash, task.chain
                )
            })?;
        code_hashes.extend(
            block_observations
                .observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(&task.chain, raw_block, observation)
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }
    for chunk in code_hashes.chunks(LIVE_CODE_HASH_PROGRESS_ROWS) {
        upsert_raw_code_hashes(pool, chunk).await?;
        record_progress(pool, progress).await?;
    }

    let canonical_baseline_block = canonical.canonical.as_ref().and_then(|canonical_head| {
        raw_blocks
            .iter()
            .find(|raw_block| raw_block.block_hash == canonical_head.block_hash)
    });
    if let Some(baseline_raw_block) = canonical_baseline_block {
        sweep_raw_code_baseline_chunk(
            pool,
            task,
            provider,
            baseline_raw_block,
            watched_addresses,
            loaded_plan_admission_epoch,
            coverage_frontiers,
            progress,
        )
        .await?;
    }

    Ok(())
}

/// One capped step of the per-chain code-observation sweep.
///
/// The sweep walks the sorted watched surface behind a process-lifetime
/// cursor: each tick verifies a capped address batch and fetches code only
/// for those with no stored non-orphaned observation, and upserts per
/// [`RAW_CODE_BASELINE_FETCH_CHUNK_ADDRESSES`]-sized provider round so
/// stored observations survive failures and restarts. A finished sweep records
/// the admission epoch under which its in-memory plan was loaded; when a plan
/// with a newer epoch is applied, a fresh sweep re-verifies the surface (cheap
/// membership probes; only genuinely missing addresses are fetched) so newly
/// watched addresses are eventually baselined too.
#[expect(clippy::too_many_arguments)]
async fn sweep_raw_code_baseline_chunk(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    baseline_raw_block: &RawBlock,
    watched_addresses: SelectedAddressSet<'_>,
    loaded_plan_admission_epoch: i64,
    coverage_frontiers: &ChainCoverageFrontiers,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let sorted_watched_addresses = watched_addresses.as_sorted_slice();
    if sorted_watched_addresses.is_empty() {
        return Ok(());
    }

    let mut frontier = coverage_frontiers.raw_code_baseline_frontier(&task.chain);
    if frontier.completed_admission_epoch == Some(loaded_plan_admission_epoch) {
        return Ok(());
    }
    let sweep_admission_epoch = match frontier.sweep_admission_epoch {
        Some(epoch) => epoch,
        None => {
            frontier.sweep_admission_epoch = Some(loaded_plan_admission_epoch);
            frontier.verified_through_address = None;
            loaded_plan_admission_epoch
        }
    };

    let start_index = match &frontier.verified_through_address {
        Some(verified_through) => {
            sorted_watched_addresses.partition_point(|address| address <= verified_through)
        }
        None => 0,
    };
    let batch_end_index = (start_index + raw_code_baseline_max_addresses_per_tick())
        .min(sorted_watched_addresses.len());
    let batch = &sorted_watched_addresses[start_index..batch_end_index];
    if batch.is_empty() {
        frontier.completed_admission_epoch = Some(sweep_admission_epoch);
        frontier.sweep_admission_epoch = None;
        frontier.verified_through_address = None;
        coverage_frontiers.store_raw_code_baseline_frontier(&task.chain, frontier);
        return Ok(());
    }

    let missing_addresses =
        load_raw_code_baseline_addresses_missing_observations(pool, &task.chain, batch).await?;
    for chunk in missing_addresses.chunks(RAW_CODE_BASELINE_FETCH_CHUNK_ADDRESSES) {
        let observations = provider
            .fetch_code_observations_at_block_hashes(&[ProviderBlockCodeObservationRequest {
                block_number: baseline_raw_block.block_number,
                block_hash: baseline_raw_block.block_hash.clone(),
                addresses: chunk.to_vec(),
            }])
            .await?;
        let mut chunk_code_hashes = Vec::<RawCodeHash>::new();
        for block_observations in &observations {
            chunk_code_hashes.extend(
                block_observations
                    .observations
                    .iter()
                    .map(|observation| {
                        provider_code_observation_to_raw_code_hash(
                            &task.chain,
                            baseline_raw_block,
                            observation,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        upsert_raw_code_hashes(pool, &chunk_code_hashes).await?;
        // Every batch address at or below the chunk's last address is now
        // either stored-verified or freshly upserted, so the cursor may
        // advance past a failure boundary without losing that progress.
        frontier.verified_through_address = chunk.last().cloned();
        coverage_frontiers.store_raw_code_baseline_frontier(&task.chain, frontier.clone());
        record_progress(pool, progress).await?;
    }

    frontier.verified_through_address = batch.last().cloned();
    if batch_end_index == sorted_watched_addresses.len() {
        frontier.completed_admission_epoch = Some(sweep_admission_epoch);
        frontier.sweep_admission_epoch = None;
        frontier.verified_through_address = None;
    }
    coverage_frontiers.store_raw_code_baseline_frontier(&task.chain, frontier);
    Ok(())
}

async fn record_progress(
    pool: &sqlx::PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}

async fn load_raw_log_emitter_addresses_by_block_hashes(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    generic_resolver_topic0s: &[String],
) -> Result<BTreeMap<String, BTreeMap<String, bool>>> {
    if block_hashes.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            block_hash,
            LOWER(emitting_address) AS emitting_address,
            BOOL_OR(LOWER(topics[1]) = ANY($3::TEXT[])) AS topic0_selected
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
        GROUP BY block_hash, LOWER(emitting_address)
        ORDER BY block_hash, LOWER(emitting_address)
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(generic_resolver_topic0s)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load raw-log code observation emitters for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    let mut addresses_by_block_hash = BTreeMap::<String, BTreeMap<String, bool>>::new();
    for row in rows {
        let block_hash = row
            .try_get::<String, _>("block_hash")
            .context("missing block_hash from raw-log emitter row")?;
        let emitting_address = row
            .try_get::<String, _>("emitting_address")
            .context("missing emitting_address from raw-log emitter row")?;
        let topic0_selected = row
            .try_get::<Option<bool>, _>("topic0_selected")
            .context("missing topic0_selected from raw-log emitter row")?
            .unwrap_or(false);
        addresses_by_block_hash
            .entry(block_hash)
            .or_default()
            .insert(emitting_address, topic0_selected);
    }

    Ok(addresses_by_block_hash)
}

async fn load_raw_code_addresses_by_block_hashes(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    code_observation_addresses: &[String],
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    if block_hashes.is_empty() || code_observation_addresses.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            block_hash,
            LOWER(contract_address) AS contract_address
        FROM raw_code_hashes
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND LOWER(contract_address) = ANY($3::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        GROUP BY block_hash, LOWER(contract_address)
        ORDER BY block_hash, LOWER(contract_address)
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(code_observation_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load raw code-hash addresses for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    let mut addresses_by_block_hash = BTreeMap::<String, BTreeSet<String>>::new();
    for row in rows {
        let block_hash = row
            .try_get::<String, _>("block_hash")
            .context("missing block_hash from raw code-hash row")?;
        let contract_address = row
            .try_get::<String, _>("contract_address")
            .context("missing contract_address from raw code-hash row")?;
        addresses_by_block_hash
            .entry(block_hash)
            .or_default()
            .insert(contract_address);
    }

    Ok(addresses_by_block_hash)
}

/// Which of the (capped) baseline batch addresses still lack any stored
/// non-orphaned code observation. The bind is at most
/// [`DEFAULT_RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK`] addresses, never the whole
/// watch surface. Returned in the batch's sorted order so the sweep cursor
/// can advance chunk-by-chunk.
async fn load_raw_code_baseline_addresses_missing_observations(
    pool: &sqlx::PgPool,
    chain: &str,
    batch_addresses: &[String],
) -> Result<Vec<String>> {
    if batch_addresses.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT watched.contract_address
        FROM UNNEST($2::TEXT[]) AS watched(contract_address)
        WHERE NOT EXISTS (
            SELECT 1
            FROM raw_code_hashes
            WHERE chain_id = $1
              AND LOWER(raw_code_hashes.contract_address) = watched.contract_address
              AND canonicality_state <> 'orphaned'::canonicality_state
        )
        ORDER BY watched.contract_address
        "#,
    )
    .bind(chain)
    .bind(batch_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load missing raw code-hash baseline addresses for chain {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            row.try_get::<String, _>("contract_address")
                .context("missing contract_address from baseline address row")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK,
        parse_raw_code_baseline_max_addresses_per_tick,
    };

    #[test]
    fn baseline_per_tick_cap_parses_overrides_and_rejects_nonsense() {
        assert_eq!(
            parse_raw_code_baseline_max_addresses_per_tick(None),
            DEFAULT_RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK
        );
        assert_eq!(
            parse_raw_code_baseline_max_addresses_per_tick(Some("512")),
            512
        );
        assert_eq!(
            parse_raw_code_baseline_max_addresses_per_tick(Some(" 3 ")),
            3
        );
        assert_eq!(
            parse_raw_code_baseline_max_addresses_per_tick(Some("0")),
            DEFAULT_RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK
        );
        assert_eq!(
            parse_raw_code_baseline_max_addresses_per_tick(Some("not-a-number")),
            DEFAULT_RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK
        );
    }
}
