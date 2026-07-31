use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, Result, ensure};
use sqlx::{PgPool, types::Uuid};

use super::{
    EnsV2RegistryResourceSurfaceSyncSummary, emitters::load_active_emitters,
    load::RawLogCanonicalityFilter, sync_ens_v2_registry_resource_surface_with_scope_and_state,
    types::RegistryNameState,
};

mod cache;
mod path;
mod reuse;

use cache::{
    CachedLiveRegistryReplayState, MAX_LIVE_REGISTRY_REPLAY_STATE_WEIGHT,
    replay_state_fits_process_cache, store_live_registry_replay_state,
    take_live_registry_replay_state,
};
use path::{
    load_raw_log_closure_floor, load_registry_cache_metadata, load_selected_registry_target,
};
use reuse::reusable_process_cache_path;

#[derive(Debug, Default, Eq, PartialEq)]
pub(super) struct RegistryReplayState {
    pub(super) registry_suffix_by_address: HashMap<String, String>,
    pub(super) registry_contract_by_address: HashMap<String, Uuid>,
    pub(super) states_by_registry_token: BTreeMap<(String, String), RegistryNameState>,
    pub(super) state_keys_by_registry_namehash:
        HashMap<(String, String), BTreeSet<(String, String)>>,
    pub(super) token_aliases: HashMap<(String, String), (String, String)>,
    pub(super) current_token_alias_by_canonical_key: HashMap<(String, String), (String, String)>,
}

pub(super) use cache::invalidate_live_registry_replay_state;
pub(in crate::ens_v2_registry) use path::load_selected_registry_path_to_floor;

/// Apply an ordinary ENSv2 live poll using a best-effort process-local replay cache.
pub async fn sync_ens_v2_registry_resource_surface_live_poll(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    target_block_number: i64,
    block_hashes: &[String],
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_live_poll_with_cache_budget(
        pool,
        deployment_profile,
        chain,
        target_block_number,
        block_hashes,
        MAX_LIVE_REGISTRY_REPLAY_STATE_WEIGHT,
    )
    .await
}

#[cfg(test)]
pub(in crate::ens_v2_registry) async fn sync_ens_v2_registry_resource_surface_live_poll_with_tiny_cache(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    target_block_number: i64,
    block_hashes: &[String],
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_live_poll_with_cache_budget(
        pool,
        deployment_profile,
        chain,
        target_block_number,
        block_hashes,
        1,
    )
    .await
}

async fn sync_ens_v2_registry_resource_surface_live_poll_with_cache_budget(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    target_block_number: i64,
    block_hashes: &[String],
    max_process_cache_weight: usize,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    ensure!(
        !deployment_profile.trim().is_empty(),
        "ENSv2 live-poll deployment profile is empty"
    );

    let target_block_hash =
        load_selected_registry_target(pool, chain, target_block_number, block_hashes).await?;
    let metadata_before = load_registry_cache_metadata(pool, chain).await?;
    let cached = match take_live_registry_replay_state(pool, deployment_profile, chain) {
        Some(cached) => reusable_process_cache_path(
            pool,
            chain,
            target_block_number,
            &target_block_hash,
            &metadata_before,
            &cached,
        )
        .await?
        .map(|path| (cached, path)),
        None => None,
    };

    let (cached, selected_path) = if let Some((cached, path)) = cached {
        (Some(cached), path)
    } else {
        let registry_emitters = load_active_emitters(pool, chain, None, true).await?;
        let closure_floor =
            load_raw_log_closure_floor(pool, chain, target_block_number, &registry_emitters)
                .await?;
        let path = load_selected_registry_path_to_floor(
            pool,
            chain,
            target_block_number,
            &target_block_hash,
            closure_floor,
        )
        .await?;
        (None, path)
    };

    let (summary, replay_state) = if let Some(cached) = cached {
        let incremental_block_hashes = selected_path.hashes_after(cached.through_block_number);
        if incremental_block_hashes.is_empty() {
            (
                EnsV2RegistryResourceSurfaceSyncSummary::empty(0),
                cached.replay_state,
            )
        } else {
            sync_ens_v2_registry_resource_surface_with_scope_and_state(
                pool,
                chain,
                true,
                &incremental_block_hashes,
                None,
                RawLogCanonicalityFilter::IncludeObserved,
                Some(target_block_number),
                Some(cached.replay_state),
                true,
                false,
                false,
                Some(metadata_before.discovery_admission_epoch),
            )
            .await?
        }
    } else {
        let selected_path_hashes = selected_path.all_hashes();
        sync_ens_v2_registry_resource_surface_with_scope_and_state(
            pool,
            chain,
            true,
            &selected_path_hashes,
            None,
            RawLogCanonicalityFilter::IncludeObserved,
            Some(target_block_number),
            None,
            true,
            false,
            false,
            Some(metadata_before.discovery_admission_epoch),
        )
        .await?
    };

    let metadata_after = load_registry_cache_metadata(pool, chain).await?;
    ensure!(
        metadata_after.raw_log_input_revision == metadata_before.raw_log_input_revision,
        "ENSv2 raw-log input changed during live sync on {chain}; refusing to publish a stale replay cache"
    );
    let own_epoch_bumps = i64::try_from(summary.discovery_admission_epoch_bump_count)
        .context("ENSv2 discovery admission-epoch bump count exceeds i64")?;
    let expected_epoch = metadata_before
        .discovery_admission_epoch
        .checked_add(own_epoch_bumps)
        .context("ENSv2 discovery admission epoch overflow")?;
    ensure!(
        metadata_after.discovery_admission_epoch == expected_epoch,
        "ENSv2 discovery admission epoch changed unexpectedly during live sync on {chain}: expected {expected_epoch}, observed {}",
        metadata_after.discovery_admission_epoch
    );

    let snapshot = CachedLiveRegistryReplayState {
        through_block_number: target_block_number,
        through_block_hash: selected_path.target_block_hash,
        raw_log_input_revision: metadata_after.raw_log_input_revision,
        raw_log_retention_generation: metadata_after.raw_log_retention_generation,
        discovery_admission_epoch: metadata_after.discovery_admission_epoch,
        replay_state,
    };
    if replay_state_fits_process_cache(&snapshot.replay_state, max_process_cache_weight) {
        store_live_registry_replay_state(pool, deployment_profile, chain, snapshot);
    }

    Ok(summary)
}
