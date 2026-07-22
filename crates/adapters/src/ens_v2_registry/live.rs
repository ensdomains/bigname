use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, Result, ensure};
use sqlx::{PgPool, types::Uuid};

use super::{
    EnsV2RegistryResourceSurfaceSyncSummary, emitters::load_active_emitters,
    load::RawLogCanonicalityFilter, sync_ens_v2_registry_resource_surface_with_scope_and_state,
    types::RegistryNameState,
};

mod cache;
mod checkpoint;
mod completeness;
mod fence;
mod path;

use cache::{
    CachedLiveRegistryReplayState, MAX_LIVE_REGISTRY_REPLAY_STATE_WEIGHT,
    replay_state_fits_process_cache, store_live_registry_replay_state,
    take_live_registry_replay_state,
};
use checkpoint::{
    LiveRegistryReplayCheckpointHeader, LiveRegistryReplayCheckpointLoad,
    clear_live_registry_replay_checkpoint, load_live_registry_replay_checkpoint,
    load_live_registry_replay_checkpoint_header, stage_live_registry_replay_checkpoint,
};
pub(super) use completeness::{
    FullSourceRawLogHistoryGuard, RawLogClosureProof, has_authoritative_ens_v2_closure_through,
};
pub(super) use fence::{acquire_registry_sync_fence, release_registry_sync_fence};
use path::{
    RegistryCacheMetadata, SelectedRegistryPath, load_raw_log_closure_floor,
    load_registry_cache_metadata, load_selected_registry_target,
    raw_log_mutations_leave_cached_path_unchanged,
};

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
pub(in crate::ens_v2_registry) use checkpoint::clear_live_registry_replay_checkpoints_for_chain;
#[cfg(test)]
pub(in crate::ens_v2_registry) use checkpoint::{
    LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER, LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND,
    LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE, LIVE_REGISTRY_REPLAY_CHECKPOINT_STAGING_SCOPE,
};
pub(in crate::ens_v2_registry) use path::load_selected_registry_path_to_floor;

/// Persist the ENSv2 retained-history proof contributed by one exact provider
/// live fetch. The proof credits only the watched addresses selected for those
/// block bundles; any other required tuple must already have generation-bound
/// backfill coverage.
pub async fn record_ens_v2_live_selected_raw_log_coverage(
    pool: &PgPool,
    chain: &str,
    selected_addresses: &[String],
    selected_block_hashes: &[String],
) -> Result<()> {
    if selected_block_hashes.is_empty() {
        return Ok(());
    }
    let target_block_number = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MAX(block_number)::BIGINT
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    )
    .bind(chain)
    .bind(selected_block_hashes)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 live coverage target for {chain}"))?
    .with_context(|| format!("ENSv2 live coverage selection is absent for {chain}"))?;
    if !has_authoritative_ens_v2_closure_through(pool, chain, target_block_number).await? {
        return Ok(());
    }

    let guard = FullSourceRawLogHistoryGuard::acquire(
        acquire_registry_sync_fence(pool, chain).await?,
        chain,
    )
    .await?;
    let result = guard
        .ensure_proof_through_live_selection(
            pool,
            target_block_number,
            selected_addresses,
            selected_block_hashes,
        )
        .await;
    match result {
        Ok(_) => guard.release().await,
        Err(error) => {
            let _ = guard.abort().await;
            Err(error)
        }
    }
}

/// Rebuild the current ENSv2 retained-history proof from already-durable,
/// generation-bound coverage without running projection reconciliation.
/// Missing coverage remains a typed recovery requirement for the caller.
pub async fn ensure_ens_v2_retained_history_proof_through(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
) -> Result<()> {
    if !has_authoritative_ens_v2_closure_through(pool, chain, through_block).await? {
        return Ok(());
    }
    let guard = FullSourceRawLogHistoryGuard::acquire(
        acquire_registry_sync_fence(pool, chain).await?,
        chain,
    )
    .await?;
    match guard.ensure_proof_through(pool, through_block).await {
        Ok(_) => guard.release().await,
        Err(error) => {
            let _ = guard.abort().await;
            Err(error)
        }
    }
}

/// Apply an ordinary ENSv2 live poll from a best-effort process-local
/// lifecycle-state cache. Hydration is restricted to the exact retained
/// ancestor path of one selected target hash. Cache reuse additionally
/// requires stable raw input and discovery-admission metadata.
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
    // A live-poll call represents a complete fetch for its selected block
    // path. One transaction holds both the registry serializer and raw-log
    // mutation fence through replay, persistence, and proof advance.
    let raw_log_guard = FullSourceRawLogHistoryGuard::acquire(
        acquire_registry_sync_fence(pool, chain).await?,
        chain,
    )
    .await?;
    sync_ens_v2_registry_resource_surface_live_poll_locked(
        pool,
        deployment_profile,
        chain,
        target_block_number,
        block_hashes,
        raw_log_guard,
        max_process_cache_weight,
    )
    .await
}

async fn sync_ens_v2_registry_resource_surface_live_poll_locked(
    pool: &PgPool,
    deployment_profile: &str,
    chain: &str,
    target_block_number: i64,
    block_hashes: &[String],
    raw_log_guard: FullSourceRawLogHistoryGuard,
    max_process_cache_weight: usize,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    let current_closure_proof = raw_log_guard.load_current_proof(pool).await?;
    let target_block_hash =
        load_selected_registry_target(pool, chain, target_block_number, block_hashes).await?;
    let mut metadata_before = load_registry_cache_metadata(pool, chain).await?;
    let mut reusable = None;
    if let Some(process_cached) = take_live_registry_replay_state(pool, deployment_profile, chain)
        && let Some((proof, path)) = reusable_checkpoint_path(
            pool,
            chain,
            target_block_number,
            &target_block_hash,
            current_closure_proof,
            &metadata_before,
            process_cached.through_block_number,
            &process_cached.through_block_hash,
            process_cached.raw_log_input_revision,
            process_cached.raw_log_retention_generation,
            process_cached.discovery_admission_epoch,
        )
        .await?
    {
        reusable = Some((process_cached, proof, path));
    }
    if reusable.is_none() {
        match load_live_registry_replay_checkpoint_header(pool, deployment_profile, chain).await? {
            LiveRegistryReplayCheckpointLoad::Missing => {}
            LiveRegistryReplayCheckpointLoad::Invalid(reason) => {
                tracing::warn!(
                    deployment_profile,
                    chain,
                    reason,
                    "discarding invalid ENSv2 live checkpoint header"
                );
                clear_live_registry_replay_checkpoint(pool, deployment_profile, chain).await?;
            }
            LiveRegistryReplayCheckpointLoad::Ready(header) => {
                reusable = load_reusable_durable_checkpoint(
                    pool,
                    chain,
                    target_block_number,
                    &target_block_hash,
                    current_closure_proof,
                    &metadata_before,
                    header,
                )
                .await?;
                if reusable.is_none() {
                    clear_live_registry_replay_checkpoint(pool, deployment_profile, chain).await?;
                }
            }
        }
    }

    let (cached, closure_proof, selected_path) = if let Some((cached, proof, path)) = reusable {
        (Some(cached), proof, path)
    } else {
        let proof = if let Some(proof) = current_closure_proof {
            proof
        } else {
            raw_log_guard
                .ensure_proof_through(pool, target_block_number)
                .await?
        };
        metadata_before = load_registry_cache_metadata(pool, chain).await?;
        let registry_emitters = load_active_emitters(pool, chain, None, true).await?;
        let retained_log_floor =
            load_raw_log_closure_floor(pool, chain, target_block_number, &registry_emitters)
                .await?;
        let closure_floor =
            retained_log_floor.min(proof.proven_through_block.min(target_block_number));
        let path = load_selected_registry_path_to_floor(
            pool,
            chain,
            target_block_number,
            &target_block_hash,
            closure_floor,
        )
        .await?;
        (None, proof, path)
    };
    let pre_sync_requirements = raw_log_guard
        .load_requirements_through(pool, closure_proof, target_block_number)
        .await?;

    let sync_result = if let Some(cached) = cached {
        let incremental_block_hashes = selected_path.hashes_after(cached.through_block_number);
        if incremental_block_hashes.is_empty() {
            Ok((
                EnsV2RegistryResourceSurfaceSyncSummary::empty(0),
                cached.replay_state,
            ))
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
                Some(closure_proof.discovery_admission_epoch),
                None,
            )
            .await
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
            true,
            Some(closure_proof.discovery_admission_epoch),
            None,
        )
        .await
    };
    let (summary, replay_state) = match sync_result {
        Ok(result) => result,
        Err(error) => {
            let _ = raw_log_guard.abort().await;
            return Err(error);
        }
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
        raw_log_retention_generation: closure_proof.retention_generation,
        discovery_admission_epoch: metadata_after.discovery_admission_epoch,
        replay_state,
    };
    let fits_process_cache =
        replay_state_fits_process_cache(&snapshot.replay_state, max_process_cache_weight);
    let staged_checkpoint = if fits_process_cache {
        None
    } else {
        match stage_live_registry_replay_checkpoint(pool, deployment_profile, chain, &snapshot)
            .await
        {
            Ok(checkpoint) => Some(checkpoint),
            Err(error) => {
                let _ = raw_log_guard.abort().await;
                return Err(error
                    .context("overweight ENSv2 live replay state could not be persisted durably"));
            }
        }
    };

    raw_log_guard
        .finish(
            pool,
            closure_proof,
            target_block_number,
            summary.discovery_admission_epoch_bump_count,
            &pre_sync_requirements,
            staged_checkpoint.as_ref(),
        )
        .await?;

    if fits_process_cache {
        store_live_registry_replay_state(pool, deployment_profile, chain, snapshot);
    }
    Ok(summary)
}

async fn load_reusable_durable_checkpoint(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
    target_block_hash: &str,
    current_closure_proof: Option<RawLogClosureProof>,
    metadata: &RegistryCacheMetadata,
    header: LiveRegistryReplayCheckpointHeader,
) -> Result<
    Option<(
        CachedLiveRegistryReplayState,
        RawLogClosureProof,
        SelectedRegistryPath,
    )>,
> {
    let Some((proof, path)) = reusable_checkpoint_path(
        pool,
        chain,
        target_block_number,
        target_block_hash,
        current_closure_proof,
        metadata,
        header.through_block_number,
        &header.through_block_hash,
        header.raw_log_input_revision,
        header.raw_log_retention_generation,
        header.discovery_admission_epoch,
    )
    .await?
    else {
        tracing::warn!(
            deployment_profile = header.deployment_profile,
            chain,
            "discarding stale ENSv2 live checkpoint"
        );
        return Ok(None);
    };
    match load_live_registry_replay_checkpoint(pool, &header).await? {
        LiveRegistryReplayCheckpointLoad::Ready(snapshot) => Ok(Some((snapshot, proof, path))),
        LiveRegistryReplayCheckpointLoad::Missing => Ok(None),
        LiveRegistryReplayCheckpointLoad::Invalid(reason) => {
            tracing::warn!(
                deployment_profile = header.deployment_profile,
                chain,
                reason,
                "discarding invalid ENSv2 live checkpoint payload"
            );
            Ok(None)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn reusable_checkpoint_path(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
    target_block_hash: &str,
    current_closure_proof: Option<RawLogClosureProof>,
    metadata: &RegistryCacheMetadata,
    through_block_number: i64,
    through_block_hash: &str,
    raw_log_input_revision: i64,
    raw_log_retention_generation: i64,
    discovery_admission_epoch: i64,
) -> Result<Option<(RawLogClosureProof, SelectedRegistryPath)>> {
    let Some(proof) = current_closure_proof.filter(|proof| {
        proof.proven_through_block >= through_block_number
            && target_block_number >= through_block_number
    }) else {
        return Ok(None);
    };
    if discovery_admission_epoch != proof.discovery_admission_epoch
        || raw_log_retention_generation != proof.retention_generation
        || !metadata.retained_raw_log_history_complete
    {
        return Ok(None);
    }
    let path = load_selected_registry_path_to_floor(
        pool,
        chain,
        target_block_number,
        target_block_hash,
        through_block_number,
    )
    .await?;
    if !path.contains_anchor(through_block_number, through_block_hash)
        || !raw_log_mutations_leave_cached_path_unchanged(
            pool,
            chain,
            raw_log_input_revision,
            through_block_number,
            through_block_hash,
        )
        .await?
    {
        return Ok(None);
    }
    Ok(Some((proof, path)))
}
