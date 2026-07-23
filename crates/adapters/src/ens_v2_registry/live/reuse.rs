use super::*;

pub(super) async fn load_reusable_durable_checkpoint(
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
pub(super) async fn reusable_checkpoint_path(
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
