use super::*;

pub(super) fn ens_v1_resolver_source_family_range(
    target_ranges: &[BootstrapBackfillTargetRange],
) -> Result<Option<BackfillBlockRange>> {
    let from_block = target_ranges
        .iter()
        .map(|target_range| target_range.range.from_block)
        .min();
    let to_block = target_ranges
        .iter()
        .map(|target_range| target_range.range.to_block)
        .max();
    match (from_block, to_block) {
        (Some(from_block), Some(to_block)) => {
            BackfillBlockRange::new(from_block, to_block).map(Some)
        }
        (None, None) => Ok(None),
        _ => unreachable!("ENSv1 resolver target range min/max must be internally consistent"),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_bootstrap_source_family_backfill(
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    deployment_profile: &str,
    lease_owner: &str,
    chain: &str,
    source_family: &str,
    mut range: BackfillBlockRange,
    provider: &(impl ChainProviderOps + ?Sized),
    hash_pinned_chunk_blocks: i64,
    adapter_sync_mode: BackfillAdapterSyncMode,
    replay_completed_raw_ranges: bool,
    header_audit_mode: HeaderAuditMode,
    outcome: &mut BootstrapBackfillOutcome,
) -> Result<()> {
    let empty_target_ids = std::collections::BTreeSet::new();
    if let Some(stored_checkpoint) = load_bootstrap_segment_checkpoint(
        pool,
        deployment_profile,
        manifests_root,
        chain,
        range,
        &empty_target_ids,
    )
    .await?
    {
        if stored_checkpoint >= range.to_block {
            info!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "skipped_source_family_stored_checkpoint",
                chain,
                source_family,
                from_block = range.from_block,
                to_block = range.to_block,
                stored_checkpoint_block = stored_checkpoint,
                "source-family bootstrap backfill already has stored checkpoint coverage"
            );
            return Ok(());
        }
        if stored_checkpoint >= range.from_block {
            let resumed_from_block = stored_checkpoint.checked_add(1).with_context(|| {
                format!(
                    "stored source-family bootstrap checkpoint {stored_checkpoint} overflowed while resuming"
                )
            })?;
            info!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "resuming_source_family_after_stored_checkpoint",
                chain,
                source_family,
                from_block = range.from_block,
                resumed_from_block,
                to_block = range.to_block,
                stored_checkpoint_block = stored_checkpoint,
                "source-family bootstrap backfill resumes after stored checkpoint"
            );
            range = BackfillBlockRange::new(resumed_from_block, range.to_block)?;
        }
    }

    let source_plan = load_manifest_declared_watched_source_selector_plan(
        pool,
        chain,
        WatchedSourceSelector::SourceFamily(source_family.to_owned()),
        range.from_block,
        range.to_block,
    )
    .await
    .with_context(|| {
        format!(
            "failed to build bootstrap source-family plan for chain {chain} source_family {source_family} range {}..={}",
            range.from_block, range.to_block
        )
    })?;
    let source_identity_hash = source_identity_hash_for_backfill(&source_plan)?;
    let config = crate::backfill::BackfillJobRunConfig {
        deployment_profile: deployment_profile.to_owned(),
        idempotency_key: bootstrap_backfill_idempotency_key(
            deployment_profile,
            manifests_root,
            chain,
            &source_identity_hash,
            range,
        ),
        range,
        lease_owner: lease_owner.to_owned(),
        lease_token: generated_backfill_lease_token()?,
        lease_expires_at: backfill_lease_expires_at(BOOTSTRAP_BACKFILL_LEASE_DURATION_SECS)?,
        hash_pinned_chunk_blocks,
        adapter_sync_mode,
        header_audit_mode,
    };

    let job_outcome =
        run_resumable_hash_pinned_backfill_job(pool, &source_plan, provider, config).await?;
    outcome.add_job(&job_outcome);
    if replay_completed_raw_ranges && job_outcome.raw_log_count > 0 {
        let replay_outcome = replay_raw_fact_normalized_events(
            pool,
            RawFactNormalizedEventReplayRequest {
                deployment_profile: deployment_profile.to_owned(),
                chain: chain.to_owned(),
                selection: RawFactNormalizedEventReplaySelection::ScopedBlockRange {
                    from_block: job_outcome.from_block,
                    to_block: job_outcome.to_block,
                    source_scope: replay_source_scope_from_source_plan(&source_plan),
                },
            },
        )
        .await
        .with_context(|| {
            format!(
                "failed to replay normalized events after bootstrap source-family raw backfill for chain {chain} range {}..={}",
                job_outcome.from_block, job_outcome.to_block
            )
        })?;
        log_raw_fact_normalized_event_replay_outcome(&replay_outcome);
        outcome.normalized_replay_job_count += 1;
        outcome.normalized_replay_synced_count += replay_outcome.normalized_event_synced_count;
        outcome.normalized_replay_inserted_count += replay_outcome.normalized_event_inserted_count;
    }

    Ok(())
}
