use super::*;

#[expect(clippy::too_many_arguments)]
pub(super) async fn run_startup_bootstrap_backfills_inner(
    pool: &sqlx::PgPool,
    manifests_root: &Path,
    intake_chain_tasks: &[IntakeChainTask],
    provider_registry: &ProviderRegistry,
    hash_pinned_chunk_blocks: i64,
    adapter_sync_mode: BackfillAdapterSyncMode,
    replay_completed_raw_ranges: bool,
    header_audit_mode: HeaderAuditMode,
    bootstrap_backfill_workers: usize,
    bootstrap_backfill_range_blocks: i64,
    mut heartbeat: Option<&mut StartupHeartbeat>,
) -> Result<BootstrapBackfillOutcome> {
    validate_provider_registry_for_intake_tasks(intake_chain_tasks, provider_registry)?;
    let heartbeat_chain_ids = intake_chain_tasks
        .iter()
        .map(|task| task.chain.clone())
        .collect::<Vec<_>>();
    record_bootstrap_progress(pool, &mut heartbeat, &heartbeat_chain_ids).await?;
    let backfill_adapter_sync_mode = adapter_sync_mode.startup_hash_pinned_backfill_mode();
    let deployment_profile = deployment_profile_from_manifest_root(manifests_root);
    let lease_owner = format!("{}:bootstrap-backfill", default_backfill_lease_owner());
    let requested_worker_count =
        resolve_bootstrap_backfill_worker_count(bootstrap_backfill_workers);
    let effective_worker_count = effective_bootstrap_backfill_worker_count(
        requested_worker_count,
        backfill_adapter_sync_mode,
    );
    let mut outcome = BootstrapBackfillOutcome {
        active_chain_count: intake_chain_tasks.len(),
        requested_worker_count,
        effective_worker_count,
        range_partition_block_count: bootstrap_backfill_range_blocks,
        ..BootstrapBackfillOutcome::default()
    };

    for task in intake_chain_tasks {
        let skipped_unknown_start_targets =
            load_manifest_skipped_bootstrap_targets(pool, &task.chain).await?;
        outcome.skipped_unknown_start_target_count += skipped_unknown_start_targets.len();
        for skipped_target in &skipped_unknown_start_targets {
            info!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "skipped_unknown_start_target",
                chain = %task.chain,
                source_family = %skipped_target.source_family,
                contract_instance_id = %skipped_target.contract_instance_id,
                address = %skipped_target.address,
                skip_reason = %skipped_target.skip_reason,
                "manifest-declared bootstrap target is skipped because it has no declared start block"
            );
        }
        outcome
            .skipped_unknown_start_targets
            .extend(skipped_unknown_start_targets);

        let Some(provider) = provider_registry.provider_for(&task.chain) else {
            outcome.missing_provider_chain_count += 1;
            warn!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "idle_missing_provider",
                chain = %task.chain,
                intake_address_count = task.addresses.len(),
                "no provider source is configured for an active bootstrap chain; automatic bootstrap backfill will stay idle for this chain"
            );
            continue;
        };
        outcome.provider_configured_chain_count += 1;

        let heads = provider.fetch_chain_heads().await.with_context(|| {
            format!(
                "failed to fetch provider heads for bootstrap backfill on chain {}",
                task.chain
            )
        })?;
        let provider_finalized_head_block = bootstrap_finalized_head_block(&task.chain, &heads)?;
        record_bootstrap_progress(pool, &mut heartbeat, &heartbeat_chain_ids).await?;
        outcome.latched_finalized_heads.insert(
            task.chain.clone(),
            heads
                .finalized
                .clone()
                .expect("validated bootstrap heads must include finalized"),
        );
        let mut convergence_tracker = BootstrapConvergenceTracker::default();
        loop {
            let retention_snapshot = match heartbeat.as_deref_mut() {
                Some(heartbeat) => {
                    let mut progress =
                        StartupAdapterHeartbeat::new(heartbeat, &heartbeat_chain_ids);
                    load_bootstrap_retention_snapshot_with_progress(
                        pool,
                        &task.chain,
                        provider_finalized_head_block,
                        &mut progress,
                    )
                    .await?
                }
                None => {
                    load_bootstrap_retention_snapshot(
                        pool,
                        &task.chain,
                        provider_finalized_head_block,
                    )
                    .await?
                }
            };
            record_bootstrap_progress(pool, &mut heartbeat, &heartbeat_chain_ids).await?;
            let mut bootstrap_targets = load_manifest_declared_bootstrap_targets(pool, &task.chain)
                .await?
                .into_iter()
                .collect::<BTreeSet<_>>();
            let discovery_targets = load_discovery_bootstrap_targets_with_optional_progress(
                pool,
                &task.chain,
                provider_finalized_head_block,
                &mut heartbeat,
                &heartbeat_chain_ids,
            )
            .await?;
            let include_historical_bootstrap_targets = !discovery_targets.is_empty()
                || retention_snapshot.requires_ens_v2_history_recovery;
            extend_bootstrap_targets_with_progress(
                pool,
                &mut bootstrap_targets,
                discovery_targets,
                &mut heartbeat,
                &heartbeat_chain_ids,
            )
            .await?;
            if retention_snapshot.requires_ens_v2_history_recovery {
                let recovery_targets = load_retained_recovery_targets_with_optional_progress(
                    pool,
                    &task.chain,
                    provider_finalized_head_block,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
                )
                .await?;
                extend_bootstrap_targets_with_progress(
                    pool,
                    &mut bootstrap_targets,
                    recovery_targets,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
                )
                .await?;
            }
            let eligible_target_count = bootstrap_targets.len();
            let mut skipped_future_target_count = 0_usize;

            info!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "planning",
                chain = %task.chain,
                provider_finalized_head_block,
                bootstrap_backfill_range_policy = "authoritative_known_start_to_provider_finalized_head",
                hash_pinned_chunk_blocks,
                bootstrap_backfill_workers = requested_worker_count,
                effective_bootstrap_backfill_workers = effective_worker_count,
                bootstrap_backfill_range_blocks,
                eligible_bootstrap_target_count = eligible_target_count,
                raw_log_retention_generation = retention_snapshot.generation,
                ens_v2_retained_history_recovery = retention_snapshot
                    .requires_ens_v2_history_recovery,
                skipped_unknown_start_target_count = outcome.skipped_unknown_start_target_count,
                "automatic bootstrap targets loaded"
            );

            let mut target_ranges = Vec::new();
            for target in bootstrap_targets {
                let Some(range) = bootstrap_target_range(&target, provider_finalized_head_block)?
                else {
                    skipped_future_target_count += 1;
                    info!(
                        service = "indexer",
                        command = "run",
                        bootstrap_backfill_status = "skipped_future_target",
                        chain = %task.chain,
                        source_family = %target.source_family,
                        contract_instance_id = %target.contract_instance_id,
                        address = %target.address,
                        effective_from_block = target.effective_from_block,
                        effective_to_block = target.effective_to_block,
                        provider_finalized_head_block,
                        bootstrap_backfill_range_policy = "authoritative_known_start_to_provider_finalized_head",
                        "automatic bootstrap target starts after the provider finalized bootstrap head"
                    );
                    record_bootstrap_progress(pool, &mut heartbeat, &heartbeat_chain_ids).await?;
                    continue;
                };

                let mut range = range;
                let mut checkpoint_source_plan = load_bootstrap_source_plan_with_optional_progress(
                pool,
                &task.chain,
                std::slice::from_ref(&target),
                    range,
                    include_historical_bootstrap_targets,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to build bootstrap watched-target checkpoint source plan for chain {} target {} range {}..={}",
                    task.chain,
                    target.contract_instance_id,
                    range.from_block,
                    range.to_block
                )
            })?;
                narrow_bootstrap_source_plan_with_optional_progress(
                    pool,
                    &mut checkpoint_source_plan,
                    std::slice::from_ref(&target),
                    range,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
                )
                .await?;
                let checkpoint_source_identity = bootstrap_source_identity_with_optional_progress(
                    pool,
                    &checkpoint_source_plan,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
                )
                .await?;
                if let Some(stored_checkpoint) = load_bootstrap_target_checkpoint(
                    pool,
                    &deployment_profile,
                    &task.chain,
                    &checkpoint_source_identity,
                    range,
                    &target.contract_instance_id.to_string(),
                    retention_snapshot.generation,
                )
                .await?
                {
                    if stored_checkpoint >= range.to_block {
                        info!(
                            service = "indexer",
                            command = "run",
                            bootstrap_backfill_status = "skipped_target_stored_checkpoint",
                            chain = %task.chain,
                            source_family = %target.source_family,
                            contract_instance_id = %target.contract_instance_id,
                            address = %target.address,
                            from_block = range.from_block,
                            to_block = range.to_block,
                            stored_checkpoint_block = stored_checkpoint,
                            "automatic bootstrap target already has stored backfill checkpoint coverage"
                        );
                        record_bootstrap_progress(pool, &mut heartbeat, &heartbeat_chain_ids)
                            .await?;
                        continue;
                    }
                    if stored_checkpoint >= range.from_block {
                        let resumed_from_block = stored_checkpoint.checked_add(1).with_context(|| {
                        format!(
                            "stored bootstrap checkpoint {stored_checkpoint} overflowed while resuming target"
                        )
                    })?;
                        info!(
                            service = "indexer",
                            command = "run",
                            bootstrap_backfill_status = "resuming_target_after_stored_checkpoint",
                            chain = %task.chain,
                            source_family = %target.source_family,
                            contract_instance_id = %target.contract_instance_id,
                            address = %target.address,
                            from_block = range.from_block,
                            resumed_from_block,
                            to_block = range.to_block,
                            stored_checkpoint_block = stored_checkpoint,
                            "automatic bootstrap target resumes after stored backfill checkpoint"
                        );
                        range = BackfillBlockRange::new(resumed_from_block, range.to_block)?;
                    }
                }

                target_ranges.push(BootstrapBackfillTargetRange { target, range });
                record_bootstrap_progress(pool, &mut heartbeat, &heartbeat_chain_ids).await?;
            }

            let segments = plan_bootstrap_segments_with_optional_progress(
                pool,
                target_ranges,
                &mut heartbeat,
                &heartbeat_chain_ids,
            )
            .await?;
            for segment in segments {
                let segment_target_ids = bootstrap_segment_target_ids_with_optional_progress(
                    pool,
                    &segment.targets,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
                )
                .await?;
                let mut checkpoint_source_plan = load_bootstrap_source_plan_with_optional_progress(
                pool,
                &task.chain,
                &segment.targets,
                segment.range,
                    include_historical_bootstrap_targets,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to build bootstrap segment checkpoint source plan for chain {} range {}..={}",
                    task.chain, segment.range.from_block, segment.range.to_block
                )
            })?;
                narrow_bootstrap_source_plan_with_optional_progress(
                    pool,
                    &mut checkpoint_source_plan,
                    &segment.targets,
                    segment.range,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
                )
                .await?;
                let checkpoint_source_identity = bootstrap_source_identity_with_optional_progress(
                    pool,
                    &checkpoint_source_plan,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
                )
                .await?;
                let mut segment_range = segment.range;
                if let Some(stored_checkpoint) = load_bootstrap_segment_checkpoint(
                    pool,
                    &deployment_profile,
                    &task.chain,
                    &checkpoint_source_identity,
                    segment.range,
                    &segment_target_ids,
                    retention_snapshot.generation,
                )
                .await?
                {
                    if stored_checkpoint >= segment_range.to_block {
                        info!(
                            service = "indexer",
                            command = "run",
                            bootstrap_backfill_status = "skipped_stored_checkpoint",
                            chain = %task.chain,
                            from_block = segment.range.from_block,
                            to_block = segment.range.to_block,
                            stored_checkpoint_block = stored_checkpoint,
                            selected_target_count = segment.targets.len(),
                            "automatic bootstrap segment already has stored backfill checkpoint coverage"
                        );
                        continue;
                    }
                    if stored_checkpoint >= segment_range.from_block {
                        let resumed_from_block = stored_checkpoint.checked_add(1).with_context(|| {
                        format!(
                            "stored bootstrap checkpoint {stored_checkpoint} overflowed while resuming"
                        )
                    })?;
                        info!(
                            service = "indexer",
                            command = "run",
                            bootstrap_backfill_status = "resuming_after_stored_checkpoint",
                            chain = %task.chain,
                            from_block = segment.range.from_block,
                            resumed_from_block,
                            to_block = segment.range.to_block,
                            stored_checkpoint_block = stored_checkpoint,
                            selected_target_count = segment.targets.len(),
                            "automatic bootstrap segment resumes after stored backfill checkpoint"
                        );
                        segment_range =
                            BackfillBlockRange::new(resumed_from_block, segment_range.to_block)?;
                    }
                }

                let mut source_plan = load_bootstrap_source_plan_with_optional_progress(
                pool,
                &task.chain,
                &segment.targets,
                segment_range,
                    include_historical_bootstrap_targets,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to build bootstrap watched-target source plan for chain {} range {}..={}",
                    task.chain, segment_range.from_block, segment_range.to_block
                )
            })?;
                narrow_bootstrap_source_plan_with_optional_progress(
                    pool,
                    &mut source_plan,
                    &segment.targets,
                    segment_range,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
                )
                .await?;

                let source_identity = bootstrap_source_identity_with_optional_progress(
                    pool,
                    &source_plan,
                    &mut heartbeat,
                    &heartbeat_chain_ids,
                )
                .await?;
                let source_identity_hash = source_identity
                    .get("source_identity_hash")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .context("backfill source identity payload is missing source_identity_hash")?;
                let range_specs = hash_pinned_backfill_range_specs(
                    segment_range,
                    bootstrap_backfill_range_blocks,
                )?;
                let idempotency_key = if range_specs.len() == 1 {
                    bootstrap_backfill_idempotency_key(
                        &deployment_profile,
                        &task.chain,
                        &source_identity_hash,
                        segment_range,
                    )
                } else {
                    partitioned_bootstrap_backfill_idempotency_key(
                        &deployment_profile,
                        &task.chain,
                        &source_identity_hash,
                        segment_range,
                        bootstrap_backfill_range_blocks,
                    )
                };
                let config = crate::backfill::BackfillJobRunConfig {
                    deployment_profile: deployment_profile.clone(),
                    idempotency_key,
                    scope_idempotency_to_raw_log_retention_generation: true,
                    range: segment_range,
                    lease_owner: lease_owner.clone(),
                    lease_token: generated_backfill_lease_token()?,
                    lease_expires_at: backfill_lease_expires_at(
                        BOOTSTRAP_BACKFILL_LEASE_DURATION_SECS,
                    )?,
                    hash_pinned_chunk_blocks,
                    adapter_sync_mode: backfill_adapter_sync_mode,
                    header_audit_mode,
                };

                let job_outcome = if let Some(heartbeat) = heartbeat.as_deref_mut() {
                    run_resumable_hash_pinned_backfill_job_concurrently_with_heartbeat(
                        pool,
                        &source_plan,
                        provider,
                        config,
                        range_specs,
                        effective_worker_count,
                        heartbeat,
                        &heartbeat_chain_ids,
                    )
                    .await?
                } else {
                    run_resumable_hash_pinned_backfill_job_concurrently(
                        pool,
                        &source_plan,
                        provider,
                        config,
                        range_specs,
                        effective_worker_count,
                    )
                    .await?
                };
                outcome.add_job(&job_outcome);
                if replay_completed_raw_ranges && job_outcome.raw_log_count > 0 {
                    let replay_outcome = replay_raw_fact_normalized_events(
                    pool,
                    RawFactNormalizedEventReplayRequest {
                        deployment_profile: deployment_profile.clone(),
                        chain: task.chain.clone(),
                        selection: RawFactNormalizedEventReplaySelection::ScopedBlockRange {
                            from_block: job_outcome.from_block,
                            to_block: job_outcome.to_block,
                            source_scope: replay_source_scope_from_source_plan(
                                &source_plan,
                                job_outcome.from_block,
                                job_outcome.to_block,
                            ),
                        },
                    },
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to replay normalized events after bootstrap raw backfill for chain {} range {}..={}",
                        task.chain, job_outcome.from_block, job_outcome.to_block
                    )
                })?;
                    log_raw_fact_normalized_event_replay_outcome(&replay_outcome);
                    outcome.normalized_replay_job_count += 1;
                    outcome.normalized_replay_synced_count +=
                        replay_outcome.normalized_event_synced_count;
                    outcome.normalized_replay_inserted_count +=
                        replay_outcome.normalized_event_inserted_count;
                }
                record_bootstrap_progress(pool, &mut heartbeat, &heartbeat_chain_ids).await?;
            }

            let pass_status = match heartbeat.as_deref_mut() {
                Some(heartbeat) => {
                    let mut progress =
                        StartupAdapterHeartbeat::new(heartbeat, &heartbeat_chain_ids);
                    finish_bootstrap_convergence_pass_with_progress(
                        pool,
                        &task.chain,
                        provider_finalized_head_block,
                        retention_snapshot,
                        adapter_sync_mode,
                        &mut progress,
                    )
                    .await?
                }
                None => {
                    finish_bootstrap_convergence_pass(
                        pool,
                        &task.chain,
                        provider_finalized_head_block,
                        retention_snapshot,
                        adapter_sync_mode,
                    )
                    .await?
                }
            };
            if pass_status == BootstrapPassStatus::Stable {
                outcome.eligible_target_count += eligible_target_count;
                outcome.skipped_future_target_count += skipped_future_target_count;
                break;
            }
            match heartbeat.as_deref_mut() {
                Some(heartbeat) => {
                    let mut progress =
                        StartupAdapterHeartbeat::new(heartbeat, &heartbeat_chain_ids);
                    convergence_tracker
                        .record_retry(
                            pool,
                            &task.chain,
                            provider_finalized_head_block,
                            pass_status,
                            Some(&mut progress),
                        )
                        .await?;
                }
                None => {
                    convergence_tracker
                        .record_retry(
                            pool,
                            &task.chain,
                            provider_finalized_head_block,
                            pass_status,
                            None,
                        )
                        .await?;
                }
            }
            warn!(
                service = "indexer",
                command = "run",
                bootstrap_backfill_status = "retry_retention_authority_changed",
                chain = %task.chain,
                planned_raw_log_retention_generation = retention_snapshot.generation,
                planned_discovery_admission_epoch = retention_snapshot.discovery_admission_epoch,
                pass_status = ?pass_status,
                "raw-log retention or ENSv2 discovery authority changed during automatic bootstrap; retrying the complete chain planning pass"
            );
        }
    }

    super::logging::log_bootstrap_backfill_outcome(&outcome, hash_pinned_chunk_blocks);

    Ok(outcome)
}
