use anyhow::Result;
use tokio::time::Duration;
use tracing::info;

#[path = "startup_heartbeat.rs"]
pub(crate) mod startup_heartbeat;

use crate::{
    backfill::BackfillAdapterSyncMode,
    bootstrap_backfill::run_startup_bootstrap_backfills_with_heartbeat,
    cli::RunArgs,
    normalized_replay_catchup::{NormalizedReplayCatchupConfig, run_normalized_replay_catchup},
    provider::{ChainProviderKind, ProviderRegistry},
    provider_configuration::ProviderSourceArgs,
    reconciliation::HeaderAuditMode,
    replay::deployment_profile_from_manifest_root,
    resolver_profile_convergence::drain_resolver_profile_input_changes,
    run_mode::IndexerRunMode,
    runtime::{
        IntakeChainTask, ManifestRuntimeState, build_manifest_runtime_state_with_watch_scope,
        ensure_manifest_root_ready, intake_runtime_state, load_manifest_repository,
        log_intake_chain_tasks, log_manifest_runtime_state, log_manifest_summary,
        log_provider_registry, log_watched_chain_plan, manifest_normalized_event_kind_count,
        run_poll_loop, sync_discovery_adapter_owned_raw_log_state_with_heartbeat,
        sync_intake_chain_tasks, sync_startup_adapter_owned_raw_log_state_with_heartbeat,
        validate_provider_registry_for_intake_tasks, watched_chain_plan_state,
        widen_runtime_state_to_live_watch_scope_with_admission_epochs,
    },
};

use startup_heartbeat::StartupHeartbeat;

pub(crate) async fn run(args: RunArgs) -> Result<()> {
    let heartbeat_instance_id =
        bigname_storage::resolve_service_instance_id(args.heartbeat_instance_id.as_deref())?;
    let manifest_repository = load_manifest_repository(&args.manifests_root)?;
    let deployment_profile = deployment_profile_from_manifest_root(&args.manifests_root);
    let manifest_summary = manifest_repository.summary().clone();
    log_manifest_summary(&manifest_summary);
    ensure_manifest_root_ready(&manifest_summary)?;

    let (pool, _runtime_rederive_guard) =
        bigname_storage::connect_with_base_normalized_rederive_writer_guard(
            &args.database,
            "bigname-indexer",
        )
        .await?;
    bigname_storage::register_service_loop(
        &pool,
        bigname_storage::INDEXER_SERVICE_NAME,
        &heartbeat_instance_id,
    )
    .await?;
    let mut startup_heartbeat = StartupHeartbeat::new(
        heartbeat_instance_id.clone(),
        Duration::from_secs(args.poll_interval_secs.max(1)),
    );
    let adapter_sync_mode = BackfillAdapterSyncMode::parse(&args.hash_pinned_adapter_sync)?;
    let header_audit_mode =
        HeaderAuditMode::from_retain_audit_fields(args.retain_header_audit_fields);
    let run_mode = IndexerRunMode::new(adapter_sync_mode, args.normalized_replay_catchup_enabled);
    let manifest_runtime_state = build_manifest_runtime_state_with_watch_scope(
        &pool,
        &manifest_repository,
        run_mode.bootstrap_watch_scope,
    )
    .await?;
    let bootstrap_chain_ids = manifest_runtime_state
        .watched_chain_plan
        .iter()
        .map(|chain| chain.chain.clone())
        .collect::<Vec<_>>();
    startup_heartbeat
        .record(&pool, &bootstrap_chain_ids)
        .await?;
    if run_mode.sync_adapter_before_startup_backfill {
        sync_startup_adapter_owned_raw_log_state_with_heartbeat(
            &pool,
            &deployment_profile,
            &manifest_runtime_state.watched_chain_plan,
            args.startup_discovery_page_logs,
            &mut startup_heartbeat,
            &bootstrap_chain_ids,
        )
        .await?;
    } else {
        info!(
            service = "indexer",
            adapter_sync_mode = adapter_sync_mode.as_str(),
            bootstrap_watch_scope = run_mode.bootstrap_watch_scope.as_str(),
            "startup adapter-owned raw-log sync does not run before bootstrap backfill"
        );
    }
    log_manifest_runtime_state(&manifest_runtime_state);
    log_watched_chain_plan("startup", &manifest_runtime_state.watched_chain_plan);
    let intake_chain_tasks =
        sync_intake_chain_tasks(&pool, &manifest_runtime_state.watched_chain_plan).await?;
    let startup_chain_ids = intake_chain_tasks
        .iter()
        .map(|task| task.chain.clone())
        .collect::<Vec<_>>();
    startup_heartbeat.record(&pool, &startup_chain_ids).await?;
    log_intake_chain_tasks("startup", &intake_chain_tasks);
    let provider_registry = args.provider_registry()?;
    validate_provider_registry_for_intake_tasks(&intake_chain_tasks, &provider_registry)?;
    log_provider_registry("startup", &intake_chain_tasks, &provider_registry);
    // Automatic normalized catch-up replays bounded chunks after raw bootstrap drains.
    let replay_completed_startup_raw_ranges = false;
    let bootstrap_backfill_outcome = run_startup_bootstrap_backfills_with_heartbeat(
        &pool,
        &args.manifests_root,
        &intake_chain_tasks,
        &provider_registry,
        args.hash_pinned_chunk_blocks,
        adapter_sync_mode,
        replay_completed_startup_raw_ranges,
        header_audit_mode,
        args.bootstrap_backfill_workers,
        args.bootstrap_backfill_range_blocks,
        &mut startup_heartbeat,
    )
    .await?;
    if run_mode.sync_adapter_after_startup_backfill {
        info!(
            service = "indexer",
            adapter_sync_mode = adapter_sync_mode.as_str(),
            effective_backfill_adapter_sync_mode =
                run_mode.startup_backfill_adapter_sync_mode.as_str(),
            "startup bootstrap backfill drained; syncing adapter-owned raw-log state before live polling"
        );
        sync_startup_adapter_owned_raw_log_state_with_heartbeat(
            &pool,
            &deployment_profile,
            &manifest_runtime_state.watched_chain_plan,
            args.startup_discovery_page_logs,
            &mut startup_heartbeat,
            &startup_chain_ids,
        )
        .await?;
    } else if run_mode.sync_discovery_adapters_after_startup_backfill {
        info!(
            service = "indexer",
            adapter_sync_mode = adapter_sync_mode.as_str(),
            effective_backfill_adapter_sync_mode =
                run_mode.startup_backfill_adapter_sync_mode.as_str(),
            "startup bootstrap backfill drained; syncing only discovery-materializing adapter families before the live-plan widen"
        );
        sync_discovery_adapter_owned_raw_log_state_with_heartbeat(
            &pool,
            &deployment_profile,
            &manifest_runtime_state.watched_chain_plan,
            args.startup_discovery_page_logs,
            &mut startup_heartbeat,
            &startup_chain_ids,
        )
        .await?;
    }

    // Bootstrap backfill has drained, so the narrow bootstrap scope has served its purpose. Widen
    // before spawning replay catch-up: both reconcile `contract_instance_addresses`, and the widen
    // must not race it. The reload also picks up any discovery edges the post-bootstrap
    // adapter-owned sync just materialized, which is why it runs even when the scopes are equal.
    let (manifest_runtime_state, intake_chain_tasks, watched_plan_admission_epochs) =
        widen_to_live_watch_scope(
            &pool,
            &run_mode,
            &manifest_runtime_state,
            &provider_registry,
        )
        .await?;
    let live_chain_ids = intake_chain_tasks
        .iter()
        .map(|task| task.chain.clone())
        .collect::<Vec<_>>();
    startup_heartbeat.record(&pool, &live_chain_ids).await?;
    if adapter_sync_mode != BackfillAdapterSyncMode::RawOnly
        && !run_mode.normalized_replay_catchup_enabled
    {
        drain_resolver_profile_input_changes(&pool).await?;
    }
    if run_mode.normalized_replay_catchup_enabled {
        let catchup_config = NormalizedReplayCatchupConfig::new(
            deployment_profile.clone(),
            intake_chain_tasks
                .iter()
                .map(|task| task.chain.clone())
                .collect::<Vec<_>>(),
            args.normalized_replay_catchup_chunk_blocks,
            args.normalized_replay_catchup_max_logs_per_chunk,
            args.normalized_replay_catchup_poll_interval_secs,
        )?
        .with_defer_projection_indexes(args.normalized_replay_defer_projection_indexes);
        let catchup_pool = pool.clone();
        let catchup_provider_registry = provider_registry.clone();
        tokio::spawn(async move {
            if let Err(error) = run_normalized_replay_catchup(
                catchup_pool,
                catchup_config,
                catchup_provider_registry,
                header_audit_mode,
            )
            .await
            {
                tracing::warn!(
                    service = "indexer",
                    error = ?error,
                    "automatic normalized-event replay catch-up task exited"
                );
            }
        });
    }

    let watched_chain_plan_state =
        watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
    let intake_runtime_state = intake_runtime_state(&intake_chain_tasks);

    info!(
        service = "indexer",
        version = crate::SOFTWARE_VERSION,
        build_sha = crate::BUILD_SHA,
        schema_migration_version = bigname_storage::latest_migration_version(),
        projection_replay_version = bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
        permissions_current_publication_version = bigname_storage::PERMISSIONS_CURRENT_PUBLICATION_VERSION,
        manifests_root = %manifest_runtime_state.manifest_summary.root.display(),
        manifests_status = manifest_runtime_state.manifest_summary.status.as_str(),
        manifest_namespace_count = manifest_runtime_state.manifest_summary.namespace_count,
        manifest_source_family_count = manifest_runtime_state.manifest_summary.source_family_count,
        manifest_count = manifest_runtime_state.manifest_summary.manifest_count,
        manifest_sync_status = manifest_runtime_state.sync_summary.status.as_str(),
        synced_manifest_count = manifest_runtime_state.sync_summary.synced_manifest_count,
        synced_active_manifest_count = manifest_runtime_state.sync_summary.active_manifest_count,
        synced_root_count = manifest_runtime_state.sync_summary.root_count,
        synced_contract_count = manifest_runtime_state.sync_summary.contract_count,
        synced_capability_count = manifest_runtime_state.sync_summary.capability_count,
        synced_discovery_rule_count = manifest_runtime_state.sync_summary.discovery_rule_count,
        removed_manifest_count = manifest_runtime_state.sync_summary.removed_manifest_count,
        cleared_discovery_edge_count = manifest_runtime_state.sync_summary.cleared_discovery_edge_count,
        stored_active_manifest_count = manifest_runtime_state.discovery_admission.active_manifest_count,
        stored_active_root_count = manifest_runtime_state.discovery_admission.active_root_count,
        stored_active_contract_count = manifest_runtime_state.discovery_admission.active_contract_count,
        stored_active_rule_count = manifest_runtime_state.discovery_admission.active_rule_count,
        normalized_event_sync_total_count = manifest_runtime_state.manifest_normalized_event_summary.total_synced_count,
        normalized_event_inserted_total_count = manifest_runtime_state.manifest_normalized_event_summary.total_inserted_count,
        normalized_event_kind_count = manifest_runtime_state.manifest_normalized_event_summary.by_kind.len(),
        source_manifest_updated_event_count = manifest_normalized_event_kind_count(
            &manifest_runtime_state.manifest_normalized_event_summary,
            "SourceManifestUpdated"
        ),
        capability_changed_event_count = manifest_normalized_event_kind_count(
            &manifest_runtime_state.manifest_normalized_event_summary,
            "CapabilityChanged"
        ),
        proxy_implementation_changed_event_count = manifest_normalized_event_kind_count(
            &manifest_runtime_state.manifest_normalized_event_summary,
            "ProxyImplementationChanged"
        ),
        watched_entry_count_total = manifest_runtime_state.watched_contract_summary.source_entry_count,
        watched_manifest_root_entry_count = manifest_runtime_state.watched_contract_summary.manifest_root_count,
        watched_manifest_contract_entry_count = manifest_runtime_state.watched_contract_summary.manifest_contract_count,
        watched_discovery_edge_entry_count = manifest_runtime_state.watched_contract_summary.discovery_edge_count,
        watched_chain_count = manifest_runtime_state.watched_contract_summary.chains.len(),
        watched_runtime_chain_count = watched_chain_plan_state.chain_count,
        watched_runtime_address_count = watched_chain_plan_state.address_count,
        watched_runtime_entry_count = watched_chain_plan_state.entry_count,
        intake_runtime_chain_count = intake_runtime_state.chain_count,
        intake_runtime_address_count = intake_runtime_state.address_count,
        intake_runtime_entry_count = intake_runtime_state.entry_count,
        intake_cold_start_chain_count = intake_runtime_state.cold_start_chain_count,
        intake_resumable_chain_count = intake_runtime_state.resumable_chain_count,
        intake_safe_checkpoint_chain_count = intake_runtime_state.safe_checkpoint_chain_count,
        intake_finalized_checkpoint_chain_count = intake_runtime_state.finalized_checkpoint_chain_count,
        provider_configured_chain_count = provider_registry.configured_chain_count(),
        json_rpc_provider_configured_chain_count =
            provider_registry.configured_chain_count_by_kind(ChainProviderKind::JsonRpc),
        reth_db_provider_configured_chain_count =
            provider_registry.configured_chain_count_by_kind(ChainProviderKind::RethDb),
        bootstrap_backfill_active_chain_count = bootstrap_backfill_outcome.active_chain_count,
        bootstrap_backfill_provider_configured_chain_count = bootstrap_backfill_outcome.provider_configured_chain_count,
        bootstrap_backfill_missing_provider_chain_count = bootstrap_backfill_outcome.missing_provider_chain_count,
        bootstrap_backfill_eligible_target_count = bootstrap_backfill_outcome.eligible_target_count,
        bootstrap_backfill_drained_job_count = bootstrap_backfill_outcome.drained_job_count,
        bootstrap_backfill_skipped_future_target_count = bootstrap_backfill_outcome.skipped_future_target_count,
        bootstrap_backfill_reserved_range_count = bootstrap_backfill_outcome.reserved_range_count,
        bootstrap_backfill_completed_range_count = bootstrap_backfill_outcome.completed_range_count,
        bootstrap_backfill_range_policy = "authoritative_known_start_to_provider_finalized_head",
        bootstrap_backfill_workers = bootstrap_backfill_outcome.requested_worker_count,
        effective_bootstrap_backfill_workers = bootstrap_backfill_outcome.effective_worker_count,
        bootstrap_backfill_range_blocks = bootstrap_backfill_outcome.range_partition_block_count,
        hash_pinned_chunk_blocks = args.hash_pinned_chunk_blocks,
        hash_pinned_adapter_sync = adapter_sync_mode.as_str(),
        header_audit_mode = header_audit_mode.as_str(),
        effective_hash_pinned_backfill_adapter_sync =
            run_mode.startup_backfill_adapter_sync_mode.as_str(),
        live_poll_adapter_sync = run_mode.live_poll_adapter_sync_enabled,
        live_poll_adapter_sync_after_normalized_replay_catchup =
            run_mode.live_poll_adapter_sync_after_normalized_replay_catchup,
        normalized_replay_catchup_enabled = run_mode.normalized_replay_catchup_enabled,
        startup_discovery_page_logs = args.startup_discovery_page_logs,
        normalized_replay_catchup_chunk_blocks = args.normalized_replay_catchup_chunk_blocks,
        normalized_replay_catchup_max_logs_per_chunk = args.normalized_replay_catchup_max_logs_per_chunk,
        normalized_replay_catchup_poll_interval_secs = args.normalized_replay_catchup_poll_interval_secs,
        normalized_replay_defer_projection_indexes = args.normalized_replay_defer_projection_indexes,
        adapter_sync_on_manifest_refresh = run_mode.broad_runtime_refresh_enabled,
        manifest_observation_refresh_enabled = run_mode.broad_runtime_refresh_enabled,
        discovery_refresh_enabled = run_mode.discovery_refresh_enabled,
        watched_plan_refresh_interval_secs = args.poll_interval_secs,
        poll_interval_secs = args.poll_interval_secs,
        bootstrap_watch_scope = run_mode.bootstrap_watch_scope.as_str(),
        live_watch_scope = run_mode.live_watch_scope.as_str(),
        "indexer booted"
    );

    run_poll_loop(
        &pool,
        &heartbeat_instance_id,
        args.manifests_root,
        manifest_runtime_state,
        intake_chain_tasks,
        watched_plan_admission_epochs,
        &provider_registry,
        args.poll_interval_secs,
        run_mode.live_watch_scope,
        run_mode.broad_runtime_refresh_enabled,
        run_mode.live_poll_adapter_sync_enabled,
        run_mode.live_poll_adapter_sync_after_normalized_replay_catchup,
        run_mode.broad_runtime_refresh_enabled,
        run_mode.discovery_refresh_enabled,
        run_mode.resolver_profile_convergence_enabled,
        run_mode.broad_runtime_refresh_enabled,
        header_audit_mode,
        args.event_silent_reverse_resolver_addresses,
        bootstrap_backfill_outcome.latched_finalized_heads,
        // Process-lifetime verified-coverage frontier: deep-gap promotion
        // verifies fact coverage in large chunks once, then every poll cycle
        // is an O(1) in-memory check.
        &crate::reconciliation::ChainCoverageFrontiers::default(),
    )
    .await
}

async fn widen_to_live_watch_scope(
    pool: &sqlx::PgPool,
    run_mode: &IndexerRunMode,
    manifest_runtime_state: &ManifestRuntimeState,
    provider_registry: &ProviderRegistry,
) -> Result<(
    ManifestRuntimeState,
    Vec<IntakeChainTask>,
    std::collections::BTreeMap<String, i64>,
)> {
    // No equal-scope short-circuit: even when bootstrap already ran at the live scope (`inline`),
    // the post-bootstrap adapter-owned sync may have materialized new discovery edges, and this
    // reload is what carries them into the live plan.
    let previous_watch_state = watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
    let (live_manifest_runtime_state, watched_plan_admission_epochs) =
        widen_runtime_state_to_live_watch_scope_with_admission_epochs(pool, manifest_runtime_state)
            .await?;
    let live_intake_chain_tasks =
        sync_intake_chain_tasks(pool, &live_manifest_runtime_state.watched_chain_plan).await?;
    validate_provider_registry_for_intake_tasks(&live_intake_chain_tasks, provider_registry)?;

    let live_watch_state =
        watched_chain_plan_state(&live_manifest_runtime_state.watched_chain_plan);
    info!(
        service = "indexer",
        bootstrap_watch_scope = run_mode.bootstrap_watch_scope.as_str(),
        live_watch_scope = run_mode.live_watch_scope.as_str(),
        previous_watched_address_count = previous_watch_state.address_count,
        watched_address_count = live_watch_state.address_count,
        watched_entry_count_total = live_watch_state.entry_count,
        "widened watch plan from bootstrap scope to live scope before polling"
    );
    log_watched_chain_plan("live", &live_manifest_runtime_state.watched_chain_plan);
    log_intake_chain_tasks("live", &live_intake_chain_tasks);

    Ok((
        live_manifest_runtime_state,
        live_intake_chain_tasks,
        watched_plan_admission_epochs,
    ))
}
