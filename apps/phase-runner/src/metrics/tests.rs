use super::*;
use crate::{
    heads::BlockMarker,
    phase::{
        BlockRange, PhaseBatchOutcome, PhaseContext, PhaseName, PhaseProgress, PhaseResume,
        RedoAttemptFence, RunMode,
    },
};

fn metric_row(phase: &str) -> PhaseMetricRow {
    PhaseMetricRow {
        chain_id: "ethereum-mainnet".to_owned(),
        phase_name: phase.to_owned(),
        phase_status: "running".to_owned(),
        verification_level: None,
        current_block_number: Some(70),
        target_block_number: Some(100),
        input_content_hash: Some(crate::INTERPRETER_CONTENT_HASH.to_owned()),
        redo_in_progress: false,
        redo_mode: None,
        redo_current_block_number: None,
        redo_target_block_number: None,
        heartbeat_age_seconds: Some(600),
        chain_head_block_number: Some(100),
    }
}

#[test]
fn registers_the_pipeline_metric_families_with_build_identity() -> Result<()> {
    let loop_heartbeat = RunnerLoopHeartbeat::default();
    loop_heartbeat.record_progress("ethereum-mainnet");
    let progress = RunnerPhaseProgress::default();
    progress.seed_chain("ethereum-mainnet");
    let metrics = PipelineMetrics::new(900, loop_heartbeat, progress)?;
    metrics.apply_phase_progress();
    metrics.apply_rows(&[metric_row("interpret")])?;
    metrics.served_lag.apply(&[served_lag::ServedLagRow {
        chain_id: "ethereum-mainnet".to_owned(),
        observed_head_block_number: Some(104),
        publication_block_number: Some(100),
    }]);

    let scrape = metrics.registry.encode()?;
    for metric_type in [
        "# TYPE build_info gauge",
        "# TYPE phase_runner_phase_current_block gauge",
        "# TYPE phase_runner_phase_status gauge",
        "# TYPE phase_runner_process_start_timestamp_milliseconds gauge",
        "# TYPE phase_runner_heartbeat_age_seconds gauge",
        "# TYPE phase_runner_heartbeat_stale_threshold_seconds gauge",
        "# TYPE phase_runner_loop_heartbeat_age_seconds gauge",
        "# TYPE phase_runner_head_lag_blocks gauge",
        "# TYPE phase_runner_reinterpretation_required gauge",
        "# TYPE phase_runner_phase_batches_since_cursor_advance gauge",
        "# TYPE phase_runner_phase_cursor_stall_age_seconds gauge",
        "# TYPE phase_runner_served_lag_blocks gauge",
        "# TYPE phase_runner_served_publication_block gauge",
    ] {
        assert!(scrape.contains(metric_type), "missing {metric_type}");
    }
    assert!(scrape.contains("phase_runner_served_lag_blocks{chain=\"ethereum-mainnet\"} 4"));
    assert!(
        scrape.contains("phase_runner_served_publication_block{chain=\"ethereum-mainnet\"} 100")
    );
    assert!(scrape.contains("build_sha="));
    assert!(scrape.contains("interpreter_content_hash="));
    Ok(())
}

#[test]
fn updates_failure_freshness_lag_verification_and_redo_signals() -> Result<()> {
    let loop_heartbeat = RunnerLoopHeartbeat::default();
    loop_heartbeat.record_progress("ethereum-mainnet");
    let metrics = PipelineMetrics::new(900, loop_heartbeat, RunnerPhaseProgress::default())?;
    metrics.apply_rows(&[metric_row("interpret")])?;
    assert_eq!(
        metrics
            .phase_status
            .with_label_values(&["ethereum-mainnet", "interpret", "running"])
            .get(),
        1
    );
    let mut interpret = metric_row("interpret");
    interpret.phase_status = "failed".to_owned();
    interpret.input_content_hash = Some("older-fingerprint".to_owned());
    let mut project = metric_row("project");
    project.redo_in_progress = true;
    project.redo_mode = Some("redo".to_owned());
    project.redo_current_block_number = Some(75);
    project.redo_target_block_number = Some(100);
    let mut verify = metric_row("verify");
    verify.phase_status = "completed".to_owned();
    verify.verification_level = Some("node_checked".to_owned());

    metrics.apply_rows(&[interpret, project, verify])?;

    assert_eq!(
        metrics
            .phase_status
            .with_label_values(&["ethereum-mainnet", "interpret", "failed"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .phase_status
            .with_label_values(&["ethereum-mainnet", "interpret", "running"])
            .get(),
        0
    );
    assert_eq!(
        metrics
            .heartbeat_age_seconds
            .with_label_values(&["ethereum-mainnet", "interpret"])
            .get(),
        600
    );
    assert_eq!(
        metrics
            .loop_heartbeat_age_seconds
            .with_label_values(&["ethereum-mainnet"])
            .get(),
        0
    );
    assert_eq!(
        metrics
            .head_lag_blocks
            .with_label_values(&["ethereum-mainnet", "interpret"])
            .get(),
        30
    );
    assert_eq!(
        metrics
            .verification_level
            .with_label_values(&["ethereum-mainnet", "node_checked"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .redo_mode
            .with_label_values(&["ethereum-mainnet", "project", "redo"])
            .get(),
        1
    );
    assert_eq!(
        metrics
            .reinterpretation_required
            .with_label_values(&["ethereum-mainnet"])
            .get(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn a_failed_refresh_after_a_commit_reports_refresh_failure() -> Result<()> {
    let metrics = PipelineMetrics::new(
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
    )?;
    metrics.refresh_success.set(1);
    let unreachable = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy("postgres://bigname@127.0.0.1:1/bigname")?;

    assert!(metrics.refresh_after_commit(&unreachable).await.is_err());
    assert_eq!(metrics.refresh_success.get(), 0);
    Ok(())
}

#[test]
fn an_incoherent_observed_head_warns_once_per_change() -> Result<()> {
    let metrics = PipelineMetrics::new(
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
    )?;
    let lag = &metrics.served_lag;
    assert!(lag.incoherent_changed("chain", Some((99, 100))));
    assert!(!lag.incoherent_changed("chain", Some((99, 100))));
    assert!(lag.incoherent_changed("chain", Some((98, 100))));
    assert!(!lag.incoherent_changed("chain", None));
    assert!(lag.incoherent_changed("chain", Some((98, 100))));
    Ok(())
}

#[test]
fn served_gauges_reconcile_chains_the_query_no_longer_returns() -> Result<()> {
    let metrics = PipelineMetrics::new(
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
    )?;
    metrics.served_lag.configure(&[
        "project-row-gone".to_owned(),
        "configured-emptied".to_owned(),
    ]);
    let phase = |chain: &str, phase: &str| PhaseMetricRow {
        chain_id: chain.to_owned(),
        ..metric_row(phase)
    };
    let served = |chain: &str| served_lag::ServedLagRow {
        chain_id: chain.to_owned(),
        observed_head_block_number: Some(104),
        publication_block_number: Some(100),
    };
    metrics.apply_rows(&[
        phase("configured-emptied", "project"),
        phase("project-row-gone", "live"),
        phase("project-row-gone", "project"),
        phase("unconfigured-emptied", "project"),
    ])?;
    metrics.served_lag.apply(&[
        served("configured-emptied"),
        served("project-row-gone"),
        served("unconfigured-emptied"),
    ]);
    let scrape = metrics.registry.encode()?;
    assert!(scrape.contains("phase_runner_served_lag_blocks{chain=\"project-row-gone\"} 4\n"));

    // Every Project row is gone; one chain keeps its Live row.
    metrics.apply_rows(&[phase("project-row-gone", "live")])?;
    metrics.served_lag.apply(&[]);

    let scrape = metrics.registry.encode()?;
    for chain in ["project-row-gone", "configured-emptied"] {
        for gauge in [
            "phase_runner_served_lag_blocks",
            "phase_runner_served_publication_block",
        ] {
            let line = format!("{gauge}{{chain=\"{chain}\"}} -1\n");
            assert!(scrape.contains(&line), "missing {line}");
        }
    }
    assert!(
        !scrape.contains("chain=\"unconfigured-emptied\""),
        "a chain that is neither configured nor returned leaves the served gauges"
    );
    Ok(())
}

#[test]
fn served_lag_is_unavailable_without_both_sides() {
    assert_eq!(served_lag::served_lag(Some(104), Some(100)), 4);
    assert_eq!(
        served_lag::served_lag(Some(99), Some(100)),
        -1,
        "an observed head below the publication is unavailable, not caught up"
    );
    assert_eq!(served_lag::served_lag(Some(100), Some(100)), 0);
    assert_eq!(served_lag::served_lag(None, Some(100)), -1);
    assert_eq!(served_lag::served_lag(Some(104), None), -1);
    assert_eq!(served_lag::served_lag(None, None), -1);
}

#[test]
fn head_lag_uses_the_observed_provider_target() {
    let mut live = metric_row("live");
    live.current_block_number = Some(70);
    live.target_block_number = Some(90);
    live.chain_head_block_number = Some(100);

    assert_eq!(head_lag(&live), 20);
}

#[test]
fn configured_chain_loop_heartbeat_does_not_require_phase_rows() -> Result<()> {
    let loop_heartbeat = RunnerLoopHeartbeat::default();
    loop_heartbeat.record_progress("new-chain");
    loop_heartbeat.record_progress("queued-chain");
    let metrics = PipelineMetrics::new(900, loop_heartbeat, RunnerPhaseProgress::default())?;

    metrics.apply_rows(&[])?;

    assert_eq!(
        metrics
            .loop_heartbeat_age_seconds
            .with_label_values(&["new-chain"])
            .get(),
        0
    );
    metrics.loop_heartbeat.remove_progress("new-chain");
    assert_eq!(metrics.loop_heartbeat.age_seconds("new-chain"), None);
    assert_eq!(metrics.loop_heartbeat.age_seconds("queued-chain"), Some(0));
    Ok(())
}

#[test]
fn progress_snapshots_keep_normal_and_repair_modes_isolated() -> Result<()> {
    let progress = RunnerPhaseProgress::default();
    progress.seed_chain("chain");
    let metrics = PipelineMetrics::new(900, RunnerLoopHeartbeat::default(), progress.clone())?;
    for mode in [
        RunMode::Normal,
        RunMode::Redo(BlockRange::new(1, 9)?),
        RunMode::RecomputeFlags(BlockRange::new(1, 9)?),
    ] {
        let context = progress_context(mode.clone());
        let first = progress.begin_batch(&context);
        progress.record_committed(first, &pinned_outcome());
        let second = progress.begin_batch(&context);
        progress.record_committed(second, &pinned_outcome());
    }
    metrics.apply_phase_progress();

    for mode in ["normal", "redo", "recompute_flags"] {
        assert_eq!(
            metrics
                .batches_since_cursor_advance
                .with_label_values(&["chain", "interpret", mode])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .cursor_stall_age_seconds
                .with_label_values(&["chain", "interpret", mode])
                .get(),
            0
        );
    }

    progress.clear_phase("chain", PhaseName::Interpret);
    metrics.apply_phase_progress();
    for mode in ["normal", "redo", "recompute_flags"] {
        assert_eq!(
            metrics
                .batches_since_cursor_advance
                .with_label_values(&["chain", "interpret", mode])
                .get(),
            0
        );
    }
    Ok(())
}

fn progress_context(mode: RunMode) -> PhaseContext {
    let execution_range = mode.range();
    PhaseContext {
        chain_id: "chain".into(),
        phase: PhaseName::Interpret,
        mode,
        redo_attempt: execution_range.map(|execution_range| RedoAttemptFence {
            generation: 4,
            execution_range,
        }),
        sources: Arc::from([]),
        available_heads: None,
        live_handoff: None,
        resume: PhaseResume {
            current: Some(BlockMarker::new(1, "one").expect("marker")),
            ..PhaseResume::default()
        },
    }
}

fn pinned_outcome() -> PhaseBatchOutcome {
    PhaseBatchOutcome::Continue(PhaseProgress {
        current: Some(BlockMarker::new(1, "one").expect("marker")),
        ..PhaseProgress::default()
    })
}

#[test]
fn project_batches_set_last_batch_gauges_and_add_up_written_rows() -> Result<()> {
    let metrics = PipelineMetrics::new(
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
    )?;
    let feed = RunnerMetricsFeed::default();
    let batch = |blocks: u64, inserted: u64, deleted: u64| bigname_project::WriteSummary {
        blocks,
        changed_events: blocks * 3,
        staged_events: blocks * 40,
        scope_keys: [("names", blocks * 2), ("resources", blocks)].into(),
        deleted: [("name_current", deleted)].into(),
        inserted: [("name_current", inserted), ("child_registration_events", 1)].into(),
        stage_elapsed_ms: [("scope", 250), ("publish", 1_500)].into(),
    };
    // Two batches commit before the metrics task wakes: the gauges show the second, the counter
    // adds both.
    feed.project_batch("ethereum-sepolia", &batch(1, 5, 4));
    feed.project_batch("ethereum-sepolia", &batch(2, 7, 6));
    metrics.project_writes.apply(feed.take_project_writes());
    // Nothing is pending after an apply, so a refresh with no new batch changes nothing.
    metrics.project_writes.apply(feed.take_project_writes());

    let scrape = metrics.registry.encode()?;
    for line in [
        "# TYPE phase_runner_project_rows_written_total counter",
        "# TYPE phase_runner_project_stage_duration_seconds gauge",
        "phase_runner_project_batch_blocks{chain=\"ethereum-sepolia\"} 2\n",
        "phase_runner_project_changed_events{chain=\"ethereum-sepolia\"} 6\n",
        "phase_runner_project_staged_events{chain=\"ethereum-sepolia\"} 80\n",
        "phase_runner_project_scope_keys{chain=\"ethereum-sepolia\",scope=\"names\"} 4\n",
        "phase_runner_project_scope_keys{chain=\"ethereum-sepolia\",scope=\"resources\"} 2\n",
        "phase_runner_project_rows_written_total{chain=\"ethereum-sepolia\",kind=\"inserted\",table=\"name_current\"} 12\n",
        "phase_runner_project_rows_written_total{chain=\"ethereum-sepolia\",kind=\"deleted\",table=\"name_current\"} 10\n",
        "phase_runner_project_rows_written_total{chain=\"ethereum-sepolia\",kind=\"inserted\",table=\"child_registration_events\"} 2\n",
        "phase_runner_project_stage_duration_seconds{chain=\"ethereum-sepolia\",stage=\"publish\"} 1.5\n",
        "phase_runner_project_stage_duration_seconds{chain=\"ethereum-sepolia\",stage=\"scope\"} 0.25\n",
    ] {
        assert!(scrape.contains(line), "missing {line}");
    }
    Ok(())
}

#[test]
fn family_loops_set_their_own_time_and_lag_and_count_skips() -> Result<()> {
    let metrics = PipelineMetrics::new(
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
    )?;
    let feed = RunnerMetricsFeed::default();
    let marker = |number: i64| bigname_project::Marker {
        number,
        hash: format!("0x{number:064x}"),
    };
    let outcome = |elapsed_ms: u64, current: i64, skipped: Option<&str>| {
        bigname_project::families::FamilyOutcome {
            target: Some(marker(14)),
            marker: Some(marker(current)),
            elapsed_ms,
            skipped: skipped.map(str::to_owned),
            ..Default::default()
        }
    };
    feed.project_families(
        "ethereum-sepolia",
        &outcome(40, 12, Some("block 13: failed")),
    );
    feed.project_families("ethereum-sepolia", &outcome(250, 14, None));
    metrics.project_writes.apply(feed.take_project_writes());

    let scrape = metrics.registry.encode()?;
    for line in [
        "# TYPE phase_runner_project_family_skips_total counter",
        "phase_runner_project_families_seconds{chain=\"ethereum-sepolia\"} 0.25\n",
        "phase_runner_project_family_lag_blocks{chain=\"ethereum-sepolia\"} 0\n",
        "phase_runner_project_family_skips_total{chain=\"ethereum-sepolia\"} 1\n",
    ] {
        assert!(scrape.contains(line), "missing {line}");
    }
    Ok(())
}
