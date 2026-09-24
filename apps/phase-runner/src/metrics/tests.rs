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
fn exports_the_active_step_of_a_long_project_run() -> Result<()> {
    use bigname_project::{PROJECT_STEPS, StepObserver};

    let feed = RunnerMetricsFeed::default();
    feed.seed_chain("idle-chain");
    let metrics = PipelineMetrics::new(
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
    )?;
    feed.project_step("rebuilding", Some("resolver"));
    metrics.project_steps.apply(&feed.project_steps.snapshot());

    let scrape = metrics.registry.encode()?;
    for metric_type in [
        "# TYPE phase_runner_project_step gauge",
        "# TYPE phase_runner_project_step_index gauge",
        "# TYPE phase_runner_project_step_total gauge",
    ] {
        assert!(scrape.contains(metric_type), "missing {metric_type}");
    }
    let resolver = PROJECT_STEPS.iter().position(|step| *step == "resolver");
    for (line, value) in [
        (
            "phase_runner_project_step{chain=\"rebuilding\",step=\"resolver\"}",
            1,
        ),
        (
            "phase_runner_project_step{chain=\"rebuilding\",step=\"prepare\"}",
            0,
        ),
        (
            "phase_runner_project_step_index{chain=\"rebuilding\"}",
            resolver.map_or(-1, |index| i64::try_from(index + 1).unwrap_or(-1)),
        ),
        (
            "phase_runner_project_step_total{chain=\"rebuilding\"}",
            i64::try_from(PROJECT_STEPS.len())?,
        ),
        ("phase_runner_project_step_index{chain=\"idle-chain\"}", 0),
        ("phase_runner_project_step_total{chain=\"idle-chain\"}", 0),
    ] {
        assert!(
            scrape.contains(&format!("{line} {value}\n")),
            "missing {line} {value}"
        );
    }

    feed.project_step("rebuilding", None);
    metrics.project_steps.apply(&feed.project_steps.snapshot());
    let scrape = metrics.registry.encode()?;
    for step in PROJECT_STEPS {
        assert!(scrape.contains(&format!(
            "phase_runner_project_step{{chain=\"rebuilding\",step=\"{step}\"}} 0\n"
        )));
    }
    assert!(scrape.contains("phase_runner_project_step_index{chain=\"rebuilding\"} 0\n"));
    assert!(scrape.contains("phase_runner_project_step_total{chain=\"rebuilding\"} 0\n"));
    Ok(())
}

#[tokio::test]
async fn a_step_change_does_not_signal_the_commit_wakeup() {
    use bigname_project::StepObserver;

    let feed = RunnerMetricsFeed::default();
    feed.project_step("rebuilding", Some("prepare"));
    let wait = Duration::from_millis(50);
    assert!(
        tokio::time::timeout(wait, feed.project_steps.changed())
            .await
            .is_ok()
    );
    assert!(tokio::time::timeout(wait, feed.committed()).await.is_err());
}

#[tokio::test]
async fn the_step_worker_reaches_idle_without_the_served_lag_worker() -> Result<()> {
    use bigname_project::{PROJECT_STEPS, StepObserver};

    let feed = RunnerMetricsFeed::default();
    feed.seed_chain("rebuilding");
    let metrics = PipelineMetrics::new(
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
    )?;
    metrics.project_steps.apply(&feed.project_steps.snapshot());
    metrics.refresh_success.set(1);
    let cancellation = CancellationToken::new();
    // Only the step worker runs: no served-lag refresh ever starts or finishes, as if
    // one were held pending for the whole test.
    let worker = tokio::spawn(project_step_loop(
        metrics.project_steps.clone(),
        feed.project_steps.clone(),
        cancellation.clone(),
    ));
    let resolver = PROJECT_STEPS
        .iter()
        .position(|step| *step == "resolver")
        .map_or(-1, |index| i64::try_from(index + 1).unwrap_or(-1));
    let index_is = |value: i64| {
        let line = format!("phase_runner_project_step_index{{chain=\"rebuilding\"}} {value}\n");
        let registry = metrics.registry.clone();
        async move {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                if registry.encode()?.contains(&line) {
                    return Ok::<_, anyhow::Error>(());
                }
                ensure!(
                    tokio::time::Instant::now() < deadline,
                    "the step worker never exported {line}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    };

    feed.project_step("rebuilding", Some("resolver"));
    index_is(resolver).await?;
    feed.project_step("rebuilding", None);
    index_is(0).await?;

    // Changes recorded back to back share one wakeup; the worker applies the latest
    // snapshot, which is idle.
    feed.project_step("rebuilding", Some("resolver"));
    index_is(resolver).await?;
    for step in ["integrity", "publish", "commit"] {
        feed.project_step("rebuilding", Some(step));
    }
    feed.project_step("rebuilding", None);
    index_is(0).await?;
    let scrape = metrics.registry.encode()?;
    for step in PROJECT_STEPS {
        assert!(scrape.contains(&format!(
            "phase_runner_project_step{{chain=\"rebuilding\",step=\"{step}\"}} 0\n"
        )));
    }
    assert!(scrape.contains("phase_runner_project_step_total{chain=\"rebuilding\"} 0\n"));
    assert_eq!(
        metrics.refresh_success.get(),
        1,
        "step changes leave the refresh flag alone"
    );

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(2), worker).await??;
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
