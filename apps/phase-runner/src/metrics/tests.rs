use super::*;

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
    let metrics = PipelineMetrics::new(900)?;
    metrics.apply_rows(&[metric_row("interpret")])?;

    let scrape = metrics.registry.encode()?;
    for metric_type in [
        "# TYPE build_info gauge",
        "# TYPE phase_runner_phase_current_block gauge",
        "# TYPE phase_runner_phase_status gauge",
        "# TYPE phase_runner_heartbeat_age_seconds gauge",
        "# TYPE phase_runner_heartbeat_stale_threshold_seconds gauge",
        "# TYPE phase_runner_head_lag_blocks gauge",
        "# TYPE phase_runner_reinterpretation_required gauge",
    ] {
        assert!(scrape.contains(metric_type), "missing {metric_type}");
    }
    assert!(scrape.contains("build_sha="));
    assert!(scrape.contains("interpreter_content_hash="));
    Ok(())
}

#[test]
fn updates_failure_freshness_lag_verification_and_redo_signals() -> Result<()> {
    let metrics = PipelineMetrics::new(900)?;
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

#[test]
fn head_lag_uses_the_observed_provider_target() {
    let mut live = metric_row("live");
    live.current_block_number = Some(70);
    live.target_block_number = Some(90);
    live.chain_head_block_number = Some(100);

    assert_eq!(head_lag(&live), 20);
}
