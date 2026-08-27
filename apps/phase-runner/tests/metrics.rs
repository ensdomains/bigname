#[allow(dead_code)]
mod support;

use std::{
    io::{Read, Write},
    net::SocketAddr,
};

use anyhow::{Context, Result, ensure};
use phase_runner::RunnerPhaseProgress;
use phase_runner::metrics::RunnerLoopHeartbeat;
use phase_runner::state::PhaseStore;
use tokio_util::sync::CancellationToken;

use support::ScratchDatabase;

#[tokio::test]
async fn endpoint_exports_failed_phase_and_stale_heartbeat_signals() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_metrics").await?;
    let chain = "ethereum-mainnet";
    PhaseStore::new(scratch.pool().clone())
        .initialize_chain(chain)
        .await?;
    seed_metric_state(scratch.pool(), chain).await?;

    let cancellation = CancellationToken::new();
    let loop_heartbeat = RunnerLoopHeartbeat::default();
    loop_heartbeat.record_progress(chain);
    let phase_progress = RunnerPhaseProgress::default();
    phase_progress.seed_chain(chain);
    let address = phase_runner::metrics::start(
        "127.0.0.1:0".parse()?,
        scratch.pool().clone(),
        cancellation.clone(),
        900,
        loop_heartbeat,
        phase_progress,
    )
    .await?;
    let response = tokio::task::spawn_blocking(move || scrape(address))
        .await
        .context("phase metrics scrape task panicked")??;
    let body = parse_http_scrape(&response)?;

    assert_eq!(
        sample(
            body,
            "phase_runner_phase_status",
            &[
                "chain=\"ethereum-mainnet\"",
                "phase=\"interpret\"",
                "status=\"failed\""
            ]
        )?,
        1.0
    );
    assert_eq!(
        sample(
            body,
            "phase_runner_heartbeat_age_seconds",
            &["chain=\"ethereum-mainnet\"", "phase=\"interpret\""]
        )?,
        -1.0,
        "a phase without a heartbeat must use the documented missing-value sentinel"
    );
    ensure!(
        sample(
            body,
            "phase_runner_heartbeat_age_seconds",
            &["chain=\"ethereum-mainnet\"", "phase=\"live\""]
        )? >= 1_190.0,
        "the heartbeat must be older than the configured 900-second stale threshold"
    );
    assert_eq!(
        sample(
            body,
            "phase_runner_phase_current_block",
            &["chain=\"ethereum-mainnet\"", "phase=\"live\""]
        )?,
        70.0
    );
    assert_eq!(
        sample(
            body,
            "phase_runner_phase_target_block",
            &["chain=\"ethereum-mainnet\"", "phase=\"live\""]
        )?,
        90.0
    );
    assert_eq!(
        sample(
            body,
            "phase_runner_head_lag_blocks",
            &["chain=\"ethereum-mainnet\"", "phase=\"live\""]
        )?,
        20.0
    );
    assert_eq!(
        sample(
            body,
            "phase_runner_verification_level",
            &["chain=\"ethereum-mainnet\"", "level=\"node_checked\""]
        )?,
        1.0
    );
    assert_eq!(
        sample(
            body,
            "phase_runner_redo_current_block",
            &["chain=\"ethereum-mainnet\"", "phase=\"project\""]
        )?,
        60.0
    );
    assert_eq!(
        sample(
            body,
            "phase_runner_reinterpretation_required",
            &["chain=\"ethereum-mainnet\""]
        )?,
        1.0
    );
    assert_eq!(
        sample(body, "phase_runner_metrics_refresh_success", &[])?,
        1.0
    );
    ensure!(
        sample(
            body,
            "phase_runner_process_start_timestamp_milliseconds",
            &[]
        )? > 0.0,
        "the endpoint must export a process-start value for restart detection"
    );
    ensure!(
        sample(
            body,
            "phase_runner_loop_heartbeat_age_seconds",
            &["chain=\"ethereum-mainnet\""]
        )? <= 1.0,
        "the in-process runner-loop heartbeat must be exported"
    );
    assert_eq!(
        sample(
            body,
            "phase_runner_heartbeat_stale_threshold_seconds",
            &["threshold_seconds=\"900\""]
        )?,
        900.0
    );
    for phase in ["ingest", "interpret", "project", "verify", "live"] {
        for mode in ["normal", "redo", "recompute_flags"] {
            for metric in [
                "phase_runner_phase_batches_since_cursor_advance",
                "phase_runner_phase_cursor_stall_age_seconds",
            ] {
                assert_eq!(
                    sample(
                        body,
                        metric,
                        &[
                            "chain=\"ethereum-mainnet\"",
                            &format!("phase=\"{phase}\""),
                            &format!("mode=\"{mode}\""),
                        ],
                    )?,
                    0.0
                );
            }
        }
    }
    for line in body
        .lines()
        .filter(|line| line.starts_with("phase_runner_phase_cursor_stall"))
    {
        ensure!(!line.contains("hash=") && !line.contains("source="));
    }
    ensure!(body.contains("build_info{"));

    cancellation.cancel();
    tokio::task::yield_now().await;
    scratch.cleanup().await
}

async fn seed_metric_state(pool: &sqlx::PgPool, chain: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, 'head-100', 100, now(), 'canonical')",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, 'head-100', 100)",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'failed',
             current_block_number = 50,
             current_block_hash = 'interpret-50',
             target_block_number = 100,
             target_block_hash = 'interpret-100',
             input_content_hash = 'older-fingerprint',
             last_error = 'terminal interpretation error',
             started_at = now() - interval '20 minutes',
             finished_at = now() - interval '10 minutes'
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running',
             current_block_number = 50,
             current_block_hash = 'project-50',
             target_block_number = 100,
             target_block_hash = 'project-100',
             input_content_hash = 'older-fingerprint',
             redo_in_progress = true,
             redo_mode = 'redo',
             redo_previous_phase_status = 'completed',
             redo_previous_started_at = now() - interval '30 minutes',
             redo_previous_finished_at = now() - interval '20 minutes',
             redo_from_block_number = 40,
             redo_to_block_number = 100,
             redo_current_block_number = 60,
             redo_current_block_hash = 'project-60',
             redo_target_block_number = 100,
             redo_target_block_hash = 'project-100',
             started_at = now() - interval '10 minutes'
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             verification_level = 'node_checked',
             current_block_number = 80,
             current_block_hash = 'verify-80',
             target_block_number = 100,
             target_block_hash = 'verify-100',
             started_at = now() - interval '20 minutes',
             finished_at = now() - interval '10 minutes'
         WHERE chain_id = $1 AND phase_name = 'verify'",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running',
             current_block_number = 70,
             current_block_hash = 'live-70',
             target_block_number = 90,
             target_block_hash = 'live-90',
             input_content_hash = $2,
             started_at = now() - interval '20 minutes'
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .bind(phase_runner::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO service_heartbeats (
             service_name, instance_id, chain_id, phase_name, started_at, heartbeat_at
         ) VALUES (
             'phase-runner', 'stalled-runner', $1, 'live',
             now() - interval '30 minutes', now() - interval '20 minutes'
         )",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    Ok(())
}

fn scrape(address: SocketAddr) -> Result<String> {
    let mut stream = std::net::TcpStream::connect(address)?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

fn parse_http_scrape(response: &str) -> Result<&str> {
    let (head, body) = response
        .split_once("\r\n\r\n")
        .context("metrics response did not contain an HTTP header boundary")?;
    ensure!(head.starts_with("HTTP/1.1 200"));
    Ok(body)
}

fn sample(body: &str, name: &str, labels: &[&str]) -> Result<f64> {
    let line = body
        .lines()
        .find(|line| line.starts_with(name) && labels.iter().all(|label| line.contains(label)))
        .with_context(|| format!("missing metric {name} with labels {labels:?}"))?;
    line.rsplit_once(' ')
        .context("metric sample is missing its value")?
        .1
        .parse()
        .with_context(|| format!("metric sample has an invalid value: {line}"))
}
