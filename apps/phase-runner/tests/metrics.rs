#[allow(dead_code)]
mod support;

use std::{
    io::{Read, Write},
    net::SocketAddr,
};

use anyhow::{Context, Result, ensure};
use phase_runner::RunnerPhaseProgress;
use phase_runner::metrics::{RunnerLoopHeartbeat, RunnerMetricsFeed};
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
        RunnerMetricsFeed::default(),
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

#[tokio::test]
async fn endpoint_exports_served_lag_against_the_readable_project_publication() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_served_lag").await?;
    let store = PhaseStore::new(scratch.pool().clone());
    for chain in [
        "served",
        "orphaned",
        "older-hash",
        "failed",
        "published-head",
        "ingest-ahead",
        "running",
        "same-height-fork",
        "ahead-of-head",
        "no-head-row",
        "unobserved",
    ] {
        store.initialize_chain(chain).await?;
        seed_served_lag_state(scratch.pool(), chain).await?;
    }
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = 'orphaned' AND block_number = 90",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state SET target_block_number = NULL, target_block_hash = NULL
         WHERE chain_id IN ('published-head', 'unobserved') AND phase_name = 'live'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = 'older-fingerprint'
         WHERE chain_id = 'older-hash' AND phase_name = 'project'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'failed', last_error = 'terminal projection error'
         WHERE chain_id = 'failed' AND phase_name = 'project'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state SET target_block_number = 95, target_block_hash = 'x-95'
         WHERE chain_id = 'ingest-ahead' AND phase_name = 'live'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state SET phase_status = 'running', finished_at = NULL
         WHERE chain_id = 'running' AND phase_name = 'project'",
    )
    .execute(scratch.pool())
    .await?;
    // The published block is a same-height fork of the readable block 90.
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ('same-height-fork', 'same-height-fork-90b', 90, now(), 'orphaned')",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_hash = 'same-height-fork-90b', target_block_hash = 'same-height-fork-90b'
         WHERE chain_id = 'same-height-fork' AND phase_name = 'project'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_number = 90, latest_block_hash = 'ahead-of-head-90'
         WHERE chain_id = 'ahead-of-head'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_number = 100, current_block_hash = 'ahead-of-head-100',
             target_block_number = 100, target_block_hash = 'ahead-of-head-100'
         WHERE chain_id = 'ahead-of-head' AND phase_name = 'project'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query("DELETE FROM chain_heads WHERE chain_id IN ('no-head-row', 'unobserved')")
        .execute(scratch.pool())
        .await?;

    let cancellation = CancellationToken::new();
    let feed = RunnerMetricsFeed::default();
    feed.seed_chain("configured-without-rows");
    feed.seed_chain("served");
    // Publication gauges follow the family marker, independently of runner progress or
    // a failed attempt beyond that still-readable publication.
    sqlx::query(
        "INSERT INTO project_family_marker(chain_id,current_block_number,current_block_hash,
        block_timestamp,input_content_hash,sequence,state)
        SELECT chain_id,current_block_number,current_block_hash,now(),input_content_hash,1,'live'
        FROM chain_phase_state WHERE phase_name='project'",
    )
    .execute(scratch.pool())
    .await?;
    let address = phase_runner::metrics::start(
        "127.0.0.1:0".parse()?,
        scratch.pool().clone(),
        cancellation.clone(),
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
        feed.clone(),
    )
    .await?;
    let first_refresh_tick = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let response = tokio::task::spawn_blocking(move || scrape(address))
        .await
        .context("phase metrics scrape task panicked")??;
    let body = parse_http_scrape(&response)?;

    // Observed head 104, stored head 100, publication 90 unless a case changes it.
    for (chain, lag, publication) in [
        ("served", 14.0, 90.0),
        ("running", 14.0, 90.0),
        ("same-height-fork", -1.0, -1.0),
        ("ahead-of-head", -1.0, -1.0),
        ("no-head-row", -1.0, -1.0),
        ("orphaned", -1.0, -1.0),
        ("older-hash", -1.0, -1.0),
        ("failed", 14.0, 90.0),
        ("published-head", 10.0, 90.0),
        ("ingest-ahead", 10.0, 90.0),
        ("unobserved", -1.0, -1.0),
        ("configured-without-rows", -1.0, -1.0),
    ] {
        let label = format!("chain=\"{chain}\"");
        let labels = [label.as_str()];
        assert_eq!(
            sample(body, "phase_runner_served_lag_blocks", &labels)?,
            lag,
            "served lag for {chain}"
        );
        assert_eq!(
            sample(body, "phase_runner_served_publication_block", &labels)?,
            publication,
            "served publication for {chain}"
        );
    }

    // A committed family publication refreshes before the periodic tick.
    sqlx::query(
        "UPDATE project_family_marker SET current_block_number=100,
        current_block_hash='served-100', sequence=sequence+1 WHERE chain_id='served'",
    )
    .execute(scratch.pool())
    .await?;
    feed.batch_committed();
    let deadline = first_refresh_tick - std::time::Duration::from_millis(500);
    loop {
        let response = tokio::task::spawn_blocking(move || scrape(address))
            .await
            .context("phase metrics scrape task panicked")??;
        let body = parse_http_scrape(&response)?;
        let labels = ["chain=\"served\""];
        if sample(body, "phase_runner_served_publication_block", &labels)? == 100.0 {
            assert_eq!(
                sample(body, "phase_runner_served_lag_blocks", &labels)?,
                4.0
            );
            break;
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "a committed batch must refresh the served-lag gauges before the next refresh tick"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // A chain whose Project row disappears while its Live row remains reads -1.
    sqlx::query(
        "DELETE FROM chain_phase_state WHERE chain_id = 'served' AND phase_name = 'project'",
    )
    .execute(scratch.pool())
    .await?;
    feed.batch_committed();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let response = tokio::task::spawn_blocking(move || scrape(address))
            .await
            .context("phase metrics scrape task panicked")??;
        let body = parse_http_scrape(&response)?;
        let labels = ["chain=\"served\""];
        if sample(body, "phase_runner_served_publication_block", &labels)? == -1.0 {
            assert_eq!(
                sample(body, "phase_runner_served_lag_blocks", &labels)?,
                -1.0
            );
            break;
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "a chain that lost its Project row must stop reporting its old publication"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    cancellation.cancel();
    tokio::task::yield_now().await;
    scratch.cleanup().await
}

/// Chain head and Project at block 90; the latest Live batch observed block 104.
async fn seed_served_lag_state(pool: &sqlx::PgPool, chain: &str) -> Result<()> {
    for block in [90_i64, 100] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, block_number, block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, now(), 'canonical')",
        )
        .bind(chain)
        .bind(format!("{chain}-{block}"))
        .bind(block)
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $1 || '-100', 100)",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = 90,
             current_block_hash = $1 || '-90',
             target_block_number = 90,
             target_block_hash = $1 || '-90',
             input_content_hash = $2,
             started_at = now() - interval '2 minutes',
             finished_at = now() - interval '1 minute'
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .bind(phase_runner::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running',
             current_block_number = 100,
             current_block_hash = $1 || '-100',
             target_block_number = 104,
             target_block_hash = $1 || '-104',
             input_content_hash = $2,
             started_at = now() - interval '2 minutes'
         WHERE chain_id = $1 AND phase_name = 'live'",
    )
    .bind(chain)
    .bind(phase_runner::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    Ok(())
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

#[tokio::test]
async fn endpoint_exports_committed_family_publication_metrics() -> Result<()> {
    use phase_runner::{
        heads::{BlockMarker, HeadMarkers},
        phase::{Phase, PhaseContext, PhaseName, PhaseResume, RunMode},
        project_phase::ProjectPhase,
    };

    let scratch = ScratchDatabase::create("phase_runner_project_writes").await?;
    let chain = "ethereum-sepolia";
    sqlx::raw_sql(
        &include_str!("../../../crates/project/tests/rebuild_performance/seed.sql")
            .replace("__NAMES__", "40")
            .replace("__CHAIN__", chain),
    )
    .execute(scratch.pool())
    .await?;

    let cancellation = CancellationToken::new();
    let feed = RunnerMetricsFeed::default();
    let address = phase_runner::metrics::start(
        "127.0.0.1:0".parse()?,
        scratch.pool().clone(),
        cancellation.clone(),
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
        feed.clone(),
    )
    .await?;
    let project = ProjectPhase::new(scratch.pool().clone()).with_metrics_feed(feed.clone());
    let marker = |block: i64| BlockMarker::new(block, format!("0x{block:064x}"));
    let context = |target: i64, resume: Option<i64>| -> Result<PhaseContext> {
        Ok(PhaseContext {
            chain_id: chain.to_owned(),
            phase: PhaseName::Project,
            mode: RunMode::Normal,
            redo_attempt: None,
            sources: Vec::new().into(),
            available_heads: Some(HeadMarkers {
                latest: marker(target)?,
                safe: None,
                finalized: None,
            }),
            live_handoff: None,
            resume: PhaseResume {
                current: resume.map(marker).transpose()?,
                ..PhaseResume::default()
            },
        })
    };
    // The seed spreads the names over blocks 1 to 40: a full rebuild to block 30, then one batch
    // for blocks 31 to 40.
    project.run_batch(context(30, None)?).await?;
    project.run_batch(context(40, Some(30))?).await?;
    feed.batch_committed();

    let chain_label = format!("chain=\"{chain}\"");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let body = loop {
        let response = tokio::task::spawn_blocking(move || scrape(address))
            .await
            .context("phase metrics scrape task panicked")??;
        let body = parse_http_scrape(&response)?.to_owned();
        if sample(
            &body,
            "phase_runner_project_family_block_seconds_count",
            &[&chain_label],
        )
        .is_ok_and(|count| count > 0.0)
        {
            break body;
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "a committed Project batch must reach the family metrics before the next refresh tick"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    ensure!(
        sample(
            &body,
            "phase_runner_project_family_lag_blocks",
            &[&chain_label]
        )? == 0.0,
        "the family publication reaches the requested target"
    );
    sample(
        &body,
        "phase_runner_project_families_seconds",
        &[&chain_label],
    )?;
    ensure!(
        sample(
            &body,
            "phase_runner_project_family_block_seconds_count",
            &[&chain_label]
        )? > 0.0,
        "each family block is observed"
    );
    ensure!(
        !body.contains("phase_runner_project_family_skips_total"),
        "a family failure fails the batch; nothing counts skips"
    );
    cancellation.cancel();
    tokio::task::yield_now().await;
    scratch.cleanup().await
}

/// Two chains whose deployment profile admits an ENSv2 root registry, one of them with its
/// managed proxy repointed to an implementation the manifest does not list. Then a chain whose
/// proxy forwards to a listed implementation with no admitted root registry, and a chain with
/// neither. Only the admission sets `cut_over`, and only the proxy rows set `unadmitted`.
#[tokio::test]
async fn endpoint_exports_the_universal_resolver_cutover_per_chain() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_metrics_universal_resolver").await?;
    let pool = scratch.pool();
    let store = PhaseStore::new(pool.clone());
    let chains = ["admitted", "unadmitted", "proxy-only", "no-events"];
    for chain in chains {
        store.initialize_chain(chain).await?;
    }
    for (chain, family, status, role) in [
        ("admitted", "ens_v2_root_l1", "active", "root_registry"),
        ("unadmitted", "ens_v2_root_l1", "active", "root_registry"),
        ("proxy-only", "ens_v2_root_l1", "shadow", "root_registry"),
        ("proxy-only", "ens_v2_registry_l1", "active", "registry"),
    ] {
        sqlx::query(
            "INSERT INTO bigname_phase.manifest_versions (manifest_version, namespace,
                 source_family, chain_id, deployment_label, rollout_status, normalizer_version,
                 file_path, manifest_payload)
             VALUES (1, 'ens', $2, $1, 'fixture', $3, 'fixture', $1 || '/' || $2,
                 jsonb_build_object('contracts', jsonb_build_array(
                     jsonb_build_object('role', $4::text, 'address', '0xr00t', 'start_block', 5))))",
        )
        .bind(chain)
        .bind(family)
        .bind(status)
        .bind(role)
        .execute(pool)
        .await?;
    }
    for (chain, proxy, role, implementation, kind, block) in [
        (
            "admitted",
            "0xtop",
            "universal_resolver",
            "0xmanaged",
            "universal_resolver_proxy",
            10,
        ),
        (
            "admitted",
            "0xmanaged",
            "universal_resolver_managed",
            "0xv2",
            "admitted_universal_resolver",
            20,
        ),
        (
            "unadmitted",
            "0xtop",
            "universal_resolver",
            "0xmanaged",
            "universal_resolver_proxy",
            10,
        ),
        (
            "unadmitted",
            "0xmanaged",
            "universal_resolver_managed",
            "0xnew",
            "other",
            11_821_680,
        ),
        (
            "proxy-only",
            "0xtop",
            "universal_resolver",
            "0xv2",
            "admitted_universal_resolver",
            30,
        ),
    ] {
        sqlx::query(
            "INSERT INTO bigname_phase.project_universal_resolver_proxy (
                 chain_id, proxy_address, proxy_role, implementation, implementation_kind,
                 block_number, transaction_index, log_index, event_identity
             ) VALUES ($1, $2, $3, $4, $5, $6, 0, 0, $1 || $2)",
        )
        .bind(chain)
        .bind(proxy)
        .bind(role)
        .bind(implementation)
        .bind(kind)
        .bind(block)
        .execute(pool)
        .await?;
    }

    let cancellation = CancellationToken::new();
    let address = phase_runner::metrics::start(
        "127.0.0.1:0".parse()?,
        pool.clone(),
        cancellation.clone(),
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
        RunnerMetricsFeed::default(),
    )
    .await?;
    let response = tokio::task::spawn_blocking(move || scrape(address))
        .await
        .context("phase metrics scrape task panicked")??;
    cancellation.cancel();
    let body = parse_http_scrape(&response)?;
    let gauges = chains
        .iter()
        .map(|chain| {
            let label = format!("chain=\"{chain}\"");
            Ok((
                sample(body, "phase_runner_universal_resolver_cut_over", &[&label])?,
                sample(
                    body,
                    "phase_runner_universal_resolver_unadmitted",
                    &[&label],
                )?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        gauges,
        vec![(1.0, 0.0), (1.0, 1.0), (0.0, 0.0), (0.0, 0.0)],
        "per chain: (cut_over, unadmitted)"
    );
    scratch.cleanup().await
}
