//! Served-lag gauges measure the live family publication; rebuilding, absent and foreign
//! markers report unavailable even when an older Project progress row is present.
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

/// Blocks 90, 95 and 100 readable, the stored head at 100, Live's observed head at 104, and the
/// Project row completed at 90, older than the live family publication.
async fn seed_chain(pool: &sqlx::PgPool, chain: &str) -> Result<()> {
    for block in [90_i64, 95, 100] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, block_number, block_timestamp, canonicality_state
             ) VALUES ($1, $1 || '-' || $2, $2, now(), 'canonical')",
        )
        .bind(chain)
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
    for (phase, status, block, target) in [
        ("project", "completed", 90, 90),
        ("live", "running", 100, 104),
    ] {
        sqlx::query(
            "UPDATE chain_phase_state
             SET phase_status = $3, current_block_number = $4,
                 current_block_hash = $1 || '-' || $4, target_block_number = $5,
                 target_block_hash = $1 || '-' || $5, input_content_hash = $6,
                 started_at = now() - interval '2 minutes',
                 finished_at = CASE WHEN $3 = 'completed' THEN now() END
             WHERE chain_id = $1 AND phase_name = $2",
        )
        .bind(chain)
        .bind(phase)
        .bind(status)
        .bind(block)
        .bind(target)
        .bind(phase_runner::INTERPRETER_CONTENT_HASH)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_marker(pool: &sqlx::PgPool, chain: &str, state: &str, hash: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_family_marker (
             chain_id, current_block_number, current_block_hash, block_timestamp,
             input_content_hash, sequence, state
         ) VALUES ($1, 95, $1 || '-95', now(), $3, 4, $2)",
    )
    .bind(chain)
    .bind(state)
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

async fn scrape_gauges(pool: &sqlx::PgPool, chains: &[&str]) -> Result<Vec<(f64, f64)>> {
    let cancellation = CancellationToken::new();
    let feed = RunnerMetricsFeed::default();
    let address = phase_runner::metrics::start(
        "127.0.0.1:0".parse()?,
        pool.clone(),
        cancellation.clone(),
        900,
        RunnerLoopHeartbeat::default(),
        RunnerPhaseProgress::default(),
        feed,
    )
    .await?;
    let response = tokio::task::spawn_blocking(move || scrape(address))
        .await
        .context("phase metrics scrape task panicked")??;
    cancellation.cancel();
    let body = parse_http_scrape(&response)?;
    chains
        .iter()
        .map(|chain| {
            let label = format!("chain=\"{chain}\"");
            Ok((
                sample(body, "phase_runner_served_lag_blocks", &[&label])?,
                sample(body, "phase_runner_served_publication_block", &[&label])?,
            ))
        })
        .collect()
}

#[tokio::test]
async fn served_lag_measures_only_the_live_family_marker() -> Result<()> {
    let scratch = ScratchDatabase::create("phase_runner_served_lag_families").await?;
    let pool = scratch.pool();
    let store = PhaseStore::new(pool.clone());
    let chains = ["live-marker", "rebuilding", "older-hash", "no-marker"];
    for chain in chains {
        store.initialize_chain(chain).await?;
        seed_chain(pool, chain).await?;
    }
    seed_marker(
        pool,
        "live-marker",
        "live",
        phase_runner::INTERPRETER_CONTENT_HASH,
    )
    .await?;
    seed_marker(
        pool,
        "rebuilding",
        "bootstrap_pending",
        phase_runner::INTERPRETER_CONTENT_HASH,
    )
    .await?;
    seed_marker(pool, "older-hash", "live", "older-fingerprint").await?;

    // Only a live marker of this build is a publication; rebuilding, absent and
    // foreign markers report unavailable (-1).
    assert_eq!(
        scrape_gauges(pool, &chains).await?,
        vec![(9.0, 95.0), (-1.0, -1.0), (-1.0, -1.0), (-1.0, -1.0)]
    );
    scratch.cleanup().await
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
