use std::{io::Read, path::Path, time::Instant};

use bigname_adapters::schema_v2::{StateCacheCapacity, prepare_schema_v2_batch_lookahead};

/// Opt-in operator experiment. All connections enforce read-only mode; no writer is called.
#[tokio::test]
#[ignore = "requires a read-only mainnet database and optional saved full-state baseline"]
async fn readonly_mainnet_batches() -> anyhow::Result<()> {
    let url = std::env::var("BIGNAME_LOOKAHEAD_PROBE_DATABASE_URL")?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET default_transaction_read_only = on")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET search_path = bigname_phase, public")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET statement_timeout = '60s'")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await?;
    let first = std::env::var("BIGNAME_LOOKAHEAD_PROBE_FROM")
        .unwrap_or_else(|_| "14684500".to_owned())
        .parse::<i64>()?;
    let batches = std::env::var("BIGNAME_LOOKAHEAD_PROBE_BATCHES")
        .unwrap_or_else(|_| "1".to_owned())
        .parse::<i64>()?;
    anyhow::ensure!(
        (1..=32).contains(&batches),
        "probe batch count must be 1..=32"
    );
    for batch in 0..batches {
        let started = Instant::now();
        let from = first + batch * 500;
        let loaded = super::batch_input(
            &pool,
            "ethereum-mainnet",
            from,
            from + 499,
            None,
            StateCacheCapacity::Entries(65_536),
        )
        .await?;
        let load_ms = started.elapsed().as_millis();
        let count = loaded.restored_event_count;
        let raw = loaded.input.raw_logs.len();
        let nodes = loaded.lookahead_nodes.unwrap();
        let prepared = prepare_schema_v2_batch_lookahead(
            loaded.input,
            loaded.provenance_manifests,
            loaded.adapter_session.unwrap(),
            &nodes,
            StateCacheCapacity::Entries(65_536),
        )?;
        let values = crate::load::prior_state_values(
            &pool,
            "ethereum-mainnet",
            from,
            prepared.state_value_requests(),
        )
        .await?;
        let (output, session) = prepared.finish(values)?;
        eprintln!(
            "lookahead from={from} nodes={} prior_events={count} raw_logs={raw} normalized_events={} load_ms={load_ms} total_ms={} rss_kib={}",
            nodes.len(),
            output.normalized_events.len(),
            started.elapsed().as_millis(),
            rss_kib()
        );
        if batch == 0
            && let Ok(path) = std::env::var("BIGNAME_LOOKAHEAD_PROBE_OUTPUT")
        {
            std::fs::write(&path, format!("{output:?}"))?;
            if let Ok(baseline) = std::env::var("BIGNAME_LOOKAHEAD_PROBE_BASELINE") {
                anyhow::ensure!(
                    same_file(Path::new(&path), Path::new(&baseline))?,
                    "lookahead complete BatchOutput differs from full-state baseline"
                );
                eprintln!("lookahead complete_output_identical=true");
            }
        }
        drop((output, session, nodes));
        eprintln!("lookahead released from={from} rss_kib={}", rss_kib());
    }
    Ok(())
}

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or(0)
}

fn same_file(left: &Path, right: &Path) -> std::io::Result<bool> {
    if std::fs::metadata(left)?.len() != std::fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = std::fs::File::open(left)?;
    let mut right = std::fs::File::open(right)?;
    let mut a = [0; 65536];
    let mut b = [0; 65536];
    loop {
        let n = left.read(&mut a)?;
        if n == 0 {
            return Ok(true);
        }
        right.read_exact(&mut b[..n])?;
        if a[..n] != b[..n] {
            return Ok(false);
        }
    }
}
