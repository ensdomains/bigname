use std::time::Instant;

use super::support::*;
use crate::RunMode;

/// Reproducible database benchmark over ABI-encoded resolver updates. Setup is outside
/// the timer. Every timed run includes validation, retries, tail loading and writes, and
/// must leave all interpreted rows identical to the one-worker baseline.
///
/// BIGNAME_SPECULATION_BENCH_BLOCKS=24 BIGNAME_SPECULATION_BENCH_NAMES=100
/// BIGNAME_SPECULATION_BENCH_BATCH_BLOCKS=2 BIGNAME_SPECULATION_BENCH_WORKERS=4
/// cargo test -p bigname-interpret speculative_interpret_benchmark --release -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "database CPU/throughput benchmark; run explicitly in release mode"]
async fn speculative_interpret_benchmark() -> TestResult {
    let blocks = setting("BIGNAME_SPECULATION_BENCH_BLOCKS", 24)?;
    let names = setting("BIGNAME_SPECULATION_BENCH_NAMES", 100)?;
    let batch_blocks = setting("BIGNAME_SPECULATION_BENCH_BATCH_BLOCKS", 2)?;
    let workers = setting("BIGNAME_SPECULATION_BENCH_WORKERS", 4)?;
    anyhow::ensure!(blocks >= batch_blocks * 2 && names > 0 && workers > 1);
    for hot in [false, true] {
        let mut baseline = None;
        let mut serial_seconds = 0.0;
        for parallelism in [1, workers] {
            let database = database("interpret_spec_benchmark", "mainnet").await?;
            seed_records(database.pool(), i64::from(blocks), names as usize, hot).await?;
            let interpreter = engine(database.pool(), parallelism, batch_blocks);
            let cpu_started = process_cpu_ticks();
            let started = Instant::now();
            complete(
                &interpreter,
                CHAIN,
                FIRST,
                FIRST + i64::from(blocks) - 1,
                RunMode::Normal,
            )
            .await?;
            let elapsed = started.elapsed();
            let cpu_ticks = process_cpu_ticks()
                .zip(cpu_started)
                .and_then(|(finished, started)| finished.checked_sub(started));
            let stats = interpreter.speculation_stats();
            let stored = snapshot(database.pool(), CHAIN).await?;
            let stored_events = stored["normalized_events"].len();
            let raw_logs = u64::from(blocks) * u64::from(names) * 3;
            if let Some(baseline) = &baseline {
                assert_snapshots(&stored, baseline);
            } else {
                serial_seconds = elapsed.as_secs_f64();
                baseline = Some(stored);
            }
            eprintln!(
                "speculation-benchmark workload={} workers={parallelism} blocks={blocks} names_per_block={names} batch_blocks={batch_blocks} raw_logs={raw_logs} stored_events={stored_events} elapsed_ms={} logs_per_second={:.1} speedup={:.3} cpu_ticks={cpu_ticks:?} rss_kib={} stats={stats:?}",
                if hot {
                    "shared-names"
                } else {
                    "disjoint-names"
                },
                elapsed.as_millis(),
                raw_logs as f64 / elapsed.as_secs_f64(),
                serial_seconds / elapsed.as_secs_f64(),
                rss_kib(),
            );
            drop(interpreter);
            database.cleanup().await?;
        }
    }
    Ok(())
}

fn setting(name: &str, default: u32) -> TestResult<u32> {
    Ok(std::env::var(name).map_or(Ok(default), |value| value.parse())?)
}

// Linux exposes total process user and system CPU time in clock ticks, including workers.
// This is separate from elapsed time, which also includes database waits.
fn process_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields = stat
        .rsplit_once(')')?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let user: u64 = fields.get(11)?.parse().ok()?;
    let system: u64 = fields.get(12)?.parse().ok()?;
    user.checked_add(system)
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
