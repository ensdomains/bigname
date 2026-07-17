use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::backfill::{BackfillBlockRange, BackfillJobRunConfig};

use super::config::CapacityGuardConfig;

pub(super) const CAPACITY_FAILURE_REASON: &str = "ops catch-up capacity guard breached";

static NEXT_CAPACITY_PROBE_ID: AtomicU64 = AtomicU64::new(0);

#[rustfmt::skip]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CapacitySnapshot { pub(super) postgres_database_size_bytes: u64, pub(super) writable_free_disk_bytes: u64, pub(super) estimated_chunk_write_bytes: u64, pub(super) breach_reasons: Vec<&'static str> }

pub(super) async fn check_capacity(
    pool: &sqlx::PgPool,
    config: &CapacityGuardConfig,
    range: BackfillBlockRange,
) -> Result<CapacitySnapshot> {
    let postgres_database_size_bytes = postgres_database_size_bytes(pool).await?;
    ensure_path_is_writable(&config.writable_free_disk_path)?;
    let writable_free_disk_bytes = writable_free_disk_bytes(&config.writable_free_disk_path)?;
    let block_count = u64::try_from(range.to_block - range.from_block + 1)
        .context("catch-up chunk block count does not fit in u64")?;
    let estimated_chunk_write_bytes = config
        .estimated_bytes_per_block
        .checked_mul(block_count)
        .context("catch-up estimated write amplification overflowed")?;

    let mut breach_reasons = Vec::new();
    if config.postgres_max_bytes.is_some_and(|limit| {
        postgres_database_size_bytes.saturating_add(estimated_chunk_write_bytes) > limit
    }) {
        breach_reasons.push("postgres_database_size");
    }
    if writable_free_disk_bytes
        < config
            .min_writable_free_disk_bytes
            .saturating_add(estimated_chunk_write_bytes)
    {
        breach_reasons.push("writable_free_disk");
    }

    Ok(CapacitySnapshot {
        postgres_database_size_bytes,
        writable_free_disk_bytes,
        estimated_chunk_write_bytes,
        breach_reasons,
    })
}

async fn postgres_database_size_bytes(pool: &sqlx::PgPool) -> Result<u64> {
    let size = sqlx::query_scalar::<_, i64>("SELECT pg_database_size(current_database())::BIGINT")
        .fetch_one(pool)
        .await
        .context("failed to read current Postgres database size")?;
    u64::try_from(size).context("Postgres database size was negative")
}

fn ensure_path_is_writable(path: &Path) -> Result<()> {
    let directory = if path.is_dir() {
        path
    } else {
        path.parent().unwrap_or(path)
    };
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_nanos();
    let sequence = NEXT_CAPACITY_PROBE_ID.fetch_add(1, Ordering::Relaxed);
    let probe = directory.join(format!(
        ".bigname-catchup-capacity-probe-{}-{unique}-{sequence}",
        std::process::id()
    ));
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .and_then(|mut file| file.write_all(b""))
        .with_context(|| {
            format!(
                "failed to write capacity probe under {}",
                directory.display()
            )
        })?;
    fs::remove_file(&probe).with_context(|| {
        format!(
            "failed to remove capacity probe file {} after writable check",
            probe.display()
        )
    })
}

fn writable_free_disk_bytes(path: &Path) -> Result<u64> {
    let output = Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .with_context(|| format!("failed to run df for {}", path.display()))?;
    if !output.status.success() {
        bail!(
            "df failed for {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8(output.stdout).context("df output was not UTF-8")?;
    let line = stdout
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .context("df output did not include a data row")?;
    let available_kib = line
        .split_whitespace()
        .nth(3)
        .context("df output did not include available KiB")?
        .parse::<u64>()
        .context("df available KiB was not numeric")?;
    available_kib
        .checked_mul(1024)
        .context("df available bytes overflowed u64")
}

// Capacity metadata keeps each range, finalized head, limit, snapshot, and error explicit.
#[expect(clippy::too_many_arguments)]
pub(super) fn capacity_metadata(
    status: &str,
    config: &BackfillJobRunConfig,
    range: BackfillBlockRange,
    finalized_head_block_number: i64,
    finalized_head_block_hash: &str,
    capacity_config: &CapacityGuardConfig,
    snapshot: Option<&CapacitySnapshot>,
    error: Option<&anyhow::Error>,
) -> Value {
    json!({
        "phase": "capacity_guard",
        "capacity_status": status,
        "capacity_breach_reasons": snapshot.map(|snapshot| snapshot.breach_reasons.clone()).unwrap_or_default(),
        "range_start_block_number": range.from_block,
        "range_end_block_number": range.to_block,
        "finalized_head_block_number": finalized_head_block_number,
        "finalized_head_block_hash": finalized_head_block_hash,
        "idempotency_key": config.idempotency_key,
        "postgres_database_size_bytes": snapshot.map(|snapshot| snapshot.postgres_database_size_bytes),
        "postgres_max_bytes": capacity_config.postgres_max_bytes,
        "writable_free_disk_path": capacity_config.writable_free_disk_path.display().to_string(),
        "writable_free_disk_bytes": snapshot.map(|snapshot| snapshot.writable_free_disk_bytes),
        "min_writable_free_disk_bytes": capacity_config.min_writable_free_disk_bytes,
        "estimated_bytes_per_block": capacity_config.estimated_bytes_per_block,
        "estimated_chunk_write_bytes": snapshot.map(|snapshot| snapshot.estimated_chunk_write_bytes),
        "object_cache_budget_checked": false,
        "error": error.map(|error| format!("{error:#}")),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use super::*;

    #[test]
    fn concurrent_writable_capacity_probes_use_distinct_files() -> Result<()> {
        const WORKER_COUNT: usize = 32;
        const PROBES_PER_WORKER: usize = 8;

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "bigname-capacity-probe-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).with_context(|| {
            format!(
                "failed to create capacity probe test directory {}",
                directory.display()
            )
        })?;

        let barrier = Arc::new(Barrier::new(WORKER_COUNT));
        let results = thread::scope(|scope| {
            let handles = (0..WORKER_COUNT)
                .map(|_| {
                    let barrier = Arc::clone(&barrier);
                    let directory = &directory;
                    scope.spawn(move || {
                        barrier.wait();
                        for _ in 0..PROBES_PER_WORKER {
                            ensure_path_is_writable(directory)?;
                        }
                        Result::<()>::Ok(())
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join())
                .collect::<Vec<_>>()
        });

        let residual_files = fs::read_dir(&directory)
            .with_context(|| {
                format!(
                    "failed to inspect capacity probe test directory {}",
                    directory.display()
                )
            })?
            .count();
        fs::remove_dir(&directory).with_context(|| {
            format!(
                "failed to remove capacity probe test directory {}",
                directory.display()
            )
        })?;

        for result in results {
            result.expect("capacity probe worker panicked")?;
        }
        assert_eq!(residual_files, 0, "capacity probes must remove their files");
        Ok(())
    }
}
