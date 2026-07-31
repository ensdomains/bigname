use std::{
    fs::{self, OpenOptions},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    pin::Pin,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use sqlx::PgPool;

use crate::{
    config::CapacityConfig,
    error::{RunnerError, RunnerResult},
};

static NEXT_PROBE_ID: AtomicU64 = AtomicU64::new(0);

pub type CapacityFuture<'a> =
    Pin<Box<dyn Future<Output = RunnerResult<CapacityMeasurement>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityMeasurement {
    pub database_size_bytes: u64,
    pub free_disk_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityStatus {
    pub measurement: CapacityMeasurement,
    pub reserved_write_bytes: u64,
    pub breach_reasons: Vec<&'static str>,
}

impl CapacityStatus {
    pub fn is_available(&self) -> bool {
        self.breach_reasons.is_empty()
    }
}

pub trait CapacityProbe: Send + Sync {
    fn measure<'a>(&'a self, pool: &'a PgPool, writable_path: &'a Path) -> CapacityFuture<'a>;
}

#[derive(Clone, Debug, Default)]
pub struct SystemCapacityProbe;

impl CapacityProbe for SystemCapacityProbe {
    fn measure<'a>(&'a self, pool: &'a PgPool, writable_path: &'a Path) -> CapacityFuture<'a> {
        Box::pin(async move {
            let database_size =
                sqlx::query_scalar::<_, i64>("SELECT pg_database_size(current_database())::BIGINT")
                    .fetch_one(pool)
                    .await
                    .map_err(|error| {
                        RunnerError::transient(format!(
                            "failed to read current PostgreSQL database size: {error}"
                        ))
                    })?;
            let database_size_bytes = u64::try_from(database_size).map_err(|_| {
                RunnerError::data_integrity("PostgreSQL database size was negative")
            })?;
            let writable_path = writable_path.to_owned();
            let free_disk_bytes = tokio::task::spawn_blocking(move || {
                ensure_path_is_writable(&writable_path)?;
                writable_free_disk_bytes(&writable_path)
            })
            .await
            .map_err(|error| {
                RunnerError::transient(format!("disk capacity probe task failed: {error}"))
            })??;
            Ok(CapacityMeasurement {
                database_size_bytes,
                free_disk_bytes,
            })
        })
    }
}

#[derive(Clone)]
pub struct CapacityGuard {
    config: CapacityConfig,
    probe: Arc<dyn CapacityProbe>,
}

impl CapacityGuard {
    pub fn system(config: CapacityConfig) -> Self {
        Self::new(config, Arc::new(SystemCapacityProbe))
    }

    pub fn new(config: CapacityConfig, probe: Arc<dyn CapacityProbe>) -> Self {
        Self { config, probe }
    }

    pub fn poll_interval(&self) -> std::time::Duration {
        self.config.poll_interval
    }

    pub async fn check(
        &self,
        pool: &PgPool,
        reserved_write_bytes: u64,
    ) -> RunnerResult<CapacityStatus> {
        let measurement = self.probe.measure(pool, &self.config.writable_path).await?;
        let mut breach_reasons = Vec::new();
        if self.config.database_max_bytes.is_some_and(|maximum| {
            measurement
                .database_size_bytes
                .saturating_add(reserved_write_bytes)
                > maximum
        }) {
            breach_reasons.push("database_size");
        }
        if measurement.free_disk_bytes
            < self
                .config
                .minimum_free_disk_bytes
                .saturating_add(reserved_write_bytes)
        {
            breach_reasons.push("free_disk");
        }
        Ok(CapacityStatus {
            measurement,
            reserved_write_bytes,
            breach_reasons,
        })
    }
}

fn ensure_path_is_writable(path: &Path) -> RunnerResult<()> {
    let directory = if path.is_dir() {
        path
    } else {
        path.parent().unwrap_or(path)
    };
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| RunnerError::transient(format!("system clock is before epoch: {error}")))?
        .as_nanos();
    let sequence = NEXT_PROBE_ID.fetch_add(1, Ordering::Relaxed);
    let probe_path = directory.join(format!(
        ".phase-runner-capacity-probe-{}-{unique}-{sequence}",
        std::process::id()
    ));
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe_path)
        .and_then(|mut file| file.write_all(b""))
        .map_err(|error| {
            RunnerError::transient(format!(
                "failed to write capacity probe under {}: {error}",
                directory.display()
            ))
        })?;
    fs::remove_file(&probe_path).map_err(|error| {
        RunnerError::transient(format!(
            "failed to remove capacity probe {}: {error}",
            probe_path.display()
        ))
    })
}

fn writable_free_disk_bytes(path: &PathBuf) -> RunnerResult<u64> {
    let output = Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .map_err(|error| {
            RunnerError::transient(format!("failed to run df for {}: {error}", path.display()))
        })?;
    if !output.status.success() {
        return Err(RunnerError::transient(format!(
            "df failed for {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let stdout = String::from_utf8(output.stdout)
        .map_err(|error| RunnerError::transient(format!("df output was not UTF-8: {error}")))?;
    let line = stdout
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .ok_or_else(|| RunnerError::transient("df output did not include a data row"))?;
    let available_kib = line
        .split_whitespace()
        .nth(3)
        .ok_or_else(|| RunnerError::transient("df output did not include available KiB"))?
        .parse::<u64>()
        .map_err(|error| {
            RunnerError::transient(format!("df available KiB was not numeric: {error}"))
        })?;
    available_kib
        .checked_mul(1024)
        .ok_or_else(|| RunnerError::data_integrity("df available bytes overflowed u64"))
}
