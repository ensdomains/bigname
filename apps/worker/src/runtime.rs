use std::{
    net::SocketAddr,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use bigname_metrics::{
    BuildInfo, HistogramTimer, HistogramVec, IntCounterVec, IntGauge, MetricsRegistry,
    MetricsServer,
};
use sqlx::PgPool;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const PROJECTION_APPLY_QUEUE_DEPTH_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

struct WorkerMetrics {
    registry: MetricsRegistry,
    projection_apply_queue_depth: IntGauge,
    projection_apply_duration: HistogramVec,
    projection_rebuilds: IntCounterVec,
    projection_rebuild_keys_requested: IntCounterVec,
    projection_rebuild_rows: IntCounterVec,
    projection_rebuild_duration: HistogramVec,
}

impl WorkerMetrics {
    fn new() -> anyhow::Result<Self> {
        let registry = MetricsRegistry::new(BuildInfo {
            build_sha: crate::BUILD_SHA,
            replay_version: bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
            schema_version: bigname_storage::latest_migration_version(),
        })?;
        let replay_version = registry.int_gauge(
            "replay_version",
            "Projection replay version compiled into this worker.",
        )?;
        replay_version.set(i64::from(
            bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
        ));
        Ok(Self {
            projection_apply_queue_depth: registry.int_gauge(
                "projection_apply_queue_depth",
                "Pending projection invalidations, sampled at most once every five seconds.",
            )?,
            projection_apply_duration: registry.histogram_vec(
                "projection_apply_duration_seconds",
                "Duration of one projection derive-and-apply iteration.",
                &[],
            )?,
            projection_rebuilds: registry.int_counter_vec(
                "projection_rebuilds_total",
                "Projection rebuild steps by projection and bounded outcome.",
                &["projection", "outcome"],
            )?,
            projection_rebuild_keys_requested: registry.int_counter_vec(
                "projection_rebuild_keys_requested_total",
                "Projection keys requested for rebuild.",
                &["projection"],
            )?,
            projection_rebuild_rows: registry.int_counter_vec(
                "projection_rebuild_rows_total",
                "Projection rows upserted or deleted during rebuild.",
                &["projection", "operation"],
            )?,
            projection_rebuild_duration: registry.histogram_vec(
                "projection_rebuild_duration_seconds",
                "Projection rebuild step duration.",
                &["projection"],
            )?,
            registry,
        })
    }
}

#[derive(Default)]
struct QueueDepthRefreshGate {
    last_refresh: Mutex<Option<Instant>>,
}

impl QueueDepthRefreshGate {
    fn claim(&self, now: Instant) -> bool {
        let mut last_refresh = self
            .last_refresh
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if last_refresh.is_some_and(|last_refresh| {
            now.saturating_duration_since(last_refresh)
                < PROJECTION_APPLY_QUEUE_DEPTH_REFRESH_INTERVAL
        }) {
            return false;
        }
        *last_refresh = Some(now);
        true
    }
}

fn worker_metrics() -> &'static WorkerMetrics {
    static METRICS: OnceLock<WorkerMetrics> = OnceLock::new();
    METRICS.get_or_init(|| WorkerMetrics::new().expect("worker metrics must register"))
}

pub(crate) async fn bind_metrics(bind_addr: SocketAddr) -> anyhow::Result<MetricsServer> {
    MetricsServer::bind(bind_addr, worker_metrics().registry.clone()).await
}

pub(crate) fn projection_apply_timer() -> HistogramTimer {
    worker_metrics()
        .projection_apply_duration
        .with_label_values(&[] as &[&str])
        .start_timer()
}

pub(crate) fn set_projection_apply_queue_depth(depth: i64) {
    worker_metrics().projection_apply_queue_depth.set(depth);
}

pub(crate) async fn refresh_projection_apply_queue_depth(pool: &PgPool) {
    static REFRESH_GATE: OnceLock<QueueDepthRefreshGate> = OnceLock::new();
    if !REFRESH_GATE
        .get_or_init(QueueDepthRefreshGate::default)
        .claim(Instant::now())
    {
        return;
    }
    match sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM projection_invalidations
        WHERE state = 'pending'::projection_invalidation_state
        "#,
    )
    .fetch_one(pool)
    .await
    {
        Ok(queue_depth) => set_projection_apply_queue_depth(queue_depth),
        Err(error) => warn!(
            service = "worker",
            error = ?error,
            "failed to refresh projection apply queue-depth metric"
        ),
    }
}

pub(crate) fn projection_rebuild_timer(projection: &'static str) -> HistogramTimer {
    worker_metrics()
        .projection_rebuild_duration
        .with_label_values(&[projection])
        .start_timer()
}

pub(crate) fn record_projection_rebuild_completed(
    projection: &'static str,
    requested: usize,
    upserted: usize,
    deleted: u64,
) {
    let metrics = worker_metrics();
    metrics
        .projection_rebuilds
        .with_label_values(&[projection, "completed"])
        .inc();
    metrics
        .projection_rebuild_keys_requested
        .with_label_values(&[projection])
        .inc_by(usize_count(requested));
    for (operation, count) in [("upserted", usize_count(upserted)), ("deleted", deleted)] {
        metrics
            .projection_rebuild_rows
            .with_label_values(&[projection, operation])
            .inc_by(count);
    }
}

pub(crate) fn record_projection_rebuild_failed(projection: &'static str) {
    worker_metrics()
        .projection_rebuilds
        .with_label_values(&[projection, "failed"])
        .inc();
}

pub(crate) fn record_projection_rebuild_skipped(projection: &'static str) {
    worker_metrics()
        .projection_rebuilds
        .with_label_values(&[projection, "skipped"])
        .inc();
}

fn usize_count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
pub(crate) fn projection_rebuild_count(projection: &str, outcome: &str) -> u64 {
    worker_metrics()
        .projection_rebuilds
        .with_label_values(&[projection, outcome])
        .get()
}

pub(crate) fn init_tracing(service: &'static str, emit_logs_to_stderr: bool) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var_os("BIGNAME_LOG_JSON").is_some() {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(false);
        if emit_logs_to_stderr {
            subscriber.with_writer(std::io::stderr).init();
        } else {
            subscriber.init();
        }
    } else {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false);
        if emit_logs_to_stderr {
            subscriber.with_writer(std::io::stderr).init();
        } else {
            subscriber.init();
        }
    }

    info!(
        service = service,
        version = crate::SOFTWARE_VERSION,
        build_sha = crate::BUILD_SHA,
        "logging configured"
    );
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use anyhow::{Context, Result, ensure};

    use super::*;

    #[test]
    fn projection_apply_queue_depth_refresh_is_rate_limited() {
        let gate = QueueDepthRefreshGate::default();
        let started = Instant::now();

        assert!(gate.claim(started));
        assert!(!gate.claim(started + PROJECTION_APPLY_QUEUE_DEPTH_REFRESH_INTERVAL / 2));
        assert!(gate.claim(started + PROJECTION_APPLY_QUEUE_DEPTH_REFRESH_INTERVAL));
    }

    #[test]
    fn projection_rebuild_row_counter_excludes_requested_keys() {
        let metrics = worker_metrics();
        let projection = "metrics_unit_consistency_test";
        let requested_rows = metrics
            .projection_rebuild_rows
            .with_label_values(&[projection, "requested"]);
        let requested_keys = metrics
            .projection_rebuild_keys_requested
            .with_label_values(&[projection]);
        let rows_before = requested_rows.get();
        let keys_before = requested_keys.get();

        record_projection_rebuild_completed(projection, 2, 3, 4);

        assert_eq!(requested_rows.get(), rows_before);
        assert_eq!(requested_keys.get(), keys_before + 2);
    }

    #[tokio::test]
    async fn metrics_endpoint_serves_parseable_worker_scrape() -> Result<()> {
        worker_metrics()
            .projection_rebuilds
            .with_label_values(&["metrics_test", "completed"])
            .inc_by(0);
        worker_metrics()
            .projection_rebuild_keys_requested
            .with_label_values(&["metrics_test"])
            .inc_by(0);
        let server = bind_metrics("127.0.0.1:0".parse()?).await?;
        let address = server.local_addr()?;
        let task = tokio::spawn(server.serve());
        let response = tokio::task::spawn_blocking(move || scrape(address))
            .await
            .context("worker metrics scrape task panicked")??;
        task.abort();

        let body = parse_http_scrape(&response)?;
        ensure!(body.contains("# TYPE build_info gauge"));
        ensure!(body.contains("# TYPE projection_rebuilds_total counter"));
        ensure!(body.contains("# TYPE projection_rebuild_keys_requested_total counter"));
        ensure!(body.contains("# TYPE replay_version gauge"));
        Ok(())
    }

    fn scrape(address: SocketAddr) -> Result<String> {
        let mut stream = std::net::TcpStream::connect(address)?;
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    fn parse_http_scrape(response: &str) -> Result<&str> {
        let (head, body) = response
            .split_once("\r\n\r\n")
            .context("metrics response did not contain an HTTP header boundary")?;
        ensure!(head.starts_with("HTTP/1.1 200"));
        let mut samples = 0usize;
        for line in body
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
        {
            let (_, value) = line
                .rsplit_once(' ')
                .with_context(|| format!("invalid Prometheus sample: {line}"))?;
            value.parse::<f64>()?;
            samples += 1;
        }
        ensure!(samples > 0, "metrics scrape contained no samples");
        Ok(body)
    }
}
