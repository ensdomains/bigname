use std::{future::Future, net::SocketAddr, sync::OnceLock, time::Duration};

use bigname_metrics::{
    BuildInfo, HistogramTimer, HistogramVec, IntCounterVec, MetricsRegistry, MetricsServer,
};

use crate::provider::{ChainProvider, ChainProviderKind};

#[path = "metrics/reconcile.rs"]
mod reconcile;

struct IndexerMetrics {
    registry: MetricsRegistry,
    reconcile: reconcile::ReconcileMetrics,
    catchup_chunks: IntCounterVec,
    catchup_chunk_duration: HistogramVec,
    catchup_raw_logs_scanned: IntCounterVec,
    catchup_raw_logs_matched: IntCounterVec,
    normalized_events_derived: IntCounterVec,
    normalized_events_upserted: IntCounterVec,
    catchup_iteration_duration: HistogramVec,
    admission_retries: IntCounterVec,
    fence_wait: HistogramVec,
    coverage_recovery_jobs: IntCounterVec,
    coverage_provider_queries: IntCounterVec,
    coverage_violation_scan_duration: HistogramVec,
    live_blocks_ingested: IntCounterVec,
    live_logs_ingested: IntCounterVec,
    reorg_events: IntCounterVec,
    provider_lookup_duration: HistogramVec,
}

#[derive(Clone)]
struct ProviderMetricContext {
    chain: String,
    provider_kind: &'static str,
}

#[derive(clap::Args, Debug)]
pub(crate) struct MetricsArgs {
    #[arg(
        long = "metrics-bind-addr",
        env = "BIGNAME_INDEXER_METRICS_BIND_ADDR",
        default_value = "127.0.0.1:9465"
    )]
    pub(crate) bind_addr: SocketAddr,
}

tokio::task_local! {
    static PROVIDER_METRIC_CONTEXT: ProviderMetricContext;
}

tokio::task_local! {
    static COVERAGE_PROVIDER_QUERY_CONTEXT: ();
}

impl IndexerMetrics {
    fn new() -> anyhow::Result<Self> {
        let registry = MetricsRegistry::new(BuildInfo {
            build_sha: crate::BUILD_SHA,
            replay_version: bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
            schema_version: bigname_storage::latest_migration_version(),
        })?;
        Ok(Self {
            reconcile: reconcile::ReconcileMetrics::new(&registry)?,
            catchup_chunks: registry.int_counter_vec(
                "catchup_chunks_total",
                "Completed normalized-event catch-up chunks.",
                &["chain"],
            )?,
            catchup_chunk_duration: registry.histogram_vec(
                "catchup_chunk_duration_seconds",
                "Normalized-event catch-up chunk duration.",
                &["chain"],
            )?,
            catchup_raw_logs_scanned: registry.int_counter_vec(
                "catchup_raw_logs_scanned_total",
                "Raw logs scanned by normalized-event catch-up.",
                &["chain"],
            )?,
            catchup_raw_logs_matched: registry.int_counter_vec(
                "catchup_raw_logs_matched_total",
                "Raw logs matched by normalized-event catch-up.",
                &["chain"],
            )?,
            normalized_events_derived: registry.int_counter_vec(
                "normalized_events_derived_total",
                "Normalized events derived by catch-up.",
                &["chain"],
            )?,
            normalized_events_upserted: registry.int_counter_vec(
                "normalized_events_upserted_total",
                "New normalized event identities inserted by catch-up persistence.",
                &["chain"],
            )?,
            catchup_iteration_duration: registry.histogram_vec(
                "catchup_iteration_duration_seconds",
                "Duration of one normalized-event catch-up lane iteration.",
                &["chain"],
            )?,
            admission_retries: registry.int_counter_vec(
                "admission_retries_total",
                "Projection replay admission retries.",
                &["chain"],
            )?,
            fence_wait: registry.histogram_vec(
                "fence_wait_seconds",
                "Time spent waiting to retry projection replay admission.",
                &["chain"],
            )?,
            coverage_recovery_jobs: registry.int_counter_vec(
                "coverage_recovery_jobs_total",
                "Coverage recovery jobs by bounded outcome.",
                &["chain", "outcome"],
            )?,
            coverage_provider_queries: registry.int_counter_vec(
                "coverage_provider_queries_total",
                "Chain-provider queries issued by coverage recovery.",
                &["chain"],
            )?,
            coverage_violation_scan_duration: registry.histogram_vec(
                "coverage_violation_scan_duration_seconds",
                "Duration of a generation-bound coverage violation scan.",
                &["chain"],
            )?,
            live_blocks_ingested: registry.int_counter_vec(
                "live_blocks_ingested_total",
                "Live block payloads persisted by chain.",
                &["chain"],
            )?,
            live_logs_ingested: registry.int_counter_vec(
                "live_logs_ingested_total",
                "Selected live logs persisted by chain.",
                &["chain"],
            )?,
            reorg_events: registry.int_counter_vec(
                "reorg_events_total",
                "Successfully reconciled live reorgs by chain.",
                &["chain"],
            )?,
            provider_lookup_duration: registry.histogram_vec(
                "provider_lookup_duration_seconds",
                "Duration of chain-provider lookups used by intake or catch-up.",
                &["chain", "provider_kind"],
            )?,
            registry,
        })
    }
}

fn indexer_metrics() -> &'static IndexerMetrics {
    static METRICS: OnceLock<IndexerMetrics> = OnceLock::new();
    METRICS.get_or_init(|| IndexerMetrics::new().expect("indexer metrics must register"))
}

pub(crate) async fn bind(bind_addr: SocketAddr) -> anyhow::Result<MetricsServer> {
    MetricsServer::bind(bind_addr, indexer_metrics().registry.clone()).await
}

pub(crate) async fn spawn_listener(bind_addr: SocketAddr) -> anyhow::Result<()> {
    let server = bind(bind_addr).await?;
    tracing::info!(service = "indexer", %bind_addr, "metrics listener bound");
    tokio::spawn(async move {
        if let Err(error) = server.serve().await {
            tracing::error!(
                service = "indexer",
                error = %format!("{error:#}"),
                "metrics listener exited"
            );
        }
    });
    Ok(())
}

pub(crate) async fn configure_reconcile_progress(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
) -> anyhow::Result<()> {
    reconcile::configure(pool, deployment_profile).await
}

pub(crate) fn record_catchup_chunk(
    chain: &str,
    duration: Duration,
    scanned_raw_logs: usize,
    matched_raw_logs: usize,
    normalized_events_derived: usize,
    normalized_events_upserted: usize,
) {
    let metrics = indexer_metrics();
    metrics.catchup_chunks.with_label_values(&[chain]).inc();
    metrics
        .catchup_chunk_duration
        .with_label_values(&[chain])
        .observe(duration.as_secs_f64());
    metrics
        .catchup_raw_logs_scanned
        .with_label_values(&[chain])
        .inc_by(count(scanned_raw_logs));
    metrics
        .catchup_raw_logs_matched
        .with_label_values(&[chain])
        .inc_by(count(matched_raw_logs));
    metrics
        .normalized_events_derived
        .with_label_values(&[chain])
        .inc_by(count(normalized_events_derived));
    metrics
        .normalized_events_upserted
        .with_label_values(&[chain])
        .inc_by(count(normalized_events_upserted));
}

pub(crate) fn catchup_iteration_timer(chain: &str) -> HistogramTimer {
    indexer_metrics()
        .catchup_iteration_duration
        .with_label_values(&[chain])
        .start_timer()
}

pub(crate) fn record_admission_retry(chain: &str, waited: Duration) {
    let metrics = indexer_metrics();
    metrics.admission_retries.with_label_values(&[chain]).inc();
    metrics
        .fence_wait
        .with_label_values(&[chain])
        .observe(waited.as_secs_f64());
}

pub(crate) fn record_coverage_recovery_job(chain: &str, outcome: &'static str) {
    let outcome = match outcome {
        "completed" => "completed",
        "failed" => "failed",
        "deferred" => "deferred",
        "terminal" => "terminal",
        "pending" => "pending",
        _ => "unknown",
    };
    indexer_metrics()
        .coverage_recovery_jobs
        .with_label_values(&[chain, outcome])
        .inc();
}

pub(crate) fn coverage_violation_scan_timer(chain: &str) -> HistogramTimer {
    indexer_metrics()
        .coverage_violation_scan_duration
        .with_label_values(&[chain])
        .start_timer()
}

pub(crate) fn record_live_intake(chain: &str, block_count: usize, log_count: usize) {
    let metrics = indexer_metrics();
    metrics
        .live_blocks_ingested
        .with_label_values(&[chain])
        .inc_by(count(block_count));
    metrics
        .live_logs_ingested
        .with_label_values(&[chain])
        .inc_by(count(log_count));
}

pub(crate) fn record_reorg(chain: &str) {
    indexer_metrics()
        .reorg_events
        .with_label_values(&[chain])
        .inc();
}

pub(crate) fn with_provider_metrics<Output>(
    chain: &str,
    provider_kind: ChainProviderKind,
    future: impl Future<Output = Output>,
) -> impl Future<Output = Output> {
    let provider_kind = match provider_kind {
        ChainProviderKind::JsonRpc => "json_rpc",
        ChainProviderKind::RethDb => "reth_db",
    };
    PROVIDER_METRIC_CONTEXT.scope(
        ProviderMetricContext {
            chain: chain.to_owned(),
            provider_kind,
        },
        Box::pin(future),
    )
}

pub(crate) fn with_coverage_provider_queries<Output>(
    future: impl Future<Output = Output>,
) -> impl Future<Output = Output> {
    COVERAGE_PROVIDER_QUERY_CONTEXT.scope((), Box::pin(future))
}

pub(crate) fn with_coverage_provider_metrics<Output>(
    chain: &str,
    provider_kind: ChainProviderKind,
    future: impl Future<Output = Output>,
) -> impl Future<Output = Output> {
    with_provider_metrics(chain, provider_kind, with_coverage_provider_queries(future))
}

pub(crate) fn provider_lookup_timer(provider: &ChainProvider) -> Option<HistogramTimer> {
    let context = PROVIDER_METRIC_CONTEXT
        .try_with(Clone::clone)
        .ok()
        .or_else(|| {
            provider.metrics_chain().map(|chain| ProviderMetricContext {
                chain: chain.to_owned(),
                provider_kind: provider_kind_label(provider.kind()),
            })
        })?;
    let metrics = indexer_metrics();
    if COVERAGE_PROVIDER_QUERY_CONTEXT.try_with(|()| ()).is_ok() {
        metrics
            .coverage_provider_queries
            .with_label_values(&[&context.chain])
            .inc();
    }
    Some(
        metrics
            .provider_lookup_duration
            .with_label_values(&[context.chain.as_str(), context.provider_kind])
            .start_timer(),
    )
}

fn provider_kind_label(provider_kind: ChainProviderKind) -> &'static str {
    match provider_kind {
        ChainProviderKind::JsonRpc => "json_rpc",
        ChainProviderKind::RethDb => "reth_db",
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
pub(crate) fn catchup_chunks(chain: &str) -> u64 {
    indexer_metrics()
        .catchup_chunks
        .with_label_values(&[chain])
        .get()
}

#[cfg(test)]
pub(crate) fn normalized_events_derived(chain: &str) -> u64 {
    indexer_metrics()
        .normalized_events_derived
        .with_label_values(&[chain])
        .get()
}

#[cfg(test)]
pub(crate) fn admission_retries(chain: &str) -> u64 {
    indexer_metrics()
        .admission_retries
        .with_label_values(&[chain])
        .get()
}

#[cfg(test)]
pub(crate) fn fence_wait_observations(chain: &str) -> u64 {
    indexer_metrics()
        .fence_wait
        .with_label_values(&[chain])
        .get_sample_count()
}

#[cfg(test)]
pub(crate) fn coverage_violation_scan_observations(chain: &str) -> u64 {
    indexer_metrics()
        .coverage_violation_scan_duration
        .with_label_values(&[chain])
        .get_sample_count()
}

#[cfg(test)]
pub(crate) fn coverage_recovery_jobs(chain: &str, outcome: &str) -> u64 {
    indexer_metrics()
        .coverage_recovery_jobs
        .with_label_values(&[chain, outcome])
        .get()
}

#[cfg(test)]
pub(crate) fn provider_lookup_observations(chain: &str, provider_kind: &str) -> u64 {
    indexer_metrics()
        .provider_lookup_duration
        .with_label_values(&[chain, provider_kind])
        .get_sample_count()
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use anyhow::{Context, Result, ensure};

    use super::*;

    #[tokio::test]
    async fn metrics_endpoint_serves_parseable_indexer_scrape() -> Result<()> {
        reconcile::initialize_endpoint_test_series();
        indexer_metrics()
            .catchup_chunks
            .with_label_values(&["metrics-test-chain"])
            .inc_by(0);
        indexer_metrics()
            .normalized_events_derived
            .with_label_values(&["metrics-test-chain"])
            .inc_by(0);
        let server = bind("127.0.0.1:0".parse()?).await?;
        let address = server.local_addr()?;
        let task = tokio::spawn(server.serve());
        let response = tokio::task::spawn_blocking(move || scrape(address))
            .await
            .context("indexer metrics scrape task panicked")??;
        task.abort();

        let body = parse_http_scrape(&response)?;
        ensure!(body.contains("# TYPE build_info gauge"));
        ensure!(body.contains("# TYPE catchup_chunks_total counter"));
        ensure!(body.contains("# TYPE normalized_events_derived_total counter"));
        ensure!(body.contains(
            "# TYPE startup_adapter_reconcile_normalized_events_processed_total counter"
        ));
        ensure!(body.contains("# TYPE startup_adapter_reconcile_staged_items gauge"));
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
