use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::{
    Router,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus::{Encoder, Registry, TextEncoder, core::Collector};

pub use prometheus::{
    GaugeVec, HistogramOpts, HistogramTimer, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec,
};

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

#[derive(Clone, Copy, Debug)]
pub struct BuildInfo<'a> {
    pub build_sha: &'a str,
    pub interpreter_content_hash: &'a str,
}

#[derive(Clone)]
pub struct MetricsRegistry {
    inner: Registry,
}

impl MetricsRegistry {
    pub fn new(build: BuildInfo<'_>) -> Result<Self> {
        let registry = Self {
            inner: Registry::new(),
        };
        let build_info = GaugeVec::new(
            prometheus::Opts::new(
                "build_info",
                "Build and interpretation identity for this process.",
            ),
            &["build_sha", "interpreter_content_hash"],
        )
        .context("failed to define build_info")?;
        build_info
            .with_label_values(&[build.build_sha, build.interpreter_content_hash])
            .set(1.0);
        registry.register(build_info)?;
        Ok(registry)
    }

    pub fn register<C>(&self, collector: C) -> Result<C>
    where
        C: Collector + Clone + 'static,
    {
        self.inner
            .register(Box::new(collector.clone()))
            .context("failed to register Prometheus collector")?;
        Ok(collector)
    }

    pub fn int_counter_vec(
        &self,
        name: &str,
        help: &str,
        labels: &[&str],
    ) -> Result<IntCounterVec> {
        let counter = IntCounterVec::new(prometheus::Opts::new(name, help), labels)
            .with_context(|| format!("failed to define {name}"))?;
        self.register(counter)
    }

    pub fn int_gauge(&self, name: &str, help: &str) -> Result<IntGauge> {
        let gauge =
            IntGauge::new(name, help).with_context(|| format!("failed to define {name}"))?;
        self.register(gauge)
    }

    pub fn int_gauge_vec(&self, name: &str, help: &str, labels: &[&str]) -> Result<IntGaugeVec> {
        let gauge = IntGaugeVec::new(prometheus::Opts::new(name, help), labels)
            .with_context(|| format!("failed to define {name}"))?;
        self.register(gauge)
    }

    pub fn histogram_vec(&self, name: &str, help: &str, labels: &[&str]) -> Result<HistogramVec> {
        let histogram = HistogramVec::new(
            HistogramOpts::new(name, help).buckets(duration_buckets()),
            labels,
        )
        .with_context(|| format!("failed to define {name}"))?;
        self.register(histogram)
    }

    pub fn encode(&self) -> Result<String> {
        let metric_families = self.inner.gather();
        let mut output = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut output)
            .context("failed to encode Prometheus metrics")?;
        String::from_utf8(output).context("Prometheus encoder produced invalid UTF-8")
    }
}

pub struct MetricsServer {
    listener: tokio::net::TcpListener,
    registry: MetricsRegistry,
}

impl MetricsServer {
    pub async fn bind(bind_addr: SocketAddr, registry: MetricsRegistry) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("failed to bind metrics listener at {bind_addr}"))?;
        Ok(Self { listener, registry })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .context("failed to read metrics listener address")
    }

    pub async fn serve(self) -> Result<()> {
        let router = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(self.registry);
        axum::serve(self.listener, router)
            .await
            .context("metrics server exited unexpectedly")
    }
}

async fn metrics_handler(
    axum::extract::State(registry): axum::extract::State<MetricsRegistry>,
) -> Response {
    match registry.encode() {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
            body,
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {error:#}\n"),
        )
            .into_response(),
    }
}

fn duration_buckets() -> Vec<f64> {
    vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
        600.0, 1_800.0, 3_600.0, 14_400.0,
    ]
}
