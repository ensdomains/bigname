use std::{net::SocketAddr, sync::OnceLock};

use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
    response::Response,
};
use bigname_metrics::{
    BuildInfo, HistogramTimer, HistogramVec, IntCounterVec, IntGauge, MetricsRegistry,
    MetricsServer,
};

struct ApiMetrics {
    registry: MetricsRegistry,
    http_requests: IntCounterVec,
    http_request_duration: HistogramVec,
    verified_execution: IntCounterVec,
    verified_execution_duration: HistogramVec,
    http_requests_in_flight: IntGauge,
    verified_execution_in_flight: IntGauge,
}

impl ApiMetrics {
    fn new() -> anyhow::Result<Self> {
        let registry = MetricsRegistry::new(BuildInfo {
            build_sha: crate::BUILD_SHA,
            replay_version: bigname_storage::CURRENT_PROJECTION_REPLAY_VERSION,
            schema_version: bigname_storage::latest_migration_version(),
        })?;
        let http_requests = registry.int_counter_vec(
            "http_requests_total",
            "HTTP requests completed by route template, method, and status class.",
            &["route", "method", "status_class"],
        )?;
        let http_request_duration = registry.histogram_vec(
            "http_request_duration_seconds",
            "HTTP request duration by route template.",
            &["route"],
        )?;
        let verified_execution = registry.int_counter_vec(
            "verified_execution_total",
            "On-demand verified executions by bounded outcome.",
            &["outcome"],
        )?;
        let verified_execution_duration = registry.histogram_vec(
            "verified_execution_duration_seconds",
            "Duration of on-demand verified execution.",
            &[],
        )?;
        let http_requests_in_flight = registry.int_gauge(
            "http_requests_in_flight",
            "HTTP requests currently executing in API middleware.",
        )?;
        let verified_execution_in_flight = registry.int_gauge(
            "verified_execution_in_flight",
            "Verified requests currently holding a verified-execution permit.",
        )?;
        Ok(Self {
            registry,
            http_requests,
            http_request_duration,
            verified_execution,
            verified_execution_duration,
            http_requests_in_flight,
            verified_execution_in_flight,
        })
    }
}

fn api_metrics() -> &'static ApiMetrics {
    static METRICS: OnceLock<ApiMetrics> = OnceLock::new();
    METRICS.get_or_init(|| ApiMetrics::new().expect("API metrics must register"))
}

pub(crate) async fn bind(bind_addr: SocketAddr) -> anyhow::Result<MetricsServer> {
    MetricsServer::bind(bind_addr, api_metrics().registry.clone()).await
}

pub(crate) async fn track_http_request(request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| path.as_str().to_owned())
        .unwrap_or_else(|| "unmatched".to_owned());
    let method = bounded_method(request.method());
    let timer = api_metrics()
        .http_request_duration
        .with_label_values(&[&route])
        .start_timer();
    let _in_flight = GaugeGuard::new(api_metrics().http_requests_in_flight.clone());

    let response = next.run(request).await;
    timer.observe_duration();
    api_metrics()
        .http_requests
        .with_label_values(&[route.as_str(), method, status_class(response.status())])
        .inc();
    response
}

fn bounded_method(method: &axum::http::Method) -> &'static str {
    match *method {
        axum::http::Method::GET => "GET",
        axum::http::Method::HEAD => "HEAD",
        axum::http::Method::POST => "POST",
        axum::http::Method::PUT => "PUT",
        axum::http::Method::PATCH => "PATCH",
        axum::http::Method::DELETE => "DELETE",
        axum::http::Method::OPTIONS => "OPTIONS",
        _ => "OTHER",
    }
}

fn status_class(status: axum::http::StatusCode) -> &'static str {
    match status.as_u16() {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

#[must_use]
pub(crate) struct GaugeGuard {
    gauge: IntGauge,
}

impl GaugeGuard {
    fn new(gauge: IntGauge) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

pub(crate) fn verified_in_flight_guard() -> GaugeGuard {
    GaugeGuard::new(api_metrics().verified_execution_in_flight.clone())
}

#[must_use]
pub(crate) struct VerifiedExecutionTimer {
    _timer: HistogramTimer,
    completed: bool,
}

impl VerifiedExecutionTimer {
    pub(crate) fn finish(mut self, outcome: &'static str) {
        api_metrics()
            .verified_execution
            .with_label_values(&[bounded_outcome(outcome)])
            .inc();
        self.completed = true;
    }
}

impl Drop for VerifiedExecutionTimer {
    fn drop(&mut self) {
        if !self.completed {
            api_metrics()
                .verified_execution
                .with_label_values(&["error"])
                .inc();
        }
    }
}

pub(crate) fn verified_execution_timer() -> VerifiedExecutionTimer {
    VerifiedExecutionTimer {
        _timer: api_metrics()
            .verified_execution_duration
            .with_label_values(&[] as &[&str])
            .start_timer(),
        completed: false,
    }
}

pub(crate) fn json_outcome(value: &serde_json::Value) -> &'static str {
    value
        .get("status")
        .and_then(serde_json::Value::as_str)
        .map(bounded_outcome)
        .unwrap_or("unknown")
}

fn bounded_outcome(outcome: &str) -> &'static str {
    match outcome {
        "success" => "success",
        "not_found" => "not_found",
        "mismatch" => "mismatch",
        "unsupported" => "unsupported",
        "invalid_name" => "invalid_name",
        "execution_failed" => "execution_failed",
        "superseded" => "superseded",
        "error" => "error",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use anyhow::{Context, Result, ensure};
    use axum::{Router, body::Body, http::Request, middleware, routing::get};
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn metrics_endpoint_serves_parseable_api_scrape() -> Result<()> {
        api_metrics()
            .http_requests
            .with_label_values(&["/metrics-test", "GET", "2xx"])
            .inc_by(0);
        let server = bind("127.0.0.1:0".parse()?).await?;
        let address = server.local_addr()?;
        let task = tokio::spawn(server.serve());
        let response = tokio::task::spawn_blocking(move || scrape(address))
            .await
            .context("API metrics scrape task panicked")??;
        task.abort();

        let body = parse_http_scrape(&response)?;
        ensure!(body.contains("# TYPE build_info gauge"));
        ensure!(body.contains("# TYPE http_requests_total counter"));
        Ok(())
    }

    #[tokio::test]
    async fn request_counter_uses_route_template_without_raw_path_values() -> Result<()> {
        let before = api_metrics()
            .http_requests
            .with_label_values(&["/v2/names/{name}", "GET", "2xx"])
            .get();
        let app = Router::new()
            .route("/v2/names/{name}", get(|| async { "metric test" }))
            .layer(middleware::from_fn(track_http_request));

        for name in ["alice.eth", "bob.eth"] {
            app.clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/v2/names/{name}"))
                        .body(Body::empty())?,
                )
                .await?;
        }

        let after = api_metrics()
            .http_requests
            .with_label_values(&["/v2/names/{name}", "GET", "2xx"])
            .get();
        ensure!(after >= before + 2);
        let scrape = api_metrics().registry.encode()?;
        ensure!(!scrape.contains("alice.eth"));
        ensure!(!scrape.contains("bob.eth"));
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
        for line in body.lines().filter(|line| !line.is_empty()) {
            if line.starts_with('#') {
                continue;
            }
            let (_, value) = line
                .rsplit_once(' ')
                .with_context(|| format!("invalid Prometheus sample: {line}"))?;
            value
                .parse::<f64>()
                .with_context(|| format!("invalid Prometheus sample value: {line}"))?;
            samples += 1;
        }
        ensure!(samples > 0, "metrics scrape contained no samples");
        Ok(body)
    }
}
