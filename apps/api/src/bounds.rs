use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Result, ensure};
use axum::{
    BoxError, Router,
    error_handling::HandleErrorLayer,
    extract::{ConnectInfo, Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use clap::Args;
use tokio::sync::Semaphore;
use tower::{
    ServiceBuilder, limit::GlobalConcurrencyLimitLayer, load_shed::LoadShedLayer,
    timeout::TimeoutLayer,
};

use crate::ApiError;

#[path = "bounds/classification.rs"]
mod classification;
use classification::is_verified_execution_request;

const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_DB_STATEMENT_TIMEOUT_MS: u64 = 25_000;
const DEFAULT_MAX_IN_FLIGHT: usize = 1_024;
const DEFAULT_HEALTH_MAX_IN_FLIGHT: usize = 4;
const DEFAULT_VERIFIED_MAX_IN_FLIGHT: usize = 128;
const DEFAULT_RATE_LIMIT_BURST: u32 = 10;
const DEFAULT_RATE_LIMIT_MAX_CLIENTS: usize = 65_536;

#[derive(Args, Clone, Debug)]
pub(crate) struct ApiBoundsConfig {
    #[arg(
        long,
        env = "BIGNAME_API_REQUEST_TIMEOUT_MS",
        default_value_t = DEFAULT_REQUEST_TIMEOUT_MS
    )]
    pub(crate) request_timeout_ms: u64,
    #[arg(
        long,
        env = "BIGNAME_API_DB_STATEMENT_TIMEOUT_MS",
        default_value_t = DEFAULT_DB_STATEMENT_TIMEOUT_MS
    )]
    pub(crate) db_statement_timeout_ms: u64,
    #[arg(
        long,
        env = "BIGNAME_API_MAX_IN_FLIGHT",
        default_value_t = DEFAULT_MAX_IN_FLIGHT
    )]
    pub(crate) max_in_flight: usize,
    #[arg(
        long,
        env = "BIGNAME_API_HEALTH_MAX_IN_FLIGHT",
        default_value_t = DEFAULT_HEALTH_MAX_IN_FLIGHT
    )]
    pub(crate) health_max_in_flight: usize,
    #[arg(
        long,
        env = "BIGNAME_API_VERIFIED_EXECUTION_MAX_IN_FLIGHT",
        default_value_t = DEFAULT_VERIFIED_MAX_IN_FLIGHT
    )]
    pub(crate) verified_execution_max_in_flight: usize,
    #[arg(
        long,
        env = "BIGNAME_API_VERIFIED_RATE_LIMIT_PER_SECOND",
        default_value_t = 0_u32
    )]
    pub(crate) verified_rate_limit_per_second: u32,
    #[arg(
        long,
        env = "BIGNAME_API_VERIFIED_RATE_LIMIT_BURST",
        default_value_t = DEFAULT_RATE_LIMIT_BURST
    )]
    pub(crate) verified_rate_limit_burst: u32,
    #[arg(
        long,
        env = "BIGNAME_API_VERIFIED_RATE_LIMIT_MAX_CLIENTS",
        default_value_t = DEFAULT_RATE_LIMIT_MAX_CLIENTS
    )]
    pub(crate) verified_rate_limit_max_clients: usize,
    #[arg(
        long,
        env = "BIGNAME_API_TRUST_X_FORWARDED_FOR",
        default_value_t = false
    )]
    pub(crate) trust_x_forwarded_for: bool,
}

impl Default for ApiBoundsConfig {
    fn default() -> Self {
        Self {
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            db_statement_timeout_ms: DEFAULT_DB_STATEMENT_TIMEOUT_MS,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            health_max_in_flight: DEFAULT_HEALTH_MAX_IN_FLIGHT,
            verified_execution_max_in_flight: DEFAULT_VERIFIED_MAX_IN_FLIGHT,
            verified_rate_limit_per_second: 0,
            verified_rate_limit_burst: DEFAULT_RATE_LIMIT_BURST,
            verified_rate_limit_max_clients: DEFAULT_RATE_LIMIT_MAX_CLIENTS,
            trust_x_forwarded_for: false,
        }
    }
}

impl ApiBoundsConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.request_timeout_ms > 0,
            "BIGNAME_API_REQUEST_TIMEOUT_MS must be greater than zero"
        );
        ensure!(
            self.db_statement_timeout_ms > 0,
            "BIGNAME_API_DB_STATEMENT_TIMEOUT_MS must be greater than zero"
        );
        ensure!(
            self.max_in_flight > 1,
            "BIGNAME_API_MAX_IN_FLIGHT must be greater than one"
        );
        ensure!(
            self.health_max_in_flight > 0,
            "BIGNAME_API_HEALTH_MAX_IN_FLIGHT must be greater than zero"
        );
        ensure!(
            self.verified_execution_max_in_flight > 0
                && self.verified_execution_max_in_flight < self.max_in_flight,
            "BIGNAME_API_VERIFIED_EXECUTION_MAX_IN_FLIGHT must be greater than zero and lower than BIGNAME_API_MAX_IN_FLIGHT"
        );
        if self.verified_rate_limit_per_second > 0 {
            ensure!(
                self.verified_rate_limit_burst > 0,
                "BIGNAME_API_VERIFIED_RATE_LIMIT_BURST must be greater than zero when rate limiting is enabled"
            );
            ensure!(
                self.verified_rate_limit_max_clients > 0,
                "BIGNAME_API_VERIFIED_RATE_LIMIT_MAX_CLIENTS must be greater than zero when rate limiting is enabled"
            );
        }
        Ok(())
    }

    pub(crate) const fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    pub(crate) const fn db_statement_timeout(&self) -> Duration {
        Duration::from_millis(self.db_statement_timeout_ms)
    }
}

pub(crate) fn apply_request_bounds<S>(
    bounded_router: Router<S>,
    health_router: Router<S>,
    config: &ApiBoundsConfig,
) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let verified_admission = VerifiedExecutionAdmission::new(config);
    let global_admission = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_global_bound_error))
        .layer(LoadShedLayer::new())
        .layer(GlobalConcurrencyLimitLayer::new(config.max_in_flight));
    let health_admission = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_global_bound_error))
        .layer(LoadShedLayer::new())
        .layer(GlobalConcurrencyLimitLayer::new(
            config.health_max_in_flight,
        ));
    let request_timeout = ServiceBuilder::new()
        .layer(HandleErrorLayer::new(handle_global_bound_error))
        .layer(TimeoutLayer::new(config.request_timeout()));

    let bounded_router = bounded_router
        .layer(middleware::from_fn_with_state(
            verified_admission,
            enforce_verified_execution_bounds,
        ))
        .layer(global_admission);

    health_router
        .layer(health_admission)
        // Keep the bounded router on the right so its fallback also remains behind global
        // admission; only a matched health route may use the reserved health capacity.
        .merge(bounded_router)
        .layer(request_timeout)
}

async fn handle_global_bound_error(error: BoxError) -> Response {
    if error.is::<tower::timeout::error::Elapsed>() {
        return bound_error(
            StatusCode::REQUEST_TIMEOUT,
            "request_timeout",
            "request exceeded the configured time limit",
        );
    }
    if error.is::<tower::load_shed::error::Overloaded>() {
        return overloaded_response();
    }

    tracing::error!(service = "api", error = ?error, "request-bound middleware failed");
    ApiError::internal_error("request-bound middleware failed").into_response()
}

#[derive(Clone)]
struct VerifiedExecutionAdmission {
    semaphore: Arc<Semaphore>,
    rate_limiter: Option<Arc<ClientRateLimiter>>,
    trust_x_forwarded_for: bool,
}

impl VerifiedExecutionAdmission {
    fn new(config: &ApiBoundsConfig) -> Self {
        let rate_limiter = (config.verified_rate_limit_per_second > 0).then(|| {
            Arc::new(ClientRateLimiter::new(
                config.verified_rate_limit_per_second,
                config.verified_rate_limit_burst,
                config.verified_rate_limit_max_clients,
            ))
        });
        Self {
            semaphore: Arc::new(Semaphore::new(config.verified_execution_max_in_flight)),
            rate_limiter,
            trust_x_forwarded_for: config.trust_x_forwarded_for,
        }
    }
}

async fn enforce_verified_execution_bounds(
    State(admission): State<VerifiedExecutionAdmission>,
    request: Request,
    next: Next,
) -> Response {
    if !is_verified_execution_request(request.method(), request.uri()) {
        return next.run(request).await;
    }

    if let Some(rate_limiter) = admission.rate_limiter.as_ref() {
        match rate_limiter.check(client_key(&request, admission.trust_x_forwarded_for)) {
            RateLimitDecision::Allowed => {}
            RateLimitDecision::Limited => return rate_limited_response(),
            RateLimitDecision::TableFull {
                tracked_clients,
                rejection_count,
            } => {
                if should_log_table_full(rejection_count) {
                    tracing::warn!(
                        service = "api",
                        tracked_clients,
                        max_clients = rate_limiter.max_clients,
                        rejection_count,
                        "verified request rate limiter client table is full; rejecting new client"
                    );
                }
                return rate_limited_response();
            }
        }
    }

    let Ok(permit) = admission.semaphore.clone().try_acquire_owned() else {
        return overloaded_response();
    };
    let response = next.run(request).await;
    drop(permit);
    response
}

fn overloaded_response() -> Response {
    bound_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "overloaded",
        "API is temporarily overloaded",
    )
}

fn rate_limited_response() -> Response {
    bound_error(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limited",
        "verified execution request rate limit exceeded",
    )
}

fn should_log_table_full(rejection_count: u64) -> bool {
    rejection_count.is_power_of_two()
}

fn bound_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    let mut response = ApiError {
        status,
        code,
        message: message.to_owned(),
    }
    .into_response();
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ClientKey {
    Ip(IpAddr),
    Unidentified,
}

fn client_key(request: &Request, trust_x_forwarded_for: bool) -> ClientKey {
    let forwarded_ip = trust_x_forwarded_for.then(|| {
        request
            .headers()
            .get_all("x-forwarded-for")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .filter_map(parse_forwarded_ip)
            .next_back()
    });
    forwarded_ip
        .flatten()
        .or_else(|| {
            request
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|connect_info| connect_info.0.ip())
        })
        .map(normalize_client_ip)
        .map(ClientKey::Ip)
        .unwrap_or(ClientKey::Unidentified)
}

fn normalize_client_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V4(address) => IpAddr::V4(address),
        IpAddr::V6(address) => address
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or_else(|| IpAddr::V6((u128::from(address) & (u128::MAX << 64)).into())),
    }
}

fn parse_forwarded_ip(value: &str) -> Option<IpAddr> {
    let value = value.trim();
    value
        .parse::<IpAddr>()
        .ok()
        .or_else(|| value.parse::<SocketAddr>().ok().map(|address| address.ip()))
}

struct ClientRateLimiter {
    rate_per_second: f64,
    burst: f64,
    max_clients: usize,
    buckets: Mutex<HashMap<ClientKey, TokenBucket>>,
    table_full_rejections: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RateLimitDecision {
    Allowed,
    Limited,
    TableFull {
        tracked_clients: usize,
        rejection_count: u64,
    },
}

impl ClientRateLimiter {
    fn new(rate_per_second: u32, burst: u32, max_clients: usize) -> Self {
        Self {
            rate_per_second: f64::from(rate_per_second),
            burst: f64::from(burst),
            max_clients,
            buckets: Mutex::new(HashMap::new()),
            table_full_rejections: AtomicU64::new(0),
        }
    }

    fn check(&self, client: ClientKey) -> RateLimitDecision {
        let now = Instant::now();
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(bucket) = buckets.get_mut(&client) {
            bucket.refill(now, self.rate_per_second, self.burst);
            return if bucket.take() {
                RateLimitDecision::Allowed
            } else {
                RateLimitDecision::Limited
            };
        }

        if buckets.len() >= self.max_clients {
            buckets.retain(|_, bucket| {
                bucket.available(now, self.rate_per_second, self.burst) < self.burst
            });
            if buckets.len() >= self.max_clients {
                let tracked_clients = buckets.len();
                drop(buckets);
                return RateLimitDecision::TableFull {
                    tracked_clients,
                    rejection_count: self
                        .table_full_rejections
                        .fetch_add(1, Ordering::Relaxed)
                        .saturating_add(1),
                };
            }
        }
        buckets.insert(client, TokenBucket::after_first_request(now, self.burst));
        RateLimitDecision::Allowed
    }
}

struct TokenBucket {
    tokens: f64,
    updated_at: Instant,
}

impl TokenBucket {
    fn after_first_request(now: Instant, burst: f64) -> Self {
        Self {
            tokens: burst - 1.0,
            updated_at: now,
        }
    }

    fn available(&self, now: Instant, rate_per_second: f64, burst: f64) -> f64 {
        (self.tokens + now.duration_since(self.updated_at).as_secs_f64() * rate_per_second)
            .min(burst)
    }

    fn refill(&mut self, now: Instant, rate_per_second: f64, burst: f64) {
        self.tokens = self.available(now, rate_per_second, burst);
        self.updated_at = now;
    }

    fn take(&mut self) -> bool {
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

#[cfg(test)]
#[path = "bounds/tests.rs"]
mod tests;
