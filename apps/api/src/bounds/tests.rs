use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request as HttpRequest, Uri},
    routing::get,
};
use serde_json::{Value, json};
use tokio::sync::{Notify, Semaphore};
use tower::ServiceExt;

use super::*;

fn test_config() -> ApiBoundsConfig {
    ApiBoundsConfig {
        request_timeout_ms: 1_000,
        db_statement_timeout_ms: 900,
        max_in_flight: 4,
        health_max_in_flight: 4,
        verified_execution_max_in_flight: 1,
        verified_rate_limit_per_second: 0,
        verified_rate_limit_burst: 1,
        verified_rate_limit_max_clients: 16,
        trust_x_forwarded_for: false,
    }
}

#[test]
fn default_config_is_valid_and_keeps_rate_limiting_disabled() {
    let config = ApiBoundsConfig::default();
    config.validate().expect("default bounds must be valid");
    assert_eq!(config.request_timeout(), Duration::from_secs(30));
    assert_eq!(config.db_statement_timeout(), Duration::from_secs(25));
    assert_eq!(config.health_max_in_flight, 4);
    assert_eq!(config.verified_rate_limit_per_second, 0);
}

#[test]
fn health_is_the_only_route_that_bypasses_global_load_shedding() {
    let bypass_paths = crate::API_ROUTE_DEFINITIONS
        .iter()
        .copied()
        .filter(|route| route.bypasses_global_load_shed())
        .map(|route| route.path)
        .collect::<Vec<_>>();

    assert_eq!(bypass_paths, vec!["/healthz"]);
}

#[test]
fn verified_request_classifier_covers_live_execution_modes() {
    let cases = [
        ("/v1/primary-names/0x01?mode=verified", true),
        ("/v1/primary-names/0x01", true),
        ("/v1/primary-names/0x01?mode=declared", true),
        ("/v1/profiles/names/alice.eth?mode=both", true),
        ("/v1/profiles/names/alice.eth?mode=%20both%20", true),
        ("/v1/profiles/names/alice.eth", true),
        ("/v1/profiles/names/alice.eth?mode=%20%20", false),
        ("/v1/profiles/names/alice.eth?mode=declared", false),
        ("/v1/names/ens/alice.eth/records?mode=auto", true),
        ("/v1/names/ens/alice.eth/records?mode=%20auto%20", true),
        (
            "/v1/names/ens/alice.eth/records?mode=auto&include=resolver_address",
            false,
        ),
        (
            "/v1/names/ens/alice.eth/records?mode=auto&include=known_text_keys",
            false,
        ),
        (
            "/v1/names/ens/alice.eth/records?mode=auto&include=avatar",
            true,
        ),
        (
            "/v1/names/ens/alice.eth/records?mode=auto&texts=description",
            true,
        ),
        ("/v1/names/ens/alice.eth/records?mode=declared", false),
        ("/v2/addresses/0x01/primary-name", true),
        ("/v2/addresses/0x01/primary-name?source=indexed", true),
        ("/v2/names/alice.eth?source=ver%69fied", true),
        ("/v2/names/alice.eth?source=%20verified%20", true),
        ("/v2/names/alice.eth?source=auto", false),
        ("/v2/names/alice.eth/records?source=auto", false),
        ("/v2/names/alice.eth/records?source=%20auto%20", false),
        ("/v2/names/alice.eth/records?source=auto&keys=%20%20", false),
        (
            "/v2/names/alice.eth/records?source=auto&keys=addr%3A60",
            true,
        ),
        ("/v2/diagnostics/names/alice.eth/records", true),
        ("/v2/diagnostics/names/alice.eth/execution", false),
    ];

    for (uri, expected) in cases {
        let uri = uri.parse::<Uri>().expect("test URI must parse");
        assert_eq!(
            is_verified_execution_request(&Method::GET, &uri),
            expected,
            "unexpected classification for {uri}"
        );
    }
}

#[test]
fn client_ip_uses_the_edge_appended_forwarded_address() {
    let mut request = HttpRequest::builder()
        .header("x-forwarded-for", "198.51.100.10, 203.0.113.20")
        .body(Body::empty())
        .expect("request must build");
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 3000))));

    assert_eq!(
        client_key(&request, true),
        ClientKey::Ip("203.0.113.20".parse().expect("IP must parse"))
    );
}

#[test]
fn direct_peer_cannot_replace_rate_limit_key_with_a_forwarded_header() {
    let request_for = |forwarded_for: &str| {
        let mut request = HttpRequest::builder()
            .header("x-forwarded-for", forwarded_for)
            .body(Body::empty())
            .expect("request must build");
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 3000))));
        request
    };

    assert_eq!(
        client_key(&request_for("198.51.100.10"), false),
        client_key(&request_for("203.0.113.20"), false),
        "an untrusted direct peer must not select its bucket with X-Forwarded-For"
    );
}

#[test]
fn ipv6_client_keys_are_bucketed_by_64_bit_prefix() {
    let first = forwarded_request("/", "2001:db8:abcd:12::1");
    let same_prefix = forwarded_request("/", "2001:db8:abcd:12::ffff");
    let other_prefix = forwarded_request("/", "2001:db8:abcd:13::1");

    assert_eq!(client_key(&first, true), client_key(&same_prefix, true));
    assert_ne!(client_key(&first, true), client_key(&other_prefix, true));
}

#[test]
fn ipv4_mapped_ipv6_client_keys_use_the_embedded_ipv4_address() {
    let first = forwarded_request("/", "::ffff:203.0.113.1");
    let second = forwarded_request("/", "::ffff:198.51.100.2");
    let native = forwarded_request("/", "203.0.113.1");

    assert_eq!(client_key(&first, true), client_key(&native, true));
    assert_ne!(client_key(&first, true), client_key(&second, true));
}

#[tokio::test]
async fn request_timeout_returns_json_error_envelope() {
    let mut config = test_config();
    config.request_timeout_ms = 5;
    let app = apply_request_bounds(
        Router::new().route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                "late"
            }),
        ),
        Router::new(),
        &config,
    );

    let response = app
        .oneshot(request("/slow"))
        .await
        .expect("bounded request must complete");

    assert_error(response, StatusCode::REQUEST_TIMEOUT, "request_timeout").await;
}

#[tokio::test]
async fn shed_bypass_routes_keep_the_request_timeout_backstop() {
    let mut config = test_config();
    config.request_timeout_ms = 5;
    let app = apply_request_bounds(
        Router::new(),
        Router::new().route(
            "/healthz",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                "late"
            }),
        ),
        &config,
    );

    let response = app
        .oneshot(request("/healthz"))
        .await
        .expect("bounded health request must complete");

    assert_error(response, StatusCode::REQUEST_TIMEOUT, "request_timeout").await;
}

#[tokio::test]
async fn global_concurrency_limit_sheds_with_json_error_envelope() {
    let mut config = test_config();
    config.max_in_flight = 2;
    let (started, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let app = apply_request_bounds(
        Router::new().route(
            "/slow",
            get({
                let started = started.clone();
                let release = release.clone();
                move || {
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        started.send(()).expect("test receiver must stay open");
                        release.notified().await;
                        "ok"
                    }
                }
            }),
        ),
        Router::new(),
        &config,
    );

    let first = tokio::spawn(app.clone().oneshot(request("/slow")));
    started_rx.recv().await.expect("first request must start");
    let second = tokio::spawn(app.clone().oneshot(request("/slow")));
    started_rx.recv().await.expect("second request must start");
    let response = app
        .clone()
        .oneshot(request("/slow"))
        .await
        .expect("shed request must complete");
    assert_error(response, StatusCode::SERVICE_UNAVAILABLE, "overloaded").await;

    release.notify_waiters();
    first
        .await
        .expect("first task must join")
        .expect("first request must complete");
    second
        .await
        .expect("second task must join")
        .expect("second request must complete");
}

#[tokio::test]
async fn healthz_remains_ready_while_global_concurrency_is_saturated() {
    let mut config = test_config();
    config.max_in_flight = 2;
    let (started, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let app = apply_request_bounds(
        Router::new()
            .route(
                "/slow",
                get({
                    let started = started.clone();
                    let release = release.clone();
                    move || {
                        let started = started.clone();
                        let release = release.clone();
                        async move {
                            started.send(()).expect("test receiver must stay open");
                            release.notified().await;
                            "ok"
                        }
                    }
                }),
            )
            .route("/v1/status", get(|| async { "visible" }))
            .route("/v2/status", get(|| async { "visible" })),
        Router::new().route(
            "/healthz",
            get(|| async { axum::Json(json!({ "status": "ready" })) }),
        ),
        &config,
    );

    let first = tokio::spawn(app.clone().oneshot(request("/slow")));
    started_rx.recv().await.expect("first request must start");
    let second = tokio::spawn(app.clone().oneshot(request("/slow")));
    started_rx.recv().await.expect("second request must start");

    let health = app
        .clone()
        .oneshot(request("/healthz"))
        .await
        .expect("health request must complete");
    assert_eq!(health.status(), StatusCode::OK);
    let body = to_bytes(health.into_body(), usize::MAX)
        .await
        .expect("health body must read");
    let payload: Value = serde_json::from_slice(&body).expect("health body must be JSON");
    assert_eq!(payload.get("status"), Some(&json!("ready")));
    for uri in ["/v1/status", "/v2/status"] {
        let status = app
            .clone()
            .oneshot(request(uri))
            .await
            .expect("status request must complete");
        assert_error(status, StatusCode::SERVICE_UNAVAILABLE, "overloaded").await;
    }
    let unmatched = app
        .clone()
        .oneshot(request("/v1/not-a-route"))
        .await
        .expect("unmatched request must complete");
    assert_error(unmatched, StatusCode::SERVICE_UNAVAILABLE, "overloaded").await;

    release.notify_waiters();
    first
        .await
        .expect("first task must join")
        .expect("first request must complete");
    second
        .await
        .expect("second task must join")
        .expect("second request must complete");
}

#[tokio::test]
async fn health_has_a_dedicated_concurrency_ceiling() {
    let config = test_config();
    let health_max_in_flight = config.health_max_in_flight;
    let (started, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let app = apply_request_bounds(
        Router::new(),
        Router::new().route(
            "/healthz",
            get({
                let started = started.clone();
                let release = release.clone();
                move || {
                    let started = started.clone();
                    let release = release.clone();
                    async move {
                        started.send(()).expect("test receiver must stay open");
                        let permit = release.acquire().await.expect("semaphore must stay open");
                        permit.forget();
                        "ready"
                    }
                }
            }),
        ),
        &config,
    );

    let mut in_flight = Vec::new();
    for _ in 0..health_max_in_flight {
        in_flight.push(tokio::spawn(app.clone().oneshot(request("/healthz"))));
        started_rx.recv().await.expect("health request must start");
    }
    let shed = tokio::time::timeout(
        Duration::from_millis(50),
        app.clone().oneshot(request("/healthz")),
    )
    .await;

    release.add_permits(health_max_in_flight + 1);
    for request_task in in_flight {
        request_task
            .await
            .expect("health task must join")
            .expect("health request must complete");
    }
    let response = shed
        .expect("health requests beyond the dedicated ceiling must shed")
        .expect("shed health request must complete");
    assert_error(response, StatusCode::SERVICE_UNAVAILABLE, "overloaded").await;
}

#[tokio::test]
async fn verified_concurrency_limit_is_separate_from_cheap_requests() {
    let config = test_config();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let app = apply_request_bounds(
        Router::new()
            .route(
                "/v1/primary-names/{address}",
                get({
                    let started = started.clone();
                    let release = release.clone();
                    move || {
                        let started = started.clone();
                        let release = release.clone();
                        async move {
                            started.notify_one();
                            release.notified().await;
                            "ok"
                        }
                    }
                }),
            )
            .route("/cheap", get(|| async { "ok" })),
        Router::new(),
        &config,
    );

    let first = tokio::spawn(app.clone().oneshot(request("/v1/primary-names/0x01")));
    started.notified().await;

    let overloaded = app
        .clone()
        .oneshot(request("/v1/primary-names/0x02?mode=declared"))
        .await
        .expect("shed request must complete");
    assert_error(overloaded, StatusCode::SERVICE_UNAVAILABLE, "overloaded").await;

    let cheap = app
        .clone()
        .oneshot(request("/cheap"))
        .await
        .expect("cheap request must complete");
    assert_eq!(cheap.status(), StatusCode::OK);

    release.notify_waiters();
    first
        .await
        .expect("first task must join")
        .expect("first request must complete");
}

#[tokio::test]
async fn empty_auto_records_bypass_verified_concurrency_admission() {
    let config = test_config();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler = {
        let started = started.clone();
        let release = release.clone();
        move |uri: Uri| {
            let started = started.clone();
            let release = release.clone();
            async move {
                if uri.query().is_some_and(|query| query.contains("hold=true")) {
                    started.notify_one();
                    release.notified().await;
                }
                "ok"
            }
        }
    };
    let app = apply_request_bounds(
        Router::new()
            .route("/v1/names/{namespace}/{name}/records", get(handler.clone()))
            .route("/v2/names/{name}/records", get(handler)),
        Router::new(),
        &config,
    );

    let held = tokio::spawn(app.clone().oneshot(request(
        "/v1/names/ens/alice.eth/records?mode=auto&avatar=true&hold=true",
    )));
    started.notified().await;

    for uri in [
        "/v1/names/ens/alice.eth/records?mode=auto&include=resolver_address",
        "/v2/names/alice.eth/records?source=auto",
    ] {
        let response = app
            .clone()
            .oneshot(request(uri))
            .await
            .expect("empty auto records request must complete");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "unexpected status for {uri}"
        );
    }

    let overloaded = app
        .clone()
        .oneshot(request(
            "/v2/names/alice.eth/records?source=auto&keys=addr:60",
        ))
        .await
        .expect("non-empty auto records request must complete");
    assert_error(overloaded, StatusCode::SERVICE_UNAVAILABLE, "overloaded").await;

    release.notify_waiters();
    held.await
        .expect("held task must join")
        .expect("held request must complete");
}

#[tokio::test]
async fn empty_auto_records_do_not_consume_verified_rate_limit_tokens() {
    let mut config = test_config();
    config.verified_rate_limit_per_second = 1;
    let app = apply_request_bounds(
        Router::new()
            .route(
                "/v1/names/{namespace}/{name}/records",
                get(|| async { "ok" }),
            )
            .route("/v2/names/{name}/records", get(|| async { "ok" })),
        Router::new(),
        &config,
    );

    let first = app
        .clone()
        .oneshot(request(
            "/v1/names/ens/alice.eth/records?mode=auto&avatar=true",
        ))
        .await
        .expect("first non-empty auto records request must complete");
    assert_eq!(first.status(), StatusCode::OK);

    for uri in [
        "/v1/names/ens/alice.eth/records?mode=auto&include=resolver_address",
        "/v2/names/alice.eth/records?source=auto&keys=",
    ] {
        let response = app
            .clone()
            .oneshot(request(uri))
            .await
            .expect("empty auto records request must complete");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "unexpected status for {uri}"
        );
    }

    let limited = app
        .oneshot(request(
            "/v2/names/alice.eth/records?source=auto&keys=addr:60",
        ))
        .await
        .expect("second non-empty auto records request must complete");
    assert_error(limited, StatusCode::TOO_MANY_REQUESTS, "rate_limited").await;
}

#[tokio::test]
async fn enabled_rate_limit_is_per_forwarded_client_ip() {
    let mut config = test_config();
    config.verified_rate_limit_per_second = 1;
    config.trust_x_forwarded_for = true;
    let app = apply_request_bounds(
        Router::new().route("/v1/primary-names/{address}", get(|| async { "ok" })),
        Router::new(),
        &config,
    );

    let first = app
        .clone()
        .oneshot(forwarded_request(
            "/v1/primary-names/0x01?mode=%20verified%20",
            "203.0.113.1",
        ))
        .await
        .expect("first request must complete");
    assert_eq!(first.status(), StatusCode::OK);

    let limited = app
        .clone()
        .oneshot(forwarded_request(
            "/v1/primary-names/0x02?mode=verified",
            "203.0.113.1",
        ))
        .await
        .expect("limited request must complete");
    assert_error(limited, StatusCode::TOO_MANY_REQUESTS, "rate_limited").await;

    let other_client = app
        .oneshot(forwarded_request(
            "/v1/primary-names/0x03?mode=verified",
            "203.0.113.2",
        ))
        .await
        .expect("other-client request must complete");
    assert_eq!(other_client.status(), StatusCode::OK);
}

#[test]
fn full_rate_limit_table_fails_closed_with_an_observable_decision() {
    let limiter = ClientRateLimiter::new(1, 2, 1);
    let first = ClientKey::Ip("203.0.113.1".parse().expect("IP must parse"));
    let second = ClientKey::Ip("203.0.113.2".parse().expect("IP must parse"));

    assert_eq!(limiter.check(first), RateLimitDecision::Allowed);
    assert_eq!(
        limiter.check(second),
        RateLimitDecision::TableFull {
            tracked_clients: 1,
            rejection_count: 1,
        }
    );
    assert_eq!(
        limiter.check(second),
        RateLimitDecision::TableFull {
            tracked_clients: 1,
            rejection_count: 2,
        }
    );
    assert!(should_log_table_full(1));
    assert!(should_log_table_full(2));
    assert!(!should_log_table_full(3));
}

fn request(uri: &str) -> HttpRequest<Body> {
    HttpRequest::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request must build")
}

fn forwarded_request(uri: &str, client_ip: &str) -> HttpRequest<Body> {
    HttpRequest::builder()
        .uri(uri)
        .header("x-forwarded-for", client_ip)
        .body(Body::empty())
        .expect("request must build")
}

async fn assert_error(response: Response, status: StatusCode, code: &str) {
    assert_eq!(response.status(), status);
    assert_eq!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("*")
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("error body must read");
    let payload: Value = serde_json::from_slice(&body).expect("error body must be JSON");
    assert_eq!(payload.pointer("/error/code"), Some(&json!(code)));
    assert_eq!(payload.pointer("/error/details"), Some(&json!({})));
}
