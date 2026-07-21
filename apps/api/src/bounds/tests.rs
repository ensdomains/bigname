use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request as HttpRequest,
    routing::get,
};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tower::ServiceExt;

use super::*;

fn test_config() -> ApiBoundsConfig {
    ApiBoundsConfig {
        request_timeout_ms: 1_000,
        db_statement_timeout_ms: 900,
        max_in_flight: 4,
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
    assert_eq!(config.verified_rate_limit_per_second, 0);
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
        ("/v1/names/ens/alice.eth/records?mode=declared", false),
        ("/v2/addresses/0x01/primary-name", true),
        ("/v2/addresses/0x01/primary-name?source=indexed", true),
        ("/v2/names/alice.eth?source=ver%69fied", true),
        ("/v2/names/alice.eth?source=%20verified%20", true),
        ("/v2/names/alice.eth?source=auto", false),
        ("/v2/names/alice.eth/records?source=auto", true),
        ("/v2/names/alice.eth/records?source=%20auto%20", true),
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
        &config,
    );

    let response = app
        .oneshot(request("/slow"))
        .await
        .expect("bounded request must complete");

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
async fn enabled_rate_limit_is_per_forwarded_client_ip() {
    let mut config = test_config();
    config.verified_rate_limit_per_second = 1;
    config.trust_x_forwarded_for = true;
    let app = apply_request_bounds(
        Router::new().route("/v1/primary-names/{address}", get(|| async { "ok" })),
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
