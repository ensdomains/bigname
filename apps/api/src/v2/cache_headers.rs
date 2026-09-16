//! `Cache-Control` and weak `ETag` for indexed single-resource reads.
//!
//! The snapshot token identifies chain positions, but rebuilt projections or response logic can
//! change the representation at those same positions. Hash the serialized response body for the
//! weak validator, so conditional reads return `304 Not Modified` only for an unchanged body.
//! It applies only to the routes it is mounted
//! on (name detail, name records, resolver overview, primary name), only to `200` responses whose
//! body carries `meta.as_of_token`, and only when the request is an indexed read: a `source` of
//! `verified` or `auto` executes against a provider and is never cached, and the primary-name route
//! qualifies only when the caller asked for `source=indexed` explicitly, because its default answer
//! set includes the verified source. `POST /v1/lookup`, collections, and error responses never
//! reach this layer.

use std::borrow::Cow;

use axum::{
    body::{Body, to_bytes},
    extract::Request,
    http::{HeaderValue, Method, StatusCode, Uri, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::Value;

use super::V2Error;

/// One Ethereum slot of freshness, then revalidate against the weak validator; an edge may keep
/// serving the stale body for four more slots while it revalidates in the background.
pub(crate) const INDEXED_READ_CACHE_CONTROL: &str = "public, max-age=12, stale-while-revalidate=48";
const RESPONSE_BODY_LIMIT: usize = 64 * 1024 * 1024;

pub(crate) async fn indexed_read_cache_headers(request: Request, next: Next) -> Response {
    let cacheable = is_cacheable_indexed_read(request.method(), request.uri());
    let if_none_match = request
        .headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let response = next.run(request).await;
    if !cacheable || response.status() != StatusCode::OK {
        return response;
    }

    let (parts, body) = response.into_parts();
    let bytes = match to_bytes(body, RESPONSE_BODY_LIMIT).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return V2Error::internal_error("failed to read indexed response body").into_response();
        }
    };
    if as_of_token(&bytes).is_none() {
        return Response::from_parts(parts, Body::from(bytes));
    }
    let token = alloy_primitives::keccak256(&bytes).to_string();
    let Ok(etag) = HeaderValue::from_str(&format!("W/\"{token}\"")) else {
        return Response::from_parts(parts, Body::from(bytes));
    };

    let mut response = if if_none_match
        .as_deref()
        .is_some_and(|candidates| if_none_match_matches(candidates, &token))
    {
        let mut not_modified = Response::new(Body::empty());
        *not_modified.status_mut() = StatusCode::NOT_MODIFIED;
        not_modified
    } else {
        Response::from_parts(parts, Body::from(bytes))
    };
    response.headers_mut().insert(header::ETAG, etag);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(INDEXED_READ_CACHE_CONTROL),
    );
    response
}

/// Whether the request is an indexed read of one of the mounted single-resource routes.
pub(crate) fn is_cacheable_indexed_read(method: &Method, uri: &Uri) -> bool {
    if method != Method::GET {
        return false;
    }
    let segments = uri
        .path()
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    match segments.as_slice() {
        ["v1", "names", _] | ["v1", "names", _, "records"] => source_is_absent_or(uri, "indexed"),
        ["v1", "resolvers", _, _] => true,
        ["v1", "addresses", _, "primary-name"] => source_is_exactly(uri, "indexed"),
        _ => false,
    }
}

fn source_is_absent_or(uri: &Uri, accepted: &str) -> bool {
    query_values(uri, "source").all(|value| value.trim().is_empty() || value.trim() == accepted)
}

fn source_is_exactly(uri: &Uri, accepted: &str) -> bool {
    let mut values = query_values(uri, "source").map(|value| value.trim().to_owned());
    matches!(values.next(), Some(value) if value == accepted) && values.next().is_none()
}

fn query_values<'a>(uri: &'a Uri, key: &'a str) -> impl Iterator<Item = Cow<'a, str>> + 'a {
    form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .filter(move |(candidate, _)| candidate == key)
        .map(|(_, value)| value)
}

fn as_of_token(body: &[u8]) -> Option<String> {
    let payload: Value = serde_json::from_slice(body).ok()?;
    let token = payload.get("meta")?.get("as_of_token")?.as_str()?;
    (!token.is_empty()
        && token
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'"'))
    .then(|| token.to_owned())
}

/// RFC 9110 `If-None-Match`: a list of entity tags or `*`; weak comparison, so `W/"t"` and `"t"`
/// both match the served weak validator.
fn if_none_match_matches(candidates: &str, token: &str) -> bool {
    candidates.split(',').map(str::trim).any(|candidate| {
        candidate == "*"
            || candidate
                .strip_prefix("W/")
                .unwrap_or(candidate)
                .strip_prefix('"')
                .and_then(|rest| rest.strip_suffix('"'))
                == Some(token)
    })
}

#[cfg(test)]
mod tests {
    use axum::{
        Json, Router,
        body::to_bytes,
        http::{Request, StatusCode, header},
        middleware,
        routing::get,
    };
    use serde_json::json;
    use tower::ServiceExt;

    use super::*;

    fn router() -> Router {
        router_with_data(json!({}))
    }

    fn router_with_data(data: Value) -> Router {
        Router::new()
            .route(
                "/v1/names/{name}",
                get(move || {
                    let data = data.clone();
                    async move { Json(json!({"data": data, "meta": {"as_of_token": "abc123"}})) }
                }),
            )
            .route(
                "/v1/names/{name}/records",
                get(|| async { Json(json!({"data": {}, "meta": {}})) }),
            )
            .route(
                "/v1/resolvers/{chain_id}/{address}",
                get(|| async {
                    (
                        StatusCode::NOT_FOUND,
                        Json(json!({"error": {"code": "not_found"}, "meta": {"as_of_token": "x"}})),
                    )
                }),
            )
            .route(
                "/v1/addresses/{address}/primary-name",
                get(|| async { Json(json!({"data": {}, "meta": {"as_of_token": "abc123"}})) }),
            )
            .route_layer(middleware::from_fn(indexed_read_cache_headers))
    }

    fn default_etag() -> String {
        let bytes =
            serde_json::to_vec(&json!({"data": {}, "meta": {"as_of_token": "abc123"}})).unwrap();
        format!("W/\"{}\"", alloy_primitives::keccak256(bytes))
    }

    async fn send(uri: &str, if_none_match: Option<&str>) -> Response {
        let mut request = Request::builder().uri(uri);
        if let Some(value) = if_none_match {
            request = request.header(header::IF_NONE_MATCH, value);
        }
        router()
            .oneshot(request.body(Body::empty()).expect("request must build"))
            .await
            .expect("request must complete")
    }

    #[tokio::test]
    async fn indexed_read_carries_weak_etag_and_cache_control_from_body() {
        let response = send("/v1/names/alice.eth", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::ETAG)
                .and_then(|v| v.to_str().ok()),
            Some(default_etag().as_str())
        );
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some(INDEXED_READ_CACHE_CONTROL)
        );
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(
            serde_json::from_slice::<Value>(&body).expect("json")["meta"]["as_of_token"],
            json!("abc123")
        );
    }

    #[tokio::test]
    async fn matching_if_none_match_returns_not_modified_without_a_body() {
        let etag = default_etag();
        for candidate in [
            etag.clone(),
            etag.strip_prefix("W/").unwrap().to_owned(),
            format!("W/\"other\", {etag}"),
            "*".to_owned(),
        ] {
            let response = send("/v1/names/alice.eth", Some(&candidate)).await;
            assert_eq!(response.status(), StatusCode::NOT_MODIFIED, "{candidate}");
            assert_eq!(
                response
                    .headers()
                    .get(header::ETAG)
                    .and_then(|v| v.to_str().ok()),
                Some(default_etag().as_str())
            );
            assert!(response.headers().contains_key(header::CACHE_CONTROL));
            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            assert!(body.is_empty());
        }

        let response = send("/v1/names/alice.eth", Some("W/\"stale\"")).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn changed_representation_at_same_snapshot_invalidates_cached_response() {
        for uri in ["/v1/names/alice.eth", "/v1/names/alice.eth?at=abc123"] {
            let old = send(uri, None).await;
            let old_etag = old.headers()[header::ETAG].clone();
            let corrected = router_with_data(json!({"owner": "corrected"}))
                .oneshot(
                    Request::builder()
                        .uri(uri)
                        .header(header::IF_NONE_MATCH, old_etag.clone())
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(corrected.status(), StatusCode::OK);
            assert_ne!(corrected.headers()[header::ETAG], old_etag);
            let body = to_bytes(corrected.into_body(), usize::MAX).await.unwrap();
            let payload: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(payload["data"]["owner"], "corrected");
            assert_eq!(payload["meta"]["as_of_token"], "abc123");
        }
    }

    #[tokio::test]
    async fn verified_auto_errors_and_tokenless_bodies_are_not_cached() {
        for uri in [
            "/v1/names/alice.eth?source=verified",
            "/v1/names/alice.eth?source=auto",
            "/v1/names/alice.eth?source=ver%69fied",
            "/v1/addresses/0xabc/primary-name",
            "/v1/addresses/0xabc/primary-name?source=verified",
            "/v1/names/alice.eth/records",
            "/v1/resolvers/1/0xabc",
        ] {
            let response = send(uri, Some("*")).await;
            assert_ne!(response.status(), StatusCode::NOT_MODIFIED, "{uri}");
            assert!(
                !response.headers().contains_key(header::ETAG),
                "{uri} must not carry an ETag"
            );
            assert!(
                !response.headers().contains_key(header::CACHE_CONTROL),
                "{uri} must not carry Cache-Control"
            );
        }

        let response = send("/v1/addresses/0xabc/primary-name?source=indexed", None).await;
        assert_eq!(
            response
                .headers()
                .get(header::ETAG)
                .and_then(|v| v.to_str().ok()),
            Some(default_etag().as_str())
        );
        let response = send("/v1/names/alice.eth?source=indexed", None).await;
        assert!(response.headers().contains_key(header::ETAG));
    }

    #[test]
    fn cacheability_is_route_and_source_shaped() {
        let uri = |value: &str| value.parse::<Uri>().expect("uri");
        assert!(is_cacheable_indexed_read(
            &Method::GET,
            &uri("/v1/names/a.eth")
        ));
        assert!(is_cacheable_indexed_read(
            &Method::GET,
            &uri("/v1/names/a.eth?source=")
        ));
        assert!(is_cacheable_indexed_read(
            &Method::GET,
            &uri("/v1/names/a.eth/records")
        ));
        assert!(is_cacheable_indexed_read(
            &Method::GET,
            &uri("/v1/resolvers/1/0xabc?at=x")
        ));
        assert!(!is_cacheable_indexed_read(
            &Method::POST,
            &uri("/v1/names/a.eth")
        ));
        assert!(!is_cacheable_indexed_read(&Method::GET, &uri("/v1/lookup")));
        assert!(!is_cacheable_indexed_read(
            &Method::GET,
            &uri("/v1/names/a.eth/subnames")
        ));
        assert!(!is_cacheable_indexed_read(
            &Method::GET,
            &uri("/v1/names/a.eth?source=auto")
        ));
        assert!(!is_cacheable_indexed_read(
            &Method::GET,
            &uri("/v1/names/a.eth?source=indexed&source=auto")
        ));
        assert!(!is_cacheable_indexed_read(
            &Method::GET,
            &uri("/v1/addresses/0xabc/primary-name")
        ));
        assert!(is_cacheable_indexed_read(
            &Method::GET,
            &uri("/v1/addresses/0xabc/primary-name?source=indexed")
        ));
    }
}
