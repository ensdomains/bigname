//! Route-local extraction and cursor contract tests. Family-published paging is covered
//! by the public-router tests in tests/v2_names_windows.rs.

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    routing::get,
};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::*;

async fn probe(input: NamesQuery) -> V2Result<Json<Value>> {
    let params = input.params;
    let parent = params.parent.as_deref().map(normalize_parent).transpose()?;
    let binding = NamesCursorBinding {
        namespace: params.namespace.as_deref().unwrap_or("ens"),
        expires_after: params.expires_after,
        expires_before: params.expires_before,
        windows: input.windows.as_ref(),
        authority: params.authority.as_ref(),
        parent: parent.as_deref(),
        order: params.order.unwrap_or(SortOrder::Asc),
    };
    let cursor = names_list_cursor(&binding);
    cursor.read(params.cursor.as_deref(), &POSITION_KEYS)?;
    let position = ListPosition::new([
        (EXPIRES_AT_CURSOR_KEY, "1".to_owned()),
        (NAMESPACE_FILTER_KEY, "ens".to_owned()),
        (NAME_CURSOR_KEY, "a.eth".to_owned()),
        (NAMEHASH_CURSOR_KEY, "0xa".to_owned()),
    ]);
    Ok(Json(json!({
        "windows": input.windows.as_ref().map(ExpiryWindows::canonical),
        "page_size": params.page_size,
        "next_cursor": cursor.next(position),
        "filters": cursor_filters(&binding),
    })))
}

async fn request(app: Router, query: &str) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/names?{query}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn probe_request(query: &str) -> (StatusCode, Value) {
    request(Router::new().route("/v1/names", get(probe)), query).await
}

fn windows_query(values: &[&str]) -> String {
    let mut query = form_urlencoded::Serializer::new(String::new());
    for value in values {
        query.append_pair(WINDOW_KEY, value);
    }
    query.finish()
}

#[tokio::test]
async fn windows_http_preserves_order_precision_and_encoded_offsets() {
    let input = [
        "9223372036854775808.000000001..18446744073709551614",
        " 2026-01-01T01:00:00+01:00 .. 2026-01-01T00:00:00.000000001Z ",
        "9007199254740993..9007199254740994",
        "-0.000000002..-0.000000001",
    ];
    let (status, body) = probe_request(&windows_query(&input)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["windows"],
        "9223372036854775808.000000001..18446744073709551614,1767225600..1767225600.000000001,9007199254740993..9007199254740994,-0.000000002..-0.000000001"
    );
    assert_eq!(body["page_size"], 50);
}

#[tokio::test]
async fn windows_http_rejects_invalid_ranges_and_combinations() {
    for value in [
        "",
        "1",
        "..2",
        "1..",
        " ..2",
        "1..2..3",
        "1...2",
        "1..1",
        "2..1",
        "NaN..2",
        "1..Infinity",
        "1e2..300",
        "0.0000000001..1",
        "999999999999999999999999999999999999999..1",
        "1..2,3..4",
    ] {
        let (status, body) = probe_request(&windows_query(&[value])).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}: {body}");
        assert_eq!(body["error"]["code"], "invalid_input");
    }
    for values in [
        ["1..3", "2..4"],
        ["1..5", "2..3"],
        ["3..5", "2..4"],
        ["1..2", "01.0..2.000"],
    ] {
        assert_eq!(
            probe_request(&windows_query(&values)).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    for scalar in [
        "expires_after=",
        "expires_before=",
        "expires_after=0",
        "expires_before=3",
    ] {
        assert_eq!(
            probe_request(&format!("expires_window=1..2&{scalar}"))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    for key in NamesQueryParams::ALLOWED
        .iter()
        .filter(|key| **key != WINDOW_KEY)
    {
        let query = format!("expires_window=1..2&{key}=ens&{key}=ens");
        let (status, body) = probe_request(&query).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{key}");
        assert_eq!(
            body["error"]["message"],
            format!("query parameter must not repeat: {key}")
        );
    }
    for suffix in ["unknown=x", "q=a", "page_size=0", "page_size=201"] {
        assert_eq!(
            probe_request(&format!("expires_window=1..2&{suffix}"))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
}

#[test]
fn windows_membership_is_exact_half_open_and_in_request_order() {
    let windows = ExpiryWindows::parse(&["2..3", "-1..1.000000001", "1.000000001..2"])
        .unwrap()
        .unwrap();
    for (expiry, index) in [
        ("-1", Some(1)),
        ("1", Some(1)),
        ("1.000000001", Some(2)),
        ("1.999999999", Some(2)),
        ("2", Some(0)),
        ("3", None),
    ] {
        assert_eq!(
            windows.index_of(Some(expiry.parse().unwrap())),
            index,
            "{expiry}"
        );
    }
    assert_eq!(windows.index_of(None), None);
}

#[tokio::test]
async fn windows_http_maximum_count_and_cursor_continuation() {
    let values: Vec<_> = (0..32).rev().map(|i| format!("{i}..{}", i + 1)).collect();
    let query = windows_query(&values.iter().map(String::as_str).collect::<Vec<_>>());
    let (status, body) = probe_request(&format!("{query}&page_size=1")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let cursor = body["next_cursor"].as_str().unwrap();
    let (status, body) = probe_request(&format!("{query}&page_size=200&cursor={cursor}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["page_size"], 200);
    assert_eq!(
        probe_request(&format!("{query}&expires_window=32..33"))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn windows_cursor_binds_ordered_normalized_windows_and_other_filters() {
    let suffix = "namespace=ens&authority=ens_v1,ens_v2&parent=ETH&order=desc";
    let query = format!("expires_window=1767225600..1767312000&expires_window=0..1&{suffix}");
    let (status, body) = probe_request(&query).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let cursor = body["next_cursor"].as_str().unwrap();
    let equivalent = windows_query(&[
        "2026-01-01T01:00:00+01:00..2026-01-02T00:00:00Z",
        "00.000..1.0",
    ]);
    assert_eq!(probe_request(&format!("{equivalent}&namespace=ens&authority=ens_v2,ens_v1&parent=eth&order=desc&page_size=1&cursor={cursor}")).await.0, StatusCode::OK);
    for other in [
        query.replace(
            "1767225600..1767312000&expires_window=0..1",
            "0..1&expires_window=1767225600..1767312000",
        ),
        query.replace("&expires_window=0..1", ""),
        query.replace("0..1", "0..2"),
        query.replace("namespace=ens", "namespace=basenames"),
        query.replace("authority=ens_v1,ens_v2", "authority=ens_v1"),
        query.replace("parent=ETH", "parent=base.eth"),
        query.replace("order=desc", "order=asc"),
    ] {
        assert_eq!(
            probe_request(&format!("{other}&cursor={cursor}")).await.0,
            StatusCode::BAD_REQUEST,
            "{other}"
        );
    }
    let (_, scalar) = probe_request("expires_after=0&expires_before=1").await;
    let (_, window) = probe_request("expires_window=0..1").await;
    for (query, cursor) in [
        ("expires_window=0..1", &scalar["next_cursor"]),
        ("expires_after=0&expires_before=1", &window["next_cursor"]),
    ] {
        assert_eq!(
            probe_request(&format!("{query}&cursor={}", cursor.as_str().unwrap()))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
}

#[test]
fn windows_dto_field_is_omitted_until_assigned() {
    let row = json!({"name":"a.eth", "display_name":"a.eth", "namespace":"ens", "namehash":"0xa", "registration_status":"registered"});
    let mut row: SearchName = serde_json::from_value(row).unwrap();
    assert!(
        serde_json::to_value(&row)
            .unwrap()
            .get("expires_window_index")
            .is_none()
    );
    row.expires_window_index = Some(0);
    assert_eq!(
        serde_json::to_value(&row).unwrap()["expires_window_index"],
        0
    );
}

#[tokio::test]
async fn windows_real_route_rejects_invalid_requests_before_reading() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    let state = crate::AppState::new_with_rpc_urls(pool, bigname_lookup::ChainRpcUrls::default());
    let app = crate::app_router(state);
    for query in [
        "namespace=ens&expires_window=0..1&expires_after=",
        "namespace=ens&expires_window=0..2&expires_window=1..3",
        "namespace=ens&expires_window=0..1&page_size=201",
        "expires_window=0..1",
        "namespace=ens&expires_window=0..1&at=1",
        "namespace=ens&expires_window=0..1&finality=safe",
    ] {
        let (status, body) = request(app.clone(), query).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        assert_eq!(body["error"]["code"], "invalid_input");
    }
}

#[tokio::test]
async fn windows_extractor_preserves_shared_scalar_decoding() {
    use axum::extract::FromRequestParts;
    for query in [
        "namespace=ens&expires_after=-1.000000001&expires_before=9223372036854775808&page_size=200&authority=ens_v2,ens_v1&parent=ETH&sort=expires_at&order=desc",
        "namespace=ens&expires_before=2026-01-01T01%3A00%3A00%2B01%3A00&cursor=opaque%2Bvalue%26encoded%3Dyes",
        "namespace=ens&expires_after=&expires_before=1&page_size=1",
        "namespace=ens&expires_after=1",
    ] {
        let parts = || {
            Request::builder()
                .uri(format!("/v1/names?{query}"))
                .body(Body::empty())
                .unwrap()
                .into_parts()
                .0
        };
        let old =
            crate::v2::StrictQueryParams::<NamesQueryParams>::from_request_parts(&mut parts(), &())
                .await
                .unwrap()
                .into_inner();
        let new = NamesQuery::from_request_parts(&mut parts(), &())
            .await
            .unwrap();
        assert_eq!(new.params, old, "{query}");
        assert!(new.windows.is_none());
    }
}
