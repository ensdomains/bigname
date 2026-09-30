use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use serde_json::Value;
use sqlx::PgPool;
use tower::ServiceExt;

use super::*;

#[test]
fn openapi_documentation_links_follow_the_served_commit_with_unknown_build_fallback() {
    const COMMIT: &str = "8a959700bf476a7b694ccf85568de42b415b391e";
    const MAIN: &str = "https://github.com/ensdomains/bigname/blob/main/";
    let pinned = format!("https://github.com/ensdomains/bigname/blob/{COMMIT}/");
    let rendered = render_document("1.2.3", COMMIT);
    let document: Value = serde_json::from_str(&rendered.body).unwrap();
    assert_eq!(document["info"]["x-build-sha"], COMMIT);
    assert!(
        document["paths"]["/v1/lookup"]["post"]["description"]
            .as_str()
            .unwrap()
            .contains(&pinned)
    );
    assert!(rendered.body.contains(&pinned));
    assert!(!rendered.body.contains(MAIN));
    assert_ne!(rendered.etag, render_document("1.2.3", "unknown").etag);

    for label in ["unknown", "", "dev", "8a959700b", "custom\"build\\label\n"] {
        let rendered = render_document("version\"with\\escaping\n", label);
        let document: Value = serde_json::from_str(&rendered.body).unwrap();
        assert_eq!(document["info"]["x-build-sha"], label);
        assert_eq!(document["info"]["version"], "version\"with\\escaping\n");
        assert!(
            document["paths"]["/v1/lookup"]["post"]["description"]
                .as_str()
                .unwrap()
                .contains(MAIN)
        );
    }

    let external = "https://docs.example.test/api.md";
    let mut annotations = serde_json::json!({
        "description": format!("[docs]({MAIN}docs/api-v1.md) [custom]({external}) [pinned]({pinned}docs/api-v1.md)"),
        "properties": {"value": {"description": format!("[field]({MAIN}docs/api-v1.md#field)")}},
        "x-enum-descriptions": {"ok": format!("[enum]({MAIN}docs/api-v1.md#enum)")},
        "example": {"description": format!("{MAIN}literal-wire-value")},
        "examples": [{"url": format!("{MAIN}literal-wire-value")}]
    });
    let examples = (
        annotations["example"].clone(),
        annotations["examples"].clone(),
    );
    pin_documentation_links(&mut annotations, &pinned);
    for description in [
        &annotations["description"],
        &annotations["properties"]["value"]["description"],
        &annotations["x-enum-descriptions"]["ok"],
    ] {
        assert!(description.as_str().unwrap().contains(&pinned));
        assert!(!description.as_str().unwrap().contains(MAIN));
    }
    assert!(
        annotations["description"]
            .as_str()
            .unwrap()
            .contains(external)
    );
    assert_eq!(
        (
            annotations["example"].clone(),
            annotations["examples"].clone()
        ),
        examples
    );
}

fn app() -> axum::Router {
    crate::app_router(crate::AppState::new(
        PgPool::connect_lazy("postgres://unused:unused@127.0.0.1:1/unused").unwrap(),
        bigname_lookup::ChainRpcUrls::default(),
    ))
}

async fn send(method: &str, tags: &[&str]) -> Response {
    let mut request = Request::builder()
        .method(method)
        .uri("/openapi.json")
        .header(header::ORIGIN, "https://docs.example");
    for tag in tags {
        request = request.header(header::IF_NONE_MATCH, *tag);
    }
    app()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn openapi_get_and_head_are_stable_json_without_database_or_provider() {
    let response = send("GET", &[]).await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    assert_eq!(headers[header::CACHE_CONTROL], CACHE_CONTROL);
    assert_eq!(headers[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let document: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(document["openapi"], "3.1.0");
    assert_eq!(document["info"]["version"], crate::SOFTWARE_VERSION);
    assert_eq!(document["info"]["x-build-sha"], crate::BUILD_SHA);
    let docs_ref = if crate::BUILD_SHA.len() == 40
        && crate::BUILD_SHA
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        crate::BUILD_SHA
    } else {
        "main"
    };
    let docs_base = format!("https://github.com/ensdomains/bigname/blob/{docs_ref}/docs/");
    assert!(
        document["paths"]["/v1/lookup"]["post"]["description"]
            .as_str()
            .unwrap()
            .contains(&docs_base)
    );
    assert!(!String::from_utf8_lossy(&body).contains("__BIGNAME_"));
    assert_eq!(
        headers[header::ETAG],
        format!("W/\"{}\"", alloy_primitives::keccak256(&body))
    );
    let repeated = send("GET", &[]).await;
    assert_eq!(repeated.headers()[header::ETAG], headers[header::ETAG]);
    assert_eq!(
        to_bytes(repeated.into_body(), usize::MAX).await.unwrap(),
        body
    );
    let head = send("HEAD", &[]).await;
    assert_eq!(head.status(), StatusCode::OK);
    for name in [header::CONTENT_TYPE, header::CACHE_CONTROL, header::ETAG] {
        assert_eq!(head.headers()[&name], headers[&name]);
    }
    assert_eq!(
        head.headers()[header::CONTENT_LENGTH],
        body.len().to_string()
    );
    assert!(
        to_bytes(head.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn openapi_conditional_reads_compare_the_rendered_representation() {
    let etag = send("GET", &[]).await.headers()[header::ETAG]
        .to_str()
        .unwrap()
        .to_owned();
    let strong = etag.strip_prefix("W/").unwrap();
    let list = format!("\"another\", {etag}");
    for method in ["GET", "HEAD"] {
        for tags in [
            vec![etag.as_str()],
            vec![strong],
            vec!["*"],
            vec![&list],
            vec!["\"another\"", &etag],
        ] {
            let response = send(method, &tags).await;
            assert_eq!(
                response.status(),
                StatusCode::NOT_MODIFIED,
                "{method} {tags:?}"
            );
            assert_eq!(response.headers()[header::ETAG], etag);
            assert_eq!(response.headers()[header::CACHE_CONTROL], CACHE_CONTROL);
            assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
            assert!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        for tag in ["W/\"stale\"", "not-an-etag", ""] {
            assert_eq!(send(method, &[tag]).await.status(), StatusCode::OK);
        }
    }
    for method in ["POST", "PUT", "DELETE", "PATCH"] {
        assert_eq!(
            send(method, &[]).await.status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
    }
    for path in ["/openapi.json/", "/openapi", "/docs", "/v2/status"] {
        assert_eq!(
            app()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND,
            "{path}"
        );
    }
}
