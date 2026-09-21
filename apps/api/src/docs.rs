use std::sync::LazyLock;

use axum::response::{Html, IntoResponse};

use crate::{BUILD_SHA, SOFTWARE_VERSION};

const DOCS_TEMPLATE: &str = include_str!("docs.html");

static DOCS_HTML: LazyLock<String> = LazyLock::new(|| {
    DOCS_TEMPLATE
        .replace("__BIGNAME_VERSION__", SOFTWARE_VERSION)
        .replace("__BIGNAME_BUILD_SHA__", BUILD_SHA)
});

/// Serve the static API reference. The page is self-contained and calls the
/// same origin for its live status pill and "try it" panels, so it needs no
/// generated OpenAPI artifact.
pub(crate) async fn docs() -> impl IntoResponse {
    Html(DOCS_HTML.as_str())
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use sqlx::PgPool;
    use tower::ServiceExt;

    use crate::{AppState, app_router};

    fn router() -> axum::Router {
        app_router(AppState::new(
            PgPool::connect_lazy_with(
                "postgres://bigname:bigname@127.0.0.1:5432/bigname"
                    .parse()
                    .expect("static test database URL must parse"),
            ),
            bigname_lookup::ChainRpcUrls::default(),
        ))
    }

    // A route the router serves but the guide omits is undiscoverable to a
    // reader of the bundled reference, so the two are held together here.
    #[test]
    fn every_served_v1_route_is_documented_in_the_guide() {
        let router_source = include_str!("v2/router.rs");
        let guide = include_str!("docs.html");
        let documented = guide
            .split("path: '")
            .skip(1)
            .filter_map(|rest| rest.split('\'').next())
            .collect::<std::collections::BTreeSet<_>>();
        let undocumented = router_source
            .split(".route(")
            .skip(1)
            .filter_map(|rest| rest.trim_start().strip_prefix('"'))
            .filter_map(|rest| rest.split('"').next())
            .filter(|path| path.starts_with("/v1/") && !documented.contains(path))
            .collect::<Vec<_>>();
        assert!(
            undocumented.is_empty(),
            "routes served but missing from apps/api/src/docs.html ENDPOINTS: {undocumented:?}"
        );
    }

    // The guide restates upstream protocol behavior in places; each such
    // claim carries the same pinned citation one of the checked-in contract
    // docs carries, so a citation no doc verified cannot appear only here.
    #[test]
    fn every_upstream_citation_in_the_guide_is_in_the_route_contract() {
        let guide = include_str!("docs.html");
        let contract = concat!(
            include_str!("../../../docs/api-v2-routes.md"),
            include_str!("../../../docs/api-v2.md"),
            include_str!("../../../docs/consumer-capabilities.md"),
            include_str!("../../../docs/projections.md"),
            include_str!("../../../docs/architecture.md")
        );
        let citations = guide
            .split("(upstream: ")
            .skip(1)
            .filter_map(|rest| rest.split(')').next())
            .collect::<Vec<_>>();
        assert!(
            !citations.is_empty(),
            "the guide's resolver-links note carries its upstream citations"
        );
        let unverified = citations
            .iter()
            .filter(|citation| !contract.contains(&format!("(upstream: {citation})")))
            .collect::<Vec<_>>();
        assert!(
            unverified.is_empty(),
            "citations in apps/api/src/docs.html absent from the contract docs (api-v2-routes, api-v2, consumer-capabilities, projections, architecture): {unverified:?}"
        );
    }

    #[tokio::test]
    async fn docs_route_serves_the_reference_page_with_the_build_version() {
        for path in ["/docs", "/docs/"] {
            let response = router()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .expect("docs request must complete");

            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let content_type = response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            assert!(
                content_type.starts_with("text/html"),
                "{path}: {content_type}"
            );

            let body = to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body must read");
            let html = std::str::from_utf8(&body).expect("docs page must be UTF-8");
            assert!(html.contains("<title>bigname API docs</title>"));
            assert!(html.contains(&format!("const VERSION = '{}'", crate::SOFTWARE_VERSION)));
            assert!(!html.contains("__BIGNAME_VERSION__"));
            assert!(!html.contains("__BIGNAME_BUILD_SHA__"));
            assert!(html.contains("/v1/lookup"));
        }
    }
}
