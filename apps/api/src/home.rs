use std::sync::LazyLock;

use axum::response::{Html, IntoResponse};

use crate::{BUILD_SHA, SOFTWARE_VERSION};

const HOME_TEMPLATE: &str = include_str!("home.html");

static HOME_HTML: LazyLock<String> = LazyLock::new(|| {
    HOME_TEMPLATE
        .replace("__BIGNAME_VERSION__", SOFTWARE_VERSION)
        .replace("__BIGNAME_BUILD_SHA__", BUILD_SHA)
});

/// Serve the static landing page. Like the API reference it is
/// self-contained and calls the same origin for its status pill and its
/// try-it line.
pub(crate) async fn home() -> impl IntoResponse {
    Html(HOME_HTML.as_str())
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

    // The landing page deep-links into the reference by page id; a renamed
    // or removed page would leave the link landing on the overview.
    #[test]
    fn every_docs_link_on_the_home_page_names_a_reference_page() {
        let home = include_str!("home.html");
        let guide = include_str!("docs.html");
        let linked = home
            .split("/docs#")
            .skip(1)
            .filter_map(|rest| {
                rest.split(|c: char| !(c.is_ascii_lowercase() || c == '-'))
                    .next()
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert!(!linked.is_empty(), "the home page links into the reference");
        let missing = linked
            .iter()
            .filter(|id| !guide.contains(&format!("id: '{id}'")))
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "apps/api/src/home.html links to reference pages absent from docs.html: {missing:?}"
        );
    }

    #[tokio::test]
    async fn root_serves_the_home_page_with_the_build_version() {
        let response = router()
            .oneshot(
                Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .expect("request must build"),
            )
            .await
            .expect("home request must complete");

        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        assert!(content_type.starts_with("text/html"), "{content_type}");

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body must read");
        let html = std::str::from_utf8(&body).expect("home page must be UTF-8");
        assert!(html.contains("<title>bigname</title>"));
        assert!(html.contains(&format!("const VERSION = '{}'", crate::SOFTWARE_VERSION)));
        assert!(!html.contains("__BIGNAME_VERSION__"));
        assert!(!html.contains("__BIGNAME_BUILD_SHA__"));
        assert!(html.contains("href=\"/docs\""));
    }
}
