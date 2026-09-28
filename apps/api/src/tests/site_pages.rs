//! The landing page and the API reference live in the static site under `site/`, outside the
//! API binary. These checks hold the site's pages and the router together in the API crate's
//! own suite, so a route added here without a manual page, or a page link to a reference page
//! that does not exist, fails CI next to the change that caused it.

use std::collections::BTreeSet;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use sqlx::PgPool;
use tower::ServiceExt;

use crate::{AppState, app_router};

const HOME: &str = include_str!("../../../../site/index.html");
const GUIDE: &str = include_str!("../../../../site/docs/index.html");

// A route the router serves but the guide omits is undiscoverable to a
// reader of the reference, so the two are held together here.
#[test]
fn every_served_v1_route_is_documented_in_the_guide() {
    let router_source = include_str!("../v2/router.rs");
    let documented = GUIDE
        .split("path: '")
        .skip(1)
        .filter_map(|rest| rest.split('\'').next())
        .collect::<BTreeSet<_>>();
    let undocumented = router_source
        .split(".route(")
        .skip(1)
        .filter_map(|rest| rest.trim_start().strip_prefix('"'))
        .filter_map(|rest| rest.split('"').next())
        .filter(|path| path.starts_with("/v1/") && !documented.contains(path))
        .collect::<Vec<_>>();
    assert!(
        undocumented.is_empty(),
        "routes served but missing from site/docs/index.html ENDPOINTS: {undocumented:?}"
    );
}

// The guide restates upstream protocol behavior in places; each such
// claim carries the same pinned citation one of the checked-in contract
// docs carries, so a citation no doc verified cannot appear only here.
#[test]
fn every_upstream_citation_in_the_guide_is_in_the_route_contract() {
    let contract = concat!(
        include_str!("../../../../docs/api-v1-routes.md"),
        include_str!("../../../../docs/api-v1.md"),
        include_str!("../../../../docs/consumer-capabilities.md"),
        include_str!("../../../../docs/projections.md"),
        include_str!("../../../../docs/architecture.md")
    );
    let citations = GUIDE
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
        "citations in site/docs/index.html absent from the contract docs (api-v1-routes, api-v1, consumer-capabilities, projections, architecture): {unverified:?}"
    );
}

// The landing page deep-links into the reference by page id; a renamed
// or removed page would leave the link landing on the overview.
#[test]
fn every_docs_link_on_the_home_page_names_a_reference_page() {
    let linked = HOME
        .split("docs/#")
        .skip(1)
        .filter_map(|rest| {
            rest.split(|c: char| !(c.is_ascii_lowercase() || c == '-'))
                .next()
        })
        .collect::<BTreeSet<_>>();
    assert!(!linked.is_empty(), "the home page links into the reference");
    let missing = linked
        .iter()
        .filter(|id| !GUIDE.contains(&format!("id: '{id}'")))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "site/index.html links to reference pages absent from site/docs/index.html: {missing:?}"
    );
}

// The landing page is self-contained: opening it must not reach any other
// origin for its own resources (no web fonts, preconnects, external
// stylesheets, or scripts).
#[test]
fn home_page_loads_no_third_party_resources() {
    for banned in [
        "fonts.googleapis.com",
        "fonts.gstatic.com",
        "rel=\"preconnect\"",
        "rel=\"stylesheet\"",
        "@import",
        "src=\"http",
        "src=\"//",
        "url(http",
    ] {
        assert!(
            !HOME.contains(banned),
            "site/index.html must not load {banned}"
        );
    }
    for link in HOME.split("<link").skip(1) {
        let tag = link.split('>').next().unwrap_or_default();
        assert!(
            tag.contains("href=\"data:"),
            "site/index.html <link> must be inline: <link{tag}>"
        );
    }
}

// Grep-level guards for the try-it line: one request at a time, only the
// newest request writes the output, and the curl command is shell-quoted
// with a visible fallback when the clipboard is unavailable.
#[test]
fn try_it_line_is_single_flight_and_curl_is_safe() {
    assert!(HOME.contains("function setTryBusy(on)"));
    assert!(HOME.contains("if (tryBusy) return;"));
    assert!(HOME.contains("querySelectorAll('input, .run, [data-view]')"));
    assert!(
        HOME.matches("if (gen !== tryGen) return;").count() >= 2,
        "both the success and the error path must drop stale responses"
    );
    assert!(HOME.contains("finally { if (gen === tryGen) setTryBusy(false);"));
    // A network change invalidates and cancels the request in flight, and the
    // request and its failure message use the API chosen at submission.
    assert!(HOME.contains("tryGen++; netGen++;"));
    assert!(HOME.contains("if (tryAbort) { tryAbort.abort(); tryAbort = null; }"));
    assert!(HOME.contains("await fetch(base + u,"));
    assert!(HOME.contains("Nothing answered at ${esc(base)}"));

    assert!(HOME.contains("const curlCmd = () => `curl -s ${shq("));
    assert!(
        !HOME.contains("curl -s '${"),
        "curl URL must go through shq"
    );
    assert!(!HOME.contains("navigator.clipboard.writeText"));
    assert!(HOME.contains("typeof clip.writeText !== 'function'"));
    assert!(HOME.contains("clip.writeText(cmd).then(copied, fallback)"));
}

// The pages moved out of the binary. `/`, `/docs`, `/docs/` and (until the
// OpenAPI document ships) `/openapi.json` answer exactly like any unknown route.
#[tokio::test]
async fn former_page_routes_answer_like_an_unknown_route() {
    let app = app_router(AppState::new(
        PgPool::connect_lazy_with(
            "postgres://bigname:bigname@127.0.0.1:5432/bigname"
                .parse()
                .expect("static test database URL must parse"),
        ),
        bigname_lookup::ChainRpcUrls::default(),
    ));
    let request = |method: &str, uri: &str| {
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .expect("request must build")
    };
    for method in ["GET", "HEAD"] {
        let unknown = app
            .clone()
            .oneshot(request(method, "/no-such-route"))
            .await
            .expect("unknown request must complete");
        let unknown_status = unknown.status();
        let unknown_body = to_bytes(unknown.into_body(), usize::MAX)
            .await
            .expect("body must read");
        assert_eq!(unknown_status, StatusCode::NOT_FOUND, "{method} unknown");
        for path in ["/", "/docs", "/docs/", "/openapi.json"] {
            let removed = app
                .clone()
                .oneshot(request(method, path))
                .await
                .expect("request must complete");
            assert_eq!(removed.status(), unknown_status, "{method} {path}");
            let body = to_bytes(removed.into_body(), usize::MAX)
                .await
                .expect("body must read");
            assert_eq!(body, unknown_body, "{method} {path} body");
        }
    }
}
