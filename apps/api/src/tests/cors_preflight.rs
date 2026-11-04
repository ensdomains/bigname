/// A preflight answer carries `Access-Control-Max-Age` so a browser can reuse it. Responses to
/// other requests carry the wildcard origin and no max age.
#[tokio::test]
async fn cors_preflight_answers_carry_a_one_day_max_age() -> Result<()> {
    const ORIGIN: &str = "https://app.ens.dev";
    let database = TestDatabase::new_migrated().await?;
    let app = app_router(database.app_state());

    for (uri, method) in [
        ("/v1/lookup", "POST"),
        ("/v1/names/alice.eth", "GET"),
        ("/openapi.json", "GET"),
        ("/healthz", "GET"),
    ] {
        let preflight = Request::builder()
            .method("OPTIONS")
            .uri(uri)
            .header("origin", ORIGIN)
            .header("access-control-request-method", method)
            .header("access-control-request-headers", "content-type")
            .body(Body::empty())?;
        let response = app.clone().oneshot(preflight).await?;
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let headers = response.headers();
        assert_eq!(headers["access-control-max-age"], "86400", "{uri}");
        assert_eq!(headers["access-control-allow-origin"], "*", "{uri}");
        assert_eq!(headers["access-control-allow-methods"], "*", "{uri}");
        assert_eq!(headers["access-control-allow-headers"], "*", "{uri}");
    }

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/status")
                .header("origin", ORIGIN)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    assert!(response.headers().get("access-control-max-age").is_none());

    database.cleanup().await
}
