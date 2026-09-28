/// GraphQL was removed (TYR-19). `/graphql` must answer exactly like any other unknown route, for
/// every method a former client could send, while the REST status and health routes still serve.
#[tokio::test]
async fn removed_graphql_route_answers_like_an_unknown_route() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let app = app_router(database.app_state());

    let request = |method: &str, uri: &str| {
        let builder = Request::builder().method(method).uri(uri);
        match method {
            "POST" => builder
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query":"{ __typename }"}"#)),
            "OPTIONS" => builder
                .header("origin", "https://app.ens.dev")
                .header("access-control-request-method", "POST")
                .header("access-control-request-headers", "content-type")
                .body(Body::empty()),
            _ => builder.body(Body::empty()),
        }
        .expect("request must build")
    };
    for method in ["GET", "POST", "OPTIONS"] {
        let unknown = app
            .clone()
            .oneshot(request(method, "/no-such-route"))
            .await?;
        let removed = app.clone().oneshot(request(method, "/graphql")).await?;
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND, "{method} unknown");
        assert_eq!(removed.status(), unknown.status(), "{method} /graphql");
        let unknown_body = to_bytes(unknown.into_body(), usize::MAX).await?;
        let removed_body = to_bytes(removed.into_body(), usize::MAX).await?;
        assert_eq!(removed_body, unknown_body, "{method} /graphql body");
    }

    let status = app
        .clone()
        .oneshot(Request::builder().uri("/v1/status").body(Body::empty())?)
        .await?;
    assert_eq!(status.status(), StatusCode::OK);
    seed_expected_phase_chains(&database, &["1"]).await?;
    seed_phase_runner_heartbeat(&database, "1", "now()").await?;
    assert_eq!(healthz_payload(&database).await?["status"], json!("ready"));

    database.cleanup().await
}
