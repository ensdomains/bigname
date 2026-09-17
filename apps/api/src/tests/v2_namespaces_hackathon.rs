/// The separately evidenced `sepolia-hackathon` deployment profile declares its own ENS
/// execution entrypoint (the hackathon deployment's Universal Resolver, shadow rollout) and an
/// active ENSv1 registry on `ethereum-sepolia`, so both verified capabilities stop reporting
/// `execution_entrypoint_not_declared` and turn on the moment a Sepolia provider is configured.
#[tokio::test]
async fn v2_namespace_ens_reports_verified_capabilities_under_the_hackathon_profile() -> Result<()>
{
    let database = TestDatabase::new(true).await?;
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia-hackathon");
    let repository = bigname_manifests::load_repository(manifest_root)?;
    bigname_manifests::sync_schema_v2_repository(&database.lookup_pool, &repository).await?;

    let response = app_router(database.app_state())
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("namespace request must build"),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    assert_eq!(
        payload["data"]["networks"],
        json!([{ "network": "ethereum-sepolia", "chain_id": 11155111 }])
    );
    for capability in ["verified_records", "verified_primary_name"] {
        assert_eq!(
            payload["data"]["capabilities"][capability],
            json!({
                "completeness": "unsupported",
                "unsupported_reason": "execution_provider_not_configured",
                "chains": {
                    "11155111": {
                        "completeness": "unsupported",
                        "unsupported_reason": "execution_provider_not_configured"
                    }
                }
            }),
            "{capability}: {payload}"
        );
    }

    let configured = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            "ethereum-sepolia=http://rpc.test".to_owned(),
        ])?)
        .await?;
    let response = app_router(configured)
        .oneshot(
            Request::builder()
                .uri("/v1/namespaces/ens")
                .body(Body::empty())
                .expect("namespace request must build"),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    for capability in ["verified_records", "verified_primary_name"] {
        assert_eq!(
            payload["data"]["capabilities"][capability],
            json!({
                "completeness": "full",
                "chains": { "11155111": { "completeness": "full" } }
            }),
            "{capability}: {payload}"
        );
    }

    database.cleanup().await
}
