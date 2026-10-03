/// Official Sepolia execution declarations report provider readiness independently
/// of indexed capability completeness.
#[tokio::test]
async fn v2_namespace_ens_reports_verified_capabilities_under_the_official_sepolia_profile() -> Result<()>
{
    let database = TestDatabase::new(true).await?;
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
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
        json!([{
            "network": "ethereum-sepolia",
            "chain_id": 11155111,
            "resolution": { "protocol": "ens_v1", "since_block": null }
        }])
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

/// The Sepolia network's `resolution` follows the client-facing Universal Resolver proxy chain
/// that Project derives from `Upgraded` events under the checked-in manifests, including a
/// rollback to an unlisted implementation and the return to the listed one.
#[tokio::test]
async fn v2_namespace_ens_reports_the_resolution_protocol_across_sepolia_upgrades() -> Result<()> {
    const CHAIN: &str = "ethereum-sepolia";
    const TOP: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
    const MANAGED: &str = "0x6d80f2172cfdec5730fe683860c33d26fc42e6f1";
    const ADMITTED: &str = "0x24e1d8e068620b647ca097f961a61055f4f42d72";
    const UNLISTED: &str = "0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3";
    let database = TestDatabase::new(true).await?;
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    let repository = bigname_manifests::load_repository(manifest_root)?;
    bigname_manifests::sync_schema_v2_repository(&database.lookup_pool, &repository).await?;

    let network = |resolution: Value| {
        json!([{ "network": "ethereum-sepolia", "chain_id": 11155111, "resolution": resolution }])
    };
    let (status, payload) = read_family_response(&database, "/v1/namespaces/ens").await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    assert_eq!(
        payload["data"]["networks"],
        network(json!({ "protocol": "ens_v1", "since_block": null })),
        "no observed upgrade"
    );

    for (block, proxy, implementation, expected) in [
        (11821679, TOP, MANAGED, json!({ "protocol": "ens_v1", "since_block": 11821679 })),
        (11821680, MANAGED, ADMITTED, json!({ "protocol": "ens_v2", "since_block": 11821680 })),
        (11821681, MANAGED, UNLISTED, json!({ "protocol": "ens_v1", "since_block": 11821681 })),
        (11821682, MANAGED, ADMITTED, json!({ "protocol": "ens_v2", "since_block": 11821682 })),
    ] {
        let hash = format!("0xresolution{block}");
        seed_schema_v2_lookup_head(&database.pool, CHAIN, block, &hash, "2026-10-01T00:00:00Z")
            .await?;
        let mut upgrade = history_event(
            &format!("resolution-upgraded-{block}"),
            None,
            None,
            Some(CHAIN),
            Some(block),
            Some(&hash),
            Some(&format!("0xupgrade{block}")),
            Some(0),
            CanonicalityState::Canonical,
        );
        upgrade.event_kind = "Upgraded".into();
        upgrade.source_family = "ens_execution".into();
        upgrade.before_state = json!({});
        upgrade.after_state = json!({"proxy_address": proxy, "implementation": implementation});
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[upgrade]).await?;
        publish_test_families_on(&database.pool, CHAIN, block).await?;

        let (status, payload) = read_family_response(&database, "/v1/namespaces/ens").await?;
        assert_eq!(status, StatusCode::OK, "{payload:#}");
        assert_eq!(
            payload["data"]["networks"],
            network(expected),
            "after the upgrade at {block}"
        );
        assert!(payload["meta"].get("as_of").is_none(), "{payload:#}");
    }

    database.cleanup().await
}

/// Mainnet ENS has an execution entrypoint but no proxy upgrade, so it reads ENSv1 with no start;
/// Basenames networks have no ENSv1/ENSv2 split and carry no `resolution`.
#[tokio::test]
async fn v2_namespace_resolution_is_ens_v1_on_mainnet_and_absent_for_basenames() -> Result<()> {
    let database = TestDatabase::new(true).await?;
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let repository = bigname_manifests::load_repository(manifest_root)?;
    bigname_manifests::sync_schema_v2_repository(&database.lookup_pool, &repository).await?;

    let (status, ens) = read_family_response(&database, "/v1/namespaces/ens").await?;
    assert_eq!(status, StatusCode::OK, "{ens:#}");
    assert_eq!(
        ens["data"]["networks"],
        json!([{
            "network": "ethereum",
            "chain_id": 1,
            "resolution": { "protocol": "ens_v1", "since_block": null }
        }])
    );
    let (status, basenames) = read_family_response(&database, "/v1/namespaces/basenames").await?;
    assert_eq!(status, StatusCode::OK, "{basenames:#}");
    let networks = basenames["data"]["networks"].as_array().expect("networks");
    assert!(!networks.is_empty(), "{basenames:#}");
    assert!(
        networks.iter().all(|network| network.get("resolution").is_none()),
        "{basenames:#}"
    );

    database.cleanup().await
}
