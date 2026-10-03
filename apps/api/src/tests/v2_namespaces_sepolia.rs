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
        json!([{ "network": "ethereum-sepolia", "chain_id": 11155111 }]),
        "no family publication, so no resolution"
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

const RESOLUTION_TOP: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
const RESOLUTION_MANAGED: &str = "0x6d80f2172cfdec5730fe683860c33d26fc42e6f1";
const RESOLUTION_ADMITTED: &str = "0x24e1d8e068620b647ca097f961a61055f4f42d72";
const RESOLUTION_UNLISTED: &str = "0x5d25c1d6acbb71b7a28aa7899618a3412a8303e3";

async fn sync_checked_in_manifests(database: &TestDatabase, profile: &str) -> Result<()> {
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests")
        .join(profile);
    let repository = bigname_manifests::load_repository(manifest_root)?;
    bigname_manifests::sync_schema_v2_repository(&database.lookup_pool, &repository).await?;
    Ok(())
}

/// Publishes the families at `block`, with a Universal Resolver proxy `Upgraded` there if given.
async fn publish_resolution_block(
    pool: &PgPool,
    chain: &str,
    block: i64,
    upgrade: Option<(&str, &str)>,
) -> Result<()> {
    let hash = format!("0xresolution{block}");
    seed_schema_v2_lookup_head(pool, chain, block, &hash, "2026-10-01T00:00:00Z").await?;
    if let Some((proxy, implementation)) = upgrade {
        let mut event = history_event(
            &format!("resolution-upgraded-{block}"),
            None,
            None,
            Some(chain),
            Some(block),
            Some(&hash),
            Some(&format!("0xupgrade{block}")),
            Some(0),
            CanonicalityState::Canonical,
        );
        event.event_kind = "Upgraded".into();
        event.source_family = "ens_execution".into();
        event.before_state = json!({});
        event.after_state = json!({"proxy_address": proxy, "implementation": implementation});
        bigname_storage::insert_normalized_event_fixtures(pool, &[event]).await?;
    }
    publish_test_families_on(pool, chain, block).await
}

fn sepolia_network(resolution: Option<Value>) -> Value {
    let mut network = json!({ "network": "ethereum-sepolia", "chain_id": 11155111 });
    if let Some(resolution) = resolution {
        network["resolution"] = resolution;
    }
    json!([network])
}

/// The Sepolia network's `resolution` follows the client-facing Universal Resolver proxy path
/// that Project derives from `Upgraded` events under the checked-in manifests, including a
/// rollback to an unlisted implementation and the return to the listed one, and is withheld
/// while the publication is not servable.
#[tokio::test]
async fn v2_namespace_ens_reports_the_resolution_protocol_across_sepolia_upgrades() -> Result<()> {
    const CHAIN: &str = "ethereum-sepolia";
    let database = TestDatabase::new(true).await?;
    sync_checked_in_manifests(&database, "sepolia").await?;

    let (status, payload) = read_family_response(&database, "/v1/namespaces/ens").await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    assert_eq!(
        payload["data"]["networks"],
        sepolia_network(None),
        "no family publication yet"
    );

    for (block, upgrade, expected) in [
        (11821678, None, json!({ "protocol": "ens_v1", "since_block": null })),
        (
            11821679,
            Some((RESOLUTION_TOP, RESOLUTION_MANAGED)),
            json!({ "protocol": "ens_v1", "since_block": 11821679 }),
        ),
        (
            11821680,
            Some((RESOLUTION_MANAGED, RESOLUTION_ADMITTED)),
            json!({ "protocol": "ens_v2", "since_block": 11821680 }),
        ),
        (
            11821681,
            Some((RESOLUTION_MANAGED, RESOLUTION_UNLISTED)),
            json!({ "protocol": "ens_v1", "since_block": 11821681 }),
        ),
        (
            11821682,
            Some((RESOLUTION_MANAGED, RESOLUTION_ADMITTED)),
            json!({ "protocol": "ens_v2", "since_block": 11821682 }),
        ),
        // Repointing the client-facing proxy later dates the state, not the managed row.
        (
            11821683,
            Some((RESOLUTION_TOP, RESOLUTION_MANAGED)),
            json!({ "protocol": "ens_v2", "since_block": 11821683 }),
        ),
    ] {
        publish_resolution_block(&database.pool, CHAIN, block, upgrade).await?;
        let (status, payload) = read_family_response(&database, "/v1/namespaces/ens").await?;
        assert_eq!(status, StatusCode::OK, "{payload:#}");
        assert_eq!(
            payload["data"]["networks"],
            sepolia_network(Some(expected)),
            "after block {block}"
        );
        assert!(payload["meta"].get("as_of").is_none(), "{payload:#}");
    }

    // A Project redo over the publication makes name reads stale; the field is withheld with it.
    sqlx::query(
        "UPDATE chain_phase_state SET redo_in_progress = true, redo_mode = 'redo',
             redo_from_block_number = 11821683, redo_to_block_number = 11821683,
             redo_previous_phase_status = phase_status,
             redo_previous_started_at = started_at, redo_previous_finished_at = finished_at,
             phase_status = 'running', started_at = now(), finished_at = NULL,
             redo_attempt_generation = redo_attempt_generation + 1
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .execute(&database.pool)
    .await?;
    let (status, payload) = read_family_response(&database, "/v1/namespaces/ens").await?;
    assert_eq!(status, StatusCode::OK, "{payload:#}");
    assert_eq!(payload["data"]["networks"], sepolia_network(None));
    assert!(
        payload["data"]["capabilities"]["name_profile"].is_object(),
        "{payload:#}"
    );

    database.cleanup().await
}

/// Mainnet ENS has an execution entrypoint but no proxy upgrade, so once published it reads
/// ENSv1 with no start; Basenames networks have no ENSv1/ENSv2 split and carry no `resolution`.
#[tokio::test]
async fn v2_namespace_resolution_is_ens_v1_on_mainnet_and_absent_for_basenames() -> Result<()> {
    let database = TestDatabase::new(true).await?;
    sync_checked_in_manifests(&database, "mainnet").await?;
    publish_resolution_block(&database.pool, "ethereum-mainnet", 23_000_000, None).await?;

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
