// TYR-86: verified name detail executes the records route's chain-neutral inventory keys, or the
// bounded profile set when that inventory lists none.

const SEPOLIA_DETAIL_BLOCK: i64 = 21_000_003;
const SEPOLIA_DETAIL_HASH: &str = "0xsepolia-detail";
const SEPOLIA_DETAIL_TIME: &str = "2026-04-17T00:00:03Z";
const SEPOLIA_DETAIL_EXECUTED: &str = "0x0000000000000000000000000000000000000e0e";

/// alice.eth on the Sepolia ENS deployment with an admitted Universal Resolver, a resolver, and
/// an indexed `addr:60` write, as `seed_schema_v2_ens_record_lookup` seeds it on Mainnet. With
/// `keep_record_write = false` the resolver holds no indexed record, so the inventory lists no key.
async fn seed_sepolia_record_lookup(database: &TestDatabase, keep_record_write: bool) -> Result<()> {
    database.initialize_lookup_schema().await?;
    let pool = database.lookup_pool().await?;
    seed_schema_v2_lookup_head(
        &pool,
        "ethereum-sepolia",
        SEPOLIA_DETAIL_BLOCK,
        SEPOLIA_DETAIL_HASH,
        SEPOLIA_DETAIL_TIME,
    )
    .await?;
    seed_schema_v2_ens_manifest_on_chain(
        &pool,
        "ethereum-sepolia",
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0203),
        true,
    )
    .await?;
    seed_record_lookup_inputs(
        &pool,
        "ethereum-sepolia",
        "ens",
        "alice.eth",
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0201),
        Uuid::from_u128(0xc200_0000_0000_0000_0000_0000_0000_0202),
        SEPOLIA_DETAIL_BLOCK,
        SEPOLIA_DETAIL_HASH,
        SEPOLIA_DETAIL_TIME,
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    if !keep_record_write {
        sqlx::query("DELETE FROM normalized_events WHERE event_kind = 'RecordChanged'")
            .execute(&pool)
            .await?;
        rebuild_fixture_families(
            &pool,
            "ethereum-sepolia",
            SEPOLIA_DETAIL_BLOCK,
            SEPOLIA_DETAIL_HASH,
        )
        .await?;
    }
    Ok(())
}

/// Answers `calls` Universal Resolver reads by their inner getter: `addr(bytes32)` returns
/// `address`, `text(bytes32,"url")` a URL, and every other text or contenthash read is unset.
/// Record calls run concurrently, so the answer follows the request rather than its order.
async fn spawn_record_getter_mock_rpc(
    calls: usize,
    address: &'static str,
    revert: bool,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<Value>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind record getter mock RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::new();
        for _ in 0..calls {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept record getter mock RPC request")?;
            let request = read_primary_name_mock_rpc_request(&mut socket).await?;
            let data = request["params"][0]["data"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase();
            let response = if revert {
                json!({"__rpc_error": {"code": -32000, "message": "execution reverted"}})
            } else if data.contains("3b3b57de") {
                resolution_universal_resolver_addr60_response(address)
            } else if data.contains(&hex::encode("url")) && !data.contains(&hex::encode("avatar")) {
                resolution_universal_resolver_text_response("https://alice.example")
            } else {
                resolution_universal_resolver_text_response("")
            };
            write_primary_name_mock_rpc_response(&mut socket, response).await?;
            requests.push(request);
        }
        Ok(requests)
    });
    Ok((url, handle))
}

async fn sepolia_verified_get(
    database: &TestDatabase,
    rpc_url: &str,
    uri: &str,
) -> Result<(StatusCode, Value)> {
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-sepolia={rpc_url}"),
        ])?)
        .await?;
    let response = app_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty()).expect("request must build"))
        .await
        .context("sepolia verified request failed")?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

#[tokio::test]
async fn verified_name_detail_executes_the_chain_neutral_inventory_keys_on_sepolia() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_sepolia_record_lookup(&database, true).await?;
    // Detail requests the inventory's addr:60; the records route then reads the same key.
    let (rpc_url, rpc_handle) =
        spawn_record_getter_mock_rpc(2, SEPOLIA_DETAIL_EXECUTED, false).await?;

    let (status, detail) =
        sepolia_verified_get(&database, &rpc_url, "/v1/names/alice.eth?source=verified").await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["meta"]["source"], json!("verified"));
    let data = &detail["data"];
    assert_eq!(data["status"], json!("ok"), "{detail}");
    assert!(data.get("unsupported_reason").is_none(), "{detail}");
    assert_eq!(data["addresses"], json!({"60": SEPOLIA_DETAIL_EXECUTED}), "{detail}");
    assert_eq!(data["primary_address"], json!(SEPOLIA_DETAIL_EXECUTED));
    assert_eq!(data["unsupported_fields"], json!(["content_hash", "text_records"]));
    // Registration facts stay indexed.
    assert_eq!(data["registration_status"], json!("active"));
    assert_eq!(
        data["registrant"],
        json!("0x0000000000000000000000000000000000000def")
    );

    let (status, records) = sepolia_verified_get(
        &database,
        &rpc_url,
        "/v1/names/alice.eth/records?source=verified",
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{records}");
    assert_eq!(
        records["data"]["records"]["addr:60"],
        json!({"status": "ok", "value": SEPOLIA_DETAIL_EXECUTED})
    );
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 2);
    database.cleanup().await
}

#[tokio::test]
async fn verified_name_detail_reads_the_profile_set_without_inventory_keys() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_sepolia_record_lookup(&database, false).await?;
    let (rpc_url, rpc_handle) = spawn_record_getter_mock_rpc(
        crate::v2::support::PROFILE_FALLBACK_RECORD_KEYS.len(),
        SEPOLIA_DETAIL_EXECUTED,
        false,
    )
    .await?;

    let (status, detail) =
        sepolia_verified_get(&database, &rpc_url, "/v1/names/alice.eth?source=verified").await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let data = &detail["data"];
    assert_eq!(data["status"], json!("ok"), "{detail}");
    assert_eq!(data["addresses"], json!({"60": SEPOLIA_DETAIL_EXECUTED}), "{detail}");
    assert_eq!(data["primary_address"], json!(SEPOLIA_DETAIL_EXECUTED));
    // Unset avatar, description and email are served as absent, not as unsupported.
    assert_eq!(
        data["text_records"],
        json!({"url": "https://alice.example"}),
        "{detail}"
    );
    assert!(data.get("content_hash").is_none(), "{detail}");
    assert!(data.get("unsupported_fields").is_none(), "{detail}");

    let requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(requests.len(), crate::v2::support::PROFILE_FALLBACK_RECORD_KEYS.len());
    database.cleanup().await
}

#[tokio::test]
async fn verified_name_detail_reports_a_failed_getter_as_failed() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_sepolia_record_lookup(&database, true).await?;
    let (rpc_url, rpc_handle) =
        spawn_record_getter_mock_rpc(1, SEPOLIA_DETAIL_EXECUTED, true).await?;

    let (status, detail) =
        sepolia_verified_get(&database, &rpc_url, "/v1/names/alice.eth?source=verified").await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let data = &detail["data"];
    assert_eq!(data["status"], json!("failed"), "{detail}");
    assert_eq!(data["failure_reason"], json!("resolver_call_reverted"));
    assert!(data.get("addresses").is_none(), "{detail}");
    assert_eq!(
        data["unsupported_fields"],
        json!(["addresses", "content_hash", "primary_address", "text_records"])
    );
    assert_eq!(data["registration_status"], json!("active"));
    assert_eq!(join_primary_name_mock_rpc_requests(rpc_handle).await?.len(), 1);
    database.cleanup().await
}

#[tokio::test]
async fn verified_name_detail_dispatches_nothing_for_an_ineligible_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // A released lease serves no resolver, so verified detail refuses before any provider call:
    // the provider below is unreachable and would fail the request if it were dialled.
    seed_alice_state_inputs(&database, AliceInputState::Released).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let unreachable = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={unreachable}"),
        ])?)
        .await?;
    let response = app_router(state)
        .oneshot(
            Request::builder()
                .uri("/v1/names/alice.eth?source=verified")
                .body(Body::empty())
                .expect("request must build"),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let payload: Value = read_json(response).await?;
    let data = &payload["data"];
    assert_eq!(data["status"], json!("unsupported"), "{payload}");
    assert_eq!(data["unsupported_reason"], json!("verified_records_not_supported"));
    assert_eq!(data["registration_status"], json!("released"));
    database.cleanup().await
}
