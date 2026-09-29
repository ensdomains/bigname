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

/// The record key a Universal Resolver `resolve(name, data)` call reads, from its inner getter
/// selector and, for `text`, the key bytes in the calldata.
fn requested_record_key(data: &str) -> String {
    let data = data.to_ascii_lowercase();
    if data.contains("3b3b57de") {
        return "addr:60".to_owned();
    }
    if data.contains("bc1c58d1") {
        return "contenthash".to_owned();
    }
    if data.contains("59d1d43c") {
        if data.contains(&hex::encode("avatar")) {
            return "avatar".to_owned();
        }
        for key in ["description", "email", "url"] {
            if data.contains(&hex::encode(key)) {
                return format!("text:{key}");
            }
        }
        return "text:?".to_owned();
    }
    "unknown".to_owned()
}

/// The default answers: `addr:60` is the executed address, `text:url` a URL, and every other
/// text or contenthash read is unset.
fn profile_answer(key: &str) -> Value {
    match key {
        "addr:60" => resolution_universal_resolver_addr60_response(SEPOLIA_DETAIL_EXECUTED),
        "text:url" => resolution_universal_resolver_text_response("https://alice.example"),
        _ => resolution_universal_resolver_text_response(""),
    }
}

fn reverted(_: &str) -> Value {
    json!({"__rpc_error": {"code": -32000, "message": "execution reverted"}})
}

/// Answers `calls` Universal Resolver reads with `answer(record key)` and returns the record keys
/// read, sorted. Record calls run concurrently, so each answer follows its request rather than
/// the arrival order.
async fn spawn_record_getter_mock_rpc(
    calls: usize,
    answer: fn(&str) -> Value,
) -> Result<(String, tokio::task::JoinHandle<Result<Vec<String>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind record getter mock RPC listener")?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut keys = Vec::new();
        for _ in 0..calls {
            let (mut socket, _) = listener
                .accept()
                .await
                .context("failed to accept record getter mock RPC request")?;
            let request = read_primary_name_mock_rpc_request(&mut socket).await?;
            let key = requested_record_key(request["params"][0]["data"].as_str().unwrap_or_default());
            write_primary_name_mock_rpc_response(&mut socket, answer(&key)).await?;
            keys.push(key);
        }
        keys.sort();
        Ok(keys)
    });
    Ok((url, handle))
}

async fn joined_keys(handle: tokio::task::JoinHandle<Result<Vec<String>>>) -> Result<Vec<String>> {
    handle.await.context("record getter mock RPC task panicked")?
}

const PROFILE_KEYS_SORTED: [&str; 6] = [
    "addr:60",
    "avatar",
    "contenthash",
    "text:description",
    "text:email",
    "text:url",
];

fn unreachable_rpc_url() -> Result<String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(format!("http://{}", listener.local_addr()?))
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
    let (rpc_url, rpc_handle) = spawn_record_getter_mock_rpc(2, profile_answer).await?;

    let (status, detail) =
        sepolia_verified_get(&database, &rpc_url, "/v1/names/alice.eth?source=verified").await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["meta"]["source"], json!("verified"));
    let data = &detail["data"];
    assert_eq!(data["status"], json!("ok"), "{detail}");
    assert!(data.get("unsupported_reason").is_none(), "{detail}");
    // `records` lists exactly the keys the lookup read; the forward name is not read.
    assert_eq!(
        data["records"],
        json!({
            "seen_addresses": ["60"],
            "addresses": {"60": SEPOLIA_DETAIL_EXECUTED},
            "seen_texts": [],
            "texts": {},
            "seen_abis": [],
            "abis": {},
            "seen_singletons": []
        }),
        "{detail}"
    );
    assert_eq!(data["primary_address"], json!(SEPOLIA_DETAIL_EXECUTED));
    assert!(data.get("unsupported_fields").is_none(), "{detail}");
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
    assert_eq!(joined_keys(rpc_handle).await?, ["addr:60", "addr:60"]);
    database.cleanup().await
}

#[tokio::test]
async fn verified_name_detail_reads_the_profile_set_without_inventory_keys() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_sepolia_record_lookup(&database, false).await?;
    let (rpc_url, rpc_handle) =
        spawn_record_getter_mock_rpc(PROFILE_KEYS_SORTED.len(), profile_answer).await?;

    let (status, detail) =
        sepolia_verified_get(&database, &rpc_url, "/v1/names/alice.eth?source=verified").await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let data = &detail["data"];
    assert_eq!(data["status"], json!("ok"), "{detail}");
    assert_eq!(data["primary_address"], json!(SEPOLIA_DETAIL_EXECUTED));
    // Unset avatar, description, email and contenthash are served as cleared (`null`), not as
    // unknown. The inventory row lists no key and no ABI write.
    assert_eq!(
        data["records"],
        json!({
            "seen_addresses": ["60"],
            "addresses": {"60": SEPOLIA_DETAIL_EXECUTED},
            "seen_texts": ["avatar", "description", "email", "url"],
            "texts": {
                "avatar": null,
                "description": null,
                "email": null,
                "url": "https://alice.example"
            },
            "seen_abis": [],
            "abis": {},
            "seen_singletons": ["contenthash"],
            "contenthash": null
        }),
        "{detail}"
    );
    assert!(data.get("unsupported_fields").is_none(), "{detail}");

    assert_eq!(joined_keys(rpc_handle).await?, PROFILE_KEYS_SORTED);
    database.cleanup().await
}

#[tokio::test]
async fn verified_name_detail_reports_a_failed_getter_as_failed() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_sepolia_record_lookup(&database, true).await?;
    let (rpc_url, rpc_handle) = spawn_record_getter_mock_rpc(1, reverted).await?;

    let (status, detail) =
        sepolia_verified_get(&database, &rpc_url, "/v1/names/alice.eth?source=verified").await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let data = &detail["data"];
    assert_eq!(data["status"], json!("failed"), "{detail}");
    assert_eq!(data["failure_reason"], json!("resolver_call_reverted"));
    // The failed key stays listed with no value.
    assert_eq!(data["records"]["seen_addresses"], json!(["60"]), "{detail}");
    assert_eq!(data["records"]["addresses"], json!({}), "{detail}");
    assert!(data.get("primary_address").is_none(), "{detail}");
    assert_eq!(data["unsupported_fields"], json!(["primary_address"]));
    assert_eq!(data["registration_status"], json!("active"));
    assert_eq!(joined_keys(rpc_handle).await?, ["addr:60"]);
    database.cleanup().await
}

#[tokio::test]
async fn verified_name_detail_dispatches_nothing_for_an_ineligible_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // A released lease serves no resolver, so verified detail refuses before any provider call:
    // the provider below is unreachable and would fail the request if it were dialled.
    seed_alice_state_inputs(&database, AliceInputState::Released).await?;
    let unreachable = unreachable_rpc_url()?;
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

/// Sorted by key, the name-level failure is the first failed key's: `contenthash` fails before
/// `text:url` reverts, so the reason is `contenthash`'s. Unset keys stay served.
fn mixed_answer(key: &str) -> Value {
    match key {
        "contenthash" => json!({"__rpc_error": {"code": -32000, "message": "upstream unavailable"}}),
        "text:url" => reverted(key),
        _ => profile_answer(key),
    }
}

#[tokio::test]
async fn verified_name_detail_takes_the_first_failed_key_in_key_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_sepolia_record_lookup(&database, false).await?;
    let (rpc_url, rpc_handle) =
        spawn_record_getter_mock_rpc(PROFILE_KEYS_SORTED.len(), mixed_answer).await?;

    let (status, detail) =
        sepolia_verified_get(&database, &rpc_url, "/v1/names/alice.eth?source=verified").await?;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let data = &detail["data"];
    assert_eq!(data["status"], json!("failed"), "{detail}");
    assert_eq!(data["failure_reason"], json!("resolver_call_failed"), "{detail}");
    // The failed contenthash and the reverted text:url stay listed with no value.
    let records = &data["records"];
    assert_eq!(records["addresses"], json!({"60": SEPOLIA_DETAIL_EXECUTED}), "{detail}");
    assert_eq!(records["seen_texts"], json!(["avatar", "description", "email", "url"]), "{detail}");
    assert_eq!(
        records["texts"],
        json!({"avatar": null, "description": null, "email": null}),
        "{detail}"
    );
    // Read but failed: listed, value unknown.
    assert_eq!(records["seen_singletons"], json!(["contenthash"]), "{detail}");
    assert!(records.get("contenthash").is_none(), "{detail}");
    assert!(data.get("unsupported_fields").is_none(), "{detail}");
    assert_eq!(joined_keys(rpc_handle).await?, PROFILE_KEYS_SORTED);
    database.cleanup().await
}

/// An eligible registered name whose exact resolver is null and which has no inventory still
/// reads the profile set: the lookup discovers the resolver through the Universal Resolver. Only
/// registration or serving-path ineligibility guarantees that no call is made.
#[tokio::test]
async fn verified_name_detail_discovers_a_null_resolver_without_inventory() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    database.initialize_lookup_schema().await?;
    let lookup_pool = database.lookup_pool().await?;
    seed_schema_v2_ens_record_lookup(
        &lookup_pool,
        21_000_003,
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        "2026-04-17T00:00:03Z",
        "0x0000000000000000000000000000000000000def",
    )
    .await?;
    append_name_resolver_input(
        &database,
        "ens",
        "alice.eth",
        "0x0000000000000000000000000000000000000000",
    )
    .await?;
    let (rpc_url, rpc_handle) =
        spawn_record_getter_mock_rpc(PROFILE_KEYS_SORTED.len(), profile_answer).await?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={rpc_url}"),
        ])?)
        .await?;

    let indexed = v2_name_record_payload_for_database(&database, "/v1/names/alice.eth").await?;
    assert!(indexed["data"].get("resolver").is_none(), "{indexed}");
    assert!(indexed["data"].get("records").is_none(), "the name has no inventory: {indexed}");
    assert_eq!(indexed["data"]["unsupported_fields"], json!(["primary_address"]), "{indexed}");

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
    assert_eq!(data["status"], json!("ok"), "{payload}");
    assert!(data.get("resolver").is_none(), "{payload}");
    assert_eq!(data["records"]["addresses"], json!({"60": SEPOLIA_DETAIL_EXECUTED}), "{payload}");
    assert_eq!(data["records"]["texts"]["url"], json!("https://alice.example"));
    // With no inventory row the ABI content types cannot be listed.
    assert!(data["records"].get("seen_abis").is_none(), "{payload}");
    assert_eq!(
        data["records"]["abi_unsupported_reason"],
        json!("inventory_not_available"),
        "{payload}"
    );
    assert_eq!(joined_keys(rpc_handle).await?, PROFILE_KEYS_SORTED);
    lookup_pool.close().await;
    database.cleanup().await
}

/// 200 indexed text keys plus the indexed `addr:60` exceed the 200-key default limit: the request
/// is refused before any call, and the provider is unreachable to prove it.
#[tokio::test]
async fn verified_name_detail_refuses_an_oversized_inventory_without_a_call() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_sepolia_record_lookup(&database, true).await?;
    let writes = (0..crate::v2::MAX_PAGE_SIZE)
        .map(|index| family_fixture_record_write(&format!("text:key-{index}"), None))
        .collect::<Vec<_>>();
    insert_family_fixture_record_writes(
        &database.pool,
        "ens",
        "ethereum-sepolia",
        "alice.eth",
        "0x1000000000000000000000000000000000000001",
        SEPOLIA_DETAIL_BLOCK,
        SEPOLIA_DETAIL_HASH,
        &writes,
    )
    .await?;
    rebuild_fixture_families(
        &database.pool,
        "ethereum-sepolia",
        SEPOLIA_DETAIL_BLOCK,
        SEPOLIA_DETAIL_HASH,
    )
    .await?;

    let (status, payload) = sepolia_verified_get(
        &database,
        &unreachable_rpc_url()?,
        "/v1/names/alice.eth?source=verified",
    )
    .await?;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{payload}");
    assert_eq!(
        payload["error"]["message"],
        json!("inventory-derived record key sets support at most 200 record keys")
    );
    database.cleanup().await
}
