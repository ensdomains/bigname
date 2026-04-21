use std::{collections::BTreeMap, sync::Mutex};

include!("support.rs");

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedRpcRequest {
    method: String,
    params: Vec<Value>,
}

#[tokio::test]
async fn hash_pinned_backfill_persists_range_and_is_idempotent_without_advancing_checkpoints()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(901);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for backfill test")?;
    insert_contract_instance(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;
    sqlx::query(
        r#"
            INSERT INTO chain_checkpoints (
                chain_id,
                canonical_block_hash,
                canonical_block_number,
                safe_block_hash,
                safe_block_number,
                finalized_block_hash,
                finalized_block_number
            )
            VALUES (
                'ethereum-mainnet',
                '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
                7,
                '0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
                6,
                '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                5
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert checkpoint guard row for backfill test")?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let watched_chain = watched_plan
        .iter()
        .find(|chain| chain.chain == "ethereum-mainnet")
        .cloned()
        .expect("backfill test chain must be watched");
    let block_42 = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x9999999999999999999999999999999999999999999999999999999999999999"),
        42,
    );
    let block_43 = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        43,
    );
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let (provider, server) = number_resolving_provider(
        vec![block_42.clone(), block_43.clone()],
        Arc::clone(&requests),
    )
    .await?;

    let range = BackfillBlockRange::new(42, 43)?;
    let outcome =
        run_hash_pinned_backfill_range(database.pool(), &watched_chain, &provider, range).await?;
    assert_eq!(
        outcome,
        backfill::BackfillOutcome {
            chain: "ethereum-mainnet".to_owned(),
            from_block: 42,
            to_block: 43,
            resolved_block_count: 2,
            raw_block_count: 2,
            raw_transaction_count: 2,
            raw_receipt_count: 2,
            raw_log_count: 2,
            raw_code_hash_count: 2,
        }
    );

    let rerun =
        run_hash_pinned_backfill_range(database.pool(), &watched_chain, &provider, range).await?;
    assert_eq!(rerun, outcome);

    assert_eq!(table_count(database.pool(), "raw_blocks").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_transactions").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_receipts").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_logs").await?, 2);
    assert_eq!(table_count(database.pool(), "raw_code_hashes").await?, 2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'PreimageObserved'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(table_count(database.pool(), "chain_lineage").await?, 0);
    assert_eq!(
        sqlx::query_as::<_, (String, i64, String, i64, String, i64)>(
            r#"
            SELECT
                canonical_block_hash,
                canonical_block_number,
                safe_block_hash,
                safe_block_number,
                finalized_block_hash,
                finalized_block_number
            FROM chain_checkpoints
            WHERE chain_id = 'ethereum-mainnet'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        (
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            7,
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
            6,
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_owned(),
            5,
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_number = 42"
        )
        .fetch_one(database.pool())
        .await?,
        "observed".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_number = 43"
        )
        .fetch_one(database.pool())
        .await?,
        "observed".to_owned()
    );

    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(requests.len(), 20);
    assert_eq!(requests[0].method, "eth_getBlockByNumber");
    assert_eq!(
        requests[0].params.first().and_then(Value::as_str),
        Some("0x2a")
    );
    assert_eq!(requests[1].method, "eth_getBlockByNumber");
    assert_eq!(
        requests[1].params.first().and_then(Value::as_str),
        Some("0x2b")
    );
    assert_eq!(requests[2].method, "eth_getBlockByHash");
    assert_eq!(
        requests[2].params.first().and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );
    assert_eq!(requests[5].method, "eth_getCode");
    assert_eq!(
        requests[5]
            .params
            .get(1)
            .and_then(Value::as_object)
            .and_then(|selection| selection.get("blockHash"))
            .and_then(Value::as_str),
        Some(block_42.block_hash.as_str())
    );
    assert_eq!(requests[6].method, "eth_getBlockByHash");
    assert_eq!(
        requests[6].params.first().and_then(Value::as_str),
        Some(block_43.block_hash.as_str())
    );
    assert_eq!(requests[9].method, "eth_getCode");
    assert_eq!(
        requests[9]
            .params
            .get(1)
            .and_then(Value::as_object)
            .and_then(|selection| selection.get("blockHash"))
            .and_then(Value::as_str),
        Some(block_43.block_hash.as_str())
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn hash_pinned_backfill_fails_missing_hash_payload_without_number_fallback() -> Result<()> {
    let database = TestDatabase::new().await?;
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let requests = Arc::new(Mutex::new(Vec::<RecordedRpcRequest>::new()));
    let request_log = Arc::clone(&requests);

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        request_log
            .lock()
            .expect("request log must not be poisoned")
            .push(RecordedRpcRequest {
                method: method.to_owned(),
                params: params.clone(),
            });

        let result = match method {
            "eth_getBlockByNumber" => {
                assert_eq!(params.first().and_then(Value::as_str), Some("0x2a"));
                rpc_block_bundle_payload(&provider_block(block_hash, None, 42))
            }
            "eth_getBlockByHash" => Value::Null,
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }))
    .await?;
    let provider = provider::JsonRpcProvider::new(&url)?;
    let watched_chain = WatchedChainPlan {
        chain: "ethereum-mainnet".to_owned(),
        addresses: Vec::new(),
        manifest_root_entry_count: 1,
        manifest_contract_entry_count: 0,
        discovery_edge_entry_count: 0,
    };

    let error = run_hash_pinned_backfill_range(
        database.pool(),
        &watched_chain,
        &provider,
        BackfillBlockRange::new(42, 42)?,
    )
    .await
    .expect_err("missing hash-scoped block payload must fail");
    assert!(
        format!("{error:#}").contains(
            "provider did not return block 0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ),
        "unexpected error: {error:#}"
    );
    let requests = requests
        .lock()
        .expect("request log must not be poisoned")
        .clone();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        vec!["eth_getBlockByNumber", "eth_getBlockByHash"]
    );

    server.abort();
    database.cleanup().await
}

async fn number_resolving_provider(
    blocks: Vec<ProviderBlock>,
    requests: Arc<Mutex<Vec<RecordedRpcRequest>>>,
) -> Result<(provider::JsonRpcProvider, JoinHandle<()>)> {
    let blocks_by_hash = Arc::new(
        blocks
            .into_iter()
            .map(|block| (block.block_hash.clone(), block))
            .collect::<BTreeMap<_, _>>(),
    );
    let hashes_by_number = Arc::new(
        blocks_by_hash
            .values()
            .map(|block| (block.block_number, block.block_hash.clone()))
            .collect::<BTreeMap<_, _>>(),
    );

    let (url, server) = spawn_json_rpc_server(Arc::new(move |body| {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = body
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        requests
            .lock()
            .expect("request log must not be poisoned")
            .push(RecordedRpcRequest {
                method: method.to_owned(),
                params: params.clone(),
            });

        let result = match method {
            "eth_getBlockByNumber" => {
                assert_eq!(params.get(1), Some(&Value::Bool(false)));
                let block_number = params
                    .first()
                    .and_then(Value::as_str)
                    .map(parse_rpc_block_number)
                    .expect("block number parameter must be present");
                let block_hash = hashes_by_number
                    .get(&block_number)
                    .unwrap_or_else(|| panic!("unexpected block number request: {body}"));
                let block = blocks_by_hash
                    .get(block_hash)
                    .expect("number index must point at a fixture block");
                rpc_block_bundle_payload(block)
            }
            "eth_getBlockByHash" => {
                assert_eq!(params.get(1), Some(&Value::Bool(true)));
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let block = blocks_by_hash
                    .get(&block_hash)
                    .unwrap_or_else(|| panic!("unexpected block hash request: {body}"));
                rpc_block_bundle_payload(block)
            }
            "eth_getLogs" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_object)
                    .and_then(|filter| filter.get("blockHash"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let block = blocks_by_hash
                    .get(&block_hash)
                    .unwrap_or_else(|| panic!("unexpected log request: {body}"));
                Value::Array(vec![rpc_log_payload(block)])
            }
            "eth_getBlockReceipts" => {
                let block_hash = params
                    .first()
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let block = blocks_by_hash
                    .get(&block_hash)
                    .unwrap_or_else(|| panic!("unexpected receipt request: {body}"));
                Value::Array(vec![rpc_receipt_payload(block)])
            }
            "eth_getCode" => {
                let block_hash = params
                    .get(1)
                    .and_then(Value::as_object)
                    .and_then(|selection| selection.get("blockHash"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                assert!(
                    blocks_by_hash.contains_key(&block_hash),
                    "unexpected code block selection: {body}"
                );
                Value::String("0x6001600155".to_owned())
            }
            _ => panic!("unexpected RPC request: {body}"),
        };

        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": result,
        })
    }))
    .await?;

    Ok((provider::JsonRpcProvider::new(&url)?, server))
}

fn parse_rpc_block_number(value: &str) -> i64 {
    i64::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16)
        .expect("test RPC block number must be valid hex")
}

async fn table_count(pool: &PgPool, table_name: &str) -> Result<i64> {
    let query = format!("SELECT COUNT(*) FROM {table_name}");
    sqlx::query_scalar::<_, i64>(&query)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count {table_name} rows"))
}
