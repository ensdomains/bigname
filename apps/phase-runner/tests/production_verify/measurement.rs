use super::*;
use bigname_ingest::measurement::Session;

fn observations(logs: &CapturedLogs) -> Vec<Value> {
    logs.text()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|v| v["target"] == "bigname_memory")
        .map(|v| v["fields"].clone())
        .collect()
}

#[tokio::test]
async fn real_store_and_rpc_complete_nonempty_comparison_and_preserve_failures() -> Result<()> {
    let scratch = ScratchDatabase::create("measurement_real_rpc").await?;
    seed_ingest_identities(scratch.pool(), SEPOLIA).await?;
    seed_watch_manifest(scratch.pool(), SEPOLIA).await?;
    for number in 0i64..=5 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,$4,to_timestamp($4),'finalized')")
            .bind(SEPOLIA).bind(hash(number)).bind((number > 0).then(|| hash(number - 1))).bind(number).execute(scratch.pool()).await?;
    }
    sqlx::query("INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address) VALUES ($1,$2,2,$3,0,$4)")
        .bind(SEPOLIA).bind(hash(2)).bind(hash(100)).bind(CONTRACT).execute(scratch.pool()).await?;
    sqlx::query("INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data) VALUES ($1,$2,2,$3,0,0,$4,$5,$6)")
        .bind(SEPOLIA).bind(hash(2)).bind(hash(100)).bind(CONTRACT).bind(vec![transfer_topic0()]).bind(vec![1u8]).execute(scratch.pool()).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, Router::new().route("/", post(rpc)))
            .await
            .unwrap();
    });
    let chain = sepolia_role_chain("https://intake.invalid", &endpoint)?;
    let phase = VerifyPhase::new(scratch.verification_database(1).await?);
    phase.preflight(SEPOLIA, &chain.sources, &RunMode::Normal)?;
    let context = PhaseContext {
        chain_id: SEPOLIA.into(),
        phase: PhaseName::Verify,
        mode: RunMode::Normal,
        redo_attempt: None,
        sources: chain.sources.clone(),
        available_heads: Some(HeadMarkers {
            latest: BlockMarker::new(5, hash(5))?,
            safe: Some(BlockMarker::new(5, hash(5))?),
            finalized: Some(BlockMarker::new(5, hash(5))?),
        }),
        live_handoff: None,
        resume: Default::default(),
    };
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(logs.clone())
        .finish();
    let session = Session::new("real-rpc").unwrap();
    let result = session
        .scope(phase.run_batch(context.clone()))
        .with_subscriber(subscriber)
        .await?;
    assert!(matches!(result, PhaseBatchOutcome::Complete(_)));
    let events = observations(&logs);
    for stage in [
        "stored_query_return",
        "provider_query_return",
        "rpc_verification_split_response",
        "rpc_text_and_json",
        "rpc_json_and_result_clone",
        "both_vectors",
        "compare_matched",
    ] {
        assert!(
            events.iter().any(|v| v["stage"] == stage),
            "missing {stage}"
        );
    }
    let transport = events
        .iter()
        .filter(|v| v["stage"] == "rpc_transport")
        .collect::<Vec<_>>();
    assert!(
        transport
            .iter()
            .any(|v| v["transport_outcome"] == "rpc_error"
                && v["split_to"].as_u64().unwrap() - v["split_from"].as_u64().unwrap() > 2)
    );
    assert!(
        transport
            .iter()
            .any(|v| v["transport_outcome"] == "success" && v["rpc_method"] == "eth_getLogs")
    );
    for (index, event) in transport.iter().enumerate() {
        assert_eq!(event["transport_ordinal"], index + 1);
    }
    let stored = events
        .iter()
        .find(|v| v["stage"] == "stored_final")
        .unwrap();
    let reference = events
        .iter()
        .find(|v| v["stage"] == "provider_final")
        .unwrap();
    assert_eq!(stored["rows"], 1);
    assert_eq!(stored["logical_bytes"], reference["logical_bytes"]);
    assert!(
        events
            .iter()
            .filter(|v| v["stage"] == "rpc_response_text")
            .all(|v| v["attempt"] == 1)
    );
    // Boundary fixture changes data; no production checkpoint is pre-populated as success.
    sqlx::query("UPDATE raw_logs SET data=$1 WHERE chain_id=$2")
        .bind(vec![2u8])
        .bind(SEPOLIA)
        .execute(scratch.pool())
        .await?;
    assert_eq!(
        session
            .scope(phase.run_batch(context))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::VerificationMismatch
    );
    assert!(session.valid());
    server.abort();
    drop(phase);
    scratch.cleanup().await
}

fn hash(number: i64) -> String {
    format!("0x{:064x}", number + 1)
}
async fn rpc(Json(request): Json<Value>) -> Json<Value> {
    Json(match request.as_array() {
        Some(calls) => Value::Array(calls.iter().map(rpc_response).collect()),
        None => rpc_response(&request),
    })
}

fn rpc_response(request: &Value) -> Value {
    let id = request["id"].clone();
    let result = match request["method"].as_str().unwrap() {
        "eth_getBlockByNumber" => {
            let requested = request["params"][0].as_str().unwrap();
            let number = match requested {
                "latest" | "safe" | "finalized" => 5,
                number => i64::from_str_radix(number.trim_start_matches("0x"), 16).unwrap(),
            };
            assert!((0..=5).contains(&number), "block outside fixture range");
            json!({"hash":hash(number),"parentHash":hash(number-1),"number":format!("0x{number:x}"),"timestamp":format!("0x{number:x}")})
        }
        "eth_getLogs" => {
            let from = i64::from_str_radix(
                request["params"][0]["fromBlock"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("0x"),
                16,
            )
            .unwrap();
            let to = i64::from_str_radix(
                request["params"][0]["toBlock"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("0x"),
                16,
            )
            .unwrap();
            if to - from > 2 {
                return json!({"jsonrpc":"2.0","id":id,"error":{"code":-32005,"message":"query exceeds max results"}});
            }
            if from <= 2 && to >= 2 {
                json!([{"blockHash":hash(2),"blockNumber":"0x2","transactionHash":hash(100),"transactionIndex":"0x0","logIndex":"0x0","address":CONTRACT,"topics":[transfer_topic0()],"data":"0x01","removed":false}])
            } else {
                json!([])
            }
        }
        method => panic!("unexpected method {method}"),
    };
    json!({"jsonrpc":"2.0","id":id,"result":result})
}
