//! Retained October contract execution through the real Engine, writer and Project.
use super::*;
use bigname_adapters::schema_v2::seam::{
    LOG_INDEX_KEY, TOKEN_CONTROL_TRANSFERRED_EVENT_KIND, TOKEN_LINEAGE_ID_KEY,
};
use serde_json::Value;

const LOGICAL: &str = "ens:0x25951599af2d0b44f9144675c5d830ddfd835e6bac3c4aea1baa7c9d0c9d9524";
const LEASE: &str = "334dff9e-552a-5b39-a5ea-64487c24cbc1";
const SUCCESSOR: &str = "86629b8a-bda8-5bc1-b3b2-e611da3c7bd4";
const MIGRATION: i64 = 50;

fn string<'a>(value: &'a Value, key: &str) -> TestResult<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| format!("missing {key}").into())
}

fn quantity(value: &Value, key: &str) -> TestResult<i64> {
    Ok(i64::from_str_radix(
        string(value, key)?.trim_start_matches("0x"),
        16,
    )?)
}

fn numeric(value: &Value, key: &str) -> TestResult<String> {
    Ok(U256::from_str_radix(string(value, key)?.trim_start_matches("0x"), 16)?.to_string())
}

async fn seed_chain(
    pool: &PgPool,
    chain: &[Value],
    time_step: i64,
    prefix_only: bool,
) -> TestResult {
    for row in chain {
        let block = &row["block"];
        let number = quantity(block, "number")?;
        let hash = string(block, "hash")?;
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,$4,to_timestamp($5),'canonical')")
            .bind(CHAIN).bind(hash).bind(if number == 0 { None } else { Some(string(block,"parentHash")?) })
            .bind(number).bind((quantity(block,"timestamp")? + number * time_step) as f64).execute(pool).await?;
        for tx in block["transactions"]
            .as_array()
            .ok_or("transactions missing")?
        {
            let tx_hash = string(tx, "hash")?;
            let tx_index = quantity(tx, "transactionIndex")?;
            let receipt = row["receipts"]
                .as_array()
                .ok_or("receipts missing")?
                .iter()
                .find(|receipt| receipt["transactionHash"] == tx_hash)
                .ok_or("receipt missing")?;
            assert_eq!(string(receipt, "blockHash")?, hash);
            assert_eq!(quantity(receipt, "blockNumber")?, number);
            assert_eq!(quantity(receipt, "transactionIndex")?, tx_index);
            sqlx::query("INSERT INTO raw_transactions (chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address,input,value) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::text::numeric)")
                .bind(CHAIN).bind(hash).bind(number).bind(tx_hash).bind(tx_index)
                .bind(string(tx,"from")?).bind(tx["to"].as_str())
                .bind(alloy_primitives::hex::decode(string(tx,"input")?)?).bind(numeric(tx,"value")?).execute(pool).await?;
            sqlx::query("INSERT INTO raw_receipts (chain_id,block_hash,block_number,transaction_hash,transaction_index,contract_address,status,gas_used,cumulative_gas_used,logs_bloom) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::text::numeric,$9::text::numeric,$10)")
                .bind(CHAIN).bind(hash).bind(number).bind(tx_hash).bind(tx_index)
                .bind(receipt["contractAddress"].as_str()).bind(quantity(receipt,"status")? != 0)
                .bind(numeric(receipt,"gasUsed")?).bind(numeric(receipt,"cumulativeGasUsed")?)
                .bind(alloy_primitives::hex::decode(string(receipt,"logsBloom")?)?).execute(pool).await?;
            for log in receipt["logs"].as_array().ok_or("logs missing")? {
                if prefix_only && number == MIGRATION && quantity(log, "logIndex")? > 0 {
                    continue;
                }
                assert_eq!(string(log, "blockHash")?, hash);
                assert_eq!(string(log, "transactionHash")?, tx_hash);
                let topics: Vec<String> = log["topics"]
                    .as_array()
                    .ok_or("topics missing")?
                    .iter()
                    .map(|v| v.as_str().map(str::to_owned).ok_or("topic is not text"))
                    .collect::<std::result::Result<_, _>>()?;
                sqlx::query("INSERT INTO raw_logs (chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)")
                    .bind(CHAIN).bind(hash).bind(number).bind(tx_hash).bind(tx_index).bind(quantity(log,"logIndex")?)
                    .bind(string(log,"address")?).bind(topics).bind(alloy_primitives::hex::decode(string(log,"data")?)?).execute(pool).await?;
            }
        }
    }
    let last = &chain.last().ok_or("empty chain")?["block"];
    sqlx::query("INSERT INTO chain_heads (chain_id,latest_block_hash,latest_block_number) VALUES ($1,$2,$3)")
        .bind(CHAIN).bind(string(last,"hash")?).bind(quantity(last,"number")?).execute(pool).await?;
    for phase in ["ingest", "interpret", "project", "verify", "live"] {
        sqlx::query("INSERT INTO chain_phase_state (chain_id,phase_name) VALUES ($1,$2) ON CONFLICT DO NOTHING")
            .bind(CHAIN).bind(phase).execute(pool).await?;
    }
    Ok(())
}

async fn prepare(chain: &[Value], time_step: i64, prefix_only: bool) -> TestResult<TestDatabase> {
    let db = database("interpret_intermediary_migration").await?;
    stamp_interpreter_hash(db.pool(), bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    let manifests = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/intermediary-migration/manifests");
    sync_schema_v2_repository(db.pool(), &load_repository(&manifests)?).await?;
    seed_chain(db.pool(), chain, time_step, prefix_only).await?;
    Ok(db)
}

fn request(mode: RunMode) -> BatchRequest {
    BatchRequest {
        chain_id: CHAIN.to_owned(),
        from_block: 0,
        to_block: MIGRATION,
        resume_current: None,
        mode,
    }
}

/// The zero-step input is the exact captured chain. The twelve-second step is an explicitly
/// generated timing control, proving that the failure is not confined to equal Anvil timestamps.
#[tokio::test]
async fn intermediary_migration_preserves_ordinary_prefix_across_restore_batching_and_redo()
-> TestResult {
    let chain: Vec<Value> = serde_json::from_str(include_str!(
        "../../../tests/fixtures/intermediary-migration/chain.json"
    ))?;
    let deployment: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/intermediary-migration/deployment.json"
    ))?;
    assert_eq!(chain.len(), 51);
    assert_eq!(
        quantity(&chain.last().ok_or("empty chain")?["block"], "number")?,
        MIGRATION
    );
    for time_step in [0, 12] {
        // Deliberately incomplete final raw-log selection: stop immediately after A -> batcher.
        // This is an ordinary-interpretation control, not a second captured transaction.
        let control = prepare(&chain, time_step, true).await?;
        Engine::new(control.pool().clone())
            .run_batch(request(RunMode::Normal))
            .await?;
        let ordinary_prefix = prefix_snapshot(control.pool()).await?;
        let mut snapshots = Vec::new();
        for mode in ["continuing", "cold", "combined"] {
            let db = prepare(&chain, time_step, false).await?;
            let pool = db.pool();
            let mut engine = Engine::new(pool.clone());
            let mut next = request(RunMode::Normal);
            let mut earlier = None;
            if mode != "combined" {
                engine = engine
                    .with_blocks_per_batch(std::num::NonZeroU32::new(MIGRATION as u32).unwrap());
                let prior = engine.run_batch(next.clone()).await?;
                assert_eq!(prior.current.number, MIGRATION - 1);
                assert!(!prior.complete);
                next.resume_current = Some(prior.current);
                earlier = Some(earlier_events(pool).await?);
                if mode == "cold" {
                    engine = Engine::new(pool.clone());
                }
            }
            let outcome = engine.run_batch(next).await?;
            assert_eq!(outcome.current.number, MIGRATION);
            assert!(outcome.complete);
            if let Some(earlier) = earlier {
                assert_eq!(
                    earlier_events(pool).await?,
                    earlier,
                    "older observations remain unchanged"
                );
            }
            let prefix = prefix_snapshot(pool).await?;
            assert!(
                prefix == ordinary_prefix,
                "time step {time_step}, {mode}: {}",
                equivalence::first_json_difference(&ordinary_prefix, &prefix, "$")
            );
            assert_committed(pool, &chain, &deployment).await?;
            let before_redo = snapshot(pool).await?;
            let redo = Engine::new(pool.clone())
                .run_batch(request(RunMode::Redo))
                .await?;
            assert!(redo.complete);
            let after_redo = snapshot(pool).await?;
            assert!(
                before_redo == after_redo,
                "time step {time_step}, {mode}: {}",
                equivalence::first_json_difference(&before_redo, &after_redo, "$")
            );
            assert_eq!(prefix_snapshot(pool).await?, ordinary_prefix);
            assert_committed(pool, &chain, &deployment).await?;
            snapshots.push(before_redo);
            db.cleanup().await?;
        }
        for other in &snapshots[1..] {
            assert!(
                snapshots[0] == *other,
                "time step {time_step}: {}",
                equivalence::first_json_difference(&snapshots[0], other, "$")
            );
        }
        control.cleanup().await?;
    }
    Ok(())
}

async fn earlier_events(pool: &PgPool) -> TestResult<Vec<Value>> {
    Ok(sqlx::query_scalar("SELECT to_jsonb(n) - ARRAY['normalized_event_id','observed_at'] FROM normalized_events n WHERE block_number < $1 ORDER BY event_identity")
        .bind(MIGRATION).fetch_all(pool).await?)
}

async fn snapshot(pool: &PgPool) -> TestResult<Value> {
    let events: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(n) - ARRAY['normalized_event_id','observed_at'] FROM normalized_events n ORDER BY event_identity")
        .fetch_all(pool).await?;
    let bindings: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(b) - ARRAY['inserted_at','observed_at'] FROM surface_bindings b ORDER BY surface_binding_id",
    )
    .fetch_all(pool)
    .await?;
    Ok(serde_json::json!({"events":events,"bindings":bindings}))
}

async fn prefix_snapshot(pool: &PgPool) -> TestResult<Value> {
    let events: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(n) - ARRAY['normalized_event_id','observed_at'] FROM normalized_events n WHERE block_number=$1 AND log_index=0 ORDER BY event_identity")
        .bind(MIGRATION).fetch_all(pool).await?;
    let bindings: Vec<Value> = sqlx::query_scalar(&format!("SELECT to_jsonb(b) - ARRAY['inserted_at','observed_at','active_to'] FROM surface_bindings b WHERE block_number=$1 AND provenance->>'{LOG_INDEX_KEY}'='0' ORDER BY surface_binding_id"))
        .bind(MIGRATION).fetch_all(pool).await?;
    assert!(!events.is_empty());
    assert_eq!(
        bindings.len(),
        1,
        "the prefix opens its ordinary registry authority"
    );
    Ok(serde_json::json!({"events":events,"bindings":bindings}))
}

async fn assert_committed(pool: &PgPool, chain: &[Value], deployment: &Value) -> TestResult {
    let counts: (i64, i64) = sqlx::query_as("SELECT count(*) FILTER (WHERE authority_arm='ens_v1'),count(*) FILTER (WHERE authority_arm='ens_v2') FROM surface_bindings WHERE logical_name_id=$1 AND active_to IS NULL AND canonicality_state='canonical'")
        .bind(LOGICAL).fetch_one(pool).await?;
    assert_eq!(counts, (0, 1));
    let successor: Uuid = sqlx::query_scalar("SELECT resource_id FROM surface_bindings WHERE logical_name_id=$1 AND authority_arm='ens_v2' AND active_to IS NULL")
        .bind(LOGICAL).fetch_one(pool).await?;
    assert_eq!(successor.to_string(), SUCCESSOR);
    let boundaries: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE logical_name_id=$1 AND event_kind='MigrationApplied' AND consumer_visibility='activated'")
        .bind(LOGICAL).fetch_one(pool).await?;
    assert_eq!(boundaries, 1);
    let unexpected: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM surface_bindings WHERE active_from >= active_to OR (logical_name_id=$1 AND block_number=$2 AND authority_arm='ens_v1' AND provenance->>'{LOG_INDEX_KEY}'<>'0')"))
        .bind(LOGICAL).bind(MIGRATION).fetch_one(pool).await?;
    assert_eq!(
        unexpected, 0,
        "only the ordinary prefix can open an ENSv1 binding"
    );
    let prefix_closed: bool = sqlx::query_scalar(&format!("SELECT b.active_from < b.active_to AND b.active_to=l.block_timestamp + interval '4 microseconds' FROM surface_bindings b JOIN chain_lineage l USING (chain_id,block_hash) WHERE b.logical_name_id=$1 AND b.block_number=$2 AND b.provenance->>'{LOG_INDEX_KEY}'='0'"))
        .bind(LOGICAL).bind(MIGRATION).fetch_one(pool).await?;
    assert!(
        prefix_closed,
        "the prefix has a nonempty interval ending at exact cleanup"
    );
    let transfers: Vec<(i64, Uuid, Value, Value)> = sqlx::query_as(&format!("SELECT log_index,resource_id,before_state,after_state FROM normalized_events WHERE block_number=$1 AND source_family='ens_v1_registrar_l1' AND event_kind='{TOKEN_CONTROL_TRANSFERRED_EVENT_KIND}' ORDER BY log_index"))
        .bind(MIGRATION).fetch_all(pool).await?;
    assert_eq!(transfers.len(), 3);
    for (transfer, log, from, to) in [
        (&transfers[0], 0, &deployment["a"], &deployment["batcher"]),
        (
            &transfers[1],
            1,
            &deployment["batcher"],
            &deployment["controller"],
        ),
        (
            &transfers[2],
            4,
            &deployment["controller"],
            &deployment["graveyard"],
        ),
    ] {
        assert_eq!(transfer.0, log);
        assert_eq!(transfer.1.to_string(), LEASE);
        assert_eq!(&transfer.2["from"], from);
        assert_eq!(&transfer.3["to"], to);
        assert_eq!(
            transfer.3[TOKEN_LINEAGE_ID_KEY],
            "027d5746-78cf-5a9a-a86b-ac8a4fad5598"
        );
    }
    assert_eq!(transfers[2].3["registrar_surface_retired"], true);
    let marker = bigname_project::Marker {
        number: MIGRATION,
        hash: string(&chain.last().ok_or("empty chain")?["block"], "hash")?.to_owned(),
    };
    let token = bigname_project::families::input_token(pool, CHAIN).await?;
    let outcome = bigname_project::families::apply(
        pool,
        CHAIN,
        &marker,
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await?;
    assert_eq!(outcome.marker, Some(marker));
    let summary = bigname_storage::families::name::load_family_name(pool, LOGICAL)
        .await?
        .ok_or("published intermediary migration name")?
        .declared_summary;
    assert_eq!(
        summary["registration"]["authority_kind"], "ens_v2_registry",
        "{summary:#}"
    );
    assert_eq!(
        summary["control"]["registry_owner"], deployment["a"],
        "{summary:#}"
    );
    Ok(())
}
