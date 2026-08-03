use std::{str::FromStr, time::Duration};

use alloy_primitives::{Address, Bytes};
use alloy_sol_types::SolValue;
use anyhow::{Context, Result as AnyResult, bail};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

use crate::{
    BASENAMES_NAMESPACE, ChainRpcUrls, ENS_NAMESPACE, EnsPrimaryNameStatus, ErrorKind,
    LedgerAction, LookupEngine, LookupRequest, LookupResponse, RecordSelector,
    abi::{dns_encode_name, hex_string, namehash},
    ccip::encode_offchain_lookup_for_test,
};

const ETHEREUM: &str = "ethereum-mainnet";
const BASE: &str = "base-mainnet";
const ETHEREUM_HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
const ETHEREUM_LATER_HASH: &str =
    "0x3333333333333333333333333333333333333333333333333333333333333333";
const ETHEREUM_PRIOR_HASH: &str =
    "0x4444444444444444444444444444444444444444444444444444444444444444";
const BASE_HASH: &str = "0x2222222222222222222222222222222222222222222222222222222222222222";
const UNIVERSAL_RESOLVER: &str = "0xeeeeeeee14d718c2b47d9923deab1335e144eeee";
const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const BASE_L1_RESOLVER: &str = "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31";
const INDEXED_VALUE: &str = "https://indexed.example";
const LIVE_VALUE: &str = "https://live.example";

enum RpcResponse {
    Result(Value),
    Error {
        code: i64,
        message: String,
        data: Value,
    },
}

#[derive(Clone, Copy)]
enum FixtureKind {
    Ens,
    Basenames,
}

struct Fixture {
    database: TestDatabase,
    logical_name_id: String,
}

impl Fixture {
    fn pool(&self) -> &PgPool {
        self.database.pool()
    }

    async fn cleanup(self) -> AnyResult<()> {
        self.database.cleanup().await
    }
}

#[test]
fn lookup_request_canonicalizes_and_deduplicates_selectors() -> crate::Result<()> {
    let request = LookupRequest::new("ens:alice.eth", ["addr:060", "addr:60", "text:url"])?;
    assert_eq!(
        request.records,
        vec![
            RecordSelector::parse("addr:60")?,
            RecordSelector::parse("text:url")?,
        ]
    );
    Ok(())
}

#[test]
fn lookup_request_preserves_exact_text_selector_keys() -> crate::Result<()> {
    let request = LookupRequest::new("ens:alice.eth", ["text:url ", "text: "])?;
    assert_eq!(
        request.records,
        vec![
            RecordSelector {
                record_key: "text: ".to_owned(),
                record_family: "text".to_owned(),
                selector_key: Some(" ".to_owned()),
            },
            RecordSelector {
                record_key: "text:url ".to_owned(),
                record_family: "text".to_owned(),
                selector_key: Some("url ".to_owned()),
            },
        ]
    );
    Ok(())
}

#[test]
fn lookup_request_rejects_postgres_unsafe_text_selectors() {
    let error = LookupRequest::new("ens:alice.eth", ["text:a\0b"])
        .expect_err("NUL-bearing text selectors cannot cross the PostgreSQL boundary");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[test]
fn avatar_comparison_uses_the_text_avatar_inventory_entry() -> crate::Result<()> {
    let entries = json!([{
        "record_key": "text:avatar",
        "record_family": "text",
        "selector_key": "avatar",
        "status": "success",
        "value": { "kind": "text", "value": "ipfs://avatar" },
    }]);
    assert_eq!(
        crate::store::indexed_answer(&entries, &RecordSelector::parse("avatar")?),
        json!({ "status": "success", "value": "ipfs://avatar" })
    );
    Ok(())
}

#[tokio::test]
async fn agreeing_resolution_writes_nothing_to_divergence_ledger() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(
        INDEXED_VALUE,
    ))])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let result = run_lookup(&fixture, &rpc_url).await;
    let outcome = finish_fixture(fixture, result).await?;

    assert_eq!(outcome.records[0].ledger_action, LedgerAction::None);
    assert_eq!(outcome.records[0].value, Some(json!(INDEXED_VALUE)));
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn disagreement_writes_one_ledger_row_with_answers_and_anchor() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let result = run_lookup(&fixture, &rpc_url).await;

    let response = result?;
    assert_eq!(response.records[0].ledger_action, LedgerAction::Written);
    let (count, indexed, live, positions): (i64, Value, Value, Value) = sqlx::query_as(
        r#"
        SELECT count(*) OVER (), indexed_result, live_result, observed_positions
        FROM resolution_divergences
        WHERE cleared_at IS NULL
        "#,
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(count, 1);
    assert_eq!(
        indexed,
        json!({ "status": "success", "value": INDEXED_VALUE })
    );
    assert_eq!(live, json!({ "status": "success", "value": LIVE_VALUE }));
    assert_eq!(positions["ethereum"]["block_hash"], ETHEREUM_HASH);
    assert_eq!(positions["ethereum"]["block_number"], 10);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn unsupported_resolver_inventory_does_not_block_live_lookup() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE record_inventory_current
         SET support_status = 'unsupported',
             unsupported_reason = 'resolver_family_unsupported'",
    )
    .execute(fixture.pool())
    .await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(response.records[0].value, Some(json!(LIVE_VALUE)));
    assert_eq!(response.records[0].ledger_action, LedgerAction::Written);
    assert_eq!(ledger_count(fixture.pool()).await?, 1);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn out_of_class_projected_topology_is_not_executed() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(
             jsonb_set(
                 declared_summary,
                 '{topology,subregistry_path}',
                 '[{\"logical_name_id\":\"ens:ancestor\"}]'::jsonb
             ),
             '{topology,resolver_path,0,logical_name_id}',
             to_jsonb('ens:ancestor'::text)
         )",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("linked-subregistry path must not execute");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn wildcard_lookup_executes_without_an_indexed_record_row() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(
             jsonb_set(
                 declared_summary,
                 '{topology,wildcard}',
                 jsonb_build_object(
                     'source', jsonb_build_object('logical_name_id', 'ens:ancestor'),
                     'matched_labels', jsonb_build_array('alice')
                 )
             ),
             '{topology,resolver_path,0,logical_name_id}',
             to_jsonb('ens:ancestor'::text)
         )",
    )
    .execute(fixture.pool())
    .await?;
    sqlx::query("DELETE FROM record_inventory_current")
        .execute(fixture.pool())
        .await?;

    let result = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(result.records[0].value, Some(json!(LIVE_VALUE)));
    assert_eq!(result.records[0].ledger_action, LedgerAction::None);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn closed_surface_binding_is_not_readable() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET active_to = '2026-08-04T00:00:00Z'",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("closed surface binding must not be readable");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn projected_position_timestamp_must_match_canonical_lineage() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE name_current
         SET chain_positions = jsonb_set(
             chain_positions,
             '{ethereum,timestamp}',
             to_jsonb('2026-08-03T00:00:01Z'::text)
         )",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("projected position timestamp must identify canonical lineage");
    assert_eq!(error.kind(), ErrorKind::Stale);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn record_lookup_missing_selected_state_is_stale() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: -32000,
        message: "missing state".to_owned(),
        data: Value::Null,
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;

    let error = run_lookup(&fixture, &rpc_url)
        .await
        .expect_err("missing selected state must be stale");
    assert_eq!(error.kind(), ErrorKind::Stale);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn row_unchanged_guard_rejects_two_session_projection_modification() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let engine = lookup_engine(&pool, &rpc_url)?;
    let logical_name_id = fixture.logical_name_id.clone();
    let update_pool = pool.clone();
    let result = engine
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query(
                "UPDATE record_inventory_current
                 SET entries = jsonb_set(entries, '{0,value,value}', '\"https://concurrent.example\"')",
            )
            .execute(&update_pool)
            .await
            .expect("second session must update the compared projection row");
        })
        .await;

    let error = result.expect_err("stale projection token must reject the ledger write");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn reorg_between_read_and_write_rejects_and_leaves_no_active_row() -> AnyResult<()> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let engine = lookup_engine(&pool, &rpc_url)?;
    let logical_name_id = fixture.logical_name_id.clone();
    let reorg_pool = pool.clone();
    let result = engine
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            let mut transaction = reorg_pool.begin().await.expect("reorg session must begin");
            sqlx::query("DELETE FROM chain_heads WHERE chain_id = $1")
                .bind(ETHEREUM)
                .execute(&mut *transaction)
                .await
                .expect("reorg session must detach the old head");
            sqlx::query(
                "UPDATE chain_lineage SET canonicality_state = 'orphaned'
                 WHERE chain_id = $1 AND block_hash = $2",
            )
            .bind(ETHEREUM)
            .bind(ETHEREUM_HASH)
            .execute(&mut *transaction)
            .await
            .expect("reorg session must orphan the compared block");
            transaction
                .commit()
                .await
                .expect("reorg session must commit");
        })
        .await;

    let error = result.expect_err("orphaned lookup anchor must reject the ledger write");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger_count(&pool).await?, 0);
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn reorged_agreement_does_not_clear_an_older_active_divergence() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(
        INDEXED_VALUE,
    ))])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 9, '2026-08-02T23:59:59Z', 'canonical')",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_PRIOR_HASH)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result
        ) VALUES (
            $1,
            $2,
            '0x1000000000000000000000000000000000000001',
            'text:url',
            $3,
            '{"status":"success","value":"https://older-indexed.example"}'::jsonb,
            '{"status":"success","value":"https://older-live.example"}'::jsonb
        )
        "#,
    )
    .bind(&fixture.logical_name_id)
    .bind(ETHEREUM)
    .bind(json!({
        "ethereum": {
            "chain_id": ETHEREUM,
            "block_number": 9,
            "block_hash": ETHEREUM_PRIOR_HASH,
            "timestamp": "2026-08-02T23:59:59Z",
        }
    }))
    .execute(&pool)
    .await?;

    let engine = lookup_engine(&pool, &rpc_url)?;
    let logical_name_id = fixture.logical_name_id.clone();
    let reorg_pool = pool.clone();
    let result = engine
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            let mut transaction = reorg_pool.begin().await.expect("reorg session must begin");
            sqlx::query("DELETE FROM chain_heads WHERE chain_id = $1")
                .bind(ETHEREUM)
                .execute(&mut *transaction)
                .await
                .expect("reorg session must detach the old head");
            sqlx::query(
                "UPDATE chain_lineage SET canonicality_state = 'orphaned'
                 WHERE chain_id = $1 AND block_hash = $2",
            )
            .bind(ETHEREUM)
            .bind(ETHEREUM_HASH)
            .execute(&mut *transaction)
            .await
            .expect("reorg session must orphan the agreement observation");
            transaction
                .commit()
                .await
                .expect("reorg session must commit");
        })
        .await;

    let error = result.expect_err("orphaned agreement must not clear an older divergence");
    assert_eq!(error.kind(), ErrorKind::ConcurrentState);
    let active_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM resolution_divergences WHERE cleared_at IS NULL")
            .fetch_one(&pool)
            .await?;
    assert_eq!(active_count, 1);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn successful_ccip_result_is_never_persisted() -> AnyResult<()> {
    let (gateway_url, gateway_handle) = spawn_gateway(vec![0xca, 0xfe]).await?;
    let sender = Address::from_str(BASE_L1_RESOLVER)?;
    let offchain_data = encode_offchain_lookup_for_test(
        sender,
        vec![gateway_url],
        vec![0x12, 0x34],
        [0x01, 0x02, 0x03, 0x04],
        vec![0xab],
    );
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: Value::String(offchain_data),
        },
        RpcResponse::Result(encoded_basenames_text_result(LIVE_VALUE)),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let result = run_lookup(&fixture, &rpc_url).await?;

    assert_eq!(result.records[0].value, Some(json!(LIVE_VALUE)));
    assert!(result.records[0].ccip_read);
    assert_eq!(result.records[0].ledger_action, LedgerAction::SkippedCcip);
    assert_eq!(ledger_count(&pool).await?, 0);
    assert_eq!(result.observed_positions["base"]["block_hash"], BASE_HASH);
    assert_eq!(
        result.observed_positions["ethereum"]["block_hash"],
        ETHEREUM_HASH
    );

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 2);
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    gateway_handle
        .await
        .context("gateway task was cancelled")??;
    Ok(())
}

#[tokio::test]
async fn ccip_result_bypasses_a_concurrent_inventory_change() -> AnyResult<()> {
    let (gateway_url, gateway_handle) = spawn_gateway(vec![0xca, 0xfe]).await?;
    let sender = Address::from_str(BASE_L1_RESOLVER)?;
    let offchain_data = encode_offchain_lookup_for_test(
        sender,
        vec![gateway_url],
        vec![0x12, 0x34],
        [0x01, 0x02, 0x03, 0x04],
        vec![0xab],
    );
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: Value::String(offchain_data),
        },
        RpcResponse::Result(encoded_basenames_text_result(LIVE_VALUE)),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    let pool = fixture.pool().clone();
    let engine = lookup_engine(&pool, &rpc_url)?;
    let logical_name_id = fixture.logical_name_id.clone();
    let update_pool = pool.clone();
    let result = engine
        .lookup_with_before_persist(lookup_request(&logical_name_id)?, move || async move {
            sqlx::query(
                "UPDATE record_inventory_current
                 SET entries = jsonb_set(
                     entries,
                     '{0,value,value}',
                     '\"https://concurrent.example\"'
                 )",
            )
            .execute(&update_pool)
            .await
            .expect("second session must update the projection row");
        })
        .await?;

    assert_eq!(result.records[0].value, Some(json!(LIVE_VALUE)));
    assert!(result.records[0].ccip_read);
    assert_eq!(result.records[0].ledger_action, LedgerAction::SkippedCcip);
    assert_eq!(ledger_count(&pool).await?, 0);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    gateway_handle
        .await
        .context("gateway task was cancelled")??;
    Ok(())
}

#[tokio::test]
async fn basenames_uses_projected_auxiliary_execution_position() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(
        encoded_basenames_text_result(INDEXED_VALUE),
    )])
    .await?;
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 11, '2026-08-03T00:00:01Z', 'canonical')",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_LATER_HASH)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_hash = $2, latest_block_number = 11
         WHERE chain_id = $1",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_LATER_HASH)
    .execute(fixture.pool())
    .await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    assert_eq!(
        response.observed_positions["ethereum"]["block_hash"],
        ETHEREUM_HASH
    );
    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn basenames_shadow_execution_manifest_is_not_authority() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET rollout_status = 'shadow',
             manifest_payload = jsonb_set(
                 manifest_payload,
                 '{capability_flags,verified_resolution,status}',
                 to_jsonb('shadow'::text)
             )
         WHERE source_family = 'basenames_execution'",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("shadow Basenames execution manifest must not execute");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn basenames_v1_execution_manifest_is_not_authority() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_version = 1
         WHERE source_family = 'basenames_execution'",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("Basenames execution manifest v1 must not execute");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ens_execution_manifest_without_resolution_capability_is_not_authority() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET rollout_status = 'shadow',
             manifest_payload = '{\"capability_flags\": {}}'::jsonb
         WHERE source_family = 'ens_execution'",
    )
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("ENS execution without an explicit capability must not execute");
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn unsupported_active_ens_manifest_does_not_fall_back_to_shadow() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET rollout_status = 'shadow',
             manifest_payload = jsonb_set(
                 manifest_payload,
                 '{capability_flags,verified_resolution,status}',
                 to_jsonb('shadow'::text)
             )
         WHERE source_family = 'ens_execution'",
    )
    .execute(fixture.pool())
    .await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (2, $1, 'ens_execution', $2, 'test', 'active', 'test',
                 'test/ens/ens_execution-v2.toml', '{\"capability_flags\": {}}'::jsonb)
         RETURNING manifest_id",
    )
    .bind(ENS_NAMESPACE)
    .bind(ETHEREUM)
    .fetch_one(fixture.pool())
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, $2, 'contract', 'universal_resolver', $3::uuid, $4,
                 'universal_resolver', 'none')",
    )
    .bind(manifest_id)
    .bind(ETHEREUM)
    .bind("00000000-0000-0000-0000-000000000103")
    .bind(UNIVERSAL_RESOLVER)
    .execute(fixture.pool())
    .await?;

    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("an active manifest without the capability must not fall back to shadow");
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = jsonb_build_object(
                 'capability_flags', jsonb_build_object(
                     'verified_resolution', jsonb_build_object('status', 'supported')
                 )
             )
         WHERE manifest_id = $1",
    )
    .bind(manifest_id)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "UPDATE manifest_contract_instances
         SET role = 'replacement_resolver'
         WHERE manifest_id = $1",
    )
    .bind(manifest_id)
    .execute(fixture.pool())
    .await?;
    let error = lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
        .expect_err("an active manifest without the role must not fall back to shadow");
    assert_eq!(error.kind(), ErrorKind::Unsupported);

    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_lookup_uses_manifest_entrypoints_and_readable_head() -> AnyResult<()> {
    let target = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let reverse_resolver = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(Value::String(hex_string(
            &Address::from_str(reverse_resolver)?.abi_encode(),
        ))),
        RpcResponse::Result(Value::String(hex_string(&"alice.eth".abi_encode()))),
        RpcResponse::Result(encoded_address_result(target)?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = '{}'::jsonb
         WHERE source_family = 'ens_v1_registry_l1'",
    )
    .execute(fixture.pool())
    .await?;
    let result = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name(target)
        .await?;

    assert_eq!(result.status, EnsPrimaryNameStatus::Success);
    assert_eq!(result.name.as_deref(), Some("alice.eth"));
    assert_eq!(result.normalized_name.as_deref(), Some("alice.eth"));
    assert_eq!(result.forward_address.as_deref(), Some(target));
    assert_eq!(
        result.reverse_resolver_address.as_deref(),
        Some(reverse_resolver)
    );
    assert!(!result.ccip_read);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests[0]["params"][0]["to"], ENS_REGISTRY);
    assert_eq!(requests[1]["params"][0]["to"], reverse_resolver);
    assert_eq!(requests[2]["params"][0]["to"], UNIVERSAL_RESOLVER);
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    Ok(())
}

#[tokio::test]
async fn primary_name_missing_forward_address_is_not_found() -> AnyResult<()> {
    let target = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let reverse_resolver = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![
        RpcResponse::Result(Value::String(hex_string(
            &Address::from_str(reverse_resolver)?.abi_encode(),
        ))),
        RpcResponse::Result(Value::String(hex_string(&"alice.eth".abi_encode()))),
        RpcResponse::Result(encoded_address_result(
            "0x0000000000000000000000000000000000000000",
        )?),
    ])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;

    let result = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name(target)
        .await?;
    assert_eq!(result.status, EnsPrimaryNameStatus::NotFound);
    assert_eq!(result.forward_address, None);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_selected_block_error_is_stale() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: -32000,
        message: "header not found".to_owned(),
        data: Value::Null,
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;

    let error = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name("0x8e8db5ccef88cca9d624701db544989c996e3216")
        .await
        .expect_err("unavailable selected block must be stale");
    assert_eq!(error.kind(), ErrorKind::Stale);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_missing_selected_state_is_stale() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Error {
        code: -32000,
        message: "missing state".to_owned(),
        data: Value::Null,
    }])
    .await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;

    let error = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name("0x8e8db5ccef88cca9d624701db544989c996e3216")
        .await
        .expect_err("missing selected state must be stale");
    assert_eq!(error.kind(), ErrorKind::Stale);

    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok(())
}

#[tokio::test]
async fn primary_name_configured_response_timeout_is_in_band() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_hanging_rpc().await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;
    let rpc_urls = ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={rpc_url}")])?
        .with_http_timeouts(Duration::from_millis(50), Duration::from_millis(150))?;

    let result = LookupEngine::new(fixture.pool().clone(), rpc_urls)
        .lookup_ens_primary_name("0x8e8db5ccef88cca9d624701db544989c996e3216")
        .await?;
    assert_eq!(result.status, EnsPrimaryNameStatus::ExecutionFailed);
    assert_eq!(
        result.failure_reason.as_deref(),
        Some("resolver_call_failed")
    );

    rpc_handle.abort();
    fixture.cleanup().await?;
    Ok(())
}

async fn finish_fixture(
    fixture: Fixture,
    result: crate::Result<LookupResponse>,
) -> AnyResult<LookupResponse> {
    let count = ledger_count(fixture.pool()).await?;
    fixture.cleanup().await?;
    let response = result?;
    assert_eq!(count, 0);
    Ok(response)
}

fn lookup_engine(pool: &PgPool, rpc_url: &str) -> AnyResult<LookupEngine> {
    let rpc_urls = ChainRpcUrls::from_entries(&[format!("{ETHEREUM}={rpc_url}")])?;
    Ok(LookupEngine::new(pool.clone(), rpc_urls))
}

async fn run_lookup(fixture: &Fixture, rpc_url: &str) -> crate::Result<LookupResponse> {
    lookup_engine(fixture.pool(), rpc_url)
        .map_err(|error| crate::LookupError::configuration(error.to_string()))?
        .lookup(lookup_request(&fixture.logical_name_id)?)
        .await
}

fn lookup_request(logical_name_id: &str) -> crate::Result<LookupRequest> {
    LookupRequest::new(logical_name_id, ["text:url"])
}

async fn ledger_count(pool: &PgPool) -> AnyResult<i64> {
    sqlx::query_scalar("SELECT count(*) FROM resolution_divergences")
        .fetch_one(pool)
        .await
        .context("failed to count resolution divergences")
}

async fn setup_fixture(kind: FixtureKind, indexed_value: &str) -> AnyResult<Fixture> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new("bigname_lookup").pool_max_connections(6))
            .await?;
    apply_baseline(database.pool()).await?;
    let (namespace, name, resolver_chain, resolver_hash, entrypoint, role, source_family) =
        match kind {
            FixtureKind::Ens => (
                ENS_NAMESPACE,
                "alice.eth",
                ETHEREUM,
                ETHEREUM_HASH,
                UNIVERSAL_RESOLVER,
                "universal_resolver",
                "ens_execution",
            ),
            FixtureKind::Basenames => (
                BASENAMES_NAMESPACE,
                "alice.base.eth",
                BASE,
                BASE_HASH,
                BASE_L1_RESOLVER,
                "l1_resolver",
                "basenames_execution",
            ),
        };
    seed_heads(database.pool(), kind).await?;
    seed_manifest(
        database.pool(),
        namespace,
        source_family,
        role,
        entrypoint,
        "00000000-0000-0000-0000-000000000103",
    )
    .await?;
    let namehash = hex_string(&namehash(name)?);
    let logical_name_id = format!("{namespace}:{namehash}");
    let dns_name = dns_encode_name(name)?;
    let resource_id = "00000000-0000-0000-0000-000000000101";
    let binding_id = "00000000-0000-0000-0000-000000000102";
    let resolver_address = "0x1000000000000000000000000000000000000001";
    let inventory_positions = json!({
        "target_block_number": 10,
        "target_block_hash": resolver_hash,
    });
    let resolver_slot = if resolver_chain == ETHEREUM {
        "ethereum"
    } else {
        "base"
    };
    let mut name_positions = json!({
        (resolver_slot): {
            "chain_id": resolver_chain,
            "block_number": 10,
            "block_hash": resolver_hash,
            "timestamp": "2026-08-03T00:00:00Z",
        },
    });
    if matches!(kind, FixtureKind::Basenames) {
        name_positions["ethereum"] = json!({
            "chain_id": ETHEREUM,
            "block_number": 10,
            "block_hash": ETHEREUM_HASH,
            "timestamp": "2026-08-03T00:00:00Z",
        });
    }
    let boundary = json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id,
        "normalized_event_id": 1,
        "event_kind": "ResolverChanged",
        "chain_position": {
            "chain_id": resolver_chain,
            "block_number": 10,
            "block_hash": resolver_hash,
            "timestamp": "2026-08-03T00:00:00Z",
        },
    });
    let transport = match kind {
        FixtureKind::Ens => json!({
            "source_chain_id": null,
            "target_chain_id": null,
            "contract_address": null,
            "latest_event_kind": null,
        }),
        FixtureKind::Basenames => json!({
            "source_chain_id": BASE,
            "target_chain_id": ETHEREUM,
            "contract_address": entrypoint,
            "latest_event_kind": "ResolverChanged",
        }),
    };
    let topology = json!({
        "registry_path": [],
        "subregistry_path": [],
        "resolver_path": [{
            "logical_name_id": logical_name_id,
            "resource_id": resource_id,
            "chain_id": resolver_chain,
            "address": resolver_address,
        }],
        "wildcard": { "source": null, "matched_labels": [] },
        "alias": { "final_target": null, "hops": [] },
        "version_boundaries": { "record_version_boundary": boundary },
        "transport": transport,
    });
    seed_identity_and_projection(
        database.pool(),
        namespace,
        name,
        &logical_name_id,
        &namehash,
        &dns_name,
        resolver_chain,
        resolver_hash,
        resource_id,
        binding_id,
        &topology,
        &boundary,
        &name_positions,
        &inventory_positions,
        indexed_value,
    )
    .await?;

    Ok(Fixture {
        database,
        logical_name_id,
    })
}

async fn apply_baseline(pool: &PgPool) -> AnyResult<()> {
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(pool).await?;
    }
    Ok(())
}

async fn seed_heads(pool: &PgPool, kind: FixtureKind) -> AnyResult<()> {
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 10, '2026-08-03T00:00:00Z', 'canonical')",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, 10)",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_HASH)
    .execute(pool)
    .await?;
    if matches!(kind, FixtureKind::Basenames) {
        sqlx::query(
            "INSERT INTO chain_lineage
                (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ($1, $2, 10, '2026-08-03T00:00:00Z', 'canonical')",
        )
        .bind(BASE)
        .bind(BASE_HASH)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
             VALUES ($1, $2, 10)",
        )
        .bind(BASE)
        .bind(BASE_HASH)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_manifest(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
    role: &str,
    address: &str,
    contract_id: &str,
) -> AnyResult<()> {
    let manifest_version = if source_family == "basenames_execution" {
        2
    } else {
        1
    };
    sqlx::query(
        "INSERT INTO contract_instances
            (contract_instance_id, chain_id, contract_kind)
         VALUES ($1::uuid, $2, 'contract')",
    )
    .bind(contract_id)
    .bind(ETHEREUM)
    .execute(pool)
    .await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES ($5, $1, $2, $3, 'test', 'active', 'test', $4,
                 jsonb_build_object('capability_flags', jsonb_build_object(
                     'verified_resolution', jsonb_build_object('status', 'supported'))))
         RETURNING manifest_id",
    )
    .bind(namespace)
    .bind(source_family)
    .bind(ETHEREUM)
    .bind(format!("test/{namespace}/{source_family}.toml"))
    .bind(manifest_version)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances
            (manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind)
         VALUES ($1, $2, 'contract', $3, $4::uuid, $5, $3, 'none')",
    )
    .bind(manifest_id)
    .bind(ETHEREUM)
    .bind(role)
    .bind(contract_id)
    .bind(address)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn seed_identity_and_projection(
    pool: &PgPool,
    namespace: &str,
    name: &str,
    logical_name_id: &str,
    namehash: &str,
    dns_name: &[u8],
    chain_id: &str,
    block_hash: &str,
    resource_id: &str,
    binding_id: &str,
    topology: &Value,
    boundary: &Value,
    name_positions: &Value,
    inventory_positions: &Value,
    indexed_value: &str,
) -> AnyResult<()> {
    sqlx::query(
        "INSERT INTO resources
            (resource_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3, 10, 'canonical')",
    )
    .bind(resource_id)
    .bind(chain_id)
    .bind(block_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces
            (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, $2, $3, ARRAY[$3], $4, $5, ARRAY[$5], 'test', 'active',
                 $6, $7, 10, 'canonical')",
    )
    .bind(logical_name_id)
    .bind(namespace)
    .bind(name)
    .bind(dns_name)
    .bind(namehash)
    .bind(chain_id)
    .bind(block_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings
            (surface_binding_id, logical_name_id, resource_id, binding_kind, active_from,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path',
                 '2026-08-03T00:00:00Z', $4, $5, 10, 'canonical')",
    )
    .bind(binding_id)
    .bind(logical_name_id)
    .bind(resource_id)
    .bind(chain_id)
    .bind(block_hash)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_current
            (logical_name_id, namespace, raw_name, namehash, surface_binding_id,
             resource_id, binding_kind, declared_summary, support_status,
             provenance, chain_positions, canonicality_summary, manifest_version)
         VALUES ($1, $2, $3, $4, $5::uuid, $6::uuid, 'declared_registry_path',
                 jsonb_build_object('topology', $7::jsonb), 'supported', '{}', $8,
                 jsonb_build_object('state', 'canonical'), 1)",
    )
    .bind(logical_name_id)
    .bind(namespace)
    .bind(name)
    .bind(namehash)
    .bind(binding_id)
    .bind(resource_id)
    .bind(topology)
    .bind(name_positions)
    .execute(pool)
    .await?;
    let selectors = json!([{
        "record_key": "text:url",
        "record_family": "text",
        "selector_key": "url",
    }]);
    let entries = json!([{
        "record_key": "text:url",
        "record_family": "text",
        "selector_key": "url",
        "status": "success",
        "value": { "kind": "text", "value": indexed_value },
    }]);
    sqlx::query(
        "INSERT INTO record_inventory_current
            (resource_id, record_version_boundary_key, record_version_boundary,
             selectors, unsupported_families, entries, support_status, provenance,
             chain_positions, canonicality_summary, manifest_version)
         VALUES ($1::uuid, 'boundary-1', $2, $3, '[]', $4, 'supported', '{}', $5,
                 jsonb_build_object('state', 'canonical'), 1)",
    )
    .bind(resource_id)
    .bind(boundary)
    .bind(selectors)
    .bind(entries)
    .bind(inventory_positions)
    .execute(pool)
    .await?;
    Ok(())
}

fn encoded_text_result(value: &str) -> Value {
    let record_result = (value.to_owned(),).abi_encode_params();
    let universal_result = (Bytes::from(record_result), Address::ZERO).abi_encode_params();
    Value::String(hex_string(&universal_result))
}

fn encoded_basenames_text_result(value: &str) -> Value {
    let record_result = (value.to_owned(),).abi_encode_params();
    let l1_resolver_result = (Bytes::from(record_result),).abi_encode_params();
    Value::String(hex_string(&l1_resolver_result))
}

fn encoded_address_result(address: &str) -> AnyResult<Value> {
    let record_result = Address::from_str(address)?.abi_encode();
    let universal_result = (Bytes::from(record_result), Address::ZERO).abi_encode_params();
    Ok(Value::String(hex_string(&universal_result)))
}

async fn spawn_mock_rpc(
    responses: Vec<RpcResponse>,
) -> AnyResult<(String, JoinHandle<AnyResult<Vec<Value>>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let mut requests = Vec::with_capacity(responses.len());
        for response in responses {
            let (mut socket, _) = listener.accept().await?;
            requests.push(read_http_json_body(&mut socket).await?);
            write_rpc_response(&mut socket, response).await?;
        }
        Ok(requests)
    });
    Ok((url, handle))
}

async fn spawn_gateway(response: Vec<u8>) -> AnyResult<(String, JoinHandle<AnyResult<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        read_http_json_body(&mut socket).await?;
        let body = json!({ "data": hex_string(&response) }).to_string();
        write_http_response(&mut socket, &body).await
    });
    Ok((url, handle))
}

async fn spawn_hanging_rpc() -> AnyResult<(String, JoinHandle<AnyResult<()>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        read_http_json_body(&mut socket).await?;
        std::future::pending::<()>().await;
        #[allow(unreachable_code)]
        Ok(())
    });
    Ok((url, handle))
}

async fn read_http_json_body(socket: &mut TcpStream) -> AnyResult<Value> {
    let mut buffer = Vec::new();
    let mut scratch = [0_u8; 1024];
    let (body_start, content_length) = loop {
        let read = socket.read(&mut scratch).await?;
        if read == 0 {
            bail!("HTTP request closed before its headers completed");
        }
        buffer.extend_from_slice(&scratch[..read]);
        if let Some(body_start) = buffer
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)
        {
            let headers = std::str::from_utf8(&buffer[..body_start])?;
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>())
                })
                .transpose()?
                .context("HTTP request omitted content-length")?;
            break (body_start, length);
        }
    };
    while buffer.len() < body_start + content_length {
        let read = socket.read(&mut scratch).await?;
        if read == 0 {
            bail!("HTTP request closed before its body completed");
        }
        buffer.extend_from_slice(&scratch[..read]);
    }
    serde_json::from_slice(&buffer[body_start..body_start + content_length])
        .context("HTTP request body was not JSON")
}

async fn write_rpc_response(socket: &mut TcpStream, response: RpcResponse) -> AnyResult<()> {
    let payload = match response {
        RpcResponse::Result(result) => json!({ "jsonrpc": "2.0", "id": 1, "result": result }),
        RpcResponse::Error {
            code,
            message,
            data,
        } => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "error": { "code": code, "message": message, "data": data },
        }),
    };
    write_http_response(socket, &payload.to_string()).await
}

async fn write_http_response(socket: &mut TcpStream, body: &str) -> AnyResult<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

async fn join_rpc(handle: JoinHandle<AnyResult<Vec<Value>>>) -> AnyResult<Vec<Value>> {
    handle.await.context("mock RPC task was cancelled")?
}

fn assert_hash_pinned(requests: &[Value], expected_hash: &str) {
    assert!(!requests.is_empty());
    for request in requests {
        assert_eq!(request["method"], "eth_call");
        assert_eq!(request["params"][1]["blockHash"], expected_hash);
        assert_eq!(request["params"][1]["requireCanonical"], true);
    }
}
