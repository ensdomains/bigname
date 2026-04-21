use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use serde_json::json;
use sqlx::types::time::OffsetDateTime;
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use super::*;
use crate::{
    NormalizedEvent, RawBlock, RawCallSnapshot, RawCodeHash, RawLog, RawReceipt, RawTransaction,
    default_database_url, list_canonical_raw_log_replay_inputs,
    list_canonical_raw_log_replay_inputs_for_block_hashes, upsert_chain_lineage_blocks,
    upsert_normalized_events, upsert_raw_blocks, upsert_raw_call_snapshots, upsert_raw_code_hashes,
    upsert_raw_logs, upsert_raw_receipts, upsert_raw_transactions,
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for audit tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_storage_audit_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for audit tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect audit test pool")?;

        crate::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for audit tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

fn timestamp(block_number: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000 + block_number)
        .expect("test timestamp must be valid")
}

fn lineage_block(
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    state: CanonicalityState,
) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: timestamp(block_number),
        logs_bloom: Some(vec![block_number as u8]),
        transactions_root: Some(format!("0xtxroot{block_number:02x}")),
        receipts_root: Some(format!("0xrcroot{block_number:02x}")),
        state_root: Some(format!("0xstroot{block_number:02x}")),
        canonicality_state: state,
    }
}

fn raw_block(block_hash: &str, parent_hash: Option<&str>, block_number: i64) -> RawBlock {
    RawBlock {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.map(str::to_owned),
        block_number,
        block_timestamp: timestamp(block_number),
        logs_bloom: Some(vec![block_number as u8]),
        transactions_root: Some(format!("0xtxroot{block_number:02x}")),
        receipts_root: Some(format!("0xrcroot{block_number:02x}")),
        state_root: Some(format!("0xstroot{block_number:02x}")),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_transaction(block_hash: &str, block_number: i64) -> RawTransaction {
    RawTransaction {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtx{block_number:02x}"),
        transaction_index: 0,
        from_address: "0x0000000000000000000000000000000000000001".to_owned(),
        to_address: Some("0x0000000000000000000000000000000000000002".to_owned()),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_receipt(block_hash: &str, block_number: i64) -> RawReceipt {
    RawReceipt {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtx{block_number:02x}"),
        transaction_index: 0,
        contract_address: None,
        status: Some(true),
        gas_used: Some(21_000),
        cumulative_gas_used: Some(21_000),
        logs_bloom: Some(vec![0xaa]),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_log(block_hash: &str, block_number: i64, log_index: i64) -> RawLog {
    raw_log_with_state(
        block_hash,
        block_number,
        log_index,
        CanonicalityState::Canonical,
    )
}

fn raw_log_with_state(
    block_hash: &str,
    block_number: i64,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> RawLog {
    RawLog {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtx{block_number:02x}"),
        transaction_index: 0,
        log_index,
        emitting_address: "0x0000000000000000000000000000000000000003".to_owned(),
        topics: vec!["0xtopic0".to_owned()],
        data: vec![0xde, 0xad],
        canonicality_state,
    }
}

fn raw_code_hash(block_hash: &str, block_number: i64) -> RawCodeHash {
    RawCodeHash {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        contract_address: "0x0000000000000000000000000000000000000003".to_owned(),
        code_hash: format!("0xcode{block_number:02x}"),
        code_byte_length: 123,
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn raw_call_snapshot(block_hash: &str, block_number: i64) -> RawCallSnapshot {
    RawCallSnapshot {
        chain_id: "eth-mainnet".to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        request_hash: format!("0xrequest{block_number:02x}"),
        request_payload: json!({
            "to": "0x0000000000000000000000000000000000000003",
            "data": "0x"
        }),
        response_hash: format!("0xresponse{block_number:02x}"),
        response_payload: json!({ "result": "0x01" }),
        canonicality_state: CanonicalityState::Canonical,
    }
}

fn normalized_event(block_hash: &str, block_number: i64, index: i64) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: format!("eth-mainnet:{block_hash}:event:{index}"),
        namespace: "ens".to_owned(),
        logical_name_id: Some("ens:alice.eth".to_owned()),
        resource_id: None,
        event_kind: "NameRegistered".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("eth-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(block_hash.to_owned()),
        transaction_hash: Some(format!("0xtx{block_number:02x}")),
        log_index: Some(index),
        raw_fact_ref: json!({ "raw_log": index }),
        derivation_kind: "log".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({ "name": "alice.eth" }),
    }
}

fn manifest_code_hash_drift_alert(event_identity: &str, address: &str) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "ManifestCodeHashDriftAlert".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 7,
        source_manifest_id: None,
        chain_id: Some("eth-mainnet".to_owned()),
        block_number: Some(123),
        block_hash: Some("0xalertblock".to_owned()),
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": 42,
            "contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000111",
            "address": address,
            "observed_block_number": 123,
            "observed_block_hash": "0xalertblock"
        }),
        derivation_kind: "manifest_alert".to_owned(),
        canonicality_state: CanonicalityState::Canonical,
        before_state: json!({}),
        after_state: json!({
            "alert_type": "manifest_code_hash_drift",
            "alert_status": "active",
            "chain": "eth-mainnet",
            "source_family": "ens_v1_registry_l1",
            "declaration_kind": "contract",
            "declaration_name": "registry",
            "contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000111",
            "address": address,
            "expected_code_hash": "0xexpected",
            "observed_code_hash": "0xobserved",
            "observed_code_byte_length": 512,
            "observed_block_number": 123,
            "observed_block_hash": "0xalertblock",
            "observed_canonicality_state": "canonical",
            "watched_source": "manifest_contract",
            "source_manifest_id": 42
        }),
    }
}

fn manifest_proxy_implementation_alert(event_identity: &str) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "ManifestProxyImplementationAlert".to_owned(),
        source_family: "ens_v1_wrapper_l1".to_owned(),
        manifest_version: 9,
        source_manifest_id: None,
        chain_id: Some("eth-mainnet".to_owned()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": 43,
            "discovery_edge_id": 99,
            "proxy_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000222",
            "implementation_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000333"
        }),
        derivation_kind: "manifest_alert".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "alert_type": "manifest_proxy_implementation_edge",
            "alert_status": "active",
            "chain": "eth-mainnet",
            "source_family": "ens_v1_wrapper_l1",
            "proxy_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000222",
            "proxy_address": "0xproxy",
            "implementation_contract_instance_id": "0e7ec7ac-e000-0000-0000-000000000333",
            "implementation_address": "0ximpl",
            "declaration_name": "name_wrapper",
            "role": "name_wrapper",
            "proxy_kind": "eip1967",
            "admission": "observed",
            "active_from_block_number": 120,
            "active_to_block_number": null,
            "provenance": {
                "slot": "eip1967.proxy.implementation"
            }
        }),
    }
}

fn ignored_manifest_alert_event() -> NormalizedEvent {
    NormalizedEvent {
        event_identity: "manifest_alert:ignored".to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "SourceManifestUpdated".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        manifest_version: 7,
        source_manifest_id: None,
        chain_id: Some("eth-mainnet".to_owned()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({}),
        derivation_kind: "manifest_alert".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({}),
    }
}

#[tokio::test]
async fn audit_reports_lineage_status_and_block_scoped_counts() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[lineage_block(
            "0xaaa",
            Some("0x999"),
            100,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_raw_blocks(database.pool(), &[raw_block("0xaaa", Some("0x999"), 100)]).await?;
    upsert_raw_transactions(database.pool(), &[raw_transaction("0xaaa", 100)]).await?;
    upsert_raw_receipts(database.pool(), &[raw_receipt("0xaaa", 100)]).await?;
    upsert_raw_logs(
        database.pool(),
        &[raw_log("0xaaa", 100, 0), raw_log("0xaaa", 100, 1)],
    )
    .await?;
    upsert_raw_code_hashes(database.pool(), &[raw_code_hash("0xaaa", 100)]).await?;
    upsert_raw_call_snapshots(database.pool(), &[raw_call_snapshot("0xaaa", 100)]).await?;
    upsert_normalized_events(
        database.pool(),
        &[
            normalized_event("0xaaa", 100, 0),
            normalized_event("0xaaa", 100, 1),
        ],
    )
    .await?;

    let inspection = inspect_block_canonicality(database.pool(), "eth-mainnet", "0xaaa").await?;

    assert_eq!(inspection.status, CanonicalityInspectionStatus::Canonical);
    assert_eq!(inspection.lineage_state, Some(CanonicalityState::Canonical));
    assert_eq!(inspection.parent_hash.as_deref(), Some("0x999"));
    assert_eq!(inspection.block_number, Some(100));
    assert_eq!(inspection.raw_fact_counts.raw_block_count, 1);
    assert_eq!(inspection.raw_fact_counts.raw_code_hash_count, 1);
    assert_eq!(inspection.raw_fact_counts.raw_transaction_count, 1);
    assert_eq!(inspection.raw_fact_counts.raw_receipt_count, 1);
    assert_eq!(inspection.raw_fact_counts.raw_log_count, 2);
    assert_eq!(inspection.raw_fact_counts.raw_call_snapshot_count, 1);
    assert_eq!(inspection.raw_fact_counts.total(), 7);
    assert_eq!(inspection.normalized_event_count, 2);

    database.cleanup().await
}

#[tokio::test]
async fn audit_reports_missing_block_status_without_counts() -> Result<()> {
    let database = TestDatabase::new().await?;

    let inspection =
        inspect_block_canonicality(database.pool(), "eth-mainnet", "0xmissing").await?;

    assert_eq!(inspection.status, CanonicalityInspectionStatus::Missing);
    assert_eq!(inspection.lineage_state, None);
    assert_eq!(inspection.parent_hash, None);
    assert_eq!(inspection.block_number, None);
    assert_eq!(inspection.raw_fact_counts, RawFactAuditCounts::default());
    assert_eq!(inspection.normalized_event_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn audit_range_reports_stored_lineage_order_and_orphaned_status() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0x010", None, 10, CanonicalityState::Canonical),
            lineage_block("0x011", Some("0x010"), 11, CanonicalityState::Orphaned),
            lineage_block("0x012", Some("0x011"), 12, CanonicalityState::Safe),
        ],
    )
    .await?;

    let inspections = inspect_canonicality_range(database.pool(), "eth-mainnet", 10, 12).await?;

    assert_eq!(
        inspections
            .iter()
            .map(|inspection| (
                inspection.block_hash.as_str(),
                inspection.status,
                inspection.block_number
            ))
            .collect::<Vec<_>>(),
        vec![
            ("0x010", CanonicalityInspectionStatus::Canonical, Some(10)),
            ("0x011", CanonicalityInspectionStatus::Orphaned, Some(11)),
            ("0x012", CanonicalityInspectionStatus::Safe, Some(12)),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn stored_lineage_range_lists_only_stored_rows_in_stable_order() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0x012b", Some("0x010"), 12, CanonicalityState::Safe),
            lineage_block("0x010", None, 10, CanonicalityState::Canonical),
            lineage_block("0x012a", Some("0x010"), 12, CanonicalityState::Orphaned),
        ],
    )
    .await?;

    let rows = list_stored_lineage_range(database.pool(), "eth-mainnet", 10, 12).await?;

    assert_eq!(
        rows.iter()
            .map(|row| (
                row.block_number,
                row.block_hash.as_str(),
                row.parent_hash.as_deref(),
                row.canonicality_state
            ))
            .collect::<Vec<_>>(),
        vec![
            (10, "0x010", None, CanonicalityState::Canonical),
            (12, "0x012a", Some("0x010"), CanonicalityState::Orphaned),
            (12, "0x012b", Some("0x010"), CanonicalityState::Safe),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn manifest_drift_audit_lists_stored_alert_observations() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[
            manifest_proxy_implementation_alert("manifest_alert:proxy"),
            manifest_code_hash_drift_alert("manifest_alert:code:z", "0xregistry-z"),
            ignored_manifest_alert_event(),
            manifest_code_hash_drift_alert("manifest_alert:code:a", "0xregistry-a"),
        ],
    )
    .await?;

    let inspection = list_manifest_drift_alert_observations(database.pool()).await?;

    assert_eq!(inspection.total_alert_count(), 3);
    assert_eq!(
        inspection
            .code_hash_drift_alerts
            .iter()
            .map(|alert| alert.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["manifest_alert:code:a", "manifest_alert:code:z"]
    );
    for alert in &inspection.code_hash_drift_alerts {
        assert_eq!(alert.alert_kind, ManifestDriftAlertKind::CodeHashDrift);
        assert_eq!(alert.alert_kind.event_kind(), "ManifestCodeHashDriftAlert");
        assert_eq!(alert.alert_kind.alert_type(), "manifest_code_hash_drift");
        assert_eq!(alert.source_family, "ens_v1_registry_l1");
        assert_eq!(alert.manifest_version, 7);
        assert_eq!(alert.chain_id.as_deref(), Some("eth-mainnet"));
        assert_eq!(alert.block_number, Some(123));
        assert_eq!(alert.block_hash.as_deref(), Some("0xalertblock"));
        assert_eq!(alert.canonicality_state, CanonicalityState::Canonical);
        assert_eq!(
            alert.alert_state["expected_code_hash"].as_str(),
            Some("0xexpected")
        );
        assert_eq!(
            alert.alert_state["observed_code_hash"].as_str(),
            Some("0xobserved")
        );
    }
    assert_eq!(
        inspection.code_hash_drift_alerts[0].alert_state["address"].as_str(),
        Some("0xregistry-a")
    );
    assert_eq!(
        inspection.code_hash_drift_alerts[1].alert_state["address"].as_str(),
        Some("0xregistry-z")
    );
    assert_eq!(inspection.proxy_implementation_alerts.len(), 1);
    let proxy_alert = &inspection.proxy_implementation_alerts[0];
    assert_eq!(
        proxy_alert.alert_kind,
        ManifestDriftAlertKind::ProxyImplementation
    );
    assert_eq!(proxy_alert.event_identity, "manifest_alert:proxy");
    assert_eq!(proxy_alert.source_family, "ens_v1_wrapper_l1");
    assert_eq!(proxy_alert.manifest_version, 9);
    assert_eq!(proxy_alert.canonicality_state, CanonicalityState::Finalized);
    assert_eq!(proxy_alert.alert_state["proxy_address"], "0xproxy");
    assert_eq!(proxy_alert.alert_state["implementation_address"], "0ximpl");
    assert_eq!(proxy_alert.raw_fact_ref["discovery_edge_id"], 99);

    database.cleanup().await
}

#[tokio::test]
async fn manifest_drift_audit_does_not_mutate_alert_observations() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_normalized_events(
        database.pool(),
        &[
            manifest_code_hash_drift_alert("manifest_alert:readonly:code", "0xregistry"),
            manifest_proxy_implementation_alert("manifest_alert:readonly:proxy"),
        ],
    )
    .await?;

    let before = list_manifest_drift_alert_observations(database.pool()).await?;
    let before_total = load_normalized_event_total(database.pool()).await?;

    let inspection = list_manifest_drift_alert_observations(database.pool()).await?;

    let after = list_manifest_drift_alert_observations(database.pool()).await?;
    let after_total = load_normalized_event_total(database.pool()).await?;
    assert_eq!(inspection, before);
    assert_eq!(after, before);
    assert_eq!(after_total, before_total);

    database.cleanup().await
}

#[tokio::test]
async fn raw_log_replay_inputs_include_only_canonical_persisted_facts() -> Result<()> {
    let database = TestDatabase::new().await?;

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            lineage_block("0x100", None, 100, CanonicalityState::Canonical),
            lineage_block("0x101", Some("0x100"), 101, CanonicalityState::Safe),
            lineage_block("0x102", Some("0x101"), 102, CanonicalityState::Finalized),
            lineage_block("0x103", Some("0x102"), 103, CanonicalityState::Observed),
            lineage_block("0x104", Some("0x103"), 104, CanonicalityState::Orphaned),
        ],
    )
    .await?;

    upsert_raw_logs(
        database.pool(),
        &[
            raw_log("0x100", 100, 0),
            raw_log("0x101", 101, 0),
            raw_log("0x102", 102, 0),
            raw_log("0x103", 103, 0),
            raw_log("0x104", 104, 0),
            raw_log_with_state("0x102", 102, 9, CanonicalityState::Orphaned),
        ],
    )
    .await?;

    let range_inputs =
        list_canonical_raw_log_replay_inputs(database.pool(), "eth-mainnet", 100, 104).await?;

    assert_eq!(
        range_inputs
            .iter()
            .map(|input| (
                input.block_hash.as_str(),
                input.lineage_canonicality_state,
                input.log_index,
                input.raw_canonicality_state
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "0x100",
                CanonicalityState::Canonical,
                0,
                CanonicalityState::Canonical
            ),
            (
                "0x101",
                CanonicalityState::Safe,
                0,
                CanonicalityState::Canonical
            ),
            (
                "0x102",
                CanonicalityState::Finalized,
                0,
                CanonicalityState::Canonical
            ),
        ]
    );

    let hash_inputs = list_canonical_raw_log_replay_inputs_for_block_hashes(
        database.pool(),
        "eth-mainnet",
        &["0x102".to_owned(), "0x100".to_owned(), "0x103".to_owned()],
    )
    .await?;

    assert_eq!(
        hash_inputs
            .iter()
            .map(|input| (
                input.block_number,
                input.block_hash.as_str(),
                input.log_index
            ))
            .collect::<Vec<_>>(),
        vec![(100, "0x100", 0), (102, "0x102", 0)]
    );

    database.cleanup().await
}

async fn load_normalized_event_total(pool: &PgPool) -> Result<i64> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS event_count
        FROM normalized_events
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to load normalized-event total")?;

    row.try_get("event_count")
        .context("missing normalized-event total")
}
