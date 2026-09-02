use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, ManifestInput, RawBlockInput, RawLogInput,
    interpret_schema_v2_batch,
    seam::{LOG_INDEX_KEY, TRANSACTION_INDEX_KEY},
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use time::OffsetDateTime;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

sol! { event RegistrarNameRegistered(uint256 indexed id, address indexed owner, uint256 expires); }

async fn database(name: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    for sql in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::query(
        "INSERT INTO chain_lineage (
                 chain_id, block_hash, block_number, block_timestamp, canonicality_state
             ) VALUES ('batch-test', '0x01', 1, to_timestamp(1), 'canonical')",
    )
    .execute(database.pool())
    .await?;
    Ok(database)
}

#[tokio::test]
async fn registrar_only_batch_persists_resource_keyed_events_without_a_surface() -> TestResult {
    let database = database("interpret_registrar_only_without_surface").await?;
    let encoded = RegistrarNameRegistered {
        id: U256::from(1),
        owner: "0x0000000000000000000000000000000000000042".parse::<Address>()?,
        expires: U256::from(42),
    }
    .encode_log_data();
    let mut topics = encoded
        .topics()
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    topics[0] = format!(
        "{:#x}",
        keccak256("NameRegistered(uint256,address,uint256)")
    );
    let mut output = interpret_schema_v2_batch(BatchInput {
        chain_id: "batch-test".to_owned(),
        manifests: vec![ManifestInput { manifest_id: 1, manifest_version: 1, namespace: "ens".to_owned(), source_family: "ens_v1_registrar_l1".to_owned(), chain_id: "batch-test".to_owned(), deployment_label: "test".to_owned(), normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(), payload_json: json!({"abi":{"events":[{"name":"NameRegistered","fragment":"event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires)","emitter_roles":["registrar"],"normalized_events":["RegistrationGranted","ExpiryChanged","PermissionChanged","AuthorityEpochChanged"]}]}}).to_string() }],
        discovery_rules: vec![],
        admissions: vec![AddressAdmissionInput { address: "0x0000000000000000000000000000000000000042".to_owned(), contract_instance_id: sqlx::types::Uuid::from_u128(1), source_manifest_id: Some(1), role: Some("registrar".to_owned()), discovery_edge_kind: None, discovery_from_contract_instance_id: None, discovery_observation_key: None, active_from_block: Some(0), active_to_block: None }],
        prior_events: vec![], blocks: vec![RawBlockInput { chain_id: "batch-test".to_owned(), block_hash: "0x01".to_owned(), block_number: 1, block_timestamp: OffsetDateTime::UNIX_EPOCH, canonicality_state: "canonical".to_owned() }], raw_logs: vec![RawLogInput { chain_id: "batch-test".to_owned(), block_hash: "0x01".to_owned(), block_number: 1, block_timestamp: OffsetDateTime::UNIX_EPOCH, canonicality_state: "canonical".to_owned(), transaction_hash: "0xtx".to_owned(), transaction_index: 0, log_index: 0, emitting_address: "0x0000000000000000000000000000000000000042".to_owned(), topics, data: encoded.data.to_vec() }],
    })?;
    assert!(output.name_surfaces.is_empty());
    let resource_id = output.resources[0].resource_id;
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1, 'batch-test', '0x01', 1, 'canonical')").bind(resource_id).execute(database.pool()).await?;
    output
        .normalized_events
        .iter_mut()
        .for_each(|event| event.source_manifest_id = None);
    let mut transaction = database.pool().begin().await?;
    events(&mut transaction, &output.normalized_events).await?;
    transaction.commit().await?;
    database.cleanup().await?;
    Ok(())
}

fn event(identity: &str, after_state: serde_json::Value) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: None,
        event_kind: "RecordChanged".to_owned(),
        source_family: "batch_test".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: "batch-test".to_owned(),
        block_number: Some(1),
        block_hash: Some("0x01".to_owned()),
        transaction_hash: Some("0xtx".to_owned()),
        transaction_index: Some(0),
        log_index: Some(0),
        raw_fact_ref: json!({}),
        derivation_kind: "ens_v2_resolver".to_owned(),
        canonicality_state: "canonical".to_owned(),
        before_state: json!({}),
        after_state,
        migration_correlation_ids: vec![],
        consumer_visibility: "activated".to_owned(),
        before_state_explicit: false,
    }
}

#[tokio::test]
async fn duplicate_identity_failure_rolls_back_and_retry_keeps_sequence_semantics() -> TestResult {
    let database = database("interpret_normalized_batch_duplicate").await?;
    let mut transaction = database.pool().begin().await?;
    let error = events(
        &mut transaction,
        &[
            event("duplicate", json!({"value":1})),
            event("duplicate", json!({"value":2})),
            event("not-attempted", json!({"value":3})),
        ],
    )
    .await
    .expect_err("divergent duplicate identity must fail");
    assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("conflicting batch rows [1=duplicate]"),
        "{error}"
    );
    transaction.rollback().await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(count, 0, "failed writer transaction committed partial rows");

    let mut retry = database.pool().begin().await?;
    events(
        &mut retry,
        &[
            event("duplicate", json!({"value":1})),
            event("not-attempted", json!({"value":3})),
        ],
    )
    .await?;
    retry.commit().await?;
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT normalized_event_id, event_identity
             FROM normalized_events ORDER BY normalized_event_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rows,
        vec![(3, "duplicate".to_owned()), (4, "not-attempted".to_owned())]
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn stored_divergence_stops_before_suffix_and_retry_keeps_sequence_semantics() -> TestResult {
    let database = database("interpret_normalized_batch_stored_divergence").await?;
    let mut seed = database.pool().begin().await?;
    events(&mut seed, &[event("stored", json!({"value":1}))]).await?;
    seed.commit().await?;

    let mut transaction = database.pool().begin().await?;
    events(
        &mut transaction,
        &[
            event("stored", json!({"value":2})),
            event("suffix", json!({"value":3})),
        ],
    )
    .await
    .expect_err("stored divergent identity must fail before the suffix");
    transaction.rollback().await?;

    let mut retry = database.pool().begin().await?;
    events(
        &mut retry,
        &[
            event("stored", json!({"value":1})),
            event("suffix", json!({"value":3})),
        ],
    )
    .await?;
    retry.commit().await?;
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT normalized_event_id, event_identity
             FROM normalized_events ORDER BY normalized_event_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rows,
        vec![(1, "stored".to_owned()), (4, "suffix".to_owned())]
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn values_boundary_persists_every_column_and_sequential_id() -> TestResult {
    let database = database("interpret_normalized_batch_values_boundary").await?;
    sqlx::raw_sql(
        "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                 namehash, labelhashes, normalizer_version, visibility_state, chain_id,
                 block_hash, block_number, canonicality_state
             ) VALUES (
                 'ens:0xname', 'ens', 'name.eth', ARRAY['name','eth'], '\\x'::bytea,
                 '0xname', ARRAY['0xlabel','0xeth'], 'test', 'active', 'batch-test',
                 '0x01', 1, 'canonical'
             );
             INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number, canonicality_state
             ) VALUES (
                 '00000000-0000-0000-0000-000000000001', 'batch-test',
                 '0x01', 1, 'canonical'
             );",
    )
    .execute(database.pool())
    .await?;
    let submitted = (0_i64..501)
        .map(|index| {
            let mut event = event(&format!("boundary-{index:03}"), json!({"after": index}));
            event.logical_name_id = Some("ens:0xname".to_owned());
            event.resource_id = Some(sqlx::types::Uuid::from_u128(1));
            event.event_kind = if index % 2 == 0 {
                "RecordChanged".to_owned()
            } else {
                "PermissionChanged".to_owned()
            };
            event.source_family = format!("batch_test_{index}");
            event.manifest_version = index + 1;
            event.transaction_hash = Some(format!("0xtx{index:03}"));
            event.transaction_index = Some(index);
            event.log_index = Some(index + 1);
            event.raw_fact_ref = json!({"raw": index});
            event.derivation_kind = if index % 2 == 0 {
                "ens_v2_resolver".to_owned()
            } else {
                "ens_v2_permissions".to_owned()
            };
            event.canonicality_state = if index % 2 == 0 {
                "canonical".to_owned()
            } else {
                "safe".to_owned()
            };
            event.before_state = json!({"before": index});
            event.migration_correlation_ids = vec![format!("correlation-{index:03}")];
            event.consumer_visibility = if index % 2 == 0 {
                "activated".to_owned()
            } else {
                "candidate".to_owned()
            };
            event
        })
        .collect::<Vec<_>>();
    let expected = submitted
        .iter()
        .enumerate()
        .map(|(index, event)| {
            json!({
                "normalized_event_id": index + 1,
                "event_identity": event.event_identity,
                "namespace": event.namespace,
                "logical_name_id": event.logical_name_id,
                "resource_id": event.resource_id,
                "event_kind": event.event_kind,
                "source_family": event.source_family,
                "manifest_version": event.manifest_version,
                "source_manifest_id": event.source_manifest_id,
                "chain_id": event.chain_id,
                "block_number": event.block_number,
                "block_hash": event.block_hash,
                "transaction_hash": event.transaction_hash,
                (TRANSACTION_INDEX_KEY): event.transaction_index,
                (LOG_INDEX_KEY): event.log_index,
                "raw_fact_ref": event.raw_fact_ref,
                "derivation_kind": event.derivation_kind,
                "canonicality_state": event.canonicality_state,
                "before_state": event.before_state,
                "after_state": event.after_state,
                "migration_correlation_ids": event.migration_correlation_ids,
                "consumer_visibility": event.consumer_visibility,
            })
        })
        .collect::<Vec<_>>();

    let mut transaction = database.pool().begin().await?;
    events(&mut transaction, &submitted).await?;
    transaction.commit().await?;
    let persisted: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT to_jsonb(stored) - 'observed_at'
             FROM normalized_events stored
             ORDER BY normalized_event_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(persisted, expected);
    let observed_at_count: i64 =
        sqlx::query_scalar("SELECT count(DISTINCT observed_at) FROM normalized_events")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(observed_at_count, 1);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn constraint_error_names_writer_batch_and_every_submitted_row() -> TestResult {
    let database = database("interpret_normalized_batch_context").await?;
    let mut invalid = event("invalid", json!({}));
    invalid.event_kind = "NotAnEventKind".to_owned();
    let mut transaction = database.pool().begin().await?;
    let error = events(&mut transaction, &[event("valid", json!({})), invalid])
        .await
        .expect_err("invalid event kind must fail the batch");
    assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    let message = error.to_string();
    assert!(message.contains("normalized-event batch"), "{message}");
    assert!(message.contains("0=valid"), "{message}");
    assert!(message.contains("1=invalid"), "{message}");
    transaction.rollback().await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(database.pool())
        .await?;
    assert_eq!(count, 0);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn constraint_failure_stops_before_suffix_and_retry_keeps_sequence_semantics() -> TestResult {
    let database = database("interpret_normalized_batch_constraint_retry").await?;
    let mut invalid = event("invalid", json!({"value": 1}));
    invalid.logical_name_id = Some("ens:missing".to_owned());
    let mut transaction = database.pool().begin().await?;
    let error = events(
        &mut transaction,
        &[
            event("prefix", json!({"value": 0})),
            invalid,
            event("suffix", json!({"value": 2})),
        ],
    )
    .await
    .expect_err("the invalid row must stop before the suffix row");
    assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("normalized-event batch; batch rows [1=invalid]"),
        "{error}"
    );
    transaction.rollback().await?;

    let mut retry = database.pool().begin().await?;
    events(
        &mut retry,
        &[
            event("prefix", json!({"value": 0})),
            event("invalid", json!({"value": 1})),
            event("suffix", json!({"value": 2})),
        ],
    )
    .await?;
    retry.commit().await?;
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT normalized_event_id, event_identity
             FROM normalized_events ORDER BY normalized_event_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rows,
        vec![
            (3, "prefix".to_owned()),
            (4, "invalid".to_owned()),
            (5, "suffix".to_owned()),
        ]
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn preflight_error_caps_row_identities_and_reports_total() -> TestResult {
    let database = database("interpret_normalized_preflight_context_cap").await?;
    sqlx::query("DROP TABLE normalized_events")
        .execute(database.pool())
        .await?;
    let submitted = (0..501)
        .map(|index| event(&format!("preflight-{index:03}"), json!({})))
        .collect::<Vec<_>>();
    let mut transaction = database.pool().begin().await?;
    let error = events(&mut transaction, &submitted)
        .await
        .expect_err("missing normalized table must fail preflight");
    let message = error.to_string();
    assert!(message.contains("0=preflight-000"), "{message}");
    assert!(message.contains("499=preflight-499"), "{message}");
    assert!(!message.contains("500=preflight-500"), "{message}");
    assert!(message.contains("1 more; 501 total"), "{message}");
    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
