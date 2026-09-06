use bigname_adapters::schema_v2::seam::{LOG_INDEX_KEY, TRANSACTION_INDEX_KEY};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

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
async fn duplicate_identity_failure_rolls_back_and_accepts_corrected_retry() -> TestResult {
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
    let rows: Vec<(i64, String, serde_json::Value)> = sqlx::query_as(
        "SELECT normalized_event_id, event_identity, after_state
             FROM normalized_events ORDER BY normalized_event_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rows.iter().map(|row| row.1.as_str()).collect::<Vec<_>>(),
        ["duplicate", "not-attempted"]
    );
    assert!(rows[0].0 < rows[1].0);
    assert_eq!(rows[0].2, json!({"value": 1}));
    assert_eq!(rows[1].2, json!({"value": 3}));
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn stored_divergence_rolls_back_and_preserves_retained_identity() -> TestResult {
    let database = database("interpret_normalized_batch_stored_divergence").await?;
    let mut seed = database.pool().begin().await?;
    events(&mut seed, &[event("stored", json!({"value":1}))]).await?;
    seed.commit().await?;
    let retained: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(row) FROM normalized_events row")
            .fetch_one(database.pool())
            .await?;

    let mut transaction = database.pool().begin().await?;
    let error = events(
        &mut transaction,
        &[
            event("stored", json!({"value":2})),
            event("suffix", json!({"value":3})),
        ],
    )
    .await
    .expect_err("stored divergent identity must fail");
    assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
    assert!(error.to_string().contains("0=stored"));
    transaction.rollback().await?;
    let persisted: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(row) FROM normalized_events row")
            .fetch_all(database.pool())
            .await?;
    assert_eq!(persisted, [retained]);

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
    let rows: Vec<(i64, String, serde_json::Value)> = sqlx::query_as(
        "SELECT normalized_event_id, event_identity, after_state
             FROM normalized_events ORDER BY normalized_event_id",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!((rows[0].0, rows[0].1.as_str()), (1, "stored"));
    assert_eq!(rows[0].2, json!({"value": 1}));
    assert_eq!(rows[1].2, json!({"value": 3}));
    assert_eq!(rows[1].1, "suffix");
    assert_eq!(rows.len(), 2);
    assert!(rows[0].0 < rows[1].0);
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn values_boundary_persists_every_column_and_sequential_id() -> TestResult {
    let database = database("interpret_normalized_batch_values_boundary").await?;
    let mut empty = database.pool().begin().await?;
    events(&mut empty, &[]).await?;
    empty.commit().await?;
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
async fn missing_references_reject_whole_transaction_and_allow_corrected_retry() -> TestResult {
    for reference in ["name", "resource", "manifest", "lineage"] {
        let database = database("interpret_normalized_missing_reference").await?;
        let mut invalid = event("invalid", json!({"value": 1}));
        let mut corrected = invalid.clone();
        match reference {
            "name" => invalid.logical_name_id = Some("ens:missing".to_owned()),
            "resource" => invalid.resource_id = Some(sqlx::types::Uuid::from_u128(99)),
            "manifest" => {
                let id = sqlx::query_scalar::<_, i64>(
                    "INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                     chain_id, deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
                     VALUES (1, 'other', 'batch_test', 'batch-test', 'test', 'active', 'test', 'test', '{}')
                     RETURNING manifest_id",
                ).fetch_one(database.pool()).await?;
                invalid.source_manifest_id = Some(id);
                corrected.source_manifest_id = Some(id);
                corrected.namespace = "other".to_owned();
            }
            _ => invalid.block_hash = Some("0xmissing".to_owned()),
        }
        let prefix = if reference == "name" { 500 } else { 1 };
        let mut submitted = (0..prefix)
            .map(|i| event(&format!("prefix-{i}"), json!({"value": i})))
            .collect::<Vec<_>>();
        submitted.push(invalid);
        submitted.push(event("suffix", json!({"value": 2})));
        let mut transaction = database.pool().begin().await?;
        let error = events(&mut transaction, &submitted)
            .await
            .expect_err(reference);
        assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity, "{error}");
        assert!(
            error.to_string().contains("foreign key constraint"),
            "{error}"
        );
        assert!(
            error.to_string().contains(&format!("{prefix}=invalid")),
            "{error}"
        );
        transaction.rollback().await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?;
        assert_eq!(
            count, 0,
            "{reference}: earlier statements must also roll back"
        );
        submitted[prefix] = corrected;
        let mut retry = database.pool().begin().await?;
        events(&mut retry, &submitted).await?;
        retry.commit().await?;
        let rows: Vec<(i64, String, serde_json::Value)> = sqlx::query_as(
            "SELECT normalized_event_id, event_identity, after_state FROM normalized_events ORDER BY normalized_event_id",
        ).fetch_all(database.pool()).await?;
        assert_eq!(rows.len(), submitted.len());
        assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
        for (row, expected) in rows.iter().zip(&submitted) {
            assert_eq!(
                (&row.1, &row.2),
                (&expected.event_identity, &expected.after_state)
            );
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn compatible_replay_and_repeated_identities_preserve_order() -> TestResult {
    let database = database("interpret_normalized_mixed_success").await?;
    let mut seed = database.pool().begin().await?;
    events(&mut seed, &[event("stored", json!({"value": "stored"}))]).await?;
    seed.commit().await?;
    let keys = [
        "fresh-a", "stored", "repeat", "repeat", "fresh-b", "repeat", "fresh-c",
    ];
    let mut submitted = keys
        .iter()
        .map(|key| event(key, json!({"value": key})))
        .collect::<Vec<_>>();
    submitted[1].canonicality_state = "safe".to_owned();
    submitted[3].canonicality_state = "safe".to_owned();
    submitted[5].canonicality_state = "finalized".to_owned();
    let mut previous = None;
    for _ in 0..2 {
        let mut transaction = database.pool().begin().await?;
        events(&mut transaction, &submitted).await?;
        transaction.commit().await?;
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(
            "SELECT to_jsonb(row) - 'observed_at' FROM normalized_events row ORDER BY normalized_event_id",
        ).fetch_all(database.pool()).await?;
        let expected = [
            (1, "stored", "safe"),
            (2, "fresh-a", "canonical"),
            (4, "repeat", "finalized"),
            (6, "fresh-b", "canonical"),
            (8, "fresh-c", "canonical"),
        ];
        assert_eq!(rows.len(), expected.len());
        for (row, (id, key, state)) in rows.iter().zip(expected) {
            assert_eq!(row["normalized_event_id"], id);
            assert_eq!(row["event_identity"], key);
            assert_eq!(row["canonicality_state"], state);
            assert_eq!(row["after_state"], json!({"value": key}));
        }
        if let Some(previous) = previous {
            assert_eq!(rows, previous);
        }
        previous = Some(rows);
    }
    let mut sentinel = database.pool().begin().await?;
    events(&mut sentinel, &[event("sentinel", json!({}))]).await?;
    sentinel.commit().await?;
    let id: i64 = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events WHERE event_identity = 'sentinel'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        id, 16,
        "successful replay consumes the same IDs as successful insertion attempts"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn insert_error_identifies_only_the_attempted_slice() -> TestResult {
    let database = database("interpret_normalized_insert_context").await?;
    sqlx::query("DROP TABLE normalized_events")
        .execute(database.pool())
        .await?;
    let submitted = (0..501)
        .map(|index| event(&format!("insert-{index:03}"), json!({})))
        .collect::<Vec<_>>();
    let mut transaction = database.pool().begin().await?;
    let error = events(&mut transaction, &submitted)
        .await
        .expect_err("missing normalized table must fail INSERT");
    assert_eq!(error.kind(), crate::ErrorKind::Transient);
    let message = error.to_string();
    assert!(
        message.contains("failed to write normalized-event batch"),
        "{message}"
    );
    assert!(message.contains("0=insert-000"), "{message}");
    assert!(message.contains("499=insert-499"), "{message}");
    assert!(!message.contains("500=insert-500"), "{message}");
    assert!(!message.contains("501 total"), "{message}");
    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
