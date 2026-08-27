use bigname_adapters::schema_v2::{
    BatchInput, BatchOutput, ManifestInput, RawBlockInput, RawLogInput, interpret_schema_v2_batch,
};
use bigname_domain::normalization::ENS_NORMALIZER_VERSION;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use time::OffsetDateTime;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn malformed_adapter_output() -> TestResult<BatchOutput> {
    let chain_id = "decode-skip-chain";
    let block_hash = "block-42";
    let block_timestamp = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(42);
    let topic0 = format!(
        "{:#x}",
        alloy_primitives::keccak256("AddrChanged(bytes32,address)")
    );
    Ok(interpret_schema_v2_batch(BatchInput {
        chain_id: chain_id.to_owned(),
        manifests: vec![ManifestInput {
            manifest_id: 1,
            manifest_version: 1,
            namespace: "ens".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            chain_id: chain_id.to_owned(),
            deployment_label: "test".to_owned(),
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
            payload_json: serde_json::json!({
                "abi": {
                    "events": [{
                        "name": "AddrChanged",
                        "fragment": "event AddrChanged(bytes32 indexed node, address a)",
                        "emitter_roles": [],
                        "normalized_events": ["RecordChanged"]
                    }]
                }
            })
            .to_string(),
        }],
        discovery_rules: Vec::new(),
        admissions: Vec::new(),
        prior_events: Vec::new(),
        blocks: vec![RawBlockInput {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number: 42,
            block_timestamp,
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs: vec![RawLogInput {
            chain_id: chain_id.to_owned(),
            block_hash: block_hash.to_owned(),
            block_number: 42,
            block_timestamp,
            canonicality_state: "canonical".to_owned(),
            transaction_hash: "transaction-42".to_owned(),
            transaction_index: 0,
            log_index: 7,
            emitting_address: "0x0000000000000000000000000000000000000042".to_owned(),
            topics: vec![topic0],
            data: vec![0x01],
        }],
    })?)
}

#[tokio::test]
async fn redo_replay_of_the_same_adapter_batch_records_one_row() -> TestResult {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("interpret_decode_skip_replay").pool_max_connections(1),
    )
    .await?;
    for sql in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../../../schema-v2/baseline/10_phase_state.sql"),
        include_str!("../../../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../../../schema-v2/baseline/12_project_generation_failures.sql"),
        include_str!("../../../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::query("SELECT set_config('bigname.interpreter_content_hash', 'test-hash', false)")
        .execute(database.pool())
        .await?;
    let output = malformed_adapter_output()?;
    assert_eq!(output.decode_skips.len(), 1);
    let skip = &output.decode_skips[0];

    crate::write::batch(
        database.pool(),
        &skip.chain_id,
        None,
        false,
        false,
        0,
        &[],
        &output,
    )
    .await?;
    crate::write::batch(
        database.pool(),
        &skip.chain_id,
        Some((42, 42)),
        true,
        false,
        0,
        &[],
        &output,
    )
    .await?;

    let original_context = skip.decode_context.clone();
    let mut conflicting = output.clone();
    conflicting.decode_skips[0].decode_context = "conflicting replay context".to_owned();
    crate::write::batch(
        database.pool(),
        &skip.chain_id,
        None,
        false,
        false,
        0,
        &[],
        &conflicting,
    )
    .await?;

    let rows: Vec<(String, i64, String, i64, String, String)> = sqlx::query_as(
        "SELECT chain_id, block_number, transaction_hash, log_index,
                interpreter_content_hash, decode_context
         FROM interpret_decode_skips",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rows,
        vec![(
            skip.chain_id.clone(),
            skip.block_number,
            skip.transaction_hash.clone(),
            skip.log_index,
            "test-hash".to_owned(),
            original_context.clone(),
        )]
    );

    sqlx::query("SELECT set_config('bigname.interpreter_content_hash', 'rotated-hash', false)")
        .execute(database.pool())
        .await?;
    crate::write::batch(
        database.pool(),
        &skip.chain_id,
        None,
        false,
        false,
        0,
        &[],
        &conflicting,
    )
    .await?;
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT interpreter_content_hash, decode_context
         FROM interpret_decode_skips
         ORDER BY interpreter_content_hash",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rows,
        vec![
            (
                "rotated-hash".to_owned(),
                "conflicting replay context".to_owned(),
            ),
            ("test-hash".to_owned(), original_context),
        ]
    );

    database.cleanup().await?;
    Ok(())
}
