use alloy_sol_types::SolEvent;
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, DiscoveryRuleInput, ManifestInput,
    RawBlockInput, RawLogInput, interpret_schema_v2_batch,
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

async fn decode_skip_database(name: &str) -> TestResult<TestDatabase> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(1)).await?;
    database.create_phase_schema().await?;
    for sql in [
        include_str!("../../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../../storage/schema/baseline/02_raw_facts.sql"),
        include_str!("../../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../../../storage/schema/baseline/06_projections.sql"),
        include_str!("../../../../storage/schema/baseline/07_labels.sql"),
        include_str!("../../../../storage/schema/baseline/08_heartbeats.sql"),
        include_str!("../../../../storage/schema/baseline/09_divergence.sql"),
        include_str!("../../../../storage/schema/baseline/10_phase_state.sql"),
        include_str!("../../../../storage/schema/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../../storage/schema/baseline/12_project_generation_failures.sql"),
        include_str!("../../../../storage/schema/baseline/13_interpret_decode_skips.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    sqlx::query("SELECT set_config('bigname.interpreter_content_hash', 'test-hash', false)")
        .execute(database.pool())
        .await?;
    Ok(database)
}

#[tokio::test]
async fn redo_replay_of_the_same_adapter_batch_records_one_row() -> TestResult {
    let database = decode_skip_database("interpret_decode_skip_replay").await?;
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

alloy_sol_types::sol! {
    event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
}

/// An ENSv2 registry admitted by its own announcement, which no manifest declares, emits a
/// `TransferBatch` whose ids and values differ in length.
fn inconsistent_adapter_output() -> TestResult<BatchOutput> {
    let chain_id = "inconsistent-skip-chain";
    let block_hash = "block-42";
    let block_timestamp = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(42);
    let registry = "0x0000000000000000000000000000000000000042";
    let instance = sqlx::types::Uuid::from_u128(0x42);
    let account = alloy_primitives::Address::repeat_byte(0x51);
    let encoded = TransferBatch {
        operator: account,
        from: account,
        to: alloy_primitives::Address::repeat_byte(0x52),
        ids: vec![
            alloy_primitives::U256::from(7),
            alloy_primitives::U256::from(8),
        ],
        values: vec![alloy_primitives::U256::from(1)],
    }
    .encode_log_data();
    Ok(interpret_schema_v2_batch(BatchInput {
        chain_id: chain_id.to_owned(),
        manifests: vec![ManifestInput {
            manifest_id: 1,
            manifest_version: 1,
            namespace: "ens".to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            chain_id: chain_id.to_owned(),
            deployment_label: "test".to_owned(),
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
            payload_json: serde_json::json!({
                "abi": {
                    "events": [{
                        "name": "TransferBatch",
                        "fragment": "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)",
                        "emitter_roles": ["registry"],
                        "normalized_events": ["TokenControlTransferred"]
                    }]
                }
            })
            .to_string(),
        }],
        discovery_rules: vec![DiscoveryRuleInput {
            manifest_id: 1,
            edge_kind: "registry_announcement".to_owned(),
            from_role: Some("registry".to_owned()),
            admission: "reachable_from_root".to_owned(),
        }],
        admissions: vec![AddressAdmissionInput {
            address: registry.to_owned(),
            contract_instance_id: instance,
            source_manifest_id: Some(1),
            role: Some("registry".to_owned()),
            discovery_edge_kind: Some("registry_announcement".to_owned()),
            discovery_from_contract_instance_id: Some(instance),
            discovery_observation_key: Some("registry-announcement:self".to_owned()),
            active_from_block: Some(0),
            active_to_block: None,
        }],
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
            log_index: 3,
            emitting_address: registry.to_owned(),
            topics: encoded
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect(),
            data: encoded.data.to_vec(),
        }],
    })?)
}

#[tokio::test]
async fn a_redo_of_an_inconsistent_skip_records_one_row() -> TestResult {
    let database = decode_skip_database("interpret_inconsistent_skip_replay").await?;
    let output = inconsistent_adapter_output()?;
    assert_eq!(output.decode_skips.len(), 1);
    let skip = &output.decode_skips[0];
    assert_eq!(
        skip.inconsistency_site,
        Some("transfer_batch_length_mismatch")
    );
    assert_eq!(
        skip.decode_context,
        "inconsistent transfer_batch_length_mismatch: TransferBatch ids and values differ in length"
    );

    for (redo_range, prepare_redo) in [(None, false), (Some((42, 42)), true)] {
        crate::write::batch(
            database.pool(),
            &skip.chain_id,
            redo_range,
            prepare_redo,
            false,
            0,
            &[],
            &output,
        )
        .await?;
    }

    let rows: Vec<(String, i64, String, i64, String, String, String)> = sqlx::query_as(
        "SELECT chain_id, block_number, transaction_hash, log_index, emitting_address,
                interpreter_content_hash, decode_context
         FROM interpret_decode_skips",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rows,
        vec![(
            skip.chain_id.clone(),
            42,
            "transaction-42".to_owned(),
            3,
            skip.emitting_address.clone(),
            "test-hash".to_owned(),
            skip.decode_context.clone(),
        )]
    );

    database.cleanup().await?;
    Ok(())
}
