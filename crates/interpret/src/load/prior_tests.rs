use alloy_primitives::{Address, B256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::seam::STATE_SCOPE_KEY;
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, ManifestInput, RawBlockInput, RawLogInput,
    StateCacheCapacity, begin_schema_v2_adapter_restore, prepare_schema_v2_batch_incremental,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::Uuid;
use time::{Duration, OffsetDateTime};

use super::restore_events;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const CHAIN: &str = "restore-order";
const REGISTRY: &str = "0x0000000000000000000000000000000000000042";
const OWNER_A: &str = "0x00000000000000000000000000000000000000a1";
const OWNER_B: &str = "0x00000000000000000000000000000000000000b2";
const OWNER_C: &str = "0x00000000000000000000000000000000000000c3";

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
}

#[tokio::test]
async fn same_block_restore_replays_normalized_emission_order() -> TestResult {
    let database = database().await?;
    let label = keccak256(b"ordered");
    let child = child_node(B256::ZERO, label);
    seed_retained_events(database.pool(), &child).await?;

    let mut restore = begin_schema_v2_adapter_restore(
        CHAIN.to_owned(),
        vec![manifest()],
        Vec::new(),
        vec![admission()],
        StateCacheCapacity::Unlimited,
    )?;
    let mut connection = database.pool().acquire().await?;
    assert_eq!(
        restore_events(&mut connection, CHAIN, 2, &mut restore).await?,
        2
    );
    let session = restore.finish(Some(OffsetDateTime::UNIX_EPOCH + Duration::SECOND));
    drop(connection);

    let encoded = NewOwner {
        node: B256::ZERO,
        label,
        owner: OWNER_C.parse::<Address>()?,
    }
    .encode_log_data();
    let block_timestamp = OffsetDateTime::UNIX_EPOCH + Duration::seconds(2);
    let prepared = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest()],
            discovery_rules: Vec::new(),
            admissions: vec![admission()],
            prior_events: Vec::new(),
            blocks: vec![RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "block-2".to_owned(),
                block_number: 2,
                block_timestamp,
                canonicality_state: "canonical".to_owned(),
            }],
            raw_logs: vec![RawLogInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "block-2".to_owned(),
                block_number: 2,
                block_timestamp,
                canonicality_state: "canonical".to_owned(),
                transaction_hash: "tx-2".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: REGISTRY.to_owned(),
                topics: encoded
                    .topics()
                    .iter()
                    .map(|topic| format!("{topic:#x}"))
                    .collect(),
                data: encoded.data.to_vec(),
            }],
        },
        Some(session),
        StateCacheCapacity::Unlimited,
    )?;
    let (output, _) = prepared.finish(Vec::new())?;
    let owner_change = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "AuthorityTransferred"
                && event.after_state["source_event"] == "NewOwner"
        })
        .expect("later owner change");
    assert_eq!(owner_change.before_state["owner"], OWNER_B);

    database.cleanup().await?;
    Ok(())
}

async fn database() -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new("interpret_restore_order")).await?;
    for statement in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(statement).execute(database.pool()).await?;
    }
    Ok(database)
}

async fn seed_retained_events(pool: &sqlx::PgPool, child: &str) -> TestResult {
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, 'block-1', 1, to_timestamp(1), 'canonical')",
    )
    .bind(CHAIN)
    .execute(pool)
    .await?;
    let statement = format!(
        "INSERT INTO normalized_events
             (normalized_event_id, event_identity, namespace, event_kind, source_family,
              manifest_version, chain_id, block_number, block_hash, transaction_hash,
              transaction_index, log_index, raw_fact_ref, derivation_kind,
              canonicality_state, after_state)
         OVERRIDING SYSTEM VALUE
         SELECT id, identity, 'ens', 'AuthorityTransferred', 'ens_v1_registry_l1',
                1, $1, 1, 'block-1', transaction_hash, transaction_index, log_index,
                jsonb_build_object('{STATE_SCOPE_KEY}', 'retained-owner'),
                'raw_log_preimage_observation', 'canonical',
                jsonb_build_object(
                    'source_event', 'NewOwner', 'child_node', $2::text,
                    'owner', owner, 'authority_kind', 'registry_only',
                    'authority_key', 'registry-only:restore-order:' || $2::text
                )
         FROM (VALUES
             (100::bigint, 'owner-a', 'tx-a', 1::bigint, 1::bigint, $3::text),
             (200::bigint, 'owner-b', 'tx-b', 0::bigint, 0::bigint, $4::text)
         ) retained(id, identity, transaction_hash, transaction_index, log_index, owner)"
    );
    sqlx::query(&statement)
        .bind(CHAIN)
        .bind(child)
        .bind(OWNER_A)
        .bind(OWNER_B)
        .execute(pool)
        .await?;
    Ok(())
}

fn manifest() -> ManifestInput {
    ManifestInput {
        manifest_id: 1,
        manifest_version: 1,
        namespace: "ens".to_owned(),
        source_family: "ens_v1_registry_l1".to_owned(),
        chain_id: CHAIN.to_owned(),
        deployment_label: "fixture".to_owned(),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        payload_json: json!({"abi":{"events":[{
            "name":"NewOwner",
            "fragment":"event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            "emitter_roles":["registry"],
            "normalized_events":["SubregistryChanged", "AuthorityTransferred"]
        }]}})
        .to_string(),
    }
}

fn admission() -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: REGISTRY.to_owned(),
        contract_instance_id: Uuid::from_u128(1),
        source_manifest_id: Some(1),
        role: Some("registry".to_owned()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    }
}

fn child_node(parent: B256, label: B256) -> String {
    let mut input = [0_u8; 64];
    input[..32].copy_from_slice(parent.as_slice());
    input[32..].copy_from_slice(label.as_slice());
    format!("{:#x}", keccak256(input))
}
