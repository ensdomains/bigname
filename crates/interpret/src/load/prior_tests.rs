use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::seam::{
    INTERPRETER_STATE_KEY, PREIMAGE_OBSERVATION_EVENT_KIND, REGISTRY_ANNOUNCEMENT_EDGE_KIND,
};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, DiscoveryRuleInput, ManifestInput, RawBlockInput,
    RawLogInput, StateCacheCapacity, begin_schema_v2_adapter_restore,
    prepare_schema_v2_batch_incremental,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::Uuid;
use time::{Duration, OffsetDateTime};

use super::restore_events;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const CHAIN: &str = "restore-order";
const REGISTRY: &str = "0x0000000000000000000000000000000000000042";
const SENDER: &str = "0x00000000000000000000000000000000000000a1";

sol! {
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
}

#[tokio::test]
async fn same_block_restore_replays_normalized_emission_order() -> TestResult {
    // Same-block states with distinct keys replay in emission order, so a block-derived expiry retirement emitted after its reservation is restored after it.
    let database = database().await?;
    let token = versioned_token("stale", 0);
    let reservation_resource = seed_retained_events(database.pool(), token).await?;

    let mut restore = begin_schema_v2_adapter_restore(
        CHAIN.to_owned(),
        vec![manifest()],
        discovery_rules(),
        vec![admission()],
        StateCacheCapacity::Unlimited,
    )?;
    let mut connection = database.pool().acquire().await?;
    assert_eq!(
        restore_events(&mut connection, CHAIN, 2, &mut restore).await?,
        3
    );
    let session = restore.finish(Some(OffsetDateTime::UNIX_EPOCH + Duration::SECOND));
    drop(connection);

    let encoded = ExpiryUpdated {
        tokenId: token,
        newExpiry: 100,
        sender: SENDER.parse::<Address>()?,
    }
    .encode_log_data();
    let block_timestamp = OffsetDateTime::UNIX_EPOCH + Duration::seconds(2);
    let prepared = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest()],
            discovery_rules: discovery_rules(),
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
    let revival = output.normalized_events.iter().find(|event| {
        event.event_kind == "RegistrationRenewed" && event.resource_id == Some(reservation_resource)
    });
    let event_kinds = output
        .normalized_events
        .iter()
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    let revival = revival.unwrap_or_else(|| {
        panic!("missing resource-scoped RegistrationRenewed; emitted {event_kinds:?}")
    });
    assert!(revival.logical_name_id.is_none());
    assert_eq!(revival.after_state["revived_from_expiry"], true);

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

async fn seed_retained_events(pool: &sqlx::PgPool, token: U256) -> TestResult<Uuid> {
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, 'block-1', 1, to_timestamp(1), 'canonical')",
    )
    .bind(CHAIN)
    .execute(pool)
    .await?;
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: vec![manifest()],
            discovery_rules: discovery_rules(),
            admissions: vec![admission()],
            prior_events: Vec::new(),
            blocks: vec![RawBlockInput {
                chain_id: CHAIN.to_owned(),
                block_hash: "block-1".to_owned(),
                block_number: 1,
                block_timestamp: OffsetDateTime::UNIX_EPOCH + Duration::SECOND,
                canonicality_state: "canonical".to_owned(),
            }],
            raw_logs: vec![raw_log(
                LabelReserved {
                    tokenId: token,
                    labelHash: keccak256(b"stale"),
                    label: "stale".to_owned(),
                    expiry: 1,
                    sender: SENDER.parse::<Address>()?,
                }
                .encode_log_data(),
                1,
            )],
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(Vec::new())?;
    assert_eq!(
        output
            .normalized_events
            .iter()
            .map(|event| event.event_kind.as_str())
            .collect::<Vec<_>>(),
        [
            "RegistrationReserved",
            "RegistrationReleased",
            PREIMAGE_OBSERVATION_EVENT_KIND
        ]
    );
    let reservation = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationReserved")
        .expect("reservation event");
    let retirement = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationReleased")
        .expect("immediate-expiry retirement event");
    assert_eq!(
        retirement.after_state["source_event"],
        "RegistryPathExpired"
    );
    assert_eq!(
        retirement.after_state["terminal_reason"],
        "registry_name_binding_expired"
    );
    assert_ne!(
        reservation.raw_fact_ref[INTERPRETER_STATE_KEY],
        retirement.raw_fact_ref[INTERPRETER_STATE_KEY]
    );
    let reservation_resource = reservation.resource_id.expect("reservation resource");
    assert_eq!(retirement.resource_id, Some(reservation_resource));

    for surface in &output.name_surfaces {
        sqlx::query(
            "INSERT INTO name_surfaces
                 (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
                  labelhashes, normalizer_version, visibility_state, normalization_errors,
                  deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
                  provenance, canonicality_state)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                     $16, $17::canonicality_state)",
        )
        .bind(&surface.logical_name_id)
        .bind(&surface.namespace)
        .bind(&surface.raw_name)
        .bind(&surface.raw_labels)
        .bind(&surface.dns_encoded_name)
        .bind(&surface.namehash)
        .bind(&surface.labelhashes)
        .bind(&surface.normalizer_version)
        .bind(&surface.visibility_state)
        .bind(&surface.normalization_errors)
        .bind(&surface.deactivation_reason)
        .bind(surface.deactivated_at)
        .bind(&surface.chain_id)
        .bind(&surface.block_hash)
        .bind(surface.block_number)
        .bind(&surface.provenance)
        .bind(&surface.canonicality_state)
        .execute(pool)
        .await?;
    }
    for resource in &output.resources {
        sqlx::query(
            "INSERT INTO resources
                 (resource_id, chain_id, block_hash, block_number, provenance,
                  canonicality_state)
             VALUES ($1, $2, $3, $4, $5, $6::canonicality_state)
             ON CONFLICT (resource_id) DO NOTHING",
        )
        .bind(resource.resource_id)
        .bind(&resource.chain_id)
        .bind(&resource.block_hash)
        .bind(resource.block_number)
        .bind(&resource.provenance)
        .bind(&resource.canonicality_state)
        .execute(pool)
        .await?;
    }
    for (ordinal, event) in output.normalized_events.into_iter().enumerate() {
        let normalized_event_id = 100 * i64::try_from(ordinal + 1)?;
        sqlx::query(
            "INSERT INTO normalized_events
                 (normalized_event_id, event_identity, namespace, logical_name_id, resource_id,
                  event_kind, source_family, manifest_version, chain_id, block_number, block_hash,
                  transaction_hash, transaction_index, log_index, raw_fact_ref, derivation_kind,
                  canonicality_state, before_state, after_state)
             OVERRIDING SYSTEM VALUE
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                     $16, $17::canonicality_state, $18, $19)",
        )
        .bind(normalized_event_id)
        .bind(event.event_identity)
        .bind(event.namespace)
        .bind(event.logical_name_id)
        .bind(event.resource_id)
        .bind(event.event_kind)
        .bind(event.source_family)
        .bind(event.manifest_version)
        .bind(event.chain_id)
        .bind(event.block_number)
        .bind(event.block_hash)
        .bind(event.transaction_hash)
        .bind(event.transaction_index)
        .bind(event.log_index)
        .bind(event.raw_fact_ref)
        .bind(event.derivation_kind)
        .bind(event.canonicality_state)
        .bind(event.before_state)
        .bind(event.after_state)
        .execute(pool)
        .await?;
    }
    Ok(reservation_resource)
}

fn raw_log(encoded: alloy_primitives::LogData, block_number: i64) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{block_number}"),
        block_number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + Duration::seconds(block_number),
        canonicality_state: "canonical".to_owned(),
        transaction_hash: format!("tx-{block_number}"),
        transaction_index: 0,
        log_index: 0,
        emitting_address: REGISTRY.to_owned(),
        topics: encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect(),
        data: encoded.data.to_vec(),
    }
}

fn manifest() -> ManifestInput {
    ManifestInput {
        manifest_id: 1,
        manifest_version: 1,
        namespace: "ens".to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        chain_id: CHAIN.to_owned(),
        deployment_label: "fixture".to_owned(),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        payload_json: json!({"abi":{"events":[
            {
                "name":"LabelReserved",
                "fragment":"event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)",
                "emitter_roles":["registry"],
                "normalized_events":["RegistrationReserved"]
            },
            {
                "name":"ExpiryUpdated",
                "fragment":"event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)",
                "emitter_roles":["registry"],
                "normalized_events":["ExpiryChanged", "RegistrationRenewed"]
            }
        ]}})
        .to_string(),
    }
}

fn discovery_rules() -> Vec<DiscoveryRuleInput> {
    vec![DiscoveryRuleInput {
        manifest_id: 1,
        edge_kind: "subregistry".to_owned(),
        from_role: Some("registry".to_owned()),
        admission: "linked_subregistry_event".to_owned(),
    }]
}

fn admission() -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: REGISTRY.to_owned(),
        contract_instance_id: Uuid::from_u128(1),
        source_manifest_id: Some(1),
        role: None,
        discovery_edge_kind: Some(REGISTRY_ANNOUNCEMENT_EDGE_KIND.to_owned()),
        discovery_from_contract_instance_id: Some(Uuid::from_u128(1)),
        discovery_observation_key: Some("registry-announcement:detached".to_owned()),
        active_from_block: Some(0),
        active_to_block: None,
    }
}

fn versioned_token(label: &str, version: u32) -> U256 {
    let mut bytes = *keccak256(label.as_bytes());
    bytes[28..].copy_from_slice(&version.to_be_bytes());
    U256::from_be_bytes(bytes)
}
