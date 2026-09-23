//! An ENSv2 role change above the published block regenerates the token without changing its
//! holder: the registry burns the token and mints the next version to the same owner.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L541-L553 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L569-L582 @ ens_v2_sepolia_20260916@366de741)
//! The events come from the ENSv2 adapter and the relation rows from Project.

use super::*;
use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, DiscoveryRuleInput, ManifestInput,
    RawBlockInput, RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use bigname_project::{BatchRequest, Engine, RunMode};

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x657ea849311d3d5823348dded7c2aaafb3ede09e";
const HOLDER: &str = "0x0000000000000000000000000000000000c0ffee";
const GRANTEE: &str = "0x0000000000000000000000000000000000c0ff0e";
const LABEL: &str = "regen";
const MANIFEST_ID: i64 = 951;
const REGISTERED: i64 = 120;
const REGENERATED: i64 = 121;

sol! {
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
}

fn token(version: u32) -> U256 {
    let mut bytes = *keccak256(LABEL.as_bytes());
    bytes[28..].copy_from_slice(&version.to_be_bytes());
    U256::from_be_bytes(bytes)
}

fn raw(data: alloy_primitives::LogData, block: i64, index: i64) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: format!("0xhistory{block}"),
        block_number: block,
        block_timestamp: timestamp(1_700_000_000 + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:064x}"),
        transaction_index: 0,
        log_index: index,
        emitting_address: REGISTRY.into(),
        topics: data.topics().iter().map(|t| format!("{t:#x}")).collect(),
        data: data.data.to_vec(),
    }
}

fn manifest() -> ManifestInput {
    manifest_and_rules().0
}

fn manifest_and_rules() -> (ManifestInput, Vec<DiscoveryRuleInput>) {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )
    .unwrap();
    let loaded = repository
        .manifests()
        .iter()
        .find(|loaded| {
            loaded.manifest.source_family == "ens_v2_registry_l1"
                && loaded.manifest.rollout_status == bigname_manifests::RolloutStatus::Active
        })
        .unwrap();
    let mut manifest = loaded.manifest.clone();
    manifest.chain = CHAIN.into();
    let rules = manifest
        .discovery_rules
        .iter()
        .map(|rule| DiscoveryRuleInput {
            manifest_id: MANIFEST_ID,
            edge_kind: rule.edge_kind.clone(),
            from_role: Some(rule.from_role.clone()),
            admission: rule.admission.clone(),
        })
        .collect();
    let input = ManifestInput {
        manifest_id: MANIFEST_ID,
        manifest_version: manifest.manifest_version as i64,
        namespace: manifest.namespace.clone(),
        source_family: manifest.source_family.clone(),
        chain_id: CHAIN.into(),
        deployment_label: manifest.deployment_epoch.clone(),
        normalizer_version: manifest.normalizer_version.clone(),
        payload_json: serde_json::to_string(&manifest).unwrap(),
    };
    (input, rules)
}

/// The registration at 120 and the role grant that regenerates the token at 121, in the order the
/// registry emits them.
fn interpret() -> Result<BatchOutput> {
    let holder: Address = HOLDER.parse()?;
    let zero = Address::ZERO;
    let resource = U256::from(0x5eed_u64);
    let logs = vec![
        raw(
            LabelRegistered {
                tokenId: token(0),
                labelHash: keccak256(LABEL.as_bytes()),
                label: LABEL.to_owned(),
                owner: holder,
                expiry: 1_900_000_000,
                sender: holder,
            }
            .encode_log_data(),
            REGISTERED,
            0,
        ),
        raw(
            TransferSingle {
                operator: holder,
                from: zero,
                to: holder,
                id: token(0),
                value: U256::from(1),
            }
            .encode_log_data(),
            REGISTERED,
            1,
        ),
        raw(
            TokenResource {
                tokenId: token(0),
                resource,
            }
            .encode_log_data(),
            REGISTERED,
            2,
        ),
        raw(
            EACRolesChanged {
                resource,
                account: GRANTEE.parse()?,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::from(1),
            }
            .encode_log_data(),
            REGENERATED,
            0,
        ),
        raw(
            TransferSingle {
                operator: holder,
                from: holder,
                to: zero,
                id: token(0),
                value: U256::from(1),
            }
            .encode_log_data(),
            REGENERATED,
            1,
        ),
        raw(
            TokenRegenerated {
                oldTokenId: token(0),
                newTokenId: token(1),
            }
            .encode_log_data(),
            REGENERATED,
            2,
        ),
        raw(
            TransferSingle {
                operator: holder,
                from: zero,
                to: holder,
                id: token(1),
                value: U256::from(1),
            }
            .encode_log_data(),
            REGENERATED,
            3,
        ),
    ];
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests: vec![manifest_and_rules().0],
            discovery_rules: manifest_and_rules().1,
            admissions: vec![AddressAdmissionInput {
                address: REGISTRY.into(),
                contract_instance_id: Uuid::from_u128(MANIFEST_ID as u128),
                source_manifest_id: Some(MANIFEST_ID),
                role: Some("registry".into()),
                discovery_edge_kind: None,
                discovery_from_contract_instance_id: None,
                discovery_observation_key: None,
                active_from_block: Some(0),
                active_to_block: None,
            }],
            prior_events: vec![],
            blocks: (REGISTERED..=REGENERATED)
                .map(|block| RawBlockInput {
                    chain_id: CHAIN.into(),
                    block_hash: format!("0xhistory{block}"),
                    block_number: block,
                    block_timestamp: timestamp(1_700_000_000 + block),
                    canonicality_state: "canonical".into(),
                })
                .collect(),
            raw_logs: logs,
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    Ok(output)
}

async fn persist(pool: &PgPool, output: &BatchOutput) -> Result<()> {
    let manifest = manifest();
    sqlx::query(
        "INSERT INTO manifest_versions (
             manifest_id, manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload
         ) OVERRIDING SYSTEM VALUE
         VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, 'fixture/regeneration.toml', $8::jsonb)",
    )
    .bind(manifest.manifest_id)
    .bind(manifest.manifest_version)
    .bind(&manifest.namespace)
    .bind(&manifest.source_family)
    .bind(&manifest.chain_id)
    .bind(&manifest.deployment_label)
    .bind(&manifest.normalizer_version)
    .bind(&manifest.payload_json)
    .execute(pool)
    .await?;
    for lineage in &output.token_lineages {
        sqlx::query(
            "INSERT INTO token_lineages (
                 token_lineage_id, chain_id, block_hash, block_number, provenance,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, $5, $6::canonicality_state)
             ON CONFLICT (token_lineage_id) DO NOTHING",
        )
        .bind(lineage.token_lineage_id)
        .bind(&lineage.chain_id)
        .bind(&lineage.block_hash)
        .bind(lineage.block_number)
        .bind(&lineage.provenance)
        .bind(&lineage.canonicality_state)
        .execute(pool)
        .await?;
    }
    for resource in &output.resources {
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, token_lineage_id, chain_id, block_hash, block_number, provenance,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7::canonicality_state)
             ON CONFLICT (resource_id) DO NOTHING",
        )
        .bind(resource.resource_id)
        .bind(resource.token_lineage_id)
        .bind(&resource.chain_id)
        .bind(&resource.block_hash)
        .bind(resource.block_number)
        .bind(&resource.provenance)
        .bind(&resource.canonicality_state)
        .execute(pool)
        .await?;
    }
    for surface in &output.name_surfaces {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
                 labelhashes, normalizer_version, visibility_state, normalization_errors,
                 deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
                 provenance, canonicality_state
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                 $17::canonicality_state
             )
             ON CONFLICT (logical_name_id) DO NOTHING",
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
    for binding in &output.surface_bindings {
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
                 active_from, chain_id, block_hash, block_number, provenance, canonicality_state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)",
        )
        .bind(binding.surface_binding_id)
        .bind(&binding.logical_name_id)
        .bind(binding.resource_id)
        .bind(&binding.binding_kind)
        .bind(&binding.authority_arm)
        .bind(binding.active_from)
        .bind(&binding.chain_id)
        .bind(&binding.block_hash)
        .bind(binding.block_number)
        .bind(&binding.provenance)
        .bind(&binding.canonicality_state)
        .execute(pool)
        .await?;
    }
    for event in &output.normalized_events {
        sqlx::query(
            "INSERT INTO normalized_events (
                 event_identity, namespace, logical_name_id, resource_id, event_kind,
                 source_family, manifest_version, source_manifest_id, chain_id, block_number,
                 block_hash, transaction_hash, transaction_index, log_index, raw_fact_ref,
                 derivation_kind, canonicality_state, before_state, after_state,
                 migration_correlation_ids, consumer_visibility
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                 $17::canonicality_state, $18, $19, $20, $21
             )",
        )
        .bind(&event.event_identity)
        .bind(&event.namespace)
        .bind(&event.logical_name_id)
        .bind(event.resource_id)
        .bind(&event.event_kind)
        .bind(&event.source_family)
        .bind(event.manifest_version)
        .bind(event.source_manifest_id)
        .bind(&event.chain_id)
        .bind(event.block_number)
        .bind(&event.block_hash)
        .bind(&event.transaction_hash)
        .bind(event.transaction_index)
        .bind(event.log_index)
        .bind(&event.raw_fact_ref)
        .bind(&event.derivation_kind)
        .bind(&event.canonicality_state)
        .bind(&event.before_state)
        .bind(&event.after_state)
        .bind(&event.migration_correlation_ids)
        .bind(&event.consumer_visibility)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn project_to(pool: &PgPool, target: i64) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: REGISTERED,
            affected_to_block: target,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

/// The holder's token-holder row: its cited event id, that event's kind, and its cited block.
async fn holder_row(pool: &PgPool) -> Result<Option<(i64, String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT event.normalized_event_id, event.event_kind,
                (anc.chain_positions ->> 'block_number')::bigint
         FROM bigname_phase.address_names_current anc
         JOIN normalized_events event
           ON event.normalized_event_id = (anc.provenance ->> 'normalized_event_id')::bigint
         WHERE anc.address = $1 AND anc.relation = 'token_holder'",
    )
    .bind(HOLDER)
    .fetch_optional(pool)
    .await?)
}

async fn owner_history_at(pool: &PgPool, block: i64) -> Result<Vec<String>> {
    let page = bigname_storage::load_address_history_page_for_relations(
        pool,
        HOLDER,
        None,
        Some(&[bigname_storage::AddressNameRelation::TokenHolder]),
        bigname_storage::HistoryScope::Both,
        true,
        None,
        50,
        bigname_storage::HistorySummaryMode::None,
        &bigname_storage::HistoryPageOptions {
            publication_block_bounds: Some(std::collections::BTreeMap::from([(
                CHAIN.to_owned(),
                block,
            )])),
            block_window: Some(bigname_storage::HistoryBlockWindow {
                ranges: vec![bigname_storage::ChainBlockRange {
                    chain_id: CHAIN.to_owned(),
                    from_block: None,
                    to_block: Some(block),
                }],
            }),
            ..bigname_storage::HistoryPageOptions::default()
        },
        false,
    )
    .await?;
    Ok(page
        .rows
        .into_iter()
        .map(|row| {
            format!(
                "{}@{}",
                row.event_kind,
                row.block_number.unwrap_or_default()
            )
        })
        .collect())
}

#[tokio::test]
async fn role_change_above_the_bound_keeps_the_token_holder_at_the_bound() -> Result<()> {
    let output = interpret()?;
    // The regeneration is interpreted as a `TokenRegenerated` row beside the role grant; the
    // burn and the mint to the same holder write no transfer rows.
    let regenerated_kinds = output
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(REGENERATED))
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    assert!(
        regenerated_kinds.contains(&"TokenRegenerated"),
        "{regenerated_kinds:?}"
    );
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, REGISTERED..=REGENERATED).await?;
    persist(&database.pool, &output).await?;

    project_to(&database.pool, REGISTERED).await?;
    let registered = holder_row(&database.pool).await?;
    let held = owner_history_at(&database.pool, REGISTERED).await?;
    project_to(&database.pool, REGENERATED).await?;
    let regenerated = holder_row(&database.pool).await?;
    let after = owner_history_at(&database.pool, REGISTERED).await?;
    database.cleanup().await?;

    let (_, kind, block) = registered
        .as_ref()
        .context("the registration projects a holder row")?;
    assert_eq!((kind.as_str(), *block), ("RegistrationGranted", REGISTERED));
    assert_eq!(
        regenerated, registered,
        "the regeneration moved the holder row's citation"
    );
    assert!(
        held.iter()
            .any(|row| row.starts_with("RegistrationGranted@")),
        "the registration is in the holder's history at the bound: {held:?}"
    );
    assert_eq!(
        after, held,
        "a role change above the bound changed the holder's history at the bound"
    );
    Ok(())
}
