use super::*;
use alloy_primitives::{B256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, ManifestInput, PriorEventInput, RawBlockInput,
    RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use bigname_project::{BatchRequest, Engine, RunMode};
use time::OffsetDateTime;

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x0000000000000000000000000000000000000091";
const OWNER: &str = "0x00000000000000000000000000000000000000ab";
const NAME: &str = "child.parent.eth";
const BLOCK: i64 = 120;
sol! { event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner); }
fn namehash(name: &str) -> B256 {
    name.rsplit('.').fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
    })
}
fn manifests() -> Vec<ManifestInput> {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
    )
    .unwrap();
    let loaded = repository
        .manifests()
        .iter()
        .find(|loaded| {
            loaded.manifest.chain == CHAIN
                && loaded.manifest.source_family == "ens_v1_registry_l1"
                && loaded.version_tag == "v3"
        })
        .unwrap();
    vec![ManifestInput {
        manifest_id: 911,
        manifest_version: loaded.manifest.manifest_version as i64,
        namespace: loaded.manifest.namespace.clone(),
        source_family: loaded.manifest.source_family.clone(),
        chain_id: CHAIN.into(),
        deployment_label: loaded.manifest.deployment_epoch.clone(),
        normalizer_version: loaded.manifest.normalizer_version.clone(),
        payload_json: serde_json::to_string(&loaded.manifest).unwrap(),
    }]
}
// ENSRegistry.setSubnodeOwner emits NewOwner without creating a BaseRegistrar lease.
// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
#[tokio::test]
async fn registry_owned_subname_permission_handle_is_followable_from_real_new_owner() -> Result<()>
{
    let timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_120)?;
    let hash = format!("0x{BLOCK:064x}");
    let encoded = NewOwner {
        node: namehash("parent.eth"),
        label: keccak256("child"),
        owner: OWNER.parse()?,
    }
    .encode_log_data();
    let (output, _) = prepare_schema_v2_batch_incremental(BatchInput {
        chain_id: CHAIN.into(), manifests: manifests(), discovery_rules: vec![],
        admissions: vec![AddressAdmissionInput {
            address: REGISTRY.into(), contract_instance_id: Uuid::from_u128(911),
            source_manifest_id: Some(911), role: Some("registry".into()),
            discovery_edge_kind: None, discovery_from_contract_instance_id: None,
            discovery_observation_key: None, active_from_block: Some(0), active_to_block: None,
        }], prior_events: vec![PriorEventInput {
            retained_state_key: "known-subname-preimage".into(), chain_id: CHAIN.into(),
            namespace: "ens".into(), logical_name_id: Some(format!("ens:{:#x}", namehash(NAME))),
            resource_id: None, event_kind: "PreimageObserved".into(), source_family: "ens_v1_registry_l1".into(),
            manifest_version: 3, source_manifest_id: Some(911), emitting_address: None, state_scope: None,
            block_timestamp: Some(timestamp - time::Duration::seconds(1)),
            after_state: json!({"namehash": format!("{:#x}", namehash(NAME)),
                "raw_labels": ["child", "parent", "eth"], "visibility_state": "active", "surface_known": true}),
        }], blocks: vec![RawBlockInput {
            chain_id: CHAIN.into(), block_hash: hash.clone(), block_number: BLOCK,
            block_timestamp: timestamp, canonicality_state: "finalized".into(),
        }], raw_logs: vec![RawLogInput {
            chain_id: CHAIN.into(), block_hash: hash.clone(), block_number: BLOCK,
            block_timestamp: timestamp, canonicality_state: "finalized".into(),
            transaction_hash: format!("0x{:064x}", 912), transaction_index: 0, log_index: 0,
            emitting_address: REGISTRY.into(), topics: encoded.topics().iter().map(|t| format!("{t:#x}")).collect(),
            data: encoded.data.to_vec(),
        }],
    }, None, StateCacheCapacity::Unlimited)?.finish(vec![])?;
    assert!(output.decode_skips.is_empty());
    assert!(
        output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "RegistrationGranted")
    );
    let bound = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "SurfaceBound")
        .unwrap_or_else(|| panic!("real registry binding: {:?}", output.normalized_events));
    assert_eq!(bound.after_state["authority_kind"], "registry_only");
    let id = bound.resource_id.expect("registry resource");
    let logical = bound.logical_name_id.as_deref().unwrap();
    let database = TestDatabase::new_migrated().await?;
    database.seed_snapshot_selector_chain_positions(&json!({CHAIN: {
        "chain_id": CHAIN, "block_hash": hash, "block_number": BLOCK, "timestamp": "2023-11-14T22:15:20Z"
    }})).await?;
    // Retain only verified preimage knowledge. Ownership, bindings, permissions and the current
    // summary are generated from NewOwner by the adapter and Project.
    sqlx::query(
        "INSERT INTO bigname_phase.name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
        dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, chain_id,
        block_hash, block_number, canonicality_state)
        VALUES ($1, 'ens', $2, $3, $4, $5, $6, 'ensip15', 'active', $7, $8, $9, 'finalized')",
    )
    .bind(logical)
    .bind(NAME)
    .bind(vec!["child", "parent", "eth"])
    .bind(b"\x05child\x06parent\x03eth\x00".as_slice())
    .bind(format!("{:#x}", namehash(NAME)))
    .bind(
        ["child", "parent", "eth"]
            .map(|l| format!("{:#x}", keccak256(l)))
            .to_vec(),
    )
    .bind(CHAIN)
    .bind(&hash)
    .bind(BLOCK)
    .execute(&database.pool)
    .await?;
    persist(&database.pool, &output).await?;
    Engine::new(database.pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: BLOCK,
            affected_from_block: BLOCK,
            affected_to_block: BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    let row = bigname_storage::load_name_current(&database.pool, logical)
        .await?
        .expect("projected name");
    assert_eq!(row.resource_id, Some(id));
    assert!(row.declared_summary["registration"]["resource_id"].is_null());
    assert!(
        !bigname_storage::resource_is_registry_control_for_registrar_lease(&database.pool, id)
            .await?
    );
    let by_name =
        v2_permissions_payload_for_database(&database, &format!("/v1/permissions?name={NAME}"))
            .await?;
    assert!(!by_name["data"].as_array().unwrap().is_empty(), "{by_name}");
    let by_address = v2_address_names_payload_for_database(
        &database,
        &format!("/v1/addresses/{OWNER}/names?include=role_summary&namespace=ens"),
    )
    .await?;
    assert!(
        by_address["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["permission_resource_id"] == id.to_string()),
        "{by_address}"
    );
    let followed = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={id}"),
    )
    .await?;
    let mut expected_audit = by_name["data"].clone();
    for grant in expected_audit.as_array_mut().unwrap() {
        grant["authority_context"] = json!("resource_audit");
    }
    assert_eq!(
        followed["data"], expected_audit,
        "the advertised registry-owned subname handle must select its own grants"
    );
    // Losing a current row does not invent a registrar lease: the subname resource remains
    // auditable by the handle that was actually published.
    sqlx::query("DELETE FROM bigname_phase.name_current WHERE logical_name_id = $1")
        .bind(logical)
        .execute(&database.pool)
        .await?;
    let audit = v2_permissions_payload_for_database(
        &database,
        &format!("/v1/permissions?registration_id={id}"),
    )
    .await?;
    assert!(!audit["data"].as_array().unwrap().is_empty(), "{audit}");
    database.cleanup().await
}

pub async fn persist(pool: &PgPool, output: &BatchOutput) -> Result<()> {
    for manifest in manifests() {
        sqlx::query(
            "INSERT INTO manifest_versions (
                     manifest_id, manifest_version, namespace, source_family, chain_id,
                     deployment_label, rollout_status, normalizer_version, file_path,
                     manifest_payload
                 ) OVERRIDING SYSTEM VALUE
                 VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, $8, $9::jsonb)
                 ON CONFLICT (manifest_id) DO NOTHING",
        )
        .bind(manifest.manifest_id)
        .bind(manifest.manifest_version)
        .bind(&manifest.namespace)
        .bind(&manifest.source_family)
        .bind(&manifest.chain_id)
        .bind(&manifest.deployment_label)
        .bind(&manifest.normalizer_version)
        .bind(format!("fixture/handoff/{}.toml", manifest.source_family))
        .bind(&manifest.payload_json)
        .execute(pool)
        .await?;
    }
    for resource in &output.resources {
        sqlx::query(
            "INSERT INTO resources (
                     resource_id, token_lineage_id, chain_id, block_hash, block_number,
                     provenance, canonicality_state
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
                     logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
                     namehash, labelhashes, normalizer_version, visibility_state,
                     normalization_errors, deactivation_reason, deactivated_at, chain_id,
                     block_hash, block_number, provenance, canonicality_state
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
                     surface_binding_id, logical_name_id, resource_id, binding_kind,
                     authority_arm, active_from, chain_id, block_hash, block_number,
                     provenance, canonicality_state
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
                     source_family, manifest_version, source_manifest_id, chain_id,
                     block_number, block_hash, transaction_hash, transaction_index, log_index,
                     raw_fact_ref, derivation_kind, canonicality_state, before_state,
                     after_state, migration_correlation_ids, consumer_visibility
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
