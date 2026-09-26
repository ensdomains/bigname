//! Raw ENSv1 logs replayed through the real schema v2 adapter onto the families_support fixture
//! (the shape of `handoff_scenario` in crates/project/tests/address_names_projection.rs), so a
//! fixture reads exactly the normalized events, identities and emission ordinals the adapter
//! writes. `persist` writes the columns Interpret writes for normalized events and lineage;
//! bindings are written for observe-only histories with no rebinding, without Interpret's
//! `active_to`, conflict handling and closure ordering (crates/interpret/src/write/identity.rs
//! :30-75, :257-300), and no manifest contract instance or preimage rows are written. A history
//! that rebinds a name needs the Interpret write path, not this one. The checked-in Sepolia
//! manifests (`manifests/sepolia`) are admitted at fixed fixture addresses; block hashes and
//! times are the fixture lineage's.
use alloy_primitives::{Address, B256, LogData, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Result, ensure};
use bigname_adapters::schema_v2::{
    AdapterSession, AddressAdmissionInput, BatchInput, BatchOutput, ManifestInput, RawBlockInput,
    RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use bigname_manifests::load_repository;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::support::{CHAIN, hash};

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event TransferSingle(
        address indexed operator,
        address indexed from,
        address indexed to,
        uint256 id,
        uint256 value
    );
    event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
}

pub const REGISTRY: &str = "0x0000000000000000000000000000000000000c01";
pub const REGISTRAR: &str = "0x0000000000000000000000000000000000000c02";
pub const WRAPPER: &str = "0x0000000000000000000000000000000000000c03";
/// The wrapped-registrar controller that calls `registerAndWrapETH2LD`; it emits nothing here.
pub const CONTROLLER: &str = "0x0000000000000000000000000000000000000c04";
const MANIFESTS: [(i64, &str, &str); 3] = [
    (931, "ens_v1_registry_l1", "registry"),
    (932, "ens_v1_registrar_l1", "registrar"),
    (933, "ens_v1_wrapper_l1", "name_wrapper"),
];

pub fn address(value: &str) -> Address {
    value.parse().expect("fixture address")
}

pub fn namehash(labels: &[&str]) -> B256 {
    labels.iter().rev().fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
    })
}

pub fn labelhash(label: &str) -> B256 {
    keccak256(label.as_bytes())
}

/// The DNS wire encoding of `labels`.
pub fn dns_encode(labels: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for label in labels {
        out.push(u8::try_from(label.len()).expect("a short label"));
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
    out
}

/// The families_support block clock.
pub fn block_time(block: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000 + block * 12).expect("fixture time")
}

/// One raw log: `log_index` in transaction `transaction` of `block`, emitted by `emitter`.
pub fn log(
    encoded: LogData,
    block: i64,
    transaction: i64,
    log_index: i64,
    emitter: &str,
) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.to_owned(),
        block_hash: hash(block),
        block_number: block,
        block_timestamp: block_time(block),
        canonicality_state: "canonical".to_owned(),
        transaction_hash: format!("0x{:064x}", 0x7000 + block * 16 + transaction),
        transaction_index: transaction,
        log_index,
        emitting_address: emitter.to_owned(),
        topics: encoded
            .topics()
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect(),
        data: encoded.data.to_vec(),
    }
}

pub fn manifests() -> Vec<ManifestInput> {
    let repository = load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )
    .expect("the checked-in Sepolia manifests load");
    MANIFESTS
        .into_iter()
        .map(|(manifest_id, family, _)| {
            let loaded = repository
                .manifests()
                .iter()
                .find(|loaded| {
                    loaded.manifest.chain == CHAIN
                        && loaded.manifest.source_family == family
                        && loaded.version_tag == "v1"
                })
                .unwrap_or_else(|| panic!("the checked-in {family} v1 manifest exists"));
            ManifestInput {
                manifest_id,
                manifest_version: i64::try_from(loaded.manifest.manifest_version)
                    .expect("manifest version fits i64"),
                namespace: loaded.manifest.namespace.clone(),
                source_family: loaded.manifest.source_family.clone(),
                chain_id: loaded.manifest.chain.clone(),
                deployment_label: loaded.manifest.deployment_epoch.clone(),
                normalizer_version: loaded.manifest.normalizer_version.clone(),
                payload_json: serde_json::to_string(&loaded.manifest)
                    .expect("the checked-in manifest serializes"),
            }
        })
        .collect()
}

fn admissions() -> Vec<AddressAdmissionInput> {
    MANIFESTS
        .into_iter()
        .zip([REGISTRY, REGISTRAR, WRAPPER])
        .map(|((manifest_id, _, role), address)| AddressAdmissionInput {
            address: address.to_owned(),
            contract_instance_id: Uuid::from_u128(u128::try_from(manifest_id).expect("id")),
            source_manifest_id: Some(manifest_id),
            role: Some(role.to_owned()),
            discovery_edge_kind: None,
            discovery_from_contract_instance_id: None,
            discovery_observation_key: None,
            active_from_block: Some(0),
            active_to_block: None,
        })
        .collect()
}

/// One block's raw logs as one adapter batch.
pub fn batch(block: i64, raw_logs: Vec<RawLogInput>) -> BatchInput {
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: admissions(),
        prior_events: Vec::new(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.to_owned(),
            block_hash: hash(block),
            block_number: block,
            block_timestamp: block_time(block),
            canonicality_state: "canonical".to_owned(),
        }],
        raw_logs,
    }
}

/// Interpret the batches in order through one adapter session.
pub fn interpret(batches: Vec<BatchInput>) -> Result<Vec<BatchOutput>> {
    let mut session: Option<AdapterSession> = None;
    let mut out = Vec::new();
    for input in batches {
        let (output, next) =
            prepare_schema_v2_batch_incremental(input, session, StateCacheCapacity::Unlimited)?
                .finish(Vec::new())?;
        ensure!(
            output.decode_skips.is_empty(),
            "every fixture log decodes: {:?}",
            output.decode_skips
        );
        session = Some(next);
        out.push(output);
    }
    Ok(out)
}

/// Persist adapter output the way Interpret does for the rows Project and the families read:
/// the manifest versions the events cite, token lineages, resources, name surfaces, surface
/// bindings with their closures, and normalized events with every column Interpret writes.
pub async fn persist(pool: &PgPool, output: &BatchOutput) -> Result<()> {
    for manifest in manifests() {
        // ON CONFLICT (manifest_id) arbitrates the primary key only; a second active version of
        // one namespace, family and chain would still fail the partial unique index
        // manifest_versions_one_active_idx (04_manifests.sql:42-44), so write one per family.
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
        .bind(format!("fixture/replay/{}.toml", manifest.source_family))
        .bind(&manifest.payload_json)
        .execute(pool)
        .await?;
    }
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
    for closure in &output.binding_closures {
        sqlx::query(
            "UPDATE surface_bindings
             SET active_to = $2
             WHERE logical_name_id = $1
               AND chain_id = $3
               AND authority_arm = $4
               AND ($5::uuid IS NULL OR surface_binding_id <> $5)
               AND (
                   block_number < $6
                   OR (
                       block_number = $6
                       AND (
                           COALESCE((provenance ->> 'transaction_index')::bigint, -1),
                           COALESCE((provenance ->> 'log_index')::bigint, -1)
                       ) < ($7, $8)
                   )
               )
               AND (active_to IS NULL OR active_to > $2)",
        )
        .bind(&closure.logical_name_id)
        .bind(closure.active_to)
        .bind(&closure.chain_id)
        .bind(&closure.authority_arm)
        .bind(closure.except_surface_binding_id)
        .bind(closure.block_number)
        .bind(closure.transaction_index)
        .bind(closure.log_index)
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

/// Interpret the batches and persist every output.
pub async fn replay(pool: &PgPool, batches: Vec<BatchInput>) -> Result<Vec<BatchOutput>> {
    let outputs = interpret(batches)?;
    for output in &outputs {
        persist(pool, output).await?;
    }
    Ok(outputs)
}

/// A wrapped .eth second-level name `label`, registered to the NameWrapper and wrapped for
/// `owner` in one transaction of `block`, in the order the contracts emit it: the registrar
/// mints the token to the wrapper, the registry names the wrapper owner of the node, the
/// registrar emits its NameRegistered, then the wrapper mints its ERC-1155 token and emits
/// NameWrapped (`registerAndWrapETH2LD` through `_wrapETH2LD`, which burns PARENT_CANNOT_CONTROL
/// and IS_DOT_ETH, then `_wrap`: the mint, then NameWrapped). The mint's operator is the caller,
/// the wrapped-registrar controller.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L305 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f).
pub fn wrapped_registration(label: &str, owner: &str, expires: u64, block: i64) -> BatchInput {
    let (label_hash, node) = (labelhash(label), namehash(&[label, "eth"]));
    let token = U256::from_be_bytes(label_hash.0);
    let wrapper = address(WRAPPER);
    let fuses = (1_u32 << 16) | (1_u32 << 17);
    batch(
        block,
        vec![
            log(
                Transfer {
                    from: Address::ZERO,
                    to: wrapper,
                    tokenId: token,
                }
                .encode_log_data(),
                block,
                0,
                0,
                REGISTRAR,
            ),
            log(
                NewOwner {
                    node: namehash(&["eth"]),
                    label: label_hash,
                    owner: wrapper,
                }
                .encode_log_data(),
                block,
                0,
                1,
                REGISTRY,
            ),
            log(
                NameRegistered {
                    id: token,
                    owner: wrapper,
                    expires: U256::from(expires),
                }
                .encode_log_data(),
                block,
                0,
                2,
                REGISTRAR,
            ),
            log(
                TransferSingle {
                    operator: address(CONTROLLER),
                    from: Address::ZERO,
                    to: address(owner),
                    id: U256::from_be_bytes(node.0),
                    value: U256::from(1_u64),
                }
                .encode_log_data(),
                block,
                0,
                3,
                WRAPPER,
            ),
            log(
                NameWrapped {
                    node,
                    name: dns_encode(&[label, "eth"]).into(),
                    owner: address(owner),
                    fuses,
                    expiry: expires + 7_776_000,
                }
                .encode_log_data(),
                block,
                0,
                4,
                WRAPPER,
            ),
        ],
    )
}

/// The wrapper's ERC-721-shaped token approval of `approved` for `label`.eth by `owner`.
pub fn wrapper_approval(label: &str, owner: &str, approved: &str, block: i64) -> BatchInput {
    batch(
        block,
        vec![log(
            Approval {
                owner: address(owner),
                approved: address(approved),
                tokenId: U256::from_be_bytes(namehash(&[label, "eth"]).0),
            }
            .encode_log_data(),
            block,
            0,
            0,
            WRAPPER,
        )],
    )
}

/// One ERC-1155 TransferSingle of `label`.eth from `from` to `to`.
pub fn wrapper_transfer(label: &str, from: &str, to: &str, block: i64) -> BatchInput {
    batch(
        block,
        vec![log(
            TransferSingle {
                operator: address(from),
                from: address(from),
                to: address(to),
                id: U256::from_be_bytes(namehash(&[label, "eth"]).0),
                value: U256::from(1_u64),
            }
            .encode_log_data(),
            block,
            0,
            0,
            WRAPPER,
        )],
    )
}

/// An unwrapped .eth second-level name `label` registered to `owner` in one transaction of
/// `block`, in the order the registrar's `_register` emits it: the ERC-721 mint, the registry's
/// NewOwner for the node, then the registrar's NameRegistered
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f).
pub fn registration(label: &str, owner: &str, expires: u64, block: i64) -> BatchInput {
    let label_hash = labelhash(label);
    let token = U256::from_be_bytes(label_hash.0);
    batch(
        block,
        vec![
            log(
                Transfer {
                    from: Address::ZERO,
                    to: address(owner),
                    tokenId: token,
                }
                .encode_log_data(),
                block,
                0,
                0,
                REGISTRAR,
            ),
            new_owner_log(label, owner, block, 1),
            log(
                NameRegistered {
                    id: token,
                    owner: address(owner),
                    expires: U256::from(expires),
                }
                .encode_log_data(),
                block,
                0,
                2,
                REGISTRAR,
            ),
        ],
    )
}

fn new_owner_log(label: &str, owner: &str, block: i64, log_index: i64) -> RawLogInput {
    log(
        NewOwner {
            node: namehash(&["eth"]),
            label: labelhash(label),
            owner: address(owner),
        }
        .encode_log_data(),
        block,
        0,
        log_index,
        REGISTRY,
    )
}

/// The registry's NewOwner of `label`.eth to `owner` alone in `block`, as the registrar's
/// `reclaim` writes it
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f).
pub fn new_owner(label: &str, owner: &str, block: i64) -> BatchInput {
    batch(block, vec![new_owner_log(label, owner, block, 0)])
}
