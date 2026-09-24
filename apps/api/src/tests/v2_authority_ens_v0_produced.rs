//! `ens_v0` end to end: raw ENSv1 registry and controller logs through the adapter, persisted as
//! Interpret persists them, projected by Project, and read through the public routes. The name
//! is recorded only in the 2017 registry until the current registry writes its record
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L46 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f).

use super::v2_history_bounded_rebinding::{
    REGISTRAR_MANIFEST, REGISTRY_MANIFEST, manifests, persist, persist_with_manifests,
};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AdapterSession, AddressAdmissionInput, BatchInput, BatchOutput, ManifestInput, RawBlockInput,
    RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use bigname_project::{BatchRequest, Engine, Marker, RunMode};

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const OLD_REGISTRY: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";
const CONTROLLER: &str = "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5";
const OWNER: &str = "0x00000000000000000000000000000000000000ab";
const NEXT_OWNER: &str = "0x00000000000000000000000000000000000000cd";
const NAME: &str = "pointer.eth";
/// The 2017 registry records `pointer.eth` and `hidden.eth`.
const OLD_RECORD: i64 = 130;
/// The legacy controller's label-bearing renewal materializes `pointer.eth`.
const SURFACED: i64 = 131;
/// The current registry writes `pointer.eth`'s first record, for the same owner.
const HANDOFF: i64 = 132;
/// A later current-registry `Transfer`.
const MOVED: i64 = 133;
/// The current registry's owner is cleared.
const CLEARED: i64 = 134;

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event Transfer(bytes32 indexed node, address owner);
    event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
}

fn namehash(name: &str) -> B256 {
    name.rsplit('.').fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
    })
}

fn raw(data: alloy_primitives::LogData, block: i64, index: i64, emitter: &str) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: format!("0xhistory{block}"),
        block_number: block,
        block_timestamp: timestamp(1_700_000_000 + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:064x}"),
        transaction_index: 0,
        log_index: index,
        emitting_address: emitter.into(),
        topics: data.topics().iter().map(|t| format!("{t:#x}")).collect(),
        data: data.data.to_vec(),
    }
}

fn admission(manifest_id: i64, instance: u128, role: &str, address: &str) -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: address.into(),
        contract_instance_id: Uuid::from_u128(instance),
        source_manifest_id: Some(manifest_id),
        role: Some(role.into()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    }
}

fn new_owner(label: &str, owner: &str, block: i64, index: i64, emitter: &str) -> RawLogInput {
    raw(
        NewOwner {
            node: namehash("eth"),
            label: keccak256(label.as_bytes()),
            owner: owner.parse().unwrap(),
        }
        .encode_log_data(),
        block,
        index,
        emitter,
    )
}

fn transfer(owner: &str, block: i64) -> RawLogInput {
    raw(
        Transfer {
            node: namehash(NAME),
            owner: owner.parse().unwrap(),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRY,
    )
}

/// Every block's logs, in emission order.
fn logs(block: i64) -> Vec<RawLogInput> {
    match block {
        OLD_RECORD => vec![
            new_owner("pointer", OWNER, block, 0, OLD_REGISTRY),
            new_owner("hidden", OWNER, block, 1, OLD_REGISTRY),
        ],
        SURFACED => vec![raw(
            NameRenewed {
                name: "pointer".into(),
                label: keccak256(b"pointer"),
                cost: U256::from(1),
                expires: U256::from(4_102_444_800_u64),
            }
            .encode_log_data(),
            block,
            0,
            CONTROLLER,
        )],
        HANDOFF => vec![new_owner("pointer", OWNER, block, 0, REGISTRY)],
        MOVED => vec![transfer(NEXT_OWNER, block)],
        CLEARED => vec![transfer(&Address::ZERO.to_string(), block)],
        _ => Vec::new(),
    }
}

fn interpret(block: i64, session: Option<AdapterSession>) -> Result<(BatchOutput, AdapterSession)> {
    let input = BatchInput {
        chain_id: CHAIN.into(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: vec![
            admission(REGISTRY_MANIFEST, 971, "registry", REGISTRY),
            admission(REGISTRY_MANIFEST, 972, "registry_old", OLD_REGISTRY),
            admission(
                REGISTRAR_MANIFEST,
                973,
                "legacy_registrar_controller",
                CONTROLLER,
            ),
        ],
        prior_events: Vec::new(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.into(),
            block_hash: format!("0xhistory{block}"),
            block_number: block,
            block_timestamp: timestamp(1_700_000_000 + block),
            canonicality_state: "canonical".into(),
        }],
        raw_logs: logs(block),
    };
    let (output, session) =
        prepare_schema_v2_batch_incremental(input, session, StateCacheCapacity::Unlimited)?
            .finish(Vec::new())?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    Ok((output, session))
}

async fn project(pool: &PgPool, target: i64, resume: Option<i64>) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: resume.map_or(0, |previous| previous + 1),
            affected_to_block: target,
            resume_current: resume.map(|number| Marker {
                number,
                hash: format!("0xhistory{number}"),
            }),
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

async fn authority_selections(pool: &PgPool) -> Result<Vec<(String, Value)>> {
    Ok(sqlx::query_as(
        "SELECT raw_name, provenance -> 'authority_selection'
         FROM bigname_phase.name_current WHERE namespace = 'ens' ORDER BY raw_name",
    )
    .fetch_all(pool)
    .await?)
}

/// The product responses for `pointer.eth` that carry `authority`.
async fn product_reads(database: &TestDatabase) -> Result<Vec<Value>> {
    let detail = v2_get_json(database, &format!("/v1/names/{NAME}")).await?;
    let lookup = v2_lookup_json(
        database,
        json!({"profile": "detail", "inputs": [{"id": "name", "name": NAME}]}),
    )
    .await?;
    let mut reads = vec![detail["data"].clone(), lookup["data"][0]["record"].clone()];
    for owner in [OWNER, NEXT_OWNER] {
        let names = v2_address_names_payload_for_database(
            database,
            &format!("/v1/addresses/{owner}/names"),
        )
        .await?;
        reads.extend(
            names["data"]
                .as_array()
                .expect("address names")
                .iter()
                .filter(|row| row["name"] == NAME)
                .cloned(),
        );
    }
    Ok(reads)
}

async fn address_names(database: &TestDatabase, owner: &str, filter: &str) -> Result<Vec<String>> {
    let payload = v2_address_names_payload_for_database(
        database,
        &format!("/v1/addresses/{owner}/names?{filter}"),
    )
    .await?;
    Ok(payload["data"]
        .as_array()
        .expect("address names")
        .iter()
        .map(|row| row["name"].as_str().expect("row name").to_owned())
        .collect())
}

async fn handoff_diagnostic(database: &TestDatabase) -> Result<Option<Value>> {
    let payload = v2_get_json(database, &format!("/v1/diagnostics/names/{NAME}/authority")).await?;
    Ok(payload["data"].get("registry_handoff").cloned())
}

/// Reads `pointer.eth` with its registry generation removed, as a row projected before the
/// generation existed would read, then puts the generation back.
async fn reads_without_generation(database: &TestDatabase) -> Result<Vec<Value>> {
    let saved: Value = sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection' FROM bigname_phase.name_current
         WHERE raw_name = $1",
    )
    .bind(NAME)
    .fetch_one(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET provenance = provenance #- '{authority_selection,registry_generation}'
         WHERE raw_name = $1",
    )
    .bind(NAME)
    .execute(&database.pool)
    .await?;
    let reads = product_reads(database).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET provenance = jsonb_set(provenance, '{authority_selection}', $2)
         WHERE raw_name = $1",
    )
    .bind(NAME)
    .bind(saved)
    .execute(&database.pool)
    .await?;
    Ok(reads)
}

#[tokio::test]
async fn produced_registry_only_name_serves_ens_v0_until_the_current_registry_records_it()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fresh = TestDatabase::new_migrated().await?;
    // The routes read both public namespaces; the Base index is published and empty.
    database
        .seed_snapshot_selector_chain_positions(&json!({"base": {
            "chain_id": "base-mainnet",
            "block_number": 1,
            "block_hash": "0xcount-base-empty",
            "timestamp": "2024-01-01T00:00:00Z",
        }}))
        .await?;
    let mut session = None;
    for block in OLD_RECORD..=CLEARED {
        // The served head advances with each projected block.
        seed_v2_history_blocks(&database, block..=block).await?;
        seed_v2_history_blocks(&fresh, block..=block).await?;
        let (output, next) = interpret(block, session)?;
        session = Some(next);
        persist(&database.pool, &output).await?;
        persist(&fresh.pool, &output).await?;
        project(
            &database.pool,
            block,
            (block > OLD_RECORD).then(|| block - 1),
        )
        .await?;
        // A full rebuild at the same position classifies every name the same way.
        project(&fresh.pool, block, None).await?;
        database
            .seed_snapshot_selector_chain_positions(&json!({CHAIN: {
                "chain_id": CHAIN,
                "block_number": block,
                "block_hash": format!("0xhistory{block}"),
                "timestamp": crate::v2::format_timestamp(timestamp(1_700_000_000 + block)),
            }}))
            .await?;
        let selections = authority_selections(&database.pool).await?;
        assert_eq!(
            selections,
            authority_selections(&fresh.pool).await?,
            "block {block}"
        );
        // A bare 2017-registry record creates no public name.
        let hidden: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM bigname_phase.name_current WHERE lower(namehash) = $1",
        )
        .bind(format!("{:#x}", namehash("hidden.eth")))
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(hidden, 0, "block {block}");
        if block == OLD_RECORD {
            assert!(selections.is_empty(), "{selections:?}");
            continue;
        }
        let [(name, selection)] = selections.as_slice() else {
            panic!("block {block}: {selections:?}");
        };
        assert_eq!(name, NAME);
        assert_eq!(selection["authority_arm"], "ens_v1", "block {block}");
        let reads = product_reads(&database).await?;
        assert!(
            reads[..2].iter().all(Value::is_object),
            "block {block}: {reads:?}"
        );
        for read in &reads {
            let text = read.to_string();
            assert!(
                !text.contains("registry_handoff") && !text.contains("registry_generation"),
                "block {block}: {read}"
            );
            assert!(read.get("migrated_at").is_none(), "block {block}: {read}");
        }
        match block {
            SURFACED => {
                assert_eq!(selection["registry_generation"], "old");
                assert!(selection.get("registry_handoff_block_number").is_none());
                assert!(
                    reads.iter().all(|read| read["authority"] == "ens_v0"),
                    "{reads:?}"
                );
                assert_eq!(handoff_diagnostic(&database).await?, None);
                // `ens_v0` changes no other field of the registry-only row.
                let legacy = reads_without_generation(&database).await?;
                assert_eq!(legacy.len(), reads.len());
                for (read, legacy) in reads.iter().zip(&legacy) {
                    let mut read = read.clone();
                    read["authority"] = json!("ens_v1");
                    assert_eq!(&read, legacy);
                }
                assert_eq!(
                    address_names(&database, OWNER, "authority=ens_v0").await?,
                    [NAME]
                );
                assert!(
                    address_names(&database, OWNER, "authority=ens_v1")
                        .await?
                        .is_empty()
                );
                assert!(
                    address_names(&database, OWNER, "is_migrated=true")
                        .await?
                        .is_empty()
                );
                let registration = reads[0]["registration_id"].clone();
                assert!(registration.is_string(), "{}", reads[0]);
                let followed = v2_permissions_payload_for_database(
                    &database,
                    &format!(
                        "/v1/permissions?registration_id={}",
                        registration.as_str().unwrap()
                    ),
                )
                .await?;
                assert!(
                    followed["data"]
                        .as_array()
                        .is_some_and(|rows| rows.iter().any(|row| row["address"] == OWNER)),
                    "{followed}"
                );
            }
            HANDOFF | MOVED => {
                assert_eq!(selection["registry_generation"], "current", "block {block}");
                assert_eq!(selection["registry_handoff_block_number"], HANDOFF);
                assert!(
                    reads.iter().all(|read| read["authority"] == "ens_v1"),
                    "block {block}: {reads:?}"
                );
                assert_eq!(
                    handoff_diagnostic(&database).await?,
                    Some(json!({"block_number": HANDOFF}))
                );
                let holder = if block == HANDOFF { OWNER } else { NEXT_OWNER };
                assert_eq!(
                    address_names(&database, holder, "authority=ens_v1").await?,
                    [NAME]
                );
                assert!(
                    address_names(&database, holder, "authority=ens_v0")
                        .await?
                        .is_empty()
                );
            }
            CLEARED => {
                assert_eq!(selection["ownerless_registry"], true, "{selection}");
                assert!(
                    reads.iter().all(|read| read.get("authority").is_none()),
                    "{reads:?}"
                );
            }
            _ => unreachable!(),
        }
    }
    fresh.cleanup().await?;
    database.cleanup().await
}

/// A same-owner `setOwner` as the node's first current-registry write still derives the
/// `AuthorityTransferred` Project reads the handoff from, although the owner matches the one the
/// 2017 registry holds.
#[test]
fn a_same_owner_transfer_handoff_derives_a_current_registry_record() -> Result<()> {
    let mut session = None;
    let mut handoff = Vec::new();
    for block in [OLD_RECORD, SURFACED, HANDOFF] {
        let raw_logs = if block == HANDOFF {
            vec![transfer(OWNER, block)]
        } else {
            logs(block)
        };
        let input = BatchInput {
            chain_id: CHAIN.into(),
            manifests: manifests(),
            discovery_rules: Vec::new(),
            admissions: vec![
                admission(REGISTRY_MANIFEST, 971, "registry", REGISTRY),
                admission(REGISTRY_MANIFEST, 972, "registry_old", OLD_REGISTRY),
                admission(
                    REGISTRAR_MANIFEST,
                    973,
                    "legacy_registrar_controller",
                    CONTROLLER,
                ),
            ],
            prior_events: Vec::new(),
            blocks: vec![RawBlockInput {
                chain_id: CHAIN.into(),
                block_hash: format!("0xhistory{block}"),
                block_number: block,
                block_timestamp: timestamp(1_700_000_000 + block),
                canonicality_state: "canonical".into(),
            }],
            raw_logs,
        };
        let (output, next) =
            prepare_schema_v2_batch_incremental(input, session, StateCacheCapacity::Unlimited)?
                .finish(Vec::new())?;
        session = Some(next);
        if block == HANDOFF {
            handoff = output.normalized_events;
        }
    }
    assert!(
        handoff
            .iter()
            .any(|event| event.event_kind == "AuthorityTransferred"
                && event.after_state["emitter_role"] == "registry"
                && event.after_state["node"] == format!("{:#x}", namehash(NAME))),
        "{handoff:?}"
    );
    Ok(())
}

mod marked {
    use super::*;

    pub(super) const REGISTRY: &str = "0x00000000000000000000000000000000000000a1";
    pub(super) const OLD_REGISTRY: &str = "0x00000000000000000000000000000000000000a7";
    pub(super) const REGISTRAR: &str = "0x00000000000000000000000000000000000000a2";
    pub(super) const WRAPPER: &str = "0x00000000000000000000000000000000000000a6";
    pub(super) const HOLDER: &str = "0x00000000000000000000000000000000000000a3";
    pub(super) const NAME: &str = "marked.eth";

    sol! {
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event TokenTransfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    }

    pub(super) fn token_transfer(
        from: &str,
        to: &str,
        id: U256,
        block: i64,
        index: i64,
    ) -> RawLogInput {
        let mut data = TokenTransfer {
            from: from.parse().unwrap(),
            to: to.parse().unwrap(),
            tokenId: id,
        }
        .encode_log_data();
        // Solidity's ERC-721 event is named Transfer; disambiguate it from ENSRegistry.Transfer.
        data.topics_mut()[0] = keccak256("Transfer(address,address,uint256)");
        raw(data, block, index, REGISTRAR)
    }

    /// The Sepolia registry, registrar and wrapper declarations, whose numeric BaseRegistrar
    /// grant reconciles its whole transaction, with the 2017-style registry admitted beside the
    /// current one.
    pub(super) fn inputs() -> (Vec<ManifestInput>, Vec<AddressAdmissionInput>) {
        let repository = bigname_manifests::load_repository(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
        )
        .unwrap();
        let mut manifests = Vec::new();
        let mut admissions = Vec::new();
        for (id, family, address, role) in [
            (981, "ens_v1_registry_l1", REGISTRY, "registry"),
            (982, "ens_v1_registrar_l1", REGISTRAR, "registrar"),
            (983, "ens_v1_wrapper_l1", WRAPPER, "name_wrapper"),
        ] {
            let loaded = repository
                .manifests()
                .iter()
                .find(|m| {
                    m.manifest.source_family == family
                        && m.manifest.rollout_status == bigname_manifests::RolloutStatus::Active
                })
                .unwrap();
            let mut manifest = loaded.manifest.clone();
            manifest.chain = CHAIN.into();
            manifests.push(ManifestInput {
                manifest_id: id,
                manifest_version: manifest.manifest_version as i64,
                namespace: manifest.namespace.clone(),
                source_family: family.into(),
                chain_id: CHAIN.into(),
                deployment_label: manifest.deployment_epoch.clone(),
                normalizer_version: manifest.normalizer_version.clone(),
                payload_json: serde_json::to_string(&manifest).unwrap(),
            });
            admissions.push(admission(id, id as u128, role, address));
        }
        admissions.push(admission(981, 984, "registry_old", OLD_REGISTRY));
        (manifests, admissions)
    }
}

/// The Sepolia `registerAndWrapETH2LD` shape: the registrar mints to the wrapper and writes the
/// registry record in the same transaction as its numeric `NameRegistered`, then the wrapper
/// wraps the name
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L131-L153 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L278 @ ens_v1@91c966f).
/// The node was recorded in the 2017-style registry first.
fn marked_logs() -> Result<Vec<RawLogInput>> {
    use marked::*;
    let id = U256::from_be_bytes(keccak256("marked").0);
    Ok(vec![
        new_owner("marked", HOLDER, 120, 0, OLD_REGISTRY),
        token_transfer(&Address::ZERO.to_string(), WRAPPER, id, 121, 0),
        new_owner("marked", WRAPPER, 121, 1, REGISTRY),
        raw(
            NameRegistered {
                id,
                owner: WRAPPER.parse()?,
                expires: U256::from(1_900_000_000_u64),
            }
            .encode_log_data(),
            121,
            2,
            REGISTRAR,
        ),
        raw(
            NameWrapped {
                node: namehash(NAME),
                name: b"\x06marked\x03eth\0".to_vec().into(),
                owner: HOLDER.parse()?,
                fuses: 0,
                expiry: 1_900_000_000,
            }
            .encode_log_data(),
            121,
            3,
            WRAPPER,
        ),
    ])
}

fn interpret_marked(
    blocks: std::ops::RangeInclusive<i64>,
    session: Option<AdapterSession>,
) -> Result<(BatchOutput, AdapterSession)> {
    let (manifests, admissions) = marked::inputs();
    let raw_logs = marked_logs()?
        .into_iter()
        .filter(|log| blocks.contains(&log.block_number))
        .collect();
    let (output, session) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests,
            discovery_rules: vec![],
            admissions,
            prior_events: vec![],
            blocks: blocks
                .map(|block| RawBlockInput {
                    chain_id: CHAIN.into(),
                    block_hash: format!("0xhistory{block}"),
                    block_number: block,
                    block_timestamp: timestamp(1_700_000_000 + block),
                    canonicality_state: "canonical".into(),
                })
                .collect(),
            raw_logs,
        },
        session,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    Ok((output, session))
}

#[tokio::test]
async fn same_transaction_registration_reads_as_the_current_registry_record() -> Result<()> {
    let (whole, _) = interpret_marked(120..=121, None)?;
    let (first, session) = interpret_marked(120..=120, None)?;
    let (second, _) = interpret_marked(121..=121, Some(session))?;
    let events = |outputs: &[&BatchOutput]| {
        outputs
            .iter()
            .flat_map(|output| &output.normalized_events)
            .map(|event| (event.event_identity.clone(), event.after_state.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(events(&[&whole]), events(&[&first, &second]));
    let grant = whole
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .expect("registration grant");
    assert_eq!(grant.after_state["registry_migrated"], true, "{grant:?}");
    assert_keeps_current_registry_write(&whole, 121);

    let selection_of = |pool: PgPool| async move {
        sqlx::query_scalar::<_, Value>(
            "SELECT provenance -> 'authority_selection' FROM bigname_phase.name_current
             WHERE raw_name = $1",
        )
        .bind(marked::NAME)
        .fetch_one(&pool)
        .await
    };
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, 120..=121).await?;
    persist_with_manifests(&database.pool, &marked::inputs().0, &whole).await?;
    project(&database.pool, 121, None).await?;
    let selection = selection_of(database.pool.clone()).await?;
    // Incremental Project over the per-block output reaches the same row at the handoff block.
    let incremental = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&incremental, 120..=121).await?;
    persist_with_manifests(&incremental.pool, &marked::inputs().0, &first).await?;
    project(&incremental.pool, 120, None).await?;
    persist_with_manifests(&incremental.pool, &marked::inputs().0, &second).await?;
    project(&incremental.pool, 121, Some(120)).await?;
    assert_eq!(selection, selection_of(incremental.pool.clone()).await?);
    incremental.cleanup().await?;
    assert_eq!(selection["authority_arm"], "ens_v1", "{selection}");
    assert_eq!(selection["registry_generation"], "current", "{selection}");
    assert_eq!(
        selection["registry_handoff_block_number"], 121,
        "{selection}"
    );
    database.cleanup().await
}

/// Reconciliation keeps a current-registry ownership write in the block of a registration it
/// marks `registry_migrated`, so Project reads the handoff from that write and needs no reading
/// of the marker.
fn assert_keeps_current_registry_write(output: &BatchOutput, block: i64) {
    let marked = output.normalized_events.iter().any(|event| {
        event.block_number == Some(block) && event.after_state["registry_migrated"] == true
    });
    assert!(
        marked,
        "no registration marked registry_migrated at {block}"
    );
    let kept = output.normalized_events.iter().any(|event| {
        event.block_number == Some(block)
            && event.event_kind == "AuthorityTransferred"
            && event.after_state["emitter_role"] == "registry"
            && event.after_state["child_node"] == format!("{:#x}", namehash(marked::NAME))
    });
    assert!(
        kept,
        "reconciliation dropped every current-registry write at {block}"
    );
}

/// The resolver-bearing controller shape: the registrar mints to the controller and writes the
/// registry record for it, the controller then reclaims the record for the buyer and hands the
/// token over. The controller's own write is transient and reconciliation drops its setup
/// permission, but the buyer's write stays
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L287-L317 @ ens_v1@91c966f).
#[test]
fn reconciliation_keeps_the_last_current_registry_write_of_a_transient_owner() -> Result<()> {
    use marked::*;
    const CONTROLLER: &str = "0x00000000000000000000000000000000000000c7";
    let id = U256::from_be_bytes(keccak256("marked").0);
    let (manifests, admissions) = inputs();
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests,
            discovery_rules: vec![],
            admissions,
            prior_events: vec![],
            blocks: (120..=121)
                .map(|block| RawBlockInput {
                    chain_id: CHAIN.into(),
                    block_hash: format!("0xhistory{block}"),
                    block_number: block,
                    block_timestamp: timestamp(1_700_000_000 + block),
                    canonicality_state: "canonical".into(),
                })
                .collect(),
            raw_logs: vec![
                new_owner("marked", HOLDER, 120, 0, OLD_REGISTRY),
                token_transfer(&Address::ZERO.to_string(), CONTROLLER, id, 121, 0),
                new_owner("marked", CONTROLLER, 121, 1, REGISTRY),
                raw(
                    NameRegistered {
                        id,
                        owner: CONTROLLER.parse()?,
                        expires: U256::from(1_900_000_000_u64),
                    }
                    .encode_log_data(),
                    121,
                    2,
                    REGISTRAR,
                ),
                new_owner("marked", HOLDER, 121, 3, REGISTRY),
                token_transfer(CONTROLLER, HOLDER, id, 121, 4),
            ],
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    assert_keeps_current_registry_write(&output, 121);
    Ok(())
}
