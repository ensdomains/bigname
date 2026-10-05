//! F9 account approvals and F16 registry entries of the ENSv2 registries, folded from what the
//! adapter writes for the logs a PermissionedRegistry emits, under the checked-in Sepolia
//! manifests. Each test encodes the registry's own events in the order the contract emits them,
//! interprets them with the adapter and applies the result with the family runner.
//!
//! `_register` emits LabelRegistered, mints (TransferSingle from zero), emits TokenResource and
//! grants the owner's roles; a role change burns and mints the next token version around
//! TokenRegenerated; `unregister` emits LabelUnregistered and burns.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L448-L514 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L578-L588 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L227-L238 @ ens_v2_sepolia_20261001@07e55a05)
#[path = "families_support/mod.rs"]
mod support;

use alloy_primitives::{Address, LogData, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, BatchOutput, DiscoveryRuleInput, ManifestInput,
    RawBlockInput, RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use bigname_project::families::{self, FamilyMode};
use serde_json::{Value, json};
use sqlx::types::Uuid;
use support::{CHAIN, Event, Fixture, hash};
use time::OffsetDateTime;

sol! {
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
    event ParentUpdated(address indexed parent, string label, address indexed sender);
    event ApprovalForAll(address indexed account, address indexed operator, bool approved);
}

const ETH_REGISTRY: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const ROOT_REGISTRY: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";
/// A registry no manifest declares, admitted by its own RegistryCreated announcement.
const USER_REGISTRY: &str = "0x00000000000000000000000000000000000000e7";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const OPERATOR: &str = "0x00000000000000000000000000000000000000c0";
const SENDER: &str = "0x00000000000000000000000000000000000000d0";
const REGISTRY_MANIFEST: i64 = 501;
const ROOT_MANIFEST: i64 = 502;
const FAR: u64 = 2_000_000_000;
const ENTRY_OWNER: &str = "project_ens_v2_entry_owner";
const REGISTRY_PARENT: &str = "project_ens_v2_registry_parent";
const APPROVAL: &str = "project_account_approval";

fn address(text: &str) -> Address {
    text.parse().expect("fixture address")
}

/// The label's id with `version` in its low 32 bits, as `LibLabel.withVersion` builds a token
/// id or a resource.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/LibLabel.sol:L7-L16 @ ens_v2_sepolia_20261001@07e55a05)
fn versioned(label: &str, version: u32) -> U256 {
    let id = U256::from_be_bytes(keccak256(label.as_bytes()).0);
    (id >> 32 << 32) | U256::from(version)
}

fn word(value: U256) -> String {
    format!("0x{value:064x}")
}

fn entry(label: &str) -> String {
    word(versioned(label, 0))
}

fn instance(address: &str) -> Uuid {
    Uuid::from_u128(u128::from_str_radix(&address[34..], 16).expect("fixture address"))
}

fn manifests() -> Result<Vec<ManifestInput>> {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    [
        (REGISTRY_MANIFEST, "ens_v2_registry_l1"),
        (ROOT_MANIFEST, "ens_v2_root_l1"),
    ]
    .into_iter()
    .map(|(manifest_id, family)| {
        let loaded = repository
            .manifests()
            .iter()
            .find(|loaded| loaded.manifest.source_family == family)
            .with_context(|| format!("checked-in Sepolia {family} manifest"))?;
        Ok(ManifestInput {
            manifest_id,
            manifest_version: i64::try_from(loaded.manifest.manifest_version)?,
            namespace: loaded.manifest.namespace.clone(),
            source_family: family.to_owned(),
            chain_id: CHAIN.to_owned(),
            deployment_label: loaded.manifest.deployment_epoch.clone(),
            normalizer_version: loaded.manifest.normalizer_version.clone(),
            payload_json: serde_json::to_string(&loaded.manifest)?,
        })
    })
    .collect()
}

fn rules() -> Vec<DiscoveryRuleInput> {
    let rule = |manifest_id, edge_kind: &str, role: &str| DiscoveryRuleInput {
        manifest_id,
        edge_kind: edge_kind.to_owned(),
        from_role: Some(role.to_owned()),
        admission: "reachable_from_root".to_owned(),
    };
    vec![
        rule(REGISTRY_MANIFEST, "subregistry", "registry"),
        rule(REGISTRY_MANIFEST, "resolver", "registry"),
        rule(REGISTRY_MANIFEST, "registry_announcement", "registry"),
        rule(ROOT_MANIFEST, "subregistry", "root_registry"),
        rule(ROOT_MANIFEST, "resolver", "root_registry"),
    ]
}

fn admissions() -> Vec<AddressAdmissionInput> {
    let declared = |address: &str, manifest_id, role: &str| AddressAdmissionInput {
        address: address.to_owned(),
        contract_instance_id: instance(address),
        source_manifest_id: Some(manifest_id),
        role: Some(role.to_owned()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    };
    let mut announced = declared(USER_REGISTRY, REGISTRY_MANIFEST, "registry");
    announced.discovery_edge_kind = Some("registry_announcement".to_owned());
    announced.discovery_from_contract_instance_id = Some(announced.contract_instance_id);
    announced.discovery_observation_key = Some("registry-announcement:self".to_owned());
    // A resolver a registry points at is admitted under the registry's manifest too; it is
    // not a registry, and its ApprovalForAll is a resolver approval.
    let mut resolver = declared(RESOLVER, REGISTRY_MANIFEST, "registry");
    resolver.role = None;
    resolver.discovery_edge_kind = Some("resolver".to_owned());
    resolver.discovery_from_contract_instance_id = Some(instance(ETH_REGISTRY));
    resolver.discovery_observation_key = Some("resolver:fixture".to_owned());
    vec![
        declared(ETH_REGISTRY, REGISTRY_MANIFEST, "registry"),
        declared(ROOT_REGISTRY, ROOT_MANIFEST, "root_registry"),
        announced,
        resolver,
    ]
}

const RESOLVER: &str = "0x00000000000000000000000000000000000000f1";

/// The logs of a chain, each block's in emission order.
#[derive(Default)]
struct Logs {
    raw: Vec<RawLogInput>,
}

impl Logs {
    fn push(&mut self, block: i64, emitter: &str, data: LogData) -> &mut Self {
        let log_index = self
            .raw
            .iter()
            .filter(|raw| raw.block_number == block)
            .count() as i64;
        self.raw.push(RawLogInput {
            chain_id: CHAIN.to_owned(),
            block_hash: hash(block),
            block_number: block,
            block_timestamp: timestamp(block),
            canonicality_state: "canonical".to_owned(),
            transaction_hash: format!("0x{block:064x}"),
            transaction_index: 0,
            log_index,
            emitting_address: emitter.to_owned(),
            topics: data
                .topics()
                .iter()
                .map(|topic| format!("{topic:#x}"))
                .collect(),
            data: data.data.to_vec(),
        });
        self
    }

    /// `_register` with an owner: the logs of one registration of `label` at token and role
    /// version `version`.
    fn register(
        &mut self,
        block: i64,
        registry: &str,
        label: &str,
        (token_version, role_version): (u32, u32),
        owner: &str,
    ) -> &mut Self {
        let token = versioned(label, token_version);
        let resource = versioned(label, role_version);
        self.push(
            block,
            registry,
            LabelRegistered {
                tokenId: token,
                labelHash: keccak256(label.as_bytes()),
                label: label.to_owned(),
                owner: address(owner),
                expiry: FAR,
                sender: address(SENDER),
            }
            .encode_log_data(),
        )
        .push(block, registry, mint(token, owner))
        .push(
            block,
            registry,
            TokenResource {
                tokenId: token,
                resource,
            }
            .encode_log_data(),
        )
        .push(
            block,
            registry,
            EACRolesChanged {
                resource,
                account: address(owner),
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::from(0x1111),
            }
            .encode_log_data(),
        )
    }

    fn approve(
        &mut self,
        block: i64,
        registry: &str,
        owner: &str,
        operator: &str,
        approved: bool,
    ) -> &mut Self {
        self.push(
            block,
            registry,
            ApprovalForAll {
                account: address(owner),
                operator: address(operator),
                approved,
            }
            .encode_log_data(),
        )
    }
}

fn transfer(token: U256, from: &str, to: &str) -> LogData {
    TransferSingle {
        operator: address(SENDER),
        from: address(from),
        to: address(to),
        id: token,
        value: U256::from(1),
    }
    .encode_log_data()
}

fn mint(token: U256, to: &str) -> LogData {
    transfer(token, "0x0000000000000000000000000000000000000000", to)
}

fn burn(token: U256, from: &str) -> LogData {
    transfer(token, from, "0x0000000000000000000000000000000000000000")
}

fn timestamp(block: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_800_000_000 + block * 12).expect("fixture time")
}

fn interpret(logs: &Logs, blocks: i64) -> Result<BatchOutput> {
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: manifests()?,
            discovery_rules: rules(),
            admissions: admissions(),
            prior_events: Vec::new(),
            blocks: (0..=blocks)
                .map(|block| RawBlockInput {
                    chain_id: CHAIN.to_owned(),
                    block_hash: hash(block),
                    block_number: block,
                    block_timestamp: timestamp(block),
                    canonicality_state: "canonical".to_owned(),
                })
                .collect(),
            raw_logs: logs.raw.clone(),
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(Vec::new())?;
    anyhow::ensure!(
        output.decode_skips.is_empty(),
        "the adapter skipped a log: {:?}",
        output.decode_skips
    );
    Ok(output)
}

/// Write the adapter's events as Interpret would and return a fixture with `blocks` blocks.
async fn persisted(prefix: &str, logs: &Logs, blocks: i64) -> Result<(Fixture, BatchOutput)> {
    let output = interpret(logs, blocks)?;
    let fixture = Fixture::new(prefix, blocks).await?;
    for event in &output.normalized_events {
        let resource = event.resource_id.map(|id| id.to_string());
        if let Some(resource) = resource.as_deref() {
            fixture.resource(resource).await?;
        }
        if let Some(name) = event.logical_name_id.as_deref() {
            let namehash = name.split_once(':').map_or(name, |(_, hash)| hash);
            fixture.surface(name, namehash).await?;
        }
        fixture
            .event(Event {
                identity: &event.event_identity,
                chain: CHAIN,
                block: event.block_number.context("event block")?,
                position: event.transaction_index.zip(event.log_index),
                kind: &event.event_kind,
                family: &event.source_family,
                name: event.logical_name_id.as_deref(),
                resource: resource.as_deref(),
                before: event.before_state.clone(),
                after: event.after_state.clone(),
                raw: event.raw_fact_ref.clone(),
            })
            .await?;
    }
    Ok((fixture, output))
}

async fn entry_row(fixture: &Fixture, registry: &str, label: &str) -> Result<Value> {
    let key = entry(label);
    fixture
        .rows(ENTRY_OWNER)
        .await?
        .into_iter()
        .find(|row| row["registry"] == registry && row["entry_key"] == key.as_str())
        .with_context(|| format!("no entry row for {label} in {registry}"))
}

fn assert_entry(
    row: &Value,
    status: &str,
    owner: Option<&str>,
    token: U256,
    resource: Option<U256>,
) {
    assert_eq!(row["status"], status, "{row:#}");
    assert_eq!(row["owner"], json!(owner), "{row:#}");
    assert_eq!(row["token_id"], word(token), "{row:#}");
    assert_eq!(
        row["upstream_resource"],
        json!(resource.map(word)),
        "{row:#}"
    );
    assert_eq!(row["resource_id"].is_null(), resource.is_none(), "{row:#}");
}

async fn approval(fixture: &Fixture, registry: &str, owner: &str) -> Result<Option<Value>> {
    Ok(fixture
        .rows(APPROVAL)
        .await?
        .into_iter()
        .find(|row| row["authority_contract"] == registry && row["owner"] == owner))
}

/// One name through its registry's whole token life: registered, approved to an operator,
/// transferred, regenerated by a role change, renewed, unregistered and registered again.
#[tokio::test]
async fn a_registry_entry_follows_its_token_through_the_registry_logs() -> Result<()> {
    let label = "alice";
    let mut logs = Logs::default();
    logs.register(1, ETH_REGISTRY, label, (0, 0), ALICE)
        .approve(2, ETH_REGISTRY, ALICE, OPERATOR, true)
        .push(3, ETH_REGISTRY, transfer(versioned(label, 0), ALICE, BOB));
    // A role grant regenerates the token: the resource keeps its version.
    logs.push(
        4,
        ETH_REGISTRY,
        EACRolesChanged {
            resource: versioned(label, 0),
            account: address(OPERATOR),
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: U256::from(1),
        }
        .encode_log_data(),
    )
    .push(4, ETH_REGISTRY, burn(versioned(label, 0), BOB))
    .push(
        4,
        ETH_REGISTRY,
        TokenRegenerated {
            oldTokenId: versioned(label, 0),
            newTokenId: versioned(label, 1),
        }
        .encode_log_data(),
    )
    .push(4, ETH_REGISTRY, mint(versioned(label, 1), BOB))
    .push(
        5,
        ETH_REGISTRY,
        ExpiryUpdated {
            tokenId: versioned(label, 1),
            newExpiry: FAR + 7,
            sender: address(SENDER),
        }
        .encode_log_data(),
    )
    .push(
        6,
        ETH_REGISTRY,
        LabelUnregistered {
            tokenId: versioned(label, 1),
            sender: address(SENDER),
        }
        .encode_log_data(),
    )
    .push(6, ETH_REGISTRY, burn(versioned(label, 1), BOB))
    // Unregister advanced both versions, so the next registration has token 2 and resource 1.
    .register(7, ETH_REGISTRY, label, (2, 1), ALICE)
    .approve(8, ETH_REGISTRY, ALICE, OPERATOR, false);
    let (fixture, _) = persisted("families_ens_v2_registry_life", &logs, 8).await?;

    fixture.apply(1, FamilyMode::Normal).await?;
    let row = entry_row(&fixture, ETH_REGISTRY, label).await?;
    assert_entry(
        &row,
        "registered",
        Some(ALICE),
        versioned(label, 0),
        Some(versioned(label, 0)),
    );
    assert_eq!(row["expiry"], json!(FAR));
    assert_eq!(
        row["registry_contract_instance_id"],
        instance(ETH_REGISTRY).to_string()
    );
    assert_eq!(row["owner_position"]["log_index"], 0);
    assert_eq!(row["resource_position"]["log_index"], 2);
    assert!(approval(&fixture, ETH_REGISTRY, ALICE).await?.is_none());

    fixture.apply(2, FamilyMode::Normal).await?;
    let approved = approval(&fixture, ETH_REGISTRY, ALICE)
        .await?
        .context("the approval row")?;
    assert_eq!(approved["authority_kind"], "ens_v2_registry");
    assert_eq!(approved["subject"], OPERATOR);
    assert_eq!(approved["relation_kind"], "operator");
    assert_eq!(approved["approved"], true);
    assert_eq!(
        approved["effective_powers"],
        json!([]),
        "an ENSv2 approval stores no power: the operator's powers are the token owner's"
    );
    assert_eq!(
        approved["authority_contract_instance_id"],
        instance(ETH_REGISTRY).to_string()
    );
    assert_eq!(
        approved["transfer_behavior"],
        json!({"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"})
    );

    // The transfer moves the token; the approval stays Alice's and is not rewritten.
    fixture.assert_undo_restores(3).await?;
    let row = entry_row(&fixture, ETH_REGISTRY, label).await?;
    assert_entry(
        &row,
        "registered",
        Some(BOB),
        versioned(label, 0),
        Some(versioned(label, 0)),
    );
    assert_eq!(row["owner_position"]["block_number"], 3);
    assert_eq!(
        approval(&fixture, ETH_REGISTRY, ALICE).await?,
        Some(approved.clone())
    );

    // The burn and mint around TokenRegenerated are not transfers: Bob keeps the entry.
    fixture.assert_undo_restores(4).await?;
    let row = entry_row(&fixture, ETH_REGISTRY, label).await?;
    assert_entry(
        &row,
        "registered",
        Some(BOB),
        versioned(label, 1),
        Some(versioned(label, 0)),
    );
    assert_eq!(row["owner_position"]["block_number"], 3);

    fixture.apply(5, FamilyMode::Normal).await?;
    let row = entry_row(&fixture, ETH_REGISTRY, label).await?;
    assert_eq!(row["expiry"], json!(FAR + 7));
    assert_eq!(row["owner"], BOB);

    // `unregister` sets the entry's expiry to the block time and burns the token.
    fixture.assert_undo_restores(6).await?;
    let row = entry_row(&fixture, ETH_REGISTRY, label).await?;
    assert_entry(
        &row,
        "unregistered",
        None,
        versioned(label, 1),
        Some(versioned(label, 0)),
    );
    assert_eq!(row["expiry"], json!(1_800_000_000 + 6 * 12));

    fixture.apply(7, FamilyMode::Normal).await?;
    let row = entry_row(&fixture, ETH_REGISTRY, label).await?;
    assert_entry(
        &row,
        "registered",
        Some(ALICE),
        versioned(label, 2),
        Some(versioned(label, 1)),
    );
    assert_eq!(row["expiry"], json!(FAR));

    // A revocation stays as a row, and undoing it brings the approval back.
    fixture.assert_undo_restores(8).await?;
    let revoked = approval(&fixture, ETH_REGISTRY, ALICE)
        .await?
        .context("the revoked approval row")?;
    assert_eq!(revoked["approved"], false);
    assert_eq!(revoked["effective_powers"], json!([]));
    assert_eq!(revoked["block_number"], 8);
    families::undo_to(&fixture.pool, CHAIN, 7).await?;
    assert_eq!(
        approval(&fixture, ETH_REGISTRY, ALICE).await?,
        Some(approved)
    );
    fixture.apply(8, FamilyMode::Normal).await?;

    assert_eq!(fixture.rows(ENTRY_OWNER).await?.len(), 1);
    fixture.assert_rebuild_equal(8).await?;
    fixture.cleanup().await
}

/// Approvals and entries are kept per registry: the declared ETHRegistry and RootRegistry and
/// a registry admitted by its own announcement each have their own, and a resolver admitted
/// under the registry manifest has none.
#[tokio::test]
async fn every_admitted_registry_keeps_its_own_approvals_and_entries() -> Result<()> {
    let mut logs = Logs::default();
    logs.register(1, ETH_REGISTRY, "alice", (0, 0), ALICE)
        .register(1, ROOT_REGISTRY, "eth", (0, 0), BOB)
        .register(1, USER_REGISTRY, "sub", (0, 0), ALICE)
        .approve(2, ETH_REGISTRY, ALICE, OPERATOR, true)
        .approve(2, ROOT_REGISTRY, BOB, OPERATOR, true)
        .approve(2, USER_REGISTRY, ALICE, OPERATOR, true)
        // Self-approval is allowed on chain and is kept as the fact it is.
        .approve(2, USER_REGISTRY, ALICE, ALICE, true)
        .approve(2, RESOLVER, ALICE, OPERATOR, true)
        .approve(3, USER_REGISTRY, ALICE, OPERATOR, false);
    let (fixture, output) = persisted("families_ens_v2_registry_emitters", &logs, 3).await?;
    assert!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "AccountPermissionChanged")
            .all(|event| event.derivation_kind == "standard_approval"
                && event.logical_name_id.is_none()
                && event.resource_id.is_none())
    );

    fixture.apply(2, FamilyMode::Normal).await?;
    let approvals = fixture.rows(APPROVAL).await?;
    let mut keys = approvals
        .iter()
        .map(|row| {
            assert_eq!(row["authority_kind"], "ens_v2_registry");
            assert_eq!(row["approved"], true);
            (
                row["authority_contract"].as_str().unwrap().to_owned(),
                row["owner"].as_str().unwrap().to_owned(),
                row["subject"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    keys.sort();
    let key = |registry: &str, owner: &str, subject: &str| {
        (registry.to_owned(), owner.to_owned(), subject.to_owned())
    };
    assert_eq!(
        keys,
        vec![
            key(USER_REGISTRY, ALICE, ALICE),
            key(USER_REGISTRY, ALICE, OPERATOR),
            key(ROOT_REGISTRY, BOB, OPERATOR),
            key(ETH_REGISTRY, ALICE, OPERATOR),
        ],
        "the resolver's ApprovalForAll is not a registry approval"
    );
    for (registry, label, owner) in [
        (ETH_REGISTRY, "alice", ALICE),
        (ROOT_REGISTRY, "eth", BOB),
        (USER_REGISTRY, "sub", ALICE),
    ] {
        let row = entry_row(&fixture, registry, label).await?;
        assert_entry(
            &row,
            "registered",
            Some(owner),
            versioned(label, 0),
            Some(versioned(label, 0)),
        );
    }

    fixture.assert_undo_restores(3).await?;
    let revoked = fixture
        .rows(APPROVAL)
        .await?
        .into_iter()
        .filter(|row| row["approved"] == false)
        .collect::<Vec<_>>();
    assert_eq!(revoked.len(), 1);
    assert_eq!(revoked[0]["authority_contract"], USER_REGISTRY);
    assert_eq!(revoked[0]["subject"], OPERATOR);
    fixture.assert_rebuild_equal(3).await?;
    fixture.cleanup().await
}

/// A reservation holds a label without a token, a batch transfer moves several tokens, and a
/// registry's ParentUpdated names the parent entry whose owner a WrapperRegistry gives its
/// root roles to.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L273-L287 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn reservations_batch_transfers_and_parents_are_kept_per_registry() -> Result<()> {
    let mut logs = Logs::default();
    logs.push(
        1,
        ETH_REGISTRY,
        LabelReserved {
            tokenId: versioned("held", 0),
            labelHash: keccak256(b"held"),
            label: "held".to_owned(),
            expiry: FAR,
            sender: address(SENDER),
        }
        .encode_log_data(),
    )
    .register(1, ETH_REGISTRY, "one", (0, 0), ALICE)
    .register(1, ETH_REGISTRY, "two", (0, 0), ALICE)
    .push(
        2,
        USER_REGISTRY,
        ParentUpdated {
            parent: address(ETH_REGISTRY),
            label: "one".to_owned(),
            sender: address(SENDER),
        }
        .encode_log_data(),
    )
    .push(
        3,
        ETH_REGISTRY,
        TransferBatch {
            operator: address(SENDER),
            from: address(ALICE),
            to: address(BOB),
            ids: vec![versioned("one", 0), versioned("two", 0)],
            values: vec![U256::from(1), U256::from(1)],
        }
        .encode_log_data(),
    )
    // Registering the reserved label mints its first token.
    .register(4, ETH_REGISTRY, "held", (0, 0), BOB)
    .push(
        5,
        USER_REGISTRY,
        ParentUpdated {
            parent: Address::ZERO,
            label: String::new(),
            sender: address(SENDER),
        }
        .encode_log_data(),
    );
    let (fixture, _) = persisted("families_ens_v2_registry_shapes", &logs, 5).await?;

    fixture.apply(2, FamilyMode::Normal).await?;
    let held = entry_row(&fixture, ETH_REGISTRY, "held").await?;
    assert_entry(&held, "reserved", None, versioned("held", 0), None);
    assert_eq!(held["expiry"], json!(FAR));
    let parent = fixture.rows(REGISTRY_PARENT).await?;
    assert_eq!(parent.len(), 1);
    assert_eq!(parent[0]["registry"], USER_REGISTRY);
    assert_eq!(parent[0]["parent"], ETH_REGISTRY);
    assert_eq!(parent[0]["raw_label_hex"], "6f6e65");
    assert_eq!(
        parent[0]["parent_entry_key"],
        entry_row(&fixture, ETH_REGISTRY, "one").await?["entry_key"],
        "the parent row names the entry the label has in the parent registry"
    );

    fixture.assert_undo_restores(3).await?;
    for label in ["one", "two"] {
        let row = entry_row(&fixture, ETH_REGISTRY, label).await?;
        assert_entry(
            &row,
            "registered",
            Some(BOB),
            versioned(label, 0),
            Some(versioned(label, 0)),
        );
    }

    fixture.assert_undo_restores(4).await?;
    let held = entry_row(&fixture, ETH_REGISTRY, "held").await?;
    assert_entry(
        &held,
        "registered",
        Some(BOB),
        versioned("held", 0),
        Some(versioned("held", 0)),
    );

    fixture.assert_undo_restores(5).await?;
    let parent = fixture.rows(REGISTRY_PARENT).await?;
    assert_eq!(parent.len(), 1);
    assert!(parent[0]["parent"].is_null());
    assert_eq!(parent[0]["block_number"], 5);
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}

/// `renew` by a root renewer revives an entry `unregister` left without a token: the entry is
/// held with no owner under the token id the log names, whether it was registered or reserved
/// before.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L227-L258 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn a_renewal_revives_an_unregistered_entry_without_an_owner() -> Result<()> {
    let unregister = |token: U256| {
        LabelUnregistered {
            tokenId: token,
            sender: address(SENDER),
        }
        .encode_log_data()
    };
    let renew = |token: U256, expiry: u64| {
        ExpiryUpdated {
            tokenId: token,
            newExpiry: expiry,
            sender: address(SENDER),
        }
        .encode_log_data()
    };
    let mut logs = Logs::default();
    logs.register(1, ETH_REGISTRY, "alice", (0, 0), ALICE)
        .push(
            1,
            ETH_REGISTRY,
            LabelReserved {
                tokenId: versioned("held", 0),
                labelHash: keccak256(b"held"),
                label: "held".to_owned(),
                expiry: FAR,
                sender: address(SENDER),
            }
            .encode_log_data(),
        )
        .push(2, ETH_REGISTRY, unregister(versioned("alice", 0)))
        .push(2, ETH_REGISTRY, burn(versioned("alice", 0), ALICE))
        // A reservation has no token to burn, so its version stays.
        .push(2, ETH_REGISTRY, unregister(versioned("held", 0)))
        .push(3, ETH_REGISTRY, renew(versioned("alice", 1), FAR + 1))
        .push(3, ETH_REGISTRY, renew(versioned("held", 0), FAR + 2))
        .register(4, ETH_REGISTRY, "alice", (1, 1), BOB);
    let (fixture, _) = persisted("families_ens_v2_registry_revival", &logs, 4).await?;

    fixture.apply(2, FamilyMode::Normal).await?;
    let alice = entry_row(&fixture, ETH_REGISTRY, "alice").await?;
    assert_entry(
        &alice,
        "unregistered",
        None,
        versioned("alice", 0),
        Some(versioned("alice", 0)),
    );
    assert_eq!(
        entry_row(&fixture, ETH_REGISTRY, "held").await?["status"],
        "unregistered"
    );

    fixture.assert_undo_restores(3).await?;
    let alice = entry_row(&fixture, ETH_REGISTRY, "alice").await?;
    assert_entry(&alice, "reserved", None, versioned("alice", 1), None);
    assert_eq!(alice["expiry"], json!(FAR + 1));
    assert_eq!(alice["owner_position"]["block_number"], 2);
    let held = entry_row(&fixture, ETH_REGISTRY, "held").await?;
    assert_entry(&held, "reserved", None, versioned("held", 0), None);
    assert_eq!(held["expiry"], json!(FAR + 2));

    // Registering the revived entry mints at the version the renewal named.
    fixture.assert_undo_restores(4).await?;
    let alice = entry_row(&fixture, ETH_REGISTRY, "alice").await?;
    assert_entry(
        &alice,
        "registered",
        Some(BOB),
        versioned("alice", 1),
        Some(versioned("alice", 1)),
    );
    fixture.assert_rebuild_equal(4).await?;
    fixture.cleanup().await
}

/// A transfer seen before the entry's registration still names the token's owner, and a log
/// that names no owner leaves the owner unknown rather than absent.
#[tokio::test]
async fn an_entry_first_seen_mid_life_keeps_what_its_logs_say() -> Result<()> {
    let fixture = Fixture::new("families_ens_v2_registry_mid_life", 2).await?;
    let token = word(versioned("late", 3));
    let resource = support::uuid(0x77);
    fixture
        .write(
            1,
            0,
            "TokenControlTransferred",
            "ens_v2_registry_l1",
            None,
            None,
            json!({"source_event": "TransferSingle", "operator": SENDER, "to": BOB,
                   "amount": "1", "token_id": token, "upstream_resource": null,
                   "token_lineage_id": null, "registry_hydration_pending": true,
                   "registry_contract_instance_id": instance(USER_REGISTRY).to_string()}),
            USER_REGISTRY,
        )
        .await?;
    let other = word(versioned("other", 1));
    fixture
        .write(
            1,
            1,
            "TokenResourceLinked",
            "ens_v2_registry_l1",
            None,
            Some(&resource),
            json!({"source_event": "TokenResource", "token_id": other,
                   "current_token_id": other, "upstream_resource": word(versioned("other", 0)),
                   "resource_id": resource,
                   "registry_contract_instance_id": instance(USER_REGISTRY).to_string()}),
            USER_REGISTRY,
        )
        .await?;
    // Events the adapter restates for a name whose path changed carry no sender and are not
    // the registry's own word on the token.
    fixture
        .write(
            2,
            0,
            "RegistrationReleased",
            "ens_v2_registry_l1",
            None,
            None,
            json!({"source_event": "LabelUnregistered", "status": "released",
                   "terminal_reason": "registry_name_binding_changed", "token_id": token,
                   "registry_contract_instance_id": instance(USER_REGISTRY).to_string()}),
            USER_REGISTRY,
        )
        .await?;
    fixture
        .write(
            2,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            None,
            None,
            json!({"source_event": "LabelRegistered", "registrant": ALICE, "expiry": FAR,
                   "authority_kind": "ens_v2_registry", "token_id": token,
                   "current_token_id": token, "status": "registered",
                   "registry_contract_instance_id": instance(USER_REGISTRY).to_string()}),
            USER_REGISTRY,
        )
        .await?;

    fixture.apply(2, FamilyMode::Normal).await?;
    let late = entry_row(&fixture, USER_REGISTRY, "late").await?;
    assert_entry(&late, "registered", Some(BOB), versioned("late", 3), None);
    assert!(late["expiry"].is_null(), "no log has stated the expiry");
    assert_eq!(late["block_number"], 1, "the restated events wrote nothing");
    let other = entry_row(&fixture, USER_REGISTRY, "other").await?;
    assert_entry(
        &other,
        "unknown",
        None,
        versioned("other", 1),
        Some(versioned("other", 0)),
    );
    fixture.assert_rebuild_equal(2).await?;
    fixture.cleanup().await
}
