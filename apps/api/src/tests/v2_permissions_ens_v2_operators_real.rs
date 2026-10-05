// ENSv2 registry operators and root holders on a registration read, from the registry's own logs
// through the adapter and the family runner. `_register` emits LabelRegistered, mints, emits
// TokenResource and grants the owner's roles; a transfer moves the owner's roles with the token;
// a role grant regenerates the token; a root renewer revives an expired entry with `renew`.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L448-L514 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L528-L543 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L578-L588 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L243-L258 @ ens_v2_sepolia_20261001@07e55a05)
use super::*;
use alloy_primitives::{Address, LogData, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput, DiscoveryRuleInput, ManifestInput, RawBlockInput,
    RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};

sol! {
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
    event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId);
    event ApprovalForAll(address indexed account, address indexed operator, bool approved);
}

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x00000000000000000000000000000000000000d4";
const INSTANCE: u128 = 0x942;
const ALICE: &str = "0x00000000000000000000000000000000000000a1";
const BOB: &str = "0x00000000000000000000000000000000000000b1";
const OPERATOR: &str = "0x00000000000000000000000000000000000000c1";
const ROOT_HOLDER: &str = "0x00000000000000000000000000000000000000e1";
const ROOT_MIXED: &str = "0x00000000000000000000000000000000000000e5";
const ROOT_TRANSFER_ONLY: &str = "0x00000000000000000000000000000000000000e6";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const LABEL: &str = "alice";
/// The entry expires in the block whose time is this: block 130.
const EXPIRY: u64 = 1_700_000_130;

fn bit(index: usize) -> U256 {
    U256::from(1) << index
}

/// renew, set_resolver, admin_set_resolver and can_transfer_admin.
fn owner_roles() -> U256 {
    bit(16) | bit(24) | bit(152) | bit(156)
}

fn account(text: &str) -> Address {
    text.parse().expect("fixture address")
}

/// The label's id with `version` in its low 32 bits.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/LibLabel.sol:L7-L16 @ ens_v2_sepolia_20261001@07e55a05)
fn versioned(version: u32) -> U256 {
    let id = U256::from_be_bytes(keccak256(LABEL.as_bytes()).0);
    (id >> 32 << 32) | U256::from(version)
}

/// The admitted Sepolia ENSv2 registry family declaration, moved onto this test's chain.
fn registry_family() -> (ManifestInput, AddressAdmissionInput) {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )
    .unwrap();
    let loaded = repository
        .manifests()
        .iter()
        .find(|m| {
            m.manifest.source_family == "ens_v2_registry_l1"
                && m.manifest.rollout_status == bigname_manifests::RolloutStatus::Active
        })
        .unwrap();
    let mut manifest = loaded.manifest.clone();
    manifest.chain = CHAIN.into();
    (
        ManifestInput {
            manifest_id: INSTANCE as i64,
            manifest_version: manifest.manifest_version as i64,
            namespace: manifest.namespace.clone(),
            source_family: "ens_v2_registry_l1".into(),
            chain_id: CHAIN.into(),
            deployment_label: manifest.deployment_epoch.clone(),
            normalizer_version: manifest.normalizer_version.clone(),
            payload_json: serde_json::to_string(&manifest).unwrap(),
        },
        AddressAdmissionInput {
            address: REGISTRY.into(),
            contract_instance_id: Uuid::from_u128(INSTANCE),
            source_manifest_id: Some(INSTANCE as i64),
            role: Some("registry".into()),
            discovery_edge_kind: None,
            discovery_from_contract_instance_id: None,
            discovery_observation_key: None,
            active_from_block: Some(0),
            active_to_block: None,
        },
    )
}

/// The registry's logs, each block's in emission order.
#[derive(Default)]
struct Logs(Vec<RawLogInput>);

impl Logs {
    fn push(&mut self, block: i64, data: LogData) -> &mut Self {
        let log_index = self.0.iter().filter(|raw| raw.block_number == block).count() as i64;
        self.0.push(RawLogInput {
            chain_id: CHAIN.into(),
            block_hash: format!("0xhistory{block}"),
            block_number: block,
            block_timestamp: timestamp(1_700_000_000 + block),
            canonicality_state: "canonical".into(),
            transaction_hash: format!("0x{block:064x}"),
            transaction_index: 0,
            log_index,
            emitting_address: REGISTRY.into(),
            topics: data.topics().iter().map(|topic| format!("{topic:#x}")).collect(),
            data: data.data.to_vec(),
        });
        self
    }

    fn roles(&mut self, block: i64, resource: U256, holder: &str, old: U256, new: U256) -> &mut Self {
        self.push(block, EACRolesChanged { resource, account: account(holder),
            oldRoleBitmap: old, newRoleBitmap: new }.encode_log_data())
    }

    fn transfer(&mut self, block: i64, token: U256, from: &str, to: &str) -> &mut Self {
        self.push(block, TransferSingle { operator: account(ROOT_HOLDER), from: account(from),
            to: account(to), id: token, value: U256::from(1) }.encode_log_data())
    }

    fn approve(&mut self, block: i64, owner: &str, operator: &str, approved: bool) -> &mut Self {
        self.push(block, ApprovalForAll { account: account(owner), operator: account(operator),
            approved }.encode_log_data())
    }

    /// `_register` of the label to Alice, expiring at `expiry`.
    fn register(&mut self, block: i64, expiry: u64) -> &mut Self {
        let (resource, token) = (versioned(0), versioned(0));
        self.push(block, LabelRegistered { tokenId: token, labelHash: keccak256(LABEL.as_bytes()),
            label: LABEL.into(), owner: account(ALICE), expiry,
            sender: account(ROOT_HOLDER) }.encode_log_data())
            .transfer(block, token, ZERO, ALICE)
            .push(block, TokenResource { tokenId: token, resource }.encode_log_data())
            .roles(block, resource, ALICE, U256::ZERO, owner_roles())
    }
}

fn registry_logs() -> Logs {
    let mut logs = Logs::default();
    let (resource, token) = (versioned(0), versioned(0));
    // The root holder is the registrar and holds root `renew`, which lets it revive an entry.
    logs.roles(120, U256::ZERO, ROOT_HOLDER, U256::ZERO, bit(0) | bit(16) | bit(128))
        .roles(120, U256::ZERO, ROOT_MIXED, U256::ZERO, bit(16) | bit(156))
        .roles(120, U256::ZERO, ROOT_TRANSFER_ONLY, U256::ZERO, bit(156))
        .register(121, EXPIRY)
        .approve(122, ALICE, OPERATOR, true)
        // The operator's own grant regenerates the token; the resource keeps its version.
        .roles(122, resource, OPERATOR, U256::ZERO, bit(20))
        .transfer(122, token, ALICE, ZERO)
        .push(122, TokenRegenerated { oldTokenId: token, newTokenId: versioned(1) }.encode_log_data())
        .transfer(122, versioned(1), ZERO, ALICE)
        .transfer(124, versioned(1), ALICE, BOB)
        .roles(124, resource, ALICE, owner_roles(), U256::ZERO)
        .roles(124, resource, BOB, U256::ZERO, owner_roles())
        .approve(125, BOB, OPERATOR, true)
        .approve(126, BOB, OPERATOR, false)
        .approve(127, BOB, OPERATOR, true)
        .push(131, ExpiryUpdated { tokenId: versioned(1), newExpiry: EXPIRY + 1_000,
            sender: account(ROOT_HOLDER) }.encode_log_data());
    logs
}

async fn publish(database: &TestDatabase, block: i64) -> Result<()> {
    publish_test_families_on(&database.pool, CHAIN, block).await?;
    database.seed_snapshot_selector_chain_positions(&json!({CHAIN: {
        "chain_id": CHAIN, "block_number": block, "block_hash": format!("0xhistory{block}"),
        "timestamp": bigname_storage::UnixSeconds::from(timestamp(1_700_000_000 + block)).internal_string(),
    }})).await
}

/// The registry's contract instance. `declared` puts it in an active ENSv2 registry manifest, as
/// the ETH registry is; otherwise the instance holds its address as a discovered registry does.
async fn declare(database: &TestDatabase, declared: bool) -> Result<()> {
    let instance = Uuid::from_u128(INSTANCE);
    sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1, $2, 'contract')")
        .bind(instance).bind(CHAIN).execute(&database.pool).await?;
    if !declared {
        sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
                active_from_block_number) VALUES ($1, $2, $3, 0)")
            .bind(instance).bind(CHAIN).bind(REGISTRY).execute(&database.pool).await?;
        return Ok(());
    }
    let manifest: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version, namespace, source_family,
            chain_id, deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES (1, 'ens', 'ens_v2_registry_l1', $1, 'fixture', 'active', 'fixture',
            'fixture/registry.toml', '{}') RETURNING manifest_id")
        .bind(CHAIN).fetch_one(&database.pool).await?;
    sqlx::query("INSERT INTO manifest_contract_instances (manifest_id, chain_id, declaration_kind, declaration_name,
            contract_instance_id, declared_address, role, proxy_kind)
        VALUES ($1, $2, 'contract', 'fixture', $3, $4, 'registry', 'none')")
        .bind(manifest).bind(CHAIN).bind(instance).bind(REGISTRY).execute(&database.pool).await?;
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
            active_from_block_number, source_manifest_id, provenance)
        VALUES ($1, $2, $3, 0, $4, jsonb_build_object('source', 'manifest_declaration', 'manifest_id', $4))")
        .bind(instance).bind(CHAIN).bind(REGISTRY).bind(manifest).execute(&database.pool).await?;
    Ok(())
}

/// The adapter's binding of alice.eth opened at `block`, as Interpret stores it.
fn binding(output: &bigname_adapters::schema_v2::BatchOutput, block: i64) -> Result<SurfaceBinding> {
    let opened = output
        .surface_bindings
        .iter()
        .find(|binding| binding.block_number == block)
        .with_context(|| format!("a binding opened at block {block}"))?;
    Ok(SurfaceBinding {
        surface_binding_id: opened.surface_binding_id,
        logical_name_id: "ens:alice.eth".to_owned(),
        resource_id: opened.resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        authority_arm: opened.authority_arm.clone(),
        active_from: opened.active_from,
        active_to: None,
        chain_id: CHAIN.to_owned(),
        block_hash: opened.block_hash.clone(),
        block_number: opened.block_number,
        provenance: opened.provenance.clone(),
        canonicality_state: CanonicalityState::Canonical,
    })
}

/// Interprets the registry's logs and stores what Interpret would, with the binding the
/// registration opens. `declared` puts the registry in an active manifest; without it the
/// registry is one discovery admitted. Returns the token resource and the adapter's output.
async fn seed(
    database: &TestDatabase,
    logs: Logs,
    declared: bool,
) -> Result<(Uuid, bigname_adapters::schema_v2::BatchOutput)> {
    let (manifest, admission) = registry_family();
    let (output, _) = prepare_schema_v2_batch_incremental(
        BatchInput {
            chain_id: CHAIN.into(),
            manifests: vec![manifest],
            discovery_rules: ["subregistry", "resolver", "registry_announcement"]
                .map(|edge_kind| DiscoveryRuleInput {
                    manifest_id: INSTANCE as i64,
                    edge_kind: edge_kind.into(),
                    from_role: Some("registry".into()),
                    admission: "reachable_from_root".into(),
                })
                .into(),
            admissions: vec![admission],
            prior_events: vec![],
            blocks: (120..=132)
                .map(|block| RawBlockInput {
                    chain_id: CHAIN.into(),
                    block_hash: format!("0xhistory{block}"),
                    block_number: block,
                    block_timestamp: timestamp(1_700_000_000 + block),
                    canonicality_state: "canonical".into(),
                })
                .collect(),
            raw_logs: logs.0,
        },
        None,
        StateCacheCapacity::Unlimited,
    )?
    .finish(vec![])?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    let token = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "PermissionChanged")
        .and_then(|event| event.resource_id)
        .context("the owner's grant names the token resource")?;

    seed_v2_history_blocks(database, 120..=132).await?;
    declare(database, declared).await?;
    let surface = output.name_surfaces.first().context("the registration names alice.eth")?;
    let mut name = collection_name_surface("ens:alice.eth", "alice.eth", &surface.namehash, 121);
    name.block_hash = "0xhistory121".into();
    upsert_test_name_surfaces(&database.pool, &[name]).await?;
    upsert_test_token_lineages(&database.pool, &output.token_lineages.iter()
        .map(|l| address_name_token_lineage(l.token_lineage_id, &l.block_hash, l.block_number))
        .collect::<Vec<_>>()).await?;
    upsert_test_resources(&database.pool, &output.resources.iter()
        .map(|r| address_name_resource(r.resource_id, r.token_lineage_id, &r.block_hash, r.block_number))
        .collect::<Vec<_>>()).await?;
    let events = output
        .normalized_events
        .iter()
        .map(|e| {
            let mut event = v2_history_event(&e.event_identity, e.logical_name_id.as_deref(),
                e.resource_id, &e.event_kind, e.block_number.unwrap());
            event.log_index = e.log_index;
            event.transaction_hash = e.transaction_hash.clone();
            event.source_family = e.source_family.clone();
            event.manifest_version = e.manifest_version;
            event.derivation_kind = e.derivation_kind.clone();
            event.raw_fact_ref = e.raw_fact_ref.clone();
            event.before_state = e.before_state.clone();
            event.after_state = e.after_state.clone();
            event
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    upsert_test_surface_bindings(&database.pool, &[binding(&output, 121)?]).await?;
    Ok((token, output))
}

/// `(address, scope kind, account owner, powers)` of every row, in served order.
fn rows(page: &Value) -> Vec<(String, String, Value, Value)> {
    page["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| (
            row["address"].as_str().unwrap().to_owned(),
            row["grant_scope"]["kind"].as_str().unwrap().to_owned(),
            row["grant_scope"]["detail"]["owner"].clone(),
            row["powers"].clone(),
        ))
        .collect()
}

fn operator_rows(page: &Value) -> Vec<&Value> {
    page["data"].as_array().unwrap().iter()
        .filter(|row| row["grant_relation"] == "operator").collect()
}

fn row(address: &str, kind: &str, owner: Option<&str>, powers: Value) -> (String, String, Value, Value) {
    (address.to_owned(), kind.to_owned(), json!(owner), powers)
}

#[tokio::test]
async fn registration_read_lists_operators_and_root_holders_through_the_token_life() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (token, output) = seed(&database, registry_logs(), true).await?;
    let uri = format!("/v1/permissions?registration_id={token}");
    let owner_powers = json!(["renew", "set_resolver", "admin_set_resolver", "can_transfer_admin"]);
    let operator_own = row(OPERATOR, "registry", None, json!(["set_subregistry"]));
    let root_rows = [
        row(ROOT_HOLDER, "root", None, json!(["registrar", "renew", "admin_registrar"])),
        row(ROOT_MIXED, "root", None, json!(["renew"])),
    ];

    // Alice owns the token and approved the operator, who also holds a role of its own.
    publish(&database, 123).await?;
    let page = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(rows(&page), vec![
        row(ALICE, "registry", None, owner_powers.clone()),
        row(OPERATOR, "account", Some(ALICE), owner_powers.clone()),
        operator_own.clone(),
        root_rows[0].clone(),
        root_rows[1].clone(),
    ], "{page:#}");
    assert_eq!(operator_rows(&page), vec![&json!({
        "address": OPERATOR,
        "grant_relation": "operator",
        "grant_scope": {"kind": "account", "detail": {"chain_id": 1,
            "authority_kind": "ens_v2_registry", "authority_contract": REGISTRY, "owner": ALICE}},
        "powers": owner_powers,
        "registration_id": token.to_string(),
        "name": "alice.eth",
        "authority_context": "resource_audit",
    })]);
    let root_row = &page["data"][3];
    assert_eq!(root_row["grant_scope"]["detail"], json!({"registry": {"chain_id": 1, "address": REGISTRY}}));
    assert_eq!(root_row["registration_id"], token.to_string(), "a root row of the token read");
    // A declared registry lists its operators and root holders; resolver approvals stay out.
    assert_eq!(page["meta"]["unlisted_permission_surfaces"], json!(["resolver_approvals"]), "{page:#}");

    // One row per page walks the same rows across the direct, operator and root boundary.
    let mut paged = Vec::new();
    let mut next = format!("{uri}&page_size=1");
    loop {
        let one = v2_permissions_payload_for_database(&database, &next).await?;
        paged.extend(rows(&one));
        let Some(cursor) = one["page"]["next_cursor"].as_str() else { break };
        next = format!("{uri}&page_size=1&cursor={cursor}");
    }
    assert_eq!(paged, rows(&page));
    // The name read is the same relation, with every row a row of the name's registration.
    let by_name = v2_permissions_payload_for_database(&database, "/v1/permissions?name=alice.eth").await?;
    assert_eq!(rows(&by_name), rows(&page), "{by_name:#}");
    for row in by_name["data"].as_array().unwrap() {
        assert_eq!(row["name"], "alice.eth", "{row}");
        assert_eq!(row["authority_context"], "current_for_name", "{row}");
        assert_eq!(row["registration_id"], token.to_string(), "{row}");
    }
    assert_eq!(by_name["meta"]["unlisted_permission_surfaces"], json!(["resolver_approvals"]));

    // The bounded read behind `role_summary` carries the operator and leaves the root holders
    // to the registry read.
    let summary = bigname_storage::load_bounded_effective_permissions_by_resource_ids(&database.pool, &[token], None, 1_000).await?;
    assert_eq!(
        summary.iter().map(|row| (row.subject.as_str(), row.scope.storage_key())).collect::<Vec<_>>(),
        vec![
            (ALICE, "registry".to_owned()),
            (OPERATOR, format!("account:{CHAIN}:ens_v2_registry:{REGISTRY}:{ALICE}")),
            (OPERATOR, "registry".to_owned()),
        ],
    );

    // The operator's own address read carries both of its rows.
    let by_operator = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?address={OPERATOR}&include=lineage")).await?;
    assert_eq!(rows(&by_operator), vec![
        row(OPERATOR, "account", Some(ALICE), owner_powers.clone()), operator_own.clone(),
    ], "{by_operator:#}");
    assert_eq!(by_operator["data"][0]["lineage"]["grant"], json!({"kind": "event"}));
    // `can_transfer_admin` on the root is nothing on a token; the root's own read keeps it.
    let filtered = v2_permissions_payload_for_database(&database,
        &format!("{uri}&address={ROOT_TRANSFER_ONLY}")).await?;
    assert_eq!(filtered["data"], json!([]), "{filtered:#}");
    let root = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?registry=1:{REGISTRY}")).await?;
    assert_eq!(rows(&root), vec![
        root_rows[0].clone(),
        row(ROOT_MIXED, "root", None, json!(["renew", "can_transfer_admin"])),
        row(ROOT_TRANSFER_ONLY, "root", None, json!(["can_transfer_admin"])),
    ], "{root:#}");
    assert!(root["data"].as_array().unwrap().iter().all(|row| row["registration_id"] != token.to_string()));

    // The token moved to Bob, who approved nobody: Alice's approval no longer reaches it.
    publish(&database, 124).await?;
    let page = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(rows(&page), vec![
        row(BOB, "registry", None, owner_powers.clone()),
        operator_own.clone(),
        root_rows[0].clone(),
        root_rows[1].clone(),
    ], "{page:#}");
    let by_operator = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?address={OPERATOR}")).await?;
    assert_eq!(rows(&by_operator), vec![operator_own.clone()], "{by_operator:#}");

    // Bob approves, revokes and approves again.
    for (block, approved) in [(125, true), (126, false), (129, true)] {
        publish(&database, block).await?;
        let page = v2_permissions_payload_for_database(&database, &uri).await?;
        let owners = operator_rows(&page).iter()
            .map(|row| row["grant_scope"]["detail"]["owner"].clone()).collect::<Vec<_>>();
        assert_eq!(owners, if approved { vec![json!(BOB)] } else { vec![] }, "block {block}: {page:#}");
    }

    // The entry expires in block 130: the registry reports no owner, so nobody's operators.
    // The path expiry closes the binding the registration opened.
    let closure = output
        .binding_closures
        .iter()
        .find(|closure| closure.block_number == 130)
        .context("a binding closure at block 130")?;
    let expired = SurfaceBinding { active_to: Some(closure.active_to), ..binding(&output, 121)? };
    upsert_test_surface_bindings(&database.pool, &[expired]).await?;
    publish(&database, 130).await?;
    let page = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(page["data"], json!([]), "the expired registration serves no row: {page:#}");
    let by_operator = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?address={OPERATOR}")).await?;
    assert!(operator_rows(&by_operator).is_empty(), "{by_operator:#}");

    // A root renewer revives the entry; Bob still holds the token and his approval stands.
    upsert_test_surface_bindings(&database.pool, &[binding(&output, 131)?]).await?;
    publish(&database, 131).await?;
    let page = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(rows(&page), vec![
        row(BOB, "registry", None, owner_powers.clone()),
        row(OPERATOR, "account", Some(BOB), owner_powers),
        operator_own,
        root_rows[0].clone(),
        root_rows[1].clone(),
    ], "{page:#}");
    database.cleanup().await
}

/// Only the token owner's own token roles reach an operator, and only an account the owner
/// approved: not the owner's root roles, not the operator's operators, not the owner itself.
#[tokio::test]
async fn operator_rows_carry_only_the_owners_token_roles() -> Result<()> {
    const FAR: u64 = 1_800_000_000;
    let database = TestDatabase::new_migrated().await?;
    let mut logs = Logs::default();
    logs.roles(120, U256::ZERO, ALICE, U256::ZERO, bit(0) | bit(128))
        .register(121, FAR)
        .approve(122, ALICE, OPERATOR, true)
        .approve(122, OPERATOR, BOB, true)
        .approve(122, ALICE, ALICE, true)
        // Alice gives up every role on the token and keeps the token and her root roles.
        // The revocation regenerates the token, as a grant does.
        .roles(124, versioned(0), ALICE, owner_roles(), U256::ZERO)
        .transfer(124, versioned(0), ALICE, ZERO)
        .push(124, TokenRegenerated { oldTokenId: versioned(0), newTokenId: versioned(1) }.encode_log_data())
        .transfer(124, versioned(1), ZERO, ALICE);
    let (token, _) = seed(&database, logs, false).await?;
    let uri = format!("/v1/permissions?registration_id={token}");
    let owner_powers = json!(["renew", "set_resolver", "admin_set_resolver", "can_transfer_admin"]);
    let alice_root = row(ALICE, "root", None, json!(["registrar", "admin_registrar"]));

    publish(&database, 123).await?;
    let page = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(rows(&page), vec![
        row(ALICE, "registry", None, owner_powers.clone()),
        alice_root.clone(),
        row(OPERATOR, "account", Some(ALICE), owner_powers),
    ], "{page:#}");
    // The registry is not declared: its code may add holders these rows do not show.
    assert_eq!(page["meta"]["unlisted_permission_surfaces"],
        json!(["ens_v2_registry_operators", "resolver_approvals"]), "{page:#}");
    let by_bob = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?address={BOB}")).await?;
    assert_eq!(by_bob["data"], json!([]), "an operator's operator has nothing: {by_bob:#}");

    publish(&database, 124).await?;
    let page = v2_permissions_payload_for_database(&database, &uri).await?;
    assert_eq!(rows(&page), vec![alice_root], "{page:#}");
    // A registration whose only rows are root holders still has its restriction block: no
    // admin of any token role is left on the token or the root.
    assert_eq!(page["restrictions"], json!({
        "kind": "ens_v2_registry",
        "locked_roles": ["unregister", "renew", "set_subregistry", "set_resolver", "transfer"],
        "registration_id": token.to_string(),
    }), "{page:#}");
    let by_operator = v2_permissions_payload_for_database(&database,
        &format!("/v1/permissions?address={OPERATOR}")).await?;
    assert_eq!(by_operator["data"], json!([]), "{by_operator:#}");
    database.cleanup().await
}
