//! TYR-449: the mainnet profile's numeric BaseRegistrar `NameRegistered` and `NameRenewed` are
//! lease lifecycle rows unless an admitted controller event for the same node follows in the same
//! transaction. Every fixture reads the checked-in mainnet manifests at their real addresses.
use super::*;

const REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const LEGACY_CONTROLLER: &str = "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5";
const NAME_WRAPPER: &str = "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401";
const MIGRATOR: &str = "0x00000000000000000000000000000000000000c1";
const HOLDER: &str = "0x00000000000000000000000000000000000000c2";
const NEXT_HOLDER: &str = "0x00000000000000000000000000000000000000c3";
const REGISTRY_MANIFEST: i64 = 4491;
const REGISTRAR_MANIFEST: i64 = 4492;
const WRAPPER_MANIFEST: i64 = 4493;
const DAY: i64 = 86_400;
/// The migrated lease's expiry. Blocks below 10 are live, 10 to 19 are in grace, and 20 on are
/// past grace.
const EXPIRY: u64 = 1_000;
/// The label the migration mint never reveals.
const LABEL: &str = "unlearned";

mod base {
    alloy_sol_types::sol! {
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NameRenewed(uint256 indexed id, uint256 expires);
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    }
}

mod legacy {
    alloy_sol_types::sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
        event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
    }
}

fn timestamp(block: i64) -> i64 {
    let expiry = i64::try_from(EXPIRY).expect("small expiry");
    if block < 10 {
        block
    } else if block < 20 {
        expiry + 30 * DAY + block
    } else {
        expiry + 90 * DAY + block
    }
}

fn checked_in(source_family: &str, manifest_id: i64) -> ManifestInput {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet");
    let repository = bigname_manifests::load_repository(&root).expect("checked-in mainnet profile");
    let manifest = repository
        .manifests()
        .iter()
        .map(|loaded| &loaded.manifest)
        .find(|manifest| {
            manifest.source_family == source_family
                && manifest.rollout_status == bigname_manifests::RolloutStatus::Active
        })
        .unwrap_or_else(|| panic!("an active mainnet {source_family} manifest"));
    ManifestInput {
        manifest_id,
        manifest_version: 1,
        namespace: "ens".to_owned(),
        source_family: source_family.to_owned(),
        chain_id: CHAIN.to_owned(),
        deployment_label: "mainnet".to_owned(),
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        payload_json: serde_json::to_string(manifest).expect("serializable manifest"),
    }
}

/// The checked-in registrar manifest with the numeric declarations the mainnet profile carried
/// before TYR-449, which interpreted neither numeric event as a lifecycle row.
fn registrar_before_numeric_lifecycle() -> ManifestInput {
    let mut manifest = checked_in("ens_v1_registrar_l1", REGISTRAR_MANIFEST);
    let mut payload: serde_json::Value = serde_json::from_str(&manifest.payload_json).unwrap();
    for event in payload["abi"]["events"].as_array_mut().unwrap() {
        let declared = match event["fragment"].as_str().unwrap() {
            "event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires)" => {
                json!(["RegistrationReleased"])
            }
            "event NameRenewed(uint256 indexed id, uint256 expires)" => {
                json!(["RegistrationRenewed", "ExpiryChanged"])
            }
            _ => continue,
        };
        event["normalized_events"] = declared;
    }
    manifest.payload_json = payload.to_string();
    manifest
}

fn admitted(manifest_id: i64, role: &str, address: &str, instance: u128) -> AddressAdmissionInput {
    let mut value = admission(manifest_id, role);
    value.address = address.to_owned();
    value.contract_instance_id = Uuid::from_u128(instance);
    value
}

fn admissions() -> Vec<AddressAdmissionInput> {
    vec![
        admitted(REGISTRY_MANIFEST, "registry", REGISTRY, 4491),
        admitted(REGISTRAR_MANIFEST, "registrar", BASE_REGISTRAR, 4492),
        admitted(
            REGISTRAR_MANIFEST,
            "legacy_registrar_controller",
            LEGACY_CONTROLLER,
            4494,
        ),
        admitted(WRAPPER_MANIFEST, "name_wrapper", NAME_WRAPPER, 4493),
    ]
}

fn manifests(registrar: ManifestInput) -> Vec<ManifestInput> {
    vec![
        checked_in("ens_v1_registry_l1", REGISTRY_MANIFEST),
        registrar,
        checked_in("ens_v1_wrapper_l1", WRAPPER_MANIFEST),
    ]
}

/// Logs in order, each transaction a block's `transaction` index, log indexes block-wide.
#[derive(Default)]
struct Logs(Vec<RawLogInput>);

impl Logs {
    fn push(
        &mut self,
        block: i64,
        transaction: i64,
        emitter: &str,
        encoded: alloy_primitives::LogData,
    ) {
        let log_index = i64::try_from(
            self.0
                .iter()
                .filter(|raw| raw.block_number == block)
                .count(),
        )
        .unwrap();
        let mut raw = raw_at_transaction(encoded, block, transaction, log_index, emitter);
        raw.block_timestamp =
            OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(timestamp(block));
        self.0.push(raw);
    }

    fn input(&self, registrar: ManifestInput, through: i64) -> BatchInput {
        BatchInput {
            chain_id: CHAIN.to_owned(),
            manifests: manifests(registrar),
            discovery_rules: Vec::new(),
            admissions: admissions(),
            prior_events: Vec::new(),
            blocks: (1..=through).map(block).collect(),
            raw_logs: self.0.clone(),
        }
    }

    fn run(&self, through: i64) -> anyhow::Result<BatchOutput> {
        interpret_test_batch(self.input(
            checked_in("ens_v1_registrar_l1", REGISTRAR_MANIFEST),
            through,
        ))
    }
}

fn block(number: i64) -> RawBlockInput {
    RawBlockInput {
        chain_id: CHAIN.to_owned(),
        block_hash: format!("block-{number}"),
        block_number: number,
        block_timestamp: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(timestamp(number)),
        canonicality_state: "canonical".to_owned(),
    }
}

fn labelhash(label: &str) -> B256 {
    keccak256(label.as_bytes())
}

fn token(label: &str) -> U256 {
    U256::from_be_slice(labelhash(label).as_slice())
}

fn node(label: &str) -> String {
    super::common::namehash(&[label.to_owned(), "eth".to_owned()])
}

fn name(label: &str) -> String {
    format!("ens:{}", node(label))
}

fn eth() -> B256 {
    super::common::namehash(&["eth".to_owned()])
        .parse()
        .unwrap()
}

fn address(value: &str) -> Address {
    value.parse().unwrap()
}

/// The registry's `eth` record, so `[labelhash].eth` is a proven path.
fn eth_root(logs: &mut Logs) {
    logs.push(
        1,
        0,
        REGISTRY,
        super::v1_registry::NewOwner {
            node: B256::ZERO,
            label: labelhash("eth"),
            owner: address(BASE_REGISTRAR),
        }
        .encode_log_data(),
    );
}

fn numeric_registration(label: &str, owner: &str, expires: u64) -> alloy_primitives::LogData {
    base::NameRegistered {
        id: token(label),
        owner: address(owner),
        expires: U256::from(expires),
    }
    .encode_log_data()
}

fn numeric_renewal(label: &str, expires: u64) -> alloy_primitives::LogData {
    base::NameRenewed {
        id: token(label),
        expires: U256::from(expires),
    }
    .encode_log_data()
}

fn token_transfer(label: &str, from: &str, to: &str) -> alloy_primitives::LogData {
    base::Transfer {
        from: address(from),
        to: address(to),
        tokenId: token(label),
    }
    .encode_log_data()
}

fn new_owner(label: &str, owner: &str) -> alloy_primitives::LogData {
    super::v1_registry::NewOwner {
        node: eth(),
        label: labelhash(label),
        owner: address(owner),
    }
    .encode_log_data()
}

fn registry_transfer(label: &str, owner: &str) -> alloy_primitives::LogData {
    super::v1_registry::Transfer {
        node: node(label).parse().unwrap(),
        owner: address(owner),
    }
    .encode_log_data()
}

fn controller_registration(label: &str, owner: &str, expires: u64) -> alloy_primitives::LogData {
    legacy::NameRegistered {
        name: label.to_owned(),
        label: labelhash(label),
        owner: address(owner),
        cost: U256::from(1_u64),
        expires: U256::from(expires),
    }
    .encode_log_data()
}

fn controller_renewal(label: &str, expires: u64) -> alloy_primitives::LogData {
    legacy::NameRenewed {
        name: label.to_owned(),
        label: labelhash(label),
        cost: U256::from(1_u64),
        expires: U256::from(expires),
    }
    .encode_log_data()
}

/// The 2020 migration mint in one transaction at `block`: the migrator registers the token to
/// itself, writes the registry record to HOLDER and hands HOLDER the token
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L152 @ ens_v1@91c966f).
fn migration_mint(logs: &mut Logs, block: i64, label: &str) {
    logs.push(
        block,
        0,
        BASE_REGISTRAR,
        token_transfer(label, ZERO_ADDRESS, MIGRATOR),
    );
    logs.push(block, 0, REGISTRY, new_owner(label, MIGRATOR));
    logs.push(
        block,
        0,
        BASE_REGISTRAR,
        numeric_registration(label, MIGRATOR, EXPIRY),
    );
    logs.push(block, 0, REGISTRY, registry_transfer(label, HOLDER));
    logs.push(
        block,
        0,
        BASE_REGISTRAR,
        token_transfer(label, MIGRATOR, HOLDER),
    );
}

/// A legacy controller registration for HOLDER in one transaction at `block`: the controller
/// registers to itself, reclaims the record for HOLDER, hands over the token and emits its own
/// label-bearing event
/// (upstream: .refs/ens_v1/deployments/archive/ETHRegistrarController_mainnet_9380471.sol/ETHRegistrarController_mainnet_9380471.json:L35 @ ens_v1@91c966f).
fn controller_mint(
    logs: &mut Logs,
    block: i64,
    transaction: i64,
    label: &str,
    owner: &str,
    expires: u64,
) {
    logs.push(
        block,
        transaction,
        BASE_REGISTRAR,
        token_transfer(label, ZERO_ADDRESS, LEGACY_CONTROLLER),
    );
    logs.push(
        block,
        transaction,
        REGISTRY,
        new_owner(label, LEGACY_CONTROLLER),
    );
    logs.push(
        block,
        transaction,
        BASE_REGISTRAR,
        numeric_registration(label, LEGACY_CONTROLLER, expires),
    );
    logs.push(block, transaction, REGISTRY, new_owner(label, owner));
    logs.push(
        block,
        transaction,
        BASE_REGISTRAR,
        token_transfer(label, LEGACY_CONTROLLER, owner),
    );
    logs.push(
        block,
        transaction,
        LEGACY_CONTROLLER,
        controller_registration(label, owner, expires),
    );
}

fn of_kind<'a>(output: &'a BatchOutput, kind: &str) -> Vec<&'a NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == kind && event.source_family == "ens_v1_registrar_l1")
        .collect()
}

fn at_block(events: Vec<&NormalizedEvent>, number: i64) -> Vec<&NormalizedEvent> {
    events
        .into_iter()
        .filter(|event| event.block_number == Some(number))
        .collect()
}

/// The single registrar grant of the migration mint at block 2, with its lease.
fn mint_grant(output: &BatchOutput) -> (&NormalizedEvent, Uuid) {
    let grants = at_block(of_kind(output, "RegistrationGranted"), 2);
    assert_eq!(
        grants.len(),
        1,
        "the migration mint grants one lease: {grants:#?}"
    );
    (grants[0], grants[0].resource_id.expect("a lease resource"))
}

/// The authority the last epoch change of `logical_name_id` at `block` moves to.
fn final_authority(
    output: &BatchOutput,
    logical_name_id: &str,
    block: i64,
) -> (Option<Uuid>, serde_json::Value) {
    output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "AuthorityEpochChanged"
                && event.block_number == Some(block)
                && event.logical_name_id.as_deref() == Some(logical_name_id)
        })
        .max_by_key(|event| event.log_index)
        .map(|event| {
            (
                event.resource_id,
                event.after_state["authority_kind"].clone(),
            )
        })
        .expect("an authority epoch change")
}

/// Row 1: the migration mint grants one lease named by namehash with the token id and the
/// migrator as registrant. The registry write to HOLDER after the registration opens a
/// registry-only interval, and the token's move to HOLDER brings the lease back, so the
/// transaction ends on the lease.
#[test]
fn the_migration_mint_is_a_lease_named_by_namehash() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    let output = logs.run(2)?;
    let (grant, lease) = mint_grant(&output);
    assert_eq!(grant.logical_name_id.as_deref(), Some(name(LABEL).as_str()));
    assert_eq!(
        grant.after_state["token_id"],
        format!("{:#x}", labelhash(LABEL))
    );
    assert_eq!(grant.after_state["registrant"], MIGRATOR);
    assert_eq!(grant.after_state["authority_kind"], "registrar");
    for kind in ["ExpiryChanged", "PermissionChanged"] {
        let rows = at_block(of_kind(&output, kind), 2);
        assert!(
            rows.iter().any(|row| row.resource_id == Some(lease)
                && row.logical_name_id.as_deref() == Some(name(LABEL).as_str())),
            "{kind}: {rows:#?}"
        );
    }
    let registrar_resources = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && matches!(
                    event.event_kind.as_str(),
                    "RegistrationGranted" | "ExpiryChanged" | "TokenControlTransferred"
                )
        })
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        registrar_resources,
        [lease].into(),
        "one lease across the mint"
    );
    let divergence = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "AuthorityTransferred"
                && event.after_state["source_event"] == "Transfer"
                && event.after_state["authority_kind"] == "registry_only"
        })
        .expect("the registry write after the registration diverges");
    assert_ne!(divergence.resource_id, Some(lease));
    let transferred = at_block(of_kind(&output, "TokenControlTransferred"), 2);
    assert_eq!(transferred.len(), 1, "{transferred:#?}");
    assert_eq!(transferred[0].resource_id, Some(lease));
    assert_eq!(transferred[0].after_state["to"], HOLDER);
    assert_eq!(
        final_authority(&output, &name(LABEL), 2),
        (Some(lease), json!("registrar")),
        "the mint ends on the lease"
    );
    Ok(())
}

/// Row 2: a later registrar transfer of the numeric lease is named.
#[test]
fn a_transfer_of_a_numeric_lease_is_named() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    logs.push(
        3,
        0,
        BASE_REGISTRAR,
        token_transfer(LABEL, HOLDER, NEXT_HOLDER),
    );
    let output = logs.run(3)?;
    let (_, lease) = mint_grant(&output);
    let transferred = at_block(of_kind(&output, "TokenControlTransferred"), 3);
    assert_eq!(transferred.len(), 1, "{transferred:#?}");
    assert_eq!(transferred[0].resource_id, Some(lease));
    assert_eq!(
        transferred[0].logical_name_id.as_deref(),
        Some(name(LABEL).as_str())
    );
    assert_eq!(transferred[0].after_state["to"], NEXT_HOLDER);
    Ok(())
}

/// Row 4: past its grace period the numeric lease is released by name, once.
#[test]
fn a_numeric_lease_past_grace_releases_by_name() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    let output = logs.run(21)?;
    let (_, lease) = mint_grant(&output);
    let releases = of_kind(&output, "RegistrationReleased");
    assert_eq!(releases.len(), 1, "{releases:#?}");
    assert_eq!(releases[0].block_number, Some(20));
    assert_eq!(releases[0].resource_id, Some(lease));
    assert_eq!(
        releases[0].logical_name_id.as_deref(),
        Some(name(LABEL).as_str())
    );
    Ok(())
}

/// Row 5a: a controller renewal that reveals the label renews the numeric lease under the same
/// name and leaves every earlier row as it was.
#[test]
fn a_later_controller_renewal_keeps_the_numeric_lease_identity() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    let before = logs.run(2)?;
    logs.push(3, 0, BASE_REGISTRAR, numeric_renewal(LABEL, 50_000_000));
    logs.push(
        3,
        0,
        LEGACY_CONTROLLER,
        controller_renewal(LABEL, 50_000_000),
    );
    let output = logs.run(3)?;
    let (_, lease) = mint_grant(&output);
    assert!(at_block(of_kind(&output, "RegistrationGranted"), 3).is_empty());
    let renewed = at_block(of_kind(&output, "RegistrationRenewed"), 3);
    assert_eq!(renewed.len(), 1, "{renewed:#?}");
    assert_eq!(renewed[0].resource_id, Some(lease));
    assert_eq!(
        renewed[0].logical_name_id.as_deref(),
        Some(name(LABEL).as_str())
    );
    assert_eq!(
        renewed[0].raw_fact_ref["emitting_address"], LEGACY_CONTROLLER,
        "the controller owns the renewal"
    );
    assert!(
        output
            .name_surfaces
            .iter()
            .any(|surface| surface.logical_name_id == name(LABEL)),
        "the label lands on the same surface"
    );
    let earlier = |output: &BatchOutput| {
        output
            .normalized_events
            .iter()
            .filter(|event| event.block_number < Some(3))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(earlier(&output), earlier(&before));
    Ok(())
}

/// The logs of row 6: a legacy controller registration in one transaction.
fn controller_registration_logs() -> Logs {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    controller_mint(&mut logs, 2, 0, "known", HOLDER, EXPIRY);
    logs
}

/// Rows 6 and 7: with a controller event for the node after it in the transaction, the numeric
/// event is skipped and the output is exactly what the profile derived before TYR-449.
fn assert_unchanged(logs: &Logs, through: i64) -> anyhow::Result<BatchOutput> {
    let output = logs.run(through)?;
    let before = interpret_test_batch(logs.input(registrar_before_numeric_lifecycle(), through))?;
    assert_eq!(output.normalized_events, before.normalized_events);
    assert_eq!(output.surface_bindings, before.surface_bindings);
    assert_eq!(output.binding_closures, before.binding_closures);
    assert_eq!(output.resources, before.resources);
    assert_eq!(output.name_surfaces, before.name_surfaces);
    Ok(output)
}

/// Row 6: a numeric registration with the controller's after it in the same transaction gives one
/// grant and one lease, at the controller's log.
#[test]
fn a_controller_registration_still_owns_its_lease() -> anyhow::Result<()> {
    let logs = controller_registration_logs();
    let output = assert_unchanged(&logs, 2)?;
    let grants = of_kind(&output, "RegistrationGranted");
    assert_eq!(grants.len(), 1, "{grants:#?}");
    assert_eq!(
        grants[0].raw_fact_ref["emitting_address"],
        LEGACY_CONTROLLER
    );
    let leases = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_registrar_l1"
                && matches!(
                    event.event_kind.as_str(),
                    "RegistrationGranted" | "ExpiryChanged" | "TokenControlTransferred"
                )
        })
        .filter_map(|event| event.resource_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(leases.len(), 1, "{leases:?}");
    Ok(())
}

/// Row 7: a numeric renewal with the controller's after it in the same transaction gives one
/// renewal, as before.
#[test]
fn a_controller_renewal_still_owns_its_renewal() -> anyhow::Result<()> {
    let mut logs = controller_registration_logs();
    logs.push(3, 0, BASE_REGISTRAR, numeric_renewal("known", 50_000_000));
    logs.push(
        3,
        0,
        LEGACY_CONTROLLER,
        controller_renewal("known", 50_000_000),
    );
    let output = assert_unchanged(&logs, 3)?;
    let renewed = at_block(of_kind(&output, "RegistrationRenewed"), 3);
    assert_eq!(renewed.len(), 1, "{renewed:#?}");
    assert_eq!(
        renewed[0].raw_fact_ref["emitting_address"],
        LEGACY_CONTROLLER
    );
    assert_eq!(of_kind(&output, "RegistrationGranted").len(), 1);
    Ok(())
}

/// Row 7b: a controller event covers only the numeric event of its kind just before it. A
/// numeric renewal through another caller earlier in the transaction is still its own renewal.
#[test]
fn an_uncovered_numeric_renewal_before_a_controller_renewal_still_renews() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    logs.push(3, 0, BASE_REGISTRAR, numeric_renewal(LABEL, 40_000_000));
    logs.push(3, 0, BASE_REGISTRAR, numeric_renewal(LABEL, 50_000_000));
    logs.push(
        3,
        0,
        LEGACY_CONTROLLER,
        controller_renewal(LABEL, 50_000_000),
    );
    let output = logs.run(3)?;
    let (_, lease) = mint_grant(&output);
    let renewed = at_block(of_kind(&output, "RegistrationRenewed"), 3);
    let emitted = renewed
        .iter()
        .map(|event| {
            (
                event.raw_fact_ref["emitting_address"].clone(),
                event.after_state["expiry"].clone(),
                event.resource_id,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        emitted,
        [
            (json!(BASE_REGISTRAR), json!(40_000_000), Some(lease)),
            (json!(LEGACY_CONTROLLER), json!(50_000_000), Some(lease)),
        ],
        "{renewed:#?}"
    );
    Ok(())
}

/// The Graveyard's launch block in these fixtures: every migration-family declaration starts here.
const LAUNCH: i64 = 30;
const MIGRATION_MANIFEST: i64 = 4495;
const GRAVEYARD: &str = "0xda021a2c1c2f99574d861ef8c6fa745851beb7f6";
/// The expiry the Graveyard registers a cleared token with: the largest `uint64` less the grace
/// period, so the token never enters grace.
const GRAVEYARD_CLEANUP_EXPIRY: u64 = u64::MAX - 90 * 86_400;

/// `logs` under `registrar` with the checked-in Mainnet migration manifest admitted from
/// [`LAUNCH`] at its real addresses.
fn with_migration(logs: &Logs, registrar: ManifestInput, through: i64) -> BatchInput {
    let mut input = logs.input(registrar, through);
    input
        .manifests
        .push(checked_in("ens_v2_migration_l1", MIGRATION_MANIFEST));
    for (instance, (role, address)) in [
        (
            "unlocked_migration_controller",
            "0xab821ca676cf2e0790fa273526a165cfa86aeb64",
        ),
        (
            "locked_migration_controller",
            "0x3cea18620883a69ed349b23328a3b6302598e51a",
        ),
        ("graveyard", GRAVEYARD),
        (
            "ens_v1_renewal_bridge",
            "0x56c447f308f463a01121c41e72b0e7e7557fcb68",
        ),
        (
            "verifiable_factory",
            "0x77510ba162dfea3d8989e3f0928a691b345730d2",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut admission = admitted(
            MIGRATION_MANIFEST,
            role,
            address,
            4500 + u128::try_from(instance).unwrap(),
        );
        admission.active_from_block = Some(LAUNCH);
        input.admissions.push(admission);
    }
    input
}

/// Row 12: a post-launch Graveyard cleanup of a released lease carries only registrar and
/// registry logs and no controller event. It opens no lease, and the output is exactly what the
/// profile derived before TYR-449
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f).
#[test]
fn a_graveyard_cleanup_is_not_a_grant() -> anyhow::Result<()> {
    let mut logs = controller_registration_logs();
    logs.push(
        LAUNCH,
        0,
        BASE_REGISTRAR,
        token_transfer("known", HOLDER, ZERO_ADDRESS),
    );
    logs.push(
        LAUNCH,
        0,
        BASE_REGISTRAR,
        token_transfer("known", ZERO_ADDRESS, GRAVEYARD),
    );
    logs.push(LAUNCH, 0, REGISTRY, new_owner("known", GRAVEYARD));
    logs.push(
        LAUNCH,
        0,
        BASE_REGISTRAR,
        numeric_registration("known", GRAVEYARD, GRAVEYARD_CLEANUP_EXPIRY),
    );
    let output = interpret_test_batch(with_migration(
        &logs,
        checked_in("ens_v1_registrar_l1", REGISTRAR_MANIFEST),
        LAUNCH,
    ))?;
    let before = interpret_test_batch(with_migration(
        &logs,
        registrar_before_numeric_lifecycle(),
        LAUNCH,
    ))?;
    let grants = at_block(of_kind(&output, "RegistrationGranted"), LAUNCH);
    assert!(grants.is_empty(), "{grants:#?}");
    assert!(
        output.normalized_events.iter().any(|event| {
            event.source_family == "ens_v2_migration_l1"
                && event.after_state["lifecycle_classification"] == "graveyard_cleanup"
        }),
        "the cleanup is classified as one"
    );
    assert_eq!(output.normalized_events, before.normalized_events);
    assert_eq!(output.surface_bindings, before.surface_bindings);
    assert_eq!(output.binding_closures, before.binding_closures);
    assert_eq!(output.resources, before.resources);
    assert_eq!(output.name_surfaces, before.name_surfaces);
    Ok(())
}

/// The basis for the redo's row estimate: the interpret rows one migration mint adds in its own
/// transaction, and those its lapse adds through release, each against the declarations before
/// TYR-449.
#[test]
fn one_migration_mint_and_its_lapse_add_a_counted_number_of_rows() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    let count = |registrar: ManifestInput| -> anyhow::Result<(Vec<String>, Vec<String>, usize)> {
        let output = interpret_test_batch(logs.input(registrar, 21))?;
        let kinds = |mint: bool| {
            let mut kinds = output
                .normalized_events
                .iter()
                .filter(|event| (event.block_number == Some(2)) == mint)
                .filter(|event| event.block_number > Some(1))
                .map(|event| event.event_kind.clone())
                .collect::<Vec<_>>();
            kinds.sort();
            kinds
        };
        Ok((
            kinds(true),
            kinds(false),
            output.surface_bindings.len() + output.binding_closures.len() + output.resources.len(),
        ))
    };
    let (mint, lapse, other) = count(checked_in("ens_v1_registrar_l1", REGISTRAR_MANIFEST))?;
    let (mint_before, lapse_before, other_before) = count(registrar_before_numeric_lifecycle())?;
    let difference = |now: usize, before: usize| now as i64 - before as i64;
    let added = (
        difference(mint.len(), mint_before.len()),
        difference(lapse.len(), lapse_before.len()),
        difference(other, other_before),
    );
    assert_eq!(
        added,
        (10, 4, 2),
        "normalized rows added by the mint and by the lapse, other interpret rows added\n\
         mint now {mint:?}\nmint before {mint_before:?}\nlapse now {lapse:?}\n\
         lapse before {lapse_before:?}\nother {other} before {other_before}"
    );
    Ok(())
}

/// Row 8: wrapping a migrated name after the renewal that revealed its label records the numeric
/// lease as the wrapped registrar resource.
#[test]
fn a_wrap_after_a_numeric_lease_records_that_lease() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    logs.push(3, 0, BASE_REGISTRAR, numeric_renewal(LABEL, 50_000_000));
    logs.push(
        3,
        0,
        LEGACY_CONTROLLER,
        controller_renewal(LABEL, 50_000_000),
    );
    logs.push(
        4,
        0,
        BASE_REGISTRAR,
        token_transfer(LABEL, HOLDER, NAME_WRAPPER),
    );
    logs.push(4, 0, REGISTRY, new_owner(LABEL, NAME_WRAPPER));
    let mut dns = vec![u8::try_from(LABEL.len())?];
    dns.extend(LABEL.as_bytes());
    dns.extend(b"\x03eth\0");
    logs.push(
        4,
        0,
        NAME_WRAPPER,
        super::NameWrapped {
            node: node(LABEL).parse()?,
            name: dns.into(),
            owner: address(HOLDER),
            fuses: 196_608,
            expiry: 50_000_000 + 90 * 86_400,
        }
        .encode_log_data(),
    );
    let output = logs.run(4)?;
    let (_, lease) = mint_grant(&output);
    let wrapped = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_wrapper_l1"
                && event.after_state["source_event"] == "NameWrapped"
                && matches!(
                    event.event_kind.as_str(),
                    "TokenControlTransferred" | "ExpiryChanged" | "SurfaceBound"
                )
        })
        .collect::<Vec<_>>();
    assert!(!wrapped.is_empty());
    for row in wrapped {
        assert_eq!(
            row.after_state["wrapped_registrar_resource_id"],
            lease.to_string(),
            "{row:#?}"
        );
    }
    Ok(())
}

/// The mint at block 2 released at block 20, then `logs` from block 21.
fn released_then(extend: impl FnOnce(&mut Logs)) -> anyhow::Result<(BatchOutput, Uuid)> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    extend(&mut logs);
    let output = logs.run(21)?;
    let (_, lease) = mint_grant(&output);
    assert_eq!(
        at_block(of_kind(&output, "RegistrationReleased"), 20).len(),
        1
    );
    Ok((output, lease))
}

/// Row 9: a controller registration after the release starts a new lease at the controller's
/// log, and the numeric registration in its transaction is skipped.
#[test]
fn a_re_registration_after_release_starts_a_new_lease() -> anyhow::Result<()> {
    let (output, lease) = released_then(|logs| {
        controller_mint(logs, 21, 0, LABEL, NEXT_HOLDER, 90_000_000);
    })?;
    let grants = at_block(of_kind(&output, "RegistrationGranted"), 21);
    assert_eq!(grants.len(), 1, "{grants:#?}");
    assert_eq!(
        grants[0].raw_fact_ref["emitting_address"],
        LEGACY_CONTROLLER
    );
    assert_ne!(grants[0].resource_id, Some(lease));
    assert_eq!(
        grants[0].logical_name_id.as_deref(),
        Some(name(LABEL).as_str())
    );
    Ok(())
}

/// Row 10: a registration on the registrar alone after the release is a named successor lease.
#[test]
fn a_register_only_grant_after_release_is_a_named_successor_lease() -> anyhow::Result<()> {
    let (output, lease) = released_then(|logs| {
        logs.push(
            21,
            0,
            BASE_REGISTRAR,
            token_transfer(LABEL, ZERO_ADDRESS, NEXT_HOLDER),
        );
        logs.push(
            21,
            0,
            BASE_REGISTRAR,
            numeric_registration(LABEL, NEXT_HOLDER, 90_000_000),
        );
    })?;
    let grants = at_block(of_kind(&output, "RegistrationGranted"), 21);
    assert_eq!(grants.len(), 1, "{grants:#?}");
    assert_ne!(grants[0].resource_id, Some(lease));
    assert_eq!(
        grants[0].logical_name_id.as_deref(),
        Some(name(LABEL).as_str())
    );
    assert_eq!(grants[0].after_state["registrant"], NEXT_HOLDER);
    Ok(())
}

/// Row 13a: a numeric renewal with no controller event renews the numeric lease by name and moves
/// its release with the new expiry.
#[test]
fn a_numeric_renewal_without_a_controller_event_moves_the_expiry() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    logs.push(3, 0, BASE_REGISTRAR, numeric_renewal(LABEL, 50_000_000));
    let output = logs.run(21)?;
    let (_, lease) = mint_grant(&output);
    assert!(at_block(of_kind(&output, "RegistrationGranted"), 3).is_empty());
    let renewed = at_block(of_kind(&output, "RegistrationRenewed"), 3);
    assert_eq!(renewed.len(), 1, "{renewed:#?}");
    assert_eq!(renewed[0].resource_id, Some(lease));
    assert_eq!(
        renewed[0].logical_name_id.as_deref(),
        Some(name(LABEL).as_str())
    );
    assert_eq!(renewed[0].after_state["registrant"], HOLDER);
    assert_eq!(renewed[0].after_state["expiry"], 50_000_000);
    assert!(
        of_kind(&output, "RegistrationReleased").is_empty(),
        "the renewal moved the release"
    );
    Ok(())
}

/// Row 13b: a numeric renewal with no controller event and no retained lease is a synthetic
/// grant with a zero registrant, as a controller renewal of an unknown lease is.
#[test]
fn a_numeric_renewal_without_a_lease_is_a_synthetic_grant() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    logs.push(2, 0, REGISTRY, new_owner(LABEL, HOLDER));
    logs.push(3, 0, BASE_REGISTRAR, numeric_renewal(LABEL, 50_000_000));
    let output = logs.run(3)?;
    let grants = at_block(of_kind(&output, "RegistrationGranted"), 3);
    assert_eq!(grants.len(), 1, "{grants:#?}");
    assert_eq!(grants[0].after_state["registrant"], ZERO_ADDRESS);
    assert_eq!(
        grants[0].logical_name_id.as_deref(),
        Some(name(LABEL).as_str())
    );
    let renewed = at_block(of_kind(&output, "RegistrationRenewed"), 3);
    assert_eq!(renewed.len(), 1, "{renewed:#?}");
    assert_eq!(renewed[0].resource_id, grants[0].resource_id);
    Ok(())
}

/// Row 14: a numeric registration before any registry record for the node writes unnamed rows on
/// its lease. A later record proves the path, and the release past grace is named.
#[test]
fn a_numeric_grant_before_the_surface_stays_unnamed_until_the_record() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    logs.push(
        2,
        0,
        BASE_REGISTRAR,
        token_transfer(LABEL, ZERO_ADDRESS, HOLDER),
    );
    logs.push(
        2,
        0,
        BASE_REGISTRAR,
        numeric_registration(LABEL, HOLDER, EXPIRY),
    );
    logs.push(3, 0, REGISTRY, new_owner(LABEL, HOLDER));
    let output = logs.run(21)?;
    let (grant, lease) = mint_grant(&output);
    assert_eq!(grant.logical_name_id, None);
    let releases = of_kind(&output, "RegistrationReleased");
    assert_eq!(releases.len(), 1, "{releases:#?}");
    assert_eq!(releases[0].resource_id, Some(lease));
    assert_eq!(
        releases[0].logical_name_id.as_deref(),
        Some(name(LABEL).as_str())
    );
    Ok(())
}

/// Row 17: the numeric lease and its release come out the same from one batch, from a live
/// session, and from state restored from the stored rows and from their compaction.
#[test]
fn numeric_lease_rows_survive_redo_and_restore() -> anyhow::Result<()> {
    let mut logs = Logs::default();
    eth_root(&mut logs);
    migration_mint(&mut logs, 2, LABEL);
    let first_input = logs.input(checked_in("ens_v1_registrar_l1", REGISTRAR_MANIFEST), 2);
    let (first, session) = interpret_test_batch_incremental(first_input.clone(), None)?;
    let (_, lease) = mint_grant(&first);
    let mut tail = first_input.clone();
    tail.raw_logs.clear();
    tail.blocks = (3..=21).map(block).collect();
    let (live, _) = interpret_test_batch_incremental(tail.clone(), Some(session))?;
    let mut full_input = first_input.clone();
    full_input.blocks.extend(tail.blocks.clone());
    let full = interpret_test_batch(full_input)?;
    assert_eq!(
        live.normalized_events,
        full.normalized_events
            .into_iter()
            .filter(|event| event.block_number >= Some(3))
            .collect::<Vec<_>>()
    );
    for prior in [
        first.normalized_events.iter().map(prior_event).collect(),
        seam::fold_prior_events(vec![], &first.normalized_events, &first_input.blocks)?,
    ] {
        tail.prior_events = prior;
        let restored = interpret_test_batch(tail.clone())?;
        assert_eq!(restored.normalized_events, live.normalized_events);
        assert_eq!(restored.surface_bindings, live.surface_bindings);
    }
    let releases = of_kind(&live, "RegistrationReleased");
    assert_eq!(releases.len(), 1, "{releases:#?}");
    assert_eq!(releases[0].resource_id, Some(lease));
    assert_eq!(
        releases[0].logical_name_id.as_deref(),
        Some(name(LABEL).as_str())
    );
    Ok(())
}
