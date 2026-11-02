//! A wrapped registrar controller's renewal moves the NameWrapper expiry exactly when
//! `NameWrapper.renew` writes it. The wrapper writes only after `registrar.ownerOf` answers, and
//! `ownerOf` answers only while the renewed lease is later than the block timestamp. A renewal
//! that leaves the lease expired, such as `ETHRenewerV1.syncWrapper`'s `renew(label, 0)` on a name
//! in grace or an underpaid renewal in grace, leaves the wrapper expiry as it was.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312-L337 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L157-L169 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/registrar/ETHRenewerV1.sol:L106-L112 @ ens_v2_sepolia_20260629@ccaeb58)

use super::*;

const WRAPPER_MANIFEST_ID: i64 = 7401;
const REGISTRAR_MANIFEST_ID: i64 = 7402;
const WRAPPER: &str = "0x0000000000000000000000000000000000000043";
const WRAPPED_CONTROLLER: &str = "0x0000000000000000000000000000000000000044";
const LEGACY_CONTROLLER: &str = "0x0000000000000000000000000000000000000045";
const HOLDER: &str = "0x0000000000000000000000000000000000000051";
const DAY: u64 = 24 * 60 * 60;
const GRACE: u64 = 90 * DAY;
const YEAR: u64 = 365 * DAY;
/// `PARENT_CANNOT_CONTROL | IS_DOT_ETH`, the fuses `_wrapETH2LD` always burns.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1015 @ ens_v1@91c966f)
const DOT_ETH_FUSES: u32 = 0x30000;
/// The lease when the name was wrapped, and the NameWrapper expiry the wrap stored from it.
const WRAPPED_LEASE: u64 = 1_700_000_000;
const WRAPPED_EXPIRY: u64 = WRAPPED_LEASE + GRACE;
/// The lease after a renewal through a controller that calls only `BaseRegistrar.renew`, which
/// leaves the wrapper expiry behind.
const BYPASSED_LEASE: u64 = WRAPPED_LEASE + YEAR;
/// A block inside that lease's grace period, which is past the stored wrapper expiry.
const IN_GRACE: u64 = BYPASSED_LEASE + 10 * DAY;
const RENEWAL_BLOCK: i64 = 3;
const PROBE_BLOCK: i64 = 4;

/// How the deployment declares the wrapped controller's `NameRenewed`. Mainnet declares it with
/// `RegistrationGranted`, so it takes the registrar name-event path. Sepolia declares only
/// `ExpiryChanged` and `PreimageObserved`, so it takes the enrichment path. Both variants keep
/// the BaseRegistrar's `NameRenewed` lifecycle-enabled, so it records the transaction's registrar
/// word. Mainnet declares that event without `RegistrationGranted`, so there the word is the
/// lease the controller's event sets, which the held-lease row covers.
#[derive(Clone, Copy, Debug)]
enum Declaration {
    Mainnet,
    Sepolia,
}

const DECLARATIONS: [Declaration; 2] = [Declaration::Mainnet, Declaration::Sepolia];

fn input(declaration: Declaration, mut raw_logs: Vec<RawLogInput>) -> BatchInput {
    raw_logs.sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    let controller_events: &[&str] = match declaration {
        Declaration::Mainnet => &[
            "RegistrationGranted",
            "RegistrationRenewed",
            "ExpiryChanged",
            "SurfaceUnbound",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "ResolverChanged",
            "PreimageObserved",
        ],
        Declaration::Sepolia => &["ExpiryChanged", "PreimageObserved"],
    };
    let wrapper_manifest = manifest_with_events(
        WRAPPER_MANIFEST_ID,
        "ens",
        "ens_v1_wrapper_l1",
        &[(
            "NameWrapped",
            "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
            &["name_wrapper"],
            &[
                "TokenControlTransferred",
                "ExpiryChanged",
                "PermissionScopeChanged",
                "SurfaceBound",
                "AuthorityEpochChanged",
            ],
        )],
    );
    let registrar_manifest = manifest_with_events(
        REGISTRAR_MANIFEST_ID,
        "ens",
        "ens_v1_registrar_l1",
        &[
            (
                "NameRenewed",
                "event NameRenewed(uint256 indexed id, uint256 expires)",
                &["registrar"],
                &[
                    "RegistrationGranted",
                    "RegistrationRenewed",
                    "ExpiryChanged",
                ],
            ),
            (
                "NameRenewed",
                "event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires)",
                &[
                    "legacy_registrar_controller",
                    "wrapped_registrar_controller",
                ],
                controller_events,
            ),
        ],
    );
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![wrapper_manifest, registrar_manifest],
        discovery_rules: Vec::new(),
        admissions: vec![
            admit(WRAPPER_MANIFEST_ID, "name_wrapper", WRAPPER, 74_011),
            admit(REGISTRAR_MANIFEST_ID, "registrar", CONTRACT, 74_021),
            admit(
                REGISTRAR_MANIFEST_ID,
                "wrapped_registrar_controller",
                WRAPPED_CONTROLLER,
                74_022,
            ),
            admit(
                REGISTRAR_MANIFEST_ID,
                "legacy_registrar_controller",
                LEGACY_CONTROLLER,
                74_023,
            ),
        ],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    }
}

fn admit(manifest_id: i64, role: &str, address: &str, instance: u128) -> AddressAdmissionInput {
    let mut entry = admission(manifest_id, role);
    entry.address = address.to_owned();
    entry.contract_instance_id = Uuid::from_u128(instance);
    entry
}

/// A log at `block`, in that block's transaction `transaction`, at Unix time `timestamp`.
fn log(
    encoded: alloy_primitives::LogData,
    block: i64,
    transaction: i64,
    log_index: i64,
    timestamp: u64,
    emitter: &str,
) -> RawLogInput {
    let mut raw = raw_at_transaction(encoded, block, transaction, log_index, emitter);
    raw.block_timestamp = OffsetDateTime::UNIX_EPOCH
        + time::Duration::seconds(i64::try_from(timestamp).expect("timestamp fits i64"));
    raw
}

fn node(label: &str) -> B256 {
    super::super::common::namehash(&[label.to_owned(), "eth".to_owned()])
        .parse()
        .expect("namehash")
}

fn token(label: &str) -> U256 {
    U256::from_be_slice(keccak256(label.as_bytes()).as_slice())
}

fn base_renewed(label: &str, expires: u64) -> alloy_primitives::LogData {
    with_topic0(
        v1_registrar::BaseNameRenewed {
            id: token(label),
            expires: U256::from(expires),
        }
        .encode_log_data(),
        keccak256(b"NameRenewed(uint256,uint256)"),
    )
}

fn controller_renewed(label: &str, cost: u64, expires: u64) -> alloy_primitives::LogData {
    wrapped_controller::NameRenewed {
        name: label.to_owned(),
        label: keccak256(label.as_bytes()),
        cost: U256::from(cost),
        expires: U256::from(expires),
    }
    .encode_log_data()
}

/// The name wrapped at block 1 with the wrap's stored expiry, then renewed at block 2 through
/// the legacy controller role, a controller that calls only `BaseRegistrar.renew` as the current
/// controller does, so the wrapper expiry trails the lease. Both blocks use transaction
/// `transaction` so several names can share them.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L352-L368 @ ens_v1@91c966f)
fn wrapped_then_bypassed(label: &str, transaction: i64) -> Vec<RawLogInput> {
    let mut dns_name = vec![u8::try_from(label.len()).expect("short label")];
    dns_name.extend_from_slice(label.as_bytes());
    dns_name.extend_from_slice(b"\x03eth\0");
    vec![
        log(
            NameWrapped {
                node: node(label),
                name: dns_name.into(),
                owner: HOLDER.parse().expect("holder"),
                fuses: DOT_ETH_FUSES,
                expiry: WRAPPED_EXPIRY,
            }
            .encode_log_data(),
            1,
            transaction,
            transaction * 10,
            WRAPPED_LEASE - 300 * DAY,
            WRAPPER,
        ),
        log(
            base_renewed(label, BYPASSED_LEASE),
            2,
            transaction,
            transaction * 10,
            WRAPPED_LEASE - 200 * DAY,
            CONTRACT,
        ),
        log(
            controller_renewed(label, 1, BYPASSED_LEASE),
            2,
            transaction,
            transaction * 10 + 1,
            WRAPPED_LEASE - 200 * DAY,
            LEGACY_CONTROLLER,
        ),
    ]
}

/// A wrapped-controller renewal in one transaction: the BaseRegistrar's `NameRenewed`, which
/// `BaseRegistrar.renew` emits inside `NameWrapper.renew`, then the controller's.
fn wrapped_renewal(
    label: &str,
    block: i64,
    transaction: i64,
    timestamp: u64,
    cost: u64,
    expires: u64,
) -> Vec<RawLogInput> {
    vec![
        log(
            base_renewed(label, expires),
            block,
            transaction,
            transaction * 10,
            timestamp,
            CONTRACT,
        ),
        log(
            controller_renewed(label, cost, expires),
            block,
            transaction,
            transaction * 10 + 1,
            timestamp,
            WRAPPED_CONTROLLER,
        ),
    ]
}

/// The registrar-family `ExpiryChanged` on the wrapper resource that a wrapped-controller
/// renewal derives, by block and name.
fn wrapper_expiries<'a>(
    output: &'a BatchOutput,
    block: i64,
    label: &str,
) -> Vec<&'a NormalizedEvent> {
    let logical_name_id = format!("ens:{:#x}", node(label));
    output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "ExpiryChanged"
                && event.source_family == "ens_v1_registrar_l1"
                && event.after_state["authority_kind"] == "wrapper"
                && event.block_number == Some(block)
                && event.logical_name_id.as_deref() == Some(logical_name_id.as_str())
        })
        .collect()
}

/// A paid renewal at the probe block that brings the lease back: its wrapper `ExpiryChanged`
/// names the wrapper expiry stored before it.
fn probe() -> Vec<RawLogInput> {
    wrapped_renewal(
        "lapsedwrap",
        PROBE_BLOCK,
        0,
        IN_GRACE + DAY,
        1,
        BYPASSED_LEASE + 2 * YEAR,
    )
}

/// Run a renewal of `lapsedwrap` at the renewal block, then the probe, under both declarations,
/// and return each output.
fn renewal_then_probe(
    renewal: Vec<RawLogInput>,
) -> anyhow::Result<Vec<(Declaration, BatchOutput)>> {
    DECLARATIONS
        .into_iter()
        .map(|declaration| {
            let logs = wrapped_then_bypassed("lapsedwrap", 0)
                .into_iter()
                .chain(renewal.clone())
                .chain(probe())
                .collect();
            Ok((declaration, interpret_test_batch(input(declaration, logs))?))
        })
        .collect()
}

fn assert_wrapper_expiry_kept(outputs: &[(Declaration, BatchOutput)]) {
    for (declaration, output) in outputs {
        assert_eq!(
            wrapper_expiries(output, RENEWAL_BLOCK, "lapsedwrap"),
            Vec::<&NormalizedEvent>::new(),
            "{declaration:?}: a renewal that leaves the lease expired derives no wrapper expiry"
        );
        let probe = wrapper_expiries(output, PROBE_BLOCK, "lapsedwrap");
        assert_eq!(probe.len(), 1, "{declaration:?}: {probe:#?}");
        assert_eq!(
            probe[0].before_state["expiry"], WRAPPED_EXPIRY,
            "{declaration:?}: the stored wrapper expiry is still the wrap's"
        );
        assert_eq!(
            probe[0].after_state["expiry"],
            BYPASSED_LEASE + 2 * YEAR + GRACE,
            "{declaration:?}: a later renewal that brings the lease back writes the wrapper"
        );
    }
}

/// `ETHRenewerV1.syncWrapper` renews through the wrapped controller with duration 0. On a name in
/// grace the lease stays expired, so the controller still emits `NameRenewed` with cost 0 and
/// the unchanged lease, and the NameWrapper writes nothing.
#[test]
fn a_sync_wrapper_renewal_in_grace_keeps_the_wrapper_expiry() -> anyhow::Result<()> {
    let renewal = wrapped_renewal("lapsedwrap", RENEWAL_BLOCK, 0, IN_GRACE, 0, BYPASSED_LEASE);
    assert_wrapper_expiry_kept(&renewal_then_probe(renewal)?);
    Ok(())
}

/// A paid renewal in grace, before ENSv2 existed, whose duration does not reach the block time:
/// the lease is still expired after it, so the NameWrapper writes nothing.
#[test]
fn an_underpaid_renewal_in_grace_keeps_the_wrapper_expiry() -> anyhow::Result<()> {
    let renewal = wrapped_renewal(
        "lapsedwrap",
        RENEWAL_BLOCK,
        0,
        IN_GRACE,
        1,
        BYPASSED_LEASE + DAY,
    );
    assert_wrapper_expiry_kept(&renewal_then_probe(renewal)?);
    Ok(())
}

/// `ownerOf` requires a strictly later expiry, so a renewal to exactly the block timestamp leaves
/// the lease expired.
#[test]
fn a_renewal_to_exactly_the_block_timestamp_keeps_the_wrapper_expiry() -> anyhow::Result<()> {
    let renewal = wrapped_renewal("lapsedwrap", RENEWAL_BLOCK, 0, IN_GRACE, 1, IN_GRACE);
    assert_wrapper_expiry_kept(&renewal_then_probe(renewal)?);
    Ok(())
}

/// Without the BaseRegistrar's `NameRenewed` in the transaction the renewal falls back to the
/// lease the state holds, and the same test applies to it.
#[test]
fn a_controller_renewal_without_the_registrar_log_tests_the_held_lease() -> anyhow::Result<()> {
    let renewal = vec![log(
        controller_renewed("lapsedwrap", 0, BYPASSED_LEASE),
        RENEWAL_BLOCK,
        0,
        0,
        IN_GRACE,
        WRAPPED_CONTROLLER,
    )];
    assert_wrapper_expiry_kept(&renewal_then_probe(renewal)?);
    Ok(())
}

/// A renewal in grace that brings the lease back is the one `NameWrapper.renew` writes: the new
/// lease plus the grace period.
#[test]
fn a_renewal_in_grace_that_brings_the_lease_back_extends_the_wrapper() -> anyhow::Result<()> {
    let renewed = BYPASSED_LEASE + YEAR;
    let renewal = wrapped_renewal("lapsedwrap", RENEWAL_BLOCK, 0, IN_GRACE, 1, renewed);
    for (declaration, output) in renewal_then_probe(renewal)? {
        let renewal = wrapper_expiries(&output, RENEWAL_BLOCK, "lapsedwrap");
        assert_eq!(renewal.len(), 1, "{declaration:?}: {renewal:#?}");
        assert_eq!(renewal[0].before_state["expiry"], WRAPPED_EXPIRY);
        assert_eq!(renewal[0].after_state["expiry"], renewed + GRACE);
        assert_eq!(renewal[0].after_state["registrar_expiry"], renewed);
        let probe = wrapper_expiries(&output, PROBE_BLOCK, "lapsedwrap");
        assert_eq!(probe.len(), 1, "{declaration:?}: {probe:#?}");
        assert_eq!(probe[0].before_state["expiry"], renewed + GRACE);
    }
    Ok(())
}

/// `ETHRenewerV1.renew` renews the BaseRegistrar directly and then runs `syncWrapper`. The paid
/// part brings the lease back first, so the sync's controller renewal writes the wrapper once.
#[test]
fn a_paid_renewal_then_its_wrapper_sync_extends_the_wrapper_once() -> anyhow::Result<()> {
    let renewed = BYPASSED_LEASE + YEAR;
    let renewal = vec![
        log(
            base_renewed("lapsedwrap", renewed),
            RENEWAL_BLOCK,
            0,
            0,
            IN_GRACE,
            CONTRACT,
        ),
        log(
            base_renewed("lapsedwrap", renewed),
            RENEWAL_BLOCK,
            0,
            1,
            IN_GRACE,
            CONTRACT,
        ),
        log(
            controller_renewed("lapsedwrap", 0, renewed),
            RENEWAL_BLOCK,
            0,
            2,
            IN_GRACE,
            WRAPPED_CONTROLLER,
        ),
    ];
    for (declaration, output) in renewal_then_probe(renewal)? {
        let renewal = wrapper_expiries(&output, RENEWAL_BLOCK, "lapsedwrap");
        assert_eq!(renewal.len(), 1, "{declaration:?}: {renewal:#?}");
        assert_eq!(renewal[0].after_state["expiry"], renewed + GRACE);
    }
    Ok(())
}

/// The legacy controller calls only `BaseRegistrar.renew`, so its renewal of a wrapped name
/// derives no wrapper expiry, even with the lease live.
#[test]
fn a_renewal_that_bypasses_the_name_wrapper_derives_no_wrapper_expiry() -> anyhow::Result<()> {
    for declaration in DECLARATIONS {
        let output =
            interpret_test_batch(input(declaration, wrapped_then_bypassed("lapsedwrap", 0)))?;
        assert_eq!(
            wrapper_expiries(&output, 2, "lapsedwrap"),
            Vec::<&NormalizedEvent>::new(),
            "{declaration:?}"
        );
    }
    Ok(())
}

/// Two renewals in one block, each in its own transaction: one brings its lease back, the other
/// leaves its lease in grace. Each transaction is decided by its own registrar word.
#[test]
fn renewals_in_one_block_are_decided_per_transaction() -> anyhow::Result<()> {
    let renewed = BYPASSED_LEASE + YEAR;
    for declaration in DECLARATIONS {
        let live = wrapped_renewal("livewrap", RENEWAL_BLOCK, 0, IN_GRACE, 1, renewed);
        let grace = wrapped_renewal("gracewrap", RENEWAL_BLOCK, 1, IN_GRACE, 0, BYPASSED_LEASE);
        let logs = wrapped_then_bypassed("livewrap", 0)
            .into_iter()
            .chain(wrapped_then_bypassed("gracewrap", 1))
            .chain(live)
            .chain(grace)
            .collect();
        let output = interpret_test_batch(input(declaration, logs))?;
        let live = wrapper_expiries(&output, RENEWAL_BLOCK, "livewrap");
        assert_eq!(live.len(), 1, "{declaration:?}: {live:#?}");
        assert_eq!(live[0].after_state["expiry"], renewed + GRACE);
        assert_eq!(
            wrapper_expiries(&output, RENEWAL_BLOCK, "gracewrap"),
            Vec::<&NormalizedEvent>::new(),
            "{declaration:?}"
        );
    }
    Ok(())
}

/// A lease the NameWrapper does not hold: `NameWrapper.renew` returns before writing when the
/// registrar token's owner is not the NameWrapper, so a wrapped-controller renewal of a name
/// that was never wrapped derives no wrapper expiry.
#[test]
fn a_wrapped_controller_renewal_of_an_unwrapped_lease_derives_no_wrapper_expiry()
-> anyhow::Result<()> {
    let renewed = BYPASSED_LEASE + YEAR;
    for declaration in DECLARATIONS {
        let mut logs = vec![log(
            base_renewed("plainlease", BYPASSED_LEASE),
            2,
            0,
            0,
            WRAPPED_LEASE - 200 * DAY,
            CONTRACT,
        )];
        let renewal = wrapped_renewal("plainlease", RENEWAL_BLOCK, 0, IN_GRACE, 1, renewed);
        logs.extend(renewal);
        let output = interpret_test_batch(input(declaration, logs))?;
        let wrapper_events = output
            .normalized_events
            .iter()
            .filter(|event| event.after_state["authority_kind"] == "wrapper")
            .count();
        assert_eq!(wrapper_events, 0, "{declaration:?}");
    }
    Ok(())
}
