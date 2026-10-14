//! A live `.eth` lease moved into the NameWrapper by `wrapETH2LD` or by a direct
//! `safeTransferFrom`. The BaseRegistrar transfer comes first, and the reclaim `NewOwner` that
//! names the NameWrapper follows in the same transaction. Between the two logs the registry still
//! names the holder, but that state is never visible outside the transaction, so no registry-only
//! authority opens there. A transfer without that reclaim still rebinds to the registry-only
//! authority, and every case is checked one batch per block, restored, and in one batch.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L279 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L778-L811 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1020 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L175 @ ens_v1@91c966f)
use super::node_record_events::{declared_address, profile};
use super::output_pins::{assert_pinned, event_rows, output_rows, pin_matches, rows};
use super::v1_pre_surface_resolver::compact_prior;
use super::*;

mod events {
    use alloy_sol_types::sol;

    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
        event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
        event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
        event NameUnwrapped(bytes32 indexed node, address owner);
    }

    pub mod registry {
        alloy_sol_types::sol! {
            event Transfer(bytes32 indexed node, address owner);
        }
    }
}

const HOLDER: &str = "0x00000000000000000000000000000000000000a5";
const OTHER: &str = "0x00000000000000000000000000000000000000b5";
const RESOLVER: &str = "0x00000000000000000000000000000000000000e5";
const GRACE_PERIOD: i64 = 90 * 24 * 60 * 60;
/// `PARENT_CANNOT_CONTROL | IS_DOT_ETH`, which `_wrapETH2LD` always adds.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1009-L1015 @ ens_v1@91c966f)
const DOT_ETH_FUSES: u32 = 0x10000 | 0x20000;
const REGISTERED: i64 = 5_000_000;
const RESOLVED: i64 = REGISTERED + 10;
const MOVED: i64 = REGISTERED + 20;
const LATER: i64 = REGISTERED + 30;
const LEASE: i64 = REGISTERED + 10_000_000;

/// The checked-in Sepolia ENSv1 registry, BaseRegistrar and NameWrapper, plus `extra` families.
pub(super) struct Fixture {
    pub(super) chain: String,
    pub(super) manifests: Vec<ManifestInput>,
    pub(super) admissions: Vec<AddressAdmissionInput>,
    pub(super) registry: String,
    pub(super) registrar: String,
    pub(super) wrapper: String,
    pub(super) controller: String,
}

impl Fixture {
    pub(super) fn new() -> anyhow::Result<Self> {
        Self::with(&[])
    }

    pub(super) fn with(extra: &[&str]) -> anyhow::Result<Self> {
        let mut families = vec![
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "ens_v1_wrapper_l1",
        ];
        families.extend_from_slice(extra);
        let (chain, manifests, admissions) = profile("sepolia", &families)?;
        Ok(Self {
            registry: declared_address(&admissions, "registry"),
            registrar: declared_address(&admissions, "registrar"),
            wrapper: declared_address(&admissions, "name_wrapper"),
            controller: declared_address(&admissions, "wrapped_registrar_controller"),
            chain,
            manifests,
            admissions,
        })
    }

    pub(super) fn batch(
        &self,
        prior_events: Vec<PriorEventInput>,
        mut raw_logs: Vec<RawLogInput>,
    ) -> BatchInput {
        for log in &mut raw_logs {
            log.chain_id = self.chain.clone();
        }
        BatchInput {
            chain_id: self.chain.clone(),
            manifests: self.manifests.clone(),
            discovery_rules: Vec::new(),
            admissions: self.admissions.clone(),
            prior_events,
            blocks: Vec::new(),
            raw_logs,
        }
    }

    fn registry_only_resource(&self) -> Uuid {
        super::common::stable_uuid(&format!(
            "resource:registry-only:{}:{}",
            self.chain,
            hex_string(node())
        ))
    }

    fn token_transfer(&self, block: i64, tx: i64, log: i64, from: &str, to: &str) -> RawLogInput {
        raw_at_transaction(
            events::Transfer {
                from: address(from),
                to: address(to),
                tokenId: token(),
            }
            .encode_log_data(),
            block,
            tx,
            log,
            &self.registrar,
        )
    }

    fn reclaim(&self, block: i64, tx: i64, log: i64, owner: &str) -> RawLogInput {
        raw_at_transaction(
            events::NewOwner {
                node: eth_node(),
                label: keccak256(LABEL),
                owner: address(owner),
            }
            .encode_log_data(),
            block,
            tx,
            log,
            &self.registry,
        )
    }

    /// `BaseRegistrar.register(id, HOLDER, duration)` from an un-admitted controller: `_mint`,
    /// `setSubnodeOwner`, then `NameRegistered`.
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f)
    fn register(&self) -> Vec<RawLogInput> {
        vec![
            self.token_transfer(REGISTERED, 0, 0, ZERO_ADDRESS, HOLDER),
            self.reclaim(REGISTERED, 0, 1, HOLDER),
            raw_at(
                events::NameRegistered {
                    id: token(),
                    owner: address(HOLDER),
                    expires: U256::from(LEASE),
                }
                .encode_log_data(),
                REGISTERED,
                2,
                &self.registrar,
            ),
        ]
    }

    /// The holder's own `ENSRegistry.setResolver`.
    /// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L95 @ ens_v1@91c966f)
    fn set_resolver(&self) -> Vec<RawLogInput> {
        vec![raw_at(
            events::NewResolver {
                node: node(),
                resolver: address(RESOLVER),
            }
            .encode_log_data(),
            RESOLVED,
            0,
            &self.registry,
        )]
    }

    /// `_wrap` for the `.eth` name: the ERC-1155 mint with `operator`, then `NameWrapped`.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L902 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L228-L258 @ ens_v1@91c966f)
    fn mint(&self, block: i64, first_log: i64, operator: &str) -> [RawLogInput; 2] {
        [
            raw_at(
                events::TransferSingle {
                    operator: address(operator),
                    from: Address::ZERO,
                    to: address(HOLDER),
                    id: U256::from_be_bytes(node().0),
                    value: U256::from(1),
                }
                .encode_log_data(),
                block,
                first_log,
                &self.wrapper,
            ),
            raw_at(
                events::NameWrapped {
                    node: node(),
                    name: b"\x07wrapped\x03eth\0".to_vec().into(),
                    owner: address(HOLDER),
                    fuses: DOT_ETH_FUSES,
                    expiry: u64::try_from(LEASE + GRACE_PERIOD).expect("expiry"),
                }
                .encode_log_data(),
                block,
                first_log + 1,
                &self.wrapper,
            ),
        ]
    }

    /// `wrapETH2LD(label, HOLDER, 0, address(0))` by the holder: `transferFrom` to the
    /// NameWrapper, `reclaim` to it, then `_wrapETH2LD` with no resolver write.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
    fn wrap_eth2ld(&self) -> Vec<RawLogInput> {
        let mut logs = vec![
            self.token_transfer(MOVED, 0, 0, HOLDER, &self.wrapper),
            self.reclaim(MOVED, 0, 1, &self.wrapper),
        ];
        logs.extend(self.mint(MOVED, 2, HOLDER));
        logs
    }

    /// `safeTransferFrom(HOLDER, NameWrapper, id, data)`: the BaseRegistrar moves the token and
    /// calls `onERC721Received`, which reclaims and wraps. The mint's operator is the caller of
    /// the NameWrapper, the BaseRegistrar. The transfer log comes first because `reclaim`
    /// requires the NameWrapper to own the token already. The ERC721 base the BaseRegistrar
    /// imports is not vendored at the pin.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L778-L808 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
    fn direct_send(&self) -> Vec<RawLogInput> {
        let mut logs = vec![
            self.token_transfer(MOVED, 0, 0, HOLDER, &self.wrapper),
            self.reclaim(MOVED, 0, 1, &self.wrapper),
        ];
        logs.extend(self.mint(MOVED, 2, &self.registrar));
        logs
    }

    /// `registerAndWrapETH2LD` from the wrapped controller: the registrar mints to the
    /// NameWrapper and names it the registry owner, then `_wrapETH2LD` mints to the holder.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
    fn register_and_wrap(&self) -> Vec<RawLogInput> {
        let mut logs = vec![
            self.token_transfer(REGISTERED, 0, 0, ZERO_ADDRESS, &self.wrapper),
            self.reclaim(REGISTERED, 0, 1, &self.wrapper),
            raw_at(
                events::NameRegistered {
                    id: token(),
                    owner: address(&self.wrapper),
                    expires: U256::from(LEASE),
                }
                .encode_log_data(),
                REGISTERED,
                2,
                &self.registrar,
            ),
        ];
        logs.extend(self.mint(REGISTERED, 3, &self.controller));
        logs
    }

    /// `unwrapETH2LD(labelhash, HOLDER, HOLDER)`: `_unwrap` burns the token, sets the registry
    /// owner and emits `NameUnwrapped`, then the registrar token returns to the registrant.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L396 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
    fn unwrap_eth2ld(&self) -> Vec<RawLogInput> {
        vec![
            raw_at(
                events::TransferSingle {
                    operator: address(HOLDER),
                    from: address(HOLDER),
                    to: Address::ZERO,
                    id: U256::from_be_bytes(node().0),
                    value: U256::from(1),
                }
                .encode_log_data(),
                LATER,
                0,
                &self.wrapper,
            ),
            raw_at(
                events::registry::Transfer {
                    node: node(),
                    owner: address(HOLDER),
                }
                .encode_log_data(),
                LATER,
                1,
                &self.registry,
            ),
            raw_at(
                events::NameUnwrapped {
                    node: node(),
                    owner: address(HOLDER),
                }
                .encode_log_data(),
                LATER,
                2,
                &self.wrapper,
            ),
            self.token_transfer(LATER, 0, 3, &self.wrapper, HOLDER),
        ]
    }

    /// Interprets `steps` one batch per step, checks that a cold restore after every step
    /// (from all rows and from the newest row per key) equals the live session, and that one
    /// batch over every step emits the same events. Returns the one-batch output.
    pub(super) fn run(&self, steps: Vec<Vec<RawLogInput>>) -> anyhow::Result<BatchOutput> {
        let mut session = None;
        let mut events = Vec::new();
        let mut all_logs = Vec::new();
        for (index, logs) in steps.into_iter().enumerate() {
            all_logs.extend(logs.clone());
            let (output, live) =
                interpret_test_batch_incremental(self.batch(Vec::new(), logs), session)?;
            events.extend(output.normalized_events);
            for (form, prior) in [
                (
                    "all rows",
                    events.iter().map(prior_event).collect::<Vec<_>>(),
                ),
                ("compacted rows", compact_prior(&events)),
            ] {
                let (_, restored) =
                    interpret_test_batch_incremental(self.batch(prior, Vec::new()), None)?;
                assert_eq!(restored, live, "restored from {form} after step {index}");
            }
            session = Some(live);
        }
        let one_shot = interpret_test_batch(self.batch(Vec::new(), all_logs))?;
        assert_eq!(
            identities(&one_shot.normalized_events),
            identities(&events),
            "one batch differs from one batch per step"
        );
        Ok(one_shot)
    }
}

const LABEL: &[u8] = b"wrapped";

fn token() -> U256 {
    U256::from_be_bytes(keccak256(LABEL).0)
}

fn eth_node() -> B256 {
    super::common::namehash(&["eth".to_owned()])
        .parse()
        .expect("eth node")
}

fn node() -> B256 {
    super::common::namehash(&["wrapped".to_owned(), "eth".to_owned()])
        .parse()
        .expect("namehash")
}

fn address(value: &str) -> Address {
    value.parse().expect("address")
}

fn hex_string(value: B256) -> String {
    format!("{value:#x}")
}

fn identities(events: &[NormalizedEvent]) -> Vec<(String, Option<Uuid>, serde_json::Value)> {
    events
        .iter()
        .map(|event| {
            (
                event.event_identity.clone(),
                event.resource_id,
                event.after_state.clone(),
            )
        })
        .collect()
}

fn at(events: &[NormalizedEvent], block: i64) -> Vec<&NormalizedEvent> {
    events
        .iter()
        .filter(|event| event.block_number == Some(block))
        .collect()
}

/// Every event of `block` that names the registry-only authority or its resource. The registrar
/// surface evidence a transfer carries cites the registration's own registry write, and the
/// registry's read copy of the resolver pointer, replayed when the name surface materializes,
/// shares the resource id, so both are left out.
fn registry_only_events<'a>(
    fixture: &Fixture,
    output: &'a BatchOutput,
    block: i64,
) -> Vec<&'a NormalizedEvent> {
    let resource = fixture.registry_only_resource();
    at(&output.normalized_events, block)
        .into_iter()
        .filter(|event| {
            event.event_kind != "ResolverChanged"
                || event.after_state["surface_materialization"] != json!(true)
        })
        .filter(|event| {
            let mut after = event.after_state.clone();
            if let Some(after) = after.as_object_mut() {
                after.remove("registrar_surface_evidence");
            }
            let text = format!("{:?} {:?}", event.before_state, after);
            event.resource_id == Some(resource)
                || text.contains("registry-only:")
                || text.contains("\"registry_only\"")
                || text.contains(&resource.to_string())
        })
        .collect()
}

fn lease(output: &BatchOutput) -> Uuid {
    output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RegistrationGranted")
        .and_then(|event| event.resource_id)
        .expect("the registration anchors the lease")
}

/// (log index, subject, resource, scope kind, powers) of the `PermissionChanged` rows of `block`.
fn permission_rows(
    output: &BatchOutput,
    block: i64,
) -> Vec<(i64, String, Option<Uuid>, String, serde_json::Value)> {
    at(&output.normalized_events, block)
        .into_iter()
        .filter(|event| event.event_kind == "PermissionChanged")
        .map(|event| {
            (
                event.log_index.unwrap_or(-1),
                event.after_state["subject"]
                    .as_str()
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                event.resource_id,
                event.after_state["scope"]["kind"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                event.after_state["effective_powers"].clone(),
            )
        })
        .collect()
}

fn assert_no_interval(fixture: &Fixture, output: &BatchOutput) {
    let leaked = registry_only_events(fixture, output, MOVED);
    assert!(
        leaked.is_empty(),
        "the move into the NameWrapper opened a registry-only authority: {leaked:#?}"
    );
    let lease = lease(output);
    let reclaim = at(&output.normalized_events, MOVED)
        .into_iter()
        .find(|event| event.log_index == Some(1) && event.event_kind == "AuthorityTransferred")
        .expect("the reclaim moves the registry owner");
    assert_eq!(reclaim.resource_id, Some(lease));
    assert_eq!(reclaim.after_state["authority_kind"], "registrar");
    assert!(
        at(&output.normalized_events, MOVED)
            .iter()
            .filter(|event| event.log_index < Some(2))
            .all(|event| !matches!(
                event.event_kind.as_str(),
                "AuthorityEpochChanged" | "SurfaceBound" | "SurfaceUnbound"
            )),
        "the lease stays bound until the wrap"
    );
    assert!(
        output
            .surface_bindings
            .iter()
            .all(|binding| binding.block_number != MOVED
                || binding.resource_id != fixture.registry_only_resource()),
        "no registry-only binding opens in the move"
    );
}

fn lease_rows(
    output: &BatchOutput,
    wrapper: &str,
) -> Vec<(i64, String, Option<Uuid>, String, serde_json::Value)> {
    let lease = lease(output);
    let wrapper = wrapper.to_ascii_lowercase();
    vec![
        (
            0,
            HOLDER.to_owned(),
            Some(lease),
            "resource".to_owned(),
            json!([]),
        ),
        (
            0,
            wrapper,
            Some(lease),
            "resource".to_owned(),
            json!(["resource_control"]),
        ),
    ]
}

#[test]
fn wrapping_a_live_lease_opens_no_registry_only_interval() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![fixture.register(), fixture.wrap_eth2ld()])?;
    assert_no_interval(&fixture, &output);
    let rows = permission_rows(&output, MOVED);
    assert_eq!(
        rows.iter()
            .filter(|row| row.0 < 2)
            .cloned()
            .collect::<Vec<_>>(),
        lease_rows(&output, &fixture.wrapper),
        "the transfer moves the lease's control once, and the reclaim adds nothing"
    );
    Ok(())
}

#[test]
fn wrapping_a_lease_with_a_registry_resolver_moves_resolver_control_on_the_lease()
-> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![
        fixture.register(),
        fixture.set_resolver(),
        fixture.wrap_eth2ld(),
    ])?;
    assert_no_interval(&fixture, &output);
    let lease = lease(&output);
    let resolver_rows = permission_rows(&output, MOVED)
        .into_iter()
        .filter(|row| row.0 < 2 && row.3 == "resolver")
        .collect::<Vec<_>>();
    assert_eq!(
        resolver_rows,
        vec![
            (
                0,
                HOLDER.to_owned(),
                Some(lease),
                "resolver".to_owned(),
                json!([])
            ),
            (
                0,
                fixture.wrapper.to_ascii_lowercase(),
                Some(lease),
                "resolver".to_owned(),
                json!(["resolver_control"]),
            ),
        ]
    );
    Ok(())
}

#[test]
fn a_direct_token_send_to_the_name_wrapper_opens_no_registry_only_interval() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![fixture.register(), fixture.direct_send()])?;
    assert_no_interval(&fixture, &output);
    assert_eq!(
        permission_rows(&output, MOVED)
            .into_iter()
            .filter(|row| row.0 < 2)
            .collect::<Vec<_>>(),
        lease_rows(&output, &fixture.wrapper)
    );
    Ok(())
}

/// The pins below were taken on the tree before this rule, one line per output row. They pin
/// shapes the rule must not touch.
#[test]
fn register_and_wrap_is_unchanged() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![fixture.register_and_wrap()])?;
    assert_pinned(
        "register-and-wrap",
        &output_rows(&output),
        include_str!("../../../tests/fixtures/interpreters/wrap-rule-pins/register-and-wrap.txt"),
    );
    Ok(())
}

#[test]
fn unwrapping_after_a_wrap_is_unchanged() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![
        fixture.register(),
        fixture.set_resolver(),
        fixture.wrap_eth2ld(),
        fixture.unwrap_eth2ld(),
    ])?;
    let mut unwrap_block = event_rows(at(&output.normalized_events, LATER));
    unwrap_block.extend(rows(
        "surface_bindings",
        output
            .surface_bindings
            .iter()
            .filter(|binding| binding.block_number == LATER),
    ));
    unwrap_block.extend(rows(
        "binding_closures",
        output
            .binding_closures
            .iter()
            .filter(|closure| closure.block_number == LATER),
    ));
    assert_pinned(
        "unwrap-block",
        &unwrap_block,
        include_str!("../../../tests/fixtures/interpreters/wrap-rule-pins/unwrap-block.txt"),
    );
    Ok(())
}

/// The unwrap as its own final batch, after a warm session and after a cold restore of the
/// compacted rows: every collection of its output, pinned on the tree before this rule.
#[test]
fn a_final_unwrap_batch_is_unchanged() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let mut session = None;
    let mut events = Vec::new();
    for logs in [
        fixture.register(),
        fixture.set_resolver(),
        fixture.wrap_eth2ld(),
    ] {
        let (output, live) =
            interpret_test_batch_incremental(fixture.batch(Vec::new(), logs), session)?;
        events.extend(output.normalized_events);
        session = Some(live);
    }
    let (_, restored) =
        interpret_test_batch_incremental(fixture.batch(compact_prior(&events), Vec::new()), None)?;
    let expected =
        include_str!("../../../tests/fixtures/interpreters/wrap-rule-pins/final-unwrap-batch.txt");
    let mut moved = Vec::new();
    for (form, prefix) in [("warm", session), ("restored", Some(restored))] {
        let (unwrap, _) = interpret_test_batch_incremental(
            fixture.batch(Vec::new(), fixture.unwrap_eth2ld()),
            prefix,
        )?;
        if !pin_matches(
            &format!("final-unwrap-batch {form}"),
            &output_rows(&unwrap),
            expected,
        ) {
            moved.push(form);
        }
    }
    assert!(
        moved.is_empty(),
        "pinned output final-unwrap-batch moved after {moved:?}: see the PIN lines above"
    );
    Ok(())
}

/// A holder contract unwraps the name and wraps it again in one transaction: `unwrapETH2LD`
/// returns the token and the registry to it, then `wrapETH2LD` moves both back. The re-wrap's
/// transfer still finds its own reclaim as the node's next registry owner write, after the
/// unwrap's `setOwner`, so no registry-only authority opens in that transaction either.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L396 @ ens_v1@91c966f)
#[test]
fn an_unwrap_and_a_rewrap_in_one_transaction_open_no_interval() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let mut both = fixture.unwrap_eth2ld();
    both.extend([
        fixture.token_transfer(LATER, 0, 4, HOLDER, &fixture.wrapper),
        fixture.reclaim(LATER, 0, 5, &fixture.wrapper),
    ]);
    both.extend(fixture.mint(LATER, 6, HOLDER));
    let output = fixture.run(vec![fixture.register(), fixture.wrap_eth2ld(), both])?;
    let leaked = registry_only_events(&fixture, &output, LATER)
        .into_iter()
        .filter(|event| event.log_index >= Some(4))
        .collect::<Vec<_>>();
    assert!(
        leaked.is_empty(),
        "the re-wrap opened a registry-only authority: {leaked:#?}"
    );
    Ok(())
}

fn assert_rebinds_at(fixture: &Fixture, output: &BatchOutput, log_index: i64) {
    let opened = registry_only_events(fixture, output, MOVED)
        .into_iter()
        .filter(|event| event.log_index == Some(log_index))
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    assert!(
        opened.contains(&"AuthorityEpochChanged") && opened.contains(&"PermissionChanged"),
        "the transfer without a following reclaim rebinds to the registry-only authority: {opened:?}"
    );
}

#[test]
fn a_transfer_to_another_holder_still_rebinds() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![
        fixture.register(),
        vec![fixture.token_transfer(MOVED, 0, 0, HOLDER, OTHER)],
    ])?;
    assert_rebinds_at(&fixture, &output, 0);
    Ok(())
}

/// A plain ERC-721 `transferFrom` to the NameWrapper calls no receiver, so nothing reclaims or
/// wraps, and the registry still names the holder.
#[test]
fn a_plain_transfer_to_the_name_wrapper_without_reclaim_still_rebinds() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![
        fixture.register(),
        vec![fixture.token_transfer(MOVED, 0, 0, HOLDER, &fixture.wrapper)],
    ])?;
    assert_rebinds_at(&fixture, &output, 0);
    Ok(())
}

/// The reclaim that names the NameWrapper comes in a later transaction of the same block. No
/// real contract emits this shape: once a plain `transferFrom` leaves the token in the
/// NameWrapper, only the NameWrapper can reclaim, and it does so only inside `wrapETH2LD` or
/// `onERC721Received`, each after its own transfer in its own transaction. The case still pins
/// that the rule reads the transfer's own transaction only, so the transfer still rebinds.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
#[test]
fn a_reclaim_in_a_later_transaction_does_not_cover_the_transfer() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![
        fixture.register(),
        vec![
            fixture.token_transfer(MOVED, 0, 0, HOLDER, &fixture.wrapper),
            fixture.reclaim(MOVED, 1, 1, &fixture.wrapper),
        ],
    ])?;
    assert_rebinds_at(&fixture, &output, 0);
    Ok(())
}

/// A holder contract sends the token to the NameWrapper with a plain `transferFrom`, then sets
/// the registry owner itself with `setOwner`. That write is a registry `Transfer`, not the
/// reclaim's `NewOwner`, so the transfer still rebinds.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
#[test]
fn a_registry_set_owner_after_the_transfer_is_not_a_reclaim() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![
        fixture.register(),
        vec![
            fixture.token_transfer(MOVED, 0, 0, HOLDER, &fixture.wrapper),
            raw_at(
                events::registry::Transfer {
                    node: node(),
                    owner: address(&fixture.wrapper),
                }
                .encode_log_data(),
                MOVED,
                1,
                &fixture.registry,
            ),
        ],
    ])?;
    assert_rebinds_at(&fixture, &output, 0);
    Ok(())
}

/// The holder points the registry at another owner with `setOwner`, which opens the
/// registry-only authority before any wrap. A later `wrapETH2LD` then finds that divergence
/// already open, so it still closes at the reclaim log, as before this rule. Pinned like the
/// rows above.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
#[test]
fn a_wrap_over_an_open_registry_divergence_is_unchanged() -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = fixture.run(vec![
        fixture.register(),
        vec![raw_at(
            events::registry::Transfer {
                node: node(),
                owner: address(OTHER),
            }
            .encode_log_data(),
            RESOLVED,
            0,
            &fixture.registry,
        )],
        fixture.wrap_eth2ld(),
    ])?;
    assert_pinned(
        "wrap-over-divergence",
        &output_rows(&output),
        include_str!(
            "../../../tests/fixtures/interpreters/wrap-rule-pins/wrap-over-divergence.txt"
        ),
    );
    Ok(())
}
