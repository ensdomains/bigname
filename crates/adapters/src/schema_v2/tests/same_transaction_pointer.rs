//! A `.eth` registration whose ENSv1 registry writes share a transaction with the ENSv2
//! reservation that discloses its label, as premigration's `register` writes them
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L177-L178 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L217-L229 @ ens_v2_sepolia_20261001@07e55a05).
//! With no resolver, `register` makes the registrant the registry owner of the node, so it may
//! write the node again in the same transaction, after the reservation
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L212-L214 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L150 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L19 @ ens_v1@91c966f).
//! The reservation's label names the node mid-transaction. A registry write after it is named
//! when it is emitted, and same-transaction reconciliation keeps that name.
use super::numeric_short_lease_fixture as fixture;
use super::*;

mod events {
    alloy_sol_types::sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
        event AddrChanged(bytes32 indexed node, address a);
        event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    }

    pub mod registry {
        alloy_sol_types::sol! {
            event Transfer(bytes32 indexed node, address owner);
        }
    }
}

const OWNER: &str = "0x00000000000000000000000000000000000000e1";
const NEXT_OWNER: &str = "0x00000000000000000000000000000000000000e2";
const RESOLVER: &str = "0x00000000000000000000000000000000000000a1";
const OTHER_RESOLVER: &str = "0x00000000000000000000000000000000000000a2";
const LEASE: u64 = 2_000_000_000;
const RESERVATION: u64 = LEASE + 62 * 86_400;

type Log = (String, alloy_primitives::LogData);

struct Catalog {
    input: BatchInput,
    registry: String,
    registrar: String,
    resolver: String,
    v2_registry: String,
}

/// The shipped Sepolia manifests on the fixture's local addresses, with no logs.
fn catalog() -> anyhow::Result<Catalog> {
    let local = fixture::fixture()?["local_contracts"].clone();
    let address = |family: &str, role: &str| local[family][role].as_str().unwrap().to_owned();
    let mut input = fixture::input()?;
    input.blocks.clear();
    input.raw_logs.clear();
    Ok(Catalog {
        input,
        registry: address("ens_v1_registry_l1", "registry"),
        registrar: address("ens_v1_registrar_l1", "registrar"),
        resolver: address("ens_v1_resolver_l1", "public_resolver"),
        v2_registry: address("ens_v2_registry_l1", "registry"),
    })
}

fn address(value: &str) -> Address {
    value.parse().expect("address")
}

fn labelhash(label: &str) -> B256 {
    keccak256(label.as_bytes())
}

fn node(label: &str) -> String {
    super::common::namehash(&[label.to_owned(), "eth".to_owned()])
}

fn name(label: &str) -> String {
    format!("ens:{}", node(label))
}

fn eth_node() -> B256 {
    super::common::namehash(&["eth".to_owned()])
        .parse()
        .expect("eth node")
}

impl Catalog {
    /// One transaction per block, its logs in order.
    fn batch(&self, blocks: Vec<(i64, Vec<Log>)>) -> BatchInput {
        let mut input = self.input.clone();
        for (block, logs) in blocks {
            let mut raws = logs
                .into_iter()
                .enumerate()
                .map(|(index, (emitter, data))| {
                    let mut raw = raw_at(data, block, index as i64, &emitter);
                    raw.chain_id = fixture::CHAIN.to_owned();
                    raw
                })
                .collect::<Vec<_>>();
            if let Some(raw) = raws.first() {
                input.blocks.push(RawBlockInput {
                    chain_id: raw.chain_id.clone(),
                    block_hash: raw.block_hash.clone(),
                    block_number: block,
                    block_timestamp: raw.block_timestamp,
                    canonicality_state: "canonical".to_owned(),
                });
            }
            input.raw_logs.append(&mut raws);
        }
        input
    }

    /// The BaseRegistrar registration of `label`.eth to `OWNER`, `before`, the ownerless ENSv2
    /// reservation of `reserved`, then `after`, in one transaction.
    fn registered(
        &self,
        label: &str,
        reserved: &str,
        before: Vec<Log>,
        after: Vec<Log>,
    ) -> Vec<Log> {
        let token = U256::from_be_bytes(labelhash(label).0);
        let mut logs = vec![
            (
                self.registrar.clone(),
                events::Transfer {
                    from: Address::ZERO,
                    to: address(OWNER),
                    tokenId: token,
                }
                .encode_log_data(),
            ),
            (
                self.registry.clone(),
                events::NewOwner {
                    node: eth_node(),
                    label: labelhash(label),
                    owner: address(OWNER),
                }
                .encode_log_data(),
            ),
            (
                self.registrar.clone(),
                events::NameRegistered {
                    id: token,
                    owner: address(OWNER),
                    expires: U256::from(LEASE),
                }
                .encode_log_data(),
            ),
        ];
        logs.extend(before);
        logs.push((
            self.v2_registry.clone(),
            events::LabelReserved {
                tokenId: versioned_token(reserved, 0),
                labelHash: labelhash(reserved),
                label: reserved.into(),
                expiry: RESERVATION,
                sender: address(OWNER),
            }
            .encode_log_data(),
        ));
        logs.extend(after);
        logs
    }

    fn pointer(&self, label: &str, resolver: &str) -> Log {
        (
            self.registry.clone(),
            events::NewResolver {
                node: node(label).parse().expect("node"),
                resolver: address(resolver),
            }
            .encode_log_data(),
        )
    }

    /// `setOwner(node, owner)`
    /// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f).
    fn set_owner(&self, label: &str, owner: &str) -> Log {
        (
            self.registry.clone(),
            events::registry::Transfer {
                node: node(label).parse().expect("node"),
                owner: address(owner),
            }
            .encode_log_data(),
        )
    }

    /// `reclaim(id, owner)`: the BaseRegistrar writes the registry owner of the node
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f).
    fn reclaim(&self, label: &str, owner: &str) -> Log {
        (
            self.registry.clone(),
            events::NewOwner {
                node: eth_node(),
                label: labelhash(label),
                owner: address(owner),
            }
            .encode_log_data(),
        )
    }

    fn addr(&self, label: &str) -> Log {
        (
            self.resolver.clone(),
            events::AddrChanged {
                node: node(label).parse().expect("node"),
                a: address(OWNER),
            }
            .encode_log_data(),
        )
    }

    /// The `.eth` node, owned by the BaseRegistrar.
    fn eth(&self) -> Vec<Log> {
        vec![(
            self.registry.clone(),
            events::NewOwner {
                node: B256::ZERO,
                label: keccak256(b"eth"),
                owner: address(&self.registrar),
            }
            .encode_log_data(),
        )]
    }
}

fn describe(event: &NormalizedEvent) -> String {
    format!(
        "{} {} {:?}@{:?}:{:?} name={:?} resource={:?} {}",
        event.event_kind,
        event.source_family,
        event.event_identity,
        event.block_number,
        event.log_index,
        event.logical_name_id,
        event.resource_id,
        event.after_state
    )
}

fn dump(output: &BatchOutput) -> String {
    output
        .normalized_events
        .iter()
        .map(describe)
        .collect::<Vec<_>>()
        .join("\n")
}

fn at(output: &BatchOutput, block: i64, log: i64) -> Vec<&NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(block) && event.log_index == Some(log))
        .collect()
}

/// The registry's own `NewResolver` row for `label` at `block`/`log`.
fn raw_pointer(output: &BatchOutput, label: &str, block: i64, log: i64) -> NormalizedEvent {
    let rows = at(output, block, log)
        .into_iter()
        .filter(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["source_event"] == "NewResolver"
                && event.after_state["node"] == node(label)
                && !event
                    .event_identity
                    .contains(":ResolverChanged:registry-read:")
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1, "{}", dump(output));
    rows[0].clone()
}

fn registry_read_copy(output: &BatchOutput, label: &str, block: i64, log: i64) -> NormalizedEvent {
    let rows = at(output, block, log)
        .into_iter()
        .filter(|event| {
            event.event_kind == "ResolverChanged"
                && event.after_state["node"] == node(label)
                && event
                    .event_identity
                    .contains(":ResolverChanged:registry-read:")
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1, "{}", dump(output));
    rows[0].clone()
}

/// Every `ResolverChanged` row on `label`'s node, in chain order.
fn pointer_rows<'a>(output: &'a BatchOutput, label: &str) -> Vec<&'a NormalizedEvent> {
    let mut rows = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.event_kind == "ResolverChanged" && event.after_state["node"] == node(label)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|event| (event.block_number, event.transaction_index, event.log_index));
    rows
}

fn registrar_resource(output: &BatchOutput, label: &str) -> Option<Uuid> {
    output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationGranted"
                && event.source_family == "ens_v1_registrar_l1"
                && event.after_state["source_event"] == "NameRegistered"
                && event.after_state["namehash"] == node(label)
        })
        .expect("registration grant")
        .resource_id
}

fn assert_named(output: &BatchOutput, event: &NormalizedEvent, label: &str) {
    assert_eq!(
        event.logical_name_id.as_deref(),
        Some(name(label).as_str()),
        "{}\n--\n{}",
        describe(event),
        dump(output)
    );
}

/// Every surviving row of the log at `block`/`log` names `label`.
fn assert_log_named(output: &BatchOutput, label: &str, block: i64, log: i64) {
    let rows = at(output, block, log);
    assert!(!rows.is_empty(), "{}", dump(output));
    for event in rows {
        assert_named(output, event, label);
    }
}

#[test]
fn pointer_before_a_same_transaction_reservation_is_served_by_its_snapshot() -> anyhow::Result<()> {
    let catalog = catalog()?;
    let output = interpret_schema_v2_batch(catalog.batch(vec![(
        10,
        catalog.registered(
            "before",
            "before",
            vec![catalog.pointer("before", RESOLVER)],
            vec![],
        ),
    )]))?;
    assert_eq!(raw_pointer(&output, "before", 10, 3).logical_name_id, None);
    let rows = pointer_rows(&output, "before");
    let last = rows.last().expect("pointer rows");
    assert_eq!(
        last.after_state["registrar_surface_snapshot"],
        true,
        "{}",
        dump(&output)
    );
    assert_eq!(last.after_state["resolver"], RESOLVER);
    assert_named(&output, last, "before");
    Ok(())
}

#[test]
fn pointer_after_a_same_transaction_reservation_keeps_its_name() -> anyhow::Result<()> {
    let catalog = catalog()?;
    let output = interpret_schema_v2_batch(catalog.batch(vec![(
        10,
        catalog.registered(
            "after",
            "after",
            vec![],
            vec![catalog.pointer("after", RESOLVER)],
        ),
    )]))?;
    let registrar = registrar_resource(&output, "after");
    let pointer = raw_pointer(&output, "after", 10, 4);
    assert_named(&output, &pointer, "after");
    assert_eq!(pointer.resource_id, registrar, "{}", dump(&output));
    assert_eq!(pointer.after_state["resolver"], RESOLVER);
    let copy = registry_read_copy(&output, "after", 10, 4);
    assert_named(&output, &copy, "after");
    assert_eq!(copy.after_state["resolver"], RESOLVER);
    assert_log_named(&output, "after", 10, 4);
    Ok(())
}

#[test]
fn pointer_in_a_later_block_is_named() -> anyhow::Result<()> {
    let catalog = catalog()?;
    let output = interpret_schema_v2_batch(catalog.batch(vec![
        (10, catalog.registered("later", "later", vec![], vec![])),
        (11, vec![catalog.pointer("later", RESOLVER)]),
    ]))?;
    let pointer = raw_pointer(&output, "later", 11, 0);
    assert_named(&output, &pointer, "later");
    assert_eq!(pointer.resource_id, registrar_resource(&output, "later"));
    Ok(())
}

#[test]
fn two_pointers_after_a_same_transaction_reservation_are_named_and_the_last_wins()
-> anyhow::Result<()> {
    let catalog = catalog()?;
    let output = interpret_schema_v2_batch(catalog.batch(vec![(
        10,
        catalog.registered(
            "twice",
            "twice",
            vec![],
            vec![
                catalog.pointer("twice", RESOLVER),
                catalog.pointer("twice", OTHER_RESOLVER),
            ],
        ),
    )]))?;
    assert_named(&output, &raw_pointer(&output, "twice", 10, 4), "twice");
    assert_named(&output, &raw_pointer(&output, "twice", 10, 5), "twice");
    let rows = pointer_rows(&output, "twice");
    for row in &rows {
        assert_named(&output, row, "twice");
    }
    assert_eq!(rows.last().unwrap().after_state["resolver"], OTHER_RESOLVER);
    Ok(())
}

#[test]
fn a_reservation_of_another_label_does_not_name_the_pointer() -> anyhow::Result<()> {
    let catalog = catalog()?;
    let output = interpret_schema_v2_batch(catalog.batch(vec![(
        10,
        catalog.registered(
            "mine",
            "theirs",
            vec![],
            vec![catalog.pointer("mine", RESOLVER)],
        ),
    )]))?;
    assert_eq!(
        raw_pointer(&output, "mine", 10, 4).logical_name_id,
        None,
        "{}",
        dump(&output)
    );
    for event in &output.normalized_events {
        let on_mine = [&event.after_state["node"], &event.after_state["namehash"]]
            .into_iter()
            .any(|value| *value == node("mine"));
        assert!(
            !(on_mine && event.logical_name_id.as_deref() == Some(name("theirs").as_str())),
            "{}",
            describe(event)
        );
    }
    Ok(())
}

#[test]
fn pointer_clear_after_a_same_transaction_reservation_keeps_its_name() -> anyhow::Result<()> {
    let catalog = catalog()?;
    let output = interpret_schema_v2_batch(catalog.batch(vec![(
        10,
        catalog.registered(
            "cleared",
            "cleared",
            vec![catalog.pointer("cleared", RESOLVER)],
            vec![catalog.pointer("cleared", ZERO_ADDRESS)],
        ),
    )]))?;
    let clear = raw_pointer(&output, "cleared", 10, 5);
    assert_named(&output, &clear, "cleared");
    assert_eq!(clear.after_state["resolver"], ZERO_ADDRESS);
    let rows = pointer_rows(&output, "cleared");
    assert_eq!(rows.last().unwrap().after_state["resolver"], ZERO_ADDRESS);
    assert_named(&output, rows.last().unwrap(), "cleared");
    assert_log_named(&output, "cleared", 10, 5);
    Ok(())
}

/// The rows of the log at `block`/`log` that do not name `label`, or every row when the log
/// left none.
fn unnamed_rows(output: &BatchOutput, label: &str, block: i64, log: i64) -> Vec<String> {
    let rows = at(output, block, log);
    if rows.is_empty() {
        return vec![format!("no row at {block}:{log}\n{}", dump(output))];
    }
    rows.into_iter()
        .filter(|event| event.logical_name_id.as_deref() != Some(name(label).as_str()))
        .map(describe)
        .collect()
}

/// Runs each `(case, logs after the reservation)` and asserts that every row of the last log
/// names `label`.
fn assert_after_named(
    catalog: &Catalog,
    label: &str,
    cases: Vec<(&str, Vec<Log>)>,
) -> anyhow::Result<()> {
    let mut failures = Vec::new();
    for (case, after) in cases {
        let log = 3 + after.len() as i64;
        let output = interpret_schema_v2_batch(
            catalog.batch(vec![(10, catalog.registered(label, label, vec![], after))]),
        )?;
        for row in unnamed_rows(&output, label, 10, log) {
            failures.push(format!("{case}: {row}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
fn registry_transfer_after_a_same_transaction_reservation_keeps_its_name() -> anyhow::Result<()> {
    let catalog = catalog()?;
    let token_transfer = || {
        (
            catalog.registrar.clone(),
            events::Transfer {
                from: address(OWNER),
                to: address(NEXT_OWNER),
                tokenId: U256::from_be_bytes(labelhash("handed").0),
            }
            .encode_log_data(),
        )
    };
    assert_after_named(
        &catalog,
        "handed",
        vec![
            (
                "to a new owner",
                vec![catalog.set_owner("handed", NEXT_OWNER)],
            ),
            (
                "after the token moved",
                vec![token_transfer(), catalog.set_owner("handed", NEXT_OWNER)],
            ),
        ],
    )
}

#[test]
fn reclaim_after_a_same_transaction_reservation_keeps_its_name() -> anyhow::Result<()> {
    let catalog = catalog()?;
    let token_transfer = || {
        (
            catalog.registrar.clone(),
            events::Transfer {
                from: address(OWNER),
                to: address(NEXT_OWNER),
                tokenId: U256::from_be_bytes(labelhash("reclaimed").0),
            }
            .encode_log_data(),
        )
    };
    assert_after_named(
        &catalog,
        "reclaimed",
        vec![
            (
                "to a new owner",
                vec![catalog.reclaim("reclaimed", NEXT_OWNER)],
            ),
            (
                "to the registrant",
                vec![catalog.reclaim("reclaimed", OWNER)],
            ),
            (
                "after the token moved",
                vec![token_transfer(), catalog.reclaim("reclaimed", NEXT_OWNER)],
            ),
        ],
    )
}

#[test]
fn pointer_after_a_same_transaction_reservation_is_named_in_every_run_shape() -> anyhow::Result<()>
{
    let catalog = catalog()?;
    let input = catalog.batch(vec![
        (10, catalog.registered("prefix", "prefix", vec![], vec![])),
        (
            11,
            catalog.registered(
                "shaped",
                "shaped",
                vec![],
                vec![catalog.pointer("shaped", RESOLVER)],
            ),
        ),
        (
            12,
            vec![
                catalog.addr("shaped"),
                catalog.pointer("shaped", OTHER_RESOLVER),
            ],
        ),
    ]);
    let full = interpret_schema_v2_batch(input.clone())?;
    assert_named(&full, &raw_pointer(&full, "shaped", 11, 4), "shaped");
    assert_named(&full, &registry_read_copy(&full, "shaped", 11, 4), "shaped");
    // The later pointer shares the restored pointer's state key. Its before is the resolver the
    // transaction set.
    let later = raw_pointer(&full, "shaped", 12, 1);
    assert_named(&full, &later, "shaped");
    assert_eq!(
        later.raw_fact_ref[seam::INTERPRETER_STATE_KEY],
        raw_pointer(&full, "shaped", 11, 4).raw_fact_ref[seam::INTERPRETER_STATE_KEY],
        "{}",
        dump(&full)
    );
    assert_eq!(
        later.before_state["resolver"],
        RESOLVER,
        "{}\n--\n{}",
        describe(&later),
        dump(&full)
    );
    let range = |from: i64, to: i64| {
        let mut range = input.clone();
        range
            .blocks
            .retain(|block| (from..=to).contains(&block.block_number));
        range
            .raw_logs
            .retain(|raw| (from..=to).contains(&raw.block_number));
        range
    };
    let mut session = None;
    let mut per_block = Vec::new();
    for block in 10..=12 {
        let (output, next) = prepare_schema_v2_batch_incremental(
            range(block, block),
            session,
            StateCacheCapacity::Unlimited,
        )?
        .finish(Vec::new())?;
        per_block.extend(output.normalized_events);
        session = Some(next);
    }
    assert_eq!(per_block, full.normalized_events, "block per batch");
    for split in [10, 11] {
        let before = range(10, split);
        let head = interpret_schema_v2_batch(before.clone())?;
        let mut tail = range(split + 1, 12);
        tail.prior_events =
            seam::fold_prior_events(Vec::new(), &head.normalized_events, &before.blocks)?;
        let restored = interpret_schema_v2_batch(tail)?;
        assert_eq!(
            restored.normalized_events,
            full.normalized_events
                .iter()
                .filter(|event| event.block_number.is_some_and(|number| number > split))
                .cloned()
                .collect::<Vec<_>>(),
            "restore after block {split}"
        );
    }
    Ok(())
}

#[test]
fn a_known_eth_node_names_the_pointer_in_every_order() -> anyhow::Result<()> {
    let catalog = catalog()?;
    let output = interpret_schema_v2_batch(catalog.batch(vec![
        (9, catalog.eth()),
        (
            10,
            catalog.registered(
                "first",
                "first",
                vec![catalog.pointer("first", RESOLVER)],
                vec![],
            ),
        ),
        (
            11,
            catalog.registered(
                "second",
                "second",
                vec![],
                vec![catalog.pointer("second", RESOLVER)],
            ),
        ),
        (12, catalog.registered("third", "third", vec![], vec![])),
        (13, vec![catalog.pointer("third", RESOLVER)]),
    ]))?;
    for (label, block, log) in [("first", 10, 3), ("second", 11, 4), ("third", 13, 0)] {
        assert_named(&output, &raw_pointer(&output, label, block, log), label);
        let rows = pointer_rows(&output, label);
        assert_eq!(rows.last().unwrap().after_state["resolver"], RESOLVER);
        assert_named(&output, rows.last().unwrap(), label);
    }
    Ok(())
}

#[test]
fn records_after_a_same_transaction_reservation_keep_their_name() -> anyhow::Result<()> {
    let catalog = catalog()?;
    let output = interpret_schema_v2_batch(catalog.batch(vec![(
        10,
        catalog.registered(
            "recorded",
            "recorded",
            vec![catalog.pointer("recorded", RESOLVER)],
            vec![catalog.addr("recorded")],
        ),
    )]))?;
    let record = at(&output, 10, 5)
        .into_iter()
        .find(|event| event.after_state["source_event"] == "AddrChanged")
        .unwrap_or_else(|| panic!("{}", dump(&output)))
        .clone();
    assert_named(&output, &record, "recorded");
    assert_eq!(record.resource_id, registrar_resource(&output, "recorded"));
    Ok(())
}
