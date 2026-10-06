use super::*;

const REGISTRY: &str = "0x0000000000000000000000000000000000000a01";
const WRAPPER: &str = "0x0000000000000000000000000000000000000a02";
const REGISTRAR: &str = "0x0000000000000000000000000000000000000a03";
const OWNER: &str = "0x0000000000000000000000000000000000000a04";

mod numeric {
    use super::*;
    sol! { event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires); }
}

fn input(logs: Vec<RawLogInput>, first: i64, last: i64) -> BatchInput {
    let registry = manifest_with_events(
        2281,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry", "registry_old"],
                &[
                    "SubregistryChanged",
                    "AuthorityTransferred",
                    "PermissionChanged",
                    "SurfaceBound",
                    "SurfaceUnbound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                ],
            ),
            (
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry", "registry_old"],
                &[
                    "AuthorityTransferred",
                    "PermissionChanged",
                    "SurfaceBound",
                    "SurfaceUnbound",
                    "AuthorityEpochChanged",
                    "ResolverChanged",
                ],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry", "registry_old"],
                &["ResolverChanged", "PermissionChanged"],
            ),
        ],
    );
    let wrapper = manifest_with_events(
        2282,
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
                "SurfaceUnbound",
                "AuthorityEpochChanged",
                "ResolverChanged",
                "PermissionChanged",
                "PreimageObserved",
            ],
        )],
    );
    let registrar = manifest_with_events(
        2283,
        "ens",
        "ens_v1_registrar_l1",
        &[(
            "NameRegistered",
            "event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires)",
            &["registrar"],
            &[
                "RegistrationGranted",
                "ExpiryChanged",
                "PermissionChanged",
                "SurfaceBound",
                "SurfaceUnbound",
                "AuthorityEpochChanged",
                "ResolverChanged",
            ],
        )],
    );
    let admissions = [
        (2281, "registry", REGISTRY),
        (2282, "name_wrapper", WRAPPER),
        (2283, "registrar", REGISTRAR),
    ]
    .into_iter()
    .map(|(id, role, address)| {
        let mut row = admission(id, role);
        row.address = address.to_owned();
        row.contract_instance_id = Uuid::from_u128(id as u128);
        row
    })
    .collect();
    BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![registry, wrapper, registrar],
        discovery_rules: Vec::new(),
        admissions,
        prior_events: Vec::new(),
        blocks: (first..=last).map(test_block).collect(),
        raw_logs: logs,
    }
}

fn owner(parent: B256, label: &[u8], address: &str, block: i64, index: i64) -> RawLogInput {
    raw_at(
        v1_registry::NewOwner {
            node: parent,
            label: keccak256(label),
            owner: address.parse().unwrap(),
        }
        .encode_log_data(),
        block,
        index,
        REGISTRY,
    )
}

fn node(labels: &[&[u8]]) -> B256 {
    super::common::namehash_raw(labels.iter().copied())
        .parse()
        .unwrap()
}

fn resolver(node: B256, block: i64, index: i64) -> RawLogInput {
    raw_at(
        v1_registry::NewResolver {
            node,
            resolver: OWNER.parse().unwrap(),
        }
        .encode_log_data(),
        block,
        index,
        REGISTRY,
    )
}

fn wrapped(labels: &[&[u8]], block: i64) -> RawLogInput {
    let mut dns = Vec::new();
    for label in labels {
        dns.push(label.len() as u8);
        dns.extend_from_slice(label);
    }
    dns.push(0);
    raw_at(
        NameWrapped {
            node: node(labels),
            name: dns.into(),
            owner: OWNER.parse().unwrap(),
            fuses: 0,
            expiry: 9_999,
        }
        .encode_log_data(),
        block,
        0,
        WRAPPER,
    )
}

#[test]
fn new_owner_proves_nested_paths_before_events_and_survives_key_replacement() -> anyhow::Result<()>
{
    let eth = node(&[b"eth"]);
    let parent = node(&[b"unknown", b"eth"]);
    let child = node(&[b"nested", b"unknown", b"eth"]);
    let prefix = input(
        vec![
            owner(B256::ZERO, b"eth", OWNER, 1, 0),
            owner(eth, b"unknown", OWNER, 1, 1),
            owner(parent, b"nested", OWNER, 1, 2),
            raw_at(
                v1_registry::Transfer {
                    node: child,
                    owner: OWNER.parse()?,
                }
                .encode_log_data(),
                2,
                0,
                REGISTRY,
            ),
        ],
        1,
        2,
    );
    let (first, live) = interpret_test_batch_incremental(prefix.clone(), None)?;
    let id = format!("ens:{child:#x}");
    let surface = first
        .name_surfaces
        .iter()
        .find(|row| row.logical_name_id == id)
        .unwrap();
    assert!(surface.raw.is_none());
    assert_eq!(
        surface.labelhashes,
        [b"nested".as_slice(), b"unknown", b"eth"].map(super::common::hash_hex)
    );
    assert!(
        first
            .normalized_events
            .iter()
            .filter(|event| event.after_state["child_node"] == format!("{child:#x}"))
            .all(|event| event.logical_name_id.as_deref() == Some(id.as_str()))
    );
    assert!(
        first
            .surface_bindings
            .iter()
            .any(|binding| binding.logical_name_id == id)
    );
    let prior = seam::fold_prior_events(Vec::new(), &first.normalized_events, &prefix.blocks)?;
    assert!(prior.iter().any(
        |event| event.logical_name_id.as_deref() == Some(id.as_str())
            && event.after_state[seam::NAME_IDENTITY_OBSERVED_KEY] == true
    ));
    let suffix = input(
        vec![resolver(child, 3, 0), owner(child, b"deeper", OWNER, 3, 1)],
        3,
        3,
    );
    let (continued, _) = interpret_test_batch_incremental(suffix.clone(), Some(live))?;
    let mut cold = suffix;
    cold.prior_events = prior;
    assert_eq!(continued, interpret_test_batch(cold)?);
    assert!(
        continued
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "ResolverChanged"
                && event.logical_name_id.as_deref() == Some(id.as_str()))
    );
    Ok(())
}

#[test]
fn arbitrary_owner_and_resolver_logs_do_not_supply_missing_ancestry() -> anyhow::Result<()> {
    let parent = node(&[b"never-observed", b"eth"]);
    let output = interpret_test_batch(input(
        vec![
            raw_at(
                v1_registry::Transfer {
                    node: parent,
                    owner: OWNER.parse()?,
                }
                .encode_log_data(),
                1,
                0,
                REGISTRY,
            ),
            resolver(parent, 1, 1),
            owner(parent, b"child", OWNER, 1, 2),
        ],
        1,
        1,
    ))?;
    assert!(output.name_surfaces.is_empty());
    assert!(
        !output
            .normalized_events
            .iter()
            .any(|event| event.after_state[seam::NAME_IDENTITY_OBSERVED_KEY] == true)
    );
    Ok(())
}

#[test]
fn invalid_bytes_win_in_both_orders_and_repeated_structural_observations() -> anyhow::Result<()> {
    for label in [b"Invalid".as_slice(), &[0xff], b"nul\0"] {
        for bytes_first in [false, true] {
            let eth = node(&[b"eth"]);
            let child = node(&[label, b"eth"]);
            let first = if bytes_first {
                wrapped(&[label, b"eth"], 2)
            } else {
                owner(eth, label, OWNER, 2, 0)
            };
            let second = if bytes_first {
                owner(eth, label, OWNER, 3, 0)
            } else {
                wrapped(&[label, b"eth"], 3)
            };
            let prefix = input(
                vec![owner(B256::ZERO, b"eth", OWNER, 1, 0), first, second],
                1,
                3,
            );
            let (output, live) = interpret_test_batch_incremental(prefix.clone(), None)?;
            let shadow = output
                .name_surfaces
                .iter()
                .find(|row| {
                    row.namehash == format!("{child:#x}") && row.visibility_state == "shadow"
                })
                .unwrap();
            assert_eq!(
                shadow.labelhashes,
                [label, b"eth"].map(super::common::hash_hex)
            );
            assert!(
                !shadow
                    .raw
                    .as_ref()
                    .unwrap()
                    .preimage_event_identity
                    .is_empty()
            );
            let prior =
                seam::fold_prior_events(Vec::new(), &output.normalized_events, &prefix.blocks)?;
            let tail = input(
                vec![
                    owner(eth, label, OWNER, 4, 0),
                    owner(eth, label, OWNER, 4, 1),
                    resolver(child, 4, 2),
                ],
                4,
                4,
            );
            let (continued, _) = interpret_test_batch_incremental(tail.clone(), Some(live))?;
            assert!(continued.surface_bindings.is_empty());
            assert!(
                !continued
                    .normalized_events
                    .iter()
                    .any(|event| event.event_kind == "SurfaceBound")
            );
            let mut cold = tail;
            cold.prior_events = prior;
            assert_eq!(continued, interpret_test_batch(cold)?);
        }
    }
    Ok(())
}

#[test]
fn same_transaction_registrar_setup_keeps_identity_and_only_registrar_binding() -> anyhow::Result<()>
{
    for registry_first in [false, true] {
        let grant = numeric::NameRegistered {
            id: U256::from_be_bytes(*keccak256(b"lease")),
            owner: OWNER.parse()?,
            expires: U256::from(9_999),
        };
        let logs = vec![
            owner(B256::ZERO, b"eth", OWNER, 1, 0),
            owner(
                node(&[b"eth"]),
                b"lease",
                OWNER,
                2,
                i64::from(!registry_first),
            ),
            raw_at(
                grant.encode_log_data(),
                2,
                i64::from(registry_first),
                REGISTRAR,
            ),
        ];
        let mut fixture = input(logs, 1, 2);
        fixture
            .raw_logs
            .sort_by_key(|log| (log.block_number, log.log_index));
        let output = interpret_test_batch(fixture.clone())?;
        let id = format!("ens:{:#x}", node(&[b"lease", b"eth"]));
        let grant = output
            .normalized_events
            .iter()
            .find(|event| event.event_kind == "RegistrationGranted")
            .unwrap();
        assert_eq!(grant.logical_name_id.as_deref(), Some(id.as_str()));
        let bindings = output
            .surface_bindings
            .iter()
            .filter(|binding| binding.logical_name_id == id)
            .collect::<Vec<_>>();
        assert_eq!(bindings.len(), 1, "{output:#?}");
        assert_eq!(Some(bindings[0].resource_id), grant.resource_id);
        assert!(
            output
                .normalized_events
                .iter()
                .any(
                    |event| event.logical_name_id.as_deref() == Some(id.as_str())
                        && event.after_state[seam::NAME_IDENTITY_OBSERVED_KEY] == true
                )
        );
        let mut next = input(vec![resolver(node(&[b"lease", b"eth"]), 3, 0)], 3, 3);
        next.prior_events =
            seam::fold_prior_events(Vec::new(), &output.normalized_events, &fixture.blocks)?;
        let restored = interpret_test_batch(next)?;
        assert!(
            restored
                .normalized_events
                .iter()
                .filter(|event| event.event_kind == "ResolverChanged")
                .all(|event| event.logical_name_id.as_deref() == Some(id.as_str()))
        );
    }
    Ok(())
}
