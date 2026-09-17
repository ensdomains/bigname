//! Resolver admission announced by the proxy (`Upgraded`) or its factory (`ProxyDeployed`)
//! rather than by a registry pointer, plus the same-block and same-batch admission-order rules.

use super::*;

const ROOT_MANIFEST: i64 = 1;
const RESOLVER_MANIFEST: i64 = 2;
const MIGRATION_MANIFEST: i64 = 3;
const RESOLVER: &str = "0x00000000000000000000000000000000000000aa";
const FACTORY: &str = "0x00000000000000000000000000000000000000fa";
const IMPLEMENTATION: &str = "0x0000000000000000000000000000000000000077";
const OTHER_IMPLEMENTATION: &str = "0x0000000000000000000000000000000000000099";
const ACCOUNT: &str = "0x0000000000000000000000000000000000000055";

sol! {
    event Upgraded(address indexed implementation);
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
}

fn root_manifest() -> ManifestInput {
    manifest(
        ROOT_MANIFEST,
        "ens_v2_root_l1",
        "ResolverUpdated",
        "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)",
        &["root_registry"],
        &["ResolverChanged"],
    )
}

fn resolver_manifest() -> ManifestInput {
    let mut manifest = manifest_with_events(
        RESOLVER_MANIFEST,
        "ens",
        "ens_v2_resolver_l1",
        &[
            (
                "Upgraded",
                "event Upgraded(address indexed implementation)",
                &[],
                &["Upgraded"],
            ),
            (
                "EACRolesChanged",
                "event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap)",
                &[],
                &["PermissionChanged"],
            ),
            (
                "TextChanged",
                "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
                &[],
                &["RecordChanged"],
            ),
        ],
    );
    let mut payload: serde_json::Value = serde_json::from_str(&manifest.payload_json).unwrap();
    payload["resolver_implementations"] =
        json!([{"role": "permissioned_resolver", "address": IMPLEMENTATION}]);
    manifest.payload_json = payload.to_string();
    manifest
}

fn migration_manifest() -> ManifestInput {
    let mut manifest = manifest(
        MIGRATION_MANIFEST,
        "ens_v2_migration_l1",
        "ProxyDeployed",
        "event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation)",
        &["verifiable_factory"],
        &["ContractDiscovered"],
    );
    let mut payload: serde_json::Value = serde_json::from_str(&manifest.payload_json).unwrap();
    payload["correlation_addresses"] = json!({
        "ens_v1_name_wrapper": "0x00000000000000000000000000000000000000e1",
        "ens_v1_base_registrar": "0x00000000000000000000000000000000000000e2",
    });
    manifest.payload_json = payload.to_string();
    manifest
}

fn root_rule() -> DiscoveryRuleInput {
    DiscoveryRuleInput {
        manifest_id: ROOT_MANIFEST,
        edge_kind: "resolver".to_owned(),
        from_role: Some("root_registry".to_owned()),
        admission: "reachable_from_root".to_owned(),
    }
}

fn factory_admission() -> AddressAdmissionInput {
    let mut value = admission(MIGRATION_MANIFEST, "verifiable_factory");
    value.address = FACTORY.to_owned();
    value
}

/// Migration correlation requires these declarations whenever the family is active.
fn migration_admissions() -> Vec<AddressAdmissionInput> {
    [
        ("graveyard", 0xe3_u128),
        ("unlocked_migration_controller", 0xe4),
        ("locked_migration_controller", 0xe5),
    ]
    .into_iter()
    .map(|(role, address)| {
        let mut value = admission(MIGRATION_MANIFEST, role);
        value.address = format!("{:#042x}", address);
        value.contract_instance_id = Uuid::from_u128(address);
        value
    })
    .chain([factory_admission()])
    .collect()
}

fn upgraded(implementation: &str, block: i64, log_index: i64) -> RawLogInput {
    raw_at(
        Upgraded {
            implementation: implementation.parse().unwrap(),
        }
        .encode_log_data(),
        block,
        log_index,
        RESOLVER,
    )
}

fn roles_changed(block: i64, log_index: i64) -> RawLogInput {
    raw_at(
        EACRolesChanged {
            resource: U256::ZERO,
            account: ACCOUNT.parse().unwrap(),
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: U256::from(1),
        }
        .encode_log_data(),
        block,
        log_index,
        RESOLVER,
    )
}

fn text_changed(block: i64, log_index: i64) -> RawLogInput {
    raw_at(
        TextChanged {
            node: keccak256(b"box"),
            indexedKey: keccak256(b"url"),
            key: "url".to_owned(),
            value: "https://example.test".to_owned(),
        }
        .encode_log_data(),
        block,
        log_index,
        RESOLVER,
    )
}

fn pointer(block: i64, log_index: i64) -> RawLogInput {
    raw_at(
        v2_registry::ResolverUpdated {
            tokenId: U256::from(1),
            resolver: RESOLVER.parse().unwrap(),
            sender: CONTRACT.parse().unwrap(),
        }
        .encode_log_data(),
        block,
        log_index,
        CONTRACT,
    )
}

fn interpret(raw_logs: Vec<RawLogInput>) -> anyhow::Result<BatchOutput> {
    interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![root_manifest(), resolver_manifest()],
        discovery_rules: vec![root_rule()],
        admissions: vec![admission(ROOT_MANIFEST, "root_registry")],
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    })
}

fn interpret_with_factory(raw_logs: Vec<RawLogInput>) -> anyhow::Result<BatchOutput> {
    interpret_test_batch(BatchInput {
        chain_id: CHAIN.to_owned(),
        manifests: vec![root_manifest(), resolver_manifest(), migration_manifest()],
        discovery_rules: vec![root_rule()],
        admissions: [admission(ROOT_MANIFEST, "root_registry")]
            .into_iter()
            .chain(migration_admissions())
            .collect(),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs,
    })
}

fn event_kinds(output: &BatchOutput) -> Vec<(i64, i64, String)> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.raw_fact_ref["kind"] != "raw_block")
        .map(|event| {
            (
                event.block_number.unwrap(),
                event.log_index.unwrap(),
                event.event_kind.clone(),
            )
        })
        .collect()
}

fn resolver_edges(output: &BatchOutput) -> Vec<&DiscoveryEdge> {
    output
        .discovery_edges
        .iter()
        .filter(|edge| edge.edge_kind == "resolver")
        .collect()
}

#[test]
fn upgraded_naming_a_declared_implementation_admits_the_proxy_from_that_block() -> anyhow::Result<()>
{
    let output = interpret(vec![
        upgraded(IMPLEMENTATION, 1, 0),
        roles_changed(1, 1),
        text_changed(2, 0),
    ])?;

    assert_eq!(
        event_kinds(&output),
        [
            (1, 0, "Upgraded".to_owned()),
            (1, 1, "PermissionChanged".to_owned()),
            (2, 0, "RecordChanged".to_owned()),
        ]
    );
    let upgraded = &output.normalized_events[0];
    assert_eq!(upgraded.source_family, "ens_v2_resolver_l1");
    assert_eq!(upgraded.source_manifest_id, Some(RESOLVER_MANIFEST));
    assert_eq!(upgraded.after_state["proxy_address"], RESOLVER);
    assert_eq!(upgraded.after_state["implementation"], IMPLEMENTATION);

    let edges = resolver_edges(&output);
    assert_eq!(edges.len(), 1);
    let edge = edges[0];
    assert_eq!(edge.discovery_source, "Upgraded");
    assert_eq!(edge.admission_basis, "declared_resolver_implementation");
    assert_eq!(edge.source_manifest_id, RESOLVER_MANIFEST);
    assert_eq!(edge.active_from_block_number, 1);
    assert_eq!(
        edge.observation_key,
        format!("resolver-announcement:upgraded:{RESOLVER}")
    );
    let implementation_edge = output
        .discovery_edges
        .iter()
        .find(|edge| edge.edge_kind == "proxy_implementation")
        .expect("Upgraded still records the proxy implementation edge");
    assert_eq!(
        edge.from_contract_instance_id,
        implementation_edge.to_contract_instance_id
    );
    assert_eq!(
        edge.to_contract_instance_id,
        implementation_edge.from_contract_instance_id
    );
    let address = output
        .contract_addresses
        .iter()
        .find(|address| address.address == RESOLVER)
        .expect("announced proxy address interval");
    assert_eq!(address.source_manifest_id, RESOLVER_MANIFEST);
    assert_eq!(address.active_from_block_number, 1);
    assert!(output.decode_skips.is_empty());
    Ok(())
}

#[test]
fn upgraded_naming_an_undeclared_implementation_is_not_selected_from_an_unknown_emitter()
-> anyhow::Result<()> {
    let output = interpret(vec![
        upgraded(OTHER_IMPLEMENTATION, 1, 0),
        text_changed(1, 1),
    ])?;

    assert!(event_kinds(&output).is_empty());
    assert!(output.discovery_edges.is_empty());
    assert!(output.decode_skips.is_empty());
    Ok(())
}

#[test]
fn proxy_deployed_naming_a_declared_implementation_admits_the_proxy() -> anyhow::Result<()> {
    let deployed = raw_at(
        ProxyDeployed {
            sender: ACCOUNT.parse()?,
            proxyAddress: RESOLVER.parse()?,
            salt: U256::from(7),
            implementation: IMPLEMENTATION.parse()?,
        }
        .encode_log_data(),
        1,
        0,
        FACTORY,
    );
    let output = interpret_with_factory(vec![deployed, text_changed(1, 1)])?;

    // An uncorrelated factory log keeps no `ContractDiscovered` row; the announcement is
    // recorded as the proxy's implementation observation instead.
    assert_eq!(
        event_kinds(&output),
        [
            (1, 0, "Upgraded".to_owned()),
            (1, 1, "RecordChanged".to_owned()),
        ]
    );
    let observed = &output.normalized_events[0];
    assert_eq!(observed.source_family, "ens_v2_resolver_l1");
    assert_eq!(observed.source_manifest_id, Some(RESOLVER_MANIFEST));
    assert_eq!(observed.after_state["source_event"], "ProxyDeployed");
    assert_eq!(observed.after_state["proxy_address"], RESOLVER);
    assert_eq!(observed.after_state["implementation"], IMPLEMENTATION);
    assert_eq!(
        observed.raw_fact_ref["state_scope"],
        format!("{RESOLVER}:-:-:-:Upgraded")
    );
    let edges = resolver_edges(&output);
    assert_eq!(edges.len(), 1);
    let edge = edges[0];
    assert_eq!(edge.discovery_source, "ProxyDeployed");
    assert_eq!(edge.admission_basis, "declared_resolver_implementation");
    assert_eq!(edge.source_manifest_id, RESOLVER_MANIFEST);
    assert_eq!(
        edge.from_contract_instance_id,
        factory_admission().contract_instance_id
    );
    assert_eq!(
        edge.observation_key,
        format!("resolver-announcement:proxydeployed:{RESOLVER}")
    );
    Ok(())
}

#[test]
fn proxy_deployed_naming_an_undeclared_implementation_admits_nothing() -> anyhow::Result<()> {
    let deployed = raw_at(
        ProxyDeployed {
            sender: ACCOUNT.parse()?,
            proxyAddress: RESOLVER.parse()?,
            salt: U256::from(7),
            implementation: OTHER_IMPLEMENTATION.parse()?,
        }
        .encode_log_data(),
        1,
        0,
        FACTORY,
    );
    let output = interpret_with_factory(vec![deployed, text_changed(1, 1)])?;

    assert!(event_kinds(&output).is_empty());
    assert!(output.discovery_edges.is_empty());
    Ok(())
}

#[test]
fn registry_pointer_admits_the_resolver_logs_earlier_in_the_same_block() -> anyhow::Result<()> {
    let output = interpret(vec![
        upgraded(OTHER_IMPLEMENTATION, 1, 0),
        roles_changed(1, 1),
        pointer(1, 2),
    ])?;

    assert_eq!(
        event_kinds(&output),
        [
            (1, 0, "Upgraded".to_owned()),
            (1, 1, "PermissionChanged".to_owned()),
            (1, 2, "ResolverChanged".to_owned()),
        ]
    );
    let edges = resolver_edges(&output);
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].discovery_source, "ResolverUpdated");
    assert!(
        output
            .discovery_edges
            .iter()
            .any(|edge| edge.edge_kind == "proxy_implementation")
    );
    assert!(output.decode_skips.is_empty());
    Ok(())
}

#[test]
fn a_log_before_a_later_block_admission_is_recorded_as_a_skip() -> anyhow::Result<()> {
    let output = interpret(vec![text_changed(1, 0), pointer(2, 0), text_changed(2, 1)])?;

    assert_eq!(
        event_kinds(&output),
        [
            (2, 0, "ResolverChanged".to_owned()),
            (2, 1, "RecordChanged".to_owned()),
        ]
    );
    assert_eq!(output.decode_skips.len(), 1);
    let skip = &output.decode_skips[0];
    assert_eq!((skip.block_number, skip.log_index), (1, 0));
    assert_eq!(skip.emitting_address, RESOLVER);
    assert_eq!(skip.source_family, "ens_v2_resolver_l1");
    assert!(!skip.match_all);
    assert!(
        skip.decode_context
            .contains("discovery admission at block 2"),
        "{}",
        skip.decode_context
    );
    Ok(())
}
