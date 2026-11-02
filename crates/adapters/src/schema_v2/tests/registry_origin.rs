//! A `ProxyDeployed` keeps its `ContractDiscovered` row, which the permission reader later uses
//! as the registry's code model, exactly when the active migration manifest declares its emitter
//! as the `verifiable_factory` and its implementation under a registry implementation role, each
//! with an admitted range that holds the log's block. Every other factory log keeps none unless
//! the controller correlation already made it a candidate.

use super::*;

const MIGRATION_MANIFEST: i64 = 3;
const PROXY: &str = "0x00000000000000000000000000000000000000ab";
const BLOCK: i64 = 20;
const WRAPPER_ROLE: &str = "wrapper_registry_implementation";
const USER_ROLE: &str = "user_registry_implementation";
const MAINNET: &str = "ethereum-mainnet";
const MAINNET_LABEL: &str = "ens_v2_mainnet_20261002";

sol! {
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
}

// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/VerifiableFactory.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const FACTORY: &str = "0x77510ba162dfea3d8989e3f0928a691b345730d2";
// The pair the 2026-10-26 redeploy installed, which the mainnet manifest declares.
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UserRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const WRAPPER: &str = "0xe86a4ef4d7a210aaa1d6a9594fc7add7cd2cf888";
const USER: &str = "0xfaa23592aca10a6d8875b18ea86954e8eb6d8d72";
// The 2026-10-02 pair, as a manifest would declare a further pair beside the current one.
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-redeploy-20261023/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-redeploy-20261026/UserRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const OLD_WRAPPER: &str = "0xcf3edb3f5501de63b57254d910b6782ee370039b";
const OLD_USER: &str = "0x75ff9adbcfce2469b097ac68f6ae650eae1351bb";
// Replaced WrapperRegistryImpl generations that no manifest declares: 2026-10-13, 2026-10-18
// and 2026-10-23.
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-root-20261013/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-redeploy-20261018/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-redeploy-20261026/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const UNDECLARED: [&str; 3] = [
    "0x93b83f7861306dffa2176fd534f9729aa38e89e1",
    "0xb0c8e6f3bdc0e225b7cad0d507d191884ee9acfc",
    "0x18580394dd1adeed3e9a933ffa7881df403b981d",
];
const HELPER: &str = "0x00000000000000000000000000000000000000e6";

/// One declaration of the migration manifest: role, address and admitted range.
#[derive(Clone, Copy)]
struct Declared(&'static str, &'static str, i64, Option<i64>);

fn current() -> Vec<Declared> {
    vec![
        Declared("verifiable_factory", FACTORY, 1, None),
        Declared(WRAPPER_ROLE, WRAPPER, BLOCK, None),
        Declared(USER_ROLE, USER, BLOCK, None),
    ]
}

fn admissions(declared: &[Declared]) -> Vec<AddressAdmissionInput> {
    [
        Declared(
            "graveyard",
            "0x00000000000000000000000000000000000000e3",
            0,
            None,
        ),
        Declared(
            "unlocked_migration_controller",
            "0x00000000000000000000000000000000000000e4",
            0,
            None,
        ),
        Declared(
            "locked_migration_controller",
            "0x00000000000000000000000000000000000000e5",
            0,
            None,
        ),
        Declared("migration_helper", HELPER, 0, None),
    ]
    .into_iter()
    .chain(declared.iter().copied())
    .enumerate()
    .map(|(index, Declared(role, address, from, to))| {
        let mut value = admission(MIGRATION_MANIFEST, role);
        value.address = address.to_owned();
        value.contract_instance_id = Uuid::from_u128(100 + index as u128);
        value.active_from_block = Some(from);
        value.active_to_block = to;
        value
    })
    .collect()
}

/// Whether one factory log at `BLOCK` from `emitter` naming `implementation` keeps its
/// `ContractDiscovered` row under a migration manifest on `chain` and `label` that declares
/// `declared`. The log's sender is no migration controller, so no correlation applies.
fn kept_on(
    chain: &str,
    label: &str,
    declared: &[Declared],
    emitter: &str,
    implementation: &str,
) -> anyhow::Result<bool> {
    let mut manifest = manifest(
        MIGRATION_MANIFEST,
        "ens_v2_migration_l1",
        "ProxyDeployed",
        "event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation)",
        &["verifiable_factory"],
        &["ContractDiscovered"],
    );
    manifest.chain_id = chain.to_owned();
    manifest.deployment_label = label.to_owned();
    let mut payload: serde_json::Value = serde_json::from_str(&manifest.payload_json)?;
    payload["chain"] = json!(chain);
    payload["correlation_addresses"] = json!({
        "ens_v1_name_wrapper": "0x00000000000000000000000000000000000000e1",
        "ens_v1_base_registrar": "0x00000000000000000000000000000000000000e2",
    });
    manifest.payload_json = payload.to_string();
    let mut deployed = raw_at(
        ProxyDeployed {
            sender: "0x0000000000000000000000000000000000000055".parse()?,
            proxyAddress: PROXY.parse()?,
            salt: U256::from(7),
            implementation: implementation.parse()?,
        }
        .encode_log_data(),
        BLOCK,
        0,
        emitter,
    );
    deployed.chain_id = chain.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: chain.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions: admissions(declared),
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![deployed],
    })?;
    let origins: Vec<_> = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v2_migration_l1" && event.event_kind == "ContractDiscovered"
        })
        .map(|event| {
            (
                event.after_state["proxy_address"].clone(),
                event.after_state["implementation"].clone(),
            )
        })
        .collect();
    match origins.as_slice() {
        [] => Ok(false),
        [(proxy, kept)]
            if proxy == PROXY
                && kept
                    .as_str()
                    .is_some_and(|kept| kept.eq_ignore_ascii_case(implementation)) =>
        {
            Ok(true)
        }
        other => anyhow::bail!("unexpected origins {other:?}"),
    }
}

fn kept(declared: &[Declared], implementation: &str) -> anyhow::Result<bool> {
    kept_on(MAINNET, MAINNET_LABEL, declared, FACTORY, implementation)
}

/// Rows 1 and 2: the declared pair is the set, and a further declared pair joins it, whatever
/// the declaration order.
#[test]
fn origins_of_every_declared_implementation_are_kept() -> anyhow::Result<()> {
    let two_pairs = [
        current(),
        vec![
            Declared(WRAPPER_ROLE, OLD_WRAPPER, 2, None),
            Declared(USER_ROLE, OLD_USER, 2, None),
        ],
    ]
    .concat();
    let mut reversed = two_pairs.clone();
    reversed.reverse();
    let mut failures = Vec::new();
    for (case, declared, implementation) in [
        ("current wrapper", current(), WRAPPER),
        ("current user", current(), USER),
        (
            "checksummed current wrapper",
            current(),
            "0xE86a4EF4d7a210AAa1d6A9594fC7aDD7Cd2Cf888",
        ),
        ("two pairs, current wrapper", two_pairs.clone(), WRAPPER),
        ("two pairs, current user", two_pairs.clone(), USER),
        ("two pairs, old wrapper", two_pairs.clone(), OLD_WRAPPER),
        ("two pairs, old user", two_pairs.clone(), OLD_USER),
        ("reversed, current wrapper", reversed.clone(), WRAPPER),
        ("reversed, old user", reversed.clone(), OLD_USER),
    ] {
        if !kept(&declared, implementation)? {
            failures.push(case);
        }
    }
    anyhow::ensure!(failures.is_empty(), "dropped: {failures:?}");
    Ok(())
}

/// Rows 3 to 7: an undeclared implementation, a log outside a declaration's admitted range, a
/// factory that is undeclared or not yet admitted, another emitter, and an address the manifest
/// declares only under another role.
#[test]
fn origins_outside_the_declared_set_are_not_kept() -> anyhow::Result<()> {
    let _dropped = allow_unmatched(&[
        (
            "0x77510ba162dfea3d8989e3f0928a691b345730d2",
            "the test proves a factory outside the declared set announces nothing",
        ),
        (
            HELPER,
            "the test proves a ProxyDeployed from the migration helper, declared under another \
             role, announces nothing",
        ),
    ]);
    let starting = |from: i64| {
        vec![
            Declared("verifiable_factory", FACTORY, 1, None),
            Declared(WRAPPER_ROLE, WRAPPER, from, None),
        ]
    };
    let ending = |to: i64| {
        vec![
            Declared("verifiable_factory", FACTORY, 1, None),
            Declared(WRAPPER_ROLE, OLD_WRAPPER, 2, Some(to)),
        ]
    };
    let factory_from = |from: i64| {
        vec![
            Declared("verifiable_factory", FACTORY, from, None),
            Declared(WRAPPER_ROLE, WRAPPER, 1, None),
        ]
    };
    // (case, declarations, emitter, implementation, kept)
    let mut cases = vec![
        ("start = block", starting(BLOCK), FACTORY, WRAPPER, true),
        (
            "start = block + 1",
            starting(BLOCK + 1),
            FACTORY,
            WRAPPER,
            false,
        ),
        ("closed at block", ending(BLOCK), FACTORY, OLD_WRAPPER, true),
        (
            "closed at block - 1",
            ending(BLOCK - 1),
            FACTORY,
            OLD_WRAPPER,
            false,
        ),
        (
            "factory from block",
            factory_from(BLOCK),
            FACTORY,
            WRAPPER,
            true,
        ),
        (
            "factory from block + 1",
            factory_from(BLOCK + 1),
            FACTORY,
            WRAPPER,
            false,
        ),
        (
            "factory undeclared",
            vec![Declared(WRAPPER_ROLE, WRAPPER, 1, None)],
            FACTORY,
            WRAPPER,
            false,
        ),
        ("another emitter", current(), HELPER, WRAPPER, false),
        (
            "the helper as implementation",
            current(),
            FACTORY,
            HELPER,
            false,
        ),
        (
            "the factory as implementation",
            current(),
            FACTORY,
            FACTORY,
            false,
        ),
    ];
    for undeclared in UNDECLARED {
        cases.push((
            "undeclared implementation",
            current(),
            FACTORY,
            undeclared,
            false,
        ));
    }
    let mut failures = Vec::new();
    for (case, declared, emitter, implementation, expected) in cases {
        let kept = kept_on(MAINNET, MAINNET_LABEL, &declared, emitter, implementation)?;
        if kept != expected {
            failures.push(format!("{case} {implementation}: kept {kept}"));
        }
    }
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// Rows 16 and 17: the chain's own migration manifest decides, under any chain and deployment
/// label. The epoch is the permission reader's gate, not this one.
#[test]
fn the_chains_manifest_decides_under_any_label() -> anyhow::Result<()> {
    for (chain, label) in [
        (MAINNET, "ens_v2_sepolia_20261001"),
        ("ethereum-sepolia", MAINNET_LABEL),
        ("ethereum-sepolia", "unreviewed"),
    ] {
        assert!(
            kept_on(chain, label, &current(), FACTORY, WRAPPER)?,
            "{chain} {label}"
        );
    }
    Ok(())
}
