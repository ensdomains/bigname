//! A `ProxyDeployed` from a reviewed deployment's VerifiableFactory that names one of that
//! deployment's reviewed registry implementations keeps its `ContractDiscovered` row, which the
//! permission reader later uses as the registry's code model. Every other factory log keeps none.

use super::*;

const MIGRATION_MANIFEST: i64 = 3;
const PROXY: &str = "0x00000000000000000000000000000000000000ab";

sol! {
    event ProxyDeployed(address indexed sender, address indexed proxyAddress, uint256 salt, address implementation);
}

struct Deployment {
    chain: &'static str,
    label: &'static str,
    factory: &'static str,
    wrapper: &'static str,
    user: &'static str,
}

// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/VerifiableFactory.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/WrapperRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/UserRegistryImpl.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const SEPOLIA: Deployment = Deployment {
    chain: "ethereum-sepolia",
    label: "ens_v2_sepolia_20261001",
    factory: "0xda70306c98e97ece36f997a21368e53298572991",
    wrapper: "0xbe768b63e5fbbfbb0ae97e9064e0002df8001880",
    user: "0x9bd8a88719068d09ecee662f36c0e3856708366a",
};

// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/VerifiableFactory.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UserRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const MAINNET: Deployment = Deployment {
    chain: "ethereum-mainnet",
    label: "ens_v2_mainnet_20261002",
    factory: "0x77510ba162dfea3d8989e3f0928a691b345730d2",
    wrapper: "0xcf3edb3f5501de63b57254d910b6782ee370039b",
    user: "0x75ff9adbcfce2469b097ac68f6ae650eae1351bb",
};

// The two WrapperRegistryImpl generations the mainnet deployment replaced. No manifest admits them.
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-root-20261013/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-redeploy-20261018/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@b1f97b91)
const ARCHIVED: [&str; 2] = [
    "0x93b83f7861306dffa2176fd534f9729aa38e89e1",
    "0xb0c8e6f3bdc0e225b7cad0d507d191884ee9acfc",
];

fn role_at(instance: u128, role: &str, address: &str, start: i64) -> AddressAdmissionInput {
    let mut value = admission(MIGRATION_MANIFEST, role);
    value.address = address.to_owned();
    value.contract_instance_id = Uuid::from_u128(instance);
    value.active_from_block = Some(start);
    value
}

/// A migration manifest on `chain` under `label` that declares `deployment`'s factory and
/// implementations, and one factory log naming `implementation`. Returns the
/// `ContractDiscovered` rows kept.
fn kept_origins(
    chain: &str,
    label: &str,
    deployment: &Deployment,
    implementation: &str,
) -> anyhow::Result<Vec<(String, String)>> {
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
    let admissions = vec![
        role_at(
            1,
            "graveyard",
            "0x00000000000000000000000000000000000000e3",
            0,
        ),
        role_at(
            2,
            "unlocked_migration_controller",
            "0x00000000000000000000000000000000000000e4",
            0,
        ),
        role_at(
            3,
            "locked_migration_controller",
            "0x00000000000000000000000000000000000000e5",
            0,
        ),
        role_at(4, "verifiable_factory", deployment.factory, 1),
        role_at(5, "wrapper_registry_implementation", deployment.wrapper, 1),
        role_at(6, "user_registry_implementation", deployment.user, 1),
    ];
    let mut deployed = raw_at(
        ProxyDeployed {
            sender: "0x0000000000000000000000000000000000000055".parse()?,
            proxyAddress: PROXY.parse()?,
            salt: U256::from(7),
            implementation: implementation.parse()?,
        }
        .encode_log_data(),
        2,
        0,
        deployment.factory,
    );
    deployed.chain_id = chain.to_owned();
    let output = interpret_test_batch(BatchInput {
        chain_id: chain.to_owned(),
        manifests: vec![manifest],
        discovery_rules: Vec::new(),
        admissions,
        prior_events: Vec::new(),
        blocks: Vec::new(),
        raw_logs: vec![deployed],
    })?;
    Ok(output
        .normalized_events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v2_migration_l1" && event.event_kind == "ContractDiscovered"
        })
        .map(|event| {
            (
                event.after_state["proxy_address"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                event.after_state["implementation"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            )
        })
        .collect())
}

fn kept(implementation: &str) -> Vec<(String, String)> {
    vec![(PROXY.to_owned(), implementation.to_owned())]
}

#[test]
fn mainnet_factory_origins_of_the_reviewed_implementations_are_kept() -> anyhow::Result<()> {
    for implementation in [MAINNET.wrapper, MAINNET.user] {
        assert_eq!(
            kept_origins(MAINNET.chain, MAINNET.label, &MAINNET, implementation)?,
            kept(implementation),
            "{implementation}"
        );
    }
    Ok(())
}

#[test]
fn mainnet_factory_origins_of_archived_implementations_are_not_kept() -> anyhow::Result<()> {
    for archived in ARCHIVED {
        // The log names an archived generation while the manifest declares the current one.
        let current = kept_origins(MAINNET.chain, MAINNET.label, &MAINNET, archived)?;
        assert!(current.is_empty(), "{archived}: {current:?}");
        // Even a manifest that declared the archived generation in the role cannot admit it.
        let declaring = Deployment {
            wrapper: archived,
            ..MAINNET
        };
        let declared = kept_origins(MAINNET.chain, MAINNET.label, &declaring, archived)?;
        assert!(declared.is_empty(), "{archived}: {declared:?}");
    }
    Ok(())
}

#[test]
fn sepolia_factory_origins_are_unchanged() -> anyhow::Result<()> {
    for implementation in [SEPOLIA.wrapper, SEPOLIA.user] {
        assert_eq!(
            kept_origins(SEPOLIA.chain, SEPOLIA.label, &SEPOLIA, implementation)?,
            kept(implementation),
            "{implementation}"
        );
    }
    Ok(())
}

#[test]
fn a_deployment_row_applies_only_under_its_own_label() -> anyhow::Result<()> {
    for (label, deployment) in [(SEPOLIA.label, &MAINNET), (MAINNET.label, &SEPOLIA)] {
        for implementation in [deployment.wrapper, deployment.user] {
            let origins = kept_origins(deployment.chain, label, deployment, implementation)?;
            assert!(origins.is_empty(), "{label} {implementation}: {origins:?}");
        }
    }
    Ok(())
}

#[test]
fn a_deployment_row_applies_only_on_its_own_chain() -> anyhow::Result<()> {
    for (chain, deployment) in [(MAINNET.chain, &SEPOLIA), (SEPOLIA.chain, &MAINNET)] {
        for implementation in [deployment.wrapper, deployment.user] {
            let origins = kept_origins(chain, deployment.label, deployment, implementation)?;
            assert!(origins.is_empty(), "{chain} {implementation}: {origins:?}");
        }
    }
    Ok(())
}
