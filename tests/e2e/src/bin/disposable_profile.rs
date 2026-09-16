use alloy_primitives::Address;
use anyhow::{Context, Result};
use bigname_e2e::harness::{manifests, repo_root};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().collect();
    anyhow::ensure!(
        args.len() == 3,
        "usage: disposable_profile deployment-addresses.json owned-output-dir"
    );
    let input: Value = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    anyhow::ensure!(input["chainId"] == 11155111, "wrong exercise chain");
    let out = PathBuf::from(&args[2]);
    // One fresh profile per chain. Compilation is a separately bounded command.
    std::fs::create_dir(&out)?;
    let address = |name: &str| -> Result<Address> {
        input["deployments"][name]["address"]
            .as_str()
            .with_context(|| format!("missing actual deployment {name}"))?
            .parse()
            .with_context(|| format!("invalid deployment address {name}"))
    };
    // Block zero is a conservative local start: include the complete deployment history.
    let group = |pairs: &[(&'static str, &str)]| -> Result<HashMap<&'static str, (Address, u64)>> {
        pairs
            .iter()
            .map(|(role, deployment)| Ok((*role, (address(deployment)?, 0))))
            .collect()
    };
    let v1 = group(&[
        ("ENSRegistry", "ENSRegistry"),
        ("registry", "ENSRegistry"),
        ("ETHRegistrar", "BaseRegistrarImplementation"),
        ("registrar", "BaseRegistrarImplementation"),
        ("name_wrapper", "NameWrapper"),
        ("public_resolver", "PublicResolver"),
    ])?;
    let v2 = group(&[
        ("RootRegistry", "RootRegistry"),
        ("root_registry", "RootRegistry"),
        ("ETHRegistry", "ETHRegistry"),
        ("registry", "ETHRegistry"),
        ("ETHRegistrar", "ETHRegistrar"),
        ("registrar", "ETHRegistrar"),
    ])?;
    let migration = group(&[
        (
            "unlocked_migration_controller",
            "UnlockedMigrationController",
        ),
        ("locked_migration_controller", "LockedMigrationController"),
        ("graveyard", "Graveyard"),
        ("verifiable_factory", "VerifiableFactory"),
        ("wrapper_registry_implementation", "WrapperRegistryImpl"),
        ("ens_v1_renewal_bridge", "ETHRenewerV1"),
        ("batch_registrar", "BatchRegistrar"),
        ("migration_helper", "MigrationHelper"),
    ])?;
    let correlations = HashMap::from([
        ("ens_v1_name_wrapper", address("NameWrapper")?),
        (
            "ens_v1_base_registrar",
            address("BaseRegistrarImplementation")?,
        ),
    ]);
    let profile = manifests::generate_local_sepolia_migration_profile(
        &out,
        &repo_root(),
        &v1,
        &v2,
        &migration,
        &correlations,
    )?;
    complete_disposable_profile(&profile.root, &repo_root(), &input)?;
    std::fs::write(
        out.join("profile-generated.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "profile": profile.root, "binary": null, "compiled": false,
            "start_block": 0, "source_semantics": "Bounded PublicResolverV2 node-record interpreter candidate; local deployment bindings and existing controller/reverse ABI admissions",
            "limitation": "Local profile is not a shipped Sepolia/Mainnet admission. PublicResolverV2 is directly declared for five node record events; runtime compatibility is unverified. Actual indexed corpus assertions determine compatibility."
        }))?,
    )?;
    Ok(())
}

// Exercise-local manifest composition only. This does not edit shipped manifests.
fn complete_disposable_profile(
    root: &std::path::Path,
    repo: &std::path::Path,
    input: &Value,
) -> Result<()> {
    use toml::Value as T;
    let address = |name: &str| -> Result<String> {
        let raw = input["deployments"][name]["address"]
            .as_str()
            .with_context(|| format!("missing actual deployment {name}"))?;
        let parsed: Address = raw.parse()?;
        anyhow::ensure!(parsed != Address::ZERO, "zero deployment {name}");
        Ok(format!("{parsed:#x}"))
    };
    let ens = root.join("ethereum/ens");
    let path = ens.join("ens_v1_registrar_l1/v1.toml");
    let mut registrar: T = std::fs::read_to_string(&path)?.parse()?;
    let reference: T = std::fs::read_to_string(
        repo.join("manifests/mainnet/ethereum/ens/ens_v1_registrar_l1/v1.toml"),
    )?
    .parse()?;
    let role = "unwrapped_registrar_controller";
    let mut contract = reference["contracts"]
        .as_array()
        .context("reference contracts")?
        .iter()
        .find(|entry| entry["role"].as_str() == Some(role))
        .context("controller role")?
        .clone();
    contract["address"] = T::String(address("ETHRegistrarController")?);
    contract["start_block"] = T::Integer(0);
    registrar["contracts"]
        .as_array_mut()
        .context("local contracts")?
        .push(contract);
    for event in reference["abi"]["events"]
        .as_array()
        .context("reference events")?
    {
        if event
            .get("emitter_roles")
            .and_then(T::as_array)
            .is_some_and(|roles| roles.iter().any(|r| r.as_str() == Some(role)))
        {
            let mut event = event.clone();
            event["emitter_roles"] = T::Array(vec![T::String(role.into())]);
            registrar["abi"]["events"]
                .as_array_mut()
                .context("local events")?
                .push(event);
        }
    }
    std::fs::write(&path, toml::to_string(&registrar)?)?;

    let reverse_dir = ens.join("ens_v1_reverse_l1");
    std::fs::create_dir_all(&reverse_dir)?;
    let mut reverse: T = std::fs::read_to_string(
        repo.join("manifests/mainnet/ethereum/ens/ens_v1_reverse_l1/v1.toml"),
    )?
    .parse()?;
    reverse["chain"] = T::String("ethereum-sepolia".into());
    for contract in reverse["contracts"]
        .as_array_mut()
        .context("reverse contracts")?
    {
        contract["address"] = T::String(address("ReverseRegistrar")?);
        contract["start_block"] = T::Integer(0);
    }
    // DefaultReverseRegistrar has a different ABI and is not aliased to ReverseClaimed.
    std::fs::write(reverse_dir.join("v1.toml"), toml::to_string(&reverse)?)?;

    for entry in std::fs::read_dir(ens.join("ens_v2_resolver_l1"))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("toml") {
            continue;
        }
        let mut resolver: T = std::fs::read_to_string(&path)?.parse()?;
        if let Some(implementations) = resolver
            .get_mut("resolver_implementations")
            .and_then(T::as_array_mut)
        {
            for implementation in implementations {
                anyhow::ensure!(
                    implementation["role"].as_str() == Some("permissioned_resolver"),
                    "unhandled resolver implementation"
                );
                implementation["address"] = T::String(address("PermissionedResolverImpl")?);
            }
        }
        let public_address = address("PublicResolverV2")?;
        resolver["contracts"]
            .as_array_mut()
            .context("resolver contracts")?
            .push(T::try_from(
                serde_json::json!({"role":"public_resolver_v2", "address":public_address,
                "proxy_kind":"none", "start_block":0}),
            )?);
        let events = resolver["abi"]["events"]
            .as_array_mut()
            .context("resolver events")?;
        // Keep the four shared empty-role ABIs for discovered PermissionedResolver instances.
        for name in [
            "AddressChanged",
            "TextChanged",
            "ContenthashChanged",
            "VersionChanged",
        ] {
            anyhow::ensure!(
                events
                    .iter()
                    .any(|event| event["name"].as_str() == Some(name)),
                "missing shared resolver event {name}"
            );
        }
        anyhow::ensure!(
            !events
                .iter()
                .any(|event| event["name"].as_str() == Some("AddrChanged")),
            "unexpected existing AddrChanged declaration"
        );
        events.push(T::try_from(serde_json::json!({"name":"AddrChanged",
            "fragment":"event AddrChanged(bytes32 indexed node, address a)",
            "emitter_roles":["public_resolver_v2"], "normalized_events":["RecordChanged"]}))?);
        std::fs::write(path, toml::to_string(&resolver)?)?;
    }
    Ok(())
}
