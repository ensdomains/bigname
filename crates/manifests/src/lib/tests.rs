use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new() -> Result<Self> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bigname-manifests-tests-{}-{unique}-{sequence}",
            std::process::id(),
        ));
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create test directory {}", path.display()))?;
        Ok(Self { path })
    }

    fn write_manifest(
        &self,
        namespace: &str,
        source_family: &str,
        version_tag: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let directory = self.path.join(namespace).join(source_family);
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create {}", directory.display()))?;
        let path = directory.join(format!("{version_tag}.toml"));
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }

    fn write_manifest_for_chain_combo(
        &self,
        chain_combo: &str,
        namespace: &str,
        source_family: &str,
        version_tag: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let directory = self
            .path
            .join(chain_combo)
            .join(namespace)
            .join(source_family);
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create {}", directory.display()))?;
        let path = directory.join(format!("{version_tag}.toml"));
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn manifest_contents() -> &'static str {
    r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
resolver_implementations = [
  { role = "permissioned_resolver", address = "0x00000000000000000000000000000000000000CC" },
]

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000001"
start_block = 12345

[[contracts]]
role = "registry"
address = "0x00000000000000000000000000000000000000AA"
proxy_kind = "erc1967"
implementation = "0x00000000000000000000000000000000000000DD"
start_block = 23456

[[abi.events]]
name = "SubregistryUpdated"
fragment = "event SubregistryUpdated(uint256 indexed node, address registry, address sender)"
emitter_roles = ["registry"]
normalized_events = ["SubregistryChanged"]
status = "supported"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
}

fn checked_in_manifest_root(profile_root: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(profile_root)
}

fn missing_test_path() -> Result<PathBuf> {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!(
        "bigname-manifests-missing-{}-{unique}",
        std::process::id()
    )))
}

fn load_one(contents: &str) -> Result<ManifestRepository> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v1", contents)?;
    load_repository(&test_dir.path)
}

#[test]
fn reports_missing_root() -> Result<()> {
    let repository = load_repository(missing_test_path()?)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::MissingRoot);
    assert!(repository.manifests().is_empty());
    Ok(())
}

#[test]
fn loads_manifest_declarations_abi_and_start_blocks() -> Result<()> {
    let repository = load_one(manifest_contents())?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.summary().manifest_count, 1);

    let manifest = &repository.manifests()[0].manifest;
    assert_eq!(manifest.roots[0].start_block, Some(12_345));
    assert_eq!(manifest.contracts[0].start_block, Some(23_456));
    assert_eq!(manifest.resolver_implementations.len(), 1);
    assert_eq!(
        manifest.resolver_implementations[0].role,
        "permissioned_resolver"
    );
    assert_eq!(manifest.abi.events[0].name, "SubregistryUpdated");
    assert_eq!(
        manifest.abi.events[0]
            .parsed_event_view()?
            .canonical_signature(),
        "SubregistryUpdated(uint256,address,address)"
    );
    Ok(())
}

#[test]
fn resolver_implementation_validation_preserves_alloy_address_grammar() -> Result<()> {
    let unprefixed = manifest_contents().replacen(
        "0x00000000000000000000000000000000000000CC",
        "00000000000000000000000000000000000000CC",
        1,
    );
    let repository = load_one(&unprefixed)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    Ok(())
}

#[test]
fn repository_loader_rejects_invalid_single_manifest_declarations() -> Result<()> {
    let base = manifest_contents();
    let duplicate_role = format!(
        "{base}\n[[contracts]]\nrole = \"registry\"\naddress = \"0x00000000000000000000000000000000000000BB\"\nproxy_kind = \"none\"\n"
    );
    let duplicate_implementation_address = base.replacen(
        "]\n\n[capability_flags]",
        "  { role = \"second_resolver\", address = \"0x00000000000000000000000000000000000000cc\" },\n]\n\n[capability_flags]",
        1,
    );
    let cases = [
        (
            "invalid ABI fragment",
            base.replacen("event SubregistryUpdated", "SubregistryUpdated", 1),
            "must use an event fragment",
        ),
        (
            "unknown ABI emitter role",
            base.replacen(
                "emitter_roles = [\"registry\"]",
                "emitter_roles = [\"missing_registry\"]",
                1,
            ),
            "unknown emitter role missing_registry",
        ),
        (
            "negative root start block",
            base.replacen("start_block = 12345", "start_block = -1", 1),
            "start_block must be a non-negative integer",
        ),
        (
            "namespace mismatch",
            base.replacen("namespace = \"ens\"", "namespace = \"basenames\"", 1),
            "does not match directory ens",
        ),
        (
            "duplicate contract role",
            duplicate_role,
            "duplicates contract role registry",
        ),
        (
            "invalid resolver implementation address",
            base.replacen(
                "0x00000000000000000000000000000000000000CC",
                "not-an-address",
                1,
            ),
            "has invalid address not-an-address",
        ),
        (
            "duplicate resolver implementation address",
            duplicate_implementation_address,
            "duplicates resolver implementation address",
        ),
        (
            "unsupported normalizer",
            base.replacen(
                "normalizer_version = \"ensip15@ens-normalize-0.1.1\"",
                "normalizer_version = \"ensip15@unknown\"",
                1,
            ),
            "unsupported normalizer_version ensip15@unknown",
        ),
    ];

    for (case, contents, expected) in cases {
        let error = load_one(&contents).expect_err(case);
        assert!(
            format!("{error:#}").contains(expected),
            "{case} returned an unexpected error: {error:#}"
        );
    }
    Ok(())
}

#[test]
fn repository_loader_rejects_role_free_role_sensitive_event() -> Result<()> {
    let test_dir = TestDir::new()?;
    let contents = manifest_contents()
        .replacen(
            "source_family = \"ens_v2_registry_l1\"",
            "source_family = \"ens_v1_registry_l1\"",
            1,
        )
        .replacen("name = \"SubregistryUpdated\"", "name = \"NewOwner\"", 1)
        .replacen(
            "event SubregistryUpdated(uint256 indexed node, address registry, address sender)",
            "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            1,
        )
        .replacen("emitter_roles = [\"registry\"]\n", "", 1);
    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v1", &contents)?;

    let error = load_repository(&test_dir.path)
        .expect_err("role-free NewOwner must fail repository validation");
    let message = error.to_string();
    assert!(message.contains("manifest ABI event NewOwner"));
    assert!(message.contains(
        "has empty emitter_roles; declare emitter_roles, or add the (source_family, event) pair to bigname_manifests::ROLE_INSENSITIVE_EVENTS with a justification that the adapter does not consume Selected.emitter_role"
    ));
    Ok(())
}

#[test]
fn repository_loader_rejects_chain_directory_mismatch() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest_for_chain_combo(
        "base",
        "ens",
        "ens_v2_registry_l1",
        "v1",
        manifest_contents(),
    )?;
    let error = load_repository(&test_dir.path).expect_err("chain directory mismatch must fail");
    assert!(
        error
            .to_string()
            .contains("does not match chain directory base"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn repository_loader_rejects_duplicate_storage_identity_and_active_versions() -> Result<()> {
    let duplicate_identity = TestDir::new()?;
    duplicate_identity.write_manifest("ens", "ens_v2_registry_l1", "v1", manifest_contents())?;
    duplicate_identity.write_manifest_for_chain_combo(
        "ethereum",
        "ens",
        "ens_v2_registry_l1",
        "v1",
        manifest_contents(),
    )?;
    let error = load_repository(&duplicate_identity.path)
        .expect_err("duplicate manifest storage identity must fail");
    assert!(
        error.to_string().contains("manifest storage identity")
            && error.to_string().contains("is declared by both"),
        "unexpected error: {error:#}"
    );

    let active_versions = TestDir::new()?;
    active_versions.write_manifest("ens", "ens_v2_registry_l1", "v1", manifest_contents())?;
    active_versions.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v2",
        &manifest_contents().replacen("manifest_version = 1", "manifest_version = 2", 1),
    )?;
    let error = load_repository(&active_versions.path)
        .expect_err("multiple active versions for one source family must fail");
    let message = error.to_string();
    assert!(
        message.contains("more than one active manifest version") && message.contains("v1, v2"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn repository_loader_rejects_preimage_attribution_overlap() -> Result<()> {
    let test_dir = TestDir::new()?;
    let ens = manifest_contents().replacen(
        "source_family = \"ens_v2_registry_l1\"",
        "source_family = \"ens_v1_registrar_l1\"",
        1,
    );
    let basenames = manifest_contents()
        .replacen("namespace = \"ens\"", "namespace = \"basenames\"", 1)
        .replacen(
            "source_family = \"ens_v2_registry_l1\"",
            "source_family = \"basenames_execution\"",
            1,
        )
        .replacen(
            "address = \"0x0000000000000000000000000000000000000001\"",
            "address = \"0x0000000000000000000000000000000000000002\"",
            1,
        );
    test_dir.write_manifest("ens", "ens_v1_registrar_l1", "v1", &ens)?;
    test_dir.write_manifest("basenames", "basenames_execution", "v1", &basenames)?;

    let error = load_repository(&test_dir.path)
        .expect_err("overlapping block-derived preimage attribution must fail");
    assert!(
        error
            .to_string()
            .contains("could assign one block-derived preimage log to two sources"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn rejects_unsupported_discovery_admission() -> Result<()> {
    let contents = manifest_contents().replacen(
        "admission = \"reachable_from_root\"",
        "admission = \"manifest_declared\"",
        1,
    );
    let error = load_one(&contents).expect_err("unsupported admission must fail");
    assert!(
        format!("{error:#}")
            .contains("unsupported authored discovery_rules[].admission \"manifest_declared\""),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn rejects_manifest_version_tag_mismatch() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v2", manifest_contents())?;
    let error = load_repository(&test_dir.path).expect_err("version mismatch must fail");
    assert!(
        error
            .to_string()
            .contains("manifest_version 1 does not match version tag v2"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn checked_in_manifest_trees_pass_repository_validation() -> Result<()> {
    for profile_root in ["manifests/mainnet", "manifests/sepolia"] {
        let repository = load_repository(checked_in_manifest_root(profile_root))?;
        assert_eq!(
            repository.summary().status,
            ManifestLoadStatus::Loaded,
            "checked-in {profile_root} manifest tree must load"
        );
    }
    Ok(())
}

#[test]
fn sepolia_migration_family_has_the_ratified_launch_bounded_inputs() -> Result<()> {
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    let migration = repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "ens_v2_migration_l1")
        .expect("Sepolia migration family");
    let roles = migration
        .manifest
        .contracts
        .iter()
        .map(|contract| (contract.role.as_str(), contract.start_block))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(roles.len(), 9);
    assert_eq!(roles["unlocked_migration_controller"], Some(11_163_401));
    assert_eq!(roles["locked_migration_controller"], Some(11_163_413));
    assert_eq!(roles["graveyard"], Some(11_163_400));
    assert_eq!(roles["ens_v1_renewal_bridge"], Some(11_163_404));
    assert_eq!(roles["verifiable_factory"], Some(11_163_324));
    assert_eq!(roles["batch_registrar"], Some(11_163_411));
    assert_eq!(roles["migration_helper"], Some(11_163_415));
    assert_eq!(roles["wrapper_registry_implementation"], Some(11_163_410));
    assert_eq!(roles["ens_v1_base_registrar"], Some(11_163_400));
    assert_eq!(
        migration.manifest.correlation_addresses["ens_v1_name_wrapper"],
        "0x0635513f179d50a207757e05759cbd106d7dfce8"
    );
    assert!(migration.manifest.capability_flags.is_empty());
    assert!(migration.manifest.discovery_rules.is_empty());
    Ok(())
}

/// The Sepolia profile admits the ENSv1 registry and wrapper families the ENSv2 migration family
/// bridges from. The registrar family is deliberately absent: the migration family already owns
/// that address's attribution through its own `ens_v1_base_registrar` contract role, so admitting
/// it here fails preimage-attribution validation. That is tracked separately, and this test pins
/// the gap so it cannot be closed by accident.
#[test]
fn sepolia_profile_admits_the_ens_v1_registry_and_wrapper_families() -> Result<()> {
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    let family = |name: &str| {
        repository
            .manifests()
            .iter()
            .find(|loaded| loaded.manifest.source_family == name)
            .map(|loaded| &loaded.manifest)
    };

    let registry = family("ens_v1_registry_l1").expect("Sepolia ENSv1 registry family");
    assert_eq!(registry.chain, "ethereum-sepolia");
    assert!(registry.rollout_status.is_active());
    let registry_roles = registry
        .contracts
        .iter()
        .map(|contract| {
            (
                contract.role.as_str(),
                (normalize_address(&contract.address), contract.start_block),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(registry_roles.len(), 2);
    assert_eq!(
        registry_roles["registry"],
        (
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
            Some(3_702_728)
        )
    );
    assert_eq!(
        registry_roles["registry_old"],
        (
            "0x94f523b8261b815b87effcf4d18e6abef18d6e4b".to_owned(),
            Some(3_702_721)
        )
    );

    let wrapper = family("ens_v1_wrapper_l1").expect("Sepolia ENSv1 wrapper family");
    assert_eq!(wrapper.chain, "ethereum-sepolia");
    assert!(wrapper.rollout_status.is_active());
    let wrapper_roles = wrapper
        .contracts
        .iter()
        .map(|contract| {
            (
                contract.role.as_str(),
                (normalize_address(&contract.address), contract.start_block),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(wrapper_roles.len(), 1);
    assert_eq!(
        wrapper_roles["name_wrapper"],
        (
            "0x0635513f179d50a207757e05759cbd106d7dfce8".to_owned(),
            Some(3_790_153)
        )
    );

    // The admitted wrapper is the contract the migration family names as its correlation address.
    // A child's ENSv1 cleanup is only observable because both agree on this address.
    let migration = family("ens_v2_migration_l1").expect("Sepolia migration family");
    assert_eq!(
        normalize_address(&migration.correlation_addresses["ens_v1_name_wrapper"]),
        wrapper_roles["name_wrapper"].0,
    );

    // Both cleanup branches a migrated child can take must be ingestible: the wrapper token parked
    // in the Graveyard, and the node unwrapped into it.
    let wrapper_events = wrapper
        .abi
        .events
        .iter()
        .map(|event| event.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for required in ["TransferSingle", "TransferBatch", "NameUnwrapped"] {
        assert!(
            wrapper_events.contains(required),
            "wrapper family must admit {required}"
        );
    }

    assert!(
        family("ens_v1_registrar_l1").is_none(),
        "Sepolia ENSv1 registrar admission collides with the migration family's \
         ens_v1_base_registrar declaration and is tracked separately"
    );
    Ok(())
}

#[test]
fn normalize_address_preserves_legacy_fallbacks() {
    assert_eq!(
        normalize_address("0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"),
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    assert_eq!(normalize_address("NOT-A-HEX-ADDRESS"), "not-a-hex-address");
    assert_eq!(normalize_address("0xABC"), "0xabc");
}

#[test]
fn invalid_root_is_reported_without_io() -> Result<()> {
    let test_dir = TestDir::new()?;
    let path = test_dir.path.join("not-a-directory");
    fs::write(&path, "not a manifest directory")?;
    let repository = load_repository(&path)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::InvalidRoot);
    Ok(())
}
