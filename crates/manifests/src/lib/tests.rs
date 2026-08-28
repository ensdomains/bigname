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

fn migration_wrapper_manifest_pair() -> (String, String) {
    let wrapper = manifest_contents()
        .replacen(
            "source_family = \"ens_v2_registry_l1\"",
            "source_family = \"ens_v1_wrapper_l1\"",
            1,
        )
        .replacen("role = \"registry\"", "role = \"name_wrapper\"", 1)
        .replacen(
            "emitter_roles = [\"registry\"]",
            "emitter_roles = [\"name_wrapper\"]",
            1,
        );
    let migration = manifest_contents()
        .replacen(
            "source_family = \"ens_v2_registry_l1\"",
            "source_family = \"ens_v2_migration_l1\"",
            1,
        )
        .replacen(
            "address = \"0x0000000000000000000000000000000000000001\"",
            "address = \"0x0000000000000000000000000000000000000002\"",
            1,
        )
        .replacen(
            "address = \"0x00000000000000000000000000000000000000AA\"",
            "address = \"0x00000000000000000000000000000000000000AB\"",
            1,
        )
        .replacen(
            "[capability_flags]",
            "[correlation_addresses]\nens_v1_name_wrapper = \"0x00000000000000000000000000000000000000AA\"\nens_v1_base_registrar = \"0x00000000000000000000000000000000000000EE\"\n\n[capability_flags]",
            1,
        );

    (wrapper, migration)
}

#[test]
fn repository_loader_rejects_mismatched_migration_name_wrapper_correlation() -> Result<()> {
    let test_dir = TestDir::new()?;
    let (wrapper, migration) = migration_wrapper_manifest_pair();
    test_dir.write_manifest("ens", "ens_v1_wrapper_l1", "v1", &wrapper)?;
    test_dir.write_manifest("ens", "ens_v2_migration_l1", "v1", &migration)?;
    load_repository(&test_dir.path).context("matching wrapper correlation must load")?;

    let mismatch = migration.replacen(
        "ens_v1_name_wrapper = \"0x00000000000000000000000000000000000000AA\"",
        "ens_v1_name_wrapper = \"0x00000000000000000000000000000000000000BB\"",
        1,
    );
    test_dir.write_manifest("ens", "ens_v2_migration_l1", "v1", &mismatch)?;
    let error = load_repository(&test_dir.path)
        .expect_err("mismatched migration-to-wrapper correlation must fail manifest load");
    let message = error.to_string();
    assert!(
        message.contains("ens_v1_name_wrapper")
            && message.contains("ens_v2_migration_l1")
            && message.contains("ens_v1_wrapper_l1")
            && message.contains("0x00000000000000000000000000000000000000bb")
            && message.contains("0x00000000000000000000000000000000000000aa"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn repository_loader_requires_both_active_migration_correlation_keys() -> Result<()> {
    for (key, declaration) in [
        (
            "ens_v1_name_wrapper",
            "ens_v1_name_wrapper = \"0x00000000000000000000000000000000000000AA\"\n",
        ),
        (
            "ens_v1_base_registrar",
            "ens_v1_base_registrar = \"0x00000000000000000000000000000000000000EE\"\n",
        ),
    ] {
        let test_dir = TestDir::new()?;
        let (_, migration) = migration_wrapper_manifest_pair();
        let missing_key = migration.replacen(declaration, "", 1);
        test_dir.write_manifest("ens", "ens_v2_migration_l1", "v1", &missing_key)?;

        let error = load_repository(&test_dir.path).expect_err(
            "every active ENSv1→ENSv2 migration family requires both runtime correlation keys",
        );
        let message = error.to_string();
        assert!(
            message.contains(key) && message.contains("ens_v2_migration_l1"),
            "unexpected error: {error:#}"
        );
    }
    Ok(())
}

#[test]
fn repository_loader_compares_wrapper_correlation_as_an_evm_address() -> Result<()> {
    let test_dir = TestDir::new()?;
    let (wrapper, migration) = migration_wrapper_manifest_pair();
    let unprefixed = migration.replacen(
        "ens_v1_name_wrapper = \"0x00000000000000000000000000000000000000AA\"",
        "ens_v1_name_wrapper = \"00000000000000000000000000000000000000AA\"",
        1,
    );
    test_dir.write_manifest("ens", "ens_v1_wrapper_l1", "v1", &wrapper)?;
    test_dir.write_manifest("ens", "ens_v2_migration_l1", "v1", &unprefixed)?;

    load_repository(&test_dir.path)
        .context("equivalent prefixed and unprefixed EVM addresses must compare equal")?;
    Ok(())
}

#[test]
fn repository_loader_requires_coadmitted_name_wrapper_role() -> Result<()> {
    let test_dir = TestDir::new()?;
    let (wrapper, migration) = migration_wrapper_manifest_pair();
    let missing_role = wrapper
        .replacen("role = \"name_wrapper\"", "role = \"wrapper_contract\"", 1)
        .replacen(
            "emitter_roles = [\"name_wrapper\"]",
            "emitter_roles = [\"wrapper_contract\"]",
            1,
        );
    test_dir.write_manifest("ens", "ens_v1_wrapper_l1", "v1", &missing_role)?;
    test_dir.write_manifest("ens", "ens_v2_migration_l1", "v1", &migration)?;

    let error = load_repository(&test_dir.path)
        .expect_err("an active wrapper family requires its name_wrapper role");
    let message = error.to_string();
    assert!(
        message.contains("ens_v1_wrapper_l1")
            && message.contains("ens_v2_migration_l1")
            && message.contains("contract role name_wrapper"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn repository_loader_rejects_mismatched_migration_base_registrar_correlation() -> Result<()> {
    let test_dir = TestDir::new()?;
    let (_, migration) = migration_wrapper_manifest_pair();
    let registrar = manifest_contents()
        .replacen(
            "source_family = \"ens_v2_registry_l1\"",
            "source_family = \"ens_v1_registrar_l1\"",
            1,
        )
        .replacen(
            "address = \"0x0000000000000000000000000000000000000001\"",
            "address = \"0x0000000000000000000000000000000000000003\"",
            1,
        )
        .replacen(
            "address = \"0x00000000000000000000000000000000000000AA\"",
            "address = \"0x00000000000000000000000000000000000000EE\"",
            1,
        )
        .replacen("role = \"registry\"", "role = \"registrar\"", 1)
        .replacen(
            "emitter_roles = [\"registry\"]",
            "emitter_roles = [\"registrar\"]",
            1,
        );
    test_dir.write_manifest("ens", "ens_v1_registrar_l1", "v1", &registrar)?;
    test_dir.write_manifest("ens", "ens_v2_migration_l1", "v1", &migration)?;
    load_repository(&test_dir.path).context("matching registrar correlation must load")?;

    let mismatch = migration.replacen(
        "ens_v1_base_registrar = \"0x00000000000000000000000000000000000000EE\"",
        "ens_v1_base_registrar = \"0x00000000000000000000000000000000000000FF\"",
        1,
    );
    test_dir.write_manifest("ens", "ens_v2_migration_l1", "v1", &mismatch)?;
    let error = load_repository(&test_dir.path)
        .expect_err("mismatched migration-to-registrar correlation must fail manifest load");
    let message = error.to_string();
    assert!(
        message.contains("ens_v1_base_registrar")
            && message.contains("ens_v2_migration_l1")
            && message.contains("ens_v1_registrar_l1")
            && message.contains("0x00000000000000000000000000000000000000ff")
            && message.contains("0x00000000000000000000000000000000000000ee"),
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
fn pre_502_dual_registrar_contract_roles_still_fail_attribution_guard() -> Result<()> {
    let test_dir = TestDir::new()?;
    let registrar = manifest_contents()
        .replacen(
            "source_family = \"ens_v2_registry_l1\"",
            "source_family = \"ens_v1_registrar_l1\"",
            1,
        )
        .replacen("role = \"registry\"", "role = \"registrar\"", 1)
        .replacen(
            "emitter_roles = [\"registry\"]",
            "emitter_roles = [\"registrar\"]",
            1,
        );
    let migration = manifest_contents()
        .replacen(
            "source_family = \"ens_v2_registry_l1\"",
            "source_family = \"ens_v2_migration_l1\"",
            1,
        )
        .replacen(
            "[capability_flags]",
            "[correlation_addresses]\nens_v1_name_wrapper = \"0x00000000000000000000000000000000000000BB\"\nens_v1_base_registrar = \"0x00000000000000000000000000000000000000AA\"\n\n[capability_flags]",
            1,
        );
    test_dir.write_manifest("ens", "ens_v1_registrar_l1", "v1", &registrar)?;
    test_dir.write_manifest("ens", "ens_v2_migration_l1", "v1", &migration)?;

    let error = load_repository(&test_dir.path)
        .expect_err("dual registrar/migration contract-role attribution must fail");
    let message = error.to_string();
    assert!(
        message.contains("could assign one block-derived preimage log to two sources")
            && message.contains("ens_v1_registrar_l1")
            && message.contains("ens_v2_migration_l1"),
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
fn checked_in_resolver_read_features_are_generation_scoped() -> Result<()> {
    let repository = load_repository(checked_in_manifest_root("manifests/mainnet"))?;
    let ens = repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "ens_v1_resolver_l1")
        .expect("ENSv1 resolver manifest");
    let flagged = ens
        .manifest
        .contracts
        .iter()
        .filter(|contract| !contract.read_features.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(flagged.len(), 1);
    assert_eq!(flagged[0].role, "public_resolver");
    assert_eq!(
        flagged[0].read_features,
        vec![ResolverReadFeature::Ensip19DefaultAddress]
    );

    let basenames = repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "basenames_base_resolver")
        .expect("Basenames resolver manifest");
    assert!(
        basenames.manifest.contracts[0].read_features.is_empty(),
        "the admitted legacy Basenames resolver must not authorize ENSIP-19 fallback"
    );
    Ok(())
}

#[test]
fn checked_in_archived_sepolia_permissioned_resolver_has_ensip19_fallback() -> Result<()> {
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    let resolver = repository
        .manifests()
        .iter()
        .find(|loaded| {
            loaded.manifest.source_family == "ens_v2_resolver_l1"
                && loaded.manifest.deployment_epoch == "ens_v2_sepolia_post_audit"
        })
        .expect("active archived-Sepolia ENSv2 resolver manifest");
    assert_eq!(
        resolver.manifest.resolver_implementations[0].read_features,
        vec![ResolverReadFeature::Ensip19DefaultAddress]
    );
    Ok(())
}

#[test]
fn repository_rejects_invalid_resolver_read_feature_declarations() -> Result<()> {
    let direct_resolver = manifest_contents()
        .replace("role = \"registry\"", "role = \"resolver\"")
        .replace("proxy_kind = \"erc1967\"", "proxy_kind = \"none\"")
        .replace(
            "start_block = 23456",
            "read_features = [\"ensip19_default_address\", \"ensip19_default_address\"]\nstart_block = 23456",
        );
    let error = load_one(&direct_resolver).expect_err("duplicate read features must fail");
    assert!(format!("{error:#}").contains("duplicates read feature"));

    let proxy = direct_resolver
        .replace("proxy_kind = \"none\"", "proxy_kind = \"erc1967\"")
        .replace(
            "[\"ensip19_default_address\", \"ensip19_default_address\"]",
            "[\"ensip19_default_address\"]",
        );
    let error = load_one(&proxy).expect_err("proxy-level read features must fail");
    assert!(format!("{error:#}").contains("resolver_implementations"));

    let unknown = direct_resolver.replace(
        "[\"ensip19_default_address\", \"ensip19_default_address\"]",
        "[\"unknown_read_feature\"]",
    );
    let error = load_one(&unknown).expect_err("unknown read features must fail");
    assert!(format!("{error:#}").contains("unknown variant"));

    let mixed_implementation_family = direct_resolver.replace(
        "[\"ensip19_default_address\", \"ensip19_default_address\"]",
        "[\"ensip19_default_address\"]",
    );
    let error = load_one(&mixed_implementation_family)
        .expect_err("implementation families must reject contract-level read features");
    assert!(format!("{error:#}").contains("implementation family"));

    let conflicting_same_address = manifest_contents()
        .replace(
            "resolver_implementations = [\n  { role = \"permissioned_resolver\", address = \"0x00000000000000000000000000000000000000CC\" },\n]\n",
            "",
        )
        .replace("role = \"registry\"", "role = \"resolver\"")
        .replace("emitter_roles = [\"registry\"]", "emitter_roles = [\"resolver\"]")
        .replace("from_role = \"registry\"", "from_role = \"resolver\"")
        .replace("proxy_kind = \"erc1967\"", "proxy_kind = \"none\"")
        .replace(
            "start_block = 23456",
            "read_features = [\"ensip19_default_address\"]\nstart_block = 23456\n\n[[contracts]]\nrole = \"a_registry\"\naddress = \"0x00000000000000000000000000000000000000aa\"\nproxy_kind = \"none\"\nstart_block = 23456",
        );
    let error = load_one(&conflicting_same_address)
        .expect_err("same-address direct resolver declarations must agree on read features");
    assert!(
        format!("{error:#}").contains("same address"),
        "unexpected error: {error:#}"
    );
    Ok(())
}

#[test]
fn sepolia_ensv1_to_ensv2_migration_family_has_the_ratified_launch_bounded_inputs() -> Result<()> {
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    let migration = repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "ens_v2_migration_l1")
        .expect("Sepolia ENSv1→ENSv2 migration family");
    let roles = migration
        .manifest
        .contracts
        .iter()
        .map(|contract| (contract.role.as_str(), contract.start_block))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(roles.len(), 8);
    assert_eq!(roles["unlocked_migration_controller"], Some(11_163_401));
    assert_eq!(roles["locked_migration_controller"], Some(11_163_413));
    assert_eq!(roles["graveyard"], Some(11_163_400));
    assert_eq!(roles["ens_v1_renewal_bridge"], Some(11_163_404));
    assert_eq!(roles["verifiable_factory"], Some(11_163_324));
    assert_eq!(roles["batch_registrar"], Some(11_163_411));
    assert_eq!(roles["migration_helper"], Some(11_163_415));
    assert_eq!(roles["wrapper_registry_implementation"], Some(11_163_410));
    assert_eq!(
        migration.manifest.correlation_addresses["ens_v1_name_wrapper"],
        "0x0635513f179d50a207757e05759cbd106d7dfce8"
    );
    assert_eq!(
        migration.manifest.correlation_addresses["ens_v1_base_registrar"],
        "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85"
    );
    assert!(migration.manifest.capability_flags.is_empty());
    assert!(migration.manifest.discovery_rules.is_empty());
    Ok(())
}

#[test]
fn mainnet_registrar_family_pins_the_base_registrar_event_surface() -> Result<()> {
    let repository = load_repository(checked_in_manifest_root("manifests/mainnet"))?;
    let registrar = repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "ens_v1_registrar_l1")
        .map(|loaded| &loaded.manifest)
        .expect("Mainnet ENSv1 registrar family");
    let required = [
        (
            "ControllerAdded",
            "event ControllerAdded(address indexed controller)",
            &["PermissionChanged"][..],
        ),
        (
            "ControllerRemoved",
            "event ControllerRemoved(address indexed controller)",
            &["PermissionChanged"][..],
        ),
        (
            "NameRegistered",
            "event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires)",
            &["RegistrationReleased"][..],
        ),
        (
            "NameRenewed",
            "event NameRenewed(uint256 indexed id, uint256 expires)",
            &["RegistrationRenewed", "ExpiryChanged"][..],
        ),
    ];

    for (name, fragment, normalized_events) in required {
        let event = registrar
            .abi
            .events
            .iter()
            .find(|event| event.name == name && event.fragment == fragment)
            .unwrap_or_else(|| panic!("missing BaseRegistrar event {fragment}"));
        assert_eq!(event.emitter_roles, ["registrar"]);
        assert_eq!(event.normalized_events, normalized_events);
    }
    Ok(())
}

/// Sepolia admits the ENSv1 registry, registrar, wrapper, and resolver manifests. The migration
/// family consumes only its declared cross-family correlation inputs; resolver events remain
/// owned by `ens_v1_resolver_l1`. BaseRegistrar raw logs belong only to `ens_v1_registrar_l1`, and
/// the `ens_v2_migration_l1` manifest keeps that address as correlation metadata.
#[test]
fn sepolia_manifests_admit_all_four_ens_v1_families() -> Result<()> {
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

    // The admitted wrapper is the contract `ens_v2_migration_l1` names as its correlation
    // address. A child's ENSv1 cleanup is only observable because both agree on this address.
    let migration = family("ens_v2_migration_l1").expect("Sepolia ENSv1→ENSv2 migration manifest");
    assert_eq!(
        normalize_address(&migration.correlation_addresses["ens_v1_name_wrapper"]),
        wrapper_roles["name_wrapper"].0,
    );

    let registrar = family("ens_v1_registrar_l1").expect("Sepolia ENSv1 registrar family");
    assert_eq!(
        registrar.contracts.len(),
        1,
        "#515 option (b) admits only BaseRegistrar; Sepolia registrar controllers stay deferred"
    );
    assert_eq!(registrar.contracts[0].role, "registrar");
    assert_eq!(registrar.contracts[0].start_block, Some(3_702_731));
    assert_eq!(
        normalize_address(&registrar.contracts[0].address),
        "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85"
    );
    assert_eq!(
        normalize_address(&migration.correlation_addresses["ens_v1_base_registrar"]),
        normalize_address(&registrar.contracts[0].address),
    );

    let resolver = family("ens_v1_resolver_l1").expect("Sepolia ENSv1 resolver family");
    assert_eq!(resolver.chain, "ethereum-sepolia");
    assert!(resolver.rollout_status.is_active());
    assert_eq!(resolver.deployment_epoch, "ens_v1");
    assert_eq!(resolver.normalizer_version, "ensip15@ens-normalize-0.1.1");
    assert!(resolver.roots.is_empty());
    assert!(resolver.discovery_rules.is_empty());
    assert!(resolver.capability_flags.is_empty());
    let resolver_contracts = resolver
        .contracts
        .iter()
        .map(|contract| {
            (
                contract.role.as_str(),
                (
                    normalize_address(&contract.address),
                    contract.proxy_kind.as_str(),
                    contract.start_block,
                ),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        resolver_contracts,
        std::collections::BTreeMap::from([
            (
                "public_resolver",
                (
                    "0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5".to_owned(),
                    "none",
                    Some(8_580_001),
                ),
            ),
            (
                "public_resolver_0ceec52",
                (
                    "0x0ceec524b2807841739d3b5e161f5bf1430ffa48".to_owned(),
                    "none",
                    Some(3_790_166),
                ),
            ),
            (
                "public_resolver_8948458",
                (
                    "0x8948458626811dd0c23eb25cc74291247077cc51".to_owned(),
                    "none",
                    Some(0),
                ),
            ),
            (
                "public_resolver_8fade66",
                (
                    "0x8fade66b79cc9f707ab26799354482eb93a5b7dd".to_owned(),
                    "none",
                    Some(0),
                ),
            ),
        ])
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
            "wrapper manifest must admit {required}"
        );
    }

    Ok(())
}

/// The declared surface of the four Sepolia ENSv1 manifests, pinned across the fields that
/// decide what gets ingested. The admission test above covers contracts, addresses, chain, and
/// rollout status; this one covers the event surface, capability flags, deployment epoch, and
/// roots, so that deleting an event block, widening a fragment type, dropping a normalized event,
/// or downgrading a capability flag fails here rather than silently changing what a live chain
/// produces.
#[test]
fn sepolia_ens_v1_families_pin_their_declared_surface() -> Result<()> {
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    let family = |name: &str| {
        let versions = repository
            .manifests()
            .iter()
            .filter(|loaded| loaded.manifest.source_family == name)
            .collect::<Vec<_>>();
        // Pinning one version is only meaningful while it is the only one; a later v2 must land
        // here deliberately rather than leave this test quietly pinning the superseded file.
        assert_eq!(versions.len(), 1, "Sepolia {name} family versions");
        &versions[0].manifest
    };
    let event_surface = |manifest: &SourceManifest| {
        let mut events = manifest
            .abi
            .events
            .iter()
            .map(|event| {
                (
                    event.name.clone(),
                    event.fragment.clone(),
                    event.emitter_roles.join(","),
                    event.normalized_events.join(","),
                )
            })
            .collect::<Vec<_>>();
        events.sort();
        events
    };
    let roots = |manifest: &SourceManifest| {
        manifest
            .roots
            .iter()
            .map(|root| {
                (
                    root.name.clone(),
                    normalize_address(&root.address),
                    root.start_block,
                )
            })
            .collect::<Vec<_>>()
    };

    let registry = family("ens_v1_registry_l1");
    assert_eq!(registry.deployment_epoch, "ens_v1");
    assert_eq!(
        roots(registry),
        vec![(
            "ENSRegistry".to_owned(),
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e".to_owned(),
            Some(3_702_728)
        )]
    );
    assert_eq!(registry.capability_flags.len(), 1);
    assert_eq!(
        registry.capability_flags["declared_children"].status,
        CapabilitySupportStatus::Supported
    );
    assert!(registry.discovery_rules.is_empty());
    let registry_emitters = "registry,registry_old";
    assert_eq!(
        event_surface(registry),
        vec![
            (
                "NewOwner".to_owned(),
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)"
                    .to_owned(),
                registry_emitters.to_owned(),
                "SubregistryChanged,AuthorityTransferred,PermissionChanged,SurfaceUnbound,\
                 SurfaceBound,AuthorityEpochChanged,ResolverChanged"
                    .to_owned(),
            ),
            (
                "NewResolver".to_owned(),
                "event NewResolver(bytes32 indexed node, address resolver)".to_owned(),
                registry_emitters.to_owned(),
                "ResolverChanged,PermissionChanged".to_owned(),
            ),
            (
                "NewTTL".to_owned(),
                "event NewTTL(bytes32 indexed node, uint64 ttl)".to_owned(),
                registry_emitters.to_owned(),
                String::new(),
            ),
            (
                "Transfer".to_owned(),
                "event Transfer(bytes32 indexed node, address owner)".to_owned(),
                registry_emitters.to_owned(),
                "AuthorityTransferred,PermissionChanged,SurfaceUnbound,SurfaceBound,\
                 AuthorityEpochChanged,ResolverChanged"
                    .to_owned(),
            ),
        ]
    );

    let wrapper = family("ens_v1_wrapper_l1");
    assert_eq!(wrapper.deployment_epoch, "ens_v1");
    assert!(wrapper.roots.is_empty());
    assert!(wrapper.capability_flags.is_empty());
    assert!(wrapper.discovery_rules.is_empty());
    let token_control = "TokenControlTransferred,PermissionChanged";
    assert_eq!(
        event_surface(wrapper),
        vec![
            (
                "ExpiryExtended".to_owned(),
                "event ExpiryExtended(bytes32 indexed node, uint64 expiry)".to_owned(),
                "name_wrapper".to_owned(),
                "ExpiryChanged".to_owned(),
            ),
            (
                "FusesSet".to_owned(),
                "event FusesSet(bytes32 indexed node, uint32 fuses)".to_owned(),
                "name_wrapper".to_owned(),
                "PermissionScopeChanged".to_owned(),
            ),
            (
                "NameUnwrapped".to_owned(),
                "event NameUnwrapped(bytes32 indexed node, address owner)".to_owned(),
                "name_wrapper".to_owned(),
                "SurfaceUnbound,SurfaceBound,AuthorityEpochChanged,ResolverChanged".to_owned(),
            ),
            (
                "NameWrapped".to_owned(),
                "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, \
                 uint64 expiry)"
                    .to_owned(),
                "name_wrapper".to_owned(),
                "TokenControlTransferred,ExpiryChanged,PermissionScopeChanged,SurfaceUnbound,\
                 SurfaceBound,AuthorityEpochChanged,ResolverChanged,PreimageObserved"
                    .to_owned(),
            ),
            (
                "TransferBatch".to_owned(),
                "event TransferBatch(address indexed operator, address indexed from, address \
                 indexed to, uint256[] ids, uint256[] values)"
                    .to_owned(),
                "name_wrapper".to_owned(),
                token_control.to_owned(),
            ),
            (
                "TransferSingle".to_owned(),
                "event TransferSingle(address indexed operator, address indexed from, address \
                 indexed to, uint256 id, uint256 value)"
                    .to_owned(),
                "name_wrapper".to_owned(),
                token_control.to_owned(),
            ),
        ]
    );

    let registrar = family("ens_v1_registrar_l1");
    assert_eq!(registrar.deployment_epoch, "ens_v1");
    assert_eq!(
        roots(registrar),
        vec![(
            "ETHRegistrar".to_owned(),
            "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85".to_owned(),
            Some(3_702_731)
        )]
    );
    assert_eq!(
        ["exact_name_profile", "name_history"].map(|flag| registrar.capability_flags[flag].status),
        [CapabilitySupportStatus::Shadow; 2]
    );
    let registrar_surface = event_surface(registrar)
        .into_iter()
        .map(|(name, fragment, roles, events)| format!("{name}|{fragment}|{roles}|{events}"))
        .collect::<Vec<_>>();
    assert_eq!(
        registrar_surface,
        [
            "ControllerAdded|event ControllerAdded(address indexed controller)|registrar|PermissionChanged",
            "ControllerRemoved|event ControllerRemoved(address indexed controller)|registrar|PermissionChanged",
            "NameRegistered|event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires)|registrar|RegistrationReleased",
            "NameRenewed|event NameRenewed(uint256 indexed id, uint256 expires)|registrar|RegistrationRenewed,ExpiryChanged",
            "Transfer|event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)|registrar|TokenControlTransferred,PermissionChanged,SurfaceUnbound,SurfaceBound,AuthorityEpochChanged,ResolverChanged",
        ]
    );

    let resolver = family("ens_v1_resolver_l1");
    let mainnet_repository = load_repository(checked_in_manifest_root("manifests/mainnet"))?;
    let mainnet_resolver = mainnet_repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "ens_v1_resolver_l1")
        .map(|loaded| &loaded.manifest)
        .expect("Mainnet ENSv1 resolver family");
    assert_eq!(event_surface(resolver), event_surface(mainnet_resolver));
    assert!(
        resolver
            .abi
            .events
            .iter()
            .all(|event| { event.emitter_roles.is_empty() && event.status.is_none() })
    );

    let v1_addresses = resolver
        .contracts
        .iter()
        .map(|contract| normalize_address(&contract.address))
        .collect::<std::collections::BTreeSet<_>>();
    let v2 = repository
        .manifests()
        .iter()
        .filter(|loaded| loaded.manifest.source_family == "ens_v2_resolver_l1")
        .max_by_key(|loaded| loaded.manifest.manifest_version)
        .map(|loaded| &loaded.manifest)
        .expect("selected Sepolia ENSv2 resolver family");
    let v2_addresses = v2
        .contracts
        .iter()
        .map(|contract| normalize_address(&contract.address))
        .chain(v2.roots.iter().map(|root| normalize_address(&root.address)))
        .chain(
            v2.resolver_implementations
                .iter()
                .map(|implementation| normalize_address(&implementation.address)),
        )
        .collect::<std::collections::BTreeSet<_>>();
    assert!(v1_addresses.is_disjoint(&v2_addresses));
    assert_eq!(
        v2.resolver_implementations
            .iter()
            .map(|implementation| {
                (
                    implementation.role.as_str(),
                    normalize_address(&implementation.address),
                )
            })
            .collect::<Vec<_>>(),
        [(
            "permissioned_resolver",
            "0x7e4b2d59938930168024201752ee5503df402303".to_owned(),
        )]
    );
    assert!(v2.contracts.is_empty());
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
