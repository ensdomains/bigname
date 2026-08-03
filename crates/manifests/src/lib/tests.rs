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
fn repository_loader_rejects_invalid_single_manifest_declarations() -> Result<()> {
    let base = manifest_contents();
    let duplicate_role = format!(
        "{base}\n[[contracts]]\nrole = \"registry\"\naddress = \"0x00000000000000000000000000000000000000BB\"\nproxy_kind = \"none\"\n"
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
