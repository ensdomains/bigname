//! Three layers protect the shared Alloy decode/codegen path: lockfile source identity, normalized
//! curated-package-identity features, and the semantic fixtures
//! `schema_v2_permutation_lane::generated_event_fragments_match_the_checked_in_manifest_abi`,
//! `registration_burst::registration_with_records_reverse_and_referrer_derives_single_burst`, and
//! `basenames::basenames_declared_state_matrix_end_to_end`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    process::Command,
};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PackageKey {
    name: String,
    version: String,
    source: Option<String>,
    checksum: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PackageIdentity {
    version: String,
    source: Option<String>,
    checksum: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DependencySlot {
    parent: PackageKey,
    dependency: String,
}

struct LockedPackage {
    key: PackageKey,
    dependencies: Vec<String>,
}

const CODEGEN_AND_DECODE_PACKAGES: [&str; 8] = [
    "alloy-json-abi",
    "alloy-primitives",
    "alloy-sol-macro",
    "alloy-sol-macro-expander",
    "alloy-sol-macro-input",
    "alloy-sol-type-parser",
    "alloy-sol-types",
    "syn-solidity",
];

const LOCKFILE_REGISTRY: &str = "registry+https://github.com/rust-lang/crates.io-index";

// Reviewed feature-activation edges present in only one workspace at PR #373 round 5; the lists
// pin logical slots and the companion fingerprints pin every parent and target source identity.
const REVIEWED_ROOT_ONLY_DEPENDENCY_SLOTS: [&str; 42] = [
    "alloy-primitives 1.5.7 (registry+https://github.com/rust-lang/crates.io-index) -> getrandom",
    "alloy-rlp 0.3.15 (registry+https://github.com/rust-lang/crates.io-index) -> alloy-rlp-derive",
    "alloy-rlp-derive 0.3.15 (registry+https://github.com/rust-lang/crates.io-index) -> proc-macro2",
    "alloy-rlp-derive 0.3.15 (registry+https://github.com/rust-lang/crates.io-index) -> quote",
    "alloy-rlp-derive 0.3.15 (registry+https://github.com/rust-lang/crates.io-index) -> syn",
    "ark-serialize 0.5.0 (registry+https://github.com/rust-lang/crates.io-index) -> ark-serialize-derive",
    "ark-serialize-derive 0.5.0 (registry+https://github.com/rust-lang/crates.io-index) -> proc-macro2",
    "ark-serialize-derive 0.5.0 (registry+https://github.com/rust-lang/crates.io-index) -> quote",
    "ark-serialize-derive 0.5.0 (registry+https://github.com/rust-lang/crates.io-index) -> syn",
    "arrayvec 0.7.6 (registry+https://github.com/rust-lang/crates.io-index) -> serde",
    "bitvec 1.0.1 (registry+https://github.com/rust-lang/crates.io-index) -> serde",
    "cc 1.2.60 (registry+https://github.com/rust-lang/crates.io-index) -> jobserver",
    "cc 1.2.60 (registry+https://github.com/rust-lang/crates.io-index) -> libc",
    "crypto-common 0.1.7 (registry+https://github.com/rust-lang/crates.io-index) -> rand_core",
    "ecdsa 0.16.9 (registry+https://github.com/rust-lang/crates.io-index) -> serdect",
    "elliptic-curve 0.13.8 (registry+https://github.com/rust-lang/crates.io-index) -> hkdf",
    "elliptic-curve 0.13.8 (registry+https://github.com/rust-lang/crates.io-index) -> pem-rfc7468",
    "elliptic-curve 0.13.8 (registry+https://github.com/rust-lang/crates.io-index) -> serdect",
    "futures-channel 0.3.32 (registry+https://github.com/rust-lang/crates.io-index) -> futures-core",
    "futures-channel 0.3.32 (registry+https://github.com/rust-lang/crates.io-index) -> futures-sink",
    "futures-util 0.3.32 (registry+https://github.com/rust-lang/crates.io-index) -> futures-channel",
    "getrandom 0.3.4 (registry+https://github.com/rust-lang/crates.io-index) -> js-sys",
    "getrandom 0.3.4 (registry+https://github.com/rust-lang/crates.io-index) -> wasm-bindgen",
    "hashbrown 0.16.1 (registry+https://github.com/rust-lang/crates.io-index) -> allocator-api2",
    "hashbrown 0.16.1 (registry+https://github.com/rust-lang/crates.io-index) -> equivalent",
    "hkdf 0.12.4 (registry+https://github.com/rust-lang/crates.io-index) -> hmac",
    "jobserver 0.1.34 (registry+https://github.com/rust-lang/crates.io-index) -> getrandom",
    "jobserver 0.1.34 (registry+https://github.com/rust-lang/crates.io-index) -> libc",
    "k256 0.13.4 (registry+https://github.com/rust-lang/crates.io-index) -> serdect",
    "k256 0.13.4 (registry+https://github.com/rust-lang/crates.io-index) -> signature",
    "once_cell 1.21.4 (registry+https://github.com/rust-lang/crates.io-index) -> critical-section",
    "once_cell 1.21.4 (registry+https://github.com/rust-lang/crates.io-index) -> portable-atomic",
    "parity-scale-codec 3.7.5 (registry+https://github.com/rust-lang/crates.io-index) -> bytes",
    "rand 0.8.5 (registry+https://github.com/rust-lang/crates.io-index) -> serde",
    "rapidhash 4.4.1 (registry+https://github.com/rust-lang/crates.io-index) -> rand",
    "rustc-hash 2.1.2 (registry+https://github.com/rust-lang/crates.io-index) -> rand",
    "sec1 0.7.3 (registry+https://github.com/rust-lang/crates.io-index) -> serdect",
    "semver 1.0.27 (registry+https://github.com/rust-lang/crates.io-index) -> serde",
    "semver 1.0.27 (registry+https://github.com/rust-lang/crates.io-index) -> serde_core",
    "serde_json 1.0.149 (registry+https://github.com/rust-lang/crates.io-index) -> indexmap",
    "serdect 0.2.0 (registry+https://github.com/rust-lang/crates.io-index) -> base16ct",
    "serdect 0.2.0 (registry+https://github.com/rust-lang/crates.io-index) -> serde",
];

const REVIEWED_E2E_ONLY_DEPENDENCY_SLOTS: [&str; 3] = [
    "getrandom 0.4.2 (registry+https://github.com/rust-lang/crates.io-index) -> js-sys",
    "getrandom 0.4.2 (registry+https://github.com/rust-lang/crates.io-index) -> rand_core",
    "getrandom 0.4.2 (registry+https://github.com/rust-lang/crates.io-index) -> wasm-bindgen",
];

const REVIEWED_ROOT_ONLY_IDENTITY_FINGERPRINT: &str = "fnv1a128:5331b9e577f86248cc237af8f416be2f";
const REVIEWED_E2E_ONLY_IDENTITY_FINGERPRINT: &str = "fnv1a128:1d703d88e39c19c82e51de14e7e16347";

#[test]
fn e2e_codegen_and_decode_lockfile_closure_matches_workspace() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workspace =
        locked_package_identities(&workspace_root.join("Cargo.lock"), "bigname-adapters");
    let e2e =
        locked_package_identities(&workspace_root.join("tests/e2e/Cargo.lock"), "bigname-e2e");
    let workspace_dependencies =
        locked_dependency_identities(&workspace_root.join("Cargo.lock"), "bigname-adapters");
    let e2e_dependencies =
        locked_dependency_identities(&workspace_root.join("tests/e2e/Cargo.lock"), "bigname-e2e");
    let workspace_features =
        curated_package_feature_union(&workspace_root, "bigname-adapters", None);
    let e2e_features = curated_package_feature_union(
        &workspace_root,
        "bigname-e2e",
        Some(Path::new("tests/e2e/Cargo.toml")),
    );

    for package in CODEGEN_AND_DECODE_PACKAGES {
        assert!(
            workspace.contains_key(package),
            "root workspace lockfile must contain curated package {package}"
        );
        assert!(
            e2e.contains_key(package),
            "e2e lockfile must contain curated package {package}"
        );
        assert!(
            feature_map_contains_package(&workspace_features, package),
            "cargo tree for the root workspace must expose curated package {package}"
        );
        assert!(
            feature_map_contains_package(&e2e_features, package),
            "cargo tree for the e2e workspace must expose curated package {package}"
        );
    }

    let mismatches =
        shared_dependency_identity_mismatches(&workspace_dependencies, &e2e_dependencies);
    assert!(
        mismatches.is_empty(),
        "e2e lockfile shared codegen/decode dependency closure must match the root workspace: {mismatches:#?}"
    );

    let feature_mismatches = curated_feature_union_mismatches(&workspace_features, &e2e_features);
    assert!(
        feature_mismatches.is_empty(),
        "e2e curated codegen/decode package feature unions must match the root workspace: {feature_mismatches:#?}"
    );

    let (root_only, e2e_only) =
        one_sided_dependency_slot_labels(&workspace_dependencies, &e2e_dependencies);
    assert_eq!(
        root_only,
        REVIEWED_ROOT_ONLY_DEPENDENCY_SLOTS
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "root-only codegen/decode dependency slots changed; review feature activation"
    );
    assert_eq!(
        e2e_only,
        REVIEWED_E2E_ONLY_DEPENDENCY_SLOTS
            .into_iter()
            .map(str::to_owned)
            .collect(),
        "e2e-only codegen/decode dependency slots changed; review feature activation"
    );
    assert_eq!(
        one_sided_dependency_identity_fingerprints(&workspace_dependencies, &e2e_dependencies),
        (
            REVIEWED_ROOT_ONLY_IDENTITY_FINGERPRINT.to_owned(),
            REVIEWED_E2E_ONLY_IDENTITY_FINGERPRINT.to_owned(),
        ),
        "one-sided codegen/decode dependency identity changed; review the reachable source"
    );

    for package in CODEGEN_AND_DECODE_PACKAGES {
        assert_eq!(
            e2e.get(package),
            workspace.get(package),
            "e2e lockfile package {package} must match the root workspace"
        );
    }
}

fn curated_package_feature_union(
    workspace_root: &Path,
    package: &str,
    manifest_path: Option<&Path>,
) -> BTreeMap<String, BTreeSet<String>> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut command = Command::new(cargo);
    command.current_dir(workspace_root).args([
        "tree", "--locked", "-p", package, "-e", "features", "--prefix", "none", "--format",
        "{p}|{f}",
    ]);
    if let Some(manifest_path) = manifest_path {
        command
            .arg("--manifest-path")
            .arg(workspace_root.join(manifest_path));
    }
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("failed to run cargo tree for {package}: {error}"));
    assert!(
        output.status.success(),
        "cargo tree for {package} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut features = BTreeMap::<String, BTreeSet<String>>::new();
    // Cargo may print the same package once per target/build unit. Unioning those occurrences
    // matches `cargo metadata`'s package-node feature normalization across the selected graph.
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some((package_identity, enabled)) = line.split_once('|') else {
            continue;
        };
        let package_identity = package_identity.trim_end_matches(" (*)");
        let Some(name) = package_identity_name(package_identity) else {
            continue;
        };
        if !CODEGEN_AND_DECODE_PACKAGES.contains(&name) {
            continue;
        }
        let package_features = features.entry(package_identity.to_owned()).or_default();
        package_features.extend(
            enabled
                .trim_end_matches(" (*)")
                .split(',')
                .filter(|feature| !feature.is_empty())
                .map(str::to_owned),
        );
    }
    features
}

fn package_identity_name(identity: &str) -> Option<&str> {
    identity.split_whitespace().next()
}

fn feature_map_contains_package(
    features: &BTreeMap<String, BTreeSet<String>>,
    package: &str,
) -> bool {
    features
        .keys()
        .any(|identity| package_identity_name(identity) == Some(package))
}

fn curated_feature_union_mismatches(
    workspace: &BTreeMap<String, BTreeSet<String>>,
    e2e: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    workspace
        .keys()
        .chain(e2e.keys())
        .filter(|identity| {
            package_identity_name(identity)
                .is_some_and(|name| CODEGEN_AND_DECODE_PACKAGES.contains(&name))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|identity| workspace.get(*identity) != e2e.get(*identity))
        .map(|identity| {
            format!(
                "{identity}: root={:?}, e2e={:?}",
                workspace.get(identity),
                e2e.get(identity),
            )
        })
        .collect()
}

fn one_sided_dependency_slot_labels(
    workspace: &BTreeMap<DependencySlot, Vec<PackageIdentity>>,
    e2e: &BTreeMap<DependencySlot, Vec<PackageIdentity>>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let root_only = workspace
        .keys()
        .filter(|slot| !e2e.contains_key(*slot))
        .map(dependency_slot_label)
        .collect();
    let e2e_only = e2e
        .keys()
        .filter(|slot| !workspace.contains_key(*slot))
        .map(dependency_slot_label)
        .collect();
    (root_only, e2e_only)
}

fn dependency_slot_label(slot: &DependencySlot) -> String {
    // The baseline names logical edges by parent version/source; checksum parity for shared
    // parents is enforced by the source-identity comparison above.
    format!(
        "{} {} ({}) -> {}",
        slot.parent.name,
        slot.parent.version,
        slot.parent.source.as_deref().unwrap_or("workspace"),
        slot.dependency,
    )
}

fn one_sided_dependency_identity_fingerprints(
    workspace: &BTreeMap<DependencySlot, Vec<PackageIdentity>>,
    e2e: &BTreeMap<DependencySlot, Vec<PackageIdentity>>,
) -> (String, String) {
    let root_only = workspace
        .iter()
        .filter(|(slot, _)| !e2e.contains_key(*slot))
        .map(|(slot, identities)| dependency_slot_identity_label(slot, identities))
        .collect::<BTreeSet<_>>();
    let e2e_only = e2e
        .iter()
        .filter(|(slot, _)| !workspace.contains_key(*slot))
        .map(|(slot, identities)| dependency_slot_identity_label(slot, identities))
        .collect::<BTreeSet<_>>();
    (
        identity_set_fingerprint(&root_only),
        identity_set_fingerprint(&e2e_only),
    )
}

fn dependency_slot_identity_label(slot: &DependencySlot, identities: &[PackageIdentity]) -> String {
    format!(
        "{}\0{}\0{}\0{}\0{}\0{:?}",
        slot.parent.name,
        slot.parent.version,
        slot.parent.source.as_deref().unwrap_or("workspace"),
        slot.parent.checksum.as_deref().unwrap_or("none"),
        slot.dependency,
        identities,
    )
}

fn identity_set_fingerprint(labels: &BTreeSet<String>) -> String {
    const FNV_1A_128_OFFSET: u128 = 0x6c62272e07bb014262b821756295c58d;
    const FNV_1A_128_PRIME: u128 = 0x0000000001000000000000000000013b;

    let mut fingerprint = FNV_1A_128_OFFSET;
    for label in labels {
        for byte in (label.len() as u64)
            .to_be_bytes()
            .into_iter()
            .chain(label.bytes())
        {
            fingerprint ^= u128::from(byte);
            fingerprint = fingerprint.wrapping_mul(FNV_1A_128_PRIME);
        }
    }
    format!("fnv1a128:{fingerprint:032x}")
}

fn package_identities(identities: Option<&Vec<PackageIdentity>>) -> Vec<String> {
    identities
        .into_iter()
        .flatten()
        .map(|identity| {
            format!(
                "{} (source={}, checksum={})",
                identity.version,
                identity.source.as_deref().unwrap_or("workspace"),
                identity.checksum.as_deref().unwrap_or("none"),
            )
        })
        .collect()
}

fn shared_dependency_identity_mismatches(
    workspace: &BTreeMap<DependencySlot, Vec<PackageIdentity>>,
    e2e: &BTreeMap<DependencySlot, Vec<PackageIdentity>>,
) -> Vec<String> {
    // Version, source, and checksum identify the compiled dependency source. Separate checks
    // compare curated features and pin every dependency slot reachable in only one workspace.
    workspace
        .keys()
        .filter(|slot| e2e.contains_key(*slot))
        .filter(|slot| workspace.get(*slot) != e2e.get(*slot))
        .map(|slot| {
            format!(
                "{} {} -> {}: root={:?}, e2e={:?}",
                slot.parent.name,
                slot.parent.version,
                slot.dependency,
                package_identities(workspace.get(slot)),
                package_identities(e2e.get(slot)),
            )
        })
        .collect()
}

#[test]
fn lockfile_identity_distinguishes_same_version_from_different_sources() {
    let registry = locked_package_identities_from(
        r#"[[package]]
name = "fixture-root"
version = "0.1.0"
dependencies = [
 "syn-solidity 1.5.7 (registry+https://github.com/rust-lang/crates.io-index)",
]

[[package]]
name = "syn-solidity"
version = "1.5.7"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "registry-checksum"
"#,
        "fixture-root",
    );
    let git = locked_package_identities_from(
        r#"[[package]]
name = "fixture-root"
version = "0.1.0"
dependencies = [
 "syn-solidity 1.5.7 (git+https://example.invalid/syn-solidity)",
]

[[package]]
name = "syn-solidity"
version = "1.5.7"
source = "git+https://example.invalid/syn-solidity"
checksum = "git-checksum"
"#,
        "fixture-root",
    );

    assert_ne!(registry, git);
}

#[test]
fn lockfile_mismatch_reports_source_and_checksum() {
    let slot = DependencySlot {
        parent: PackageKey {
            name: "alloy-sol-macro-expander".to_owned(),
            version: "1.5.7".to_owned(),
            source: Some("parent-source".to_owned()),
            checksum: Some("parent-checksum".to_owned()),
        },
        dependency: "syn".to_owned(),
    };
    let mut workspace = BTreeMap::new();
    workspace.insert(
        slot.clone(),
        vec![PackageIdentity {
            version: "2.0.117".to_owned(),
            source: Some("root-source".to_owned()),
            checksum: Some("root-checksum".to_owned()),
        }],
    );
    let mut e2e = BTreeMap::new();
    e2e.insert(
        slot,
        vec![PackageIdentity {
            version: "2.0.117".to_owned(),
            source: Some("e2e-source".to_owned()),
            checksum: Some("e2e-checksum".to_owned()),
        }],
    );

    let mismatch = shared_dependency_identity_mismatches(&workspace, &e2e).join("\n");
    assert!(mismatch.contains("root-source"), "{mismatch}");
    assert!(mismatch.contains("root-checksum"), "{mismatch}");
    assert!(mismatch.contains("e2e-source"), "{mismatch}");
    assert!(mismatch.contains("e2e-checksum"), "{mismatch}");
}

#[test]
fn lockfile_identity_distinguishes_dependency_edge_skew() {
    let workspace = locked_package_identities_from(&dual_syn_lock("1.5.7", true), "fixture-root");
    let e2e = locked_package_identities_from(&dual_syn_lock("1.6.1", true), "fixture-root");

    assert_ne!(workspace, e2e);
}

#[test]
fn lockfile_identity_ignores_an_unreachable_duplicate_version() {
    let workspace = locked_package_identities_from(&dual_syn_lock("1.5.7", true), "fixture-root");
    let e2e = locked_package_identities_from(&dual_syn_lock("1.5.7", false), "fixture-root");

    assert_eq!(workspace, e2e);
}

#[test]
fn lockfile_identity_distinguishes_uncurated_transitive_skew() {
    let workspace =
        locked_dependency_identities_from(&transitive_syn_lock("2.0.117"), "fixture-root");
    let e2e = locked_dependency_identities_from(&transitive_syn_lock("2.0.119"), "fixture-root");

    let mismatches = shared_dependency_identity_mismatches(&workspace, &e2e);
    assert_eq!(mismatches.len(), 1, "{mismatches:#?}");
    assert!(mismatches[0].contains("2.0.117"), "{mismatches:#?}");
    assert!(mismatches[0].contains("2.0.119"), "{mismatches:#?}");
}

#[test]
fn lockfile_identity_ignores_workspace_only_feature_edges() {
    let workspace = locked_dependency_identities_from(
        &transitive_syn_lock_with_feature_dependency(true),
        "fixture-root",
    );
    let e2e = locked_dependency_identities_from(
        &transitive_syn_lock_with_feature_dependency(false),
        "fixture-root",
    );

    assert!(shared_dependency_identity_mismatches(&workspace, &e2e).is_empty());
}

#[test]
fn curated_feature_union_guard_detects_a_synthetic_feature_difference() {
    let workspace = CODEGEN_AND_DECODE_PACKAGES
        .into_iter()
        .map(|package| (package.to_owned(), BTreeSet::from(["shared".to_owned()])))
        .collect::<BTreeMap<_, _>>();
    let mut e2e = workspace.clone();
    e2e.get_mut("syn-solidity")
        .expect("curated fixture contains syn-solidity")
        .insert("future-parser-mode".to_owned());

    let mismatches = curated_feature_union_mismatches(&workspace, &e2e);
    assert_eq!(mismatches.len(), 1, "{mismatches:#?}");
    assert!(mismatches[0].contains("syn-solidity"), "{mismatches:#?}");
    assert!(
        mismatches[0].contains("future-parser-mode"),
        "{mismatches:#?}"
    );
}

#[test]
fn curated_feature_union_distinguishes_package_identities() {
    let workspace = BTreeMap::from([
        (
            "syn-solidity v1.5.7".to_owned(),
            BTreeSet::from(["visit".to_owned()]),
        ),
        (
            "syn-solidity v1.6.1".to_owned(),
            BTreeSet::from(["visit-mut".to_owned()]),
        ),
    ]);
    let e2e = BTreeMap::from([
        (
            "syn-solidity v1.5.7".to_owned(),
            BTreeSet::from(["visit-mut".to_owned()]),
        ),
        (
            "syn-solidity v1.6.1".to_owned(),
            BTreeSet::from(["visit".to_owned()]),
        ),
    ]);

    let mismatches = curated_feature_union_mismatches(&workspace, &e2e);
    assert_eq!(mismatches.len(), 2, "{mismatches:#?}");
}

#[test]
fn one_sided_dependency_guard_detects_a_synthetic_edge() {
    let slot = DependencySlot {
        parent: PackageKey {
            name: "alloy-sol-macro-expander".to_owned(),
            version: "1.5.7".to_owned(),
            source: Some(LOCKFILE_REGISTRY.to_owned()),
            checksum: Some("fixture-checksum".to_owned()),
        },
        dependency: "future-helper".to_owned(),
    };
    let workspace = BTreeMap::from([(slot, Vec::new())]);
    let (root_only, e2e_only) = one_sided_dependency_slot_labels(&workspace, &BTreeMap::new());

    assert_eq!(
        root_only,
        BTreeSet::from([format!(
            "alloy-sol-macro-expander 1.5.7 ({LOCKFILE_REGISTRY}) -> future-helper"
        )])
    );
    assert!(e2e_only.is_empty());
}

#[test]
fn one_sided_dependency_guard_distinguishes_target_identity() {
    let slot = DependencySlot {
        parent: PackageKey {
            name: "alloy-sol-macro-expander".to_owned(),
            version: "1.5.7".to_owned(),
            source: Some(LOCKFILE_REGISTRY.to_owned()),
            checksum: Some("parent-checksum".to_owned()),
        },
        dependency: "future-helper".to_owned(),
    };
    let workspace_v1 = BTreeMap::from([(
        slot.clone(),
        vec![PackageIdentity {
            version: "1.0.0".to_owned(),
            source: Some(LOCKFILE_REGISTRY.to_owned()),
            checksum: Some("helper-v1".to_owned()),
        }],
    )]);
    let workspace_v2 = BTreeMap::from([(
        slot,
        vec![PackageIdentity {
            version: "2.0.0".to_owned(),
            source: Some(LOCKFILE_REGISTRY.to_owned()),
            checksum: Some("helper-v2".to_owned()),
        }],
    )]);

    assert_ne!(
        one_sided_dependency_identity_fingerprints(&workspace_v1, &BTreeMap::new()),
        one_sided_dependency_identity_fingerprints(&workspace_v2, &BTreeMap::new()),
        "the reviewed one-sided snapshot must include the target package identity"
    );
}

fn dual_syn_lock(selected_syn: &str, include_unreachable: bool) -> String {
    let unreachable = if include_unreachable {
        r#"
[[package]]
name = "syn-solidity"
version = "1.6.1"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "syn-1.6.1-checksum"
"#
    } else {
        ""
    };
    format!(
        r#"[[package]]
name = "fixture-root"
version = "0.1.0"
dependencies = [
 "alloy-sol-macro-expander",
]

[[package]]
name = "alloy-sol-macro-expander"
version = "1.5.7"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "expander-checksum"
dependencies = [
 "syn-solidity {selected_syn}",
]

[[package]]
name = "syn-solidity"
version = "1.5.7"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "syn-1.5.7-checksum"
{unreachable}"#,
    )
}

fn transitive_syn_lock(selected_syn: &str) -> String {
    format!(
        r#"[[package]]
name = "fixture-root"
version = "0.1.0"
dependencies = [
 "alloy-sol-macro-expander",
]

[[package]]
name = "alloy-sol-macro-expander"
version = "1.5.7"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "expander-checksum"
dependencies = [
 "syn {selected_syn}",
]

[[package]]
name = "syn"
version = "{selected_syn}"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "syn-{selected_syn}-checksum"
"#,
    )
}

fn transitive_syn_lock_with_feature_dependency(include_feature_dependency: bool) -> String {
    let feature_dependency = include_feature_dependency.then_some(" \"feature-helper\",\n");
    let feature_package = include_feature_dependency.then_some(
        r#"
[[package]]
name = "feature-helper"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "feature-helper-checksum"
"#,
    );
    format!(
        r#"[[package]]
name = "fixture-root"
version = "0.1.0"
dependencies = [
 "alloy-sol-macro-expander",
]

[[package]]
name = "alloy-sol-macro-expander"
version = "1.5.7"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "expander-checksum"
dependencies = [
 "syn 2.0.117",
{feature_dependency}]

[[package]]
name = "syn"
version = "2.0.117"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "syn-2.0.117-checksum"
{feature_package}"#,
        feature_dependency = feature_dependency.unwrap_or_default(),
        feature_package = feature_package.unwrap_or_default(),
    )
}

fn locked_package_identities(
    path: &Path,
    root_package: &str,
) -> BTreeMap<String, Vec<PackageIdentity>> {
    let source = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    locked_package_identities_from(&source, root_package)
}

fn locked_dependency_identities(
    path: &Path,
    root_package: &str,
) -> BTreeMap<DependencySlot, Vec<PackageIdentity>> {
    let source = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    locked_dependency_identities_from(&source, root_package)
}

fn locked_package_identities_from(
    source: &str,
    root_package: &str,
) -> BTreeMap<String, Vec<PackageIdentity>> {
    let packages = parse_locked_packages(source);
    let closure = codegen_dependency_closure(&packages, root_package);

    let mut identities = BTreeMap::<String, Vec<PackageIdentity>>::new();
    for index in closure {
        let package = &packages[index];
        identities
            .entry(package.key.name.clone())
            .or_default()
            .push(package_identity(package));
    }
    for package_identities in identities.values_mut() {
        package_identities.sort();
    }
    identities
}

fn locked_dependency_identities_from(
    source: &str,
    root_package: &str,
) -> BTreeMap<DependencySlot, Vec<PackageIdentity>> {
    let packages = parse_locked_packages(source);
    let closure = codegen_dependency_closure(&packages, root_package);
    let mut dependencies = BTreeMap::<DependencySlot, Vec<PackageIdentity>>::new();
    for index in &closure {
        let package = &packages[*index];
        for dependency in &package.dependencies {
            let dependency = &packages[dependency_target(&packages, dependency)];
            dependencies
                .entry(DependencySlot {
                    parent: package.key.clone(),
                    dependency: dependency.key.name.clone(),
                })
                .or_default()
                .push(package_identity(dependency));
        }
    }
    for identities in dependencies.values_mut() {
        identities.sort();
        identities.dedup();
    }
    dependencies
}

fn codegen_dependency_closure(packages: &[LockedPackage], root_package: &str) -> BTreeSet<usize> {
    let roots = packages
        .iter()
        .enumerate()
        .filter_map(|(index, package)| (package.key.name == root_package).then_some(index))
        .collect::<Vec<_>>();
    assert_eq!(
        roots.len(),
        1,
        "lockfile must contain exactly one {root_package} package"
    );

    let mut pending = roots;
    let mut root_reachable = BTreeSet::new();
    while let Some(index) = pending.pop() {
        if !root_reachable.insert(index) {
            continue;
        }
        pending.extend(
            packages[index]
                .dependencies
                .iter()
                .map(|dependency| dependency_target(&packages, dependency)),
        );
    }

    let mut pending = root_reachable
        .iter()
        .copied()
        .filter(|index| CODEGEN_AND_DECODE_PACKAGES.contains(&packages[*index].key.name.as_str()))
        .collect::<Vec<_>>();
    let mut closure = BTreeSet::new();
    while let Some(index) = pending.pop() {
        if !closure.insert(index) {
            continue;
        }
        pending.extend(
            packages[index]
                .dependencies
                .iter()
                .map(|dependency| dependency_target(&packages, dependency)),
        );
    }

    closure
}

fn package_identity(package: &LockedPackage) -> PackageIdentity {
    PackageIdentity {
        version: package.key.version.clone(),
        source: package.key.source.clone(),
        checksum: package.key.checksum.clone(),
    }
}

fn parse_locked_packages(source: &str) -> Vec<LockedPackage> {
    source
        .split("[[package]]")
        .skip(1)
        .map(|block| {
            let mut name = None;
            let mut version = None;
            let mut package_source = None;
            let mut checksum = None;
            let mut dependencies = Vec::new();
            let mut in_dependencies = false;
            for line in block.lines() {
                let trimmed = line.trim();
                if in_dependencies {
                    if trimmed == "]" {
                        in_dependencies = false;
                    } else if trimmed.starts_with('"') {
                        dependencies.push(unquote(trimmed.trim_end_matches(',')));
                    }
                } else if let Some(value) = line.strip_prefix("name = ") {
                    name = Some(unquote(value));
                } else if let Some(value) = line.strip_prefix("version = ") {
                    version = Some(unquote(value));
                } else if let Some(value) = line.strip_prefix("source = ") {
                    package_source = Some(unquote(value));
                } else if let Some(value) = line.strip_prefix("checksum = ") {
                    checksum = Some(unquote(value));
                } else if trimmed == "dependencies = [" {
                    in_dependencies = true;
                }
            }
            LockedPackage {
                key: PackageKey {
                    name: name.expect("locked package has a name"),
                    version: version.expect("locked package has a version"),
                    source: package_source,
                    checksum,
                },
                dependencies,
            }
        })
        .collect()
}

fn dependency_target(packages: &[LockedPackage], dependency: &str) -> usize {
    let (coordinates, source) =
        dependency
            .rsplit_once(" (")
            .map_or((dependency, None), |(coordinates, source)| {
                (
                    coordinates,
                    Some(source.strip_suffix(')').expect("dependency source closes")),
                )
            });
    let mut coordinates = coordinates.split_whitespace();
    let name = coordinates.next().expect("dependency has a package name");
    let version = coordinates.next();
    assert!(
        coordinates.next().is_none(),
        "dependency coordinates have an unexpected shape: {dependency}"
    );

    let candidates = packages
        .iter()
        .enumerate()
        .filter_map(|(index, package)| {
            (package.key.name == name
                && version.is_none_or(|version| package.key.version == version)
                && source.is_none_or(|source| package.key.source.as_deref() == Some(source)))
            .then_some(index)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        candidates.len(),
        1,
        "dependency {dependency} must resolve to exactly one locked package"
    );
    candidates[0]
}

fn unquote(value: &str) -> String {
    value.trim_matches('"').to_owned()
}
