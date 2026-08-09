use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
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

    for package in CODEGEN_AND_DECODE_PACKAGES {
        assert!(
            workspace.contains_key(package),
            "root workspace lockfile must contain curated package {package}"
        );
        assert!(
            e2e.contains_key(package),
            "e2e lockfile must contain curated package {package}"
        );
    }

    let mismatches =
        shared_dependency_identity_mismatches(&workspace_dependencies, &e2e_dependencies);
    assert!(
        mismatches.is_empty(),
        "e2e lockfile shared codegen/decode dependency closure must match the root workspace: {mismatches:#?}"
    );

    for package in CODEGEN_AND_DECODE_PACKAGES {
        assert_eq!(
            e2e.get(package),
            workspace.get(package),
            "e2e lockfile package {package} must match the root workspace"
        );
    }
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
    // Feature activation is workspace-owned, so dependency slots reachable in only one lock are
    // exempt. Shared decode/codegen behavior follows compiled version, source, and checksum,
    // matching the interpreter hash's ratified lockfile fingerprint rule.
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
