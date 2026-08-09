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
    dependencies: Vec<PackageKey>,
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

    for package in CODEGEN_AND_DECODE_PACKAGES {
        assert!(
            workspace.contains_key(package),
            "root workspace lockfile must contain curated package {package}"
        );
        assert_eq!(
            e2e.get(package),
            workspace.get(package),
            "e2e lockfile package {package} must match the root workspace"
        );
    }
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

fn locked_package_identities(
    path: &Path,
    root_package: &str,
) -> BTreeMap<String, Vec<PackageIdentity>> {
    let source = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    locked_package_identities_from(&source, root_package)
}

fn locked_package_identities_from(
    source: &str,
    root_package: &str,
) -> BTreeMap<String, Vec<PackageIdentity>> {
    let packages = parse_locked_packages(source);
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
    let mut reachable = BTreeSet::new();
    while let Some(index) = pending.pop() {
        if !reachable.insert(index) {
            continue;
        }
        pending.extend(
            packages[index]
                .dependencies
                .iter()
                .map(|dependency| dependency_target(&packages, dependency)),
        );
    }

    let mut curated = BTreeMap::<String, Vec<PackageIdentity>>::new();
    for index in reachable {
        let package = &packages[index];
        if !CODEGEN_AND_DECODE_PACKAGES.contains(&package.key.name.as_str()) {
            continue;
        }
        let mut dependencies = package
            .dependencies
            .iter()
            .map(|dependency| &packages[dependency_target(&packages, dependency)].key)
            .filter(|dependency| CODEGEN_AND_DECODE_PACKAGES.contains(&dependency.name.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        dependencies.sort();
        dependencies.dedup();
        curated
            .entry(package.key.name.clone())
            .or_default()
            .push(PackageIdentity {
                version: package.key.version.clone(),
                source: package.key.source.clone(),
                checksum: package.key.checksum.clone(),
                dependencies,
            });
    }
    for identities in curated.values_mut() {
        identities.sort();
    }
    curated
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
