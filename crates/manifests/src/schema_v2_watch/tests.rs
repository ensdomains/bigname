use super::*;

#[test]
fn declared_resolver_implementations_compile_topic1_narrowed_upgraded_watches() -> Result<()> {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = crate::load_repository(workspace_root.join("manifests/sepolia"))?;
    let resolver = repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == crate::ENS_V2_RESOLVER_SOURCE_FAMILY)
        .expect("official Sepolia resolver manifest")
        .manifest
        .clone();
    let upgraded = format!("{}", alloy_primitives::keccak256(b"Upgraded(address)"));
    let compiled = compile_watch_scope(&resolver)?;
    let implementations = compiled
        .iter()
        .filter(|entry| matches!(entry.emitter, WatchEmitter::Implementation { .. }))
        .collect::<Vec<_>>();
    assert_eq!(
        implementations.len(),
        resolver.resolver_implementations.len()
    );
    for entry in &implementations {
        assert_eq!(entry.topic0, upgraded);
    }
    assert_eq!(
        resolver.resolver_implementations[0].start_block,
        Some(11_820_406)
    );
    assert_eq!(implementations[0].start, 11_820_406);
    assert!(matches!(
        &implementations[0].emitter,
        WatchEmitter::Implementation { family, implementation }
            if family == crate::ENS_V2_RESOLVER_SOURCE_FAMILY
                && *implementation == normalize_address(&resolver.resolver_implementations[0].address)
    ));
    assert!(
        !compiled
            .iter()
            .any(|entry| entry.emitter == WatchEmitter::All && entry.topic0 == upgraded),
        "the announcement watch is topic1-narrowed, never an all-emitter Upgraded watch"
    );

    let key = WatchKey {
        emitter: implementations[0].emitter.clone(),
        topic0: upgraded.clone(),
    };
    let mut previous = BTreeMap::new();
    assert!(!watch_is_covered(Some(&previous), &key, 0));
    previous.insert(
        WatchKey {
            emitter: WatchEmitter::Implementation {
                family: crate::ENS_V2_RESOLVER_SOURCE_FAMILY.to_owned(),
                implementation: "0x00000000000000000000000000000000000000ee".to_owned(),
            },
            topic0: upgraded.clone(),
        },
        0,
    );
    assert!(
        !watch_is_covered(Some(&previous), &key, 0),
        "another implementation's entry does not cover this one"
    );
    previous.insert(key.clone(), 0);
    assert!(watch_is_covered(Some(&previous), &key, 0));
    assert!(
        watch_is_covered(Some(&previous), &key, 11_820_406),
        "raising an implementation's start is covered by its earlier entry"
    );
    previous.insert(key.clone(), 11_820_406);
    assert!(
        !watch_is_covered(Some(&previous), &key, 11_820_405),
        "lowering an implementation's start widens"
    );
    let all_only = BTreeMap::from([(
        WatchKey {
            emitter: WatchEmitter::All,
            topic0: upgraded,
        },
        0,
    )]);
    assert!(watch_is_covered(Some(&all_only), &key, 0));
    Ok(())
}

#[test]
fn checked_in_approvals_compile_only_for_declared_roles_and_intervals() -> Result<()> {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut approval_count = 0;
    for profile in ["mainnet", "sepolia"] {
        let repository = crate::load_repository(workspace_root.join("manifests").join(profile))?;
        for loaded in repository.manifests() {
            let compiled = compile_watch_scope(&loaded.manifest)?;
            for event in &loaded.manifest.abi.events {
                let parsed = event.parsed_event_view()?;
                if !crate::is_address_scoped_approval(
                    &loaded.manifest.source_family,
                    &parsed.canonical_signature(),
                ) {
                    continue;
                }
                approval_count += 1;
                let topic0 = parsed.topic0().expect("approval event topic0");
                let actual = compiled
                    .iter()
                    .filter(|entry| entry.topic0 == topic0)
                    .map(|entry| match &entry.emitter {
                        WatchEmitter::Address { family, address } => {
                            assert_eq!(family, &loaded.manifest.source_family);
                            (address.clone(), entry.start)
                        }
                        WatchEmitter::All
                        | WatchEmitter::Family { .. }
                        | WatchEmitter::Implementation { .. } => panic!(
                            "{} {} must not compile an all-emitter or discovered-family watch",
                            loaded.manifest.source_family, event.name
                        ),
                    })
                    .collect::<BTreeSet<_>>();
                let expected = loaded
                    .manifest
                    .contracts
                    .iter()
                    .filter(|contract| event.emitter_roles.contains(&contract.role))
                    .map(|contract| {
                        (
                            crate::normalize_address(&contract.address),
                            contract.start_block.unwrap_or(0),
                        )
                    })
                    .collect::<BTreeSet<_>>();
                assert_eq!(
                    actual, expected,
                    "{} {} must follow its role declarations exactly",
                    loaded.manifest.source_family, event.name
                );
            }
        }
    }
    assert_eq!(approval_count, 19);
    Ok(())
}

const ENS_V2_APPROVAL_START: u64 = 10_893_181;

fn approval_for_all_topic0() -> String {
    format!(
        "{}",
        alloy_primitives::keccak256(crate::APPROVAL_FOR_ALL_SIGNATURE.as_bytes())
    )
}

fn sepolia_manifest(source_family: &str) -> Result<SourceManifest> {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = crate::load_repository(workspace_root.join("manifests/sepolia"))?;
    Ok(repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == source_family)
        .with_context(|| format!("official Sepolia {source_family} manifest"))?
        .manifest
        .clone())
}

#[test]
fn ens_v2_registry_approvals_compile_family_wide_from_their_declared_start() -> Result<()> {
    let topic0 = approval_for_all_topic0();
    let registry = sepolia_manifest(crate::ENS_V2_REGISTRY_SOURCE_FAMILY)?;
    let entries = compile_watch_scope(&registry)?
        .into_iter()
        .filter(|entry| entry.topic0 == topic0)
        .map(|entry| (entry.emitter, entry.start))
        .collect::<BTreeSet<_>>();
    let address = |address: &str| WatchEmitter::Address {
        family: crate::ENS_V2_REGISTRY_SOURCE_FAMILY.to_owned(),
        address: address.to_owned(),
    };
    assert_eq!(
        entries,
        BTreeSet::from([
            (
                WatchEmitter::Family {
                    namespace: "ens".to_owned(),
                    family: crate::ENS_V2_REGISTRY_SOURCE_FAMILY.to_owned(),
                },
                ENS_V2_APPROVAL_START,
            ),
            (
                address("0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4"),
                11_820_399
            ),
        ]),
        "discovered registries are watched from the event start, the declared ETHRegistry from its own"
    );
    for entry in compile_watch_scope(&registry)? {
        if matches!(entry.emitter, WatchEmitter::Family { .. }) && entry.topic0 != topic0 {
            assert_eq!(entry.start, 0, "other family topics keep block zero");
        }
    }

    // The root family has no discovered emitters, so only its declared RootRegistry is watched.
    let root = sepolia_manifest(crate::ENS_V2_ROOT_SOURCE_FAMILY)?;
    let entries = compile_watch_scope(&root)?
        .into_iter()
        .filter(|entry| entry.topic0 == topic0)
        .map(|entry| (entry.emitter, entry.start))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        entries,
        BTreeSet::from([(
            WatchEmitter::Address {
                family: crate::ENS_V2_ROOT_SOURCE_FAMILY.to_owned(),
                address: "0xb458d6a3a77919449d03e7a6903c26827c1ec43f".to_owned(),
            },
            11_820_291
        )])
    );
    Ok(())
}

#[test]
fn adding_ens_v2_registry_approvals_widens_from_the_event_start() -> Result<()> {
    let chain_id = "ethereum-sepolia";
    let widened_from = |edit: fn(&mut crate::ManifestAbiEvent)| -> Result<Option<u64>> {
        let mut previous = Snapshot::default();
        let mut desired = Snapshot::default();
        for family in [
            crate::ENS_V2_REGISTRY_SOURCE_FAMILY,
            crate::ENS_V2_ROOT_SOURCE_FAMILY,
        ] {
            let mut manifest = sepolia_manifest(family)?;
            for event in &mut manifest.abi.events {
                if event.name == "ApprovalForAll" {
                    edit(event);
                }
            }
            record(&mut desired, &manifest, &manifest_payload(&manifest)?)?;
            manifest
                .abi
                .events
                .retain(|event| event.name != "ApprovalForAll");
            record(&mut previous, &manifest, &manifest_payload(&manifest)?)?;
        }
        widening_start(
            &previous,
            &desired,
            chain_id,
            &PersistedWatchCoverage::new(),
            false,
        )
    };
    assert_eq!(widened_from(|_| {})?, Some(ENS_V2_APPROVAL_START));
    assert_eq!(
        widened_from(|event| event.start_block = None)?,
        Some(0),
        "without the event start the family-wide entry widens from block zero"
    );
    Ok(())
}

#[test]
fn an_event_start_is_refused_where_no_family_wide_or_all_emitter_entry_compiles() -> Result<()> {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = workspace_root.join("manifests/sepolia/ethereum/ens");
    let load_with_start =
        |family: &str, event: &str| -> Result<Result<crate::ManifestRepository>> {
            let root = std::env::temp_dir().join(format!(
                "bigname-event-start-{family}-{}",
                std::process::id()
            ));
            let directory = root.join("ethereum/ens").join(family);
            std::fs::create_dir_all(&directory)?;
            let manifest = std::fs::read_to_string(source.join(family).join("v2.toml"))?;
            let edited = manifest.replacen(event, &format!("{event}\nstart_block = 5"), 1);
            assert_ne!(edited, manifest);
            std::fs::write(directory.join("v2.toml"), edited)?;
            let loaded = crate::load_repository(&root);
            std::fs::remove_dir_all(&root)?;
            Ok(loaded)
        };
    for (family, event) in [
        // Not a discovered-emitter family.
        ("ens_v2_root_l1", "name = \"ApprovalForAll\""),
        // A role-scoped event of a discovered-emitter family.
        ("ens_v2_resolver_l1", "name = \"ABIChanged\""),
    ] {
        let error = load_with_start(family, event)?
            .err()
            .map(|error| format!("{error:#}"));
        assert!(
            error.as_deref().is_some_and(|error| {
                error.contains("compiles no family-wide or all-emitter watch entry")
            }),
            "{family}: {error:?}"
        );
    }
    // An all-emitter event accepts a start, and its all-emitter entry compiles from it.
    let topic0 = crate::registry_announcement_topic0();
    let repository = load_with_start("ens_v2_registry_l1", "name = \"RegistryCreated\"")??;
    let registry = &repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == crate::ENS_V2_REGISTRY_SOURCE_FAMILY)
        .context("edited registry manifest")?
        .manifest;
    let all = compile_watch_scope(registry)?
        .into_iter()
        .filter(|entry| entry.emitter == WatchEmitter::All && entry.topic0 == topic0)
        .map(|entry| entry.start)
        .collect::<Vec<_>>();
    assert_eq!(all, [5]);
    Ok(())
}

/// An all-emitter entry compiles from its event's start. Raising that start over an entry the
/// previous plan compiled from block zero is covered and stamps no Ingest redo. Lowering it widens
/// from the new start, and the started entry does not cover an address entry below it.
#[test]
fn a_started_all_emitter_entry_covers_from_its_start_only() -> Result<()> {
    const START: u64 = 26_105_621;
    let chain_id = "ethereum-mainnet";
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = crate::load_repository(workspace_root.join("manifests/mainnet"))?;
    let registry = repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == crate::ENS_V2_REGISTRY_SOURCE_FAMILY)
        .context("mainnet registry manifest")?
        .manifest
        .clone();
    let with_start = |start: Option<u64>| {
        let mut manifest = registry.clone();
        for event in &mut manifest.abi.events {
            if event.name == "RegistryCreated" {
                event.start_block = start;
            }
        }
        manifest
    };
    let snapshot = |manifest: &SourceManifest| -> Result<Snapshot> {
        let mut snapshot = Snapshot::default();
        record(&mut snapshot, manifest, &manifest_payload(manifest)?)?;
        Ok(snapshot)
    };
    let widened = |previous: Option<u64>, desired: Option<u64>| -> Result<Option<u64>> {
        widening_start(
            &snapshot(&with_start(previous))?,
            &snapshot(&with_start(desired))?,
            chain_id,
            &PersistedWatchCoverage::new(),
            false,
        )
    };
    let all = WatchKey {
        emitter: WatchEmitter::All,
        topic0: crate::registry_announcement_topic0(),
    };
    let started = snapshot(&with_start(Some(START)))?;
    assert_eq!(started.watch_by_chain[chain_id].get(&all), Some(&START));
    assert_eq!(
        widened(None, Some(START))?,
        None,
        "a later start is covered"
    );
    assert_eq!(widened(Some(START), Some(START - 100))?, Some(START - 100));

    let address = WatchKey {
        emitter: WatchEmitter::Address {
            family: crate::ENS_V2_REGISTRY_SOURCE_FAMILY.to_owned(),
            address: "0x0000000000000000000000000000000000000001".to_owned(),
        },
        topic0: all.topic0.clone(),
    };
    let from_zero = BTreeMap::from([(all.clone(), 0)]);
    let from_start = BTreeMap::from([(all.clone(), START)]);
    assert!(watch_is_covered(Some(&from_zero), &all, START));
    assert!(!watch_is_covered(Some(&from_start), &address, START - 1));
    assert!(watch_is_covered(Some(&from_start), &address, START));
    Ok(())
}

/// The mainnet ENSv2 registry and resolver families are new to the profile, so each family-wide
/// and all-emitter watch entry starts at its family's earliest declaration: the ETHRegistry for
/// the registry family and the ENSv1 mirror resolver for the resolver family. No other event of
/// the profile declares a start.
#[test]
fn the_mainnet_deployment_profile_starts_new_family_entries_at_each_family_s_first_declaration()
-> Result<()> {
    const ETH_REGISTRY: &str = "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2";
    const ETH_REGISTRY_START: u64 = 26_105_621;
    const MIRROR_RESOLVER_START: u64 = 26_105_292;
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = crate::load_repository(workspace_root.join("manifests/mainnet"))?;
    let topic0 = approval_for_all_topic0();
    let mut starts = Vec::new();
    for loaded in repository.manifests() {
        let manifest = &loaded.manifest;
        for event in &manifest.abi.events {
            if let Some(start) = event.start_block {
                starts.push((manifest.source_family.as_str(), event.name.as_str(), start));
            }
        }
        for entry in compile_watch_scope(manifest)? {
            if !matches!(
                entry.emitter,
                WatchEmitter::Family { .. } | WatchEmitter::All
            ) {
                continue;
            }
            let expected = match manifest.source_family.as_str() {
                crate::ENS_V2_REGISTRY_SOURCE_FAMILY => ETH_REGISTRY_START,
                crate::ENS_V2_RESOLVER_SOURCE_FAMILY => MIRROR_RESOLVER_START,
                _ => 0,
            };
            assert_eq!(
                entry.start, expected,
                "{} {}",
                manifest.source_family, entry.topic0
            );
        }
    }
    let mut declared_starts = BTreeSet::new();
    for (family, name, start) in &starts {
        let expected = match *family {
            crate::ENS_V2_REGISTRY_SOURCE_FAMILY => ETH_REGISTRY_START,
            crate::ENS_V2_RESOLVER_SOURCE_FAMILY => MIRROR_RESOLVER_START,
            _ => 0,
        };
        assert_eq!(*start, expected, "{family} {name}");
        declared_starts.insert(*family);
    }
    assert_eq!(starts.len(), 27, "{starts:?}");
    assert_eq!(
        declared_starts,
        BTreeSet::from([
            crate::ENS_V2_REGISTRY_SOURCE_FAMILY,
            crate::ENS_V2_RESOLVER_SOURCE_FAMILY
        ])
    );
    let registry = repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == crate::ENS_V2_REGISTRY_SOURCE_FAMILY)
        .context("mainnet registry manifest")?;
    let declared = compile_watch_scope(&registry.manifest)?
        .into_iter()
        .filter(|entry| entry.topic0 == topic0)
        .map(|entry| (entry.emitter, entry.start))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        declared,
        BTreeSet::from([
            (
                WatchEmitter::Family {
                    namespace: "ens".to_owned(),
                    family: crate::ENS_V2_REGISTRY_SOURCE_FAMILY.to_owned(),
                },
                ETH_REGISTRY_START,
            ),
            (
                WatchEmitter::Address {
                    family: crate::ENS_V2_REGISTRY_SOURCE_FAMILY.to_owned(),
                    address: ETH_REGISTRY.to_owned(),
                },
                ETH_REGISTRY_START,
            ),
        ])
    );
    Ok(())
}

#[test]
fn user_registry_origin_metadata_widens_only_its_declared_address() -> Result<()> {
    let desired = sepolia_manifest("ens_v2_migration_l1")?;
    let mut previous = desired.clone();
    previous
        .contracts
        .retain(|contract| contract.role != "user_registry_implementation");
    assert_eq!(desired.contracts.len(), previous.contracts.len() + 1);
    let before = compile_watch_scope(&previous)?;
    let after = compile_watch_scope(&desired)?;
    assert!(before.iter().all(|entry| after.contains(entry)));
    let added: Vec<_> = after
        .iter()
        .filter(|entry| !before.contains(entry))
        .collect();
    assert_eq!(before.len(), 16);
    assert_eq!(after.len(), 18);
    assert_eq!(added.len(), 2);
    for entry in &added {
        assert_eq!(entry.start, 11_820_439);
        assert_eq!(
            entry.emitter,
            WatchEmitter::Address {
                family: "ens_v2_migration_l1".to_owned(),
                address: "0x9bd8a88719068d09ecee662f36c0e3856708366a".to_owned(),
            }
        );
    }
    assert_eq!(
        added
            .iter()
            .map(|entry| entry.topic0.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "0x0a2c575ff341b41da136c9ccae74ec230a927a024d18f0dccf46d123f28f5f54",
            "0xbd0c01e5bf66003280556423db4a8bf79043c146ac57f657c30049dd43316649",
        ])
    );
    let mut old_snapshot = Snapshot::default();
    let mut new_snapshot = Snapshot::default();
    record(&mut old_snapshot, &previous, &manifest_payload(&previous)?)?;
    record(&mut new_snapshot, &desired, &manifest_payload(&desired)?)?;
    assert_eq!(
        widening_start(
            &old_snapshot,
            &new_snapshot,
            "ethereum-sepolia",
            &PersistedWatchCoverage::new(),
            false
        )?,
        Some(11_820_439)
    );
    println!(
        "registry_origin_watch_plan={}",
        serde_json::json!({
            "before": before, "after": after, "added": added, "widening_start": 11_820_439
        })
    );
    Ok(())
}
