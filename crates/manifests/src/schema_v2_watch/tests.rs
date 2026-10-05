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
fn an_event_start_is_refused_where_no_family_wide_entry_compiles() -> Result<()> {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = workspace_root.join("manifests/sepolia/ethereum/ens");
    for (family, event) in [
        // Not a discovered-emitter family.
        ("ens_v2_root_l1", "name = \"ApprovalForAll\""),
        // An all-emitter event.
        ("ens_v2_registry_l1", "name = \"RegistryCreated\""),
    ] {
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
        let error = crate::load_repository(&root)
            .err()
            .map(|error| format!("{error:#}"));
        std::fs::remove_dir_all(&root)?;
        assert!(
            error
                .as_deref()
                .is_some_and(|error| error.contains("compiles no family-wide watch entry")),
            "{family}: {error:?}"
        );
    }
    Ok(())
}

#[test]
fn the_mainnet_deployment_profile_declares_no_ens_v2_approval_and_no_event_start() -> Result<()> {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repository = crate::load_repository(workspace_root.join("manifests/mainnet"))?;
    for loaded in repository.manifests() {
        assert!(
            !loaded.manifest.source_family.starts_with("ens_v2_"),
            "mainnet declares no ENSv2 family"
        );
        for event in &loaded.manifest.abi.events {
            assert_eq!(event.start_block, None, "{}", event.name);
        }
        for entry in compile_watch_scope(&loaded.manifest)? {
            if matches!(entry.emitter, WatchEmitter::Family { .. }) {
                assert_eq!(entry.start, 0);
            }
        }
    }
    Ok(())
}
