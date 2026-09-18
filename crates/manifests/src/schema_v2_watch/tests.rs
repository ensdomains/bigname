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
        assert_eq!(entry.start, 0);
    }
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
