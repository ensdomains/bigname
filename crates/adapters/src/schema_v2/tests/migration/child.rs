//! Direct-child ENSv1→ENSv2 migration correlation, ported from the validated catalog scenarios
//! `C-01`…`C-06`, `H-01`, and `H-04` on `worknotes/migration-catalog`. Two cases are derived
//! rather than ported — a second child of one parent, and a child ordered before its parent
//! registry — because the catalog has no run for either.

use super::*;

/// The observable logs of one migration level: the receiving registry's proxy creation when the
/// migrated name is locked, then the registration that level performs. The locked branch deploys
/// the child's registry and only then registers the label into the parent registry, so the proxy
/// creation always precedes the registration within the level
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L149 @ ens_v2_sepolia_20260629@ccaeb58)
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L168 @ ens_v2_sepolia_20260629@ccaeb58);
/// the emancipated branch registers into the parent's existing registry with no proxy at all
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L188 @ ens_v2@a971bd64).
/// The registration itself is emitted by the receiving registry
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467 @ ens_v2@a971bd64).
fn level_logs(level: &Value, addresses: &Value) -> anyhow::Result<Vec<RawLogInput>> {
    let block = level["migration_block"].as_i64().unwrap();
    let emitter = level["emitting_registry"].as_str().unwrap();
    let label = level["label"].as_str().unwrap().to_owned();
    let token = decimal_u256(&level["v2_token_id"])?;
    let sender = level["registration_sender"].as_str().unwrap().parse()?;
    let mut logs = v1_predecessor_logs(level, addresses)?;
    if let Some(registry) = level["registry"].as_str() {
        let created = level["registry_created_log_index"].as_i64().unwrap();
        logs.push(raw_at_transaction(
            super::super::RegistryCreated {}.encode_log_data(),
            block,
            0,
            created,
            registry,
        ));
        logs.push(raw_at_transaction(
            super::super::v2_registry::ParentUpdated {
                parent: emitter.parse()?,
                label: label.clone(),
                sender: emitter.parse()?,
            }
            .encode_log_data(),
            block,
            0,
            created + 1,
            registry,
        ));
        logs.push(raw_at_transaction(
            ProxyDeployed {
                sender: level["factory_sender"].as_str().unwrap().parse()?,
                proxyAddress: registry.parse()?,
                salt: decimal_u256(&level["factory_salt"])?,
                implementation: Address::from([0x44; 20]),
            }
            .encode_log_data(),
            block,
            0,
            level["factory_log_index"].as_i64().unwrap(),
            addresses["factory"].as_str().unwrap(),
        ));
    }
    let registration = level["v2_registration_log_index"].as_i64().unwrap();
    logs.push(raw_at_transaction(
        super::super::v2_registry::LabelRegistered {
            tokenId: token,
            labelHash: level["labelhash"].as_str().unwrap().parse()?,
            label,
            owner: level["registration_owner"].as_str().unwrap().parse()?,
            expiry: level["stored_expiry"].as_u64().unwrap(),
            sender,
        }
        .encode_log_data(),
        block,
        0,
        registration,
        emitter,
    ));
    logs.push(raw_at_transaction(
        super::super::v2_registry::TokenResource {
            tokenId: token,
            resource: token,
        }
        .encode_log_data(),
        block,
        0,
        registration + 2,
        emitter,
    ));
    if let Some(index) = level["subregistry_log_index"].as_i64() {
        logs.push(raw_at_transaction(
            super::super::v2_registry::SubregistryUpdated {
                tokenId: token,
                subregistry: level["registry"].as_str().unwrap().parse()?,
                sender: emitter.parse()?,
            }
            .encode_log_data(),
            block,
            0,
            index,
            emitter,
        ));
    }
    Ok(logs)
}

/// The child's ENSv1 side: the wrap that put it in the NameWrapper, then the cleanup the receiver
/// performs while migrating it — a locked child's token parked in the Graveyard, an emancipated
/// child's node unwrapped into it. A second-level name carries none of this here; its own
/// predecessor rule is slice 2A's.
fn v1_predecessor_logs(level: &Value, addresses: &Value) -> anyhow::Result<Vec<RawLogInput>> {
    let Some(labels) = level["labels"].as_array() else {
        return Ok(Vec::new());
    };
    let block = level["migration_block"].as_i64().unwrap();
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let node: B256 = level["namehash"].as_str().unwrap().parse()?;
    let owner: Address = level["wrapped_owner"].as_str().unwrap().parse()?;
    let registry: Address = level["emitting_registry"].as_str().unwrap().parse()?;
    let graveyard: Address = address(addresses, "graveyard")?;
    let mut encoded = Vec::new();
    for entry in labels {
        let label = entry.as_str().expect("fixture label");
        encoded.push(u8::try_from(label.len())?);
        encoded.extend_from_slice(label.as_bytes());
    }
    encoded.push(0);
    // The interpreter rejects a wrap whose DNS name does not hash to its node, so this doubles as
    // a check that the ported label path matches the catalog's namehash.
    let mut logs = vec![raw_at_transaction(
        super::super::NameWrapped {
            node,
            name: encoded.into(),
            owner,
            fuses: u32::try_from(level["wrap_fuses"].as_u64().unwrap())?,
            expiry: level["wrap_expiry"].as_u64().unwrap(),
        }
        .encode_log_data(),
        level["wrap_block"].as_i64().unwrap(),
        0,
        level["wrap_log_index"].as_i64().unwrap_or(0),
        wrapper,
    )];
    let token = U256::from_be_bytes(node.0);
    let hop = |from: Address, to: Address, log_index: i64| {
        raw_at_transaction(
            super::super::v2_registry::TransferSingle {
                operator: from,
                from,
                to,
                id: token,
                value: U256::from(1),
            }
            .encode_log_data(),
            block,
            0,
            log_index,
            wrapper,
        )
    };
    let cleanup = level["cleanup_log_index"].as_i64().unwrap_or(0);
    logs.push(hop(owner, registry, cleanup));
    if level["migration_path"] == "locked_child" {
        logs.push(hop(registry, graveyard, cleanup + 1));
    } else {
        logs.push(raw_at_transaction(
            super::super::NameUnwrapped {
                node,
                owner: graveyard,
            }
            .encode_log_data(),
            block,
            0,
            cleanup + 1,
            wrapper,
        ));
    }
    Ok(logs)
}

fn scenario_logs(
    scenario: &Value,
    addresses: &Value,
    levels: &[&str],
) -> anyhow::Result<Vec<RawLogInput>> {
    let mut logs = Vec::new();
    for level in levels {
        logs.extend(level_logs(&scenario[level], addresses)?);
    }
    Ok(ordered(logs))
}

/// Intake hands the adapter one block-ordered stream. A level's ENSv1 wrap precedes every
/// migration block, so the concatenation has to be re-sorted rather than assumed ordered.
fn ordered(mut logs: Vec<RawLogInput>) -> Vec<RawLogInput> {
    logs.sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));
    logs
}

/// Whether a log is part of a child's ENSv1 predecessor cleanup, which the ENSv1 NameWrapper
/// emits.
fn is_v1_cleanup(log: &RawLogInput, addresses: &Value) -> bool {
    log.emitting_address
        .eq_ignore_ascii_case(addresses["name_wrapper"].as_str().unwrap())
}

/// One name of the `H-01` helper batch. The scenario records the block once for the whole
/// transaction rather than on each of the four names it carries.
fn helper_level(scenario: &Value, key: &str) -> anyhow::Result<Value> {
    let mut level = scenario[key].clone();
    level["migration_block"] = scenario["migration_block"].clone();
    Ok(level)
}

fn boundaries(output: &BatchOutput) -> Vec<&NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "MigrationApplied")
        .collect()
}

fn child_boundaries(output: &BatchOutput) -> Vec<&NormalizedEvent> {
    boundaries(output)
        .into_iter()
        .filter(|event| {
            matches!(
                event.after_state["migration_path"].as_str(),
                Some("locked_child" | "emancipated_child")
            )
        })
        .collect()
}

fn assert_child_shape(
    boundary: &NormalizedEvent,
    child: &Value,
    parent: &Value,
    addresses: &Value,
) {
    let resource = &boundary.after_state["predecessor_binding"]["resource"];
    assert_eq!(boundary.consumer_visibility, "activated");
    assert_eq!(
        boundary.after_state["migration_path"],
        child["migration_path"]
    );
    assert_eq!(boundary.after_state["namehash"], child["namehash"]);
    assert_eq!(
        boundary.logical_name_id.as_deref(),
        Some(format!("ens:{}", child["namehash"].as_str().unwrap()).as_str())
    );
    // The child's ENSv1 anchor is its own wrapper node, derived from the parent's migration
    // evidence and the registered labelhash — never the `.eth` second-level selector.
    assert_eq!(resource["anchor_kind"], "wrapper_backed_child_control");
    assert_eq!(resource["contract_address"], addresses["name_wrapper"]);
    assert_eq!(resource["namehash"], child["namehash"]);
    assert_eq!(resource["wrapper_token_id"], child["namehash"]);
    assert_eq!(resource["parent_namehash"], parent["namehash"]);
    assert_eq!(resource["labelhash"], child["labelhash"]);
    // The ENSv1 side of a child ends at its cleanup, not at the registration: an emancipated
    // child's unwrap closes its wrapper binding earlier in the same transaction, so the boundary
    // records where that cleanup happened and selects the binding active immediately before it.
    assert_eq!(
        resource["selection"],
        "current_wrapper_resource_immediately_before_predecessor_cleanup"
    );
    assert_eq!(
        boundary.after_state["predecessor_binding"]["selection"],
        "active_immediately_before_predecessor_cleanup"
    );
    let cleanup = &boundary.after_state["predecessor_binding"]["predecessor_cleanup"];
    assert_eq!(cleanup["block_number"], child["migration_block"]);
    assert!(
        cleanup["log_index"].as_i64().unwrap()
            < child["v2_registration_log_index"].as_i64().unwrap(),
        "the recorded cleanup precedes the registration it backs"
    );
    assert_eq!(
        cleanup["source_event"],
        if child["migration_path"] == "locked_child" {
            "TransferSingle"
        } else {
            "NameUnwrapped"
        }
    );
    assert!(cleanup["event_identity"].as_str().is_some());
    assert_eq!(
        boundary.after_state["stored_expiry"],
        child["stored_expiry"]
    );
    assert_eq!(
        boundary.after_state["parent_migration_registry"]["address"],
        child["emitting_registry"]
    );
}

#[test]
fn locked_child_correlates_through_the_parent_migration_registry() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let output = interpret_test_batch(batch(
        scenario_logs(scenario, addresses, &["parent", "child"])?,
        &fixture,
        true,
    ))?;
    let child = child_boundaries(&output);
    assert_eq!(child.len(), 1, "one boundary per migrated child");
    assert_child_shape(child[0], &scenario["child"], &scenario["parent"], addresses);
    assert_eq!(child[0].after_state["migration_path"], "locked_child");
    // The child's own nested registry is admitted from its parent registry's deployment, so a
    // deeper level can correlate against it.
    assert!(
        output
            .migration_discovery_associations
            .iter()
            .any(|association| association
                .registry_address
                .eq_ignore_ascii_case(scenario["child"]["registry"].as_str().unwrap())),
        "the child's nested registry is admitted as migration-created"
    );
    assert_eq!(output.migration_authority_transitions.len(), 2);
    assert!(
        output
            .migration_candidate_identity_effects
            .iter()
            .any(
                |effect| effect.migration_correlation_ids == child[0].migration_correlation_ids
                    && effect.consumer_visibility == "candidate"
            )
    );
    Ok(())
}

#[test]
fn emancipated_child_correlates_without_a_nested_registry() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-02"];
    let addresses = &fixture["addresses"];
    let output = interpret_test_batch(batch(
        scenario_logs(scenario, addresses, &["parent", "child"])?,
        &fixture,
        true,
    ))?;
    let child = child_boundaries(&output);
    assert_eq!(child.len(), 1);
    assert_child_shape(child[0], &scenario["child"], &scenario["parent"], addresses);
    assert_eq!(child[0].after_state["migration_path"], "emancipated_child");
    assert_eq!(
        output
            .migration_discovery_associations
            .iter()
            .filter(|association| association
                .registry_address
                .eq_ignore_ascii_case(scenario["parent"]["registry"].as_str().unwrap()))
            .count(),
        1,
        "the detached child deploys no registry of its own"
    );
    assert_eq!(output.migration_discovery_associations.len(), 1);
    Ok(())
}

#[test]
fn self_service_child_renewal_adds_no_second_boundary() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-03"];
    let addresses = &fixture["addresses"];
    let renewal = &scenario["renewal"];
    let mut logs = scenario_logs(scenario, addresses, &["parent", "child"])?;
    logs.push(raw_at_transaction(
        super::super::v2_registry::ExpiryUpdated {
            tokenId: decimal_u256(&renewal["token_id"])?,
            newExpiry: renewal["new_expiry"].as_u64().unwrap(),
            sender: renewal["sender"].as_str().unwrap().parse()?,
        }
        .encode_log_data(),
        renewal["block"].as_i64().unwrap(),
        0,
        renewal["log_index"].as_i64().unwrap(),
        renewal["registry"].as_str().unwrap(),
    ));
    let output = interpret_test_batch(batch(logs, &fixture, true))?;
    let child = child_boundaries(&output);
    assert_eq!(
        child.len(),
        1,
        "a renewal is not a second authority boundary"
    );
    assert_child_shape(child[0], &scenario["child"], &scenario["parent"], addresses);
    // Renewal provenance for such a child is its owner, not a registrar contract.
    assert!(output.normalized_events.iter().any(|event| {
        event.event_kind == "ExpiryChanged"
            && event.after_state["sender"].as_str().is_some_and(|sender| {
                sender.eq_ignore_ascii_case(renewal["sender"].as_str().unwrap())
            })
    }));
    Ok(())
}

#[test]
fn chained_child_registries_correlate_at_unbounded_depth() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-04"];
    let addresses = &fixture["addresses"];
    let output = interpret_test_batch(batch(
        scenario_logs(scenario, addresses, &["parent", "child", "grandchild"])?,
        &fixture,
        true,
    ))?;
    let child = child_boundaries(&output);
    assert_eq!(
        child.len(),
        2,
        "each level below the second is its own child"
    );
    let third = child
        .iter()
        .find(|boundary| boundary.after_state["namehash"] == scenario["child"]["namehash"])
        .expect("3LD boundary");
    let fourth = child
        .iter()
        .find(|boundary| boundary.after_state["namehash"] == scenario["grandchild"]["namehash"])
        .expect("4LD boundary");
    assert_child_shape(third, &scenario["child"], &scenario["parent"], addresses);
    assert_child_shape(
        fourth,
        &scenario["grandchild"],
        &scenario["child"],
        addresses,
    );
    assert_ne!(
        third.migration_correlation_ids, fourth.migration_correlation_ids,
        "each child carries its own correlation identity"
    );
    Ok(())
}

#[test]
fn unmigrated_child_proves_no_boundary() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-05"];
    let addresses = &fixture["addresses"];
    assert_eq!(scenario["child"]["expected_boundary"], false);
    let output = interpret_test_batch(batch(
        scenario_logs(scenario, addresses, &["parent"])?,
        &fixture,
        true,
    ))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "a child with no ENSv2 registration has nothing to correlate"
    );
    assert_eq!(boundaries(&output).len(), 1, "only the parent migrated");
    assert!(!output.normalized_events.iter().any(|event| {
        event.logical_name_id.as_deref()
            == Some(format!("ens:{}", scenario["child"]["namehash"].as_str().unwrap()).as_str())
    }));
    // The absence has to be decided by the missing registration, not by children being
    // uncorrelatable in general: the same interpreter run over a migrated child does derive one.
    let migrated = &fixture["scenarios"]["C-01"];
    let contrast = interpret_test_batch(batch(
        scenario_logs(migrated, addresses, &["parent", "child"])?,
        &fixture,
        true,
    ))?;
    assert_eq!(
        child_boundaries(&contrast).len(),
        1,
        "the C-05 negative is only meaningful while the positive still derives a boundary"
    );
    Ok(())
}

/// The activation contract across the child catalog. Every complete child shape derives an
/// activated boundary that schedules exactly one binding write; every refused child shape derives
/// neither. A refused child can still share a batch with its migrated parent's transition.
#[test]
fn the_activation_matrix_covers_the_child_catalog() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    for (scenario, levels, activated_children) in [
        ("C-01", &["parent", "child"][..], 1),
        ("C-02", &["parent", "child"], 1),
        ("C-03", &["parent", "child"], 1),
        ("C-04", &["parent", "child", "grandchild"], 2),
        ("C-05", &["parent"], 0),
        ("C-06", &["parent", "child"], 0),
    ] {
        let mut output = interpret_test_batch(batch(
            scenario_logs(&fixture["scenarios"][scenario], addresses, levels)?,
            &fixture,
            true,
        ))?;
        let candidate_cleanups = child_boundaries(&output)
            .into_iter()
            .map(|boundary| {
                (
                    boundary.event_identity.clone(),
                    boundary.after_state["predecessor_binding"]["predecessor_cleanup"].clone(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let production = output.clone();
        super::super::super::migration::inject_activated_transition_for_test(&mut output)?;
        assert_eq!(
            output, production,
            "{scenario} test-seam activation differs byte-for-byte from production"
        );
        let children = child_boundaries(&output);
        assert_eq!(
            children.len(),
            activated_children,
            "{scenario} child boundaries"
        );
        assert!(
            children
                .iter()
                .all(|boundary| boundary.consumer_visibility == "activated"),
            "{scenario} derives activated children after activation"
        );
        for boundary in &children {
            let transitions = output
                .migration_authority_transitions
                .iter()
                .filter(|transition| transition.boundary_event_identity == boundary.event_identity)
                .collect::<Vec<_>>();
            assert_eq!(
                transitions.len(),
                1,
                "{scenario} schedules one binding write per activated child boundary"
            );
            let cleanup = candidate_cleanups
                .get(&boundary.event_identity)
                .expect("complete child boundary had a candidate cleanup");
            assert_eq!(
                boundary.after_state["predecessor_binding"]["predecessor_cleanup"], *cleanup,
                "{scenario} activation must preserve the recorded cleanup object verbatim"
            );
            assert_eq!(
                transitions[0].predecessor_selector["predecessor_cleanup"], *cleanup,
                "{scenario} transition must consume the recorded cleanup object verbatim"
            );
        }
        // Parent boundaries included: activated boundaries and scheduled writes are one to one.
        assert_eq!(
            boundaries(&output).len(),
            output.migration_authority_transitions.len(),
            "{scenario} activated boundaries and binding writes are not one to one"
        );
        assert!(
            boundaries(&output)
                .iter()
                .all(|boundary| boundary.consumer_visibility == "activated"),
            "{scenario} left a boundary candidate"
        );
        for boundary in boundaries(&output) {
            assert_eq!(
                output
                    .normalized_events
                    .iter()
                    .filter(|event| event.event_identity == boundary.event_identity)
                    .count(),
                1,
                "{scenario} kept candidate and activated copies of one boundary"
            );
            assert_eq!(
                output
                    .migration_authority_transitions
                    .iter()
                    .filter(|transition| {
                        transition.boundary_event_identity == boundary.event_identity
                    })
                    .count(),
                1,
                "{scenario} boundary has no one-to-one transition"
            );
        }
        for transition in &output.migration_authority_transitions {
            assert_eq!(
                boundaries(&output)
                    .into_iter()
                    .filter(|boundary| {
                        boundary.event_identity == transition.boundary_event_identity
                    })
                    .count(),
                1,
                "{scenario} transition has no one-to-one boundary"
            );
        }
    }
    Ok(())
}

#[test]
fn a_registry_that_is_not_migration_created_correlates_no_child() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    // Everything the positive case has except the factory log that proves the emitting registry
    // was created by a migration. The registry is still announced and still indexable, so the
    // child's registration is interpreted — it simply proves no migration.
    let parent_factory = scenario["parent"]["factory_log_index"].as_i64().unwrap();
    let parent_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let logs = scenario_logs(scenario, addresses, &["parent", "child"])?
        .into_iter()
        .filter(|log| !(log.block_number == parent_block && log.log_index == parent_factory))
        .collect::<Vec<_>>();
    let output = interpret_test_batch(batch(logs, &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "incomplete parent discovery leaves the emitter an ordinary registry"
    );
    let registration = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationGranted"
                && event.after_state["label"] == scenario["child"]["label"]
                && event.raw_fact_ref["emitting_address"]
                    .as_str()
                    .is_some_and(|address| {
                        address.eq_ignore_ascii_case(
                            scenario["child"]["emitting_registry"].as_str().unwrap(),
                        )
                    })
        })
        .expect("the child registration is still interpreted as an ordinary registry fact");
    assert_eq!(registration.consumer_visibility, "activated");
    Ok(())
}

#[test]
fn a_child_registered_before_its_parent_registry_exists_proves_nothing() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["H-01"];
    let addresses = &fixture["addresses"];
    let mut locked = helper_level(scenario, "locked")?;
    let child = helper_level(scenario, "child")?;
    // Move the parent registry's factory log after the child's registration, leaving the rest of
    // the transaction untouched. Discovery ordering is what decides this, not tx membership.
    locked["factory_log_index"] = json!(child["v2_registration_log_index"].as_i64().unwrap() + 20);
    let mut logs = level_logs(&locked, addresses)?;
    logs.extend(level_logs(&child, addresses)?);
    for log in &mut logs {
        if log.block_number == scenario["migration_block"].as_i64().unwrap() {
            log.transaction_hash = "helper-batch".to_owned();
        }
    }
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "a registry cannot be a migration parent before its own creation position"
    );

    // The same with the parent registry's announcement moved past the child too, so the whole
    // creation — announcement and factory log — follows the registration it would have to precede.
    locked["registry_created_log_index"] =
        json!(child["v2_registration_log_index"].as_i64().unwrap() + 16);
    let mut logs = level_logs(&locked, addresses)?;
    logs.extend(level_logs(&child, addresses)?);
    for log in &mut logs {
        if log.block_number == scenario["migration_block"].as_i64().unwrap() {
            log.transaction_hash = "helper-batch".to_owned();
        }
    }
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "creation position, not transaction membership, decides what a parent can back"
    );
    Ok(())
}

#[test]
fn parent_controlled_clobber_is_not_a_migration_boundary() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-06"];
    let addresses = &fixture["addresses"];
    assert_eq!(scenario["child"]["expected_boundary"], false);
    let output = interpret_test_batch(batch(
        scenario_logs(scenario, addresses, &["parent", "child"])?,
        &fixture,
        true,
    ))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "a parent-owner registration is an authority proof only, never a migration boundary"
    );
    let registration = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationGranted"
                && event.after_state["label"] == scenario["child"]["label"]
                && event.after_state["sender"].as_str().is_some_and(|sender| {
                    sender.eq_ignore_ascii_case(
                        scenario["child"]["registration_sender"].as_str().unwrap(),
                    )
                })
        })
        .expect("the clobber registration is still an ordinary registry fact");
    assert_eq!(registration.consumer_visibility, "activated");
    assert!(registration.migration_correlation_ids.is_empty());
    assert!(
        output
            .migration_candidate_identity_effects
            .iter()
            .all(|effect| effect.correlation_kind != "authority_transition"
                || effect.proposed_effect["namehash"] != scenario["child"]["namehash"])
    );
    Ok(())
}

#[test]
fn mixed_helper_batch_attributes_children_per_log() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["H-01"];
    let addresses = &fixture["addresses"];
    let locked = helper_level(scenario, "locked")?;
    let child = helper_level(scenario, "child")?;
    // All four names of the helper batch interleave in one transaction. The two second-level
    // groups the helper migrates first are co-located noise for child attribution.
    let mut logs = level_logs(&helper_level(scenario, "unwrapped")?, addresses)?;
    logs.extend(level_logs(&helper_level(scenario, "unlocked")?, addresses)?);
    logs.extend(level_logs(&locked, addresses)?);
    logs.extend(level_logs(&child, addresses)?);
    for log in &mut logs {
        if log.block_number == scenario["migration_block"].as_i64().unwrap() {
            log.transaction_hash = "helper-batch".to_owned();
        }
    }
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    let child_boundary = child_boundaries(&output);
    assert_eq!(
        child_boundary.len(),
        1,
        "only the locked group's child is a child migration"
    );
    assert_eq!(child_boundary[0].after_state["namehash"], child["namehash"]);
    assert_eq!(
        child_boundary[0].after_state["parent_migration_registry"]["namehash"],
        locked["namehash"]
    );
    // The registry that emits the child's registration did not exist before an earlier log of the
    // same transaction, so correlation must be intra-transaction ordered.
    let parent_registry = locked["registry"].as_str().unwrap();
    assert!(
        child_boundary[0].after_state["parent_migration_registry"]["address"]
            .as_str()
            .is_some_and(|address| address.eq_ignore_ascii_case(parent_registry))
    );
    let paths = boundaries(&output)
        .iter()
        .filter_map(|event| event.after_state["migration_path"].as_str())
        .collect::<Vec<_>>();
    assert!(
        paths.contains(&"locked_wrapped"),
        "the parent second-level migration keeps its own boundary in the same transaction"
    );
    Ok(())
}

#[test]
fn unmigrated_parent_leaves_no_child_evidence() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["H-04"];
    let addresses = &fixture["addresses"];
    assert_eq!(scenario["surviving_v2_registrations"], 0);
    // The helper reverts the whole call, so a child whose parent never migrated leaves only the
    // parent's own pre-migration facts. Drive that from the C-01 stream with the parent's
    // migration removed: the child's registration cannot exist without the receiver that emits it.
    let source = &fixture["scenarios"]["C-01"];
    let child_block = source["child"]["migration_block"].as_i64().unwrap();
    let logs = scenario_logs(source, addresses, &["parent", "child"])?
        .into_iter()
        .filter(|log| log.block_number == child_block)
        .collect::<Vec<_>>();
    assert!(
        !logs.is_empty(),
        "the reverted child's own logs are the input"
    );
    let output = interpret_test_batch(batch(logs, &fixture, true))?;
    assert!(boundaries(&output).is_empty(), "no parent, no boundary");
    assert!(child_boundaries(&output).is_empty());
    assert!(output.migration_candidate_identity_effects.is_empty());
    Ok(())
}

#[test]
fn a_factory_log_alone_admits_no_migration_registry() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let mut logs = level_logs(&scenario["parent"], addresses)?;
    // The child's factory log lands in the parent's own transaction, which does carry an
    // announcement — for a different registry. Admission follows the announcement of the named
    // registry, never the factory log that happens to share its transaction.
    let child = &scenario["child"];
    logs.push(raw_at_transaction(
        ProxyDeployed {
            sender: child["factory_sender"].as_str().unwrap().parse()?,
            proxyAddress: child["registry"].as_str().unwrap().parse()?,
            salt: decimal_u256(&child["factory_salt"])?,
            implementation: Address::from([0x44; 20]),
        }
        .encode_log_data(),
        scenario["parent"]["migration_block"].as_i64().unwrap(),
        0,
        scenario["parent"]["factory_log_index"].as_i64().unwrap() + 12,
        addresses["factory"].as_str().unwrap(),
    ));
    let output = interpret_test_batch(batch(logs, &fixture, true))?;
    assert!(child_boundaries(&output).is_empty());
    assert!(
        !output
            .migration_discovery_associations
            .iter()
            .any(|association| association
                .registry_address
                .eq_ignore_ascii_case(child["registry"].as_str().unwrap())),
        "a factory log is audit evidence, not registry admission"
    );
    Ok(())
}

/// Re-admits what a finished batch discovered, the way `crates/interpret/src/load.rs` does for the
/// next batch: the ordinary announcement edge and its candidate migration association.
fn carry_admissions(output: &BatchOutput, into: &mut Vec<AddressAdmissionInput>) {
    for association in &output.migration_discovery_associations {
        let edge = output.discovery_edges.iter().find(|edge| {
            edge.edge_kind == "registry_announcement"
                && edge.to_contract_instance_id == association.registry_contract_instance_id
        });
        let Some(edge) = edge else { continue };
        into.push(AddressAdmissionInput {
            address: association.registry_address.clone(),
            contract_instance_id: association.registry_contract_instance_id,
            source_manifest_id: Some(association.source_manifest_id),
            role: None,
            discovery_edge_kind: Some("registry_announcement".to_owned()),
            discovery_from_contract_instance_id: Some(edge.from_contract_instance_id),
            discovery_observation_key: Some(edge.observation_key.clone()),
            active_from_block: Some(edge.active_from_block_number),
            active_to_block: None,
        });
        into.push(AddressAdmissionInput {
            address: association.registry_address.clone(),
            contract_instance_id: association.registry_contract_instance_id,
            source_manifest_id: Some(association.source_manifest_id),
            role: None,
            discovery_edge_kind: Some("migration_registry_creation".to_owned()),
            discovery_from_contract_instance_id: Some(edge.from_contract_instance_id),
            discovery_observation_key: Some(
                json!({
                    "id": association.migration_correlation_id,
                    "evidence": association.evidence_refs,
                })
                .to_string(),
            ),
            active_from_block: Some(association.block_number),
            active_to_block: None,
        });
    }
}

fn owned(boundaries: Vec<&NormalizedEvent>) -> Vec<NormalizedEvent> {
    boundaries.into_iter().cloned().collect()
}

#[test]
fn child_boundaries_converge_across_restart_splits() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-04"];
    let addresses = &fixture["addresses"];
    let levels = ["parent", "child", "grandchild"];
    let full = interpret_test_batch(batch(
        scenario_logs(scenario, addresses, &levels)?,
        &fixture,
        true,
    ))?;
    let expected = owned(child_boundaries(&full));
    assert_eq!(expected.len(), 2);
    for splits in [vec![1_usize, 3], vec![2, 3], vec![1, 2, 3]] {
        let mut carried = Vec::new();
        let mut session = None;
        let mut seen = Vec::new();
        let mut start = 0;
        for end in splits.iter().copied() {
            let mut input = batch(
                scenario_logs(scenario, addresses, &levels[start..end])?,
                &fixture,
                true,
            );
            input.admissions.extend(carried.clone());
            let (output, next) = interpret_test_batch_incremental(input, session)?;
            carry_admissions(&output, &mut carried);
            session = Some(next);
            seen.extend(owned(child_boundaries(&output)));
            start = end;
        }
        assert_eq!(
            seen, expected,
            "a restart split must derive the same child boundaries, correlation IDs, and evidence"
        );
    }
    Ok(())
}

#[test]
fn removing_child_or_registry_evidence_returns_candidate_state_deterministically()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let all = scenario_logs(scenario, addresses, &["parent", "child"])?;
    let full = interpret_test_batch(batch(all.clone(), &fixture, true))?;
    assert_eq!(child_boundaries(&full).len(), 1);

    let parent_factory = scenario["parent"]["factory_log_index"].as_i64().unwrap();
    let parent_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let without_registry = all
        .iter()
        .filter(|log| !(log.block_number == parent_block && log.log_index == parent_factory))
        .cloned()
        .collect::<Vec<_>>();
    let reorged = interpret_test_batch(batch(without_registry, &fixture, true))?;
    assert!(
        child_boundaries(&reorged).is_empty(),
        "without the parent's migration evidence the emitting registry is an ordinary registry"
    );

    let child_block = scenario["child"]["migration_block"].as_i64().unwrap();
    let without_child = all
        .iter()
        .filter(|log| {
            log.block_number != child_block
                || log.log_index
                    < scenario["child"]["v2_registration_log_index"]
                        .as_i64()
                        .unwrap()
        })
        .cloned()
        .collect::<Vec<_>>();
    let partial = interpret_test_batch(batch(without_child, &fixture, true))?;
    assert!(child_boundaries(&partial).is_empty());
    assert_eq!(
        boundaries(&partial).len(),
        1,
        "the parent boundary survives"
    );

    let neither = all
        .iter()
        .filter(|log| log.block_number == parent_block && log.log_index != parent_factory)
        .cloned()
        .collect::<Vec<_>>();
    let bare = interpret_test_batch(batch(neither, &fixture, true))?;
    assert!(boundaries(&bare).is_empty());

    let replayed = interpret_test_batch(batch(all, &fixture, true))?;
    assert_eq!(
        replayed.normalized_events, full.normalized_events,
        "re-adding the reorged evidence returns the same candidate state"
    );
    assert_eq!(
        replayed.migration_candidate_identity_effects,
        full.migration_candidate_identity_effects
    );
    Ok(())
}

#[test]
fn two_children_of_one_parent_in_one_transaction_keep_separate_identities() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["H-01"];
    let addresses = &fixture["addresses"];
    let locked = helper_level(scenario, "locked")?;
    let first = helper_level(scenario, "child")?;
    let parent_registry = locked["registry"].as_str().unwrap().to_owned();
    let mut logs = level_logs(&locked, addresses)?;
    logs.extend(level_logs(&first, addresses)?);
    // A second locked child of the same parent, later in the same transaction, built as a full
    // level so it carries its own registry evidence and its own log positions — two children never
    // share a position, so the test survives any future position-keyed identity or dedup rule.
    let second_label = "two";
    let second_node = namehash_under(&locked["namehash"], second_label)?;
    let mut second_labels = first["labels"]
        .as_array()
        .cloned()
        .expect("ported label path");
    second_labels[0] = json!(second_label);
    let base = first["v2_registration_log_index"].as_i64().unwrap() + 10;
    let second = json!({
        "label": second_label,
        "labelhash": format!("{:#x}", keccak256(second_label.as_bytes())),
        "namehash": second_node,
        "labels": second_labels,
        "v2_token_id": versioned_token(second_label, 0).to_string(),
        "stored_expiry": first["stored_expiry"],
        "registration_sender": parent_registry,
        "registration_owner": first["registration_owner"],
        "emitting_registry": parent_registry,
        "migration_block": scenario["migration_block"],
        "v2_registration_log_index": base,
        "registry": "0x00000000000000000000000000000000000002ee",
        "factory_salt": U256::from_be_bytes(second_node.parse::<B256>()?.0).to_string(),
        "factory_sender": parent_registry,
        "factory_log_index": base - 2,
        "registry_created_log_index": base - 4,
        "wrap_block": first["wrap_block"],
        "wrap_log_index": 20,
        "cleanup_log_index": 20,
        "wrap_fuses": first["wrap_fuses"],
        "wrap_expiry": first["wrap_expiry"],
        "wrapped_owner": first["wrapped_owner"],
        "migration_path": "locked_child",
    });
    logs.extend(level_logs(&second, addresses)?);
    for log in &mut logs {
        if log.block_number == scenario["migration_block"].as_i64().unwrap() {
            log.transaction_hash = "helper-batch".to_owned();
        }
    }
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    let children = child_boundaries(&output);
    assert_eq!(
        children.len(),
        2,
        "one boundary per child, not per transaction"
    );
    let names = children
        .iter()
        .filter_map(|boundary| boundary.after_state["namehash"].as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), 2, "each child keeps its own ENSv1 identity");
    assert!(names.contains(first["namehash"].as_str().unwrap()));
    let ids = children
        .iter()
        .flat_map(|boundary| boundary.migration_correlation_ids.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        ids.len(),
        2,
        "correlation identity is per child evidence chain, never per transaction membership"
    );
    let labels = children
        .iter()
        .filter_map(|boundary| {
            boundary.after_state["predecessor_binding"]["resource"]["labelhash"].as_str()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(labels.len(), 2);
    Ok(())
}

/// A migration boundary claims that ENSv1 authority ended. The ENSv2 self-claim alone cannot show
/// that: only the child's own ENSv1 cleanup — its wrapper token parked in the Graveyard, or its
/// node unwrapped into the Graveyard — proves a predecessor existed and was retired. Without it
/// the registration is an ordinary ENSv2 fact and, under slice 2C, an authority proof.
#[test]
fn a_self_claim_without_v1_cleanup_derives_no_boundary() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let logs = scenario_logs(scenario, addresses, &["parent", "child"])?
        .into_iter()
        .filter(|log| !is_v1_cleanup(log, addresses))
        .collect::<Vec<_>>();
    let output = interpret_test_batch(batch(logs, &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "a self-claim with no ENSv1 predecessor cleanup proves no migration"
    );
    Ok(())
}

/// The receiver takes custody of the wrapper token before parking it, so a child's cleanup is two
/// hops. Only the second reaches the Graveyard, and only the second ends ENSv1 control: custody
/// moving to the receiver alone leaves the name live under ENSv1
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58).
#[test]
fn a_cleanup_that_stops_short_of_the_graveyard_derives_no_boundary() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let logs = scenario_logs(scenario, addresses, &["parent", "child"])?;
    let block = scenario["child"]["migration_block"].as_i64().unwrap();
    let graveyard_hop = logs
        .iter()
        .filter(|log| is_v1_cleanup(log, addresses) && log.block_number == block)
        .map(|log| log.log_index)
        .max()
        .expect("the child's cleanup hops");
    let logs = logs
        .into_iter()
        .filter(|log| !(is_v1_cleanup(log, addresses) && log.log_index == graveyard_hop))
        .collect::<Vec<_>>();
    let output = interpret_test_batch(batch(logs, &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "custody moving to the receiver is not the end of ENSv1 authority"
    );
    Ok(())
}

/// The ENSv1 unwrap of an emancipated child closes its wrapper binding at the unwrap log, which
/// precedes the ENSv2 registration in the same transaction, so no ENSv1 binding for that name is
/// open at the boundary's own position. That is why a child boundary records where its ENSv1
/// cleanup happened: the binding a later activation has to close is the one active immediately
/// before the cleanup. The locked shape differs — parking the wrapper token moves its owner without
/// closing anything — and both are asserted here so the asymmetry stays visible.
#[test]
fn an_emancipated_child_closes_its_ensv1_binding_before_the_registration() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let addresses = &fixture["addresses"];
    for (name, closes) in [("C-02", true), ("C-01", false)] {
        let scenario = &fixture["scenarios"][name];
        let child = &scenario["child"];
        let output = interpret_test_batch(batch(
            scenario_logs(scenario, addresses, &["parent", "child"])?,
            &fixture,
            true,
        ))?;
        let logical_name_id = format!("ens:{}", child["namehash"].as_str().unwrap());
        let registration = child["v2_registration_log_index"].as_i64().unwrap();
        let closed = output
            .binding_closures
            .iter()
            .filter(|closure| {
                closure.logical_name_id == logical_name_id
                    && closure.authority_arm == "ens_v1"
                    && closure.block_number == child["migration_block"].as_i64().unwrap()
            })
            .map(|closure| closure.log_index)
            .collect::<Vec<_>>();
        assert_eq!(
            closed.iter().any(|index| *index < registration),
            closes,
            "{name}: ENSv1 binding closed before the registration"
        );
        assert!(
            !output.surface_bindings.iter().any(|binding| {
                binding.logical_name_id == logical_name_id
                    && binding.authority_arm == "ens_v1"
                    && binding.block_number == child["migration_block"].as_i64().unwrap()
            }),
            "{name}: the cleanup opens no replacement ENSv1 binding at the boundary"
        );
    }
    Ok(())
}

/// The child's ENSv1 identity is derived from its parent registry's CREATE2 salt, while the ENSv2
/// name the registration carries comes from the registry topology. A registry whose salt names one
/// name and whose announcement places it under another leaves those two disagreeing, and no
/// boundary is provable: the evidence would retire an ENSv1 name the registration never claimed.
#[test]
fn a_parent_salt_that_disagrees_with_the_registry_topology_derives_no_boundary()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let elsewhere = &fixture["scenarios"]["C-02"]["parent"]["namehash"];
    let salt = |node: &str| -> anyhow::Result<String> {
        Ok(U256::from_be_bytes(node.parse::<B256>()?.0).to_string())
    };
    let mut parent = scenario["parent"].clone();
    parent["factory_salt"] = json!(salt(elsewhere.as_str().unwrap())?);
    // The child's own ENSv1 side and nested registry follow the name the parent's salt implies, so
    // every conjunct except the topology agreement holds.
    let derived = namehash_under(elsewhere, scenario["child"]["label"].as_str().unwrap())?;
    let mut child = scenario["child"].clone();
    child["factory_salt"] = json!(salt(&derived)?);
    child["namehash"] = json!(derived);
    child["labels"] = json!([
        scenario["child"]["label"].as_str().unwrap(),
        fixture["scenarios"]["C-02"]["parent"]["label"]
            .as_str()
            .unwrap(),
        "eth"
    ]);
    let mut logs = level_logs(&parent, addresses)?;
    logs.extend(level_logs(&child, addresses)?);
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "a child's ENSv1 identity and its ENSv2 name have to be the same name"
    );
    Ok(())
}

/// The emancipated branch unwraps into the Graveyard specifically; an unwrap that hands the node
/// back to its owner is an ordinary ENSv1 act that leaves control where it was
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64).
#[test]
fn an_unwrap_that_misses_the_graveyard_derives_no_boundary() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-02"];
    let addresses = &fixture["addresses"];
    let child = &scenario["child"];
    let mut logs = level_logs(&scenario["parent"], addresses)?;
    logs.extend(
        level_logs(child, addresses)?
            .into_iter()
            .filter(|log| !is_v1_cleanup(log, addresses)),
    );
    logs.extend(
        v1_predecessor_logs(child, addresses)?
            .into_iter()
            .filter(|log| log.block_number != child["migration_block"].as_i64().unwrap()),
    );
    logs.push(raw_at_transaction(
        super::super::NameUnwrapped {
            node: child["namehash"].as_str().unwrap().parse()?,
            owner: child["wrapped_owner"].as_str().unwrap().parse()?,
        }
        .encode_log_data(),
        child["migration_block"].as_i64().unwrap(),
        0,
        1,
        addresses["name_wrapper"].as_str().unwrap(),
    ));
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "an unwrap back to the owner retires nothing"
    );
    Ok(())
}

/// A receiver migrates several children in one transaction, so a cleanup sharing the registration's
/// transaction proves nothing on its own: the evidence has to be the registered child's own node.
#[test]
fn a_sibling_cleanup_in_the_same_transaction_proves_nothing() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let child = &scenario["child"];
    let sibling_label = "sibling";
    let mut sibling_labels = child["labels"].as_array().cloned().expect("ported labels");
    sibling_labels[0] = json!(sibling_label);
    let mut sibling = child.clone();
    let object = sibling.as_object_mut().expect("fixture level");
    object.insert("labels".to_owned(), Value::Array(sibling_labels));
    object.insert(
        "namehash".to_owned(),
        json!(namehash_under(
            &scenario["parent"]["namehash"],
            sibling_label
        )?),
    );
    object.insert("wrap_log_index".to_owned(), json!(6));
    object.insert("cleanup_log_index".to_owned(), json!(6));
    // The registered child keeps its registry and its registration; only its own cleanup is gone,
    // replaced by a sibling's complete one in the same transaction.
    let mut logs = scenario_logs(scenario, addresses, &["parent", "child"])?
        .into_iter()
        .filter(|log| !is_v1_cleanup(log, addresses))
        .collect::<Vec<_>>();
    logs.extend(v1_predecessor_logs(&sibling, addresses)?);
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "a cleanup binds to the node it retires, not to the transaction it sits in"
    );
    Ok(())
}

/// The receiver performs the child's cleanup and its ENSv2 registration in one call, so a cleanup
/// in a neighbouring transaction is a different act — an ordinary ENSv1 wind-down that happens to
/// share a block — and cannot back the registration
/// (upstream: .refs/ens_v2/contracts/src/migration/AbstractWrapperReceiver.sol:L167 @ ens_v2@a971bd64).
#[test]
fn a_cleanup_in_an_adjacent_transaction_proves_nothing() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let block = scenario["child"]["migration_block"].as_i64().unwrap();
    let mut logs = scenario_logs(scenario, addresses, &["parent", "child"])?;
    // The cleanup keeps its position ahead of the registration and only the transaction changes,
    // so this pins transaction membership alone rather than ordering.
    let mut moved = 0;
    for log in &mut logs {
        if log.block_number == block && !is_v1_cleanup(log, addresses) {
            log.transaction_index = 1;
            log.transaction_hash = format!("transaction-{block}-1");
            moved += 1;
        }
    }
    assert!(
        moved > 0,
        "the child's ENSv2 side moves to its own transaction"
    );
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "the cleanup and the registration are one call or they are unrelated"
    );
    Ok(())
}

/// The receiver retires the ENSv1 name before it registers the successor — the wrapper token is
/// parked, or the node unwrapped, and only then is the label injected into the registry
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58)
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L168 @ ens_v2_sepolia_20260629@ccaeb58)
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L180 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L188 @ ens_v2@a971bd64).
/// A cleanup logged after the registration is therefore not that sequence, whatever else it shows.
#[test]
fn a_cleanup_logged_after_the_registration_derives_no_boundary() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let mut child = scenario["child"].clone();
    let after = child["v2_registration_log_index"].as_i64().unwrap() + 6;
    child["cleanup_log_index"] = json!(after);
    let mut logs = level_logs(&scenario["parent"], addresses)?;
    logs.extend(level_logs(&child, addresses)?);
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "the successor cannot be registered before the predecessor is retired"
    );
    Ok(())
}

/// `C-06` with the cleanup it never had: even a complete ENSv1 wind-down cannot make a
/// parent-owner-controlled registration a migration. Upstream reverts that call, and the sender the
/// registry reports is the only thing separating the two
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L190 @ ens_v2@a971bd64).
#[test]
fn a_parent_controlled_registration_with_a_full_cleanup_is_still_not_a_migration()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-06"];
    let addresses = &fixture["addresses"];
    let child = &scenario["child"];
    let mut retired = child.clone();
    let object = retired.as_object_mut().expect("fixture level");
    object.insert(
        "labels".to_owned(),
        json!(["sub", scenario["parent"]["label"].as_str().unwrap(), "eth"]),
    );
    object.insert("wrap_block".to_owned(), json!(228));
    object.insert("wrap_fuses".to_owned(), json!(65_536));
    object.insert("wrap_expiry".to_owned(), child["stored_expiry"].clone());
    object.insert(
        "wrapped_owner".to_owned(),
        child["registration_owner"].clone(),
    );
    object.insert("migration_path".to_owned(), json!("emancipated_child"));
    // The cleanup precedes the registration, as a real receiver's would, so this pins the sender
    // conjunct rather than the ordering one.
    object.insert("cleanup_log_index".to_owned(), json!(10));
    object.insert("v2_registration_log_index".to_owned(), json!(20));
    let mut logs = level_logs(&scenario["parent"], addresses)?;
    logs.extend(level_logs(&retired, addresses)?);
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    assert!(
        child_boundaries(&output).is_empty(),
        "only the receiver registering the child into itself is migration evidence"
    );
    Ok(())
}

/// A child whose parent registry was proven in an earlier batch, and whose own ENSv1 wrap sits in
/// an earlier batch still. Both halves have to survive the restart: the parent's admission is
/// recovered from stored discovery evidence, and the wrapper state that makes the cleanup readable
/// comes back with the interpreter session. The three-batch shape separates those two halves —
/// wrap, then parent admission, then the child's migration — so a session-state regression that
/// only bites when they are batched apart cannot hide behind the parent's own batch. Both child
/// shapes are split, because the locked branch reads its cleanup out of retained wrapper state
/// while the emancipated branch's unwrap carries its node in the log.
#[test]
fn a_child_converges_when_its_parent_and_wrap_are_in_earlier_batches() -> anyhow::Result<()> {
    for name in ["C-01", "C-02"] {
        let fixture = fixture()?;
        let scenario = &fixture["scenarios"][name];
        let parent = scenario["parent"]["migration_block"].as_i64().unwrap();
        let child = scenario["child"]["migration_block"].as_i64().unwrap();
        assert_split_convergence(name, &[child])?;
        assert_split_convergence(name, &[parent, child])?;
    }
    Ok(())
}

fn assert_split_convergence(name: &str, splits: &[i64]) -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"][name];
    let addresses = &fixture["addresses"];
    let all = scenario_logs(scenario, addresses, &["parent", "child"])?;
    let expected = owned(child_boundaries(&interpret_test_batch(batch(
        all.clone(),
        &fixture,
        true,
    ))?));
    assert_eq!(expected.len(), 1);
    let bounds = std::iter::once(i64::MIN)
        .chain(splits.iter().copied())
        .zip(splits.iter().copied().chain(std::iter::once(i64::MAX)))
        .collect::<Vec<_>>();
    let mut carried = Vec::new();
    let mut session = None;
    let mut seen = Vec::new();
    for (start, end) in bounds {
        let mut input = batch(
            all.iter()
                .filter(|log| log.block_number >= start && log.block_number < end)
                .cloned()
                .collect(),
            &fixture,
            true,
        );
        input.admissions.extend(carried.clone());
        let (output, next) = interpret_test_batch_incremental(input, session)?;
        carry_admissions(&output, &mut carried);
        session = Some(next);
        seen.extend(owned(child_boundaries(&output)));
    }
    assert_eq!(
        seen, expected,
        "{name} split at {splits:?} must derive the same boundary, ID, and evidence"
    );
    Ok(())
}

fn namehash_under(parent: &Value, label: &str) -> anyhow::Result<String> {
    let mut input = [0_u8; 64];
    input[..32].copy_from_slice(parent.as_str().unwrap().parse::<B256>()?.as_slice());
    input[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
    Ok(format!("{:#x}", keccak256(input)))
}

/// The same level with its ENSv2 registry evidence removed: no factory log, no announcement, no
/// subregistry link. Everything else, including the ENSv1 cleanup, is untouched.
fn without_registry_evidence(level: &Value) -> Value {
    let mut level = level.clone();
    let object = level.as_object_mut().expect("fixture level");
    for key in [
        "registry",
        "factory_salt",
        "factory_sender",
        "factory_log_index",
        "registry_created_log_index",
        "subregistry_log_index",
    ] {
        object.remove(key);
    }
    level
}

/// Upstream pairs each child branch with its own receiver shape: the locked branch parks the
/// wrapper token in the Graveyard *and* deploys the child's registry, while the emancipated branch
/// unwraps into the Graveyard and deploys nothing. There is no third branch.
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L144 @ ens_v2_sepolia_20260629@ccaeb58)
/// (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/migration/LockedWrapperReceiver.sol:L149 @ ens_v2_sepolia_20260629@ccaeb58)
/// (upstream: .refs/ens_v2/contracts/src/migration/LockedWrapperReceiver.sol:L190 @ ens_v2@a971bd64)
/// So a parked wrapper token with no registry evidence is an incomplete chain, not an emancipated
/// child — classifying it by registry presence alone would silently relabel it.
#[test]
fn locked_cleanup_without_registry_evidence_derives_nothing() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let mut logs = level_logs(&scenario["parent"], addresses)?;
    logs.extend(level_logs(
        &without_registry_evidence(&scenario["child"]),
        addresses,
    )?);
    let output = interpret_test_batch(batch(ordered(logs), &fixture, true))?;
    let derived = child_boundaries(&output);
    assert!(
        derived.is_empty(),
        "a parked wrapper token without the child's registry is an incomplete chain, but derived {:?}",
        derived
            .iter()
            .map(|boundary| boundary.after_state["migration_path"].clone())
            .collect::<Vec<_>>()
    );
    Ok(())
}
