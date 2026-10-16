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

/// The open-ENSv1 WrapperRegistry case through Interpret (Pro review of PR 953, questions 5 and 8).
/// `C-06`'s parent owner registers the child directly into the parent's WrapperRegistry; here the
/// child is also wrapped in the ENSv1 NameWrapper with `PARENT_CANNOT_CONTROL` before that, and the
/// WrapperRegistry later unregisters it. Interpret opens the child's ENSv2 binding at its
/// TokenResource log, closes it at the unregister log with a release on the same resource, and
/// leaves the ENSv1 wrapper binding open, since nothing on ENSv1 moved. Project's handling of this
/// shape (name authority and the children omission) is asserted on normalized rows in
/// `crates/project/tests/issue_503_children.rs`.
/// (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L293-L307 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L195-L207 @ ens_v2@a971bd64)
#[test]
fn a_released_parent_owner_child_keeps_its_open_v1_wrapper_binding() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-06"];
    let addresses = &fixture["addresses"];
    let child = &scenario["child"];
    let child_logical = format!("ens:{}", child["namehash"].as_str().unwrap());
    let registry = child["emitting_registry"].as_str().unwrap();
    let owner: Address = child["registration_owner"].as_str().unwrap().parse()?;
    let token = decimal_u256(&child["v2_token_id"])?;
    let mut logs = scenario_logs(scenario, addresses, &["parent", "child"])?;
    let mut encoded = Vec::new();
    for label in ["sub", "c06", "eth"] {
        encoded.push(u8::try_from(label.len())?);
        encoded.extend_from_slice(label.as_bytes());
    }
    encoded.push(0);
    // PARENT_CANNOT_CONTROL, the fuse the children projection's locked-parent reachability reads.
    logs.push(raw_at_transaction(
        super::super::NameWrapped {
            node: child["namehash"].as_str().unwrap().parse()?,
            name: encoded.into(),
            owner,
            fuses: 65_536,
            expiry: 4_000_000_000,
        }
        .encode_log_data(),
        230,
        0,
        0,
        addresses["name_wrapper"].as_str().unwrap(),
    ));
    logs.push(raw_at_transaction(
        super::super::v2_registry::LabelUnregistered {
            tokenId: token,
            sender: owner,
        }
        .encode_log_data(),
        238,
        0,
        0,
        registry,
    ));
    let mut input = batch(ordered(logs), &fixture, true);
    let manifest = input
        .manifests
        .iter_mut()
        .find(|manifest| manifest.manifest_id == REGISTRY_MANIFEST_ID)
        .expect("registry manifest");
    let mut payload: Value = serde_json::from_str(&manifest.payload_json)?;
    payload["abi"]["events"]
        .as_array_mut()
        .expect("ABI events")
        .push(json!({
            "name": "LabelUnregistered",
            "fragment": "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)",
            "emitter_roles": ["registry"],
            "normalized_events": ["RegistrationReleased"],
        }));
    manifest.payload_json = serde_json::to_string(&payload)?;
    let output = interpret_test_batch(input)?;
    assert!(
        child_boundaries(&output).is_empty(),
        "a parent-owner registration is never a migration"
    );
    let registration = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationGranted"
                && event.logical_name_id.as_deref() == Some(child_logical.as_str())
                && event.resource_id.is_some()
        })
        .expect("the child registration on its TokenResource resource");
    assert_eq!(registration.consumer_visibility, "activated");
    let resource = registration.resource_id.expect("the registration resource");
    let v2_binding = output
        .surface_bindings
        .iter()
        .find(|binding| {
            binding.logical_name_id == child_logical && binding.authority_arm == "ens_v2"
        })
        .expect("the child's ENSv2 binding");
    assert_eq!(v2_binding.resource_id, resource);
    assert_eq!(
        (
            v2_binding.block_number,
            v2_binding.provenance["log_index"].as_i64()
        ),
        (
            237,
            child["v2_registration_log_index"]
                .as_i64()
                .map(|log| log + 2)
        ),
        "the ENSv2 binding opens at the TokenResource log"
    );
    let release = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationReleased"
                && event.resource_id == Some(resource)
                && event.after_state["source_event"] == "LabelUnregistered"
        })
        .expect("the release on the child resource");
    assert_eq!(
        (
            release.block_number,
            release.transaction_index,
            release.log_index
        ),
        (Some(238), Some(0), Some(0))
    );
    let v1_binding = output
        .surface_bindings
        .iter()
        .find(|binding| {
            binding.logical_name_id == child_logical && binding.authority_arm == "ens_v1"
        })
        .expect("the wrap opened the child's ENSv1 binding");
    let closures = output
        .binding_closures
        .iter()
        .filter(|closure| closure.logical_name_id == child_logical)
        .collect::<Vec<_>>();
    assert!(
        closures
            .iter()
            .filter(|closure| closure.authority_arm == "ens_v1")
            .all(|closure| closure.except_surface_binding_id == Some(v1_binding.surface_binding_id)),
        "no closure reaches the ENSv1 wrapper binding"
    );
    let last_v2 = closures
        .iter()
        .filter(|closure| closure.authority_arm == "ens_v2")
        .max_by_key(|closure| {
            (
                closure.block_number,
                closure.transaction_index,
                closure.log_index,
            )
        })
        .expect("an ENSv2 closure");
    assert_eq!(
        (
            last_v2.block_number,
            last_v2.transaction_index,
            last_v2.log_index,
            last_v2.except_surface_binding_id
        ),
        (238, 0, 0, None),
        "the unregister closes the ENSv2 binding"
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

const GATE_V1_REGISTRY: &str = "0x0000000000000000000000000000000000000099";
const GATE_RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const GATE_OWNER: &str = "0x00000000000000000000000000000000000000b1";

fn gate_batch(logs: Vec<RawLogInput>, fixture: &Value) -> BatchInput {
    let mut input = batch(ordered(logs), fixture, true);
    input.manifests.push(manifest_with_events(
        V1_REGISTRY_MANIFEST_ID,
        "ens",
        "ens_v1_registry_l1",
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry"],
                &["SubregistryChanged", "AuthorityTransferred"],
            ),
            (
                "Transfer",
                "event Transfer(bytes32 indexed node, address owner)",
                &["registry"],
                &["AuthorityTransferred"],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged", "PermissionChanged"],
            ),
        ],
    ));
    input.admissions.push(admission_at(
        V1_REGISTRY_MANIFEST_ID,
        "registry",
        GATE_V1_REGISTRY,
        102,
    ));
    input
}

fn gate_dns(labels: &[&str]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for label in labels {
        encoded.push(u8::try_from(label.len()).expect("label fits"));
        encoded.extend_from_slice(label.as_bytes());
    }
    encoded.push(0);
    encoded
}

/// `NameWrapped` with the given fuses and expiry.
fn gate_wrap_with(
    block: i64,
    log_index: i64,
    node: &str,
    labels: &[&str],
    owner: &str,
    wrapper: &str,
    (fuses, expiry): (u32, u64),
) -> RawLogInput {
    raw_at_transaction(
        super::super::NameWrapped {
            node: node.parse().expect("node parses"),
            name: gate_dns(labels).into(),
            owner: owner.parse().expect("owner parses"),
            fuses,
            expiry,
        }
        .encode_log_data(),
        block,
        0,
        log_index,
        wrapper,
    )
}

fn gate_new_owner(
    block: i64,
    log_index: i64,
    parent_node: &str,
    label: &str,
    owner: &str,
) -> RawLogInput {
    raw_at_transaction(
        super::super::v1_registry::NewOwner {
            node: parent_node.parse().expect("parent parses"),
            label: keccak256(label.as_bytes()),
            owner: owner.parse().expect("owner parses"),
        }
        .encode_log_data(),
        block,
        0,
        log_index,
        GATE_V1_REGISTRY,
    )
}

fn gate_new_resolver(block: i64, log_index: i64, node: &str) -> RawLogInput {
    raw_at_transaction(
        super::super::v1_registry::NewResolver {
            node: node.parse().expect("node parses"),
            resolver: GATE_RESOLVER.parse().expect("resolver parses"),
        }
        .encode_log_data(),
        block,
        0,
        log_index,
        GATE_V1_REGISTRY,
    )
}

fn gate_unwrap_tail(block: i64, node: &str, owner: &str, wrapper: &str) -> Vec<RawLogInput> {
    let node_b256: B256 = node.parse().expect("node parses");
    let owner_addr: Address = owner.parse().expect("owner parses");
    let mut logs = vec![
        raw_at_transaction(
            super::super::v2_registry::TransferSingle {
                operator: owner_addr,
                from: owner_addr,
                to: Address::ZERO,
                id: U256::from_be_bytes(node_b256.0),
                value: U256::from(1),
            }
            .encode_log_data(),
            block,
            1,
            0,
            wrapper,
        ),
        raw_at_transaction(
            super::super::v1_registry::Transfer {
                node: node_b256,
                owner: owner_addr,
            }
            .encode_log_data(),
            block,
            1,
            1,
            GATE_V1_REGISTRY,
        ),
        raw_at_transaction(
            super::super::NameUnwrapped {
                node: node_b256,
                owner: owner_addr,
            }
            .encode_log_data(),
            block,
            1,
            2,
            wrapper,
        ),
    ];
    for log in &mut logs {
        log.block_hash = format!("block-{block}");
        log.transaction_hash = format!("transaction-{block}-1");
    }
    logs
}

/// An original child unwrapped after its parent's locked ENSv1→ENSv2
/// migration in the same batch must not regain a registry-only successor.
/// The split history (the ENSv1→ENSv2 migration in an earlier batch, restored)
/// already suppresses it. The combined history must agree.
#[test]
fn original_child_unwrap_after_same_batch_locked_parent_migration_opens_no_successor()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let parent_label = scenario["parent"]["label"].as_str().unwrap();
    let orig_node = namehash_under(&scenario["parent"]["namehash"], "orig")?;
    let orig_id = format!("ens:{orig_node}");
    let migration_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let mut wrap_logs = gate_create_wrapped(230, 0, "orig", 65_536, &fixture)?;
    wrap_logs.push(gate_new_resolver(230, 3, &orig_node));
    let parent_logs = level_logs(&scenario["parent"], addresses)?;
    // Split history: the parent's ENSv1→ENSv2 migration restored, then the unwrap alone.
    let split_head = {
        let mut logs = wrap_logs.clone();
        logs.extend(parent_logs.clone());
        interpret_test_batch(gate_batch(logs, &fixture))?
    };
    assert!(
        split_head
            .normalized_events
            .iter()
            .any(|event| event.event_kind == "MigrationApplied"
                && event.after_state["migration_path"] == "locked_wrapped"),
        "the parent's locked ENSv1→ENSv2 migration is proven in the split head"
    );
    let mut split_input = gate_batch(
        gate_unwrap_tail(migration_block + 4, &orig_node, GATE_OWNER, wrapper),
        &fixture,
    );
    split_input.prior_events = split_head
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    carry_admissions(&split_head, &mut split_input.admissions);
    let split_tail = interpret_test_batch(split_input)?;
    assert!(
        !gate_successor_open(&split_tail, &orig_id),
        "split history must not open the successor"
    );
    // Combined histories: the ENSv1→ENSv2 migration and the unwrap in one batch, in the
    // same block, and in its block then the next.
    for unwrap_block in [migration_block, migration_block + 1] {
        let mut logs = wrap_logs.clone();
        logs.extend(parent_logs.clone());
        logs.extend(gate_unwrap_tail(
            unwrap_block,
            &orig_node,
            GATE_OWNER,
            wrapper,
        ));
        let combined = interpret_test_batch(gate_batch(logs, &fixture))?;
        assert!(
            combined
                .normalized_events
                .iter()
                .any(|event| event.event_kind == "MigrationApplied"
                    && event.after_state["migration_path"] == "locked_wrapped"
                    && event.consumer_visibility == "activated"),
            "combined batch at unwrap block {unwrap_block} proves the parent boundary"
        );
        assert!(
            !gate_successor_open(&combined, &orig_id),
            "combined batch at unwrap block {unwrap_block} opened the registry-only successor"
        );
    }
    // Restore splits for the same history: before the ENSv1→ENSv2 migration,
    // between it and the unwrap, and after the unwrap. The between
    // split is the `split_tail` run above. The other two must agree with
    // the single-batch run as well.
    let full_logs = {
        let mut logs = wrap_logs.clone();
        logs.extend(parent_logs.clone());
        logs.extend(gate_unwrap_tail(
            migration_block + 1,
            &orig_node,
            GATE_OWNER,
            wrapper,
        ));
        logs
    };
    let full = interpret_test_batch(gate_batch(full_logs.clone(), &fixture))?;
    let before_head = interpret_test_batch(gate_batch(wrap_logs.clone(), &fixture))?;
    let before_tail_logs = {
        let mut logs = parent_logs.clone();
        logs.extend(gate_unwrap_tail(
            migration_block + 1,
            &orig_node,
            GATE_OWNER,
            wrapper,
        ));
        logs
    };
    let (before_live, _) =
        interpret_test_batch_incremental(gate_batch(wrap_logs.clone(), &fixture), None).and_then(
            |(_, session)| {
                interpret_test_batch_incremental(
                    gate_batch(before_tail_logs.clone(), &fixture),
                    Some(session),
                )
            },
        )?;
    assert!(
        !gate_successor_open(&before_live, &orig_id),
        "split before the ENSv1→ENSv2 migration opened the successor"
    );
    let mut before_cold = gate_batch(before_tail_logs, &fixture);
    before_cold.prior_events = before_head
        .normalized_events
        .iter()
        .map(prior_event)
        .collect();
    carry_admissions(&before_head, &mut before_cold.admissions);
    let before_cold = interpret_test_batch(before_cold)?;
    assert_eq!(before_cold.normalized_events, before_live.normalized_events);
    // After the unwrap: re-wrapping must not transition from a
    // registry-only successor that was never opened.
    let probe = gate_wrap_existing(
        migration_block + 5,
        0,
        &orig_node,
        &["orig", parent_label, "eth"],
        wrapper,
    );
    let (full_probe_live, _) =
        interpret_test_batch_incremental(gate_batch(full_logs.clone(), &fixture), None).and_then(
            |(_, session)| {
                interpret_test_batch_incremental(gate_batch(probe.clone(), &fixture), Some(session))
            },
        )?;
    let history = interpret_test_batch(gate_batch(full_logs, &fixture))?;
    let mut probe_cold = gate_batch(probe, &fixture);
    probe_cold.prior_events = history.normalized_events.iter().map(prior_event).collect();
    carry_admissions(&history, &mut probe_cold.admissions);
    let probe_cold = interpret_test_batch(probe_cold)?;
    assert_eq!(
        probe_cold.normalized_events,
        full_probe_live.normalized_events
    );
    // The re-wrap's own registry write to the NameWrapper may open a registry-only authority
    // that its `NameWrapped` closes in the same block. A registry-only authority it closes
    // without opening it first would have survived from the unwrap.
    let mut opened_in_probe = false;
    for event in probe_cold
        .normalized_events
        .iter()
        .filter(|event| event.logical_name_id.as_deref() == Some(orig_id.as_str()))
    {
        if event.event_kind == "SurfaceBound"
            && event.after_state["authority_kind"] == "registry_only"
        {
            opened_in_probe = true;
        }
        assert!(
            event.event_kind != "SurfaceUnbound"
                || event.before_state["authority_kind"] != "registry_only"
                || opened_in_probe,
            "restored history kept a registry-only successor to close: {:?}",
            probe_cold.normalized_events
        );
    }
    let _ = full;
    Ok(())
}

/// A locked child establishes its own WrapperRegistry, created by its
/// ENSv1→ENSv2 migration, so an original grandchild unwrapping under it must
/// not regain a registry-only successor either.
#[test]
fn original_grandchild_unwrap_under_locked_child_parent_opens_no_successor() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-04"];
    let addresses = &fixture["addresses"];
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let child_label = scenario["child"]["label"].as_str().unwrap();
    let parent_label = scenario["parent"]["label"].as_str().unwrap();
    let grand_node = namehash_under(&scenario["child"]["namehash"], "orig")?;
    let grand_id = format!("ens:{grand_node}");
    let head_logs = {
        let mut logs = gate_create_wrapped_under(
            (229, 0),
            &scenario["child"],
            &["orig", child_label, parent_label, "eth"],
            65_536,
            &fixture,
        )?;
        logs.push(gate_new_resolver(229, 3, &grand_node));
        logs.extend(scenario_logs(scenario, addresses, &["parent", "child"])?);
        logs
    };
    let head = interpret_test_batch(gate_batch(head_logs, &fixture))?;
    assert!(
        head.normalized_events.iter().any(|event| {
            event.event_kind == "MigrationApplied"
                && event.after_state["migration_path"] == "locked_child"
                && event.after_state["namehash"] == scenario["child"]["namehash"]
        }),
        "the immediate parent made a locked_child ENSv1→ENSv2 migration"
    );
    let mut tail_input = gate_batch(
        gate_unwrap_tail(250, &grand_node, GATE_OWNER, wrapper),
        &fixture,
    );
    tail_input.prior_events = head.normalized_events.iter().map(prior_event).collect();
    carry_admissions(&head, &mut tail_input.admissions);
    let tail = interpret_test_batch(tail_input)?;
    assert!(
        !gate_successor_open(&tail, &grand_id),
        "grandchild under a locked_child parent opened the registry-only successor"
    );
    Ok(())
}

/// Scoped restore with no parent link: the child's retained history
/// names only the child. Its record was created before the batch, and its
/// holder's `wrap` writes the registry with `Transfer`, not a `NewOwner`
/// linking it to its parent. So a child-only scoped restore has no link
/// that pulls the parent's `MigrationApplied` in. The gate must still load
/// that boundary before it answers.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L68 @ ens_v1@91c966f)
#[test]
fn original_child_unwrap_scoped_restore_without_parent_link_opens_no_successor()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let parent_label = scenario["parent"]["label"].as_str().unwrap();
    let orig_node = namehash_under(&scenario["parent"]["namehash"], "orig")?;
    let orig_id = format!("ens:{orig_node}");
    let migration_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let mut head_logs =
        gate_wrap_existing(230, 0, &orig_node, &["orig", parent_label, "eth"], wrapper);
    head_logs.push(gate_new_resolver(230, 3, &orig_node));
    head_logs.extend(level_logs(&scenario["parent"], addresses)?);
    let head = interpret_test_batch(gate_batch(head_logs, &fixture))?;
    let mut tail_input = gate_batch(
        gate_unwrap_tail(migration_block + 4, &orig_node, GATE_OWNER, wrapper),
        &fixture,
    );
    tail_input.prior_events = head.normalized_events.iter().map(prior_event).collect();
    carry_admissions(&head, &mut tail_input.admissions);
    let tail = interpret_test_batch(tail_input)?;
    assert!(
        !gate_successor_open(&tail, &orig_id),
        "scoped restore without a parent link opened the registry-only successor"
    );
    Ok(())
}

/// The locked ENSv1→ENSv2 migration of
/// `original_child_unwrap_after_same_batch_locked_parent_migration_opens_no_successor` re-runs
/// the batch, because its first pass opened a registry-only successor for the original child.
/// A resolver proxy in that batch, admitted by an earlier batch and announced again after its
/// own role and record changes, must keep both.
#[test]
fn locked_migration_rerun_keeps_a_role_change_before_a_resolver_reannouncement()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let orig_node = namehash_under(&scenario["parent"]["namehash"], "orig")?;
    let orig_id = format!("ens:{orig_node}");
    let migration_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let mut logs = gate_create_wrapped(230, 0, "orig", 65_536, &fixture)?;
    logs.push(gate_new_resolver(230, 3, &orig_node));
    logs.extend(level_logs(&scenario["parent"], addresses)?);
    let without_unwrap =
        super::announced_resolver::with_announced_resolver(gate_batch(logs.clone(), &fixture))?;
    let single = interpret_test_batch(without_unwrap)?;
    assert!(
        super::announced_resolver::kept_record_change(&single)
            && super::announced_resolver::kept_roles_change(&single),
        "a single pass keeps the record and role changes"
    );
    logs.extend(gate_unwrap_tail(
        migration_block + 1,
        &orig_node,
        GATE_OWNER,
        wrapper,
    ));
    let output = interpret_test_batch(super::announced_resolver::with_announced_resolver(
        gate_batch(logs, &fixture),
    )?)?;
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "MigrationApplied"
                && event.after_state["migration_path"] == "locked_wrapped"
                && event.consumer_visibility == "activated"
        }),
        "the batch proves the parent's locked ENSv1→ENSv2 migration"
    );
    assert!(
        !gate_successor_open(&output, &orig_id),
        "the re-run keeps the successor closed"
    );
    assert!(
        super::announced_resolver::kept_record_change(&output),
        "the locked ENSv1→ENSv2 migration re-run dropped the resolver's record change"
    );
    assert!(
        super::announced_resolver::kept_roles_change(&output),
        "the locked ENSv1→ENSv2 migration re-run dropped the resolver's role change"
    );
    Ok(())
}

/// One batch that takes both re-runs: the locked ENSv1→ENSv2 migration of
/// `original_child_unwrap_after_same_batch_locked_parent_migration_opens_no_successor` and the
/// old-lease expiry crossing of `migrated_v1_lease_expiry_does_not_reopen_in_one_batch`. The
/// expiry scenario's ENSv1→ENSv2 migration moves to its own later transaction in the shared
/// block. Both proofs must hold, and the resolver's role and record changes before its
/// re-announcement must survive.
#[test]
fn locked_and_expiry_reruns_together_keep_a_role_change_before_a_resolver_reannouncement()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let orig_node = namehash_under(&scenario["parent"]["namehash"], "orig")?;
    let orig_id = format!("ens:{orig_node}");
    let migration_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let mut input = super::expiry::migration_and_old_lease_expiry()?;
    let expiry_block = input.blocks.last().unwrap().block_number;
    for raw in &mut input.raw_logs {
        if raw.block_number == migration_block {
            raw.transaction_index = 2;
            raw.transaction_hash = format!("transaction-{migration_block}-2");
            raw.log_index += 50;
        }
    }
    let mut logs = gate_create_wrapped(230, 0, "orig", 65_536, &fixture)?;
    logs.push(gate_new_resolver(230, 3, &orig_node));
    logs.extend(level_logs(&scenario["parent"], addresses)?);
    logs.extend(gate_unwrap_tail(
        migration_block + 1,
        &orig_node,
        GATE_OWNER,
        wrapper,
    ));
    input.blocks.extend(logs.iter().map(|raw| RawBlockInput {
        chain_id: raw.chain_id.clone(),
        block_hash: raw.block_hash.clone(),
        block_number: raw.block_number,
        block_timestamp: raw.block_timestamp,
        canonicality_state: raw.canonicality_state.clone(),
    }));
    input.blocks.sort_by_key(|block| block.block_number);
    input.blocks.dedup_by_key(|block| block.block_number);
    input.raw_logs.extend(logs);
    input.raw_logs = ordered(std::mem::take(&mut input.raw_logs));
    let output = interpret_test_batch(super::announced_resolver::with_announced_resolver(input)?)?;
    super::expiry::assert_old_lease_does_not_reopen(&output, expiry_block);
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "MigrationApplied"
                && event.after_state["migration_path"] == "locked_wrapped"
                && event.consumer_visibility == "activated"
        }),
        "the batch proves the parent's locked ENSv1→ENSv2 migration"
    );
    assert!(
        !gate_successor_open(&output, &orig_id),
        "the re-runs keep the successor closed"
    );
    assert!(
        super::announced_resolver::kept_record_change(&output),
        "the combined re-runs dropped the resolver's record change"
    );
    assert!(
        super::announced_resolver::kept_roles_change(&output),
        "the combined re-runs dropped the resolver's role change"
    );
    Ok(())
}

const GATE_AWAY_OWNER: &str = "0x00000000000000000000000000000000000000e1";
const GATE_TRANSFEREE: &str = "0x00000000000000000000000000000000000000e2";
const GATE_PROBE_OWNER: &str = "0x00000000000000000000000000000000000000e3";

fn gate_registry_transfer(block: i64, log_index: i64, node: &str, owner: &str) -> RawLogInput {
    raw_at_transaction(
        super::super::v1_registry::Transfer {
            node: node.parse().expect("node parses"),
            owner: owner.parse().expect("owner parses"),
        }
        .encode_log_data(),
        block,
        0,
        log_index,
        GATE_V1_REGISTRY,
    )
}

fn gate_token_move(block: i64, node: &str, from: &str, to: &str, wrapper: &str) -> RawLogInput {
    let from: Address = from.parse().expect("from parses");
    raw_at_transaction(
        super::super::v2_registry::TransferSingle {
            operator: from,
            from,
            to: to.parse().expect("to parses"),
            id: U256::from_be_bytes(node.parse::<B256>().expect("node parses").0),
            value: U256::from(1),
        }
        .encode_log_data(),
        block,
        0,
        0,
        wrapper,
    )
}

/// A custody-path child: its logical name id, unwrap block and probe block.
type GateCustodyChild = (String, i64, i64);

/// The NameWrapper's mint of a child token to GATE_OWNER, called by `operator`.
fn gate_mint(block: i64, log_index: i64, node: &str, operator: &str, wrapper: &str) -> RawLogInput {
    let owner: Address = GATE_OWNER.parse().expect("owner parses");
    raw_at_transaction(
        super::super::v2_registry::TransferSingle {
            operator: operator.parse().expect("operator parses"),
            from: Address::ZERO,
            to: owner,
            id: U256::from_be_bytes(node.parse::<B256>().expect("node parses").0),
            value: U256::from(1),
        }
        .encode_log_data(),
        block,
        0,
        log_index,
        wrapper,
    )
}

/// GATE_OWNER, as the holder of the wrapped C-01 parent, creates a child for itself through
/// the NameWrapper's `setSubnodeOwner`: the registry write to the NameWrapper, the mint, then
/// `NameWrapped`. The logs take `log_index` and the two after it in transaction 0.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L579-L581 @ ens_v1@91c966f)
fn gate_create_wrapped(
    block: i64,
    log_index: i64,
    label: &str,
    fuses: u32,
    fixture: &Value,
) -> anyhow::Result<Vec<RawLogInput>> {
    let parent = &fixture["scenarios"]["C-01"]["parent"];
    let parent_label = parent["label"].as_str().unwrap();
    gate_create_wrapped_under(
        (block, log_index),
        parent,
        &[label, parent_label, "eth"],
        fuses,
        fixture,
    )
}

/// `gate_create_wrapped` under the catalog `parent` level, for the child whose DNS labels are
/// `labels`, the child's own label first. The holder of the parent's token calls
/// `setSubnodeOwner`, so it is the mint's operator: the level's `wrapped_owner`, else
/// GATE_OWNER. The NameWrapper caps the child's expiry at the parent's, so a level with a
/// `wrap_expiry` caps it there.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L958-L960 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L985-L987 @ ens_v1@91c966f)
fn gate_create_wrapped_under(
    (block, log_index): (i64, i64),
    parent: &Value,
    labels: &[&str],
    fuses: u32,
    fixture: &Value,
) -> anyhow::Result<Vec<RawLogInput>> {
    let wrapper = fixture["addresses"]["name_wrapper"].as_str().unwrap();
    let parent_node = parent["namehash"].as_str().unwrap();
    let node = namehash_under(&parent["namehash"], labels[0])?;
    let operator = parent["wrapped_owner"].as_str().unwrap_or(GATE_OWNER);
    let expiry = parent["wrap_expiry"]
        .as_u64()
        .map_or(4_000_000_000, |parent_expiry| {
            parent_expiry.min(4_000_000_000)
        });
    Ok(vec![
        gate_new_owner(block, log_index, parent_node, labels[0], wrapper),
        gate_mint(block, log_index + 1, &node, operator, wrapper),
        gate_wrap_with(
            block,
            log_index + 2,
            &node,
            labels,
            GATE_OWNER,
            wrapper,
            (fuses, expiry),
        ),
    ])
}

/// GATE_OWNER's `wrap` of a child it holds in the registry: the registry write to the
/// NameWrapper, the mint, then `NameWrapped` with fuses 0 and expiry 0, as `wrap` passes them.
/// The logs take `log_index` and the two after it in transaction 0.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372-L374 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L902 @ ens_v1@91c966f)
fn gate_wrap_existing(
    block: i64,
    log_index: i64,
    node: &str,
    labels: &[&str],
    wrapper: &str,
) -> Vec<RawLogInput> {
    vec![
        gate_registry_transfer(block, log_index, node, wrapper),
        gate_mint(block, log_index + 1, node, GATE_OWNER, wrapper),
        gate_wrap_with(
            block,
            log_index + 2,
            node,
            labels,
            GATE_OWNER,
            wrapper,
            (0, 0),
        ),
    ]
}

/// One child of the C-01 parent per custody path, created at block 210 and unwrapped at its own
/// block, followed by a registry owner change. Returns the logs and the children.
///
/// The record of a child leaves the NameWrapper through the registry only while the parent is
/// unwrapped, because the NameWrapper's `setSubnodeOwner` writes only itself as the child's
/// registry owner. A parent that can be unwrapped has not burned CANNOT_UNWRAP, so its children
/// carry no parent-controlled fuses, and every child here has fuses 0. The C-01 parent's own
/// ENSv1 history, which these fixtures leave out, is unwrapped at 211-212 and wrapped and
/// locked again before its ENSv1→ENSv2 migration.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L580 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L963-L975 @ ens_v1@91c966f)
fn gate_custody_paths(
    fixture: &Value,
) -> anyhow::Result<(Vec<RawLogInput>, Vec<GateCustodyChild>)> {
    let scenario = &fixture["scenarios"]["C-01"];
    let wrapper = fixture["addresses"]["name_wrapper"].as_str().unwrap();
    let parent_node = scenario["parent"]["namehash"].as_str().unwrap();
    let parent_label = scenario["parent"]["label"].as_str().unwrap();
    let mut logs = Vec::new();
    let mut names = Vec::new();
    for (index, label) in ["away", "held", "rewrap", "moved", "plain"]
        .into_iter()
        .enumerate()
    {
        let index = i64::try_from(index)?;
        let node = namehash_under(&scenario["parent"]["namehash"], label)?;
        logs.extend(gate_create_wrapped(210, 3 * index, label, 0, fixture)?);
        let mut owner = GATE_OWNER;
        match label {
            "away" => {
                logs.push(gate_new_owner(211, 0, parent_node, label, GATE_AWAY_OWNER));
                logs.push(gate_new_owner(212, 0, parent_node, label, wrapper));
            }
            // The parent owner hands the record to the holder through the registry, and the
            // holder unwraps while it is away. The unwrap's registry write passes through the
            // holder's registry operator approval for the NameWrapper.
            // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L21 @ ens_v1@91c966f)
            "held" => {
                logs.push(gate_new_owner(211, 1, parent_node, label, GATE_OWNER));
            }
            "rewrap" => {
                logs.extend(gate_unwrap_tail(211, &node, GATE_OWNER, wrapper));
                logs.extend(gate_wrap_existing(
                    212,
                    1,
                    &node,
                    &[label, parent_label, "eth"],
                    wrapper,
                ));
            }
            "moved" => {
                logs.push(gate_token_move(
                    213,
                    &node,
                    GATE_OWNER,
                    GATE_TRANSFEREE,
                    wrapper,
                ));
                owner = GATE_TRANSFEREE;
            }
            _ => {}
        }
        let unwrap_block = 214 + index;
        let probe_block = 220 + index;
        logs.extend(gate_unwrap_tail(unwrap_block, &node, owner, wrapper));
        logs.push(gate_registry_transfer(
            probe_block,
            0,
            &node,
            GATE_PROBE_OWNER,
        ));
        names.push((format!("ens:{node}"), unwrap_block, probe_block));
    }
    Ok((logs, names))
}

/// The unwrap keeps the child bound to a registry-only authority, and the later registry
/// owner change transfers that authority instead of binding the child again.
fn assert_gate_unwrap_keeps_registry_authority(
    output: &BatchOutput,
    context: &str,
    logical_name_id: &str,
    unwrap_block: i64,
    probe_block: i64,
) {
    let binding_events = |block: i64| {
        output
            .normalized_events
            .iter()
            .filter(|event| {
                event.block_number == Some(block)
                    && event.logical_name_id.as_deref() == Some(logical_name_id)
                    && matches!(event.event_kind.as_str(), "SurfaceBound" | "SurfaceUnbound")
            })
            .collect::<Vec<_>>()
    };
    let at_unwrap = binding_events(unwrap_block);
    if let Some(last) = at_unwrap.last() {
        assert!(
            last.event_kind == "SurfaceBound"
                && last.after_state["authority_kind"] == "registry_only",
            "{context}: the unwrap left the child unbound: {at_unwrap:?}"
        );
    }
    assert!(
        binding_events(probe_block).is_empty(),
        "{context}: the registry owner change bound the child again: {:?}",
        binding_events(probe_block)
    );
    assert!(
        output.surface_bindings.iter().any(|binding| {
            binding.logical_name_id == logical_name_id
                && binding.binding_kind == "declared_registry_path"
        }),
        "{context}: no registry binding"
    );
}

/// The locked re-run trigger: the C-01 parent's locked ENSv1→ENSv2 migration, and an original
/// child "orig" unwrapped after it in the same batch. The batch activates the parent's locked
/// boundary, so Interpret re-runs it with the boundary at its position. The first pass opens
/// a registry-only successor for "orig", and the re-run closes it again. Returns the logs and
/// the child's logical name id.
fn gate_locked_rerun_logs(fixture: &Value) -> anyhow::Result<(Vec<RawLogInput>, String)> {
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let orig_node = namehash_under(&scenario["parent"]["namehash"], "orig")?;
    let migration_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let mut logs = gate_create_wrapped(230, 0, "orig", 65_536, fixture)?;
    logs.push(gate_new_resolver(230, 3, &orig_node));
    logs.extend(level_logs(&scenario["parent"], addresses)?);
    logs.extend(gate_unwrap_tail(
        migration_block + 1,
        &orig_node,
        GATE_OWNER,
        wrapper,
    ));
    Ok((logs, format!("ens:{orig_node}")))
}

/// The re-run variants of the custody paths. The expiry variant crosses an old lease's expiry
/// after an ENSv1→ENSv2 migration. The locked variant adds the locked re-run trigger of
/// `gate_locked_rerun_logs`. The custody unwraps come before that trigger's ENSv1→ENSv2
/// migration, so its position suppresses none of them. The last variant takes both re-runs in
/// one batch, with the expiry scenario's ENSv1→ENSv2 migration in its own later transaction of
/// the shared block.
#[test]
fn every_custody_path_into_a_no_lease_unwrap_keeps_the_registry_authority_on_a_rerun()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let migration_block = fixture["scenarios"]["C-01"]["parent"]["migration_block"]
        .as_i64()
        .unwrap();
    for (variant, locked, expiry) in [
        ("expiry", false, true),
        ("locked", true, false),
        ("locked and expiry", true, true),
    ] {
        let (mut logs, names) = gate_custody_paths(&fixture)?;
        let orig = if locked {
            let (locked_logs, orig) = gate_locked_rerun_logs(&fixture)?;
            logs.extend(locked_logs);
            Some(orig)
        } else {
            None
        };
        let mut expiry_block = None;
        let input = if expiry {
            let mut input = super::expiry::migration_and_old_lease_expiry()?;
            expiry_block = input.blocks.last().map(|block| block.block_number);
            if locked {
                for raw in &mut input.raw_logs {
                    if raw.block_number == migration_block {
                        raw.transaction_index = 2;
                        raw.transaction_hash = format!("transaction-{migration_block}-2");
                        raw.log_index += 50;
                    }
                }
            }
            input.blocks.extend(logs.iter().map(|raw| RawBlockInput {
                chain_id: raw.chain_id.clone(),
                block_hash: raw.block_hash.clone(),
                block_number: raw.block_number,
                block_timestamp: raw.block_timestamp,
                canonicality_state: raw.canonicality_state.clone(),
            }));
            input.blocks.sort_by_key(|block| block.block_number);
            input.blocks.dedup_by_key(|block| block.block_number);
            input.raw_logs.extend(logs);
            input.raw_logs = ordered(std::mem::take(&mut input.raw_logs));
            input
        } else {
            gate_batch(logs, &fixture)
        };
        let output = interpret_test_batch(input)?;
        if let Some(expiry_block) = expiry_block {
            super::expiry::assert_old_lease_does_not_reopen(&output, expiry_block);
        }
        if let Some(orig) = orig {
            assert!(
                output.normalized_events.iter().any(|event| {
                    event.event_kind == "MigrationApplied"
                        && event.after_state["migration_path"] == "locked_wrapped"
                        && event.consumer_visibility == "activated"
                }),
                "{variant}: the batch proves the parent's locked ENSv1→ENSv2 migration"
            );
            assert!(
                !gate_successor_open(&output, &orig),
                "{variant}: the locked re-run kept the trigger's successor open"
            );
        }
        for (logical_name_id, unwrap_block, probe_block) in names {
            assert_gate_unwrap_keeps_registry_authority(
                &output,
                &format!("{variant}: {logical_name_id}"),
                &logical_name_id,
                unwrap_block,
                probe_block,
            );
        }
    }
    Ok(())
}

/// The holder's `unwrap` names the admitted Graveyard as controller, so the registry owner
/// the unwrap leaves is the Graveyard, the ENSv1→ENSv2 migration's burn address for ENSv1
/// names. That is a burned record, not a live registry owner, so no registry-only successor
/// opens. The emancipated-branch cleanup of an ENSv1→ENSv2 migration makes the same unwrap from
/// the receiver. "grave" is a child of a locked parent with PARENT_CANNOT_CONTROL, the shape that
/// cleanup meets, and the wrapper is selected at its unwrap. "graveaway" has fuses 0, and its
/// record left the NameWrapper and came back while the parent was unwrapped, so the
/// registry-only authority is selected at its unwrap.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L414 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/Graveyard.sol:L20-L23 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L178-L180 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/libraries/LibMigration.sol:L85-L89 @ ens_v2_sepolia_20261001@07e55a05)
#[test]
fn unwrap_into_the_graveyard_keeps_no_registry_only_authority() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let wrapper = fixture["addresses"]["name_wrapper"].as_str().unwrap();
    let graveyard = fixture["addresses"]["graveyard"].as_str().unwrap();
    let parent_node = scenario["parent"]["namehash"].as_str().unwrap();
    for (label, away) in [("grave", false), ("graveaway", true)] {
        let node = namehash_under(&scenario["parent"]["namehash"], label)?;
        let id = format!("ens:{node}");
        let fuses = if away { 0 } else { 65_536 };
        let mut logs = gate_create_wrapped(210, 0, label, fuses, &fixture)?;
        if away {
            logs.push(gate_new_owner(211, 0, parent_node, label, GATE_AWAY_OWNER));
            logs.push(gate_new_owner(212, 0, parent_node, label, wrapper));
        }
        let mut unwrap = gate_unwrap_tail(214, &node, graveyard, wrapper);
        unwrap[0] = gate_token_move(214, &node, GATE_OWNER, ZERO_ADDRESS, wrapper);
        unwrap[0].transaction_index = 1;
        unwrap[0].transaction_hash = "transaction-214-1".to_owned();
        logs.extend(unwrap);
        let output = interpret_test_batch(gate_batch(logs, &fixture))?;
        let after_unwrap = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.block_number == Some(214)
                    && event.logical_name_id.as_deref() == Some(id.as_str())
                    && matches!(event.event_kind.as_str(), "SurfaceBound" | "SurfaceUnbound")
            })
            .collect::<Vec<_>>();
        assert!(
            after_unwrap
                .last()
                .is_some_and(|event| event.event_kind == "SurfaceUnbound"),
            "{label}: the unwrap into the Graveyard left the child bound: {after_unwrap:?}"
        );
        assert!(
            !output.normalized_events.iter().any(|event| {
                event.block_number == Some(214)
                    && event.logical_name_id.as_deref() == Some(id.as_str())
                    && event.event_kind == "SurfaceBound"
                    && event.after_state["authority_kind"] == "registry_only"
            }),
            "{label}: a registry-only authority was bound for the Graveyard"
        );
    }
    Ok(())
}

/// What a row leaves on the child's registry-only resource.
enum GateGraveyardEnd {
    /// No grants at all: the write ended the NameWrapper's grants and granted the Graveyard none.
    Nothing,
    /// The NameWrapper's grants, as the wrap left them, and none for the Graveyard.
    NameWrapperKeeps,
    /// None of the NameWrapper's grants.
    NameWrapperEnds,
}

/// `holder`'s `unwrap` of `node` into `to` at `block`, in transaction 1: the burn, the
/// registry owner write, then `NameUnwrapped`, from `first_log` on.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
fn gate_unwrap_into(
    (block, first_log): (i64, i64),
    node: &str,
    (holder, to): (&str, &str),
    wrapper: &str,
) -> Vec<RawLogInput> {
    let mut logs = gate_unwrap_tail(block, node, to, wrapper);
    logs[0] = gate_hop((block, 1, 0), node, (holder, holder, ZERO_ADDRESS), wrapper);
    for log in &mut logs {
        log.log_index += first_log;
    }
    logs
}

/// A NameWrapper `TransferSingle` of `node`'s token, as (operator, from, to), at
/// (block, transaction index, log index).
fn gate_hop(
    (block, transaction_index, log_index): (i64, i64, i64),
    node: &str,
    (operator, from, to): (&str, &str, &str),
    wrapper: &str,
) -> RawLogInput {
    raw_at_transaction(
        super::super::v2_registry::TransferSingle {
            operator: operator.parse().expect("operator parses"),
            from: from.parse().expect("from parses"),
            to: to.parse().expect("to parses"),
            id: U256::from_be_bytes(node.parse::<B256>().expect("node parses").0),
            value: U256::from(1),
        }
        .encode_log_data(),
        block,
        transaction_index,
        log_index,
        wrapper,
    )
}

/// The registry's `NewResolver` of `node` to `resolver` at (block, transaction index, log index).
fn gate_resolver_at(
    (block, transaction_index, log_index): (i64, i64, i64),
    node: &str,
    resolver: Address,
) -> RawLogInput {
    raw_at_transaction(
        super::super::v1_registry::NewResolver {
            node: node.parse().expect("node parses"),
            resolver,
        }
        .encode_log_data(),
        block,
        transaction_index,
        log_index,
        GATE_V1_REGISTRY,
    )
}

/// The registry's `Transfer` of `node` to `owner` at (block, transaction index, log index).
fn gate_registry_owner_at(
    (block, transaction_index, log_index): (i64, i64, i64),
    node: &str,
    owner: &str,
) -> RawLogInput {
    raw_at_transaction(
        super::super::v1_registry::Transfer {
            node: node.parse().expect("node parses"),
            owner: owner.parse().expect("owner parses"),
        }
        .encode_log_data(),
        block,
        transaction_index,
        log_index,
        GATE_V1_REGISTRY,
    )
}

/// `gyeth.eth`, registered to GATE_OWNER at block 205 and wrapped by `wrapETH2LD` at 206 with a
/// resolver. These registrar logs make no registrar identity, so no lease takes the wrap's
/// registry write over, and the NameWrapper holds the name's registry-only resource while the
/// name is wrapped. Returns the logs and the name's node.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
fn gate_wrapped_eth_name(fixture: &Value) -> anyhow::Result<(Vec<RawLogInput>, String)> {
    let registrar = fixture["addresses"]["base_registrar"].as_str().unwrap();
    let wrapper = fixture["addresses"]["name_wrapper"].as_str().unwrap();
    let eth = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    let labelhash = keccak256(b"gyeth");
    let node = format!(
        "{:#x}",
        keccak256([eth.as_slice(), labelhash.as_slice()].concat())
    );
    let owner: Address = GATE_OWNER.parse()?;
    let new_owner = |block: i64, log_index: i64, owner: Address| {
        raw_at_transaction(
            super::super::v1_registry::NewOwner {
                node: eth,
                label: labelhash,
                owner,
            }
            .encode_log_data(),
            block,
            0,
            log_index,
            GATE_V1_REGISTRY,
        )
    };
    let logs = vec![
        registrar_transfer_at(Address::ZERO, owner, labelhash, 205, 0, registrar),
        new_owner(205, 1, owner),
        raw_at_transaction(
            with_topic0(
                BaseNameRegistered {
                    id: U256::from_be_bytes(labelhash.0),
                    owner,
                    expires: U256::from(4_000_000_000_u64),
                }
                .encode_log_data(),
                keccak256(b"NameRegistered(uint256,address,uint256)"),
            ),
            205,
            0,
            2,
            registrar,
        ),
        registrar_transfer_at(owner, wrapper.parse()?, labelhash, 206, 0, registrar),
        new_owner(206, 1, wrapper.parse()?),
        gate_hop(
            (206, 0, 2),
            &node,
            (GATE_OWNER, ZERO_ADDRESS, GATE_OWNER),
            wrapper,
        ),
        gate_wrap_with(
            206,
            3,
            &node,
            &["gyeth", "eth"],
            GATE_OWNER,
            wrapper,
            (0x30000, 4_000_000_000 + 90 * 86_400),
        ),
        gate_resolver_at((206, 0, 4), &node, GATE_RESOLVER.parse()?),
    ];
    Ok((logs, node))
}

/// Every ENSv1 registry write that names the admitted Graveyard as the owner of a wrapped name,
/// checked on the name's registry-only resource. The Graveyard is the ENSv1→ENSv2 migration's
/// burn address for ENSv1 names, so no write grants it anything there, and a write that moves
/// the record away from the NameWrapper still ends the NameWrapper's grants. Each row runs in
/// one batch and cut after the setup and after each step, resumed warm, restored cold and
/// restored from folded rows, each without and with the locked re-run trigger of
/// `gate_locked_rerun_logs`.
///
/// The rows whose registry owner is the NameWrapper with the wrapper authority selected:
/// - "unwrap": the holder's `unwrap` into the Graveyard. `_unwrap`'s `setOwner` is the only
///   registry write of the NameWrapper that names an owner other than itself, so this row also
///   covers `setOwner` to the Graveyard.
/// - "emancipated cleanup": `setResolver` to zero, then the holder's `unwrap` into the
///   Graveyard, as the locked receiver cleans up an emancipated child in an ENSv1→ENSv2
///   migration.
/// - "clear": the NameWrapper's `setSubnodeRecord` of the child to the Graveyard with no
///   resolver, then the Graveyard's `unwrap` into itself, as `Graveyard.clear` cleans up a
///   wrapped child that is neither locked nor emancipated.
/// - "set record": the NameWrapper's `setRecord` naming the Graveyard, which writes the registry
///   to the NameWrapper and moves only the token.
/// - "parent write": the parent's registry write naming the Graveyard, as `Graveyard.clear`
///   makes for a child of a parent it owns in the registry. That write selects the
///   registry-only authority, so this row checks that the NameWrapper's grants end.
/// - ".eth": `setResolver` to zero, then `unwrapETH2LD` into the Graveyard and the BaseRegistrar
///   transfer to it, as the unlocked controller migrates a wrapped `.eth` name in an
///   ENSv1→ENSv2 migration.
///
/// The row with the registry-only authority selected:
/// - "away": the holder's unwrap into the Graveyard after the record left the NameWrapper and
///   came back through the registry, so the registry-only authority is selected at the unwrap.
///
/// A `.eth` name with a registrar identity has its lease take those writes over. Its row is
/// `unwrapped_child_under_wrapped_parent::unwrap_eth2ld_into_the_graveyard_moves_no_registry_only_grants`.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/Graveyard.sol:L20-L23 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/Graveyard.sol:L171 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/Graveyard.sol:L175-L199 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L178-L180 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/UnlockedMigrationController.sol:L146-L147 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L372 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L580 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L611-L628 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L943-L947 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L637-L660 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1029 @ ens_v1@91c966f)
#[test]
fn a_registry_write_naming_the_graveyard_grants_it_nothing_on_the_registry_only_resource()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let wrapper = fixture["addresses"]["name_wrapper"].as_str().unwrap();
    let graveyard = fixture["addresses"]["graveyard"].as_str().unwrap();
    let parent_node = scenario["parent"]["namehash"].as_str().unwrap();
    let (trigger, _) = gate_locked_rerun_logs(&fixture)?;
    let child = |label: &str, fuses: u32| -> anyhow::Result<(Vec<RawLogInput>, String)> {
        let node = namehash_under(&scenario["parent"]["namehash"], label)?;
        let mut logs = gate_create_wrapped(210, 0, label, fuses, &fixture)?;
        logs.push(gate_new_resolver(210, 3, &node));
        Ok((logs, node))
    };
    let mut rows = Vec::new();
    let unwrap_by_holder = |node: &str| {
        vec![gate_unwrap_into(
            (214, 0),
            node,
            (GATE_OWNER, graveyard),
            wrapper,
        )]
    };
    let (setup, node) = child("gyunwrap", 65_536)?;
    let steps = unwrap_by_holder(&node);
    rows.push(("unwrap", setup, steps, node, GateGraveyardEnd::Nothing));
    let (setup, node) = child("gyemancipated", 65_536)?;
    let mut cleanup = vec![gate_resolver_at((214, 1, 0), &node, Address::ZERO)];
    cleanup.extend(gate_unwrap_into(
        (214, 1),
        &node,
        (GATE_TRANSFEREE, graveyard),
        wrapper,
    ));
    let steps = vec![
        vec![gate_hop(
            (212, 0, 0),
            &node,
            (GATE_OWNER, GATE_OWNER, GATE_TRANSFEREE),
            wrapper,
        )],
        cleanup,
    ];
    let end = GateGraveyardEnd::Nothing;
    rows.push(("emancipated cleanup", setup, steps, node, end));
    let (setup, node) = child("gyclear", 0)?;
    let mut clear = gate_new_owner(214, 0, parent_node, "gyclear", wrapper);
    clear.transaction_index = 1;
    clear.transaction_hash = "transaction-214-1".to_owned();
    let mut clear = vec![
        clear,
        gate_resolver_at((214, 1, 1), &node, Address::ZERO),
        gate_hop(
            (214, 1, 2),
            &node,
            (graveyard, GATE_OWNER, graveyard),
            wrapper,
        ),
    ];
    clear.extend(gate_unwrap_into(
        (214, 3),
        &node,
        (graveyard, graveyard),
        wrapper,
    ));
    rows.push(("clear", setup, vec![clear], node, GateGraveyardEnd::Nothing));
    let (setup, node) = child("gyrecord", 0)?;
    let steps = vec![vec![
        gate_registry_owner_at((214, 1, 0), &node, wrapper),
        gate_hop(
            (214, 1, 1),
            &node,
            (GATE_OWNER, GATE_OWNER, graveyard),
            wrapper,
        ),
    ]];
    let end = GateGraveyardEnd::NameWrapperKeeps;
    rows.push(("set record", setup, steps, node, end));
    let (setup, node) = child("gyparent", 0)?;
    let write = gate_new_owner(214, 0, parent_node, "gyparent", graveyard);
    let end = GateGraveyardEnd::NameWrapperEnds;
    rows.push(("parent write", setup, vec![vec![write]], node, end));
    let (setup, node) = gate_wrapped_eth_name(&fixture)?;
    let registrar = fixture["addresses"]["base_registrar"].as_str().unwrap();
    let mut unwrap = vec![
        gate_hop(
            (214, 0, 0),
            &node,
            (GATE_OWNER, GATE_OWNER, GATE_TRANSFEREE),
            wrapper,
        ),
        gate_resolver_at((214, 1, 0), &node, Address::ZERO),
    ];
    unwrap.extend(gate_unwrap_into(
        (214, 1),
        &node,
        (GATE_TRANSFEREE, graveyard),
        wrapper,
    ));
    let mut released = registrar_transfer_at(
        wrapper.parse()?,
        graveyard.parse()?,
        keccak256(b"gyeth"),
        214,
        4,
        registrar,
    );
    released.transaction_index = 1;
    released.transaction_hash = "transaction-214-1".to_owned();
    unwrap.push(released);
    rows.push((".eth", setup, vec![unwrap], node, GateGraveyardEnd::Nothing));
    let (mut setup, node) = child("gyaway", 0)?;
    setup.push(gate_new_owner(
        211,
        0,
        parent_node,
        "gyaway",
        GATE_AWAY_OWNER,
    ));
    setup.push(gate_new_owner(212, 0, parent_node, "gyaway", wrapper));
    let steps = unwrap_by_holder(&node);
    rows.push(("away", setup, steps, node, GateGraveyardEnd::Nothing));
    let build = |logs: Vec<RawLogInput>| gate_batch(logs, &fixture);
    let mut failures = Vec::new();
    for (row, setup, steps, node, end) in &rows {
        let chain = &setup[0].chain_id;
        let resource = crate::schema_v2::common::stable_uuid(&format!(
            "resource:registry-only:{chain}:{node}"
        ));
        for rerun in [false, true] {
            let mut steps = steps.clone();
            if rerun {
                steps.push(trigger.clone());
            }
            for (layout, outputs) in
                super::super::unwrapped_child_under_wrapped_parent::every_layout(
                    &build, setup, &steps,
                )?
            {
                let context = format!("{row}, {layout}{}", if rerun { ", re-run" } else { "" });
                let events = || {
                    outputs
                        .iter()
                        .flat_map(|output| output.normalized_events.iter())
                };
                if rerun
                    && !events().any(|event| {
                        event.event_kind == "MigrationApplied"
                            && event.after_state["migration_path"] == "locked_wrapped"
                            && event.consumer_visibility == "activated"
                    })
                {
                    failures.push(format!(
                        "{context}: the locked re-run trigger did not activate"
                    ));
                }
                let graveyard_grants = events()
                    .filter(|event| {
                        event.event_kind == "PermissionChanged"
                            && event.resource_id == Some(resource)
                            && event.after_state["effective_powers"]
                                .as_array()
                                .is_some_and(|powers| !powers.is_empty())
                            && event.after_state["subject"]
                                .as_str()
                                .is_some_and(|subject| subject.eq_ignore_ascii_case(graveyard))
                    })
                    .collect::<Vec<_>>();
                if !graveyard_grants.is_empty() {
                    failures.push(format!(
                        "{context}: the Graveyard was granted: {graveyard_grants:?}"
                    ));
                }
                let outputs = outputs.iter().collect::<Vec<_>>();
                let grants = super::super::unwrapped_child_under_wrapped_parent::scoped_grants_on(
                    &outputs, resource,
                );
                let holds = |subject: &str| {
                    grants
                        .iter()
                        .any(|(holder, _, _)| holder.eq_ignore_ascii_case(subject))
                };
                match end {
                    GateGraveyardEnd::Nothing if !grants.is_empty() => {
                        failures.push(format!("{context}: grants left: {grants:?}"));
                    }
                    GateGraveyardEnd::NameWrapperKeeps => {
                        let expected = [
                            (
                                wrapper.to_owned(),
                                "resource".to_owned(),
                                vec!["resource_control".to_owned()],
                            ),
                            (
                                wrapper.to_owned(),
                                format!("resolver:{GATE_RESOLVER}"),
                                vec!["resolver_control".to_owned()],
                            ),
                        ]
                        .into_iter()
                        .collect::<super::super::unwrapped_child_under_wrapped_parent::LiveGrants>();
                        if grants != expected {
                            failures.push(format!("{context}: grants {grants:?}"));
                        }
                    }
                    GateGraveyardEnd::NameWrapperEnds if holds(wrapper) => {
                        failures.push(format!(
                            "{context}: the NameWrapper keeps grants: {grants:?}"
                        ));
                    }
                    _ => {}
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// The locked guard with a registry-only predecessor. "lockaway" is created wrapped with fuses
/// 0 at block 210. Its record leaves the NameWrapper at 211 and comes back at 212 while the
/// parent is unwrapped, so the registry-only authority is selected. The parent's locked
/// ENSv1→ENSv2 migration follows at its `migration_block`, and the child's unwrap at
/// `unwrap_block`. With `expiry`, the old-lease expiry crossing of
/// `migration_and_old_lease_expiry` joins the same history, with that scenario's ENSv1→ENSv2
/// migration in its own later transaction of the shared block.
/// Returns the input, the child's node and its logical name id.
fn gate_lockaway_history(
    fixture: &Value,
    unwrap_block: i64,
    expiry: bool,
) -> anyhow::Result<(BatchInput, String, String)> {
    let scenario = &fixture["scenarios"]["C-01"];
    let wrapper = fixture["addresses"]["name_wrapper"].as_str().unwrap();
    let parent_node = scenario["parent"]["namehash"].as_str().unwrap();
    let node = namehash_under(&scenario["parent"]["namehash"], "lockaway")?;
    let migration_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let mut logs = gate_create_wrapped(210, 0, "lockaway", 0, fixture)?;
    logs.push(gate_new_owner(
        211,
        0,
        parent_node,
        "lockaway",
        GATE_AWAY_OWNER,
    ));
    logs.push(gate_new_owner(212, 0, parent_node, "lockaway", wrapper));
    logs.extend(level_logs(&scenario["parent"], &fixture["addresses"])?);
    logs.extend(gate_unwrap_tail(unwrap_block, &node, GATE_OWNER, wrapper));
    if !expiry {
        let id = format!("ens:{node}");
        return Ok((gate_batch(logs, fixture), node, id));
    }
    let mut input = super::expiry::migration_and_old_lease_expiry()?;
    for raw in &mut input.raw_logs {
        if raw.block_number == migration_block {
            raw.transaction_index = 2;
            raw.transaction_hash = format!("transaction-{migration_block}-2");
            raw.log_index += 50;
        }
    }
    input.blocks.extend(logs.iter().map(|raw| RawBlockInput {
        chain_id: raw.chain_id.clone(),
        block_hash: raw.block_hash.clone(),
        block_number: raw.block_number,
        block_timestamp: raw.block_timestamp,
        canonicality_state: raw.canonicality_state.clone(),
    }));
    input.blocks.sort_by_key(|block| block.block_number);
    input.blocks.dedup_by_key(|block| block.block_number);
    input.raw_logs.extend(logs);
    input.raw_logs = ordered(std::mem::take(&mut input.raw_logs));
    let id = format!("ens:{node}");
    Ok((input, node, id))
}

/// `template` with only the blocks and logs from `first_block` up to, not including,
/// `end_block`.
fn gate_slice(template: &BatchInput, first_block: i64, end_block: i64) -> BatchInput {
    let mut input = template.clone();
    let within = |block: i64| first_block <= block && block < end_block;
    input.blocks.retain(|block| within(block.block_number));
    input.raw_logs.retain(|raw| within(raw.block_number));
    input
}

/// A registry owner change on the child after the whole history, in a block of its own that
/// follows every block of `history`.
fn gate_lockaway_probe(history: &BatchInput, node: &str) -> BatchInput {
    let last = history
        .blocks
        .iter()
        .map(|block| (block.block_number, block.block_timestamp))
        .chain(
            history
                .raw_logs
                .iter()
                .map(|raw| (raw.block_number, raw.block_timestamp)),
        )
        .max()
        .expect("the history has blocks");
    let mut raw = gate_registry_transfer(last.0 + 2, 0, node, GATE_PROBE_OWNER);
    raw.block_timestamp = last.1 + time::Duration::seconds(24);
    let mut probe = history.clone();
    probe.blocks = vec![RawBlockInput {
        chain_id: raw.chain_id.clone(),
        block_hash: raw.block_hash.clone(),
        block_number: raw.block_number,
        block_timestamp: raw.block_timestamp,
        canonicality_state: raw.canonicality_state.clone(),
    }];
    probe.raw_logs = vec![raw];
    probe
}

/// A child's normalized events, its bindings and the positions of its binding closures.
type GateChildView = (Vec<NormalizedEvent>, Vec<String>, Vec<(i64, i64, i64)>);

/// What the layout table compares for the child: its normalized events, bindings and binding
/// closures from `first_block` on.
fn gate_child_from(output: &BatchOutput, id: &str, first_block: i64) -> GateChildView {
    let events = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.logical_name_id.as_deref() == Some(id)
                && event.block_number.is_some_and(|block| block >= first_block)
        })
        .cloned()
        .collect();
    let bindings = output
        .surface_bindings
        .iter()
        .filter(|binding| binding.logical_name_id == id && binding.block_number >= first_block)
        .map(|binding| format!("{binding:?}"))
        .collect();
    let closures = output
        .binding_closures
        .iter()
        .filter(|closure| closure.logical_name_id == id && closure.block_number >= first_block)
        .map(|closure| {
            (
                closure.block_number,
                closure.transaction_index,
                closure.log_index,
            )
        })
        .collect();
    (events, bindings, closures)
}

/// Whether the child's binding is closed at `unwrap_block` and stays closed through the end
/// of `outputs`, read in order.
fn gate_released_at(outputs: &[&BatchOutput], id: &str, unwrap_block: i64) -> bool {
    let mut released = false;
    for event in outputs
        .iter()
        .flat_map(|output| output.normalized_events.iter())
        .filter(|event| event.logical_name_id.as_deref() == Some(id))
    {
        match event.event_kind.as_str() {
            "SurfaceUnbound" if event.block_number == Some(unwrap_block) => released = true,
            "SurfaceBound" if released => released = false,
            _ => {}
        }
    }
    released
}

/// Whether the child's last binding event across `outputs`, read in order, binds its
/// registry-only authority on the declared registry path, and that binding is among the
/// outputs' bindings.
fn gate_holds_registry_binding(outputs: &[&BatchOutput], id: &str) -> bool {
    let last = outputs
        .iter()
        .flat_map(|output| output.normalized_events.iter())
        .rfind(|event| {
            event.logical_name_id.as_deref() == Some(id)
                && matches!(event.event_kind.as_str(), "SurfaceBound" | "SurfaceUnbound")
        });
    last.is_some_and(|event| {
        event.event_kind == "SurfaceBound"
            && event.after_state["authority_kind"] == "registry_only"
            && event.after_state["binding_kind"] == "declared_registry_path"
            && outputs
                .iter()
                .flat_map(|output| output.surface_bindings.iter())
                .any(|binding| {
                    binding.logical_name_id == id
                        && binding.binding_kind == "declared_registry_path"
                        && Some(binding.resource_id) == event.resource_id
                })
    })
}

/// The child's binding events as (block, kind, authority kind), for failure messages.
fn gate_binding_trace(output: &BatchOutput, id: &str) -> Vec<(Option<i64>, String, Value)> {
    output
        .normalized_events
        .iter()
        .filter(|event| {
            event.logical_name_id.as_deref() == Some(id)
                && matches!(event.event_kind.as_str(), "SurfaceBound" | "SurfaceUnbound")
        })
        .map(|event| {
            let state = if event.event_kind == "SurfaceBound" {
                &event.after_state
            } else {
                &event.before_state
            };
            (
                event.block_number,
                event.event_kind.clone(),
                state["authority_kind"].clone(),
            )
        })
        .collect()
}

/// The invariant behind the locked re-run trigger: a batch's output must not depend on where
/// the batch boundaries fall relative to a locked ENSv1→ENSv2 migration and an unwrap. Each
/// row cuts the same history at one of `cuts`, resumes the tail warm and restores it cold, and
/// checks it against the one-batch run: whether the child is released at its unwrap, the child's events,
/// bindings and closures from the cut on, and the child's events in a later registry owner
/// change. A forward tail's output carries no registry-only binding of its own. Every row is
/// checked, and the failing rows are reported together.
fn assert_gate_lockaway_layouts(
    fixture: &Value,
    unwrap_block: i64,
    expiry: bool,
    cuts: &[i64],
) -> anyhow::Result<()> {
    let migration_block = fixture["scenarios"]["C-01"]["parent"]["migration_block"]
        .as_i64()
        .unwrap();
    let forward = unwrap_block > migration_block;
    let (input, node, id) = gate_lockaway_history(fixture, unwrap_block, expiry)?;
    let probe = gate_lockaway_probe(&input, &node);
    let probe_block = probe.raw_logs[0].block_number;
    let variant = format!(
        "{} unwrap{}",
        if forward { "later" } else { "earlier" },
        if expiry {
            " with the expiry crossing"
        } else {
            ""
        }
    );
    let mut failures = Vec::new();
    let (whole, session) = interpret_test_batch_incremental(input.clone(), None)?;
    assert!(
        whole.normalized_events.iter().any(|event| {
            event.event_kind == "MigrationApplied"
                && event.after_state["migration_path"] == "locked_wrapped"
                && event.consumer_visibility == "activated"
        }),
        "{variant}, one batch: the parent's locked ENSv1→ENSv2 migration is proven"
    );
    if expiry {
        let expiry_block = input.blocks.iter().map(|block| block.block_number).max();
        super::expiry::assert_old_lease_does_not_reopen(&whole, expiry_block.unwrap());
    }
    if gate_released_at(&[&whole], &id, unwrap_block) != forward {
        failures.push(format!(
            "{variant}, one batch: released at the unwrap is not {forward}: {:?}",
            gate_binding_trace(&whole, &id)
        ));
    }
    if !forward && !gate_holds_registry_binding(&[&whole], &id) {
        failures.push(format!(
            "{variant}, one batch: the child lost its registry-only binding: {:?}",
            gate_binding_trace(&whole, &id)
        ));
    }
    let (whole_probe, _) = interpret_test_batch_incremental(probe.clone(), Some(session))?;
    for &cut in cuts {
        let head_input = gate_slice(&input, i64::MIN, cut);
        let tail_input = gate_slice(&input, cut, i64::MAX);
        let (head, session) = interpret_test_batch_incremental(head_input, None)?;
        let (warm, session) = interpret_test_batch_incremental(tail_input.clone(), Some(session))?;
        let (warm_probe, _) = interpret_test_batch_incremental(probe.clone(), Some(session))?;
        let mut cold_input = tail_input;
        cold_input.prior_events = head.normalized_events.iter().map(prior_event).collect();
        carry_admissions(&head, &mut cold_input.admissions);
        let cold = interpret_test_batch(cold_input)?;
        let mut cold_probe_input = probe.clone();
        cold_probe_input.prior_events = head
            .normalized_events
            .iter()
            .chain(cold.normalized_events.iter())
            .map(prior_event)
            .collect();
        carry_admissions(&head, &mut cold_probe_input.admissions);
        carry_admissions(&cold, &mut cold_probe_input.admissions);
        let cold_probe = interpret_test_batch(cold_probe_input)?;
        for (resume, tail, tail_probe) in
            [("warm", &warm, &warm_probe), ("cold", &cold, &cold_probe)]
        {
            let context = format!("{variant}, cut before block {cut}, {resume}");
            if forward
                && tail.normalized_events.iter().any(|event| {
                    event.event_kind == "SurfaceBound"
                        && event.after_state["authority_kind"] == "registry_only"
                })
            {
                failures.push(format!(
                    "{context}: the tail binds a registry-only authority of its own"
                ));
            }
            if gate_released_at(&[&head, tail], &id, unwrap_block) != forward {
                failures.push(format!(
                    "{context}: released at the unwrap is not {forward}: {:?}",
                    gate_binding_trace(tail, &id)
                ));
            }
            if !forward && !gate_holds_registry_binding(&[&head, tail], &id) {
                failures.push(format!(
                    "{context}: the child lost its registry-only binding: {:?} then {:?}",
                    gate_binding_trace(&head, &id),
                    gate_binding_trace(tail, &id)
                ));
            }
            if gate_child_from(tail, &id, cut) != gate_child_from(&whole, &id, cut) {
                failures.push(format!(
                    "{context}: the tail differs from the one-batch run: {:?} against {:?}",
                    gate_binding_trace(tail, &id),
                    gate_binding_trace(&whole, &id)
                ));
            }
            if gate_child_from(tail_probe, &id, probe_block)
                != gate_child_from(&whole_probe, &id, probe_block)
            {
                failures.push(format!(
                    "{context}: the later registry owner change differs from the one-batch run: \
                     {:?} against {:?}",
                    gate_binding_trace(tail_probe, &id),
                    gate_binding_trace(&whole_probe, &id)
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// The batch-layout table for the locked guard with a registry-only predecessor. The child's
/// unwrap after the parent's locked ENSv1→ENSv2 migration releases it in every layout: the
/// whole history in one batch, cut before the ENSv1→ENSv2 migration's block so it and the
/// unwrap share the tail, and cut after that block. The unwrap keeps the already-selected
/// registry-only authority, so the first pass of a tail holding the ENSv1→ENSv2 migration and
/// the unwrap binds nothing that could stand in for the locked boundary.
#[test]
fn registry_only_original_child_unwrap_after_locked_parent_migration_releases_in_every_layout()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let migration_block = fixture["scenarios"]["C-01"]["parent"]["migration_block"]
        .as_i64()
        .unwrap();
    let unwrap_block = migration_block + 1;
    let cuts = [migration_block, unwrap_block];
    assert_gate_lockaway_layouts(&fixture, unwrap_block, false, &cuts)
}

/// The same table with the old-lease expiry crossing in the history, so a tail that holds the
/// ENSv1→ENSv2 migration also takes the expiry reconciliation. The unlocked ENSv1→ENSv2
/// migration leaves that name's registry record with the Graveyard, so before the
/// reconciliation its lease release binds a registry-only authority of its own. The table checks the final outputs, where the
/// reconciliation has removed it.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/UnlockedMigrationController.sol:L111-L118 @ ens_v2_sepolia_20261001@07e55a05)
#[test]
fn registry_only_original_child_unwrap_after_locked_parent_migration_releases_with_an_expiry_crossing()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let migration_block = fixture["scenarios"]["C-01"]["parent"]["migration_block"]
        .as_i64()
        .unwrap();
    let unwrap_block = migration_block + 1;
    let cuts = [migration_block, unwrap_block];
    assert_gate_lockaway_layouts(&fixture, unwrap_block, true, &cuts)
}

/// The reverse order: the child's unwrap comes before the parent's locked ENSv1→ENSv2
/// migration, so its position suppresses nothing and the child keeps its
/// registry-only authority in every layout. Each layout also checks that the child still holds
/// that binding after the unwrap, so a binding dropped earlier fails too. The cuts put the
/// unwrap and the ENSv1→ENSv2 migration in one tail, then the unwrap in the head and the
/// ENSv1→ENSv2 migration alone in the tail.
#[test]
fn registry_only_original_child_unwrap_before_locked_parent_migration_keeps_its_binding_in_every_layout()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let migration_block = fixture["scenarios"]["C-01"]["parent"]["migration_block"]
        .as_i64()
        .unwrap();
    let unwrap_block = migration_block - 2;
    let cuts = [unwrap_block, migration_block];
    assert_gate_lockaway_layouts(&fixture, unwrap_block, false, &cuts)
}

/// An original child unwrapped before its parent's locked ENSv1→ENSv2 migration later in the
/// same batch opens the registry-only successor. The ENSv1→ENSv2 migration's position is after
/// the unwrap, so it suppresses nothing earlier, in the first pass and in the locked re-run
/// alike.
#[test]
fn original_child_unwrap_before_same_batch_locked_parent_migration_opens_the_successor()
-> anyhow::Result<()> {
    let fixture = fixture()?;
    let scenario = &fixture["scenarios"]["C-01"];
    let addresses = &fixture["addresses"];
    let wrapper = addresses["name_wrapper"].as_str().unwrap();
    let orig_node = namehash_under(&scenario["parent"]["namehash"], "orig")?;
    let orig_id = format!("ens:{orig_node}");
    let migration_block = scenario["parent"]["migration_block"].as_i64().unwrap();
    let mut logs = gate_create_wrapped(230, 0, "orig", 65_536, &fixture)?;
    logs.push(gate_new_resolver(230, 3, &orig_node));
    logs.extend(gate_unwrap_tail(
        migration_block - 1,
        &orig_node,
        GATE_OWNER,
        wrapper,
    ));
    logs.extend(level_logs(&scenario["parent"], addresses)?);
    let output = interpret_test_batch(gate_batch(logs, &fixture))?;
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "MigrationApplied"
                && event.after_state["migration_path"] == "locked_wrapped"
                && event.consumer_visibility == "activated"
        }),
        "the batch proves the parent's later locked ENSv1→ENSv2 migration"
    );
    assert!(
        output.normalized_events.iter().any(|event| {
            event.event_kind == "SurfaceBound"
                && event.block_number == Some(migration_block - 1)
                && event.logical_name_id.as_deref() == Some(orig_id.as_str())
                && event.after_state["authority_kind"] == "registry_only"
        }),
        "the unwrap before the ENSv1→ENSv2 migration opened no registry-only successor: {:?}",
        output.normalized_events
    );
    Ok(())
}

fn gate_successor_open(output: &BatchOutput, logical_name_id: &str) -> bool {
    output.surface_bindings.iter().any(|binding| {
        binding.logical_name_id == logical_name_id && binding.authority_arm == "ens_v1"
    }) && output.normalized_events.iter().any(|event| {
        event.event_kind == "SurfaceBound"
            && event.logical_name_id.as_deref() == Some(logical_name_id)
            && event.after_state["authority_kind"] == "registry_only"
    })
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

#[path = "child_registry_holder.rs"]
mod registry_holder;
