//! Generated interpreter sequences checked against invariants that must hold for any ordering.
//!
//! The scenario axes adapt the ENSv1-to-ENSv2 migration scenario catalog's dimension space —
//! `dimensions.md` on the `worknotes/migration-catalog` branch, sections D1 to D7 — to what an
//! interpreter can observe: wrap state, resolver and record state, subname shape, expiry window,
//! authorization shape, which controller registered the name, and post-registration perturbations.
//! The catalog's D6 enumerates migration routes, which have no interpreter-level counterpart, and
//! several of its D7 perturbations are unreachable on a migrated node; the axes here are the
//! observable projection of that space, not a section-for-section copy. Wrapper fuse words are
//! emitted so the event shape stays realistic, and no invariant here reads a fuse value — though
//! the coverage floor does require the kind the wrapper derives from a fuse-bearing wrap.
//!
//! Knobs:
//! - `BIGNAME_PERMUTATION_CASES` — permutations per protocol world. Default 48 (144 sequences per
//!   run) keeps the lane inside the CI budget; raise it for deeper local sweeps.
//! - `BIGNAME_PERMUTATION_SEED` — base seed, decimal. Default 1846370029.
//!
//! The knobs drop some of the assertions about what the corpus reaches, because those are
//! properties of the default corpus rather than of any seed: a reduced or reseeded run drops the
//! interpretation-coverage floor, and any run that is not exactly the default corpus also drops the
//! exact artifact, detach, and burst-reach counts and the volume floors, which a deeper sweep would
//! legitimately exceed. A deeper sweep
//! at the default seed keeps the coverage floor, which only grows. The invariants themselves — the
//! ones a failure would report — run on every sequence whatever the knobs say.
//!
//! A failure reports `world=… seed=…`. Replay it with that seed and
//! `BIGNAME_PERMUTATION_CASES=1`. What a seed generates depends only on the seed and the scenario
//! pools — addresses come from the world's own base, not from a manifest — but a scenario carries
//! the manifest payloads into the interpreter, so a manifest edit changes what that unchanged log
//! sequence derives.

mod permutation;

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{Address, B256, LogData, U256, keccak256};
use alloy_sol_types::SolEvent;
use anyhow::{Context, Result, bail};
use bigname_adapters::schema_v2::{
    BatchInput, BatchOutput, interpret_schema_v2_batch,
    seam::{self, INTERPRETER_STATE_KEY},
};
use serde_json::Value;

use permutation::{
    convergence::BatchBoundaryArtifacts,
    directed::Directed,
    events::{
        V1LegacyController, V1Registry, V1Resolver, V1UnwrappedController, V1WrappedController,
        V1Wrapper, V2Registry, V2Resolver, declared_events,
    },
    invariants::{IdentityReferences, assert_upsert_guards_agree, converge, split},
    names::{dns_encode, labelhash, namehash},
    scenario::{self, BurstPhase},
    world::{
        BlockSpec, ENS_V1_MAINNET, ENS_V1_SEPOLIA, ENS_V2_SEPOLIA, GeneratedLog, Wiring, World,
        assert_pins_are_current, assert_worlds_cover_deployments, checked_in_manifests,
        declared_event_kinds, declared_event_topics,
    },
};

/// 48 permutations per world is a runtime budget. The directed
/// `wrapped_past_grace_lapse_is_batch_grid_independent` test pins issue #347 independently of the
/// generator depth that first exposed it.
const DEFAULT_CASES: u64 = 48;
const DEFAULT_SEED: u64 = 0x6e0d_5eed;
/// Distance between case seeds. Deliberately *not* the SplitMix64 increment: because that increment
/// is odd it is invertible, so every stride makes two cases the same value stream offset by some
/// fixed number of draws, and a stride equal to the increment makes that offset one. How far this
/// one puts them is asserted rather than asserted-to-be-large in a comment — see
/// `generated_scenarios_are_reproducible_from_their_seed`.
const CASE_STRIDE: u64 = 0xd134_2543_de82_ef95;
const SPLIT_SALT: u64 = 0xa076_1d64_78bd_642f;
const WORLDS: [&World; 3] = [&ENS_V1_MAINNET, &ENS_V1_SEPOLIA, &ENS_V2_SEPOLIA];
/// Any timestamp works for coverage; the axes decide which events a pool contains, not the clock.
const SETTLE_TIMESTAMP: i64 = 1_700_000_000;

#[test]
fn generated_interpreter_permutations_hold_identity_and_replay_invariants() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let cases = knob("BIGNAME_PERMUTATION_CASES", DEFAULT_CASES)?;
    let base = knob("BIGNAME_PERMUTATION_SEED", DEFAULT_SEED)?;
    if cases == 0 {
        bail!("BIGNAME_PERMUTATION_CASES must be at least 1");
    }
    let mut failures = Vec::new();
    let mut artifacts: BTreeMap<&str, BatchBoundaryArtifacts> = BTreeMap::new();
    let mut subregistry_detaches: BTreeMap<&str, usize> = BTreeMap::new();
    let mut emitted_topic0s = BTreeSet::new();
    let mut event_kinds: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    let mut derived = Vec::new();
    let mut burst_reach: BTreeMap<&str, BurstReach> = BTreeMap::new();
    let mut forced_cache_misses = 0_usize;
    for world in WORLDS {
        let wiring = Wiring::build(world, &checked_in)?;
        let declared = wiring.declared_instances();
        let manifest_ids = wiring.manifest_ids();
        let mut events = 0_usize;
        let mut logs = 0_usize;
        let mut world_artifacts = BatchBoundaryArtifacts::default();
        let mut world_detaches = 0_usize;
        let mut world_burst = BurstReach::default();
        for case in 0..cases {
            let seed = base.wrapping_add(case.wrapping_mul(CASE_STRIDE));
            let scenario = scenario::generate(world, &wiring, seed);
            logs += scenario.logs.len();
            emitted_topic0s.extend(
                scenario
                    .logs
                    .iter()
                    .filter_map(|log| log.topics.first())
                    .map(|topic| topic.to_ascii_lowercase()),
            );
            world_burst.cases += usize::from(scenario.dimensions.pre_registration_burst);
            // Absolute chain positions of the logs the burst added, with the phase the generator
            // claims for each, so the run can count how many of them the interpretation actually
            // derives an event from, per phase.
            let burst_positions: BTreeMap<(i64, i64, i64), BurstPhase> = scenario
                .logs
                .iter()
                .filter_map(|log| {
                    log.burst.map(|phase| {
                        (
                            (
                                scenario.blocks[log.block_index].number,
                                log.transaction_index,
                                log.log_index,
                            ),
                            phase,
                        )
                    })
                })
                .collect();
            let context = scenario.describe();
            let input = wiring.batch_input(&scenario.blocks, &scenario.logs)?;
            let batches = split(input.blocks.len(), seed ^ SPLIT_SALT);
            if scenario.dimensions.pre_registration_burst {
                match verify_burst_phases(&context, &scenario, &batches) {
                    Ok(cross_batch) => {
                        world_burst.cross_batch_cases += usize::from(cross_batch);
                    }
                    Err(error) => failures.push(format!("{error:?}")),
                }
            }
            match check(
                &context,
                world,
                &declared,
                &manifest_ids,
                input,
                batches,
                &burst_positions,
            ) {
                Ok(outcome) => {
                    // Per sequence, not just per world: an admission or role mismatch that dropped
                    // most sequences would leave the world total positive and the kind floor
                    // satisfied by whichever few still derived anything.
                    if outcome.events == 0 && !scenario.logs.is_empty() {
                        failures.push(format!(
                            "{context}: derived no normalized events from {} raw logs",
                            scenario.logs.len()
                        ));
                    }
                    events += outcome.events;
                    forced_cache_misses =
                        forced_cache_misses.saturating_add(outcome.tiny_cache_misses);
                    event_kinds
                        .entry(world.label)
                        .or_default()
                        .extend(outcome.event_kinds);
                    world_artifacts.absorb(outcome.artifacts);
                    world_detaches += outcome.subregistry_detaches;
                    for (total, derived) in world_burst
                        .derivations
                        .iter_mut()
                        .zip(outcome.burst_derivations)
                    {
                        *total += derived;
                    }
                }
                Err(error) => failures.push(format!("{error:?}")),
            }
        }
        eprintln!(
            "permutation_lane world={} sequences={cases} raw_logs={logs} normalized_events={events}",
            world.label
        );
        eprintln!(
            "permutation_lane world={} batch_boundary_artifacts: {world_artifacts} subregistry_detaches={world_detaches}",
            world.label
        );
        artifacts.insert(world.label, world_artifacts);
        subregistry_detaches.insert(world.label, world_detaches);
        derived.push((world.label, events, logs));
        burst_reach.insert(world.label, world_burst);
    }
    for (world, kinds) in &event_kinds {
        eprintln!(
            "permutation_lane world={world} derived_event_kinds={:?}",
            kinds.iter().collect::<Vec<_>>()
        );
    }
    if !failures.is_empty() {
        bail!(
            "{} of {} generated sequences failed:\n\n{}",
            failures.len(),
            cases * WORLDS.len() as u64,
            failures.join("\n\n")
        );
    }
    if forced_cache_misses == 0 {
        bail!("permutation lane forced no tiny-cache database reloads");
    }
    eprintln!("permutation_lane tiny_cache_misses={forced_cache_misses}");
    // Guards against one world going dark: the other world's events would keep an aggregate count
    // positive while every invariant here passed over empty vectors.
    for (label, events, logs) in &derived {
        if *events == 0 {
            bail!("{label}: derived no normalized events from {logs} raw logs");
        }
    }
    // Which interpretation paths a run reaches is a property of the corpus it drew, so only the
    // default corpus asserts it. A reduced or reseeded run is a reproduction tool, not a gate.
    if cases < DEFAULT_CASES || base != DEFAULT_SEED {
        return Ok(());
    }
    for world in WORLDS {
        assert_declared_kinds_are_reached(
            world.label,
            &declared_event_kinds(world, &checked_in)?,
            event_kinds.get(world.label),
        )?;
    }
    assert_interpretation_coverage(&event_kinds, emitted_topic0s.len())?;
    // Coverage only grows with a deeper sweep, but the pinned counts below are exact, so a sweep
    // that draws more sequences than the default reports them rather than gating on them.
    if cases != DEFAULT_CASES {
        return Ok(());
    }
    assert_pinned_artifacts(&artifacts, &subregistry_detaches)?;
    assert_burst_reach(&burst_reach)?;
    assert_volume_floors(&derived)
}

#[test]
fn production_lease_release_sequence_holds_the_same_invariants() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let directed = Directed::lease_release(&checked_in)?;
    let context = format!("directed={}", directed.id);
    let chain_id = directed.input.chain_id.clone();
    let converged = converge(&context, directed.input.clone(), directed.batches.clone())?;
    let mut references = IdentityReferences::new(
        &chain_id,
        &directed.declared_instances,
        &directed.manifest_ids,
    );
    for batch in &converged.batches {
        references.absorb(&context, &batch.blocks, &batch.output)?;
    }
    let whole = format!("{context} whole-sequence pass");
    IdentityReferences::new(
        &chain_id,
        &directed.declared_instances,
        &directed.manifest_ids,
    )
    .absorb(&whole, &converged.whole.blocks, &converged.whole.output)?;
    assert_upsert_guards_agree(&whole, &converged.whole.output)?;
    let outputs = converged
        .batches
        .into_iter()
        .map(|batch| batch.output)
        .collect::<Vec<_>>();
    directed.assert_release_reached(&outputs)
}

#[test]
fn wrapped_past_grace_lapse_is_batch_grid_independent() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V1_MAINNET, &checked_in)?;
    let input = wrapped_past_grace_lapse_input(&wiring)?;
    let boundary_block = input.blocks[1].block_number;
    let converged = converge("directed=wrapped-past-grace-lapse", input, vec![0..1, 1..2])?;
    let boundary_kinds = converged
        .whole
        .output
        .normalized_events
        .iter()
        .filter(|event| event.block_number == Some(boundary_block))
        .map(|event| event.event_kind.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        boundary_kinds,
        [
            "RegistrationReleased",
            "PermissionChanged",
            "SurfaceUnbound",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "PermissionChanged",
        ],
        "the observing block must carry the complete lease-lapse transition"
    );
    Ok(())
}

#[test]
fn v2_alias_observed_record_name_link_is_batch_grid_independent() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let node = namehash(&["alias", "eth"]);
    let expected_name = format!("ens:{node:#x}");
    let input = v2_alias_observed_record_input(&wiring)?;
    let converged = converge(
        "directed=v2-alias-observed-record-name-link",
        input,
        vec![0..1, 1..2],
    )?;
    let whole_record = converged
        .whole
        .output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .context("whole pass omitted the resolver record change")?;
    let split_record = converged
        .batches
        .iter()
        .flat_map(|batch| &batch.output.normalized_events)
        .find(|event| event.event_kind == "RecordChanged")
        .context("split replay omitted the resolver record change")?;
    assert_eq!(
        (
            whole_record.logical_name_id.as_deref(),
            split_record.logical_name_id.as_deref(),
            converged.artifacts.rebased_attributions,
        ),
        (
            Some(expected_name.as_str()),
            Some(expected_name.as_str()),
            0
        ),
        "alias-observed names must survive restored and resumed batch boundaries"
    );
    Ok(())
}

#[test]
#[ignore = "pre-existing named-resource/alias retained preimage key collision"]
// Known defect: named-resource and alias preimages can compact onto the same retained key. Keep
// this probe as evidence until a separate issue and fix domain-separate those observation classes.
fn v2_named_resource_alias_retained_key_collision_is_batch_grid_independent() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let input = v2_named_resource_alias_collision_input(&wiring)?;
    let whole = interpret_schema_v2_batch(input.clone())?;
    let collision_name = format!("ens:{:#x}", namehash(&["collision", "eth"]));
    let preimage_key = |source_event: &str| {
        whole
            .normalized_events
            .iter()
            .find(|event| {
                event.event_kind == "PreimageObserved"
                    && event.logical_name_id.as_deref() == Some(collision_name.as_str())
                    && event
                        .after_state
                        .get("source_event")
                        .and_then(Value::as_str)
                        == Some(source_event)
            })
            .and_then(|event| {
                event
                    .raw_fact_ref
                    .get(INTERPRETER_STATE_KEY)
                    .and_then(Value::as_str)
            })
            .map(str::to_owned)
            .with_context(|| format!("whole pass omitted the {source_event} preimage key"))
    };
    let named_key = preimage_key("NamedAddrResource")?;
    let alias_key = preimage_key("AliasChanged")?;
    assert_eq!(
        named_key, alias_key,
        "the probe must exercise the named-resource/alias retained-key collision"
    );
    let retained = seam::fold_prior_events(Vec::new(), &whole.normalized_events, &input.blocks)?;
    let collided_rows = retained
        .iter()
        .filter(|event| event.retained_state_key == format!("state:{named_key}"))
        .collect::<Vec<_>>();
    assert_eq!(
        collided_rows.len(),
        1,
        "both observations must compact onto one retained key"
    );
    assert_eq!(
        collided_rows[0]
            .after_state
            .get("source_event")
            .and_then(Value::as_str),
        Some("AliasChanged"),
        "the later alias observation must win retained-key compaction"
    );

    converge(
        "directed=v2-named-resource-alias-retained-key-collision",
        input,
        vec![0..2, 2..4],
    )?;
    Ok(())
}

#[test]
fn v2_unregistered_record_name_link_is_batch_grid_independent() -> Result<()> {
    // Four logs distilled from generated seed 17846172577370688067: register and link a
    // resource without a resolver, unregister, then write the late resolver record in a later
    // block.
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let node = namehash(&["alpha", "eth"]);
    let expected_name = format!("ens:{node:#x}");
    let input = v2_released_name_record_input(
        &wiring,
        vec![
            V2Resolver::NameChanged {
                node,
                name: "alpha.eth".to_owned(),
            }
            .encode_log_data(),
        ],
    )?;
    let converged = converge(
        "directed=v2-unregistered-record-name-link",
        input,
        vec![0..2, 2..3],
    )?;
    let whole_record = converged
        .whole
        .output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .context("whole pass omitted the resolver record change")?;
    let split_record = converged
        .batches
        .iter()
        .flat_map(|batch| &batch.output.normalized_events)
        .find(|event| event.event_kind == "RecordChanged")
        .context("split replay omitted the resolver record change")?;
    assert_eq!(
        whole_record.logical_name_id, split_record.logical_name_id,
        "the durable namehash-to-name link must not depend on the physical batch boundary"
    );
    assert_eq!(
        whole_record.logical_name_id.as_deref(),
        Some(expected_name.as_str())
    );
    assert_eq!(
        whole_record.resource_id, None,
        "a released registration must not leave its resource on the late record"
    );
    assert_eq!(whole_record.resource_id, split_record.resource_id);
    assert!(
        converged.artifacts.counts().is_empty(),
        "the directed replay retained batch-boundary artifacts: {}",
        converged.artifacts
    );
    Ok(())
}

#[test]
fn v2_regeneration_collision_closes_displaced_registration_in_every_replay_shape() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    for (case, collision) in [
        ("regeneration-collision", true),
        ("unregister-comparator", false),
    ] {
        let input = v2_regeneration_collision_input(&wiring, collision)?;
        let fresh = interpret_schema_v2_batch(input.clone())?;
        assert_v2_regeneration_collision_output(&fresh, collision)?;
        let converged = converge(
            &format!("directed=v2-{case}"),
            input,
            vec![0..1, 1..2, 2..3, 3..4, 4..5],
        )?;
        assert!(
            converged.artifacts.counts().is_empty(),
            "{case} retained batch-boundary byte differences: {}",
            converged.artifacts
        );
    }
    Ok(())
}

#[test]
fn v2_label_registration_preserves_a_shared_subregistry_observation_key() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let input = v2_shared_subregistry_observation_input(&wiring, SharedSubregistryCase::Label)?;
    let output = interpret_schema_v2_batch(input)?;
    let observation_key = v2_shared_subregistry_observation_key(&wiring);

    assert!(
        output.discovery_edge_closures.iter().all(|closure| {
            closure.active_to_block_number != 20_000_202
                || closure.edge_kind != "subregistry"
                || closure.observation_key != observation_key
        }),
        "the second live token closed its co-holder's subregistry edge: {:#?}",
        output.discovery_edge_closures
    );
    let converged = converge(
        "directed=v2-shared-subregistry-label-registration",
        v2_shared_subregistry_observation_input(&wiring, SharedSubregistryCase::Label)?,
        vec![0..1, 1..2, 2..3],
    )?;
    assert!(converged.artifacts.counts().is_empty());
    Ok(())
}

#[test]
fn v2_regeneration_collision_preserves_a_shared_subregistry_observation_key() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let input = v2_shared_subregistry_observation_input(&wiring, SharedSubregistryCase::Collision)?;
    let output = interpret_schema_v2_batch(input)?;
    let observation_key = v2_shared_subregistry_observation_key(&wiring);

    assert!(
        output.discovery_edge_closures.iter().all(|closure| {
            closure.active_to_block_number != 20_000_205
                || closure.edge_kind != "subregistry"
                || closure.observation_key != observation_key
        }),
        "the displaced token close retired its live co-holder's subregistry edge: {:#?}",
        output.discovery_edge_closures
    );
    let converged = converge(
        "directed=v2-shared-subregistry-regeneration-collision",
        v2_shared_subregistry_observation_input(&wiring, SharedSubregistryCase::Collision)?,
        vec![0..1, 1..2, 2..3, 3..4, 4..5, 5..6],
    )?;
    assert!(converged.artifacts.counts().is_empty());
    Ok(())
}

#[test]
fn v2_label_replacement_preserves_a_shared_subregistry_observation_key() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let input =
        v2_shared_subregistry_observation_input(&wiring, SharedSubregistryCase::Replacement)?;
    let output = interpret_schema_v2_batch(input.clone())?;
    let observation_key = v2_shared_subregistry_observation_key(&wiring);

    assert!(output.discovery_edge_closures.iter().all(|closure| {
        closure.active_to_block_number != 20_000_203
            || closure.edge_kind != "subregistry"
            || closure.observation_key != observation_key
    }));
    let converged = converge(
        "directed=v2-shared-subregistry-label-replacement",
        input,
        vec![0..1, 1..2, 2..3, 3..4],
    )?;
    assert!(converged.artifacts.counts().is_empty());
    Ok(())
}

#[test]
fn v2_unregister_preserves_a_shared_subregistry_key_until_the_last_holder_leaves() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let input =
        v2_shared_subregistry_observation_input(&wiring, SharedSubregistryCase::Unregister)?;
    let output = interpret_schema_v2_batch(input.clone())?;
    let observation_key = v2_shared_subregistry_observation_key(&wiring);

    assert!(output.discovery_edge_closures.iter().all(|closure| {
        closure.active_to_block_number != 20_000_203
            || closure.edge_kind != "subregistry"
            || closure.observation_key != observation_key
    }));
    assert!(output.discovery_edge_closures.iter().any(|closure| {
        closure.active_to_block_number == 20_000_204
            && closure.edge_kind == "subregistry"
            && closure.observation_key == observation_key
    }));
    let converged = converge(
        "directed=v2-shared-subregistry-unregister",
        input,
        vec![0..1, 1..2, 2..3, 3..4, 4..5],
    )?;
    assert!(converged.artifacts.counts().is_empty());
    Ok(())
}

#[test]
fn v2_regeneration_collision_preserves_resolver_intake_until_explicit_update() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let registry = wiring.address("ens_v2_registry_l1", "registry");
    let observation_key = |token: U256| {
        let mut bytes = token.to_be_bytes::<32>();
        bytes[28..].fill(0);
        format!(
            "resolver:{}:{:#x}",
            registry.to_ascii_lowercase(),
            U256::from_be_bytes(bytes)
        )
    };
    for (case, same_observation_key, source_key_retired, source_key_reused) in [
        ("cross-label", false, false, false),
        ("retired-source-key", false, true, false),
        ("live-source-key-reuse", false, false, true),
        ("same-key", true, false, false),
    ] {
        let input = v2_regeneration_collision_input_with_topology(
            &wiring,
            same_observation_key,
            source_key_retired,
            source_key_reused,
        )?;
        let fresh = interpret_schema_v2_batch(input.clone())?;
        let (token_a, token_b) = v2_regeneration_collision_tokens(same_observation_key);
        let old_key = observation_key(token_b);
        let new_key = observation_key(token_a);
        if source_key_retired {
            assert!(fresh.discovery_edge_closures.iter().any(|closure| {
                closure.active_to_block_number == 20_000_001
                    && closure.edge_kind == "resolver"
                    && closure.observation_key == old_key
            }));
        }
        if source_key_reused {
            assert!(fresh.discovery_edges.iter().any(|edge| {
                edge.active_from_block_number == 20_000_003
                    && edge.edge_kind == "resolver"
                    && edge.observation_key == old_key
            }));
        }
        if same_observation_key {
            assert_eq!(old_key, new_key);
            let displaced_resolver = fresh
                .contract_addresses
                .iter()
                .find(|address| address.address == "0x00000000000000000000000000000000f0000098")
                .context("the aliased displaced resolver was not materialized")?
                .contract_instance_id;
            assert!(fresh.discovery_edges.iter().any(|edge| {
                edge.active_from_block_number == 20_000_001
                    && edge.edge_kind == "resolver"
                    && edge.observation_key == new_key
                    && edge.to_contract_instance_id == displaced_resolver
            }));
            assert!(!fresh.discovery_edge_closures.iter().any(|closure| {
                closure.active_to_block_number == 20_000_002
                    && closure.edge_kind == "resolver"
                    && closure.observation_key == old_key
            }));
            assert!(!fresh.discovery_edges.iter().any(|edge| {
                edge.active_from_block_number == 20_000_002
                    && edge.edge_kind == "resolver"
                    && edge.observation_key == new_key
            }));
        }
        assert!(!fresh.discovery_edge_closures.iter().any(|closure| {
            closure.active_to_block_number == 20_000_002
                && closure.edge_kind == "resolver"
                && closure.observation_key == old_key
        }));
        assert!(!fresh.discovery_edges.iter().any(|edge| {
            edge.active_from_block_number == 20_000_002
                && edge.edge_kind == "resolver"
                && edge.observation_key == new_key
        }));
        assert_eq!(
            fresh.discovery_edge_closures.iter().any(|closure| {
                closure.active_to_block_number == 20_000_004
                    && closure.edge_kind == "resolver"
                    && closure.observation_key == old_key
            }),
            !source_key_reused,
        );
        assert!(fresh.discovery_edges.iter().any(|edge| {
            edge.active_from_block_number == 20_000_004
                && edge.edge_kind == "resolver"
                && edge.observation_key == new_key
        }));
        let converged = converge(
            &format!("directed=v2-regeneration-collision-survivor-topology-{case}"),
            input,
            vec![0..1, 1..2, 2..3, 3..4, 4..5],
        )?;
        assert!(
            converged
                .whole
                .output
                .normalized_events
                .iter()
                .all(|event| {
                    event.after_state["source_event"] != "TokenRegenerated"
                        || !matches!(
                            event.event_kind.as_str(),
                            "ResolverChanged" | "SubregistryChanged"
                        )
                }),
            "a registration collision must not clear topology inherited by the surviving token"
        );
    }
    Ok(())
}

#[test]
fn v2_regeneration_alias_protection_survives_repeated_token_restore() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let input = v2_repeated_regeneration_alias_input(&wiring)?;
    let registry = wiring.address("ens_v2_registry_l1", "registry");
    let shared = versioned_token("beta", 1);
    let mut bytes = shared.to_be_bytes::<32>();
    bytes[28..].fill(0);
    let observation_key = format!(
        "resolver:{}:{:#x}",
        registry.to_ascii_lowercase(),
        U256::from_be_bytes(bytes)
    );
    let fresh = interpret_schema_v2_batch(input.clone())?;
    assert!(!fresh.discovery_edge_closures.iter().any(|closure| {
        closure.active_to_block_number == 20_000_105
            && closure.edge_kind == "resolver"
            && closure.observation_key == observation_key
    }));
    let converged = converge(
        "directed=v2-repeated-regeneration-resolver-alias",
        input,
        vec![0..1, 1..2, 2..3, 3..4, 4..5, 5..6],
    )?;
    assert!(converged.artifacts.counts().is_empty());
    Ok(())
}

#[test]
fn v2_regeneration_collision_closes_displaced_discovery_and_child_registrations() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let input = v2_regeneration_collision_displaced_subregistry_input(&wiring)?;
    let fresh = interpret_schema_v2_batch(input.clone())?;
    let discovery_closures = fresh
        .discovery_edge_closures
        .iter()
        .filter(|closure| closure.active_to_block_number == 20_000_105)
        .map(|closure| {
            assert_eq!(closure.except_to_contract_instance_id, None);
            closure.edge_kind.as_str()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        discovery_closures,
        BTreeSet::from(["resolver", "subregistry"]),
        "the collision must close both discovery edges owned by the displaced registration"
    );

    let child_name = format!("ens:{:#x}", namehash(&["kid", "alpha", "eth"]));
    let child_terminal_kinds = fresh
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number == Some(20_000_105)
                && event.logical_name_id.as_deref() == Some(child_name.as_str())
                && event.after_state["source_event"] == "TokenRegenerated"
        })
        .map(|event| event.event_kind.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        child_terminal_kinds,
        BTreeSet::from(["RegistrationReleased", "SurfaceUnbound"]),
        "removing the displaced parent path must cascade terminal child events"
    );
    assert!(fresh.binding_closures.iter().any(|closure| {
        closure.block_number == 20_000_105
            && closure.logical_name_id == child_name
            && closure.authority_arm == "ens_v2"
    }));

    let converged = converge(
        "directed=v2-regeneration-collision-displaced-subregistry",
        input,
        vec![0..1, 1..2, 2..3, 3..4, 4..5, 5..6],
    )?;
    assert!(
        converged.artifacts.counts().is_empty(),
        "the displaced child cascade retained batch-boundary byte differences: {}",
        converged.artifacts
    );
    Ok(())
}

#[test]
fn v2_regeneration_collision_releases_a_resource_pending_displaced_registration() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let mut input = v2_regeneration_collision_input(&wiring, true)?;
    input.raw_logs.retain(|raw| {
        !(raw.block_number == 20_000_000 && raw.transaction_index == 1 && raw.log_index == 0)
    });
    let fresh = interpret_schema_v2_batch(input.clone())?;
    let alpha = format!("ens:{:#x}", namehash(&["alpha", "eth"]));
    let regenerated = fresh
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "TokenRegenerated")
        .context("the survivor must regenerate")?;
    let released = fresh
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "RegistrationReleased"
                && event.logical_name_id.as_deref() == Some(alpha.as_str())
                && event.after_state["source_event"] == "TokenRegenerated"
        })
        .context("the resource-pending displaced registration must release")?;
    assert_eq!(released.resource_id, None);
    assert_eq!(
        released.after_state["token_id"], regenerated.after_state["new_token_id"],
        "the synthetic release carries the survivor's destination token id"
    );
    assert_eq!(
        released.after_state["terminal_reason"],
        "registry_name_binding_changed"
    );
    assert!(fresh.normalized_events.iter().all(|event| {
        event.event_kind != "SurfaceUnbound"
            || event.logical_name_id.as_deref() != Some(alpha.as_str())
    }));
    assert!(fresh.binding_closures.iter().any(|closure| {
        closure.block_number == 20_000_002
            && closure.logical_name_id == alpha
            && closure.authority_arm == "ens_v2"
    }));

    let converged = converge(
        "directed=v2-regeneration-collision-resource-pending-displaced-registration",
        input,
        vec![0..1, 1..2, 2..3, 3..4, 4..5],
    )?;
    assert!(converged.artifacts.counts().is_empty());
    Ok(())
}

fn assert_v2_regeneration_collision_output(output: &BatchOutput, collision: bool) -> Result<()> {
    let alpha = format!("ens:{:#x}", namehash(&["alpha", "eth"]));
    let beta = format!("ens:{:#x}", namehash(&["beta", "eth"]));
    let alpha_resource = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(alpha.as_str())
        })
        .and_then(|event| event.resource_id)
        .context("alpha's surface must be bound before displacement")?;
    let beta_resource = output
        .normalized_events
        .iter()
        .find(|event| {
            event.event_kind == "SurfaceBound"
                && event.logical_name_id.as_deref() == Some(beta.as_str())
        })
        .and_then(|event| event.resource_id)
        .context("beta's surface must be bound before regeneration")?;
    let source_event = if collision {
        "TokenRegenerated"
    } else {
        "LabelUnregistered"
    };
    let released = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number == Some(20_000_002)
                && event.event_kind == "RegistrationReleased"
                && event.logical_name_id.as_deref() == Some(alpha.as_str())
                && event.resource_id == Some(alpha_resource)
                && event.after_state["source_event"] == source_event
        })
        .count();
    let unbound = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number == Some(20_000_002)
                && event.event_kind == "SurfaceUnbound"
                && event.logical_name_id.as_deref() == Some(alpha.as_str())
                && event.resource_id == Some(alpha_resource)
                && event.after_state["source_event"] == source_event
        })
        .count();
    let terminal_closures = output
        .binding_closures
        .iter()
        .filter(|closure| {
            closure.block_number == 20_000_002
                && closure.logical_name_id == alpha
                && closure.authority_arm == "ens_v2"
        })
        .count();
    assert_eq!(
        (released, unbound, terminal_closures),
        (1, 1, 1),
        "the displaced registration must have the comparator's exact terminal shape"
    );

    let renewals = output
        .normalized_events
        .iter()
        .filter(|event| {
            event.block_number == Some(20_000_003) && event.event_kind == "RegistrationRenewed"
        })
        .collect::<Vec<_>>();
    if collision {
        assert_eq!(renewals.len(), 1, "only the surviving token is renewed");
        assert_eq!(renewals[0].logical_name_id.as_deref(), Some(beta.as_str()));
        assert_eq!(renewals[0].resource_id, Some(beta_resource));
    } else {
        assert!(
            renewals.is_empty(),
            "the unregistered token cannot be renewed"
        );
    }

    let record = output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .context("the late resolver record must be interpreted")?;
    assert_eq!(record.logical_name_id.as_deref(), Some(alpha.as_str()));
    assert_eq!(
        record.resource_id, None,
        "the late record must not retain the displaced registration's resource"
    );
    Ok(())
}

/// `clearRecords` emits `VersionChanged` for the node after incrementing its record version.
/// (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L247-L254 @
/// ens_v2@ccaeb58)
#[test]
fn v2_unregistered_record_version_name_link_is_batch_grid_independent() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let node = namehash(&["alpha", "eth"]);
    let expected_name = format!("ens:{node:#x}");
    let input = v2_released_name_record_input(
        &wiring,
        vec![
            V2Resolver::VersionChanged {
                node,
                newVersion: 1,
            }
            .encode_log_data(),
        ],
    )?;
    let converged = converge(
        "directed=v2-unregistered-record-version-name-link",
        input,
        vec![0..2, 2..3],
    )?;
    let whole_record = converged
        .whole
        .output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordVersionChanged")
        .context("whole pass omitted the resolver record-version change")?;
    let split_record = converged
        .batches
        .iter()
        .flat_map(|batch| &batch.output.normalized_events)
        .find(|event| event.event_kind == "RecordVersionChanged")
        .context("split replay omitted the resolver record-version change")?;
    assert_eq!(whole_record.logical_name_id, split_record.logical_name_id);
    assert_eq!(
        whole_record.logical_name_id.as_deref(),
        Some(expected_name.as_str())
    );
    assert_eq!(whole_record.resource_id, None);
    assert_eq!(whole_record.resource_id, split_record.resource_id);
    assert!(
        converged.artifacts.counts().is_empty(),
        "the directed replay retained batch-boundary artifacts: {}",
        converged.artifacts
    );
    Ok(())
}

#[test]
fn v2_unregistered_record_stream_rethreads_before_state_after_restore() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let node = namehash(&["alpha", "eth"]);
    let input = v2_released_name_record_input(
        &wiring,
        vec![
            V2Resolver::NameChanged {
                node,
                name: "alpha.eth".to_owned(),
            }
            .encode_log_data(),
            V2Resolver::NameChanged {
                node,
                name: "alpha.eth".to_owned(),
            }
            .encode_log_data(),
        ],
    )?;
    let converged = converge(
        "directed=v2-unregistered-record-before-state",
        input,
        vec![0..2, 2..3, 3..4],
    )?;
    let records = converged
        .whole
        .output
        .normalized_events
        .iter()
        .filter(|event| event.event_kind == "RecordChanged")
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].before_state, records[0].after_state);
    assert_ne!(records[1].before_state, Value::Object(Default::default()));
    assert!(
        converged.artifacts.counts().is_empty(),
        "the directed replay retained batch-boundary artifacts: {}",
        converged.artifacts
    );
    Ok(())
}

#[test]
fn v2_shadow_registry_preimage_does_not_gain_record_attribution_after_restore() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let input = v2_shadow_registry_record_input(&wiring)?;
    let converged = converge(
        "directed=v2-shadow-registry-preimage",
        input,
        vec![0..1, 1..2],
    )?;
    let whole_record = converged
        .whole
        .output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .context("whole pass omitted the resolver record change")?;
    let split_record = converged
        .batches
        .iter()
        .flat_map(|batch| &batch.output.normalized_events)
        .find(|event| event.event_kind == "RecordChanged")
        .context("split replay omitted the resolver record change")?;
    assert_eq!(whole_record.logical_name_id, None);
    assert_eq!(
        whole_record.logical_name_id, split_record.logical_name_id,
        "a normalization-rejected name must not become attributable after restore"
    );
    Ok(())
}

#[test]
fn v2_shadow_alias_preimage_does_not_gain_record_attribution_after_restore() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let wiring = Wiring::build(&ENS_V2_SEPOLIA, &checked_in)?;
    let input = v2_alias_record_input(&wiring, "a\0b", "target")?;
    let converged = converge("directed=v2-shadow-alias-preimage", input, vec![0..1, 1..2])?;
    let whole_record = converged
        .whole
        .output
        .normalized_events
        .iter()
        .find(|event| event.event_kind == "RecordChanged")
        .context("whole pass omitted the resolver record change")?;
    let split_record = converged
        .batches
        .iter()
        .flat_map(|batch| &batch.output.normalized_events)
        .find(|event| event.event_kind == "RecordChanged")
        .context("split replay omitted the resolver record change")?;
    assert_eq!(whole_record.logical_name_id, None);
    assert_eq!(
        whole_record.logical_name_id, split_record.logical_name_id,
        "an alias name rejected by normalization must not become attributable after restore"
    );
    Ok(())
}

fn v2_alias_observed_record_input(wiring: &Wiring) -> Result<BatchInput> {
    v2_alias_record_input(wiring, "alias", "target")
}

fn v2_named_resource_alias_collision_input(wiring: &Wiring) -> Result<BatchInput> {
    const RESOLVER: &str = "ens_v2_resolver_l1";
    let resolver = wiring.address(RESOLVER, "resolver");
    let from_name = dns_encode(&["collision", "eth"]);
    let to_name = dns_encode(&["target", "eth"]);
    let node = namehash(&["collision", "eth"]);
    let resource = U256::from_be_bytes(node.0);
    let account: Address = "0x0000000000000000000000000000000000000529".parse()?;
    let blocks = (0..4)
        .map(|index| BlockSpec {
            number: 20_000_030 + index,
            hash: format!("0x{:064x}", 0x5292_u64 + index as u64),
            timestamp: 1_700_000_030 + index,
        })
        .collect::<Vec<_>>();
    let log = |block_index: usize, ordinal: u64, encoded: LogData| {
        let emission = scenario::emission(resolver, encoded);
        GeneratedLog {
            block_index,
            transaction_hash: format!("0x{:064x}", 0x5292_0000_u64 + ordinal),
            transaction_index: 0,
            log_index: 0,
            emitter: emission.emitter,
            topics: emission.topics,
            data: emission.data,
            burst: None,
        }
    };
    let logs = [
        log(
            0,
            0,
            V2Resolver::NamedAddrResource {
                resource,
                name: from_name.clone().into(),
                coinType: U256::from(60_u64),
            }
            .encode_log_data(),
        ),
        log(
            1,
            1,
            V2Resolver::AliasChanged {
                indexedFromName: keccak256(&from_name),
                indexedToName: keccak256(&to_name),
                fromName: from_name.into(),
                toName: to_name.into(),
            }
            .encode_log_data(),
        ),
        log(
            3,
            2,
            V2Resolver::EACRolesChanged {
                resource,
                account,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: U256::from(1_u64),
            }
            .encode_log_data(),
        ),
    ];
    wiring.batch_input(&blocks, &logs)
}

fn v2_alias_record_input(wiring: &Wiring, from_label: &str, to_label: &str) -> Result<BatchInput> {
    const RESOLVER: &str = "ens_v2_resolver_l1";
    let resolver = wiring.address(RESOLVER, "resolver");
    let from_name = dns_encode(&[from_label, "eth"]);
    let to_name = dns_encode(&[to_label, "eth"]);
    let node = namehash(&[from_label, "eth"]);
    let blocks = [
        BlockSpec {
            number: 20_000_020,
            hash: format!("0x{:064x}", 0x5290_u64),
            timestamp: 1_700_000_020,
        },
        BlockSpec {
            number: 20_000_021,
            hash: format!("0x{:064x}", 0x5291_u64),
            timestamp: 1_700_000_021,
        },
    ];
    let log = |block_index: usize, ordinal: u64, encoded: LogData| {
        let emission = scenario::emission(resolver, encoded);
        GeneratedLog {
            block_index,
            transaction_hash: format!("0x{:064x}", 0x5290_0000_u64 + ordinal),
            transaction_index: 0,
            log_index: 0,
            emitter: emission.emitter,
            topics: emission.topics,
            data: emission.data,
            burst: None,
        }
    };
    let logs = [
        log(
            0,
            0,
            V2Resolver::AliasChanged {
                indexedFromName: keccak256(&from_name),
                indexedToName: keccak256(&to_name),
                fromName: from_name.into(),
                toName: to_name.into(),
            }
            .encode_log_data(),
        ),
        log(
            1,
            1,
            V2Resolver::AddressChanged {
                node,
                coinType: U256::from(60_u64),
                newAddress: vec![0x52, 0x9].into(),
            }
            .encode_log_data(),
        ),
    ];
    wiring.batch_input(&blocks, &logs)
}

/// Builds a released-name sequence followed by the supplied resolver events.
fn v2_released_name_record_input(
    wiring: &Wiring,
    resolver_events: Vec<LogData>,
) -> Result<BatchInput> {
    const REGISTRY: &str = "ens_v2_registry_l1";
    const RESOLVER: &str = "ens_v2_resolver_l1";
    let registry = wiring.address(REGISTRY, "registry");
    let resolver = wiring.address(RESOLVER, "resolver");
    let owner: Address = "0x00000000000000000000000000000000f0000001".parse()?;
    let sender: Address = "0x00000000000000000000000000000000f0000002".parse()?;
    let label = labelhash("alpha");
    let token = U256::from_be_bytes(label.0);
    let resource = U256::from(0x0348_u64);
    let mut blocks = vec![
        BlockSpec {
            number: 20_000_000,
            hash: format!("0x{:064x}", 0x3480_u64),
            timestamp: 1_700_000_000,
        },
        BlockSpec {
            number: 20_000_001,
            hash: format!("0x{:064x}", 0x3481_u64),
            timestamp: 1_700_000_001,
        },
    ];
    blocks.extend((0..resolver_events.len()).map(|index| BlockSpec {
        number: 20_000_002 + index as i64,
        hash: format!("0x{:064x}", 0x3482_u64 + index as u64),
        // Consecutive Ethereum blocks may share a timestamp. Keeping the last topology and
        // resolver blocks on one clock isolates this fixture to name-surface restoration.
        timestamp: 1_700_000_001,
    }));
    let log = |block_index: usize, ordinal: u64, emitter: &str, encoded: LogData| {
        let emission = scenario::emission(emitter, encoded);
        GeneratedLog {
            block_index,
            transaction_hash: format!("0x{:064x}", 0x3480_0000_u64 + ordinal),
            transaction_index: ordinal as i64,
            log_index: 0,
            emitter: emission.emitter,
            topics: emission.topics,
            data: emission.data,
            burst: None,
        }
    };
    let mut logs = vec![
        log(
            0,
            0,
            registry,
            V2Registry::LabelRegistered {
                tokenId: token,
                labelHash: label,
                label: "alpha".to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            0,
            1,
            registry,
            V2Registry::TokenResource {
                tokenId: token,
                resource,
            }
            .encode_log_data(),
        ),
        log(
            1,
            2,
            registry,
            V2Registry::LabelUnregistered {
                tokenId: token,
                sender,
            }
            .encode_log_data(),
        ),
    ];
    logs.extend(
        resolver_events
            .into_iter()
            .enumerate()
            .map(|(index, event)| log(2 + index, 3 + index as u64, resolver, event)),
    );
    wiring.batch_input(&blocks, &logs)
}

/// Registers two ENSv2 names, then either regenerates the second token onto the first token's
/// occupied key or unregisters the first token as the terminal-boundary comparator. A later renewal
/// and resolver record expose any surviving attribution to the displaced registration.
fn v2_regeneration_collision_input(wiring: &Wiring, collision: bool) -> Result<BatchInput> {
    v2_regeneration_collision_input_inner(wiring, collision, false, false, false, false)
}

fn v2_regeneration_collision_input_with_topology(
    wiring: &Wiring,
    same_observation_key: bool,
    source_key_retired: bool,
    source_key_reused: bool,
) -> Result<BatchInput> {
    v2_regeneration_collision_input_inner(
        wiring,
        true,
        true,
        same_observation_key,
        source_key_retired,
        source_key_reused,
    )
}

fn v2_regeneration_collision_tokens(same_observation_key: bool) -> (U256, U256) {
    let token_b = U256::from_be_bytes(labelhash("beta").0);
    let token_a = if same_observation_key {
        let mut bytes = token_b.to_be_bytes::<32>();
        bytes[31] ^= 1;
        U256::from_be_bytes(bytes)
    } else {
        U256::from_be_bytes(labelhash("alpha").0)
    };
    (token_a, token_b)
}

fn versioned_token(label: &str, version: u32) -> U256 {
    let mut bytes = labelhash(label).0;
    bytes[28..].copy_from_slice(&version.to_be_bytes());
    U256::from_be_bytes(bytes)
}

fn v2_shared_subregistry_observation_key(wiring: &Wiring) -> String {
    let registry = wiring.address("ens_v2_registry_l1", "registry");
    let mut bytes = versioned_token("alpha", 1).to_be_bytes::<32>();
    bytes[28..].fill(0);
    format!(
        "{}:{:#x}",
        registry.to_ascii_lowercase(),
        U256::from_be_bytes(bytes)
    )
}

#[derive(Clone, Copy)]
enum SharedSubregistryCase {
    Label,
    Collision,
    Replacement,
    Unregister,
}

fn v2_shared_subregistry_observation_input(
    wiring: &Wiring,
    case: SharedSubregistryCase,
) -> Result<BatchInput> {
    const CHILD_REGISTRY: &str = "0x00000000000000000000000000000000f0000484";
    let registry = wiring.address("ens_v2_registry_l1", "registry");
    let owner: Address = "0x00000000000000000000000000000000f0000001".parse()?;
    let sender: Address = "0x00000000000000000000000000000000f0000002".parse()?;
    let alpha_v1 = versioned_token("alpha", 1);
    let alpha_v2 = versioned_token("alpha", 2);
    let gamma_v1 = versioned_token("gamma", 1);
    let block_count = match case {
        SharedSubregistryCase::Label => 3,
        SharedSubregistryCase::Collision => 6,
        SharedSubregistryCase::Replacement => 4,
        SharedSubregistryCase::Unregister => 5,
    };
    let blocks = (0..block_count)
        .map(|index| BlockSpec {
            number: 20_000_200 + index,
            hash: format!("0x{:064x}", 0x4832_u64 + index as u64),
            timestamp: 1_700_000_200 + index,
        })
        .collect::<Vec<_>>();
    let log = |block_index: usize, ordinal: u64, encoded: LogData| {
        let emission = scenario::emission(registry, encoded);
        GeneratedLog {
            block_index,
            transaction_hash: format!("0x{:064x}", 0x4832_0000_u64 + ordinal),
            transaction_index: ordinal as i64,
            log_index: 0,
            emitter: emission.emitter,
            topics: emission.topics,
            data: emission.data,
            burst: None,
        }
    };
    let registration = |block_index, ordinal, token_id, label: &str| {
        log(
            block_index,
            ordinal,
            V2Registry::LabelRegistered {
                tokenId: token_id,
                labelHash: labelhash(label),
                label: label.to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        )
    };
    let subregistry = |block_index, ordinal| {
        log(
            block_index,
            ordinal,
            V2Registry::SubregistryUpdated {
                tokenId: alpha_v1,
                subregistry: CHILD_REGISTRY.parse().expect("valid child registry"),
                sender,
            }
            .encode_log_data(),
        )
    };
    let mut logs = vec![
        registration(0, 0, alpha_v1, "alpha"),
        subregistry(1, 1),
        registration(2, 2, alpha_v2, "beta"),
    ];
    match case {
        SharedSubregistryCase::Label => {}
        SharedSubregistryCase::Collision => logs.extend([
            subregistry(3, 3),
            registration(4, 4, gamma_v1, "gamma"),
            log(
                5,
                5,
                V2Registry::TokenRegenerated {
                    oldTokenId: gamma_v1,
                    newTokenId: alpha_v2,
                }
                .encode_log_data(),
            ),
        ]),
        SharedSubregistryCase::Replacement => {
            logs.push(registration(3, 3, gamma_v1, "beta"));
        }
        SharedSubregistryCase::Unregister => logs.extend([
            log(
                3,
                3,
                V2Registry::LabelUnregistered {
                    tokenId: alpha_v2,
                    sender,
                }
                .encode_log_data(),
            ),
            log(
                4,
                4,
                V2Registry::LabelUnregistered {
                    tokenId: alpha_v1,
                    sender,
                }
                .encode_log_data(),
            ),
        ]),
    }
    wiring.batch_input(&blocks, &logs)
}

fn v2_repeated_regeneration_alias_input(wiring: &Wiring) -> Result<BatchInput> {
    let registry = wiring.address("ens_v2_registry_l1", "registry");
    let owner: Address = "0x00000000000000000000000000000000f0000001".parse()?;
    let sender: Address = "0x00000000000000000000000000000000f0000002".parse()?;
    let resolver: Address = "0x00000000000000000000000000000000f0000097".parse()?;
    let replacement_resolver: Address = "0x00000000000000000000000000000000f0000098".parse()?;
    let token_a = versioned_token("alpha", 1);
    let token_b = versioned_token("beta", 1);
    let token_c = versioned_token("gamma", 1);
    let shared_b = versioned_token("beta", 2);
    let blocks = (0..6_i64)
        .map(|index| BlockSpec {
            number: 20_000_100 + index,
            hash: format!("0x{:064x}", 0x4831_u64 + index as u64),
            timestamp: 1_700_000_100 + index,
        })
        .collect::<Vec<_>>();
    let log = |block_index: usize, ordinal: u64, encoded: LogData| {
        let emission = scenario::emission(registry, encoded);
        GeneratedLog {
            block_index,
            transaction_hash: format!("0x{:064x}", 0x4831_0000_u64 + ordinal),
            transaction_index: ordinal as i64,
            log_index: 0,
            emitter: emission.emitter,
            topics: emission.topics,
            data: emission.data,
            burst: None,
        }
    };
    let logs = vec![
        log(
            0,
            0,
            V2Registry::LabelRegistered {
                tokenId: token_a,
                labelHash: labelhash("alpha"),
                label: "alpha".to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            0,
            1,
            V2Registry::ResolverUpdated {
                tokenId: token_a,
                resolver,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            1,
            2,
            V2Registry::TokenRegenerated {
                oldTokenId: token_a,
                newTokenId: token_b,
            }
            .encode_log_data(),
        ),
        log(
            2,
            3,
            V2Registry::TokenRegenerated {
                oldTokenId: token_b,
                newTokenId: token_a,
            }
            .encode_log_data(),
        ),
        log(
            3,
            4,
            V2Registry::TokenRegenerated {
                oldTokenId: token_a,
                newTokenId: token_c,
            }
            .encode_log_data(),
        ),
        log(
            4,
            5,
            V2Registry::LabelRegistered {
                tokenId: shared_b,
                labelHash: labelhash("delta"),
                label: "delta".to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            4,
            6,
            V2Registry::ResolverUpdated {
                tokenId: shared_b,
                resolver: replacement_resolver,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            5,
            7,
            V2Registry::LabelUnregistered {
                tokenId: shared_b,
                sender,
            }
            .encode_log_data(),
        ),
    ];
    wiring.batch_input(&blocks, &logs)
}

fn v2_regeneration_collision_displaced_subregistry_input(wiring: &Wiring) -> Result<BatchInput> {
    const REGISTRY: &str = "ens_v2_registry_l1";
    const RESOLVER: &str = "ens_v2_resolver_l1";
    const CHILD_REGISTRY: &str = "0x00000000000000000000000000000000f0000483";
    let registry = wiring.address(REGISTRY, "registry");
    let resolver = wiring.address(RESOLVER, "resolver");
    let owner: Address = "0x00000000000000000000000000000000f0000001".parse()?;
    let sender: Address = "0x00000000000000000000000000000000f0000002".parse()?;
    let alpha_hash = labelhash("alpha");
    let beta_hash = labelhash("beta");
    let child_hash = labelhash("kid");
    let token_a = U256::from_be_bytes(alpha_hash.0);
    let token_b = U256::from_be_bytes(beta_hash.0);
    let child_token = U256::from_be_bytes(child_hash.0);
    let blocks = (0..6_i64)
        .map(|index| BlockSpec {
            number: 20_000_100 + index,
            hash: format!("0x{:064x}", 0x4831_u64 + index as u64),
            timestamp: 1_700_000_100 + index,
        })
        .collect::<Vec<_>>();
    let log = |block_index: usize, ordinal: u64, emitter: &str, encoded: LogData| {
        let emission = scenario::emission(emitter, encoded);
        GeneratedLog {
            block_index,
            transaction_hash: format!("0x{:064x}", 0x4831_0000_u64 + ordinal),
            transaction_index: ordinal as i64,
            log_index: 0,
            emitter: emission.emitter,
            topics: emission.topics,
            data: emission.data,
            burst: None,
        }
    };
    let logs = vec![
        log(
            0,
            0,
            registry,
            V2Registry::LabelRegistered {
                tokenId: token_a,
                labelHash: alpha_hash,
                label: "alpha".to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            0,
            1,
            registry,
            V2Registry::TokenResource {
                tokenId: token_a,
                resource: U256::from(0xa483_u64),
            }
            .encode_log_data(),
        ),
        log(
            1,
            2,
            CHILD_REGISTRY,
            V2Registry::RegistryCreated {}.encode_log_data(),
        ),
        log(
            2,
            3,
            registry,
            V2Registry::ResolverUpdated {
                tokenId: token_a,
                resolver: resolver.parse()?,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            2,
            4,
            registry,
            V2Registry::SubregistryUpdated {
                tokenId: token_a,
                subregistry: CHILD_REGISTRY.parse()?,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            2,
            5,
            CHILD_REGISTRY,
            V2Registry::ParentUpdated {
                parent: registry.parse()?,
                label: "alpha".to_owned(),
                sender,
            }
            .encode_log_data(),
        ),
        log(
            3,
            6,
            CHILD_REGISTRY,
            V2Registry::LabelRegistered {
                tokenId: child_token,
                labelHash: child_hash,
                label: "kid".to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            3,
            7,
            CHILD_REGISTRY,
            V2Registry::TokenResource {
                tokenId: child_token,
                resource: U256::from(0xc483_u64),
            }
            .encode_log_data(),
        ),
        log(
            4,
            8,
            registry,
            V2Registry::LabelRegistered {
                tokenId: token_b,
                labelHash: beta_hash,
                label: "beta".to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            4,
            9,
            registry,
            V2Registry::TokenResource {
                tokenId: token_b,
                resource: U256::from(0xb483_u64),
            }
            .encode_log_data(),
        ),
        log(
            5,
            10,
            registry,
            V2Registry::TokenRegenerated {
                oldTokenId: token_b,
                newTokenId: token_a,
            }
            .encode_log_data(),
        ),
    ];
    wiring.batch_input(&blocks, &logs)
}

fn v2_regeneration_collision_input_inner(
    wiring: &Wiring,
    collision: bool,
    survivor_topology: bool,
    same_observation_key: bool,
    source_key_retired: bool,
    source_key_reused: bool,
) -> Result<BatchInput> {
    const REGISTRY: &str = "ens_v2_registry_l1";
    const RESOLVER: &str = "ens_v2_resolver_l1";
    let registry = wiring.address(REGISTRY, "registry");
    let resolver = wiring.address(RESOLVER, "resolver");
    let owner: Address = "0x00000000000000000000000000000000f0000001".parse()?;
    let sender: Address = "0x00000000000000000000000000000000f0000002".parse()?;
    let alpha_hash = labelhash("alpha");
    let beta_hash = labelhash("beta");
    let (token_a, token_b) = v2_regeneration_collision_tokens(same_observation_key);
    let resource_a = U256::from(0xa483_u64);
    let resource_b = U256::from(0xb483_u64);
    let node_a = namehash(&["alpha", "eth"]);
    let blocks = (0..5_i64)
        .map(|index| BlockSpec {
            number: 20_000_000 + index,
            hash: format!("0x{:064x}", 0x4830_u64 + index as u64),
            timestamp: 1_700_000_000 + index,
        })
        .collect::<Vec<_>>();
    let log = |block_index: usize, ordinal: u64, emitter: &str, encoded: LogData| {
        let emission = scenario::emission(emitter, encoded);
        GeneratedLog {
            block_index,
            transaction_hash: format!("0x{:064x}", 0x4830_0000_u64 + ordinal),
            transaction_index: ordinal as i64,
            log_index: 0,
            emitter: emission.emitter,
            topics: emission.topics,
            data: emission.data,
            burst: None,
        }
    };
    let displacement = if collision {
        V2Registry::TokenRegenerated {
            oldTokenId: token_b,
            newTokenId: token_a,
        }
        .encode_log_data()
    } else {
        V2Registry::LabelUnregistered {
            tokenId: token_a,
            sender,
        }
        .encode_log_data()
    };
    let mut logs = vec![
        log(
            0,
            0,
            registry,
            V2Registry::LabelRegistered {
                tokenId: token_a,
                labelHash: alpha_hash,
                label: "alpha".to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            0,
            1,
            registry,
            V2Registry::TokenResource {
                tokenId: token_a,
                resource: resource_a,
            }
            .encode_log_data(),
        ),
        log(
            1,
            2,
            registry,
            V2Registry::LabelRegistered {
                tokenId: token_b,
                labelHash: beta_hash,
                label: "beta".to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            1,
            3,
            registry,
            V2Registry::TokenResource {
                tokenId: token_b,
                resource: resource_b,
            }
            .encode_log_data(),
        ),
        log(2, 4, registry, displacement),
        log(
            3,
            5,
            registry,
            V2Registry::ExpiryUpdated {
                tokenId: token_a,
                newExpiry: 1_850_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            4,
            6,
            resolver,
            V2Resolver::AddressChanged {
                node: node_a,
                coinType: U256::from(60_u64),
                newAddress: vec![0x48, 0x3].into(),
            }
            .encode_log_data(),
        ),
    ];
    if survivor_topology {
        let resolver = resolver.parse()?;
        logs.insert(
            2,
            log(
                0,
                2,
                registry,
                V2Registry::ResolverUpdated {
                    tokenId: token_a,
                    resolver,
                    sender,
                }
                .encode_log_data(),
            ),
        );
        logs.insert(
            5,
            log(
                1,
                4,
                registry,
                V2Registry::ResolverUpdated {
                    tokenId: token_b,
                    resolver,
                    sender,
                }
                .encode_log_data(),
            ),
        );
        if source_key_retired {
            let mut alias_bytes = token_b.to_be_bytes::<32>();
            alias_bytes[31] ^= 2;
            let alias_token = U256::from_be_bytes(alias_bytes);
            logs.push(log(
                1,
                5,
                registry,
                V2Registry::LabelRegistered {
                    tokenId: alias_token,
                    labelHash: labelhash("gamma"),
                    label: "gamma".to_owned(),
                    owner,
                    expiry: 1_800_000_000,
                    sender,
                }
                .encode_log_data(),
            ));
            logs.push(log(
                1,
                6,
                registry,
                V2Registry::LabelUnregistered {
                    tokenId: alias_token,
                    sender,
                }
                .encode_log_data(),
            ));
        }
        if source_key_reused {
            let mut alias_bytes = token_b.to_be_bytes::<32>();
            alias_bytes[31] ^= 2;
            let alias_token = U256::from_be_bytes(alias_bytes);
            logs.push(log(
                3,
                6,
                registry,
                V2Registry::LabelRegistered {
                    tokenId: alias_token,
                    labelHash: labelhash("gamma"),
                    label: "gamma".to_owned(),
                    owner,
                    expiry: 1_800_000_000,
                    sender,
                }
                .encode_log_data(),
            ));
            logs.push(log(
                3,
                7,
                registry,
                V2Registry::ResolverUpdated {
                    tokenId: alias_token,
                    resolver: "0x00000000000000000000000000000000f0000098".parse()?,
                    sender,
                }
                .encode_log_data(),
            ));
        }
        if same_observation_key {
            logs.push(log(
                1,
                5,
                registry,
                V2Registry::ResolverUpdated {
                    tokenId: token_a,
                    resolver: "0x00000000000000000000000000000000f0000098".parse()?,
                    sender,
                }
                .encode_log_data(),
            ));
        }
        logs.push(log(
            4,
            7,
            registry,
            V2Registry::ResolverUpdated {
                tokenId: token_a,
                resolver: "0x00000000000000000000000000000000f0000099".parse()?,
                sender,
            }
            .encode_log_data(),
        ));
    }
    wiring.batch_input(&blocks, &logs)
}

/// A normalization-rejected registry label establishes only a [shadow name
/// surface](../../../docs/glossary.md#surface-name-surface). Restoring its preimage must not turn it
/// into a name that later resolver records can reference.
fn v2_shadow_registry_record_input(wiring: &Wiring) -> Result<BatchInput> {
    const REGISTRY: &str = "ens_v2_registry_l1";
    const RESOLVER: &str = "ens_v2_resolver_l1";
    const SHADOW_LABEL: &str = "\0alpha";
    let registry = wiring.address(REGISTRY, "registry");
    let resolver = wiring.address(RESOLVER, "resolver");
    let owner: Address = "0x00000000000000000000000000000000f0000001".parse()?;
    let sender: Address = "0x00000000000000000000000000000000f0000002".parse()?;
    let label = labelhash(SHADOW_LABEL);
    let token = U256::from_be_bytes(label.0);
    let node = namehash(&[SHADOW_LABEL, "eth"]);
    let blocks = [
        BlockSpec {
            number: 20_000_010,
            hash: format!("0x{:064x}", 0x348a_u64),
            timestamp: 1_700_000_010,
        },
        BlockSpec {
            number: 20_000_011,
            hash: format!("0x{:064x}", 0x348b_u64),
            timestamp: 1_700_000_011,
        },
    ];
    let log = |block_index: usize, ordinal: u64, emitter: &str, encoded: LogData| {
        let emission = scenario::emission(emitter, encoded);
        GeneratedLog {
            block_index,
            transaction_hash: format!("0x{:064x}", 0x348a_0000_u64 + ordinal),
            transaction_index: 0,
            log_index: 0,
            emitter: emission.emitter,
            topics: emission.topics,
            data: emission.data,
            burst: None,
        }
    };
    let logs = [
        log(
            0,
            0,
            registry,
            V2Registry::LabelRegistered {
                tokenId: token,
                labelHash: label,
                label: SHADOW_LABEL.to_owned(),
                owner,
                expiry: 1_800_000_000,
                sender,
            }
            .encode_log_data(),
        ),
        log(
            1,
            1,
            resolver,
            V2Resolver::NameChanged {
                node,
                name: SHADOW_LABEL.to_owned(),
            }
            .encode_log_data(),
        ),
    ];
    wiring.batch_input(&blocks, &logs)
}

/// Five raw logs distilled from generated seed 18434531763410729552. The second block observes the
/// registrar lease one second after its 90-day grace period. A wrapped `.eth` registration stores
/// that grace-adjusted expiry in NameWrapper, whose `getData` clears the emancipated owner after it
/// passes. (upstream: .refs/ens_v1/contracts/wrapper/README.md:L77 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L48 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L297-L303 @
/// ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L143-L153 @
/// ens_v1@91c966f) (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L852 @
/// ens_v1@91c966f)
fn wrapped_past_grace_lapse_input(wiring: &Wiring) -> Result<BatchInput> {
    const REGISTRY: &str = "ens_v1_registry_l1";
    const REGISTRAR: &str = "ens_v1_registrar_l1";
    const WRAPPER: &str = "ens_v1_wrapper_l1";
    const EXPIRY: u64 = 1_608_204_400;
    let registry = wiring.address(REGISTRY, "registry");
    let wrapped_controller = wiring.address(REGISTRAR, "wrapped_registrar_controller");
    let wrapper = wiring.address(WRAPPER, "name_wrapper");
    let wrapper_address: Address = wrapper.parse()?;
    let registrant: Address = "0x00000000000000000000000000000000f0000001".parse()?;
    let eth_node = namehash(&["eth"]);
    let label = labelhash("alpha");
    let node = namehash(&["alpha", "eth"]);
    let blocks = [
        BlockSpec {
            number: 15_000_000,
            hash: format!("0x{:064x}", 0xb1f0_e1c0_u64),
            timestamp: 1_600_000_000,
        },
        BlockSpec {
            number: 15_000_001,
            hash: format!("0x{:064x}", 0xb1f0_e1c1_u64),
            timestamp: EXPIRY as i64 + (90 * 24 * 60 * 60) + 1,
        },
    ];
    let log = |transaction_index: i64, log_index: i64, emitter: &str, encoded: LogData| {
        let emission = scenario::emission(emitter, encoded);
        GeneratedLog {
            block_index: 0,
            transaction_hash: format!("0x{:064x}", transaction_index),
            transaction_index,
            log_index,
            emitter: emission.emitter,
            topics: emission.topics,
            data: emission.data,
            burst: None,
        }
    };
    let logs = [
        log(
            0,
            1,
            registry,
            V1Registry::NewOwner {
                node: eth_node,
                label,
                owner: wrapper_address,
            }
            .encode_log_data(),
        ),
        log(
            0,
            2,
            wrapped_controller,
            V1WrappedController::NameRegistered {
                name: "alpha".to_owned(),
                label,
                owner: registrant,
                baseCost: U256::from(1),
                premium: U256::ZERO,
                expires: U256::from(EXPIRY),
            }
            .encode_log_data(),
        ),
        log(
            1,
            0,
            registry,
            V1Registry::Transfer {
                node,
                owner: registrant,
            }
            .encode_log_data(),
        ),
        log(
            2,
            0,
            wrapper,
            V1Wrapper::NameUnwrapped {
                node,
                owner: registrant,
            }
            .encode_log_data(),
        ),
        log(
            2,
            1,
            registry,
            V1Registry::Transfer {
                node,
                owner: registrant,
            }
            .encode_log_data(),
        ),
    ];
    wiring.batch_input(&blocks, &logs)
}

/// The `sol!` fragments this lane emits decide every topic0 and every topic layout it produces.
/// Checking them against the manifest ABI of the world that admits them — the manifests carry the
/// upstream citations for those fragments — keeps a mistyped fragment from silently emitting logs
/// the interpreter drops.
#[test]
fn generated_event_fragments_match_the_checked_in_manifest_abi() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let mut declared = BTreeMap::new();
    for world in WORLDS {
        declared.insert(world.label, declared_event_topics(world, &checked_in)?);
    }
    let mut wrong = Vec::new();
    for event in declared_events() {
        let world = declared
            .get(event.world)
            .with_context(|| format!("{} names an unknown world {}", event.name, event.world))?;
        match world.get(&event.topic0.to_ascii_lowercase()) {
            None => wrong.push(format!(
                "{} ({}) -> {}: no {} manifest declares this event",
                event.name, event.signature, event.topic0, event.world
            )),
            Some(topics) if *topics != event.topics => wrong.push(format!(
                "{} ({}): emits {} topics, the {} manifest declares {topics}",
                event.name, event.signature, event.topics, event.world
            )),
            Some(_) => {}
        }
    }
    if !wrong.is_empty() {
        bail!(
            "generated event fragments disagree with the checked-in manifest ABI:\n  {}",
            wrong.join("\n  ")
        );
    }
    Ok(())
}

/// Coverage is proved over the whole dimension space rather than over whichever combinations a seed
/// draws, so a single-case replay run cannot fail for lack of coverage and a seed change cannot
/// make the lane flaky.
#[test]
fn the_dimension_space_emits_every_declared_event() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    // Keyed by world, not by topic0 alone: three fragments this lane declares for ENSv1
    // (`V1Wrapper::TransferSingle`, `V1Resolver::TextChanged`, `V1Resolver::NameChanged`) hash
    // identically to their ENSv2 namesakes, so a flat set lets one world's emission stand in for
    // the other's and silently drops the shadowed fragment from both directions of this check.
    let mut emitted = BTreeSet::new();
    for world in WORLDS {
        let wiring = Wiring::build(world, &checked_in)?;
        for dimensions in scenario::Dimensions::exhaustive() {
            for action in scenario::pool(world, &wiring, &dimensions, SETTLE_TIMESTAMP) {
                emitted.extend(
                    action
                        .emissions
                        .iter()
                        .filter_map(|emission| emission.topics.first())
                        .map(|topic| (world.label, topic.to_ascii_lowercase())),
                );
            }
        }
    }
    let declared = declared_events()
        .into_iter()
        .map(|event| ((event.world, event.topic0.to_ascii_lowercase()), event))
        .collect::<BTreeMap<_, _>>();
    let missing = declared
        .iter()
        .filter(|(key, _)| !emitted.contains(*key))
        .map(|((world, _), event)| format!("{world} {} ({})", event.name, event.signature))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "no combination of the scenario axes emits these declared events, so they cover \
             nothing:\n  {}",
            missing.join("\n  ")
        );
    }
    // The other direction keeps the declaration list from falling behind the pools: an emission it
    // does not list is checked against neither the manifest ABI nor coverage.
    let undeclared = emitted
        .iter()
        .filter(|key| !declared.contains_key(*key))
        .collect::<Vec<_>>();
    if !undeclared.is_empty() {
        bail!(
            "the scenario axes emit event signatures that declared_events() does not list, so \
             nothing checks them: {undeclared:?}"
        );
    }
    Ok(())
}

/// A retired manifest version stays checked in, so a world that keeps pinning it goes on generating
/// from the old ABI against an interpreter that has moved to the rolled-out one. The lane is where
/// that has to be loud. The other direction — a manifest declaring a normalized event nothing here
/// reaches — is `assert_declared_kinds_are_reached`, on the corpus run.
#[test]
fn worlds_pin_the_manifest_version_their_families_have_rolled_out() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    // Reported together: a stale pin in one world should not hide one in the other.
    let mut drift = WORLDS
        .iter()
        .filter_map(|world| assert_pins_are_current(world, &checked_in).err())
        .map(|error| format!("{error:?}"))
        .collect::<Vec<_>>();
    drift.extend(
        assert_worlds_cover_deployments(&WORLDS, &checked_in)
            .err()
            .map(|error| format!("{error:?}")),
    );
    if !drift.is_empty() {
        bail!("{}", drift.join("\n\n"));
    }
    Ok(())
}

#[test]
fn generated_scenarios_are_reproducible_from_their_seed() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    // `split` is a pure function of its arguments, so comparing two calls proves nothing. What has
    // to hold is that the batches partition the blocks: a split that dropped or double-counted one
    // would leave the replay comparing a different sequence from the whole pass, and every
    // convergence failure after that would be an artifact of the harness.
    for len in 1..=12 {
        let batches = split(len, DEFAULT_SEED ^ SPLIT_SALT);
        let covered = batches
            .iter()
            .flat_map(|range| range.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            covered,
            (0..len).collect::<Vec<_>>(),
            "a {len}-block split does not partition the blocks in order: {batches:?}"
        );
    }
    for world in WORLDS {
        let wiring = Wiring::build(world, &checked_in)?;
        let left = scenario::generate(world, &wiring, DEFAULT_SEED);
        let right = scenario::generate(world, &wiring, DEFAULT_SEED);
        assert_eq!(left.describe(), right.describe());
        let left_input = wiring.batch_input(&left.blocks, &left.logs)?;
        let right_input = wiring.batch_input(&right.blocks, &right.logs)?;
        assert_eq!(
            format!("{:?}", left_input.raw_logs),
            format!("{:?}", right_input.raw_logs),
            "{} scenario generation is not seed-deterministic",
            world.label
        );
        // Compared on the drawn logs, not on `describe()`: that string interpolates the seed, so it
        // differs between two seeds even for a generator that ignored the seed entirely.
        let drifted = scenario::generate(world, &wiring, DEFAULT_SEED.wrapping_add(CASE_STRIDE));
        let drifted_input = wiring.batch_input(&drifted.blocks, &drifted.logs)?;
        assert_ne!(
            format!("{:?}", left_input.raw_logs),
            format!("{:?}", drifted_input.raw_logs),
            "{} scenario generation ignores its seed",
            world.label
        );
    }
    // Differing seeds is not enough: the generator's increment is odd and therefore invertible, so
    // two seeds always produce the same value stream at *some* offset, and a stride equal to that
    // increment makes the offset one — every case would then replay its predecessor shifted by a
    // single draw, and the axes would be mechanically coupled between adjacent cases.
    // A stride of k increments puts one case's stream k draws from the next's. Both signs are
    // degenerate — advanced by k or rewound by k couple the axes just the same — so look for the
    // opening draw of each stream inside the other. A scenario draws well under a hundred values,
    // so 512 is ample, and this runs over every pair of cases rather than adjacent ones because k
    // multiplies with the gap.
    for gap in 1..DEFAULT_CASES {
        let apart = CASE_STRIDE.wrapping_mul(gap);
        let (mut earlier, mut later) = (
            permutation::rng::Rng::new(DEFAULT_SEED),
            permutation::rng::Rng::new(DEFAULT_SEED.wrapping_add(apart)),
        );
        let (ahead, behind) = (later.next_u64(), earlier.next_u64());
        // Offset zero: identical openings mean the stride vanished for this gap.
        assert_ne!(
            ahead, behind,
            "cases {gap} apart draw the same stream; CASE_STRIDE must not be a small multiple of \
             the generator's increment"
        );
        for draw in 1..512 {
            assert_ne!(
                earlier.next_u64(),
                ahead,
                "cases {gap} apart share a stream offset by {draw} draws; CASE_STRIDE must not be a \
                 small multiple of the generator's increment"
            );
            assert_ne!(
                later.next_u64(),
                behind,
                "cases {gap} apart share a stream offset by -{draw} draws; CASE_STRIDE must not be \
                 a small multiple of the generator's increment"
            );
        }
    }
    Ok(())
}

struct Outcome {
    events: usize,
    event_kinds: BTreeSet<String>,
    subregistry_detaches: usize,
    burst_derivations: [usize; BurstPhase::COUNT],
    artifacts: BatchBoundaryArtifacts,
    tiny_cache_misses: usize,
}

fn check(
    context: &str,
    world: &World,
    declared: &[uuid::Uuid],
    manifests: &[i64],
    input: bigname_adapters::schema_v2::BatchInput,
    batches: Vec<std::ops::Range<usize>>,
    burst_positions: &BTreeMap<(i64, i64, i64), BurstPhase>,
) -> Result<Outcome> {
    if batches.len() < 2 {
        bail!("{context}: a split replay of fewer than two batches proves nothing");
    }
    let converged = converge(context, input, batches)?;
    let mut references = IdentityReferences::new(world.chain_id, declared, manifests);
    let mut events = 0;
    let mut event_kinds = BTreeSet::new();
    for batch in &converged.batches {
        references.absorb(context, &batch.blocks, &batch.output)?;
        events += batch.output.normalized_events.len();
        event_kinds.extend(
            batch
                .output
                .normalized_events
                .iter()
                .map(|event| event.event_kind.clone()),
        );
    }
    // Counted on the whole-sequence pass, where `before_state` is never a casualty of where a batch
    // boundary fell, so what the sequence reaches does not depend on how it was split.
    let subregistry_detaches = converged
        .whole
        .output
        .normalized_events
        .iter()
        .filter(|event| is_subregistry_detach(event))
        .count();
    // Same whole-sequence pass as the detach count: whether a burst write derives must not depend
    // on where the batches fell.
    let mut burst_derivations = [0_usize; BurstPhase::COUNT];
    for event in &converged.whole.output.normalized_events {
        if let (Some(block), Some(transaction), Some(log)) =
            (event.block_number, event.transaction_index, event.log_index)
            && let Some(phase) = burst_positions.get(&(block, transaction, log))
        {
            burst_derivations[phase.index()] += 1;
        }
    }
    // The whole-sequence pass is the shape a backfill runs, and it may attribute rows the split
    // replay leaves unattributed, so it needs its own foreign-key and canonicality check.
    let whole = format!("{context} whole-sequence pass");
    IdentityReferences::new(world.chain_id, declared, manifests).absorb(
        &whole,
        &converged.whole.blocks,
        &converged.whole.output,
    )?;
    assert_upsert_guards_agree(&whole, &converged.whole.output)?;
    Ok(Outcome {
        events,
        event_kinds,
        subregistry_detaches,
        burst_derivations,
        artifacts: converged.artifacts,
        tiny_cache_misses: converged.tiny_cache_misses,
    })
}

/// The phase a burst marker claims is the generator's word; this checks that word against the
/// generated stream, per burst name. The reorders it exists to catch keep every log, marker, and
/// count the pins measure — emit the controller's registration event ahead of both marked writes,
/// or move the marked writes into transactions of their own, and the corpus totals are unchanged
/// while no marked write lands in the retarget interval the burst exists to reach — so counts
/// alone cannot see it. A name's ownership setup is the first registry `NewOwner` whose (node,
/// label) hashes to the node the burst writes carry, and its controller registration the first
/// controller `NameRegistered` naming that label; first is the onboarding pair, because the
/// name's re-registration and late registry writes sit at a later stage than its registration and
/// so land after it in the stream. The marked writes must also share the registration's
/// transaction: reconciliation's retarget interval is transaction-scoped, so log order alone does
/// not place a write inside it. And they must carry the burst's own selector — one same-selector
/// `AddrChanged` stream — because the post-registration rewrite proves a boundary-restored tail's
/// rethread reaches that stream only while the selector matches; a rotated record type keeps
/// every placement and count green. Returns whether any staged rewrite lands in a later batch
/// than its registration under the case's own split, feeding the cross-batch floor.
fn verify_burst_phases(
    context: &str,
    scenario: &scenario::Scenario,
    batches: &[std::ops::Range<usize>],
) -> Result<bool> {
    let new_owner = format!("{:#x}", V1Registry::NewOwner::SIGNATURE_HASH);
    let addr_changed = format!("{:#x}", V1Resolver::AddrChanged::SIGNATURE_HASH);
    let controllers = [
        format!("{:#x}", V1LegacyController::NameRegistered::SIGNATURE_HASH),
        format!("{:#x}", V1WrappedController::NameRegistered::SIGNATURE_HASH),
        format!(
            "{:#x}",
            V1UnwrappedController::NameRegistered::SIGNATURE_HASH
        ),
    ];
    let position = |log: &GeneratedLog| {
        (
            scenario.blocks[log.block_index].number,
            log.transaction_index,
            log.log_index,
        )
    };
    let mut marked: BTreeMap<&str, [Option<&GeneratedLog>; BurstPhase::COUNT]> = BTreeMap::new();
    for log in scenario.logs.iter().filter(|log| log.burst.is_some()) {
        let phase = log.burst.expect("filtered to marked logs");
        let Some(node) = log.topics.get(1) else {
            bail!("{context}: a burst-marked log carries no node topic: {log:?}");
        };
        let slot = &mut marked.entry(node.as_str()).or_default()[phase.index()];
        if slot.replace(log).is_some() {
            bail!("{context}: the burst name writing node {node} has two logs marked {phase:?}");
        }
    }
    let mut cross_batch = false;
    for (node, phases) in marked {
        if phases.iter().any(Option::is_none) {
            bail!(
                "{context}: the burst logs writing node {node} are not one per phase, so the \
                 burst shape lost a leg: {phases:?}"
            );
        }
        let [pre_ownership, retarget_window, rewrite] = phases.map(Option::unwrap);
        for (leg, phase) in [
            (pre_ownership, BurstPhase::PreOwnership),
            (retarget_window, BurstPhase::RetargetWindow),
            (rewrite, BurstPhase::PostRegistrationRewrite),
        ] {
            let selector = leg.topics.first();
            if selector != Some(&addr_changed) {
                bail!(
                    "{context}: the burst log marked {phase:?} for node {node} carries selector \
                     {selector:?}, not the burst's AddrChanged: the marked legs are one \
                     same-selector record stream, and the post-registration rewrite proves a \
                     restored tail's rethread reaches that stream only while the selector \
                     matches — a rotated record type keeps the node topic, position, \
                     transaction, batch, and every count green while that reach is gone"
                );
            }
        }
        let setup_log = scenario
            .logs
            .iter()
            .find(|log| {
                log.topics.first() == Some(&new_owner)
                    && log
                        .topics
                        .get(1..3)
                        .and_then(child_node_from_topics)
                        .as_deref()
                        == Some(node)
            })
            .with_context(|| {
                format!(
                    "{context}: no registry NewOwner sets up node {node}, which the burst writes"
                )
            })?;
        let controller_log = scenario
            .logs
            .iter()
            .find(|log| {
                log.topics
                    .first()
                    .is_some_and(|topic| controllers.contains(topic))
                    && log.topics.get(1) == setup_log.topics.get(2)
            })
            .with_context(|| {
                format!("{context}: no controller NameRegistered registers node {node}")
            })?;
        let setup = position(setup_log);
        let controller = position(controller_log);
        let write = position(pre_ownership);
        if (write.0, write.1) != (controller.0, controller.1)
            || (setup.0, setup.1) != (controller.0, controller.1)
        {
            bail!(
                "{context}: the burst for node {node} no longer sits in the registration's \
                 transaction — the pre-ownership write sits at {write:?}, the ownership setup at \
                 {setup:?}, the controller registration at {controller:?}: the retarget interval \
                 is transaction-scoped, so a write in another transaction is outside it however \
                 the log order reads"
            );
        }
        if write >= setup || write >= controller {
            bail!(
                "{context}: the burst log marked PreOwnership for node {node} sits at {write:?}, \
                 but the name's ownership setup sits at {setup:?} and its controller registration \
                 at {controller:?}: the write no longer precedes the registration — the generator \
                 reordered the registration ahead of the marked write, or the annotation went \
                 stale"
            );
        }
        let write = position(retarget_window);
        if (write.0, write.1) != (controller.0, controller.1) {
            bail!(
                "{context}: the burst log marked RetargetWindow for node {node} sits at \
                 {write:?}, outside the registration's transaction ({controller:?}): the retarget \
                 interval is transaction-scoped, so no marked write sits inside it however the \
                 log order reads"
            );
        }
        if write <= setup || write >= controller {
            bail!(
                "{context}: the burst log marked RetargetWindow for node {node} sits at \
                 {write:?}, outside the interval between the name's ownership setup at {setup:?} \
                 and its controller registration at {controller:?}: no marked write sits in \
                 reconciliation's strict retarget interval — the reach the burst exists to prove \
                 is gone, whatever the counts read"
            );
        }
        let write = position(rewrite);
        if write <= controller {
            bail!(
                "{context}: the burst log marked PostRegistrationRewrite for node {node} sits at \
                 {write:?}, on or before the name's controller registration at {controller:?}: \
                 the staged rewrite no longer follows the registration — the generator reordered, \
                 or the annotation went stale"
            );
        }
        let batch_of = |log: &GeneratedLog| {
            batches
                .iter()
                .position(|range| range.contains(&log.block_index))
        };
        cross_batch |= batch_of(controller_log) != batch_of(rewrite);
    }
    Ok(cross_batch)
}

/// The child node a registry `NewOwner`'s (node, label) topics commit to. The burst writes carry
/// that child node, so this is what ties a marked log to its name's ownership setup.
fn child_node_from_topics(path: &[String]) -> Option<String> {
    let [parent, label] = path else { return None };
    let parent = parent.parse::<B256>().ok()?;
    let label = label.parse::<B256>().ok()?;
    let mut bytes = [0_u8; 64];
    bytes[..32].copy_from_slice(parent.as_slice());
    bytes[32..].copy_from_slice(label.as_slice());
    Some(format!("{:#x}", keccak256(bytes)))
}

/// An event that clears a subregistry the name was carrying — a different interpretation path from
/// the one an attaching `SubregistryUpdated` takes. Every one the corpus reaches comes from the
/// terminal boundary the interpreter derives on `LabelUnregistered`; the interpreter derives the
/// same boundary for a registration that replaces a live token, which these pools never produce
/// because each label is registered once. A `SubregistryUpdated` naming the zero address would also
/// clear a subregistry and would count here; the pools never emit one. The match is on payload
/// shape, so it is ENSv2-only in practice: ENSv1 derives the same event kind from `NewOwner` but
/// carries an owner rather than a subregistry, and would start counting if that payload ever gained
/// one.
fn is_subregistry_detach(event: &bigname_adapters::schema_v2::NormalizedEvent) -> bool {
    event.event_kind == "SubregistryChanged"
        && event.after_state.get("subregistry") == Some(&Value::Null)
        && event
            .before_state
            .get("subregistry")
            .is_some_and(|prior| !prior.is_null())
}

/// Emitting a log proves nothing on its own — an unadmitted emitter or an undeclared event is
/// dropped silently. This is every kind the default corpus derives, so an interpretation path that
/// goes dark fails the lane instead of quietly dropping out of the run. Deriving a *new* kind is
/// not a regression and does not fail; add it here so the floor keeps tracking the corpus.
///
/// Held per world rather than as one union: 13 of these kinds are derived by both protocols, so a
/// union floor is satisfied by either world alone and an ENSv1-only or ENSv2-only path could go
/// dark without failing anything.
const REQUIRED_EVENT_KINDS: &[(&str, &[&str])] = &[
    (
        ENS_V1_MAINNET.label,
        &[
            "AuthorityEpochChanged",
            "AuthorityTransferred",
            "ExpiryChanged",
            "PermissionChanged",
            "PermissionScopeChanged",
            "PreimageObserved",
            "RecordChanged",
            "RegistrationGranted",
            "RegistrationReleased",
            "RegistrationRenewed",
            "ResolverChanged",
            "ReverseChanged",
            "SubregistryChanged",
            "SurfaceBound",
            "SurfaceUnbound",
            "TokenControlTransferred",
        ],
    ),
    (
        ENS_V1_SEPOLIA.label,
        &[
            "AuthorityEpochChanged",
            "AuthorityTransferred",
            "ExpiryChanged",
            "PermissionChanged",
            "PermissionScopeChanged",
            "PreimageObserved",
            "ResolverChanged",
            "SubregistryChanged",
            "SurfaceBound",
            "SurfaceUnbound",
            "TokenControlTransferred",
        ],
    ),
    (
        ENS_V2_SEPOLIA.label,
        &[
            "AliasChanged",
            "AuthorityTransferred",
            "ExpiryChanged",
            "ParentChanged",
            "PermissionChanged",
            "PreimageObserved",
            "RecordChanged",
            "RegistrarNameRegistered",
            "RegistrationGranted",
            "RegistrationReleased",
            "RegistrationRenewed",
            "RegistryCreated",
            "ResolverChanged",
            "SubregistryChanged",
            "SurfaceBound",
            "SurfaceUnbound",
            "TokenControlTransferred",
            "TokenRegenerated",
            "TokenResourceLinked",
            "Upgraded",
        ],
    ),
];

/// The batch-boundary differences the default corpus reproduces exactly, keyed as
/// `BatchBoundaryArtifacts::counts` reports them. The artifact classes themselves are shapes, so
/// without an equality gate a regression that blanked `before_state` on every split-replay event,
/// or dropped attribution wholesale, would stay inside an allowed shape and pass. Each count was a
/// real whole-pass versus split-replay divergence, tracked by issue #336, and emptying this table
/// was that fix's acceptance test: under the fix the default corpus produces none (ENSv1
/// `rebased_anchors:resources` fell 60 to 0), so any class that appears here is a batch-boundary
/// regression.
///
/// Pinned per world, because the three can diverge independently: a single cross-world total would
/// read the same if one world stopped while the other started.
///
/// A class missing from a row means the default corpus does not reach it, not that it cannot
/// happen — `counts` omits zero-count classes. The 600-case sweep agrees with the fix on the
/// classes it covered: ENSv1 `carried_before_states` fell 7 to 0 alongside the anchors, and the
/// four ENSv2 `rebased_attributions` catalogued by issue #348 fell to 0 when restore began rebuilding
/// the retained namehash-to-name observation. Issue #529 extended the ENSv2 corpus with an
/// alias-only name followed by a record write, so the sibling restore path is now generated as well
/// as pinned by `v2_alias_observed_record_name_link_is_batch_grid_independent`.
const EXPECTED_ARTIFACTS: &[(&str, &[(&str, usize)])] = &[
    (ENS_V1_MAINNET.label, &[]),
    (ENS_V1_SEPOLIA.label, &[]),
    (ENS_V2_SEPOLIA.label, &[]),
];

/// The first thing to rule out when a pinned count moves: these are counts over the sequences one
/// seed draws, so they are not evidence about the interpreter until the corpus is held fixed.
const DRAWN_CORPUS_CAVEAT: &str = "If the scenario pools, the axes, the seeded draw order, the \
                                   batch splitting, or a checked-in manifest changed, then what was \
                                   measured changed and these counts move with it — re-pin rather \
                                   than hunt.";

/// Terminal-boundary subregistry detaches the default corpus reaches. `SubregistryChanged` on its
/// own is satisfiable by attachment alone, so without this the detach path could go dark while the
/// coverage floor above stayed green. Per world for the same reason as the artifacts: ENSv1's
/// `SubregistryChanged` carries an owner rather than a subregistry and detaches nothing, so a
/// cross-world total would let an ENSv1 detach appearing offset the ENSv2 path going dark.
const EXPECTED_SUBREGISTRY_DETACHES: &[(&str, usize)] = &[
    (ENS_V1_MAINNET.label, 0),
    (ENS_V1_SEPOLIA.label, 0),
    (ENS_V2_SEPOLIA.label, 33),
];

/// Per-world corpus volume floors — minimum raw-log and normalized-event totals the default
/// corpus must reach, in the print order of the run line above. The artifact pins are empty since
/// the #336 fix and the kind floor needs only one witness per kind, so without these a generator
/// regression that collapses corpus volume while keeping one witness per required kind passes
/// silently. Floors, not exact pins: a deeper sweep and legitimate generator evolution both grow
/// these totals, and only the default corpus asserts them (the same gate as the pins). Derived
/// from the default-corpus run that introduced them — ens_v1_mainnet 1446 raw logs and 4554
/// normalized events, ens_v1_sepolia 1067 and 3634, and ens_v2_sepolia 965 and 1946 — with each
/// new floor at 70% of that run, truncated. The ENSv2 normalized-event floor retains its
/// pre-existing stricter value, originally 70% of the earlier 1987-event baseline.
const MINIMUM_VOLUMES: &[(&str, usize, usize)] = &[
    (ENS_V1_MAINNET.label, 1012, 3187),
    (ENS_V1_SEPOLIA.label, 746, 2543),
    (ENS_V2_SEPOLIA.label, 675, 1390),
];

/// The pre-registration burst axis's reach at the default corpus, per world: how many cases the
/// axis fired in, how many events the whole-sequence pass derives from the burst's logs per
/// marked phase — pre-ownership, retarget window, post-registration rewrite — and a floor on how
/// many of those cases land the staged rewrite in a later batch than the registration under the
/// case's own split. Presence alone would let the axis rot silently — the burst is a few percent
/// of the corpus, so zeroing its draw or malforming its logs (the interpreter then drops them)
/// moves neither the kind floor nor the volume floors. Every burst log derives exactly one event
/// today, so the phase columns are also the per-phase burst-log counts; a malformed fragment
/// lowers one. The corpus event total nets ~2 fewer than the burst-log count across the corpus —
/// the burst's extra action re-rolls each burst case's layout, and same-transaction
/// reconciliation collapses derivations the burst-free layout kept — so compare these columns
/// against the burst logs, not the corpus event delta.
///
/// The flat total did not bind the topology: emitting the controller's registration event ahead
/// of both marked writes keeps every log, marker, and count while no marked write lands in the
/// retarget interval the burst exists to reach. The per-phase columns bind what each phase
/// accounts for, and `verify_burst_phases` binds the phases themselves against the generated
/// stream — including the registration's transaction, which the retarget interval is scoped to —
/// so a reorder cannot carry stale annotations. The last column floors the cross-batch
/// placements: a rewrite the split replay always processes in the registration's own batch never
/// exercises a boundary-restored tail, which the convergence claim relies on. ENSv2's zero row
/// pins the axis as ENSv1-only until someone deliberately extends it there.
const EXPECTED_BURST_REACH: &[(&str, usize, [usize; BurstPhase::COUNT], usize)] = &[
    (ENS_V1_MAINNET.label, 8, [14, 14, 14], 5),
    (ENS_V1_SEPOLIA.label, 0, [0, 0, 0], 0),
    (ENS_V2_SEPOLIA.label, 0, [0, 0, 0], 0),
];

#[derive(Clone, Copy, Debug, Default)]
struct BurstReach {
    cases: usize,
    derivations: [usize; BurstPhase::COUNT],
    cross_batch_cases: usize,
}

/// Normalized events the pinned manifests declare that the pools deliberately never reach, with the
/// reason each one is out. Anything a manifest declares that is neither derived nor listed here
/// fails the lane, so adding an event to a manifest forces a decision instead of silently widening
/// the gap between what the manifests promise and what this lane covers.
/// Keyed by world, because a reason true of one protocol is not automatically true of the other —
/// three of these are ENSv2-only today, and a flat list would excuse ENSv1 on ENSv2's reasoning.
const UNREACHED_EVENT_KINDS: &[(&str, &str, &str)] = &[
    (
        ENS_V1_MAINNET.label,
        "RecordVersionChanged",
        "the resolver pool emits no VersionChanged, so no record-version bump is generated",
    ),
    (
        ENS_V1_SEPOLIA.label,
        "RegistrationReleased",
        "numeric BaseRegistrar registrations are candidate-only ENSv1→ENSv2 migration input and \
         the dedicated ENSv1→ENSv2 migration corpus exercises their correlation",
    ),
    (
        ENS_V1_SEPOLIA.label,
        "RegistrationRenewed",
        "numeric BaseRegistrar renewals are candidate-only ENSv1→ENSv2 migration input and the \
         dedicated ENSv1→ENSv2 migration corpus exercises their correlation",
    ),
    (
        ENS_V2_SEPOLIA.label,
        "RecordVersionChanged",
        "the generated resolver pool emits no VersionChanged; the directed released-name replay \
         covers record-version attribution",
    ),
    (
        ENS_V2_SEPOLIA.label,
        "RegistrationReserved",
        "the pools emit no LabelReserved; reservation is a registrar-side path with no registration \
         to permute",
    ),
    (
        ENS_V2_SEPOLIA.label,
        "RootPermissionChanged",
        "EACRolesChanged derives this only when it names resource zero on a registry or root, and \
         the pool always names a non-zero resource",
    ),
];

/// The manifests declare which normalized events each ABI event derives, so they — not this lane's
/// own list of what it happens to reach — are the honest denominator for coverage.
fn assert_declared_kinds_are_reached(
    world: &str,
    declared: &BTreeSet<String>,
    derived: Option<&BTreeSet<String>>,
) -> Result<()> {
    let excluded = UNREACHED_EVENT_KINDS
        .iter()
        .filter(|(excluded_world, ..)| *excluded_world == world)
        .map(|(_, kind, _)| *kind)
        .collect::<BTreeSet<_>>();
    let unreached = declared
        .iter()
        .filter(|kind| !derived.is_some_and(|kinds| kinds.contains(*kind)))
        .filter(|kind| !excluded.contains(kind.as_str()))
        .collect::<Vec<_>>();
    if !unreached.is_empty() {
        bail!(
            "{world}'s manifests declare {unreached:?}, which no scenario reaches; emit the event \
             that derives it, or add it to UNREACHED_EVENT_KINDS with the reason it is out"
        );
    }
    // Without this an entry silently outlives its reason — the pools start emitting the event, or
    // the manifest stops declaring it, and the excuse stays on the books unchallenged.
    let stale = excluded
        .iter()
        .filter(|kind| {
            !declared.contains(**kind) || derived.is_some_and(|kinds| kinds.contains(**kind))
        })
        .collect::<Vec<_>>();
    if !stale.is_empty() {
        bail!(
            "{world} lists {stale:?} in UNREACHED_EVENT_KINDS, but its manifests no longer declare \
             them or the corpus now reaches them; drop those entries"
        );
    }
    Ok(())
}

fn assert_interpretation_coverage(
    derived: &BTreeMap<&str, BTreeSet<String>>,
    emitted_topic0s: usize,
) -> Result<()> {
    assert_tables_name_every_world(
        "REQUIRED_EVENT_KINDS",
        &REQUIRED_EVENT_KINDS
            .iter()
            .map(|(world, _)| *world)
            .collect::<Vec<_>>(),
    )?;
    let missing = REQUIRED_EVENT_KINDS
        .iter()
        .filter_map(|(world, required)| {
            let derived = derived.get(world);
            let missing = required
                .iter()
                .filter(|kind| !derived.is_some_and(|kinds| kinds.contains(**kind)))
                .collect::<Vec<_>>();
            (!missing.is_empty()).then(|| format!("{world} never derived {missing:?}"))
        })
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "the corpus emitted {emitted_topic0s} distinct event signatures but those paths are \
             uncovered: {}",
            missing.join("; ")
        );
    }
    Ok(())
}

/// Every table here is keyed by world, so each is checked for a row per world as well as for the
/// row's contents. `assert_worlds_cover_deployments` forces a new world to be added when a
/// deployment appears; without this, whoever adds it satisfies that check and silently inherits no
/// pins and no floor at all.
fn assert_tables_name_every_world(table: &str, named: &[&str]) -> Result<()> {
    let missing = WORLDS
        .iter()
        .map(|world| world.label)
        .filter(|label| !named.contains(label))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!("{table} has no row for {missing:?}, so nothing that world produces is checked");
    }
    Ok(())
}

fn assert_pinned_artifacts(
    artifacts: &BTreeMap<&str, BatchBoundaryArtifacts>,
    detaches: &BTreeMap<&str, usize>,
) -> Result<()> {
    assert_tables_name_every_world(
        "EXPECTED_ARTIFACTS",
        &EXPECTED_ARTIFACTS
            .iter()
            .map(|(world, _)| *world)
            .collect::<Vec<_>>(),
    )?;
    assert_tables_name_every_world(
        "EXPECTED_SUBREGISTRY_DETACHES",
        &EXPECTED_SUBREGISTRY_DETACHES
            .iter()
            .map(|(world, _)| *world)
            .collect::<Vec<_>>(),
    )?;
    for (world, pinned) in EXPECTED_ARTIFACTS {
        let expected = pinned
            .iter()
            .map(|(key, count)| ((*key).to_owned(), *count))
            .collect::<BTreeMap<_, _>>();
        let observed = artifacts
            .get(world)
            .map(BatchBoundaryArtifacts::counts)
            .unwrap_or_default();
        if observed != expected {
            bail!(
                "{world} produced batch-boundary artifacts {observed:?}, not the pinned \
                 {expected:?}. {DRAWN_CORPUS_CAVEAT} Otherwise: a count that fell is a divergence \
                 fixed (retire it here), and one that rose or is newly named is a regression"
            );
        }
    }
    for (world, pinned) in EXPECTED_SUBREGISTRY_DETACHES {
        let observed = detaches.get(world).copied().unwrap_or_default();
        if observed != *pinned {
            bail!(
                "{world} reached {observed} terminal-boundary subregistry detaches, not the pinned \
                 {pinned}. {DRAWN_CORPUS_CAVEAT} Otherwise a fall means the sequences stopped \
                 reaching the path, and a rise means something now clears a subregistry that did \
                 not — check what `is_subregistry_detach` is matching"
            );
        }
    }
    Ok(())
}

fn assert_burst_reach(reach: &BTreeMap<&str, BurstReach>) -> Result<()> {
    assert_tables_name_every_world(
        "EXPECTED_BURST_REACH",
        &EXPECTED_BURST_REACH
            .iter()
            .map(|(world, ..)| *world)
            .collect::<Vec<_>>(),
    )?;
    for (world, cases, derivations, cross_batch) in EXPECTED_BURST_REACH {
        let observed = reach.get(world).copied().unwrap_or_default();
        if observed.cases != *cases || observed.derivations != *derivations {
            bail!(
                "{world}: the pre-registration burst fired in {} cases and derived {:?} events by \
                 phase (pre-ownership, retarget window, post-registration rewrite), not the \
                 pinned ({cases} cases, {derivations:?}). {DRAWN_CORPUS_CAVEAT} Otherwise fewer \
                 derivations at the same case count means the burst's writes stopped deriving — a \
                 fragment the interpreter now drops; more means a fragment now derives two events \
                 or the burst marking spread to logs the axis did not add; and a changed case \
                 count means the axis's draw moved",
                observed.cases,
                observed.derivations
            );
        }
        if observed.cross_batch_cases < *cross_batch {
            bail!(
                "{world}: {} burst cases land the staged rewrite in a later batch than the \
                 registration, under the pinned {cross_batch} — the corpus is losing the \
                 cross-batch rewrite placements the convergence claim relies on, and a rewrite \
                 the split replay always processes in the registration's own batch never \
                 exercises a boundary-restored tail. {DRAWN_CORPUS_CAVEAT} Otherwise the layout \
                 or the batch split stopped placing the rewrite past a boundary",
                observed.cross_batch_cases
            );
        }
    }
    Ok(())
}

fn assert_volume_floors(derived: &[(&str, usize, usize)]) -> Result<()> {
    assert_tables_name_every_world(
        "MINIMUM_VOLUMES",
        &MINIMUM_VOLUMES
            .iter()
            .map(|(world, ..)| *world)
            .collect::<Vec<_>>(),
    )?;
    for (world, min_logs, min_events) in MINIMUM_VOLUMES {
        let Some((_, events, logs)) = derived.iter().find(|(label, ..)| label == world) else {
            bail!("{world} produced nothing for MINIMUM_VOLUMES to check");
        };
        if events < min_events || logs < min_logs {
            bail!(
                "{world} produced {events} normalized events from {logs} raw logs, under the \
                 volume floor ({min_events} events, {min_logs} logs): the corpus collapsed. \
                 {DRAWN_CORPUS_CAVEAT} Otherwise find what the generator stopped emitting"
            );
        }
    }
    Ok(())
}

/// The floor is a minimum in both axes: the observed totals pass it, and one event or one log
/// below it fails — including the failure path, which the lane's own green run never exercises.
#[test]
fn volume_floors_fail_under_the_minimum() {
    let at = MINIMUM_VOLUMES
        .iter()
        .map(|(world, logs, events)| (*world, *events, *logs))
        .collect::<Vec<_>>();
    assert!(assert_volume_floors(&at).is_ok());
    let mut one_event_under = at.clone();
    one_event_under[0].1 -= 1;
    assert!(assert_volume_floors(&one_event_under).is_err());
    let mut one_log_under = at;
    one_log_under[1].2 -= 1;
    assert!(assert_volume_floors(&one_log_under).is_err());
}

#[test]
fn alias_changed_is_a_required_ens_v2_corpus_kind() {
    let required = REQUIRED_EVENT_KINDS
        .iter()
        .find_map(|(world, kinds)| (*world == ENS_V2_SEPOLIA.label).then_some(*kinds))
        .expect("ENSv2 required-event floor");
    assert!(
        required.contains(&"AliasChanged"),
        "the generated alias restore path must stay in the ENSv2 coverage floor"
    );
}

#[test]
fn burst_reach_fails_off_the_pin() {
    let at = EXPECTED_BURST_REACH
        .iter()
        .map(|(world, cases, derivations, cross_batch)| {
            (
                *world,
                BurstReach {
                    cases: *cases,
                    derivations: *derivations,
                    cross_batch_cases: *cross_batch,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert!(assert_burst_reach(&at).is_ok());
    let (world, ..) = EXPECTED_BURST_REACH[0];
    // The malformed-burst shape: cases unchanged, derivations fallen.
    let mut dropped = at.clone();
    dropped.get_mut(world).expect("a row per world").derivations[1] -= 1;
    assert!(assert_burst_reach(&dropped).is_err());
    // A derivation moved between phases keeps the flat total the old tuple pinned; only the
    // per-phase columns see it.
    let mut shifted = at.clone();
    let reach = shifted.get_mut(world).expect("a row per world");
    reach.derivations[0] += 1;
    reach.derivations[1] -= 1;
    assert!(assert_burst_reach(&shifted).is_err());
    // The zeroed-draw shape: the axis never fires.
    let mut silenced = at.clone();
    silenced.insert(world, BurstReach::default());
    assert!(assert_burst_reach(&silenced).is_err());
    // The same-batch-rewrite shape: counts intact, cross-batch placement gone.
    let mut same_batch = at.clone();
    same_batch
        .get_mut(world)
        .expect("a row per world")
        .cross_batch_cases -= 1;
    assert!(assert_burst_reach(&same_batch).is_err());
}

/// The corpus never produces a dishonest annotation, so the honesty check's failure directions
/// are unit-proved here: one name's burst shape passes; the same logs with the controller's event
/// moved ahead of both marked writes fail (the reorder the per-phase counts cannot see); the same
/// logs in the intended order but with a marked write in its own transaction fail (the retarget
/// interval is transaction-scoped); the same logs with two annotations swapped fail; and a
/// rewrite in a later block counts only when the split puts it in a later batch. Every other
/// branch of the check fails its own shape too: the retarget-window write alone leaving the
/// registration's transaction (the combined binding covers the pre-ownership write and the
/// setup), each structural defect — a marked log without a node topic, a duplicated or missing
/// phase, an ownership setup or controller registration the stream no longer carries — and each
/// remaining order binding, the retarget-window write past the controller event and the rewrite
/// ahead of it.
#[test]
fn burst_phase_annotations_fail_when_the_stream_disagrees() {
    let parent = format!("{:#x}", namehash(&["eth"]));
    let label = format!("{:#x}", labelhash("alpha"));
    let node = format!("{:#x}", namehash(&["alpha", "eth"]));
    let write_topics = || {
        vec![
            format!("{:#x}", V1Resolver::AddrChanged::SIGNATURE_HASH),
            node.clone(),
        ]
    };
    let setup_topics = || {
        vec![
            format!("{:#x}", V1Registry::NewOwner::SIGNATURE_HASH),
            parent.clone(),
            label.clone(),
        ]
    };
    let controller_topics = || {
        vec![
            format!("{:#x}", V1LegacyController::NameRegistered::SIGNATURE_HASH),
            label.clone(),
            format!("{:#x}", B256::ZERO),
        ]
    };
    let log =
        |(block_index, transaction_index, log_index), topics: Vec<String>, burst| GeneratedLog {
            block_index,
            transaction_hash: "0x00".to_owned(),
            transaction_index,
            log_index,
            emitter: "0x00000000000000000000000000000000000000aa".to_owned(),
            topics,
            data: Vec::new(),
            burst,
        };
    let scenario_for = |logs: Vec<GeneratedLog>| scenario::Scenario {
        seed: 0,
        world: &ENS_V1_MAINNET,
        dimensions: scenario::Dimensions {
            wrap_state: scenario::WrapState::Unwrapped,
            record_state: scenario::RecordState::NoResolver,
            subname_shape: scenario::SubnameShape::None,
            expiry_window: scenario::ExpiryWindow::Active,
            authority_shape: scenario::AuthorityShape::SelfOwned,
            registration_path: scenario::RegistrationPath::Unwrapped,
            perturbations: Vec::new(),
            name_count: 1,
            dense_transactions: false,
            pre_registration_burst: true,
        },
        action_names: Vec::new(),
        blocks: vec![
            permutation::world::BlockSpec {
                number: 15_000_000,
                hash: "0x01".to_owned(),
                timestamp: 1_600_000_000,
            },
            permutation::world::BlockSpec {
                number: 15_000_010,
                hash: "0x02".to_owned(),
                timestamp: 1_600_086_400,
            },
        ],
        logs,
    };
    let one_batch: Vec<std::ops::Range<usize>> = std::iter::once(0..2).collect();
    let two_batches: Vec<std::ops::Range<usize>> = Vec::from([0..1, 1..2]);
    let honest = scenario_for(vec![
        log((0, 0, 0), write_topics(), Some(BurstPhase::PreOwnership)),
        log((0, 0, 1), setup_topics(), None),
        log((0, 0, 2), write_topics(), Some(BurstPhase::RetargetWindow)),
        log((0, 0, 3), controller_topics(), None),
        log(
            (0, 0, 4),
            write_topics(),
            Some(BurstPhase::PostRegistrationRewrite),
        ),
    ]);
    assert!(
        !verify_burst_phases("honest", &honest, &one_batch).expect("the intended shape verifies"),
        "the rewrite shared the registration's batch"
    );
    let mut later_block_logs = honest.logs.clone();
    later_block_logs[4].block_index = 1;
    let later_block = scenario_for(later_block_logs);
    assert!(
        !verify_burst_phases("later-block-same-batch", &later_block, &one_batch)
            .expect("the intended shape verifies"),
        "a rewrite in a later block but the same batch is not cross-batch coverage"
    );
    assert!(
        verify_burst_phases("later-block-later-batch", &later_block, &two_batches)
            .expect("the intended shape verifies"),
        "the rewrite landed in a later batch than the registration"
    );
    let reordered = scenario_for(vec![
        log((0, 0, 0), controller_topics(), None),
        log((0, 0, 1), write_topics(), Some(BurstPhase::PreOwnership)),
        log((0, 0, 2), setup_topics(), None),
        log((0, 0, 3), write_topics(), Some(BurstPhase::RetargetWindow)),
        log(
            (0, 0, 4),
            write_topics(),
            Some(BurstPhase::PostRegistrationRewrite),
        ),
    ]);
    let error = format!(
        "{:?}",
        verify_burst_phases("reordered", &reordered, &one_batch)
            .expect_err("a stale annotation must fail")
    );
    assert!(
        error.contains("PreOwnership"),
        "the failure names the phase whose binding rotted: {error}"
    );
    // The intended log order with the pre-ownership write in a transaction of its own: every
    // phase ordering holds, but the marked write is outside the retarget interval.
    let split_transactions = scenario_for(vec![
        log((0, 0, 0), write_topics(), Some(BurstPhase::PreOwnership)),
        log((0, 1, 0), setup_topics(), None),
        log((0, 1, 1), write_topics(), Some(BurstPhase::RetargetWindow)),
        log((0, 1, 2), controller_topics(), None),
        log(
            (0, 2, 0),
            write_topics(),
            Some(BurstPhase::PostRegistrationRewrite),
        ),
    ]);
    let error = format!(
        "{:?}",
        verify_burst_phases("split-transactions", &split_transactions, &one_batch)
            .expect_err("a marked write outside the registration's transaction must fail")
    );
    assert!(
        error.contains("transaction"),
        "the failure names the transaction binding that rotted: {error}"
    );
    let mislabeled = scenario_for(vec![
        log((0, 0, 0), write_topics(), Some(BurstPhase::RetargetWindow)),
        log((0, 0, 1), setup_topics(), None),
        log((0, 0, 2), write_topics(), Some(BurstPhase::PreOwnership)),
        log((0, 0, 3), controller_topics(), None),
        log(
            (0, 0, 4),
            write_topics(),
            Some(BurstPhase::PostRegistrationRewrite),
        ),
    ]);
    assert!(
        verify_burst_phases("mislabeled", &mislabeled, &one_batch).is_err(),
        "swapped annotations must fail even though the logs are in the intended order"
    );
    let fails = |name: &str, scenario: &scenario::Scenario| {
        format!(
            "{:?}",
            verify_burst_phases(name, scenario, &one_batch)
                .expect_err("the rotted shape must fail")
        )
    };
    // The retarget-window write alone leaves the registration's transaction: the combined
    // binding covers the pre-ownership write and the setup, so only the RetargetWindow
    // transaction branch sees this shape.
    let mut window_left_logs = honest.logs.clone();
    window_left_logs[2].transaction_index = 1;
    let error = fails("window-left-transaction", &scenario_for(window_left_logs));
    assert!(
        error.contains("marked RetargetWindow")
            && error.contains("outside the registration's transaction"),
        "the failure names the retarget-window transaction binding: {error}"
    );
    // The structural bails, one rotted shape each.
    let mut no_node_topic_logs = honest.logs.clone();
    no_node_topic_logs[0].topics.truncate(1);
    let error = fails("no-node-topic", &scenario_for(no_node_topic_logs));
    assert!(
        error.contains("carries no node topic"),
        "the failure names the missing node topic: {error}"
    );
    let mut duplicate_phase_logs = honest.logs.clone();
    duplicate_phase_logs[2].burst = Some(BurstPhase::PreOwnership);
    let error = fails("duplicate-phase", &scenario_for(duplicate_phase_logs));
    assert!(
        error.contains("two logs marked"),
        "the failure names the duplicated phase: {error}"
    );
    let mut missing_phase_logs = honest.logs.clone();
    missing_phase_logs[2].burst = None;
    let error = fails("missing-phase", &scenario_for(missing_phase_logs));
    assert!(
        error.contains("not one per phase"),
        "the failure names the lost leg: {error}"
    );
    let mut orphan_setup_logs = honest.logs.clone();
    orphan_setup_logs[1].topics[2] = format!("{:#x}", labelhash("beta"));
    let error = fails("orphan-setup", &scenario_for(orphan_setup_logs));
    assert!(
        error.contains("no registry NewOwner sets up node"),
        "the failure names the missing ownership setup: {error}"
    );
    let mut orphan_controller_logs = honest.logs.clone();
    orphan_controller_logs[3].topics[1] = format!("{:#x}", labelhash("beta"));
    let error = fails("orphan-controller", &scenario_for(orphan_controller_logs));
    assert!(
        error.contains("no controller NameRegistered registers node"),
        "the failure names the missing controller registration: {error}"
    );
    // The selector rotation the position and transaction bindings cannot see: the marked leg
    // keeps its node topic, position, transaction, and batch but becomes a valid one-event write
    // of another record type — here ContenthashChanged, whose topic shape is identical — so the
    // marked legs stop being one same-selector stream. One shape rotates the rewrite leg, the
    // other the retarget-window leg.
    let mut rotated_rewrite_logs = honest.logs.clone();
    rotated_rewrite_logs[4].topics[0] =
        format!("{:#x}", V1Resolver::ContenthashChanged::SIGNATURE_HASH);
    let error = fails(
        "rotated-rewrite-selector",
        &scenario_for(rotated_rewrite_logs),
    );
    assert!(
        error.contains("marked PostRegistrationRewrite") && error.contains("same-selector"),
        "the failure names the rewrite leg's rotated selector: {error}"
    );
    let mut rotated_window_logs = honest.logs.clone();
    rotated_window_logs[2].topics[0] =
        format!("{:#x}", V1Resolver::ContenthashChanged::SIGNATURE_HASH);
    let error = fails(
        "rotated-window-selector",
        &scenario_for(rotated_window_logs),
    );
    assert!(
        error.contains("marked RetargetWindow") && error.contains("same-selector"),
        "the failure names the retarget-window leg's rotated selector: {error}"
    );
    // The remaining order bindings, still inside the registration's transaction: the
    // retarget-window write past the controller event, and the rewrite ahead of it.
    let window_after_controller = scenario_for(vec![
        log((0, 0, 0), write_topics(), Some(BurstPhase::PreOwnership)),
        log((0, 0, 1), setup_topics(), None),
        log((0, 0, 2), controller_topics(), None),
        log((0, 0, 3), write_topics(), Some(BurstPhase::RetargetWindow)),
        log(
            (0, 0, 4),
            write_topics(),
            Some(BurstPhase::PostRegistrationRewrite),
        ),
    ]);
    let error = fails("window-after-controller", &window_after_controller);
    assert!(
        error.contains("marked RetargetWindow") && error.contains("outside the interval"),
        "the failure names the retarget interval: {error}"
    );
    let rewrite_before_controller = scenario_for(vec![
        log((0, 0, 0), write_topics(), Some(BurstPhase::PreOwnership)),
        log((0, 0, 1), setup_topics(), None),
        log((0, 0, 2), write_topics(), Some(BurstPhase::RetargetWindow)),
        log(
            (0, 0, 3),
            write_topics(),
            Some(BurstPhase::PostRegistrationRewrite),
        ),
        log((0, 0, 4), controller_topics(), None),
    ]);
    let error = fails("rewrite-before-controller", &rewrite_before_controller);
    assert!(
        error.contains("no longer follows the registration"),
        "the failure names the rewrite order: {error}"
    );
}

fn knob(name: &str, fallback: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("{name}={value} must be an unsigned decimal integer")),
        Err(_) => Ok(fallback),
    }
}

/// Pins the issue #339 contract: a raw-block `binding_closures` row cannot set
/// `except_surface_binding_id` to a binding whose opening provenance does not exist.
///
/// The mechanism, directed rather than drawn, so it is deterministic and independent of the
/// generator. A lapsed lease settles at a bare block boundary, which derives a registry-only
/// resource, a [surface binding](../../../docs/glossary.md#surface-name-surface) for it, and a
/// `binding_closures` row whose `except_surface_binding_id` preserves that binding. In the same
/// block a registry `Transfer` and a registrar `NameRegistered` land in one transaction, so
/// same-transaction reconciliation folds the pending registry setup into the registration.
///
/// Missing transaction and log indexes are not a chain position. The binding side index must keep
/// that state distinct from the pending log's real `(block, 0, 0)` position, so reconciliation
/// retains the boundary binding and `except_surface_binding_id` never dangles.
///
/// The same transaction also opens a registrar binding at a real log position and emits a
/// `binding_closures` row with the matching `except_surface_binding_id`. That positive case must
/// survive: rejecting the sentinel collision must not reject a reference backed by a genuine
/// opening position.
#[test]
fn a_boundary_closure_cannot_exempt_a_binding_whose_opening_provenance_does_not_exist() -> Result<()>
{
    let checked_in = checked_in_manifests()?;
    let directed = Directed::same_transaction_setup(&checked_in)?;
    let context = format!("directed={}", directed.id);
    let converged = converge(&context, directed.input.clone(), directed.batches.clone())?;
    let output = &converged.whole.output;
    let opened = output
        .surface_bindings
        .iter()
        .map(|binding| binding.surface_binding_id)
        .collect::<BTreeSet<_>>();
    let dangling = output
        .binding_closures
        .iter()
        .filter(|closure| {
            closure
                .except_surface_binding_id
                .is_some_and(|id| !opened.contains(&id))
        })
        .map(|closure| {
            (
                closure.block_number,
                closure.transaction_index,
                closure.log_index,
                closure.except_surface_binding_id,
            )
        })
        .collect::<Vec<_>>();
    if !dangling.is_empty() {
        bail!(
            "{context}: a binding_closures row cannot set except_surface_binding_id to a binding \
             whose opening provenance does not exist, found {dangling:?}"
        );
    }
    if !opened.contains(&directed.surface_binding_id()) {
        bail!(
            "{context}: reconciliation removed boundary binding {} by treating missing provenance \
             indexes as the pending log's real chain position",
            directed.surface_binding_id()
        );
    }
    let boundary_exemption = output.binding_closures.iter().find(|closure| {
        closure.block_number == directed.release_block_number()
            && closure.transaction_index == -1
            && closure.log_index == -1
    });
    if boundary_exemption.and_then(|closure| closure.except_surface_binding_id)
        != Some(directed.surface_binding_id())
    {
        bail!(
            "{context}: the retained boundary binding is not named by the raw-block \
             except_surface_binding_id: {boundary_exemption:?}"
        );
    }

    let matching = matching_release_except_binding(&directed, output);
    if matching.is_none() {
        bail!(
            "{context}: rejecting the sentinel collision also removed every \
             except_surface_binding_id backed by a binding with a genuine matching opening \
             position"
        );
    }
    Ok(())
}

/// Runs the #339 fixture family with both orders of the two release-transaction logs. For each
/// ordering, the adapter must produce the same complete output as a rebuild when restarted before
/// the release boundary, after the boundary binding and closure have been emitted, or after every
/// physical block. `converge` also compares fresh and incremental interpretation, a compacted
/// restore, a live resumed session, and a deliberately tiny restored-state cache at every split.
#[test]
fn issue_339_event_orders_and_restart_splits_equal_rebuild() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let directed = Directed::same_transaction_setup(&checked_in)?;
    for (order, input) in issue_339_event_orders(&directed)? {
        let block_count = input.blocks.len();
        let release = block_count - 2;
        let split_grids = [
            (
                "single rebuild batch",
                std::iter::once(0..block_count).collect(),
            ),
            (
                "restart before the closure boundary",
                vec![0..release, release..block_count],
            ),
            (
                "restart after the boundary binding and closure",
                vec![0..release + 1, release + 1..block_count],
            ),
            (
                "restart after every block",
                (0..block_count).map(|index| index..index + 1).collect(),
            ),
        ];
        for (grid, splits) in split_grids {
            let context = format!("directed={} order={order} grid={grid}", directed.id);
            let converged = converge(&context, input.clone(), splits)?;
            assert_issue_339_references(&context, &directed, &converged.whole.output)?;
            let artifacts = converged.artifacts.counts();
            if !artifacts.is_empty() {
                bail!(
                    "{context}: fixture replay differs from a rebuild through batch-boundary \
                     artifacts {artifacts:?}"
                );
            }

            let mut references = IdentityReferences::new(
                &input.chain_id,
                &directed.declared_instances,
                &directed.manifest_ids,
            );
            for batch in &converged.batches {
                references.absorb(&context, &batch.blocks, &batch.output)?;
            }
        }
    }
    Ok(())
}

fn issue_339_event_orders(directed: &Directed) -> Result<Vec<(&'static str, BatchInput)>> {
    let mut transfer_before_registration = directed.input.clone();
    let mut post_closure = transfer_before_registration
        .blocks
        .last()
        .cloned()
        .context("issue #339 fixture has no release block")?;
    post_closure.block_number += 1;
    post_closure.block_hash = format!("0x{:064x}", 339_u64);
    post_closure.block_timestamp += time::Duration::seconds(1);
    transfer_before_registration.blocks.push(post_closure);

    let mut registration_before_transfer = transfer_before_registration.clone();
    let release_block_number = directed.release_block_number();
    let release_logs = registration_before_transfer
        .raw_logs
        .iter()
        .enumerate()
        .filter(|(_, log)| log.block_number == release_block_number)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if release_logs.len() != 2 {
        bail!(
            "directed={}: expected two synthetic release-transaction logs, found {}",
            directed.id,
            release_logs.len()
        );
    }
    let first = registration_before_transfer.raw_logs[release_logs[0]].log_index;
    let second = registration_before_transfer.raw_logs[release_logs[1]].log_index;
    registration_before_transfer.raw_logs[release_logs[0]].log_index = second;
    registration_before_transfer.raw_logs[release_logs[1]].log_index = first;
    registration_before_transfer
        .raw_logs
        .sort_by_key(|log| (log.block_number, log.transaction_index, log.log_index));

    Ok(vec![
        ("transfer-before-registration", transfer_before_registration),
        ("registration-before-transfer", registration_before_transfer),
    ])
}

fn assert_issue_339_references(
    context: &str,
    directed: &Directed,
    output: &BatchOutput,
) -> Result<()> {
    let opened = output
        .surface_bindings
        .iter()
        .map(|binding| binding.surface_binding_id)
        .collect::<BTreeSet<_>>();
    let dangling = output
        .binding_closures
        .iter()
        .filter_map(|closure| {
            closure
                .except_surface_binding_id
                .filter(|binding_id| !opened.contains(binding_id))
        })
        .collect::<BTreeSet<_>>();
    if !dangling.is_empty() {
        bail!("{context}: except_surface_binding_id references unopened bindings {dangling:?}");
    }
    if !opened.contains(&directed.surface_binding_id()) {
        bail!(
            "{context}: rebuild did not retain boundary binding {}",
            directed.surface_binding_id()
        );
    }
    if matching_release_except_binding(directed, output).is_none() {
        bail!(
            "{context}: the release transaction has no except_surface_binding_id backed by a \
             binding with the same genuine opening position"
        );
    }
    Ok(())
}

fn matching_release_except_binding(
    directed: &Directed,
    output: &BatchOutput,
) -> Option<(uuid::Uuid, (i64, i64, i64))> {
    output.binding_closures.iter().find_map(|closure| {
        if closure.block_number != directed.release_block_number()
            || closure.transaction_index < 0
            || closure.log_index < 0
        {
            return None;
        }
        let exempt = closure.except_surface_binding_id?;
        let binding = output
            .surface_bindings
            .iter()
            .find(|binding| binding.surface_binding_id == exempt)?;
        let opening = (
            binding.block_number,
            binding
                .provenance
                .get("transaction_index")
                .and_then(Value::as_i64)?,
            binding
                .provenance
                .get("log_index")
                .and_then(Value::as_i64)?,
        );
        (opening
            == (
                closure.block_number,
                closure.transaction_index,
                closure.log_index,
            ))
            .then_some((exempt, opening))
    })
}
