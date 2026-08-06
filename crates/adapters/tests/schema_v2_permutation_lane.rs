//! Generated interpreter sequences checked against invariants that must hold for any ordering.
//!
//! The scenario axes adapt the ENSv1-to-ENSv2 migration scenario catalog's dimension space —
//! `dimensions.md` on the `worknotes/migration-catalog` branch, sections D1 to D7 — to what an
//! interpreter can observe: wrap state, resolver and record state, subname shape, expiry window,
//! authorization shape, which controller registered the name, and post-registration perturbations.
//! The catalog's D6 enumerates migration routes, which have no interpreter-level counterpart, and
//! several of its D7 perturbations are unreachable on a migrated node; the axes here are the
//! observable projection of that space, not a section-for-section copy. Wrapper fuse words are
//! emitted so the event shape stays realistic, but no invariant here reads fuse-derived state.
//!
//! Knobs:
//! - `BIGNAME_PERMUTATION_CASES` — permutations per protocol world. Default 24 (48 sequences per
//!   run) keeps the lane inside the CI budget; raise it for deeper local sweeps.
//! - `BIGNAME_PERMUTATION_SEED` — base seed, decimal. Default 1846370029. Overriding either knob
//!   turns off the interpretation-coverage assertion, which is a property of the default corpus
//!   rather than of any seed; the invariants themselves still run.
//!
//! A failure reports `world=… seed=…`. Replay it with that seed and
//! `BIGNAME_PERMUTATION_CASES=1`, against the same checked-in manifests — a scenario embeds the
//! manifest payloads, so a manifest edit changes what a seed generates.

mod permutation;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use permutation::{
    convergence::BatchBoundaryArtifacts,
    directed::Directed,
    events::declared_events,
    invariants::{IdentityReferences, converge, split},
    scenario,
    world::{
        ENS_V1_MAINNET, ENS_V2_SEPOLIA, Wiring, World, checked_in_manifests, declared_event_topics,
    },
};

const DEFAULT_CASES: u64 = 24;
const DEFAULT_SEED: u64 = 0x6e0d_5eed;
const CASE_STRIDE: u64 = 0x9e37_79b9_7f4a_7c15;
const SPLIT_SALT: u64 = 0xa076_1d64_78bd_642f;
const WORLDS: [&World; 2] = [&ENS_V1_MAINNET, &ENS_V2_SEPOLIA];
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
    let mut artifacts = BatchBoundaryArtifacts::default();
    let mut emitted_topic0s = BTreeSet::new();
    let mut event_kinds = BTreeSet::new();
    let mut derived = Vec::new();
    for world in WORLDS {
        let wiring = Wiring::build(world, &checked_in)?;
        let declared = wiring.declared_instances();
        let mut events = 0_usize;
        let mut logs = 0_usize;
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
            let context = scenario.describe();
            let input = wiring.batch_input(&scenario.blocks, &scenario.logs)?;
            let batches = split(input.blocks.len(), seed ^ SPLIT_SALT);
            match check(&context, world, &declared, input, batches) {
                Ok(outcome) => {
                    events += outcome.events;
                    event_kinds.extend(outcome.event_kinds);
                    artifacts.absorb(outcome.artifacts);
                }
                Err(error) => failures.push(format!("{error:?}")),
            }
        }
        eprintln!(
            "permutation_lane world={} sequences={cases} raw_logs={logs} normalized_events={events}",
            world.label
        );
        derived.push((world.label, events, logs));
    }
    eprintln!("permutation_lane batch_boundary_artifacts: {artifacts}");
    eprintln!(
        "permutation_lane derived_event_kinds={:?}",
        event_kinds.iter().collect::<Vec<_>>()
    );
    if !failures.is_empty() {
        bail!(
            "{} of {} generated sequences failed:\n\n{}",
            failures.len(),
            cases * WORLDS.len() as u64,
            failures.join("\n\n")
        );
    }
    // Guards against one world going dark: the other world's events would keep an aggregate count
    // positive while every invariant here passed over empty vectors.
    for (label, events, logs) in derived {
        if events == 0 {
            bail!("{label}: derived no normalized events from {logs} raw logs");
        }
    }
    // Which interpretation paths a run reaches is a property of the corpus it drew, so only the
    // default corpus asserts it. A reduced or reseeded run is a reproduction tool, not a gate.
    if cases < DEFAULT_CASES || base != DEFAULT_SEED {
        return Ok(());
    }
    assert_interpretation_coverage(&event_kinds, emitted_topic0s.len())
}

#[test]
fn production_lease_release_sequence_holds_the_same_invariants() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let directed = Directed::lease_release(&checked_in)?;
    let context = format!("directed={}", directed.id);
    let chain_id = directed.input.chain_id.clone();
    let converged = converge(&context, directed.input.clone(), directed.batches.clone())?;
    let mut references = IdentityReferences::new(&chain_id, &directed.declared_instances);
    for batch in &converged.batches {
        references.absorb(&context, &batch.blocks, &batch.output)?;
    }
    IdentityReferences::new(&chain_id, &directed.declared_instances).absorb(
        &format!("{context} whole-sequence pass"),
        &converged.whole.blocks,
        &converged.whole.output,
    )?;
    let outputs = converged
        .batches
        .into_iter()
        .map(|batch| batch.output)
        .collect::<Vec<_>>();
    directed.assert_release_reached(&outputs)
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
                        .map(|topic| topic.to_ascii_lowercase()),
                );
            }
        }
    }
    let declared = declared_events()
        .into_iter()
        .map(|event| (event.topic0.to_ascii_lowercase(), event))
        .collect::<BTreeMap<_, _>>();
    let missing = declared
        .values()
        .filter(|event| !emitted.contains(&event.topic0.to_ascii_lowercase()))
        .map(|event| format!("{} ({})", event.name, event.signature))
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
        .filter(|topic0| !declared.contains_key(*topic0))
        .collect::<Vec<_>>();
    if !undeclared.is_empty() {
        bail!(
            "the scenario axes emit event signatures that declared_events() does not list, so \
             nothing checks them: {undeclared:?}"
        );
    }
    Ok(())
}

#[test]
fn generated_scenarios_are_reproducible_from_their_seed() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    assert_eq!(
        split(6, DEFAULT_SEED ^ SPLIT_SALT),
        split(6, DEFAULT_SEED ^ SPLIT_SALT),
        "batch splitting is not seed-deterministic"
    );
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
        let drifted = scenario::generate(world, &wiring, DEFAULT_SEED.wrapping_add(CASE_STRIDE));
        assert_ne!(
            left.describe(),
            drifted.describe(),
            "{} scenario generation ignores its seed",
            world.label
        );
    }
    Ok(())
}

struct Outcome {
    events: usize,
    event_kinds: BTreeSet<String>,
    artifacts: BatchBoundaryArtifacts,
}

fn check(
    context: &str,
    world: &World,
    declared: &[uuid::Uuid],
    input: bigname_adapters::schema_v2::BatchInput,
    batches: Vec<std::ops::Range<usize>>,
) -> Result<Outcome> {
    if batches.len() < 2 {
        bail!("{context}: a split replay of fewer than two batches proves nothing");
    }
    let converged = converge(context, input, batches)?;
    let mut references = IdentityReferences::new(world.chain_id, declared);
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
    // The whole-sequence pass is the shape a backfill runs, and it may attribute rows the split
    // replay leaves unattributed, so it needs its own foreign-key and canonicality check.
    IdentityReferences::new(world.chain_id, declared).absorb(
        &format!("{context} whole-sequence pass"),
        &converged.whole.blocks,
        &converged.whole.output,
    )?;
    Ok(Outcome {
        events,
        event_kinds,
        artifacts: converged.artifacts,
    })
}

/// Emitting a log proves nothing on its own — an unadmitted emitter or an undeclared event is
/// dropped silently. These kinds are the interpretation the corpus is meant to reach; they are
/// chosen to avoid fuse-derived state so the assertion survives adapter fuse changes.
const REQUIRED_EVENT_KINDS: &[&str] = &[
    "AuthorityEpochChanged",
    "AuthorityTransferred",
    "ExpiryChanged",
    "PermissionChanged",
    "PreimageObserved",
    "RegistrationGranted",
    "RegistrationReleased",
    "RegistrationRenewed",
    "RecordChanged",
    "ResolverChanged",
    "SubregistryChanged",
    "SurfaceBound",
    "SurfaceUnbound",
    "TokenControlTransferred",
];

fn assert_interpretation_coverage(
    derived: &BTreeSet<String>,
    emitted_topic0s: usize,
) -> Result<()> {
    let missing = REQUIRED_EVENT_KINDS
        .iter()
        .filter(|kind| !derived.contains(**kind))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "the corpus emitted {emitted_topic0s} distinct event signatures but the interpreter \
             never derived {missing:?}, so those paths are uncovered"
        );
    }
    Ok(())
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
