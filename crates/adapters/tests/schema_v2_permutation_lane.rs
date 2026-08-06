//! Generated interpreter sequences checked against invariants that must hold for any ordering.
//!
//! The scenario axes are seeded from the ENSv1-to-ENSv2 migration scenario catalog's dimension
//! space — `dimensions.md` on the `worknotes/migration-catalog` branch, sections D1 to D7:
//! pre-migration wrap state, resolver and record state, subname shape, expiry window,
//! authorization shape, registration path, and post-registration perturbations. Wrapper fuse words
//! are emitted so the event shape stays realistic, but no invariant here reads fuse-derived state.
//!
//! Knobs:
//! - `BIGNAME_PERMUTATION_CASES` — permutations per protocol world. Default 24 (48 sequences per
//!   run) keeps the lane inside the CI budget; raise it for deeper local sweeps.
//! - `BIGNAME_PERMUTATION_SEED` — base seed, decimal. Default 1846370029.
//!
//! A failure reports `world=… seed=…`. Replay it with that seed and
//! `BIGNAME_PERMUTATION_CASES=1`, against the same checked-in manifests — a scenario embeds the
//! manifest payloads, so a manifest edit changes what a seed generates.

mod permutation;

use std::collections::BTreeSet;

use anyhow::{Result, bail};
use permutation::{
    convergence::BatchBoundaryArtifacts,
    directed::Directed,
    events::declared_events,
    invariants::{IdentityReferences, converge, split},
    scenario,
    world::{
        ENS_V1_MAINNET, ENS_V2_SEPOLIA, Wiring, World, checked_in_manifests, declared_topic0s,
    },
};

const DEFAULT_CASES: u64 = 24;
const DEFAULT_SEED: u64 = 0x6e0d_5eed;
const CASE_STRIDE: u64 = 0x9e37_79b9_7f4a_7c15;
const SPLIT_SALT: u64 = 0xa076_1d64_78bd_642f;
const WORLDS: [&World; 2] = [&ENS_V1_MAINNET, &ENS_V2_SEPOLIA];

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
                Ok((derived, case_artifacts)) => {
                    events += derived;
                    artifacts.absorb(case_artifacts);
                }
                Err(error) => failures.push(format!("{error:?}")),
            }
        }
        // Guards against one world going dark: the other world's events would keep an aggregate
        // count positive while every invariant here passed over empty vectors.
        if events == 0 {
            bail!(
                "{}: derived no normalized events from {logs} raw logs",
                world.label
            );
        }
        eprintln!(
            "permutation_lane world={} sequences={cases} raw_logs={logs} normalized_events={events}",
            world.label
        );
    }
    eprintln!("permutation_lane batch_boundary_artifacts: {artifacts}");
    assert_generated_events_were_emitted(&emitted_topic0s)?;
    if !failures.is_empty() {
        bail!(
            "{} of {} generated sequences failed:\n\n{}",
            failures.len(),
            cases * WORLDS.len() as u64,
            failures.join("\n\n")
        );
    }
    Ok(())
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

/// The `sol!` fragments this lane emits decide every topic0 it produces. Checking them against the
/// checked-in manifest ABI — which carries the upstream citations for those fragments — keeps a
/// mistyped fragment from silently deleting an axis of coverage.
#[test]
fn generated_event_fragments_match_the_checked_in_manifest_abi() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let mut declared = BTreeSet::new();
    for world in WORLDS {
        declared.extend(declared_topic0s(world, &checked_in)?);
    }
    let unknown = declared_events()
        .into_iter()
        .filter(|(_, _, topic0)| !declared.contains(&topic0.to_ascii_lowercase()))
        .map(|(name, signature, topic0)| format!("{name} ({signature}) -> {topic0}"))
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        bail!(
            "no admitted manifest declares these event fragments:\n  {}",
            unknown.join("\n  ")
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

fn check(
    context: &str,
    world: &World,
    declared: &[uuid::Uuid],
    input: bigname_adapters::schema_v2::BatchInput,
    batches: Vec<std::ops::Range<usize>>,
) -> Result<(usize, BatchBoundaryArtifacts)> {
    if batches.len() < 2 {
        bail!("{context}: a split replay of fewer than two batches proves nothing");
    }
    let converged = converge(context, input, batches)?;
    let mut references = IdentityReferences::new(world.chain_id, declared);
    let mut events = 0;
    for batch in &converged.batches {
        references.absorb(context, &batch.blocks, &batch.output)?;
        events += batch.output.normalized_events.len();
    }
    // The whole-sequence pass is the shape a backfill runs, and it may attribute rows the split
    // replay leaves unattributed, so it needs its own foreign-key and canonicality check.
    IdentityReferences::new(world.chain_id, declared).absorb(
        context,
        &converged.whole.blocks,
        &converged.whole.output,
    )?;
    Ok((events, converged.artifacts))
}

fn assert_generated_events_were_emitted(emitted: &BTreeSet<String>) -> Result<()> {
    let missing = declared_events()
        .into_iter()
        .filter(|(_, _, topic0)| !emitted.contains(&topic0.to_ascii_lowercase()))
        .map(|(name, signature, _)| format!("{name} ({signature})"))
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "the generated corpus never emitted these declared events, so they cover nothing:\n  {}",
            missing.join("\n  ")
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
