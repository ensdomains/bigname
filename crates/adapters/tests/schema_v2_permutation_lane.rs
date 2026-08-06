//! Generated interpreter sequences checked against invariants that must hold for any ordering.
//!
//! The scenario axes are seeded from the ENSv1-to-ENSv2 migration scenario catalog's dimension
//! space (pre-migration wrap state, resolver/record state, subname shape, expiry window,
//! authorization shape, registration path, and post-registration perturbations). Fuse words are
//! emitted so the wrapper event shape stays realistic, but no invariant here reads fuse-derived
//! state.
//!
//! Knobs:
//! - `BIGNAME_PERMUTATION_CASES` — permutations per protocol world. Default 24 (48 sequences per
//!   run) keeps the lane inside the CI budget; raise it for deeper local sweeps.
//! - `BIGNAME_PERMUTATION_SEED` — base seed. Default 0x6e0d5eed.
//!
//! Every case prints `world=… seed=…`; replay a failure with that seed and
//! `BIGNAME_PERMUTATION_CASES=1`.

mod permutation;

use anyhow::{Context, Result};
use permutation::{
    directed::Directed,
    invariants::{Ledger, converge},
    scenario,
    world::{ENS_V1_MAINNET, ENS_V2_SEPOLIA, Wiring, World, checked_in_manifests},
};

const DEFAULT_CASES: u64 = 24;
const DEFAULT_SEED: u64 = 0x6e0d_5eed;
const CASE_STRIDE: u64 = 0x9e37_79b9_7f4a_7c15;
const SPLIT_SALT: u64 = 0xa076_1d64_78bd_642f;
const WORLDS: [&World; 2] = [&ENS_V1_MAINNET, &ENS_V2_SEPOLIA];

#[test]
fn generated_interpreter_permutations_hold_identity_and_replay_invariants() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let cases = knob("BIGNAME_PERMUTATION_CASES", DEFAULT_CASES);
    let base = knob("BIGNAME_PERMUTATION_SEED", DEFAULT_SEED);
    let mut sequences = 0_u64;
    let mut logs = 0_usize;
    let mut events = 0_usize;
    let mut carried = 0_usize;
    let mut rebased = 0_usize;
    let mut backfilled = 0_usize;
    for world in WORLDS {
        let wiring = Wiring::build(world, &checked_in)?;
        let declared = wiring.declared_instances();
        for case in 0..cases {
            let seed = base.wrapping_add(case.wrapping_mul(CASE_STRIDE));
            let scenario = scenario::generate(world, &wiring, seed);
            let context = scenario.describe();
            let input = wiring.batch_input(&scenario.blocks, &scenario.logs)?;
            let converged = converge(&context, input, seed ^ SPLIT_SALT)?;
            let mut ledger = Ledger::new(&declared);
            for batch in &converged.batches {
                ledger.absorb(&context, &batch.blocks, &batch.output)?;
                events += batch.output.normalized_events.len();
            }
            carried += converged.known.carried_before_states;
            rebased += converged.known.rebased_anchors;
            backfilled += converged.known.rebased_attributions;
            sequences += 1;
            logs += scenario.logs.len();
        }
    }
    eprintln!(
        "permutation_lane worlds={} sequences={sequences} raw_logs={logs} normalized_events={events} \
batch_local_before_state_carries={carried} batch_local_anchor_rebases={rebased} \
batch_local_attributions={backfilled}",
        WORLDS.len(),
    );
    assert!(events > 0, "permutation lane derived no normalized events");
    Ok(())
}

#[test]
fn production_lease_release_sequence_holds_the_same_invariants() -> Result<()> {
    let checked_in = checked_in_manifests()?;
    let directed = Directed::lease_release(&checked_in)?;
    let context = format!("directed={}", directed.id);
    let converged = converge(&context, directed.input.clone(), DEFAULT_SEED ^ SPLIT_SALT)?;
    let mut ledger = Ledger::new(&directed.declared_instances);
    for batch in &converged.batches {
        ledger.absorb(&context, &batch.blocks, &batch.output)?;
    }
    let outputs = converged
        .batches
        .into_iter()
        .map(|batch| batch.output)
        .collect::<Vec<_>>();
    directed.assert_release_reached(&outputs)
}

#[test]
fn generated_scenarios_are_reproducible_from_their_seed() -> Result<()> {
    let checked_in = checked_in_manifests()?;
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

fn knob(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("{name} must be an unsigned integer"))
        })
        .transpose()
        .expect("permutation knob is well formed")
        .unwrap_or(fallback)
}
