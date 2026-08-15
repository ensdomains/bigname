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
//! - `BIGNAME_PERMUTATION_CASES` — permutations per protocol world. Default 48 (96 sequences per
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

use alloy_primitives::{B256, keccak256};
use alloy_sol_types::SolEvent;
use anyhow::{Context, Result, bail};
use bigname_adapters::schema_v2::{BatchInput, BatchOutput};
use serde_json::Value;

use permutation::{
    convergence::BatchBoundaryArtifacts,
    directed::Directed,
    events::{
        V1LegacyController, V1Registry, V1Resolver, V1UnwrappedController, V1WrappedController,
        declared_events,
    },
    invariants::{IdentityReferences, assert_upsert_guards_agree, converge, split},
    names::{labelhash, namehash},
    scenario::{self, BurstPhase},
    world::{
        ENS_V1_MAINNET, ENS_V2_SEPOLIA, GeneratedLog, Wiring, World, assert_pins_are_current,
        assert_worlds_cover_deployments, checked_in_manifests, declared_event_kinds,
        declared_event_topics,
    },
};

/// 48 permutations per world, so this is a runtime budget, and below the rate of the rarer
/// batch-boundary artifacts — see `EXPECTED_ARTIFACTS` for the residual a 600-case sweep still
/// reaches.
///
/// It is no longer below a known failure. Once the ENSv1 pool started emitting the registrar mint
/// that a wrapped registration really makes, a sweep of 600 per world fails 3 of 1200 sequences,
/// every one of them `registration_path: Wrapped` with `expiry_window: PastGrace`: the split replay
/// derives an `AuthorityEpochChanged` and its `PermissionChanged` that the whole pass does not, so
/// the two disagree about when the wrapper's authority lapsed. That is a live interpreter
/// divergence this lane found and not a batch-boundary artifact — the whole pass derives strictly
/// less, which is the direction `EXPECTED_ARTIFACTS` does not cover. It is issue #347. Raising this
/// knob will report it; the fix belongs there, not in a wider allowance here.
const DEFAULT_CASES: u64 = 48;
const DEFAULT_SEED: u64 = 0x6e0d_5eed;
/// Distance between case seeds. Deliberately *not* the SplitMix64 increment: because that increment
/// is odd it is invertible, so every stride makes two cases the same value stream offset by some
/// fixed number of draws, and a stride equal to the increment makes that offset one. How far this
/// one puts them is asserted rather than asserted-to-be-large in a comment — see
/// `generated_scenarios_are_reproducible_from_their_seed`.
const CASE_STRIDE: u64 = 0xd134_2543_de82_ef95;
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
        ENS_V2_SEPOLIA.label,
        &[
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
/// Pinned per world, because the two can diverge independently: a single cross-world total would
/// read the same if one world stopped while the other started.
///
/// A class missing from a row means the default corpus does not reach it, not that it cannot
/// happen — `counts` omits zero-count classes. The 600-case sweep agrees with the fix on the
/// classes it covered (ENSv1 `carried_before_states` fell 7 to 0 alongside the anchors), with one
/// survivor it did not cover: ENSv2 `rebased_attributions` still occurs 4 times, each a late
/// resolver `RecordChanged` on a lapsed registration that the whole pass attributes through the
/// in-memory known-surface carry a boundary-restored split replay does not hold — the v2-path
/// counterpart of the stale reach the fix constrained on the v1 path, and a live residual, not a
/// pin — tracked by issue #348.
const EXPECTED_ARTIFACTS: &[(&str, &[(&str, usize)])] =
    &[(ENS_V1_MAINNET.label, &[]), (ENS_V2_SEPOLIA.label, &[])];

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
const EXPECTED_SUBREGISTRY_DETACHES: &[(&str, usize)] =
    &[(ENS_V1_MAINNET.label, 0), (ENS_V2_SEPOLIA.label, 33)];

/// Per-world corpus volume floors — minimum raw-log and normalized-event totals the default
/// corpus must reach, in the print order of the run line above. The artifact pins are empty since
/// the #336 fix and the kind floor needs only one witness per kind, so without these a generator
/// regression that collapses corpus volume while keeping one witness per required kind passes
/// silently. Floors, not exact pins: a deeper sweep and legitimate generator evolution both grow
/// these totals, and only the default corpus asserts them (the same gate as the pins). Derived
/// from the default-corpus run that introduced them — ens_v1_mainnet 1446 raw logs and 4554
/// normalized events, ens_v2_sepolia 965 and 1987 — with each floor 70% of that run, truncated.
const MINIMUM_VOLUMES: &[(&str, usize, usize)] = &[
    (ENS_V1_MAINNET.label, 1012, 3187),
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
        ENS_V2_SEPOLIA.label,
        "AliasChanged",
        "the resolver pool emits no AliasChanged; alias resolution has no interpreter state the \
         convergence checks here would exercise",
    ),
    (
        ENS_V2_SEPOLIA.label,
        "RecordVersionChanged",
        "the resolver pool emits no VersionChanged, so no record-version bump is generated",
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
