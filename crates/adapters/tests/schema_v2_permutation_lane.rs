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
//! exact artifact and detach counts, which a deeper sweep would legitimately exceed. A deeper sweep
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

use anyhow::{Context, Result, bail};
use serde_json::Value;

use permutation::{
    convergence::BatchBoundaryArtifacts,
    directed::Directed,
    events::declared_events,
    invariants::{IdentityReferences, assert_upsert_guards_agree, converge, split},
    scenario,
    world::{
        ENS_V1_MAINNET, ENS_V2_SEPOLIA, Wiring, World, assert_pins_are_current,
        assert_worlds_cover_deployments, checked_in_manifests, declared_event_kinds,
        declared_event_topics,
    },
};

/// 48 permutations per world. Deeper sweeps are clean to at least 600 per world, so this is a
/// runtime budget, and below the rate of the rarer batch-boundary artifacts — see
/// `EXPECTED_ARTIFACTS`.
///
/// It is no longer below a known failure. Once the ENSv1 pool started emitting the registrar mint
/// that a wrapped registration really makes, a sweep of 600 per world fails 3 of 1200 sequences,
/// every one of them `registration_path: Wrapped` with `expiry_window: PastGrace`: the split replay
/// derives an `AuthorityEpochChanged` and its `PermissionChanged` that the whole pass does not, so
/// the two disagree about when the wrapper's authority lapsed. That is a live interpreter
/// divergence this lane found and not a batch-boundary artifact — the whole pass derives strictly
/// less, which is the direction `EXPECTED_ARTIFACTS` does not cover. Raising this knob will report
/// it; it needs its own issue and fix, not a wider allowance here.
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
    for world in WORLDS {
        let wiring = Wiring::build(world, &checked_in)?;
        let declared = wiring.declared_instances();
        let manifest_ids = wiring.manifest_ids();
        let mut events = 0_usize;
        let mut logs = 0_usize;
        let mut world_artifacts = BatchBoundaryArtifacts::default();
        let mut world_detaches = 0_usize;
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
            match check(&context, world, &declared, &manifest_ids, input, batches) {
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
                    event_kinds
                        .entry(world.label)
                        .or_default()
                        .extend(outcome.event_kinds);
                    world_artifacts.absorb(outcome.artifacts);
                    world_detaches += outcome.subregistry_detaches;
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
    assert_pinned_artifacts(&artifacts, &subregistry_detaches)
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
    artifacts: BatchBoundaryArtifacts,
}

fn check(
    context: &str,
    world: &World,
    declared: &[uuid::Uuid],
    manifests: &[i64],
    input: bigname_adapters::schema_v2::BatchInput,
    batches: Vec<std::ops::Range<usize>>,
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
        artifacts: converged.artifacts,
    })
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
/// or dropped attribution wholesale, would stay inside an allowed shape and pass. Each count is a
/// real whole-pass versus split-replay divergence, tracked by issue #336: fixing those makes the
/// counts fall, and emptying this table is that fix's acceptance test.
///
/// Pinned per world, because at this depth the two are disjoint: the only class the default corpus
/// reaches is ENSv1's. A single cross-world total would read the same if ENSv2 started diverging
/// while ENSv1 stopped.
///
/// A class missing from a row means the default corpus does not reach it, not that it cannot
/// happen — `counts` omits zero-count classes. At 600 cases per world the same generator reaches
/// ENSv1 `carried_before_states` 7 times and ENSv2 `rebased_attributions` 4 times, both absent
/// here. So a row shrinking is only evidence of a fix once the deeper sweep agrees.
const EXPECTED_ARTIFACTS: &[(&str, &[(&str, usize)])] = &[
    (ENS_V1_MAINNET.label, &[("rebased_anchors:resources", 60)]),
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
const EXPECTED_SUBREGISTRY_DETACHES: &[(&str, usize)] =
    &[(ENS_V1_MAINNET.label, 0), (ENS_V2_SEPOLIA.label, 33)];

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

fn knob(name: &str, fallback: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("{name}={value} must be an unsigned decimal integer")),
        Err(_) => Ok(fallback),
    }
}

/// Pins issue #339: a binding closure whose `except_surface_binding_id` names a binding the same
/// batch no longer opens.
///
/// The mechanism, directed rather than drawn, so it is deterministic and independent of the
/// generator. A lapsed lease settles at a bare block boundary, which derives a registry-only
/// resource, a surface binding for it, and a closure clamping the name's binding window with that
/// binding exempted. In the same block a registry `Transfer` and a registrar `NameRegistered` land
/// in one transaction, so same-transaction reconciliation folds the pending registry setup into the
/// registration.
///
/// The two indexes then disagree about where the boundary rows sit. A binding's position comes from
/// its provenance, and boundary provenance carries no transaction or log index, so `BindingIndex`
/// defaults it to `(block, 0, 0)` — which is exactly where the pending log sits, and the binding is
/// dropped. The closure carries its own `(-1, -1)` sentinel, which is in no pending position, so it
/// survives. The exemption is left naming a binding that is gone.
///
/// Nothing downstream rejects it: there is no foreign key on that column, so the writer's
/// `surface_binding_id <> $3` clause matches no row and the closure clamps its whole window with
/// nothing exempted. Whether that loses a binding the interpreter meant to keep depends on where
/// the intended binding sits, which this test does not establish — it pins the dangling reference.
///
/// This asserts the current, wrong behaviour. When the interpreter stops emitting it, this test
/// fails and becomes the fix's acceptance test.
#[test]
fn a_boundary_closure_exempts_a_binding_the_same_batch_no_longer_opens() -> Result<()> {
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
    let expected = (
        directed.release_block_number(),
        -1,
        -1,
        Some(directed.surface_binding_id()),
    );
    if dangling != vec![expected] {
        bail!(
            "{context}: expected exactly the boundary closure {expected:?} to exempt a binding the \
             batch no longer opens, found {dangling:?}. A closure that stopped dangling is issue \
             #339 fixed — retire this test. A different one is a new defect"
        );
    }
    Ok(())
}
