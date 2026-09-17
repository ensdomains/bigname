use std::collections::BTreeMap;

use alloy_primitives::LogData;

use super::{
    events::encoded_topics,
    pool_v1, pool_v1_sepolia, pool_v2,
    rng::Rng,
    world::{BlockSpec, GeneratedLog, Wiring, World},
};

/// Where in its name's onboarding the generator intends a burst-marked log to land. Carried on
/// the marker itself, so the value the lane pins is the generator's claim; the lane then checks
/// that claim against the generated stream rather than trusting it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BurstPhase {
    /// Before the name's ownership setup (the registry's `NewOwner`).
    PreOwnership,
    /// After the ownership setup but before the controller's `NameRegistered` — reconciliation's
    /// strict retarget interval, the reach the burst exists to prove.
    RetargetWindow,
    /// The staged same-selector rewrite after the registration.
    PostRegistrationRewrite,
}

impl BurstPhase {
    pub const COUNT: usize = 3;

    pub fn index(self) -> usize {
        match self {
            Self::PreOwnership => 0,
            Self::RetargetWindow => 1,
            Self::PostRegistrationRewrite => 2,
        }
    }
}

/// One raw log, still unpositioned. Emissions inside an action stay in one transaction.
pub struct Emission {
    pub emitter: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
    /// Marks the fragments `pool_v1`'s pre-registration burst adds, with the phase the generator
    /// intends each to land in, so the lane can attribute derived events to them and pin that
    /// they derive at all.
    pub burst: Option<BurstPhase>,
}

/// Dependency stages. Within one subject — a name, the root, a registry — a later stage names
/// something an earlier stage had to create, so the repair orders them. Nothing is ordered *between*
/// subjects: names are independent on chain, and that interleaving is the permutation value.
pub mod stage {
    /// A registry announcing itself, emitted from its constructor (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@a971bd64).
    pub const ANNOUNCE: u8 = 0;
    /// A label registration — the root's own `.eth`, or a name's.
    pub const REGISTER: u8 = 1;
    /// Identity the registration creates: token mint or resource, and a name's immediate child.
    pub const IDENTITY: u8 = 2;
    /// Control handoffs over that identity: registrar and registry ownership transfers.
    pub const CONTROL: u8 = 3;
    /// Pointers hung off the token: subregistry, resolver, parent claims, wrapping. `setSubregistry`
    /// needs the token to exist (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L142-L147 @ ens_v2@a971bd64).
    pub const LINK: u8 = 4;
    /// The registrar's own `NameRegistered`, which upstream emits after the registry call returns —
    /// so after that call's `LabelRegistered`, `TokenResource`, `SubregistryUpdated` and
    /// `ResolverUpdated` (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467-L479 @ ens_v2@a971bd64).
    pub const REGISTRAR: u8 = 5;
    /// Writes that need the pointer: records, permissions, expiry and renewal, wrapper mutation.
    pub const WRITE: u8 = 6;
    /// Perturbations that only mean something once the name is set up: unwrap, late writes, reverse
    /// claims, unregistration and replacement.
    pub const LATE: u8 = 7;
}

#[derive(Default)]
pub struct Action {
    pub name: String,
    pub emissions: Vec<Emission>,
    /// The chain this action belongs to — one name, or the root. Ordering is enforced inside a
    /// chain and never between two of them.
    pub chain: String,
    /// The dependency stage this action belongs to; see [`stage`].
    pub stage: u8,
}

pub fn emission(emitter: &str, encoded: LogData) -> Emission {
    Emission {
        emitter: emitter.to_owned(),
        topics: encoded_topics(&encoded),
        data: encoded.data.to_vec(),
        burst: None,
    }
}

pub fn action(name: impl Into<String>, stage: u8, emissions: Vec<Emission>) -> Action {
    let name = name.into();
    let chain = name
        .split_once(':')
        .map_or_else(|| name.clone(), |(chain, _)| chain.to_owned());
    Action {
        name,
        emissions,
        chain,
        stage,
    }
}

/// Scenario axes seeded from the migration catalog's dimension space
/// (`worknotes/migration-catalog:dimensions.md`, D1-D7). Fuse words are emitted so the wrapper
/// event shape is realistic, but no invariant reads fuse-derived state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WrapState {
    Unwrapped,
    WrappedUnlocked,
    WrappedLocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordState {
    NoResolver,
    ResolverWithRecords,
    CustomResolverNoRecords,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubnameShape {
    None,
    RegistrySubnode,
    WrappedChild,
    DeepSubnode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpiryWindow {
    Active,
    JustExpired,
    PastGrace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorityShape {
    SelfOwned,
    OperatorTransfer,
    GiveAway,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationPath {
    Legacy,
    Wrapped,
    Unwrapped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Perturbation {
    LateRegistryWrite,
    LateRecordWrite,
    RenewalAfterExpiry,
    ReverseClaim,
    Reregistration,
    RegistryAnnouncement,
    ProxyUpgrade,
}

pub const PERTURBATIONS: &[Perturbation] = &[
    Perturbation::LateRegistryWrite,
    Perturbation::LateRecordWrite,
    Perturbation::RenewalAfterExpiry,
    Perturbation::ReverseClaim,
    Perturbation::Reregistration,
    Perturbation::RegistryAnnouncement,
    Perturbation::ProxyUpgrade,
];

#[derive(Clone, Debug)]
pub struct Dimensions {
    pub wrap_state: WrapState,
    pub record_state: RecordState,
    pub subname_shape: SubnameShape,
    pub expiry_window: ExpiryWindow,
    pub authority_shape: AuthorityShape,
    pub registration_path: RegistrationPath,
    pub perturbations: Vec<Perturbation>,
    pub name_count: usize,
    pub dense_transactions: bool,
    /// ENSv1 only: wrap each label's onboarding registration action in a same-transaction resolver
    /// burst whose record writes are log-ordered before the controller's registration, and add a
    /// rewrite of the same selector after it — the pre-registration resolver traffic the stage
    /// ordering otherwise keeps unreachable. The `Reregistration` perturbation's later registration
    /// is deliberately not wrapped. See `pool_v1::burst_around_registration` for the legality.
    pub pre_registration_burst: bool,
}

impl Dimensions {
    /// Every combination of the axes that decides *which* events a pool contains — wrap state,
    /// record state, subname shape, authorization shape, and registration path — with all
    /// perturbations enabled. The expiry window, name count, transaction density, and the burst
    /// axis only change event payloads and layout, never the set of fragments, so they stay
    /// pinned. Used to prove event coverage without depending on which combinations a seed happens
    /// to draw.
    pub fn exhaustive() -> Vec<Self> {
        let mut all = Vec::new();
        for wrap_state in [
            WrapState::Unwrapped,
            WrapState::WrappedUnlocked,
            WrapState::WrappedLocked,
        ] {
            for record_state in [
                RecordState::NoResolver,
                RecordState::ResolverWithRecords,
                RecordState::CustomResolverNoRecords,
            ] {
                for subname_shape in [
                    SubnameShape::None,
                    SubnameShape::RegistrySubnode,
                    SubnameShape::WrappedChild,
                    SubnameShape::DeepSubnode,
                ] {
                    for authority_shape in [
                        AuthorityShape::SelfOwned,
                        AuthorityShape::OperatorTransfer,
                        AuthorityShape::GiveAway,
                    ] {
                        for registration_path in [
                            RegistrationPath::Legacy,
                            RegistrationPath::Wrapped,
                            RegistrationPath::Unwrapped,
                        ] {
                            all.push(Self {
                                wrap_state,
                                record_state,
                                subname_shape,
                                expiry_window: ExpiryWindow::JustExpired,
                                authority_shape,
                                registration_path,
                                perturbations: PERTURBATIONS.to_vec(),
                                name_count: 1,
                                dense_transactions: false,
                                pre_registration_burst: true,
                            });
                        }
                    }
                }
            }
        }
        all
    }

    fn draw(rng: &mut Rng) -> Self {
        let mut perturbations = PERTURBATIONS.to_vec();
        rng.shuffle(&mut perturbations);
        perturbations.truncate(rng.between(0, PERTURBATIONS.len()));
        perturbations.sort_by_key(|value| format!("{value:?}"));
        Self {
            wrap_state: *rng.pick(&[
                WrapState::Unwrapped,
                WrapState::WrappedUnlocked,
                WrapState::WrappedLocked,
            ]),
            record_state: *rng.pick(&[
                RecordState::NoResolver,
                RecordState::ResolverWithRecords,
                RecordState::CustomResolverNoRecords,
            ]),
            subname_shape: *rng.pick(&[
                SubnameShape::None,
                SubnameShape::RegistrySubnode,
                SubnameShape::WrappedChild,
                SubnameShape::DeepSubnode,
            ]),
            expiry_window: *rng.pick(&[
                ExpiryWindow::Active,
                ExpiryWindow::JustExpired,
                ExpiryWindow::PastGrace,
            ]),
            authority_shape: *rng.pick(&[
                AuthorityShape::SelfOwned,
                AuthorityShape::OperatorTransfer,
                AuthorityShape::GiveAway,
            ]),
            registration_path: *rng.pick(&[
                RegistrationPath::Legacy,
                RegistrationPath::Wrapped,
                RegistrationPath::Unwrapped,
            ]),
            perturbations,
            name_count: rng.between(1, 3),
            dense_transactions: rng.chance(1, 3),
            // `generate` decides this from a side stream; drawing it here would shift every later
            // draw and redraw the pinned corpus.
            pre_registration_burst: false,
        }
    }

    pub fn has(&self, perturbation: Perturbation) -> bool {
        self.perturbations.contains(&perturbation)
    }
}

pub struct Scenario {
    pub seed: u64,
    pub world: &'static World,
    pub dimensions: Dimensions,
    pub action_names: Vec<String>,
    pub blocks: Vec<BlockSpec>,
    pub logs: Vec<GeneratedLog>,
}

impl Scenario {
    pub fn describe(&self) -> String {
        format!(
            "world={} seed={} dimensions={:?} blocks={} logs={} actions=[{}]",
            self.world.label,
            self.seed,
            self.dimensions,
            self.blocks.len(),
            self.logs.len(),
            self.action_names.join(", "),
        )
    }
}

const BASE_TIMESTAMP: i64 = 1_600_000_000;
const BASE_BLOCK: i64 = 15_000_000;
/// Salt for the burst axis's side stream. The main draw order must be identical whether or not the
/// burst fires — every case it does not fire in stays byte-identical to the pre-axis corpus, and
/// the drawn-corpus pins keep their anchor — so the axis draws from a stream the rest of
/// generation never touches; drawing it from the main stream would redraw every case and move the
/// pins for reasons unrelated to the burst. Only the ENSv1 pool reads the axis, so only that world
/// draws it — the side stream is discarded after this one call, so gating the draw cannot shift
/// any ENSv1 decision, and an ENSv2 failure context always prints false rather than implying a
/// burst its pool cannot build.
const PRE_REGISTRATION_BURST_SALT: u64 = 0x5bd1_e995_4a89_1d4b;

pub fn generate(world: &'static World, wiring: &Wiring, seed: u64) -> Scenario {
    let mut rng = Rng::new(seed);
    let mut dimensions = Dimensions::draw(&mut rng);
    dimensions.pre_registration_burst = world.label == "ens_v1_mainnet"
        && Rng::new(seed ^ PRE_REGISTRATION_BURST_SALT).chance(1, 4);
    let blocks = draw_blocks(&mut rng);
    let settle_timestamp = blocks.last().expect("scenario has blocks").timestamp;
    let mut actions = pool(world, wiring, &dimensions, settle_timestamp);
    rng.shuffle(&mut actions);
    repair_preconditions(&mut actions);
    let action_names = actions.iter().map(|action| action.name.clone()).collect();
    let logs = lay_out(&mut rng, &dimensions, blocks.len(), actions);
    Scenario {
        seed,
        world,
        dimensions,
        action_names,
        blocks,
        logs,
    }
}

/// Orders each subject's actions among the positions the shuffle gave that subject, so its
/// preconditions land first while its interleaving with every other subject stays as permuted.
/// Sorting the whole scenario by stage instead would make every batch boundary a dependency-closed
/// prefix and delete the orderings this lane exists to find; hoisting the root or a registry above
/// every name would assert a precondition the pools do not have, since a registry can be running
/// names before governance points the root at it.
fn repair_preconditions(actions: &mut [Action]) {
    let mut chains: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, item) in actions.iter().enumerate() {
        chains.entry(item.chain.as_str()).or_default().push(index);
    }
    for slots in chains.into_values().collect::<Vec<_>>() {
        if slots.len() < 2 {
            continue;
        }
        let mut chain = slots
            .iter()
            .map(|slot| std::mem::take(&mut actions[*slot]))
            .collect::<Vec<_>>();
        chain.sort_by_key(|item| item.stage);
        for (slot, item) in slots.into_iter().zip(chain) {
            actions[slot] = item;
        }
    }
}

pub fn pool(
    world: &World,
    wiring: &Wiring,
    dimensions: &Dimensions,
    settle_timestamp: i64,
) -> Vec<Action> {
    match world.label {
        "ens_v1_mainnet" => pool_v1::build(wiring, dimensions, settle_timestamp),
        "ens_v1_sepolia" => pool_v1_sepolia::build(wiring, dimensions, settle_timestamp),
        "ens_v2_sepolia" => pool_v2::build(wiring, dimensions, settle_timestamp),
        label => panic!("unknown permutation world {label}"),
    }
}

fn draw_blocks(rng: &mut Rng) -> Vec<BlockSpec> {
    let count = rng.between(3, 6);
    let mut blocks = Vec::with_capacity(count);
    let mut number = BASE_BLOCK;
    let mut timestamp = BASE_TIMESTAMP;
    for index in 0..count {
        blocks.push(BlockSpec {
            number,
            hash: format!("0x{:064x}", 0xb10c_0000_u64 + number as u64),
            timestamp,
        });
        number += i64::try_from(rng.between(1, 40)).expect("gap fits i64");
        // The final gap is long enough that a lapsed lease settles at a block boundary, and longer
        // than the 200 days the past-grace window backdates an expiry, so that a registration is
        // never already expired at the block it is registered in.
        timestamp += if index + 2 == count {
            i64::try_from(rng.between(201, 400)).expect("gap fits i64") * 86_400
        } else {
            i64::try_from(rng.between(1, 48)).expect("gap fits i64") * 3_600
        };
    }
    blocks
}

/// The last block carries no logs, so every scenario exercises a bare block boundary.
fn lay_out(
    rng: &mut Rng,
    dimensions: &Dimensions,
    block_count: usize,
    actions: Vec<Action>,
) -> Vec<GeneratedLog> {
    let usable = block_count.saturating_sub(1).max(1);
    let mut logs = Vec::new();
    let mut block_index = 0_usize;
    let mut transaction_index = 0_i64;
    let mut log_index = 0_i64;
    let mut placed_any = false;
    for action in actions {
        if !placed_any {
            placed_any = true;
        } else if block_index + 1 < usable && rng.chance(1, 3) {
            block_index += 1;
            transaction_index = 0;
            log_index = 0;
        } else if !dimensions.dense_transactions || !rng.chance(1, 2) {
            transaction_index += 1;
        }
        let transaction_hash = format!(
            "0x{:064x}",
            block_index * 1_000 + transaction_index as usize
        );
        for emission in action.emissions {
            logs.push(GeneratedLog {
                block_index,
                transaction_hash: transaction_hash.clone(),
                transaction_index,
                log_index,
                emitter: emission.emitter,
                topics: emission.topics,
                data: emission.data,
                burst: emission.burst,
            });
            log_index += 1;
        }
    }
    logs
}

#[test]
fn repairing_preconditions_only_reorders_within_a_subject() {
    let scrambled = [
        ("alpha:late", stage::LATE),
        ("bravo:write", stage::WRITE),
        ("alpha:register", stage::REGISTER),
        ("root:link", stage::LINK),
        ("bravo:register", stage::REGISTER),
        ("alpha:link", stage::LINK),
        ("root:register", stage::REGISTER),
    ];
    let mut actions = scrambled
        .iter()
        .map(|(name, stage)| action(*name, *stage, Vec::new()))
        .collect::<Vec<_>>();
    let occupied = |actions: &[Action]| {
        let mut slots: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for (index, item) in actions.iter().enumerate() {
            slots.entry(item.chain.clone()).or_default().push(index);
        }
        slots
    };
    let before = occupied(&actions);
    repair_preconditions(&mut actions);

    assert_eq!(
        before,
        occupied(&actions),
        "a subject moved into another subject's positions, so the interleaving the shuffle drew \
         was not preserved"
    );
    let mut names = actions.iter().map(|item| &item.name).collect::<Vec<_>>();
    names.sort();
    let mut expected = scrambled.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(
        names, expected,
        "the repair dropped or duplicated an action"
    );
    for slots in occupied(&actions).into_values() {
        let stages = slots
            .iter()
            .map(|slot| actions[*slot].stage)
            .collect::<Vec<_>>();
        assert!(
            stages.windows(2).all(|pair| pair[0] <= pair[1]),
            "a precondition still follows what it enables: {stages:?}"
        );
    }
}
