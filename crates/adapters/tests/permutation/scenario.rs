use std::collections::BTreeMap;

use alloy_primitives::LogData;

use super::{
    events::encoded_topics,
    pool_v1, pool_v2,
    rng::Rng,
    world::{BlockSpec, GeneratedLog, Wiring, World},
};

/// One raw log, still unpositioned. Emissions inside an action stay in one transaction.
pub struct Emission {
    pub emitter: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
}

pub struct Action {
    pub name: String,
    pub emissions: Vec<Emission>,
    /// Actions sharing a group are re-sorted by `rank` after shuffling, so a permutation cannot
    /// place a protocol precondition after the event that requires it. Everything else is free.
    pub group: Option<String>,
    pub rank: u8,
}

pub fn emission(emitter: &str, encoded: LogData) -> Emission {
    Emission {
        emitter: emitter.to_owned(),
        topics: encoded_topics(&encoded),
        data: encoded.data.to_vec(),
    }
}

pub fn action(name: impl Into<String>, emissions: Vec<Emission>) -> Action {
    Action {
        name: name.into(),
        emissions,
        group: None,
        rank: 0,
    }
}

pub fn ordered_action(
    name: impl Into<String>,
    group: impl Into<String>,
    rank: u8,
    emissions: Vec<Emission>,
) -> Action {
    Action {
        group: Some(group.into()),
        rank,
        ..action(name, emissions)
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
}

impl Dimensions {
    /// Every combination of the axes that decides *which* events a pool contains — wrap state,
    /// record state, subname shape, authorization shape, and registration path — with all
    /// perturbations enabled. The expiry window, name count, and transaction density only change
    /// event payloads and layout, never the set of fragments, so they stay pinned. Used to prove
    /// event coverage without depending on which combinations a seed happens to draw.
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

pub fn generate(world: &'static World, wiring: &Wiring, seed: u64) -> Scenario {
    let mut rng = Rng::new(seed);
    let dimensions = Dimensions::draw(&mut rng);
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

/// Reorders each group's actions among the positions the shuffle gave them, so preconditions land
/// first while the interleaving with every other group stays as permuted.
fn repair_preconditions(actions: &mut [Action]) {
    let mut positions: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, item) in actions.iter().enumerate() {
        if let Some(group) = item.group.as_ref() {
            positions.entry(group.clone()).or_default().push(index);
        }
    }
    for slots in positions.into_values().filter(|slots| slots.len() > 1) {
        let mut group = slots
            .iter()
            .map(|slot| std::mem::replace(&mut actions[*slot], action("", Vec::new())))
            .collect::<Vec<_>>();
        group.sort_by_key(|item| item.rank);
        for (slot, item) in slots.iter().zip(group) {
            actions[*slot] = item;
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
        _ => pool_v2::build(wiring, dimensions, settle_timestamp),
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
        // The final gap is long enough that a lapsed lease settles at a block boundary.
        timestamp += if index + 2 == count {
            i64::try_from(rng.between(90, 400)).expect("gap fits i64") * 86_400
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
            });
            log_index += 1;
        }
    }
    logs
}
