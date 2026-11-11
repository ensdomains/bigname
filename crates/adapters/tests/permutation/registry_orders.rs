//! Generated orders of ENSv2 registry events, checked with the lane's `converge`.
//!
//! `pool_v2` emits each name's events in the order the deployed `PermissionedRegistry` emits them
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464-L479 @ ens_v2@a971bd64).
//! Interpret also admits any contract that announces itself with `RegistryCreated()`, and that
//! contract can emit the same events in any order. This generator draws bounded sequences over
//! the registry's event alphabet with no ordering repair: `RegistryCreated`, `LabelRegistered`,
//! `LabelReserved`, mint, transfer and burn, `LabelUnregistered`, `TokenResource`,
//! `ResolverUpdated`, `SubregistryUpdated`, `ExpiryUpdated`, `EACRolesChanged` and
//! `TokenRegenerated`. It also draws the deployed registry's one-call registration and
//! regeneration as single actions, so those orders mix with the rest.
//!
//! Each sequence draws events from the declared Sepolia ETH registry and from an announced
//! registry, and from two labels and three token versions. So one token can carry two labels and
//! one label can sit on two tokens. Half the sequences first wire the root's `eth` to the ETH
//! registry and a name there to the announced registry, so both registries' labels have names.
//!
//! The property, for each sequence, in each of several block layouts and batch splits: live
//! adapter state after every batch equals a restore from the stored rows, and the split replay
//! emits the same rows as the whole pass, with no batch-boundary artifact.
//!
//! Knobs:
//! - `BIGNAME_REGISTRY_ORDER_CASES`: sequences per run. Default 64.
//! - `BIGNAME_REGISTRY_ORDER_SEED`: base seed, decimal. Default 2119274496.
//!
//! A failure names its seed. `BIGNAME_REGISTRY_ORDER_CASES=1 BIGNAME_REGISTRY_ORDER_SEED=<seed>`
//! replays that one sequence. A longer soak is a manual run, not a CI job:
//!
//! ```text
//! BIGNAME_REGISTRY_ORDER_CASES=20000 cargo test -p bigname-adapters --features test-activation \
//!   --test schema_v2_permutation_lane generated_registry_orders -- --nocapture
//! ```
//!
//! A failing sequence is shrunk by removing actions, then single events, while it fails the same
//! way. The report prints the minimal sequence and the difference.

use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    fmt,
    ops::Range,
    panic::{AssertUnwindSafe, catch_unwind},
    time::{Duration, Instant},
};

use alloy_primitives::{Address, LogData, U256};
use alloy_sol_types::SolEvent;
use anyhow::{Result, bail};
use bigname_adapters::schema_v2::BatchInput;

use super::{
    events::V2Registry,
    invariants::{Converged, converge, split},
    names::labelhash,
    rng::Rng,
    scenario::emission,
    world::{BlockSpec, GeneratedLog, Wiring},
};

pub const DEFAULT_CASES: u64 = 64;
pub const DEFAULT_SEED: u64 = 0x7e51_9000;
const CASE_STRIDE: u64 = 0x9fb2_1c65_1e98_df25;
const SPLIT_SALT: u64 = 0x2545_f491_4f6c_dd1d;
const MAX_ACTIONS: usize = 10;
const SHRINK_BUDGET: usize = 250;
/// Findings shrunk per failure kind. Later ones are reported as drawn.
const SHRUNK_PER_KIND: usize = 8;
const BASE_BLOCK: i64 = 20_519_000;
const BASE_TIMESTAMP: i64 = 1_700_000_000;
const BLOCK_SECONDS: i64 = 12;
const FAR_EXPIRY: u64 = 1_900_000_000;
const LABELS: [&str; 2] = ["order", "other"];
const ANNOUNCED: &str = "0x00000000000000000000000000000000f0051900";
const RESOLVERS: [&str; 2] = [
    "0x00000000000000000000000000000000f0051901",
    "0x00000000000000000000000000000000f0051902",
];
const SUBREGISTRY: &str = "0x00000000000000000000000000000000f0051903";

/// Orders the TYR-510 fix (#1420) changed: a label registered on a token that already holds a
/// resource keeps it, and the token's live registration becomes the row the label stored. The
/// sweep gates them like any order and requires the default corpus to reach both.
pub const TOKEN_RESOURCE_BEFORE_LABEL: &str =
    "LabelRegistered on a labelless token that holds a resource";
pub const LABEL_OVER_RESERVATION: &str =
    "LabelRegistered over a reservation of the same label that holds a resource";
pub const TYR_510_SHAPES: [&str; 2] = [TOKEN_RESOURCE_BEFORE_LABEL, LABEL_OVER_RESERVATION];

/// Failure kinds tracked by open tickets. The sweep shrinks one finding of each, reports them and
/// does not fail on them. Each comes out when its ticket lands.
///
/// TYR-522 S-A3, out of TYR-522's PR: a binding closed and then re-linked under the same token id.
/// The writer's upsert guard refuses the re-emitted surface binding whose `active_from` moved, and
/// the refusal stops the batch (`crates/interpret/src/write/identity.rs`). Only custom bytecode
/// emits that order. A second `TokenResource` for a token still holding its binding converges
/// since #1425 and is gated like any order.
pub const TYR_522_S_A3: &str = "TYR-522 S-A3, out of TYR-522's PR";
/// TYR-525: a `LabelRegistered` that is already expired, or whose token is already registered,
/// when it is emitted, or an `ExpiryUpdated` that cuts a label's live expiry to one that has
/// passed. Live state and a restore disagree on the expiry retirement flags, or the split replay
/// attributes, releases or transfers differently. The deployed registry reverts all three orders
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L438-L460 @ ens_v2@a971bd64)
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L222-L224 @ ens_v2@a971bd64).
pub const TYR_525: &str = "TYR-525";
/// TYR-526: a pointer update and an unregister of a token, with or without a label, then a
/// transfer of it. Only the whole pass marks the transfer's registry state as pending hydration.
pub const TYR_526: &str = "TYR-526";

/// TYR-525's symptoms: the expiry retirement flags differ between live state and a restore, the
/// split replay re-attributes an event, or the two passes release a registration on expiry
/// differently.
fn tyr_525_symptom(signature: &str, text: &str) -> bool {
    signature == "live state differs from a restore at expiry_retirement_emitted"
        || signature == "live state differs from a restore at resource_expiry_retirement_emitted"
        || signature == "batch-boundary artifact {\"rebased_attributions\": 1}"
        || (signature == "the split replay derives a different event stream"
            && text.contains(":RegistrationReleased:expiry:"))
}

/// TYR-525's two further symptoms of a token registered again while its registration is live: the
/// split replay re-attributes any number of events, or a later transfer's `token_lineage_id` and
/// `upstream_resource` differ between the passes and nothing else does.
fn tyr_525_reregistration_symptom(signature: &str, text: &str) -> bool {
    let rebased = signature
        .strip_prefix("batch-boundary artifact {\"rebased_attributions\": ")
        .and_then(|rest| rest.strip_suffix('}'))
        .is_some_and(|count| !count.is_empty() && count.bytes().all(|byte| byte.is_ascii_digit()));
    rebased
        || (signature
            == "an event's after_state differs between the whole pass and the split replay"
            && text.contains(":TokenControlTransferred:")
            && only_the_transfer_lineage_differs(text))
}

fn only_the_transfer_lineage_differs(text: &str) -> bool {
    let parse = |marker| {
        after_state(text, marker)
            .and_then(|state| serde_json::from_str::<serde_json::Value>(state).ok())
            .and_then(|state| state.as_object().cloned())
    };
    let (Some(mut whole), Some(mut replayed)) = (parse("whole="), parse("replayed=")) else {
        return false;
    };
    if whole == replayed {
        return false;
    }
    for key in ["token_lineage_id", "upstream_resource"] {
        whole.remove(key);
        replayed.remove(key);
    }
    whole == replayed
}

/// The registrations the model holds, with their expiries, so both TYR-525 shapes judge a label
/// registered again by the same rule: it counts only while the held registration is live.
#[derive(Default)]
struct Registrations(BTreeMap<(Registry, Token), Expiry>);

impl Registrations {
    /// Applies one event at its block's timestamp. For a `LabelRegistered` over a held
    /// registration, whether that registration is still live at `at`. A reservation or an
    /// unregister ends the held registration, a renewal moves its expiry and a regeneration moves it
    /// to the new token.
    fn registers_again(&mut self, atom: Atom, at: i64) -> Option<bool> {
        match atom {
            Atom::Register {
                registry,
                token,
                expiry,
                ..
            } => self
                .0
                .insert((registry, token), expiry)
                .map(|before| !passed(before, at)),
            Atom::Reserve {
                registry, token, ..
            }
            | Atom::Unregister { registry, token } => {
                self.0.remove(&(registry, token));
                None
            }
            Atom::Renew {
                registry,
                token,
                expiry,
            } => {
                if let Some(current) = self.0.get_mut(&(registry, token)) {
                    *current = expiry;
                }
                None
            }
            Atom::Regenerate { registry, from, to } => {
                if let Some(expiry) = self.0.remove(&(registry, from)) {
                    self.0.insert((registry, to), expiry);
                }
                None
            }
            _ => None,
        }
    }
}

pub const PASSED_WHEN_EMITTED: &str = "TYR-525: a registration already passed when emitted";
pub const CUT_TO_PASSED: &str = "TYR-525: a live expiry cut to a passed one";
pub const AGAIN_WHILE_LIVE: &str = "TYR-525: a registration again while the first is live";
pub const AGAIN_AFTER_LAPSE: &str = "a registration again after the first lapsed (not a ticket's)";

/// Which expiry and re-registration shapes an order reaches in `layout`, in one walk, so the
/// TYR-525 tag and the sweep's shape counts read the same model.
fn expiry_shapes(actions: &[Action], layout: Layout) -> BTreeSet<&'static str> {
    let mut registrations = Registrations::default();
    let mut expiries = BTreeMap::new();
    let mut shapes = BTreeSet::new();
    for (atom, at) in timed_atoms(actions, layout) {
        match registrations.registers_again(atom, at) {
            Some(true) => {
                shapes.insert(AGAIN_WHILE_LIVE);
            }
            Some(false) => {
                shapes.insert(AGAIN_AFTER_LAPSE);
            }
            None => {}
        }
        match atom {
            Atom::Register {
                registry,
                token,
                expiry,
                ..
            } => {
                if passed(expiry, at) {
                    shapes.insert(PASSED_WHEN_EMITTED);
                }
                expiries.insert((registry, token), expiry);
            }
            Atom::Reserve {
                registry,
                token,
                expiry,
                ..
            } => {
                expiries.insert((registry, token), expiry);
            }
            Atom::Renew {
                registry,
                token,
                expiry,
            } => {
                if let Some(&before) = expiries.get(&(registry, token))
                    && !passed(before, at)
                    && passed(expiry, at)
                {
                    shapes.insert(CUT_TO_PASSED);
                }
                if let Some(current) = expiries.get_mut(&(registry, token)) {
                    *current = expiry;
                }
            }
            Atom::Unregister { registry, token } => {
                expiries.remove(&(registry, token));
            }
            Atom::Regenerate { registry, from, to } => {
                if let Some(expiry) = expiries.remove(&(registry, from)) {
                    expiries.insert((registry, to), expiry);
                }
            }
            _ => {}
        }
    }
    shapes
}

pub const RELEASED_POINTER_TRANSFER: &str =
    "TYR-526: a transfer after a pointer update and an unregister";
pub const RELINK_AFTER_CLOSE: &str = "TYR-522 S-A3: a binding closed and linked again";

/// Every shape the sweep counts sequences for: each known tag's order, the two TYR-510 orders,
/// and a registration again after the first lapsed. The default corpus must reach each.
pub const NAMED_SHAPES: [&str; 8] = [
    PASSED_WHEN_EMITTED,
    CUT_TO_PASSED,
    AGAIN_WHILE_LIVE,
    AGAIN_AFTER_LAPSE,
    RELEASED_POINTER_TRANSFER,
    RELINK_AFTER_CLOSE,
    TOKEN_RESOURCE_BEFORE_LABEL,
    LABEL_OVER_RESERVATION,
];

/// The named shapes an order reaches in any layout the sweep runs.
fn named_shapes(actions: &[Action]) -> BTreeSet<&'static str> {
    let mut shapes = tyr_510_shapes(actions);
    for (layout, _) in RUNS {
        shapes.extend(expiry_shapes(actions, layout));
    }
    if transfers_a_released_pointer_token(actions) {
        shapes.insert(RELEASED_POINTER_TRANSFER);
    }
    if links_a_linked_token(actions) && relinks_a_closed_binding(actions) {
        shapes.insert(RELINK_AFTER_CLOSE);
    }
    shapes
}

/// A `LabelRegistered` over a token whose registration is still live at the new label's block.
fn reregisters_a_live_token(actions: &[Action], layout: Layout) -> bool {
    expiry_shapes(actions, layout).contains(AGAIN_WHILE_LIVE)
}

/// The open ticket a failure belongs to, judged on its kind, the failure text and the order's
/// shape in the layout it failed in. A kind alone never tags: each ticket names an order.
pub fn known_failure(
    signature: &str,
    text: &str,
    actions: &[Action],
    layout: Layout,
) -> Option<&'static str> {
    if signature == "the writer would reject a repeated surface_bindings row"
        && links_a_linked_token(actions)
        && relinks_a_closed_binding(actions)
    {
        return Some(TYR_522_S_A3);
    }
    if tyr_525_symptom(signature, text) && emits_a_passed_or_live_label(actions, layout) {
        return Some(TYR_525);
    }
    if tyr_525_reregistration_symptom(signature, text) && reregisters_a_live_token(actions, layout)
    {
        return Some(TYR_525);
    }
    if signature.starts_with("an event's after_state differs")
        && only_the_whole_pass_is_pending_hydration(text)
        && transfers_a_released_pointer_token(actions)
    {
        return Some(TYR_526);
    }
    None
}

fn atoms(actions: &[Action]) -> impl Iterator<Item = Atom> + '_ {
    actions
        .iter()
        .flat_map(|action| action.atoms.iter().copied())
}

/// Each event with the timestamp of the block `layout` places it in.
fn timed_atoms(actions: &[Action], layout: Layout) -> Vec<(Atom, i64)> {
    actions
        .iter()
        .zip(place(actions, layout))
        .flat_map(|(action, (block, _))| {
            let at = BASE_TIMESTAMP + BLOCK_SECONDS * block as i64;
            action.atoms.iter().map(move |atom| (*atom, at))
        })
        .collect()
}

fn passed(expiry: Expiry, at: i64) -> bool {
    i64::try_from(expiry.value()).is_ok_and(|expiry| expiry <= at)
}

/// A second `TokenResource` for a token the same registry already linked.
fn links_a_linked_token(actions: &[Action]) -> bool {
    let mut linked = BTreeSet::new();
    atoms(actions).any(|atom| match atom {
        Atom::Link {
            registry, token, ..
        } => !linked.insert((registry, token)),
        _ => false,
    })
}

fn relinks_a_closed_binding(actions: &[Action]) -> bool {
    let mut linked = BTreeSet::new();
    let mut closed = BTreeSet::new();
    for atom in atoms(actions) {
        match atom {
            Atom::Link {
                registry, token, ..
            } => {
                if closed.contains(&(registry, token)) {
                    return true;
                }
                linked.insert((registry, token));
            }
            Atom::Unregister { registry, token }
            | Atom::Register {
                registry, token, ..
            }
            | Atom::Reserve {
                registry, token, ..
            }
            | Atom::Regenerate {
                registry,
                from: token,
                ..
            } if linked.contains(&(registry, token)) => {
                closed.insert((registry, token));
            }
            _ => {}
        }
    }
    false
}

/// A `LabelRegistered` whose expiry has passed at its own block, or over a registration still live
/// at that block, or an `ExpiryUpdated` that moves a label's expiry from live to passed at its own
/// block. A lapse later in the sequence, a label registered again after the first lapsed, and a
/// renewal of a label that had already lapsed are orders the deployed registry emits and are not
/// this ticket's.
fn emits_a_passed_or_live_label(actions: &[Action], layout: Layout) -> bool {
    let shapes = expiry_shapes(actions, layout);
    [PASSED_WHEN_EMITTED, CUT_TO_PASSED, AGAIN_WHILE_LIVE]
        .iter()
        .any(|shape| shapes.contains(shape))
}

/// The diverging event's after states differ only in the whole pass marking the registry state as
/// pending hydration.
fn only_the_whole_pass_is_pending_hydration(text: &str) -> bool {
    let (Some(whole), Some(replayed)) =
        (after_state(text, "whole="), after_state(text, "replayed="))
    else {
        return false;
    };
    let flag = "\"registry_hydration_pending\":true";
    [format!("{flag},"), format!(",{flag}")]
        .iter()
        .any(|cut| whole.contains(cut.as_str()) && whole.replacen(cut.as_str(), "", 1) == replayed)
}

fn after_state<'a>(text: &'a str, marker: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|line| line.trim_start().strip_prefix(marker))
}

/// A transfer of a token after its registry updated a pointer on it and unregistered it, with or
/// without a label. A label event after the unregister starts the token over.
fn transfers_a_released_pointer_token(actions: &[Action]) -> bool {
    let mut pointed = BTreeSet::new();
    let mut released = BTreeSet::new();
    for atom in atoms(actions) {
        match atom {
            Atom::Resolver {
                registry, token, ..
            }
            | Atom::Subregistry {
                registry, token, ..
            } => {
                pointed.insert((registry, token));
            }
            Atom::Unregister { registry, token } => {
                released.insert((registry, token));
            }
            Atom::Register {
                registry, token, ..
            }
            | Atom::Reserve {
                registry, token, ..
            } => {
                released.remove(&(registry, token));
            }
            Atom::Transfer {
                registry, token, ..
            } if pointed.contains(&(registry, token)) && released.contains(&(registry, token)) => {
                return true;
            }
            _ => {}
        }
    }
    false
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Registry {
    Declared,
    Announced,
}

/// A token id: the label's hash with `version` in its low 32 bits.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Token {
    pub label: usize,
    pub version: u32,
}

impl Token {
    pub fn id(self) -> U256 {
        let mut bytes = labelhash(LABELS[self.label]).0;
        bytes[28..].copy_from_slice(&self.version.to_be_bytes());
        U256::from_be_bytes(bytes)
    }

    fn next(self) -> Self {
        Self {
            version: self.version + 1,
            ..self
        }
    }

    fn resource(self) -> Self {
        Self { version: 0, ..self }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Expiry {
    Far,
    Past,
    /// Lapses between block `n` and block `n + 1` of a layout.
    Lapses(i64),
}

impl Expiry {
    fn value(self) -> u64 {
        let at = match self {
            Self::Far => return FAR_EXPIRY,
            Self::Past => BASE_TIMESTAMP - 1_000,
            Self::Lapses(block) => BASE_TIMESTAMP + BLOCK_SECONDS * block + BLOCK_SECONDS / 2,
        };
        u64::try_from(at).expect("a generated expiry is positive")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Pointer {
    Announced,
    Other(usize),
    Clear,
}

/// One registry event. Owners and accounts are small actor numbers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Atom {
    Announce,
    /// The root registers `eth` and points it at the declared registry.
    RootEth,
    Register {
        registry: Registry,
        token: Token,
        label: usize,
        owner: u8,
        expiry: Expiry,
    },
    Reserve {
        registry: Registry,
        token: Token,
        label: usize,
        expiry: Expiry,
    },
    Mint {
        registry: Registry,
        token: Token,
        to: u8,
    },
    Transfer {
        registry: Registry,
        token: Token,
        from: u8,
        to: u8,
    },
    Burn {
        registry: Registry,
        token: Token,
        from: u8,
    },
    Unregister {
        registry: Registry,
        token: Token,
    },
    Link {
        registry: Registry,
        token: Token,
        resource: Token,
    },
    Resolver {
        registry: Registry,
        token: Token,
        to: Pointer,
    },
    Subregistry {
        registry: Registry,
        token: Token,
        to: Pointer,
    },
    Renew {
        registry: Registry,
        token: Token,
        expiry: Expiry,
    },
    /// `resource: None` is the registry's root resource.
    Roles {
        registry: Registry,
        resource: Option<Token>,
        account: u8,
        old: u8,
        new: u8,
    },
    Regenerate {
        registry: Registry,
        from: Token,
        to: Token,
    },
}

pub const ATOM_KINDS: [&str; 14] = [
    "RegistryCreated",
    "root eth",
    "LabelRegistered",
    "LabelReserved",
    "mint",
    "transfer",
    "burn",
    "LabelUnregistered",
    "TokenResource",
    "ResolverUpdated",
    "SubregistryUpdated",
    "ExpiryUpdated",
    "EACRolesChanged",
    "TokenRegenerated",
];

impl Atom {
    pub fn kind(self) -> &'static str {
        match self {
            Self::Announce => ATOM_KINDS[0],
            Self::RootEth => ATOM_KINDS[1],
            Self::Register { .. } => ATOM_KINDS[2],
            Self::Reserve { .. } => ATOM_KINDS[3],
            Self::Mint { .. } => ATOM_KINDS[4],
            Self::Transfer { .. } => ATOM_KINDS[5],
            Self::Burn { .. } => ATOM_KINDS[6],
            Self::Unregister { .. } => ATOM_KINDS[7],
            Self::Link { .. } => ATOM_KINDS[8],
            Self::Resolver { .. } => ATOM_KINDS[9],
            Self::Subregistry { .. } => ATOM_KINDS[10],
            Self::Renew { .. } => ATOM_KINDS[11],
            Self::Roles { .. } => ATOM_KINDS[12],
            Self::Regenerate { .. } => ATOM_KINDS[13],
        }
    }

    /// The registry that emits this event, none for the root.
    fn registry(self) -> Option<Registry> {
        match self {
            Self::Announce => Some(Registry::Announced),
            Self::RootEth => None,
            Self::Register { registry, .. }
            | Self::Reserve { registry, .. }
            | Self::Mint { registry, .. }
            | Self::Transfer { registry, .. }
            | Self::Burn { registry, .. }
            | Self::Unregister { registry, .. }
            | Self::Link { registry, .. }
            | Self::Resolver { registry, .. }
            | Self::Subregistry { registry, .. }
            | Self::Renew { registry, .. }
            | Self::Roles { registry, .. }
            | Self::Regenerate { registry, .. } => Some(registry),
        }
    }

    fn logs(self, addresses: &Addresses) -> Vec<(String, LogData)> {
        let sender = actor(0);
        let one = |registry: Registry, encoded: LogData| {
            vec![(addresses.emitter(registry).to_owned(), encoded)]
        };
        match self {
            Self::Announce => vec![(
                ANNOUNCED.to_owned(),
                V2Registry::RegistryCreated {}.encode_log_data(),
            )],
            Self::RootEth => {
                let eth = U256::from_be_bytes(labelhash("eth").0);
                vec![
                    (
                        addresses.root.clone(),
                        V2Registry::LabelRegistered {
                            tokenId: eth,
                            labelHash: labelhash("eth"),
                            label: "eth".to_owned(),
                            owner: sender,
                            expiry: 0,
                            sender,
                        }
                        .encode_log_data(),
                    ),
                    (
                        addresses.root.clone(),
                        V2Registry::SubregistryUpdated {
                            tokenId: eth,
                            subregistry: address(&addresses.declared),
                            sender,
                        }
                        .encode_log_data(),
                    ),
                ]
            }
            Self::Register {
                registry,
                token,
                label,
                owner,
                expiry,
            } => one(
                registry,
                V2Registry::LabelRegistered {
                    tokenId: token.id(),
                    labelHash: labelhash(LABELS[label]),
                    label: LABELS[label].to_owned(),
                    owner: actor(owner),
                    expiry: expiry.value(),
                    sender,
                }
                .encode_log_data(),
            ),
            Self::Reserve {
                registry,
                token,
                label,
                expiry,
            } => one(
                registry,
                V2Registry::LabelReserved {
                    tokenId: token.id(),
                    labelHash: labelhash(LABELS[label]),
                    label: LABELS[label].to_owned(),
                    expiry: expiry.value(),
                    sender,
                }
                .encode_log_data(),
            ),
            Self::Mint {
                registry,
                token,
                to,
            } => one(registry, transfer(token, Address::ZERO, actor(to))),
            Self::Transfer {
                registry,
                token,
                from,
                to,
            } => one(registry, transfer(token, actor(from), actor(to))),
            Self::Burn {
                registry,
                token,
                from,
            } => one(registry, transfer(token, actor(from), Address::ZERO)),
            Self::Unregister { registry, token } => one(
                registry,
                V2Registry::LabelUnregistered {
                    tokenId: token.id(),
                    sender,
                }
                .encode_log_data(),
            ),
            Self::Link {
                registry,
                token,
                resource,
            } => one(
                registry,
                V2Registry::TokenResource {
                    tokenId: token.id(),
                    resource: resource.id(),
                }
                .encode_log_data(),
            ),
            Self::Resolver {
                registry,
                token,
                to,
            } => one(
                registry,
                V2Registry::ResolverUpdated {
                    tokenId: token.id(),
                    resolver: match to {
                        Pointer::Announced => address(ANNOUNCED),
                        Pointer::Other(index) => address(RESOLVERS[index % RESOLVERS.len()]),
                        Pointer::Clear => Address::ZERO,
                    },
                    sender,
                }
                .encode_log_data(),
            ),
            Self::Subregistry {
                registry,
                token,
                to,
            } => one(
                registry,
                V2Registry::SubregistryUpdated {
                    tokenId: token.id(),
                    subregistry: match to {
                        Pointer::Announced => address(ANNOUNCED),
                        Pointer::Other(_) => address(SUBREGISTRY),
                        Pointer::Clear => Address::ZERO,
                    },
                    sender,
                }
                .encode_log_data(),
            ),
            Self::Renew {
                registry,
                token,
                expiry,
            } => one(
                registry,
                V2Registry::ExpiryUpdated {
                    tokenId: token.id(),
                    newExpiry: expiry.value(),
                    sender,
                }
                .encode_log_data(),
            ),
            Self::Roles {
                registry,
                resource,
                account,
                old,
                new,
            } => one(
                registry,
                V2Registry::EACRolesChanged {
                    resource: resource.map_or(U256::ZERO, Token::id),
                    account: actor(account),
                    oldRoleBitmap: U256::from(old),
                    newRoleBitmap: U256::from(new),
                }
                .encode_log_data(),
            ),
            Self::Regenerate { registry, from, to } => one(
                registry,
                V2Registry::TokenRegenerated {
                    oldTokenId: from.id(),
                    newTokenId: to.id(),
                }
                .encode_log_data(),
            ),
        }
    }
}

fn transfer(token: Token, from: Address, to: Address) -> LogData {
    V2Registry::TransferSingle {
        operator: actor(0),
        from,
        to,
        id: token.id(),
        value: U256::from(1),
    }
    .encode_log_data()
}

fn actor(index: u8) -> Address {
    Address::with_last_byte(0xa0 + index)
}

fn address(value: &str) -> Address {
    value.parse().expect("a generated address")
}

struct Addresses {
    root: String,
    declared: String,
}

impl Addresses {
    fn of(wiring: &Wiring) -> Self {
        Self {
            root: wiring.address("ens_v2_root_l1", "root_registry").to_owned(),
            declared: wiring.address("ens_v2_registry_l1", "registry").to_owned(),
        }
    }

    fn emitter(&self, registry: Registry) -> &str {
        match registry {
            Registry::Declared => &self.declared,
            Registry::Announced => ANNOUNCED,
        }
    }
}

/// One transaction's events. `new_block` and `same_transaction` place it in the drawn layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Action {
    pub atoms: Vec<Atom>,
    pub new_block: bool,
    pub same_transaction: bool,
}

pub struct Case {
    pub seed: u64,
    pub actions: Vec<Action>,
}

pub fn case_seed(base: u64, case: u64) -> u64 {
    base.wrapping_add(case.wrapping_mul(CASE_STRIDE))
}

pub fn generate(seed: u64) -> Case {
    let mut rng = Rng::new(seed);
    let mut actions = Vec::new();
    if rng.chance(1, 2) {
        let named = Token {
            label: 0,
            version: 0,
        };
        actions.push(action(vec![Atom::Announce], false, false));
        actions.push(action(vec![Atom::RootEth], false, false));
        actions.push(action(
            deployed_register(
                Registry::Declared,
                named,
                0,
                1,
                Expiry::Far,
                Some(Pointer::Announced),
            ),
            true,
            false,
        ));
    }
    for _ in 0..rng.between(1, MAX_ACTIONS) {
        let atoms = draw(&mut rng);
        let new_block = rng.chance(1, 2);
        let same_transaction = rng.chance(1, 4);
        actions.push(action(atoms, new_block, same_transaction));
    }
    announce_before_use(&mut rng, &mut actions);
    Case { seed, actions }
}

fn action(atoms: Vec<Atom>, new_block: bool, same_transaction: bool) -> Action {
    Action {
        atoms,
        new_block,
        same_transaction,
    }
}

fn draw(rng: &mut Rng) -> Vec<Atom> {
    let registry = if rng.chance(1, 2) {
        Registry::Declared
    } else {
        Registry::Announced
    };
    let token = draw_token(rng);
    let label = if rng.chance(3, 4) {
        token.label
    } else {
        1 - token.label
    };
    let expiry = draw_expiry(rng);
    let owner = draw_actor(rng);
    match rng.below(100) {
        0..=15 => vec![Atom::Register {
            registry,
            token,
            label,
            owner,
            expiry,
        }],
        16..=22 => vec![Atom::Reserve {
            registry,
            token,
            label,
            expiry,
        }],
        23..=27 => vec![Atom::Mint {
            registry,
            token,
            to: owner,
        }],
        28..=35 => vec![Atom::Transfer {
            registry,
            token,
            from: owner,
            to: draw_actor(rng),
        }],
        36..=38 => vec![Atom::Burn {
            registry,
            token,
            from: owner,
        }],
        39..=43 => vec![Atom::Unregister { registry, token }],
        44..=53 => vec![Atom::Link {
            registry,
            token,
            resource: if rng.chance(2, 3) {
                token.resource()
            } else {
                draw_token(rng)
            },
        }],
        54..=60 => vec![Atom::Resolver {
            registry,
            token,
            to: if rng.chance(1, 4) {
                Pointer::Clear
            } else {
                Pointer::Other(rng.below(RESOLVERS.len()))
            },
        }],
        61..=67 => vec![Atom::Subregistry {
            registry,
            token,
            to: match rng.below(3) {
                0 => Pointer::Announced,
                1 => Pointer::Other(0),
                _ => Pointer::Clear,
            },
        }],
        68..=74 => vec![Atom::Renew {
            registry,
            token,
            expiry,
        }],
        75..=80 => vec![Atom::Roles {
            registry,
            resource: (!rng.chance(1, 5)).then_some(token.resource()),
            account: owner,
            old: rng.below(4) as u8,
            new: rng.below(4) as u8,
        }],
        81..=84 => vec![Atom::Regenerate {
            registry,
            from: token,
            to: token.next(),
        }],
        85..=92 => {
            let subregistry = rng.chance(1, 3).then_some(Pointer::Announced);
            deployed_register(registry, token, label, owner, expiry, subregistry)
        }
        93..=95 => deployed_regenerate(registry, token, owner),
        96..=97 => vec![Atom::Announce],
        _ => vec![Atom::RootEth],
    }
}

fn draw_token(rng: &mut Rng) -> Token {
    Token {
        label: rng.below(LABELS.len()),
        version: u32::from(rng.chance(1, 4)),
    }
}

fn draw_expiry(rng: &mut Rng) -> Expiry {
    match rng.below(6) {
        0 => Expiry::Past,
        1 | 2 => Expiry::Lapses(rng.below(8) as i64),
        _ => Expiry::Far,
    }
}

fn draw_actor(rng: &mut Rng) -> u8 {
    1 + rng.below(3) as u8
}

/// `register` in one call: label, mint, `TokenResource`, the owner's roles, then the pointers
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L464-L479 @ ens_v2@a971bd64).
fn deployed_register(
    registry: Registry,
    token: Token,
    label: usize,
    owner: u8,
    expiry: Expiry,
    subregistry: Option<Pointer>,
) -> Vec<Atom> {
    let mut atoms = vec![
        Atom::Register {
            registry,
            token,
            label,
            owner,
            expiry,
        },
        Atom::Mint {
            registry,
            token,
            to: owner,
        },
        Atom::Link {
            registry,
            token,
            resource: token.resource(),
        },
        Atom::Roles {
            registry,
            resource: Some(token.resource()),
            account: owner,
            old: 0,
            new: 3,
        },
    ];
    if let Some(to) = subregistry {
        atoms.push(Atom::Subregistry {
            registry,
            token,
            to,
        });
    }
    atoms
}

/// `_regenerate`: burn, `TokenRegenerated`, mint of the next version
/// (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L532-L542 @ ens_v2@a971bd64).
fn deployed_regenerate(registry: Registry, token: Token, owner: u8) -> Vec<Atom> {
    vec![
        Atom::Burn {
            registry,
            token,
            from: owner,
        },
        Atom::Regenerate {
            registry,
            from: token,
            to: token.next(),
        },
        Atom::Mint {
            registry,
            token: token.next(),
            to: owner,
        },
    ]
}

/// A registry announces itself from its constructor, so no log of it precedes its first
/// `RegistryCreated` (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L113 @ ens_v2@a971bd64).
pub fn well_formed(actions: &[Action]) -> bool {
    let mut announced = false;
    for atom in actions.iter().flat_map(|action| &action.atoms) {
        if *atom == Atom::Announce {
            announced = true;
        } else if atom.registry() == Some(Registry::Announced) && !announced {
            return false;
        }
    }
    true
}

fn announce_before_use(rng: &mut Rng, actions: &mut Vec<Action>) {
    if well_formed(actions) {
        return;
    }
    let first = actions
        .iter()
        .position(|action| {
            action
                .atoms
                .iter()
                .any(|atom| atom.registry() == Some(Registry::Announced))
        })
        .expect("an ill-formed sequence uses the announced registry");
    let at = rng.below(first + 1);
    let new_block = rng.chance(1, 2);
    actions.insert(at, action(vec![Atom::Announce], new_block, false));
}

/// Which TYR-510 shapes a sequence reaches, by a model of the adapter's token entries: whether the
/// entry holds a resource, a registration, and which label.
pub fn tyr_510_shapes(actions: &[Action]) -> BTreeSet<&'static str> {
    #[derive(Clone, Copy, Default)]
    struct Entry {
        resource: bool,
        registered: bool,
        label: Option<usize>,
    }
    let mut entries: BTreeMap<(Registry, Token), Entry> = BTreeMap::new();
    let mut shapes = BTreeSet::new();
    for atom in actions.iter().flat_map(|action| &action.atoms) {
        match *atom {
            Atom::Register {
                registry,
                token,
                label,
                ..
            } => {
                let previous = entries.get(&(registry, token)).copied();
                let mut resource = false;
                if let Some(previous) = previous
                    && previous.resource
                    && !previous.registered
                {
                    if previous.label.is_none() {
                        shapes.insert(TOKEN_RESOURCE_BEFORE_LABEL);
                        resource = true;
                    } else if previous.label == Some(label) {
                        shapes.insert(LABEL_OVER_RESERVATION);
                        resource = true;
                    }
                }
                entries.insert(
                    (registry, token),
                    Entry {
                        resource,
                        registered: true,
                        label: Some(label),
                    },
                );
            }
            Atom::Reserve {
                registry,
                token,
                label,
                ..
            } => {
                entries.insert(
                    (registry, token),
                    Entry {
                        resource: token.version == 0,
                        registered: false,
                        label: Some(label),
                    },
                );
            }
            Atom::Link {
                registry, token, ..
            } => {
                entries.entry((registry, token)).or_default().resource = true;
            }
            Atom::Unregister { registry, token } => {
                entries.remove(&(registry, token));
            }
            Atom::Regenerate { registry, from, to } => {
                if let Some(entry) = entries.remove(&(registry, from)) {
                    entries.insert((registry, to), entry);
                }
            }
            _ => {}
        }
    }
    shapes
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Layout {
    /// The blocks and transactions the generator drew.
    Drawn,
    BlockPerAction,
    OneBlock,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Split {
    BatchPerBlock,
    /// The lane's seeded split into batches of one or two blocks.
    Drawn,
}

pub const RUNS: [(Layout, Split); 5] = [
    (Layout::Drawn, Split::BatchPerBlock),
    (Layout::Drawn, Split::Drawn),
    (Layout::BlockPerAction, Split::BatchPerBlock),
    (Layout::BlockPerAction, Split::Drawn),
    (Layout::OneBlock, Split::BatchPerBlock),
];

/// Each action's block index and transaction index in `layout`.
fn place(actions: &[Action], layout: Layout) -> Vec<(usize, i64)> {
    let mut placed = Vec::with_capacity(actions.len());
    let (mut block, mut transaction) = (0_usize, 0_i64);
    for (index, action) in actions.iter().enumerate() {
        if index > 0 {
            let next_block = match layout {
                Layout::Drawn => action.new_block,
                Layout::BlockPerAction => true,
                Layout::OneBlock => false,
            };
            if next_block {
                block += 1;
                transaction = 0;
            } else if !(layout == Layout::Drawn && action.same_transaction) {
                transaction += 1;
            }
        }
        placed.push((block, transaction));
    }
    placed
}

pub fn input(wiring: &Wiring, actions: &[Action], layout: Layout) -> Result<(BatchInput, usize)> {
    let addresses = Addresses::of(wiring);
    let placed = place(actions, layout);
    let blocks = placed.last().map_or(0, |(block, _)| block + 1);
    let specs = (0..blocks)
        .map(|index| {
            let offset = index as i64;
            let number = BASE_BLOCK + offset;
            BlockSpec {
                number,
                hash: format!("0x{number:064x}"),
                timestamp: BASE_TIMESTAMP + BLOCK_SECONDS * offset,
            }
        })
        .collect::<Vec<_>>();
    let mut logs = Vec::new();
    let mut next_log = vec![0_i64; blocks];
    for (action, &(block, transaction)) in actions.iter().zip(&placed) {
        let transaction_hash = format!(
            "0x{:064x}",
            ((BASE_BLOCK + block as i64) << 16) | transaction
        );
        for atom in &action.atoms {
            for (emitter, encoded) in atom.logs(&addresses) {
                let emitted = emission(&emitter, encoded);
                logs.push(GeneratedLog {
                    block_index: block,
                    transaction_hash: transaction_hash.clone(),
                    transaction_index: transaction,
                    log_index: next_log[block],
                    emitter: emitted.emitter,
                    topics: emitted.topics,
                    data: emitted.data,
                    burst: None,
                });
                next_log[block] += 1;
            }
        }
    }
    Ok((wiring.batch_input(&specs, &logs)?, blocks))
}

/// Runs one sequence through `converge` in one layout and split. A panic is a failure too.
pub fn check(
    wiring: &Wiring,
    actions: &[Action],
    layout: Layout,
    split_kind: Split,
    seed: u64,
) -> Result<Converged> {
    let (batch_input, blocks) = input(wiring, actions, layout)?;
    let batches: Vec<Range<usize>> = match split_kind {
        Split::BatchPerBlock => (0..blocks).map(|block| block..block + 1).collect(),
        Split::Drawn => split(blocks, seed ^ SPLIT_SALT),
    };
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        converge("generated registry order", batch_input, batches)
    }));
    let converged = match outcome {
        Ok(result) => result?,
        Err(panic) => bail!("panicked: {}", panic_message(&*panic)),
    };
    let artifacts = converged.artifacts.counts();
    if !artifacts.is_empty() {
        bail!(
            "batch-boundary artifacts between the whole pass and the split replay: {artifacts:?}"
        );
    }
    Ok(converged)
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
        .unwrap_or_else(|| "a panic with no message".to_owned())
}

/// A declared registry's internally inconsistent log stops the batch by design, in every layout
/// and split alike (`crates/adapters/src/schema_v2/tests/inconsistent_log_skips.rs`). The deployed
/// registry emits no such log, so the sweep counts these runs and does not fail on them.
pub const DECLARED_STOP: &str = "declared registry stops the batch on an inconsistent log";
const INCONSISTENT_LOGS: [&str; 3] = [
    "has no retained TokenResource predecessor",
    "does not confirm its retained resource",
    "TokenResource is ambiguous",
];

/// How a run failed, precise enough that shrinking keeps the same failure.
pub fn signature(error: &anyhow::Error) -> String {
    let text = format!("{error:#}");
    if text.contains("panicked: ") {
        return "panic".to_owned();
    }
    if text.contains("fresh interpretation failed")
        && INCONSISTENT_LOGS
            .iter()
            .any(|message| text.contains(message))
    {
        return DECLARED_STOP.to_owned();
    }
    if let Some(rest) = text.split_once("first difference:\n").map(|(_, rest)| rest) {
        let field = rest
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .trim_start_matches("live")
            .trim()
            .split([':', '(', ' '])
            .next()
            .unwrap_or_default();
        return format!("live state differs from a restore at {field}");
    }
    if let Some((family, _)) = text.split_once(" is emitted twice with different guarded columns") {
        let family = family.rsplit(' ').next().unwrap_or_default();
        return format!("the writer would reject a repeated {family} row");
    }
    if text.contains("different normalized-event stream") {
        return "the split replay derives a different event stream".to_owned();
    }
    if let Some((_, rest)) =
        text.split_once("diverges between the whole-sequence pass and the split replay:")
    {
        let field = rest.split_whitespace().next().unwrap_or_default();
        return format!("an event's {field} differs between the whole pass and the split replay");
    }
    if let Some((_, rest)) =
        text.split_once("batch-boundary artifacts between the whole pass and the split replay: ")
    {
        return format!("batch-boundary artifact {rest}");
    }
    let first = text.lines().next().unwrap_or_default();
    format!("other: {}", first.chars().take(120).collect::<String>())
}

/// Whether `to` is simpler than the event's expiry: a lapse or a passed expiry becomes far. No
/// expiry is ever made shorter, so shrinking cannot add an expired event.
fn simpler_expiry(atom: Atom, to: Expiry) -> bool {
    let from = match atom {
        Atom::Register { expiry, .. }
        | Atom::Reserve { expiry, .. }
        | Atom::Renew { expiry, .. } => expiry,
        _ => return false,
    };
    matches!((from, to), (Expiry::Lapses(_) | Expiry::Past, Expiry::Far))
}

/// The same event with another expiry, for the shrinker.
fn with_expiry(atom: Atom, to: Expiry) -> Option<Atom> {
    match atom {
        Atom::Register {
            registry,
            token,
            label,
            owner,
            expiry,
        } if expiry != to => Some(Atom::Register {
            registry,
            token,
            label,
            owner,
            expiry: to,
        }),
        Atom::Reserve {
            registry,
            token,
            label,
            expiry,
        } if expiry != to => Some(Atom::Reserve {
            registry,
            token,
            label,
            expiry: to,
        }),
        Atom::Renew {
            registry,
            token,
            expiry,
        } if expiry != to => Some(Atom::Renew {
            registry,
            token,
            expiry: to,
        }),
        _ => None,
    }
}

/// Removes chunks of actions, then single events, then simplifies expiries and block breaks, while
/// the sequence still fails with the same signature.
pub fn shrink(
    wiring: &Wiring,
    actions: &[Action],
    layout: Layout,
    split_kind: Split,
    seed: u64,
    signature_kept: &str,
) -> Vec<Action> {
    let mut budget = SHRINK_BUDGET;
    let mut fails = |candidate: &[Action]| {
        if candidate.is_empty() || !well_formed(candidate) || budget == 0 {
            return false;
        }
        budget -= 1;
        check(wiring, candidate, layout, split_kind, seed)
            .err()
            .is_some_and(|error| signature(&error) == signature_kept)
    };
    let mut try_replace = |current: &mut Vec<Action>, candidate: Vec<Action>| {
        let kept = fails(&candidate);
        if kept {
            *current = candidate;
        }
        kept
    };
    let mut current = actions.to_vec();
    let mut chunk = (current.len() / 2).max(1);
    loop {
        let mut removed = false;
        let mut start = 0;
        while start < current.len() {
            let end = (start + chunk).min(current.len());
            let mut candidate = current.clone();
            candidate.drain(start..end);
            if try_replace(&mut current, candidate) {
                removed = true;
            } else {
                start = end;
            }
        }
        if !removed {
            if chunk == 1 {
                break;
            }
            chunk = (chunk / 2).max(1);
        }
    }
    let mut index = 0;
    while index < current.len() {
        let mut atom = 0;
        while current[index].atoms.len() > 1 && atom < current[index].atoms.len() {
            let mut candidate = current.clone();
            candidate[index].atoms.remove(atom);
            if !try_replace(&mut current, candidate) {
                atom += 1;
            }
        }
        index += 1;
    }
    for index in 0..current.len() {
        for atom in 0..current[index].atoms.len() {
            let drawn = current[index].atoms[atom];
            if simpler_expiry(drawn, Expiry::Far)
                && let Some(simpler) = with_expiry(drawn, Expiry::Far)
            {
                let mut candidate = current.clone();
                candidate[index].atoms[atom] = simpler;
                try_replace(&mut current, candidate);
            }
        }
        for (new_block, same_transaction) in [(false, false), (true, false)] {
            let action = &current[index];
            if (action.new_block, action.same_transaction) == (new_block, same_transaction) {
                continue;
            }
            let mut candidate = current.clone();
            candidate[index].new_block = new_block;
            candidate[index].same_transaction = same_transaction;
            if try_replace(&mut current, candidate) {
                break;
            }
        }
    }
    current
}

pub fn describe(actions: &[Action], layout: Layout) -> String {
    actions
        .iter()
        .zip(place(actions, layout))
        .map(|(action, (block, transaction))| {
            let atoms = action
                .atoms
                .iter()
                .map(|atom| format!("{atom:?}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("    block {block} tx {transaction}: {atoms}")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub struct Finding {
    pub seed: u64,
    pub layout: Layout,
    pub split: Split,
    pub signature: String,
    pub drawn: usize,
    pub shrunk: bool,
    /// The open ticket this failure kind belongs to, if it is a known one.
    pub known: Option<&'static str>,
    pub minimal: Vec<Action>,
    pub error: String,
}

#[derive(Default)]
pub struct Sweep {
    pub sequences: usize,
    pub runs: usize,
    pub findings: Vec<Finding>,
    /// Per TYR-510 shape: runs that failed, runs that passed.
    pub tyr_510_shapes: BTreeMap<&'static str, [usize; 2]>,
    /// Per named shape: sequences that reach it in any layout.
    pub shape_sequences: BTreeMap<&'static str, usize>,
    pub atoms: BTreeMap<&'static str, usize>,
    pub labelled_registries: BTreeSet<Registry>,
    pub token_under_two_labels: usize,
    pub tokens_under_one_label: usize,
    /// Named `RegistrationGranted` rows by name depth: 2 for the declared registry's names, 3 for
    /// the announced registry's.
    pub named_grants: BTreeMap<usize, usize>,
    /// Runs a declared registry's inconsistent log stopped; see [`DECLARED_STOP`].
    pub declared_stops: usize,
    pub elapsed: Duration,
}

pub fn sweep(wiring: &Wiring, base: u64, cases: u64) -> Sweep {
    let started = Instant::now();
    let mut sweep = Sweep::default();
    for case in 0..cases {
        sweep_case(wiring, &generate(case_seed(base, case)), &mut sweep);
    }
    sweep.elapsed = started.elapsed();
    sweep
}

/// Runs one sequence in every layout and split. A known failure does not stop the remaining
/// runs, so a different failure in a later layout still surfaces. An unknown failure, or a
/// declared registry's stop, ends the sequence.
pub fn sweep_case(wiring: &Wiring, generated: &Case, sweep: &mut Sweep) {
    sweep.sequences += 1;
    sweep.observe(&generated.actions);
    for shape in named_shapes(&generated.actions) {
        *sweep.shape_sequences.entry(shape).or_default() += 1;
    }
    let shapes = tyr_510_shapes(&generated.actions);
    for (layout, split_kind) in RUNS {
        sweep.runs += 1;
        let result = check(
            wiring,
            &generated.actions,
            layout,
            split_kind,
            generated.seed,
        );
        for &shape in &shapes {
            sweep.tyr_510_shapes.entry(shape).or_default()[usize::from(result.is_ok())] += 1;
        }
        match result {
            Ok(converged) => sweep.count_named(&converged),
            Err(error) => {
                let kept = signature(&error);
                if kept == DECLARED_STOP {
                    sweep.declared_stops += 1;
                    break;
                }
                let drawn_known =
                    known_failure(&kept, &format!("{error:#}"), &generated.actions, layout);
                let limit = if drawn_known.is_some() {
                    1
                } else {
                    SHRUNK_PER_KIND
                };
                let shrunk = sweep
                    .findings
                    .iter()
                    .filter(|finding| finding.shrunk && finding.signature == kept)
                    .count()
                    < limit;
                let minimal = if shrunk {
                    shrink(
                        wiring,
                        &generated.actions,
                        layout,
                        split_kind,
                        generated.seed,
                        &kept,
                    )
                } else {
                    generated.actions.clone()
                };
                let error = if shrunk {
                    check(wiring, &minimal, layout, split_kind, generated.seed)
                        .err()
                        .unwrap_or(error)
                } else {
                    error
                };
                let text = format!("{error:#}");
                // Tagged only when both the drawn and the shrunk sequence carry the shape.
                let known = drawn_known.and(known_failure(&kept, &text, &minimal, layout));
                sweep.findings.push(Finding {
                    seed: generated.seed,
                    layout,
                    split: split_kind,
                    signature: kept,
                    drawn: generated.actions.len(),
                    shrunk,
                    known,
                    minimal,
                    error: text,
                });
                if known.is_none() {
                    break;
                }
            }
        }
    }
}

impl Sweep {
    fn observe(&mut self, actions: &[Action]) {
        let mut labels_by_token: BTreeMap<(Registry, Token), BTreeSet<usize>> = BTreeMap::new();
        let mut tokens_by_label: BTreeMap<(Registry, usize), BTreeSet<Token>> = BTreeMap::new();
        for atom in actions.iter().flat_map(|action| &action.atoms) {
            *self.atoms.entry(atom.kind()).or_default() += 1;
            if let Atom::Register {
                registry,
                token,
                label,
                ..
            }
            | Atom::Reserve {
                registry,
                token,
                label,
                ..
            } = *atom
            {
                self.labelled_registries.insert(registry);
                labels_by_token
                    .entry((registry, token))
                    .or_default()
                    .insert(label);
                tokens_by_label
                    .entry((registry, label))
                    .or_default()
                    .insert(token);
            }
        }
        self.token_under_two_labels +=
            usize::from(labels_by_token.values().any(|set| set.len() > 1));
        self.tokens_under_one_label +=
            usize::from(tokens_by_label.values().any(|set| set.len() > 1));
    }

    fn count_named(&mut self, converged: &Converged) {
        for event in &converged.whole.output.normalized_events {
            if event.event_kind != "RegistrationGranted" || event.logical_name_id.is_none() {
                continue;
            }
            if let Some(labels) = event.after_state["raw_labels"].as_array() {
                *self.named_grants.entry(labels.len()).or_default() += 1;
            }
        }
    }

    /// Fails on any finding. The default corpus must also reach every event kind, both
    /// multiplicities, names under both registries, and each known shape.
    pub fn verdict(&self, default_corpus: bool) -> Result<()> {
        let new = self
            .findings
            .iter()
            .filter(|finding| finding.known.is_none())
            .count();
        if new > 0 {
            bail!(
                "{new} of {} generated registry orders failed:\n{self}",
                self.sequences
            );
        }
        if !default_corpus {
            return Ok(());
        }
        for kind in ATOM_KINDS {
            if !self.atoms.contains_key(kind) {
                bail!("the default corpus draws no {kind}:\n{self}");
            }
        }
        if self.labelled_registries.len() != 2 {
            bail!("the default corpus labels tokens in one registry only:\n{self}");
        }
        if self.token_under_two_labels == 0 || self.tokens_under_one_label == 0 {
            bail!("the default corpus misses a label multiplicity:\n{self}");
        }
        for depth in [2, 3] {
            if !self.named_grants.contains_key(&depth) {
                bail!("the default corpus names no registration at depth {depth}:\n{self}");
            }
        }
        for shape in NAMED_SHAPES {
            if !self.shape_sequences.contains_key(shape) {
                bail!("the default corpus never reaches the shape {shape:?}:\n{self}");
            }
        }
        for shape in TYR_510_SHAPES {
            if !self.tyr_510_shapes.contains_key(shape) {
                bail!("the default corpus never reaches the TYR-510 shape {shape:?}:\n{self}");
            }
        }
        Ok(())
    }
}

impl fmt::Display for Sweep {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            formatter,
            "registry_orders sequences={} runs={} elapsed_ms={} findings={}",
            self.sequences,
            self.runs,
            self.elapsed.as_millis(),
            self.findings.len()
        )?;
        writeln!(formatter, "registry_orders atoms={:?}", self.atoms)?;
        writeln!(
            formatter,
            "registry_orders labelled_registries={:?} token_under_two_labels={} tokens_under_one_label={} named_grants_by_depth={:?} declared_stops={}",
            self.labelled_registries,
            self.token_under_two_labels,
            self.tokens_under_one_label,
            self.named_grants,
            self.declared_stops
        )?;
        for shape in NAMED_SHAPES {
            let count = self.shape_sequences.get(shape).copied().unwrap_or(0);
            let zero = if count == 0 { " ZERO HITS" } else { "" };
            writeln!(
                formatter,
                "registry_orders shape {shape:?}: sequences={count}{zero}"
            )?;
        }
        for (shape, [red, green]) in &self.tyr_510_shapes {
            writeln!(
                formatter,
                "registry_orders TYR-510 shape {shape:?}: failed_runs={red} passed_runs={green}"
            )?;
        }
        for finding in &self.findings {
            let mut error = finding.error.clone();
            if error.len() > 12_000 {
                let mut end = 12_000;
                while !error.is_char_boundary(end) {
                    end -= 1;
                }
                error.truncate(end);
                error.push_str(" …");
            }
            writeln!(
                formatter,
                "\nFINDING seed={} layout={:?} split={:?} failure={:?} known={:?} drawn_actions={} shrunk={} actions={}\n{}\n  error: {error}",
                finding.seed,
                finding.layout,
                finding.split,
                finding.signature,
                finding.known,
                finding.drawn,
                finding.shrunk,
                finding.minimal.len(),
                describe(&finding.minimal, finding.layout),
            )?;
        }
        Ok(())
    }
}
