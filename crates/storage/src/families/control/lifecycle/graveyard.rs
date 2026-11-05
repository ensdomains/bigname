//! A name whose current ENSv1 registry record the admitted Graveyard holds: its ENSv1
//! registration has ended, whatever its lease, binding or schedule says.
//!
//! The Graveyard is the ENSv1 `.eth` controller the ENSv2 launch leaves in place. Anyone may call
//! its `clear`. For a `.eth` second-level name it registers the name to itself through the
//! BaseRegistrar, which only succeeds once the lease is past its expiry and grace, with a
//! far-future expiry that is no lease of anyone's. That takes the registry record and burns the
//! lapsed token. A subname of a name it holds is cleared by giving the Graveyard its record. No
//! ENSv1 controller can register the name again, and the Graveyard never transfers what it holds.
//! So the name is served `released` with no expiry (`expires_at_reason: released`), no owner,
//! manager or registrant, released at the Graveyard's registry write that began its control, and
//! with the holder who controlled the name before it as `lapsed_registration.owner`
//! (docs/upstream.md).
//!
//! Who controlled the name, and when the Graveyard's control began, come from one pass over the
//! name's custody events in chain order ([`fold`]). The pass keeps constant state, so its cost is
//! one sort and one walk of the name's registry writes, NameWrapper transfers and BaseRegistrar
//! rows, and its memory is those events.
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L98-L102 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L186-L201 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L104 @ ens_v1@91c966f)
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value, json};

use super::{NameFacts, NamePlace};
use crate::families::control::{
    position::Position,
    registry::{OwnerEvent, ZERO_ADDRESS},
    rows::LifecycleEvent,
};

/// The admitted Graveyard's registry write that began its control of the name's current ENSv1
/// record, when it still holds it. An ENSv2 name has its own registry, so a label registered in
/// ENSv2 after the claim serves that registration instead. A migrated name's record is the
/// Graveyard's from its migration, which `clear` never claims, so that write is no claim even
/// where the name falls back to its ENSv1 arm.
fn claim(facts: &NameFacts) -> Option<Fold<'_>> {
    if facts.input.selection.authority_arm.as_deref() != Some("ens_v1") || facts.migration.is_some()
    {
        return None;
    }
    fold(facts).filter(|fold| fold.claim.is_some())
}

/// Serve a Graveyard-held name's registration as ended, with the holder [`fold`] names as the
/// lapsed owner.
pub(super) fn apply(
    facts: &NameFacts,
    registration: &mut Map<String, Value>,
    control: &mut Map<String, Value>,
) {
    let Some(Fold {
        claim: Some(write),
        holder,
        ..
    }) = claim(facts)
    else {
        return;
    };
    let released_at = facts
        .block_seconds
        .get(&write.position.block_number)
        .map_or(Value::Null, |seconds| json!(seconds));
    for field in [
        "expiry",
        "ens_v1_expiry",
        "grace_ends_at",
        "registrant",
        "lifecycle_recompose_at",
    ] {
        registration.insert(field.into(), Value::Null);
    }
    registration.insert("status".into(), json!("released"));
    registration.insert("lifecycle_status".into(), json!("released"));
    registration.insert("expires_at_reason".into(), json!("released"));
    registration.insert("released_at".into(), released_at.clone());
    registration.insert(
        "lapsed_registration".into(),
        json!({"owner": holder, "released_at": released_at, "release_kind": "expired"}),
    );
    for field in ["owner", "registry_owner", "registrant", "expiry"] {
        if let Some(value) = control.get_mut(field) {
            *value = Value::Null;
        }
    }
}

/// One custody event of the name.
enum Custody<'a> {
    /// A current-registry owner write of the node.
    Write(&'a OwnerEvent),
    /// A NameWrapper wrap: the mint of a new wrapped token to its recipient.
    Mint(&'a LifecycleEvent),
    /// A NameWrapper transfer of a live wrapped token.
    Transfer(&'a LifecycleEvent),
    /// A BaseRegistrar grant or transfer of a `.eth` second-level name's token.
    Lease(&'a LifecycleEvent),
}

impl Custody<'_> {
    fn position(&self) -> &Position {
        match self {
            Self::Write(event) => &event.position,
            Self::Mint(event) | Self::Transfer(event) | Self::Lease(event) => &event.position,
        }
    }
}

/// The name's custody events in chain order (block, transaction, log), and its NameWrappers'
/// addresses.
///
/// The NameWrapper's transfers sit on the resource of the wrap they move: each wrap mints a new
/// resource with its own NameWrapper candidate (adapters `wrapper.rs`, `authority_key`), so a
/// resource's first transfer is its mint. The adapter records no burn and no transfer of a token
/// whose NameWrapper authority a registry write ended (adapters `wrapper/transfer.rs`,
/// `state.rs` `transfer_v1_wrapper_owner`), and the BaseRegistrar adapter records no transfer to
/// or from zero (adapters `registrar/token_transfer.rs`).
fn custody_events(facts: &NameFacts) -> Option<Custodies<'_>> {
    let node = facts.registry_node.as_ref()?;
    let registry = node.latest_transfer()?.source_family.as_str();
    let mut wrappers = BTreeSet::new();
    let mut name_wrappers = BTreeSet::new();
    for candidate in &facts.candidates {
        if candidate.authority_kind.as_deref() == Some("wrapper")
            && let Some(address) = candidate.emitting_address.as_deref()
        {
            wrappers.insert(candidate.resource_id.as_str());
            name_wrappers.insert(address.to_ascii_lowercase());
        }
    }
    let mut events: Vec<Custody<'_>> = node
        .owner_events
        .iter()
        .filter(|event| {
            event.event_kind == "AuthorityTransferred" && event.source_family == registry
        })
        .map(|event| (&event.position, event))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .map(Custody::Write)
        .collect();
    let mut wrapped: Vec<&LifecycleEvent> = facts
        .events
        .iter()
        .filter(|event| {
            event.source_family == "ens_v1_wrapper_l1"
                && event.event_kind == "TokenControlTransferred"
                && event.to_address.is_some()
                && event
                    .resource_id
                    .as_deref()
                    .is_some_and(|resource| wrappers.contains(resource))
        })
        .collect();
    wrapped.sort_by(|left, right| left.position.cmp(&right.position));
    let mut minted = BTreeSet::new();
    events.extend(wrapped.into_iter().map(|event| {
        if minted.insert(event.resource_id.as_deref()) {
            Custody::Mint(event)
        } else {
            Custody::Transfer(event)
        }
    }));
    if facts.input.place == NamePlace::EthSecondLevel {
        events.extend(
            facts
                .events
                .iter()
                .filter(|event| {
                    event.source_family == "ens_v1_registrar_l1"
                        && lease_recipient(event).is_some()
                        && super::staged_as_own(facts, event)
                })
                .map(Custody::Lease),
        );
    }
    events.sort_by(|left, right| left.position().cmp(right.position()));
    let owner_logs = node
        .owner_events
        .iter()
        .filter(|event| event.event_kind == "SubregistryChanged" && event.source_family == registry)
        .map(|event| place(&event.position))
        .collect();
    Some(Custodies {
        events,
        name_wrappers,
        owner_logs,
    })
}

/// A name's custody events, its NameWrappers' addresses, and the place of every registry
/// `NewOwner` of its node, which the adapter records as a `SubregistryChanged` even when the
/// owner it names is unchanged (adapters `registry.rs`).
struct Custodies<'a> {
    events: Vec<Custody<'a>>,
    name_wrappers: BTreeSet<String>,
    owner_logs: BTreeSet<(i64, Option<i64>, Option<i64>)>,
}

/// A position's block, transaction and log.
fn place(position: &Position) -> (i64, Option<i64>, Option<i64>) {
    (
        position.block_number,
        position.transaction_index,
        position.log_index,
    )
}

/// A BaseRegistrar row's recipient: a grant's registrant or a transfer's recipient.
fn lease_recipient(event: &LifecycleEvent) -> Option<&str> {
    match event.event_kind.as_str() {
        "RegistrationGranted" => event.registrant.as_deref(),
        "TokenControlTransferred" => event.to_address.as_deref(),
        _ => None,
    }
}

/// The node's registry owner.
#[derive(Clone, PartialEq)]
enum Record {
    Graveyard,
    NameWrapper,
    Owner(String),
    /// No write yet, the zero address, or an owner word no getter reports.
    Nobody,
}

/// Who controls the name: the Graveyard, an address, or no one (`None`).
#[derive(Clone, Debug, PartialEq)]
enum Controller {
    Graveyard,
    Holder(String),
}

/// The run of Graveyard control the fold is in.
struct Stretch<'a> {
    /// The controller just before the Graveyard took control.
    holder: Option<String>,
    /// The lease's holder at that moment.
    lease: Option<String>,
    /// The first Graveyard registry write of the run.
    claim: Option<&'a OwnerEvent>,
}

/// What one pass over the name's custody events found.
struct Fold<'a> {
    /// The Graveyard's registry write that began its current control, when its record names it.
    claim: Option<&'a OwnerEvent>,
    holder: Option<String>,
    #[cfg(test)]
    trace: Vec<(Position, Option<String>)>,
}

/// The fold of the name's custody events.
///
/// The record's controller follows from the registry owner `R` and the NameWrapper's current
/// wrap of the node:
/// - `R` is the Graveyard: the Graveyard.
/// - `R` is the NameWrapper with a live wrap: the wrapped token's holder, the Graveyard when it
///   holds the token.
/// - `R` is the NameWrapper with no live wrap (a bare write, or a wrap a write to someone else
///   ended): no one.
/// - `R` is zero: no one. Otherwise `R`.
///
/// A registry write naming anyone but the NameWrapper ends the wrap (NameWrapper `_isWrapped`).
/// A write naming the NameWrapper keeps a live wrap (`setRecord`, the wrapped branch of
/// `setSubnodeRecord`). A write naming the NameWrapper with no live wrap is one wrap step with the
/// mint that follows it in the same transaction, with only BaseRegistrar rows between: every
/// mint path writes the record to the NameWrapper first (`wrap`, `setSubnodeOwner`,
/// `setSubnodeRecord`, `wrapETH2LD`, `onERC721Received`, `registerAndWrapETH2LD`), and no other
/// registry owner log of the node sits between that write and its mint. The adapter records no
/// write that names the record's current owner (adapters `registry.rs`), so a mint on a record
/// the NameWrapper already owns comes with no write of its own. A registry `NewOwner` still leaves
/// a `SubregistryChanged` row then. So the write and the mint are one step only when no
/// `SubregistryChanged` of the node lies strictly between them: one there is the mint's own
/// `NewOwner`, and the earlier write is bare. A mint makes its recipient the wrap's holder, replacing any older token (`_mint`), and
/// a transfer moves it. The BaseRegistrar token controls nothing until a `reclaim`, which is a
/// registry write.
///
/// The Graveyard's control runs while the controller stays the Graveyard, across a repeated
/// write to it, a BaseRegistrar move, a wrap minted to it and a transfer to it. Any other
/// controller ends the run, even inside one transaction. The claim is the run's first Graveyard
/// registry write. A wrapped token sent to the Graveyard begins a run that no write has claimed
/// yet. The lapsed holder is the controller just before the run. When no one controlled the name
/// then, there is none, whatever the lease says. Otherwise, for a `.eth` second-level name, it is
/// the lease's holder at that moment, the holder the name served before: the BaseRegistrar
/// token's holder, or, while the NameWrapper holds that token, the live wrapped token's holder
/// unless it is the Graveyard, else whoever put the token into the NameWrapper. A wrap moves the
/// token into custody and does not change who holds the lease (`wrapETH2LD` takes it from its
/// holder).
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L84 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L279 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L303 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L415 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L630 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L778-L811 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L892 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L927-L948 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076-L1080 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L228-L306 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
fn fold(facts: &NameFacts) -> Option<Fold<'_>> {
    let Custodies {
        events,
        name_wrappers,
        owner_logs,
    } = custody_events(facts)?;
    let graveyard = events.iter().rev().find_map(|event| match event {
        Custody::Write(write) if write.graveyard_held() => write.owner_getter.as_deref(),
        _ => None,
    });
    let names = |address: &str, other: Option<&str>| {
        other.is_some_and(|other| other.eq_ignore_ascii_case(address))
    };
    let mut record = Record::Nobody;
    let mut wrap: Option<String> = None;
    let mut lease: Option<String> = None;
    let mut lease_wrapped = false;
    let mut stretch: Option<Stretch<'_>> = None;
    let mut consumed = None;
    #[cfg(test)]
    let mut trace = Vec::new();
    for (index, event) in events.iter().enumerate() {
        if consumed == Some(index) {
            continue;
        }
        let controller = |record: &Record, wrap: &Option<String>| match record {
            Record::Graveyard => Some(Controller::Graveyard),
            Record::NameWrapper => wrap.as_ref().map(|holder| {
                if names(holder.as_str(), graveyard) {
                    Controller::Graveyard
                } else {
                    Controller::Holder(holder.clone())
                }
            }),
            Record::Owner(owner) => Some(Controller::Holder(owner.clone())),
            Record::Nobody => None,
        };
        let before = controller(&record, &wrap);
        let lease_before = wrap
            .clone()
            .filter(|holder| lease_wrapped && !names(holder.as_str(), graveyard))
            .or_else(|| lease.clone());
        let mut graveyard_write = None;
        match event {
            Custody::Write(write) => {
                record = if write.graveyard_held() {
                    graveyard_write = Some(*write);
                    Record::Graveyard
                } else {
                    match write.reported_owner() {
                        Some(owner) if name_wrappers.contains(&owner.to_ascii_lowercase()) => {
                            Record::NameWrapper
                        }
                        Some(owner) if !owner.eq_ignore_ascii_case(ZERO_ADDRESS) => {
                            Record::Owner(owner)
                        }
                        _ => Record::Nobody,
                    }
                };
                if record != Record::NameWrapper {
                    // An unwrap with the BaseRegistrar token still in the NameWrapper: the lease
                    // is the wrapped token's last holder's until the token comes out.
                    if lease_wrapped
                        && let Some(holder) = wrap.take()
                        && !names(holder.as_str(), graveyard)
                    {
                        lease = Some(holder);
                    }
                    wrap = None;
                } else if wrap.is_none() {
                    let transaction =
                        |position: &Position| (position.block_number, position.transaction_index);
                    let mint = events[index + 1..]
                        .iter()
                        .enumerate()
                        .take_while(|(_, next)| {
                            transaction(next.position()) == transaction(&write.position)
                        })
                        .find(|(_, next)| !matches!(next, Custody::Lease(_)))
                        .and_then(|(offset, next)| match next {
                            Custody::Mint(mint) => Some((index + 1 + offset, mint)),
                            _ => None,
                        })
                        .filter(|(_, mint)| {
                            !owner_logs.iter().any(|&(block, tx, log)| {
                                (block, tx) == transaction(&write.position)
                                    && log > write.position.log_index
                                    && log < mint.position.log_index
                            })
                        });
                    if let Some((at, mint)) = mint {
                        consumed = Some(at);
                        wrap = mint.to_address.clone();
                    }
                }
            }
            Custody::Mint(mint) => wrap = mint.to_address.clone(),
            Custody::Transfer(transfer) => {
                if wrap.is_some() {
                    wrap = transfer.to_address.clone();
                }
            }
            Custody::Lease(row) => {
                if let Some(holder) = lease_recipient(row).filter(|&holder| {
                    !holder.eq_ignore_ascii_case(ZERO_ADDRESS) && !names(holder, graveyard)
                }) {
                    lease_wrapped = name_wrappers.contains(&holder.to_ascii_lowercase());
                    if !lease_wrapped {
                        lease = Some(holder.to_owned());
                    } else if row.event_kind == "RegistrationGranted" {
                        lease = None;
                    }
                }
            }
        }
        let after = controller(&record, &wrap);
        if after == Some(Controller::Graveyard) {
            let run = stretch.get_or_insert_with(|| Stretch {
                holder: match &before {
                    Some(Controller::Holder(holder)) => Some(holder.clone()),
                    _ => None,
                },
                lease: lease_before,
                claim: None,
            });
            if run.claim.is_none() {
                run.claim = graveyard_write;
            }
        } else {
            stretch = None;
        }
        #[cfg(test)]
        trace.push((
            event.position().clone(),
            after.map(|controller| match controller {
                Controller::Graveyard => "graveyard".to_owned(),
                Controller::Holder(holder) => holder,
            }),
        ));
    }
    let run = stretch.filter(|_| record == Record::Graveyard);
    Some(Fold {
        claim: run.as_ref().and_then(|run| run.claim),
        holder: run.and_then(|run| {
            let holder = run.holder?;
            Some(run.lease.unwrap_or(holder))
        }),
        #[cfg(test)]
        trace,
    })
}

/// The controller after each custody step of the name, the Graveyard as `graveyard` and no one
/// as `None`. A write naming the NameWrapper and the mint it pairs with are one step, at the
/// write.
#[cfg(test)]
pub(super) fn trace(facts: &NameFacts) -> Vec<(Position, Option<String>)> {
    fold(facts).map(|fold| fold.trace).unwrap_or_default()
}

#[cfg(test)]
#[path = "graveyard_tests.rs"]
mod tests;
