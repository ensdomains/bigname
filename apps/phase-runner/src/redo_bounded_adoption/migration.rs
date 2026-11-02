//! The `ens_v2_migration_l1` manifest changes the redo can bound (docs/deployment.md §
//! Manifest-authority marker): an address added under a role, or a singleton role moved to
//! another address. Every reader of the family's declarations reads one at or above its start, or
//! where the declared address already existed on chain: the controller and Graveyard lookups of
//! `crates/adapters/src/schema_v2/migration.rs`, the factory origins of
//! `crates/adapters/src/schema_v2/migration/registry_origin.rs` (`supported_origin`) and the
//! Project reader of `crates/storage/src/families/control/permissions/registry_support.rs`
//! (`load`). A redo from the lowest start on either side of such a change therefore recomposes
//! every row the change can alter. The controller lookups read a controller as the indexed
//! `sender` of a factory or registry log, which no first-effect arm can find, so they rest on the
//! declared start being the deployment block. `start_refusal` refuses the bound where a log shows
//! otherwise: a log the address emitted, or a factory or registry log naming it as `sender`.
use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use sqlx::{Postgres, Transaction};

use crate::{
    error::{RunnerError, RunnerResult},
    redo_first_effect::{EMITTED_LOG, READABLE, Side, start_block},
};
use bigname_manifests::ManifestAbiEvent;

pub(crate) const MIGRATION_FAMILY: &str = "ens_v2_migration_l1";

/// Payload fields compared apart from the rest: the declarations, and the watch plan compiled
/// from them.
const CONTRACTS: &str = "contracts";
const COMPILED_WATCH: &str = "_bigname_compiled_watch";

/// The roles a factory `ProxyDeployed` log names as its indexed `sender`, read in
/// `crates/adapters/src/schema_v2/migration/registry.rs` (`correlate_registry_creation`,
/// `proxy_sender`). Such a role emits none of the family's events, so only the factory's logs can
/// show that it acted before its declared start.
const FACTORY_SENDER_ROLES: [&str; 1] = ["locked_migration_controller"];
const FACTORY_EVENT: &str = "ProxyDeployed";

/// The roles a registry `LabelRegistered` names as its indexed `sender`. The migration adapter
/// compares the `sender` of a same-transaction `RegistrationGranted`, built from that log, with
/// both controllers by address alone (`crates/adapters/src/schema_v2/migration.rs`,
/// `correlate_authority_transitions`), and its launch gate is the Graveyard's start, below a
/// redeployed controller's. The emitters are the registry family's own, read from its current
/// manifest.
const REGISTRY_SENDER_ROLES: [&str; 2] = [
    "unlocked_migration_controller",
    "locked_migration_controller",
];
const REGISTRY_FAMILY: &str = "ens_v2_registry_l1";
const REGISTRY_EVENT: &str = "LabelRegistered";
const SENDER: &str = "sender";

/// The first canonical log of emitter `$2` at or below `$3` whose topic at array position `$5`
/// is `$6`. It walks `raw_logs_emitter_idx` over that emitter's own logs, with the topics as
/// filters, as the first-effect scan's watched-address arm does.
const SENDER_LOG: &str = "SELECT log.block_number FROM raw_logs log
    JOIN chain_lineage lineage ON lineage.chain_id = log.chain_id
     AND lineage.block_hash = log.block_hash AND lineage.block_number = log.block_number
    WHERE log.chain_id = $1 AND lower(log.emitting_address) = $2 AND log.block_number <= $3
      AND lower(log.topics[1]) = $4 AND lower(log.topics[$5]) = $6 AND {READABLE}
    ORDER BY log.block_number LIMIT 1";

/// The sender statement as `start_refusal` sends it, so a test can pin its plan.
#[doc(hidden)]
pub fn sender_statement() -> String {
    SENDER_LOG.replace("{READABLE}", READABLE)
}

/// Where a changed address shows up as a log's `sender`: the declared emitters of the event, its
/// name and topic0, and the PostgreSQL array position of the `sender` topic in `raw_logs.topics`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SenderProbe {
    pub(crate) emitters: Vec<String>,
    pub(crate) event: String,
    pub(crate) topic0: String,
    pub(crate) position: i32,
}

/// A declaration that an address-only change adds, moves away from or moves to.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Changed {
    pub(crate) role: String,
    pub(crate) address: String,
    pub(crate) start: i64,
    /// Set for a role the factory names as `sender`.
    pub(crate) sender: Option<SenderProbe>,
}

/// Whether a migration manifest's change from `old` to `new` only adds an address under a role,
/// or moves a singleton role from one address to another. `Ok` holds every declaration the change
/// adds or moves, on both sides. `Err` says why the redo stays the full range. Removing a
/// declaration and editing one in place stay full by policy: the first-effect arithmetic would
/// bound them too, but nothing needs it yet.
pub(crate) fn migration_change_qualifies(
    old: Option<&Side<'_>>,
    new: Option<&Side<'_>>,
) -> Result<Vec<Changed>, String> {
    let (Some(old), Some(new)) = (old, new) else {
        return Err("the manifest is new or retired".to_owned());
    };
    if old.rollout_status != Some("active") || new.rollout_status != Some("active") {
        return Err("the manifest is not active on both sides".to_owned());
    }
    let fields = |side: &Side<'_>| -> BTreeSet<String> {
        side.payload
            .as_object()
            .into_iter()
            .flat_map(|fields| fields.keys().cloned())
            .collect()
    };
    if let Some(field) = fields(old)
        .union(&fields(new))
        .filter(|field| *field != CONTRACTS && *field != COMPILED_WATCH)
        .find(|field| old.payload[field.as_str()] != new.payload[field.as_str()])
    {
        return Err(format!("it changes `{field}`"));
    }
    let emitters: BTreeSet<&str> = new.payload["abi"]["events"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|event| event["emitter_roles"].as_array().into_iter().flatten())
        .filter_map(Value::as_str)
        .collect();
    let (before, after) = (by_role(old), by_role(new));
    let mut changed = Vec::new();
    let mut probe = None;
    for role in before.keys().chain(after.keys()).collect::<BTreeSet<_>>() {
        let (was, now) = (
            before.get(role).map_or(&[][..], Vec::as_slice),
            after.get(role).map_or(&[][..], Vec::as_slice),
        );
        if was.len() == now.len() && was.iter().all(|entry| now.contains(entry)) {
            continue;
        }
        if emitters.contains(role) {
            return Err(format!(
                "it changes the {role} declaration, whose address emits the family's events"
            ));
        }
        let entries: Vec<&Value> = match (was, now) {
            (_, []) => return Err(format!("it removes the {role} declaration")),
            ([], added) => added.to_vec(),
            ([prior], [next]) if address(prior) == address(next) => {
                return Err(format!(
                    "it edits the {role} declaration of {} in place",
                    address(prior).unwrap_or_default()
                ));
            }
            ([prior], [next]) => vec![*prior, *next],
            (was, now) if now.len() > was.len() && was.iter().all(|entry| now.contains(entry)) => {
                now.iter()
                    .filter(|entry| !was.contains(entry))
                    .copied()
                    .collect()
            }
            _ => {
                return Err(format!(
                    "it changes the {role} declarations other than by one added or moved address"
                ));
            }
        };
        let sender = if FACTORY_SENDER_ROLES.contains(role) {
            if probe.is_none() {
                probe = Some(sender_probe(new.payload, FACTORY_EVENT)?);
            }
            probe.clone().flatten()
        } else {
            None
        };
        for entry in entries {
            let address = address(entry).unwrap_or_default();
            if entry["proxy_kind"].as_str() != Some("none") || !entry["implementation"].is_null() {
                return Err(format!(
                    "the {role} declaration of {address} is a proxy, whose implementation the \
                     bounded redo does not follow"
                ));
            }
            changed.push(Changed {
                role: (*role).to_owned(),
                address,
                start: start_block(&entry["start_block"]),
                sender: sender.clone(),
            });
        }
    }
    let moved: BTreeSet<&str> = changed.iter().map(|entry| entry.address.as_str()).collect();
    if other_watches(old, &moved) != other_watches(new, &moved) {
        return Err(
            "its compiled watch plan changes beyond the added and moved addresses".to_owned(),
        );
    }
    Ok(changed)
}

/// How to find the logs naming a sender role: the payload's `[[abi.events]]` entry `name`, the
/// declared contracts of its emitter roles and the position of its indexed `sender`. `None` when
/// the payload declares no such event or no emitter, so no such log is interpreted. A fragment
/// that does not index `sender` leaves nothing to probe, so the change keeps the full range.
fn sender_probe(payload: &Value, name: &str) -> Result<Option<SenderProbe>, String> {
    let Some(event) = payload["abi"]["events"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|event| event["name"] == name)
    else {
        return Ok(None);
    };
    let roles: Vec<&str> = event["emitter_roles"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let emitters: Vec<String> = payload[CONTRACTS]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| {
            entry["role"]
                .as_str()
                .is_some_and(|role| roles.contains(&role))
        })
        .filter_map(address)
        .collect();
    if emitters.is_empty() {
        return Ok(None);
    }
    let unreadable = || format!("its {name} fragment does not index `{SENDER}`");
    let event: ManifestAbiEvent =
        serde_json::from_value(event.clone()).map_err(|_| unreadable())?;
    let parsed = event.parsed_event().map_err(|_| unreadable())?;
    let topic0 = event
        .topic0()
        .ok()
        .flatten()
        .ok_or_else(unreadable)?
        .to_ascii_lowercase();
    let indexed: Vec<&str> = parsed
        .inputs
        .iter()
        .filter(|input| input.indexed)
        .map(|input| input.name.as_str())
        .collect();
    let at = indexed
        .iter()
        .position(|name| *name == SENDER)
        .ok_or_else(unreadable)?;
    Ok(Some(SenderProbe {
        emitters,
        event: name.to_owned(),
        topic0,
        position: i32::try_from(at + 2).map_err(|_| unreadable())?,
    }))
}

/// The `contracts` entries of a payload by role.
fn by_role<'a>(side: &Side<'a>) -> BTreeMap<&'a str, Vec<&'a Value>> {
    let mut roles: BTreeMap<&str, Vec<&Value>> = BTreeMap::new();
    for entry in side.payload[CONTRACTS].as_array().into_iter().flatten() {
        roles
            .entry(entry["role"].as_str().unwrap_or_default())
            .or_default()
            .push(entry);
    }
    roles
}

fn address(entry: &Value) -> Option<String> {
    entry["address"].as_str().map(str::to_ascii_lowercase)
}

/// The compiled watch entries of a payload other than the address watches of `addresses`, which
/// every declared contract of the family compiles for each of its events
/// (`crates/manifests/src/schema_v2_watch/compile.rs`).
fn other_watches(side: &Side<'_>, addresses: &BTreeSet<&str>) -> BTreeSet<String> {
    side.payload[COMPILED_WATCH]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| {
            let emitter = &entry["emitter"];
            emitter["kind"] != "address"
                || !emitter["address"].as_str().is_some_and(|address| {
                    addresses.contains(address.to_ascii_lowercase().as_str())
                })
        })
        .map(Value::to_string)
        .collect()
}

/// Why the bound of a qualifying change is refused, when a declaration it adds or moves names an
/// address with a retained log below the declared start: one it emitted, or, for a role a factory
/// or registry log names as `sender`, such a log naming it. The start is then not the contract's
/// deployment block, and a log naming it below that start could lie below the bound. Each
/// address walks its own logs, as the first-effect scan's emitted-log arm does, and each sender
/// probe walks each of its emitters'. The registry probe reads the registry family's manifests in
/// `current`, the set the redo adopts. A registry change refuses the bound before this, by the
/// whole-redo rule.
///
/// The registry probe also covers `crates/adapters/src/schema_v2/migration/unwrapped_reconcile.rs`,
/// which reads both controllers and the Graveyard with no block. The line references here are
/// v0.14.x's. It reads them at :73-75. It cannot act on a transaction without a
/// `LabelRegistered`: it returns early without a v1 registrar `Transfer` (:80-93), and everything
/// it proves comes from the probe's authority transitions (:95-128, :183-195).
/// `correlate_authority_transitions` (`migration.rs`:318-420) builds the same-transaction
/// `RegistrationGranted` rows from `LabelRegistered` (:331-350), pushes a transition only inside
/// its loop over them (:353), and compares the addresses only there: the Graveyard with the
/// `Transfer`'s `to` (:373-375), the controllers with the registration's `sender` (:379, :385,
/// :395). The unlocked controller reaches `prove` at `unwrapped_reconcile.rs`:202, as its
/// `controller` (:221). So for registrations by the declared registry these sites no longer rest
/// on the test that each declared start is the deployment receipt's block. A registry admitted
/// through a `registry_announcement` edge also yields a `RegistrationGranted`
/// (`catalog.rs`:466-470, `protocol/v2_registry.rs`:94, :390), and the correlation does not filter
/// by emitter, but the probe reads only the declared registry.
///
/// TYR-385 is to bring block-aware controller comparisons (`Controllers::at(catalog, block)`) to
/// main as the structural fix, so that the bounded pair equals the full pair by construction
/// there. This guard is defence for this release line. It forward-merges harmlessly, because it
/// can only refuse a bound, never pass a wrong one.
pub(crate) async fn start_refusal(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    changed: &[(i64, Changed)],
    current: &[super::EventRow],
    through: i64,
) -> RunnerResult<Option<String>> {
    let mut registries = Vec::new();
    if changed
        .iter()
        .any(|(_, declaration)| REGISTRY_SENDER_ROLES.contains(&declaration.role.as_str()))
    {
        for row in current
            .iter()
            .filter(|row| row.2 == chain_id && row.4 == REGISTRY_FAMILY)
        {
            let Some(side) = super::side(Some(row)) else {
                continue;
            };
            if side.rollout_status != Some("active") {
                continue;
            }
            match sender_probe(side.payload, REGISTRY_EVENT) {
                Ok(probe) => registries.extend(probe),
                Err(reason) => {
                    return Ok(Some(format!(
                        "manifest {} of family {REGISTRY_FAMILY} cannot show where a changed \
                         migration controller registered names: {reason}",
                        row.0
                    )));
                }
            }
        }
    }
    let statement = EMITTED_LOG.replace("{READABLE}", READABLE);
    let sender_log = sender_statement();
    for (manifest_id, declaration) in changed {
        let below = (declaration.start - 1).min(through);
        if below < 0 {
            continue;
        }
        let logged: Option<i64> = sqlx::query_scalar(&statement)
            .bind(chain_id)
            .bind(vec![declaration.address.clone()])
            .bind(below)
            .fetch_one(&mut **transaction)
            .await
            .map_err(|error| {
                RunnerError::database(
                    "failed to read a changed migration address's logs below its start",
                    error,
                )
            })?;
        if let Some(block) = logged {
            return Ok(Some(format!(
                "manifest {manifest_id} of family {MIGRATION_FAMILY} declares {} {} from block \
                 {}, but that address emitted a log at block {block}, so the declared start is \
                 not its deployment block",
                declaration.role, declaration.address, declaration.start
            )));
        }
        let controller = REGISTRY_SENDER_ROLES.contains(&declaration.role.as_str());
        let registry: &[SenderProbe] = if controller { &registries } else { &[] };
        let topic = format!("0x{:0>64}", declaration.address.trim_start_matches("0x"));
        for probe in declaration.sender.iter().chain(registry) {
            for emitter in &probe.emitters {
                let named: Option<i64> = sqlx::query_scalar(&sender_log)
                    .bind(chain_id)
                    .bind(emitter)
                    .bind(below)
                    .bind(&probe.topic0)
                    .bind(probe.position)
                    .bind(&topic)
                    .fetch_optional(&mut **transaction)
                    .await
                    .map_err(|error| {
                        RunnerError::database(
                            "failed to read the logs naming a changed migration address",
                            error,
                        )
                    })?;
                if let Some(block) = named {
                    return Ok(Some(format!(
                        "manifest {manifest_id} of family {MIGRATION_FAMILY} declares {} {} from \
                         block {}, but the {} log of {emitter} at block {block} names that \
                         address as `{SENDER}`, so the declared start is not its deployment block",
                        declaration.role, declaration.address, declaration.start, probe.event
                    )));
                }
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests;
