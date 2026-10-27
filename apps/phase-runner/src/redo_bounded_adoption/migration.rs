//! The `ens_v2_migration_l1` manifest changes the redo can bound (docs/deployment.md §
//! Manifest-authority marker): an address added under a role, or a singleton role moved to
//! another address. Every reader of the family's declarations reads one at or above its start, or
//! where the declared address already existed on chain: the controller and Graveyard lookups of
//! `crates/adapters/src/schema_v2/migration.rs`, the factory origins of
//! `crates/adapters/src/schema_v2/migration/registry_origin.rs` (`supported_origin`) and the
//! Project reader of `crates/storage/src/families/control/permissions/registry_support.rs`
//! (`load`). A redo from the lowest start on either side of such a change therefore recomposes
//! every row the change can alter. The controller lookup reads a controller as a factory log's
//! indexed `sender`, which no first-effect arm can find, so it rests on the declared start being
//! the deployment block. `start_refusal` refuses the bound where a log shows otherwise.
use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use sqlx::{Postgres, Transaction};

use crate::{
    error::{RunnerError, RunnerResult},
    redo_first_effect::{EMITTED_LOG, READABLE, Side, start_block},
};

pub(crate) const MIGRATION_FAMILY: &str = "ens_v2_migration_l1";

/// Payload fields compared apart from the rest: the declarations, and the watch plan compiled
/// from them.
const CONTRACTS: &str = "contracts";
const COMPILED_WATCH: &str = "_bigname_compiled_watch";

/// A declaration that an address-only change adds, moves away from or moves to.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Changed {
    pub(crate) role: String,
    pub(crate) address: String,
    pub(crate) start: i64,
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
/// address that emitted a retained log below the declared start: the start is then not the
/// contract's deployment block, and a factory log naming it as `sender` below that start would
/// lie below the bound. Each address walks its own logs, as the first-effect scan's emitted-log
/// arm does.
pub(crate) async fn start_refusal(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    changed: &[(i64, Changed)],
    through: i64,
) -> RunnerResult<Option<String>> {
    let statement = EMITTED_LOG.replace("{READABLE}", READABLE);
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
    }
    Ok(None)
}

#[cfg(test)]
mod tests;
