//! F1 name history (`project_name_history`): the whole-history facts of a name that no other
//! family keeps, each fixed by the first event that establishes it. The row is written once
//! per name, at the first block whose events name it, and changes afterwards only when a later
//! event establishes a fact the row does not hold yet:
//!
//! - the block and block time of the first event naming the name, which the served name row
//!   reports as `registration.created_at` (name_current/build.sql, the `created` lateral: the
//!   earliest readable event of the name, of any kind, by block);
//! - whether any event naming the name comes from the ENSv2 registry families (the `corpus`
//!   lateral), which decides the coverage the row reports when no authority arm is selected;
//! - the authority arms of the name's authority events (name_authority/build.sql,
//!   `event_arms`), which decide the arm of a name with no open binding. The ENSv2 root and
//!   registry `ExpiryChanged` never vote, and their `RegistrationReleased` votes only beside a
//!   matching ENSv2 binding at or before it; that release is a retained lifecycle event, so the
//!   reader applies that rule and the row leaves those releases out.
//!
//! An undo removes or restores the row with its block, as for every journalled family.
use std::collections::BTreeSet;

use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::super::{
    input::BlockEvent,
    reduce::{Context, Preload, current, key_of, load_rows, put, set},
    store::{Row, RowSet},
    tables,
};
use crate::Result;

const V2_FAMILIES: [&str; 3] = [
    "ens_v2_root_l1",
    "ens_v2_registry_l1",
    "ens_v2_registrar_l1",
];

/// The kinds whose family votes an authority arm (name_authority/build.sql, `event_arms`).
const ARM_KINDS: [&str; 7] = [
    "RegistrationGranted",
    "RegistrationRenewed",
    "RegistrationReleased",
    "ExpiryChanged",
    "AuthorityTransferred",
    "TokenControlTransferred",
    "AuthorityEpochChanged",
];

/// The arm an authority event of `source_family` votes, as `event_arms` maps it.
fn family_arm(source_family: &str) -> Option<&'static str> {
    if source_family.starts_with("ens_v1_") {
        Some("ens_v1")
    } else if V2_FAMILIES.contains(&source_family) {
        Some("ens_v2")
    } else if source_family.starts_with("basenames_") {
        Some("basenames")
    } else {
        None
    }
}

/// The arm `event` votes into the row. The ENSv2 root and registry `ExpiryChanged` never votes
/// and their `RegistrationReleased` is decided at read.
fn stored_arm(event: &BlockEvent) -> Option<&'static str> {
    if !ARM_KINDS.contains(&event.event_kind.as_str()) {
        return None;
    }
    let root_or_registry = matches!(
        event.source_family.as_str(),
        "ens_v2_root_l1" | "ens_v2_registry_l1"
    );
    if root_or_registry
        && matches!(
            event.event_kind.as_str(),
            "ExpiryChanged" | "RegistrationReleased"
        )
    {
        return None;
    }
    family_arm(&event.source_family)
}

fn history_key(chain: &Value, name: &str) -> Row {
    key_of(&tables::NAME_HISTORY, [chain.clone(), json!(name)])
}

fn named(events: &[BlockEvent]) -> impl Iterator<Item = (&BlockEvent, &str)> {
    events
        .iter()
        .filter_map(|event| Some((event, event.logical_name_id.as_deref()?)))
}

/// The history keys one block's named events touch.
pub(in super::super) fn preload(chain: &Value, events: &[BlockEvent], into: &mut Preload) {
    let names: BTreeSet<&str> = named(events).map(|(_, name)| name).collect();
    into.add(
        &tables::NAME_HISTORY,
        names.into_iter().map(|name| history_key(chain, name)),
    );
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let table = &tables::NAME_HISTORY;
    let chain = json!(context.chain_id);
    let names: BTreeSet<&str> = named(events).map(|(_, name)| name).collect();
    load_rows(
        transaction,
        rows,
        table,
        names.iter().map(|name| history_key(&chain, name)).collect(),
    )
    .await?;
    for (event, name) in named(events) {
        let key = history_key(&chain, name);
        let existing = rows.get(table, &key).is_some();
        let mut row = current(rows, table, &key);
        let mut changed = false;
        if !existing {
            set(&mut row, "namespace", event.namespace.clone());
            set(&mut row, "first_block_number", context.block.number);
            set(&mut row, "created_at", context.block.timestamp.clone());
            set(&mut row, "has_ens_v2_events", false);
            set(&mut row, "event_arms", json!([]));
            changed = true;
        }
        if V2_FAMILIES.contains(&event.source_family.as_str())
            && row.get("has_ens_v2_events") != Some(&Value::Bool(true))
        {
            set(&mut row, "has_ens_v2_events", true);
            changed = true;
        }
        if let Some(arm) = stored_arm(event) {
            let mut arms: BTreeSet<String> = row
                .get("event_arms")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|arm| arm.as_str().map(str::to_owned))
                .collect();
            if arms.insert(arm.to_owned()) {
                set(&mut row, "event_arms", json!(arms));
                changed = true;
            }
        }
        if changed {
            put(rows, table, row, event)?;
        }
    }
    Ok(())
}
