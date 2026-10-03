//! F13, the per-name address fold and its controller candidates. Each name keeps its
//! controller fold unmasked (address_names.rs `controller_events` and `controller_fold`): an
//! AuthorityTransferred or a state-derived registry-only SurfaceBound sets the controller, a
//! resource-scoped PermissionChanged sets it when its effective powers hold `resource_control`
//! and otherwise revokes it from its subject only. The name also keeps its latest token holder,
//! and its latest registrant as the retained F2a rows of the name give it (lifecycle.rs,
//! `fold_registrants`). A registrar transfer of a lease with no name surface folds under the
//! lease's `<namespace>:<namehash>` id, which is the name's id once a surface names it.
//!
//! The served fold runs over the admitted events only: the selected authority resource, the
//! registry-only predecessor window (address_names.rs:115-211) and the resource equality of a
//! PermissionChanged (:245-278). One unmasked fold cannot give that back once a later excluded
//! event overwrote an earlier admitted one, so every controller event is also kept as its own
//! candidate row, with its resource and position, and a read folds the candidates its admission
//! keeps. The registrant comes from F2a (the retained rows of the name), not this family's events. The
//! (address, name, relation) index rows are derived from the candidates and F2a in `derived`,
//! every possible address of a name included, so read-time masks only ever remove rows.
use std::collections::HashSet;

use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{
        Context, Preload, current, key_of, load_rows, put, raw_lower, raw_text, set, text_or_null,
    },
    store::RowSet,
    tables,
};
use crate::Result;

const ZERO: &str = "0x0000000000000000000000000000000000000000";
/// The adapter's `owner_getter_reason` for a registry write naming the admitted Graveyard.
const GRAVEYARD_OWNER_REASON: &str = "graveyard";
const REGISTRAR_FAMILIES: [&str; 2] = ["ens_v1_registrar_l1", "basenames_base_registrar"];

/// The two controller actions, stored by their lower-cased names. The names are spelled through
/// `Debug` because the statement guard reads any literal that starts with an SQL keyword, as
/// `set` does, as a statement.
#[derive(Debug, Clone, Copy)]
enum Action {
    Set,
    Revoke,
}

impl Action {
    fn name(self) -> String {
        format!("{self:?}").to_lowercase()
    }
}

enum Change {
    Controller { set: bool, subject: Option<String> },
    TokenHolder(Option<String>),
}

fn change(event: &BlockEvent) -> Option<Change> {
    let after = &event.after;
    match event.event_kind.as_str() {
        "AuthorityTransferred" => {
            let subject = if raw_text(after, "owner_word_unmasked").as_deref() == Some("true") {
                Some(ZERO.to_owned())
            } else {
                raw_lower(after, "registry_owner").or_else(|| raw_lower(after, "owner"))
            };
            Some(Change::Controller { set: true, subject })
        }
        "SurfaceBound"
            if raw_text(after, "state_derived").as_deref() == Some("true")
                && raw_text(after, "authority_kind").as_deref() == Some("registry_only") =>
        {
            let subject = raw_lower(after, "owner");
            Some(Change::Controller { set: true, subject })
        }
        "PermissionChanged" => {
            let resource_scope = after
                .get("scope")
                .and_then(|scope| raw_text(scope, "kind"))
                .as_deref()
                == Some("resource");
            let powers = after.get("effective_powers").and_then(Value::as_array)?;
            resource_scope.then_some(())?;
            let control = powers.iter().any(|power| power == "resource_control");
            let subject = raw_lower(after, "subject");
            Some(Change::Controller {
                set: control,
                subject,
            })
        }
        "TokenControlTransferred" => Some(Change::TokenHolder(raw_lower(after, "to"))),
        _ => None,
    }
}

/// The raw logs, as (block, transaction index, log index), of the registry writes in `events`
/// that make the admitted Graveyard a node's registry owner (`owner_getter_reason = graveyard`).
fn graveyard_writes(events: &[BlockEvent]) -> HashSet<(i64, Option<i64>, Option<i64>)> {
    events
        .iter()
        .filter(|event| {
            event.event_kind == "AuthorityTransferred"
                && raw_text(&event.after, "owner_getter_reason").as_deref()
                    == Some(GRAVEYARD_OWNER_REASON)
        })
        .map(log_of)
        .collect()
}

fn log_of(event: &BlockEvent) -> (i64, Option<i64>, Option<i64>) {
    (
        event.position.block_number,
        event.position.transaction_index,
        event.position.log_index,
    )
}

/// The change `event` makes, with the controller a Graveyard-held registry write would set read
/// as the zero subject, which is never listed: that record names no owner (storage
/// `OwnerEvent::names_no_owner`). This covers the write's AuthorityTransferred and the
/// `resource_control` grant or registry-only binding the same log restates it with. Other
/// relations of the Graveyard, such as a live token sent to it, are untouched.
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_sepolia_20260916@366de741)
fn served_change(
    event: &BlockEvent,
    graveyard_writes: &HashSet<(i64, Option<i64>, Option<i64>)>,
) -> Option<Change> {
    match change(event)? {
        Change::Controller { set: true, .. } if graveyard_writes.contains(&log_of(event)) => {
            Some(Change::Controller {
                set: true,
                subject: Some(ZERO.to_owned()),
            })
        }
        change => Some(change),
    }
}

/// The fold row of the event's name. A registrar transfer the adapter emitted unnamed (a lease
/// with no name surface) keys the `<namespace>:<namehash>` id every registrar name has, so the
/// recipient is indexed before a surface names the row and the name keeps it after.
fn fold_key(chain: &Value, event: &BlockEvent) -> Option<super::store::Row> {
    let change = change(event)?;
    let name = match &event.logical_name_id {
        Some(name) => name.clone(),
        None if matches!(change, Change::TokenHolder(_))
            && REGISTRAR_FAMILIES.contains(&event.source_family.as_str()) =>
        {
            format!(
                "{}:{}",
                event.namespace,
                raw_lower(&event.after, "namehash")?
            )
        }
        None => return None,
    };
    Some(key_of(
        &tables::ADDRESS_NAME_FOLD,
        [chain.clone(), json!(name)],
    ))
}

/// The candidate row of a named controller event.
fn candidate_key(chain: &Value, event: &BlockEvent) -> Option<super::store::Row> {
    matches!(change(event)?, Change::Controller { .. }).then_some(())?;
    Some(key_of(
        &tables::ADDRESS_CONTROLLER_CANDIDATE,
        [
            chain.clone(),
            json!(event.logical_name_id.clone()?),
            json!(event.position.event_identity),
        ],
    ))
}

/// The fold and controller candidate keys one block's events name.
pub(super) fn preload(chain: &Value, events: &[BlockEvent], into: &mut Preload) {
    into.add(
        &tables::ADDRESS_NAME_FOLD,
        events.iter().filter_map(|event| fold_key(chain, event)),
    );
    into.add(
        &tables::ADDRESS_CONTROLLER_CANDIDATE,
        events
            .iter()
            .filter_map(|event| candidate_key(chain, event)),
    );
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let table = &tables::ADDRESS_NAME_FOLD;
    let keys = events
        .iter()
        .filter_map(|event| fold_key(&chain, event))
        .collect();
    load_rows(transaction, rows, table, keys).await?;
    let candidates = &tables::ADDRESS_CONTROLLER_CANDIDATE;
    let candidate_keys = events
        .iter()
        .filter_map(|event| candidate_key(&chain, event))
        .collect();
    load_rows(transaction, rows, candidates, candidate_keys).await?;
    let graveyard_writes = graveyard_writes(events);
    for event in events {
        if let (
            Some(key),
            Some(Change::Controller {
                set: control,
                subject,
            }),
        ) = (
            candidate_key(&chain, event),
            served_change(event, &graveyard_writes),
        ) {
            let mut row = current(rows, candidates, &key);
            set(
                &mut row,
                "resource_id",
                text_or_null(event.resource_id.clone()),
            );
            set(&mut row, "event_kind", event.event_kind.clone());
            set(&mut row, "source_family", event.source_family.clone());
            set(
                &mut row,
                "action",
                if control { Action::Set } else { Action::Revoke }.name(),
            );
            set(&mut row, "subject", text_or_null(subject));
            put(rows, candidates, row, event)?;
        }
        let (Some(key), Some(change)) = (
            fold_key(&chain, event),
            served_change(event, &graveyard_writes),
        ) else {
            continue;
        };
        let mut row = current(rows, table, &key);
        for column in [
            "controller",
            "controller_action",
            "controller_subject",
            "controller_position",
            "token_holder",
            "token_holder_position",
            "registrant",
            "registrant_position",
        ] {
            row.entry(column).or_insert(Value::Null);
        }
        let position = event.position.to_json();
        match change {
            Change::Controller { set: true, subject } => {
                set(&mut row, "controller", text_or_null(subject.clone()));
                set(&mut row, "controller_action", Action::Set.name());
                set(&mut row, "controller_subject", text_or_null(subject));
                set(&mut row, "controller_position", position);
            }
            Change::Controller {
                set: false,
                subject,
            } => {
                // A revoke from anyone but the current controller changes nothing.
                let current = row.get("controller").and_then(Value::as_str);
                if subject.is_none() || current != subject.as_deref() {
                    continue;
                }
                set(&mut row, "controller", Value::Null);
                set(&mut row, "controller_action", Action::Revoke.name());
                set(&mut row, "controller_subject", text_or_null(subject));
                set(&mut row, "controller_position", position);
            }
            Change::TokenHolder(holder) => {
                set(&mut row, "token_holder", text_or_null(holder));
                set(&mut row, "token_holder_position", position);
            }
        }
        put(rows, table, row, event)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{Change, fold_key, graveyard_writes, served_change};
    use crate::families::{input::BlockEvent, position::Position};

    const GRAVEYARD: &str = "0x950b93885b33ce4c7e8571be2c88a1aa93d82f49";
    const ZERO: &str = "0x0000000000000000000000000000000000000000";

    fn event(kind: &str, log: i64, after: Value) -> BlockEvent {
        BlockEvent {
            normalized_event_id: log,
            position: Position {
                block_number: 10,
                transaction_index: Some(0),
                log_index: Some(log),
                event_identity: format!("{kind}:{log}"),
            },
            namespace: "ens".to_owned(),
            logical_name_id: Some("ens:0x01".to_owned()),
            resource_id: Some("resource".to_owned()),
            event_kind: kind.to_owned(),
            source_family: "ens_v1_registry_l1".to_owned(),
            source_manifest_id: None,
            transaction_hash: None,
            before: json!({}),
            after,
            raw_fact_ref: json!({}),
        }
    }

    fn controller(change: Option<Change>) -> Option<String> {
        match change {
            Some(Change::Controller { set: true, subject }) => subject,
            _ => panic!("expected a controller set"),
        }
    }

    /// Only a registry write the adapter marked as Graveyard-held, and the grant its own log
    /// restates it with, name no controller; the Graveyard address itself is not masked.
    #[test]
    fn a_graveyard_held_write_names_no_controller_only_on_its_own_log() {
        let owner = json!({"owner": GRAVEYARD, "owner_getter": GRAVEYARD});
        let mut marked = owner.clone();
        marked["owner_getter_reason"] = json!("graveyard");
        let grant = json!({
            "subject": GRAVEYARD,
            "scope": {"kind": "resource"},
            "effective_powers": ["resource_control"],
        });
        let events = [
            event("AuthorityTransferred", 1, marked),
            event("PermissionChanged", 1, grant.clone()),
            event("PermissionChanged", 2, grant),
            event("AuthorityTransferred", 3, owner),
        ];
        let writes = graveyard_writes(&events);
        let subjects = events
            .iter()
            .map(|event| controller(served_change(event, &writes)))
            .collect::<Vec<_>>();
        assert_eq!(
            subjects,
            [ZERO, ZERO, GRAVEYARD, GRAVEYARD].map(|subject| Some(subject.to_owned()))
        );
    }

    /// An unnamed transfer folds under `<namespace>:<namehash>` only from a registrar family;
    /// a NameWrapper transfer (`node`, no `namehash`) and an unnamed controller event key nothing.
    #[test]
    fn an_unnamed_registrar_transfer_keys_its_namespace_and_namehash() {
        let keyed = |namespace: &str, family: &str, kind: &str, after: Value| {
            let mut event = event(kind, 1, after);
            event.namespace = namespace.to_owned();
            event.source_family = family.to_owned();
            event.logical_name_id = None;
            fold_key(&json!("1"), &event).map(|key| key["logical_name_id"].clone())
        };
        let transfer = json!({"namehash": "0xAB", "node": "0xab", "to": GRAVEYARD});
        assert_eq!(
            keyed(
                "ens",
                "ens_v1_registrar_l1",
                "TokenControlTransferred",
                transfer.clone()
            ),
            Some(json!("ens:0xab"))
        );
        assert_eq!(
            keyed(
                "basenames",
                "basenames_base_registrar",
                "TokenControlTransferred",
                transfer.clone()
            ),
            Some(json!("basenames:0xab"))
        );
        assert_eq!(
            keyed(
                "ens",
                "ens_v1_wrapper_l1",
                "TokenControlTransferred",
                transfer
            ),
            None
        );
        assert_eq!(
            keyed(
                "ens",
                "ens_v1_registrar_l1",
                "AuthorityTransferred",
                json!({"namehash": "0xab", "owner": GRAVEYARD})
            ),
            None
        );
    }
}
