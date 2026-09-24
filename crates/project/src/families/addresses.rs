//! F13, the per-name address fold. Each name keeps its controller fold unmasked
//! (address_names.rs `controller_events` and `controller_fold`): an AuthorityTransferred or a
//! state-derived registry-only SurfaceBound sets the controller, a resource-scoped
//! PermissionChanged sets it when its effective powers hold `resource_control` and otherwise
//! revokes it from its subject only. The name also keeps its latest token holder and registrant.
//! The wrapper masks and the selected authority resource are applied where the rows are read;
//! the (address, name, relation) index rows are derived from this row in `derived`.
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{Context, current, key_of, load_rows, put, raw_lower, raw_text, set, text_or_null},
    store::RowSet,
    tables,
};
use crate::Result;

const ZERO: &str = "0x0000000000000000000000000000000000000000";

enum Change {
    Controller { set: bool, subject: Option<String> },
    TokenHolder(Option<String>),
    Registrant(Option<String>),
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
        "RegistrationGranted" => Some(Change::Registrant(raw_lower(after, "registrant"))),
        _ => None,
    }
}

fn fold_key(chain: &Value, event: &BlockEvent) -> Option<super::store::Row> {
    change(event)?;
    let name = event.logical_name_id.clone()?;
    Some(key_of(
        &tables::ADDRESS_NAME_FOLD,
        [chain.clone(), json!(name)],
    ))
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
    for event in events {
        let (Some(key), Some(change)) = (fold_key(&chain, event), change(event)) else {
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
                set(&mut row, "controller_action", "set");
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
                set(&mut row, "controller_action", "revoke");
                set(&mut row, "controller_subject", text_or_null(subject));
                set(&mut row, "controller_position", position);
            }
            Change::TokenHolder(holder) => {
                set(&mut row, "token_holder", text_or_null(holder));
                set(&mut row, "token_holder_position", position);
            }
            Change::Registrant(registrant) => {
                set(&mut row, "registrant", text_or_null(registrant));
                set(&mut row, "registrant_position", position);
            }
        }
        put(rows, table, row, event)?;
    }
    Ok(())
}
