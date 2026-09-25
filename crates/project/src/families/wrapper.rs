//! F2b, NameWrapper state per wrapper resource: the latest PermissionScopeChanged's wrapper
//! state and fuses, and the latest wrapper expiry (an ExpiryChanged from the wrapper, or from
//! the registrar when it is the wrapper's NameRenewed), each with the position that set it
//! (permissions.rs, `modifiers` and `wrapper_expiries`; children.rs), and the newest wrapper
//! lifecycle event with the latest unwrap (resource_summary.rs, `wrapper_lifecycles`). Masks
//! that depend on the block clock apply at read.
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::{
    input::BlockEvent,
    reduce::{Context, current, key_of, load_rows, put, raw_text, set, text_or_null},
    store::RowSet,
    tables,
};
use crate::Result;

fn modifier(event: &BlockEvent) -> bool {
    event.event_kind == "PermissionScopeChanged" && event.source_family == "ens_v1_wrapper_l1"
}

/// A wrapper lifecycle event as the served permissions summary ranks them
/// (resource_summary.rs, `wrapper_lifecycles`): the NameWrapped mint, the NameUnwrapped epoch
/// close, or a holder grant or revoke of the resource. Returns its source and whether it leaves
/// the resource unwrapped.
fn lifecycle(event: &BlockEvent) -> Option<(&'static str, bool)> {
    if event.source_family != "ens_v1_wrapper_l1" {
        return None;
    }
    let source_event = raw_text(&event.after, "source_event");
    match event.event_kind.as_str() {
        "TokenControlTransferred" if source_event.as_deref() == Some("NameWrapped") => {
            Some(("NameWrapped", false))
        }
        "AuthorityEpochChanged" | "SurfaceUnbound"
            if source_event.as_deref() == Some("NameUnwrapped") =>
        {
            Some(("NameUnwrapped", true))
        }
        "PermissionChanged" => {
            let resource_scope =
                event.after.pointer("/scope/kind").and_then(Value::as_str) == Some("resource");
            let powers = event
                .after
                .get("effective_powers")
                .and_then(Value::as_array)?;
            // The served CTE reads COALESCE(grant_source ->> relation_kind, revocation_source
            // ->> relation_kind), which also falls through a JSON-null relation_kind. The adapter
            // never writes one: every wrapper PermissionChanged comes from v1_wrapper_states
            // (adapters schema_v2/protocol/permissions.rs:196-236, called from
            // protocol/v1/wrapper/permissions.rs:103), whose source always carries relation_kind
            // as a string (:205) and whose after-state sets exactly one of grant_source and
            // revocation_source, the other being JSON null (:229, :234).
            let relation = event
                .after
                .pointer("/grant_source/relation_kind")
                .or_else(|| event.after.pointer("/revocation_source/relation_kind"))
                .and_then(Value::as_str);
            let lifecycle = if powers.is_empty() {
                ("holder_revoke", true)
            } else {
                ("holder_grant", false)
            };
            (resource_scope && relation == Some("holder")).then_some(lifecycle)
        }
        _ => None,
    }
}

fn wrapper_expiry(event: &BlockEvent) -> bool {
    event.event_kind == "ExpiryChanged"
        && (event.source_family == "ens_v1_wrapper_l1"
            || (event.source_family == "ens_v1_registrar_l1"
                && raw_text(&event.after, "source_event").as_deref() == Some("NameRenewed")
                && raw_text(&event.after, "authority_kind").as_deref() == Some("wrapper")))
}

/// A JSON number within `[low, high]`, as its numeric text.
fn bounded(value: Option<&Value>, high: &str) -> Value {
    let Some(Value::Number(number)) = value else {
        return Value::Null;
    };
    let text = number.to_string();
    let integral = !text.contains(['.', 'e', 'E']);
    let negative = text.starts_with('-');
    let fits = integral
        && !negative
        && (text.len() < high.len() || (text.len() == high.len() && text.as_str() <= high));
    if fits {
        Value::Number(number.clone())
    } else {
        Value::Null
    }
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let chain = json!(context.chain_id);
    let table = &tables::WRAPPER_STATE;
    let relevant: Vec<(&BlockEvent, &str)> = events
        .iter()
        .filter(|event| modifier(event) || wrapper_expiry(event) || lifecycle(event).is_some())
        .filter_map(|event| Some((event, event.resource_id.as_deref()?)))
        .collect();
    let keys = relevant
        .iter()
        .map(|(_, resource)| key_of(table, [chain.clone(), json!(resource)]))
        .collect();
    load_rows(transaction, rows, table, keys).await?;
    for (event, resource) in relevant {
        let mut row = current(
            rows,
            table,
            &key_of(table, [chain.clone(), json!(resource)]),
        );
        if event.logical_name_id.is_some() {
            set(
                &mut row,
                "logical_name_id",
                text_or_null(event.logical_name_id.clone()),
            );
        }
        if let Some((source, unwrapped)) = lifecycle(event) {
            set(&mut row, "lifecycle_source", source);
            set(&mut row, "lifecycle_unwrapped", Value::Bool(unwrapped));
            set(&mut row, "lifecycle_position", event.position.to_json());
            if source == "NameUnwrapped" {
                set(&mut row, "unwrapped_position", event.position.to_json());
            }
        } else if modifier(event) {
            let state = raw_text(&event.after, "wrapper_state")
                .filter(|state| matches!(state.as_str(), "wrapped" | "emancipated" | "locked"));
            set(&mut row, "wrapper_state", text_or_null(state));
            set(
                &mut row,
                "fuses",
                bounded(event.after.get("fuses"), "9223372036854775807"),
            );
            set(&mut row, "wrapper_state_position", event.position.to_json());
        } else {
            set(
                &mut row,
                "expiry_seconds",
                bounded(event.after.get("expiry"), "18446744073709551615"),
            );
            set(&mut row, "expiry_position", event.position.to_json());
        }
        if let Some(unmasked) = event.after.get("owner_word_unmasked") {
            set(
                &mut row,
                "owner_word_unmasked",
                unmasked.as_bool().map_or(Value::Null, Value::Bool),
            );
        }
        put(rows, table, row, event)?;
    }
    Ok(())
}
