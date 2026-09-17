//! `grant_event` provenance for resolver-overview `include=roles` items.

use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{HistoryEvent as StorageHistoryEvent, ResolverCurrentRow};
use serde_json::{Value, json};
use tracing::error;

use crate::v2::{HistoryEventType, V2Error, V2Result, format_timestamp, history_event_type};

/// Key of the provenance object attached to `include=roles` items.
const ROLE_GRANT_EVENT_KEY: &str = "grant_event";

/// Resolve the granting event for each declared role holder: the earliest
/// permission-type event among the `permissions_current` provenance rows the
/// holder has in this resolver's scope whose subject is the holder. Holders
/// without a resolvable event are left untouched.
pub(super) async fn load_role_grant_events(
    pool: &sqlx::PgPool,
    row: &ResolverCurrentRow,
    chain_id_slug: &str,
    resolver_address: &str,
) -> V2Result<BTreeMap<String, Value>> {
    let subjects = row
        .declared_summary
        .get("role_holders")
        .and_then(|summary| summary.get("items"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("subject").and_then(Value::as_str))
                .map(str::to_ascii_lowercase)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    if subjects.is_empty() {
        return Ok(BTreeMap::new());
    }
    let subjects = subjects.into_iter().collect::<Vec<_>>();

    let permission_rows = bigname_storage::load_permissions_current_for_resolver_scope_subjects(
        pool,
        chain_id_slug,
        resolver_address,
        &subjects,
    )
    .await
    .map_err(|error| {
        error!(error = ?error, "failed to load resolver role permission rows");
        V2Error::internal_error("failed to load resolver role provenance")
    })?;

    let mut event_ids_by_subject: BTreeMap<String, BTreeSet<i64>> = BTreeMap::new();
    for permission_row in &permission_rows {
        let ids = permission_row
            .provenance
            .get("normalized_event_ids")
            .and_then(Value::as_array)
            .map(|ids| ids.iter().filter_map(Value::as_i64).collect::<Vec<_>>())
            .unwrap_or_default();
        event_ids_by_subject
            .entry(permission_row.subject.to_ascii_lowercase())
            .or_default()
            .extend(ids);
    }
    let all_ids = event_ids_by_subject
        .values()
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if all_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let events = bigname_storage::load_history_events_by_ids(pool, &all_ids)
        .await
        .map_err(|error| {
            error!(error = ?error, "failed to load resolver role grant events");
            V2Error::internal_error("failed to load resolver role provenance")
        })?;

    let mut grant_events = BTreeMap::new();
    for (subject, ids) in &event_ids_by_subject {
        let grant = events
            .iter()
            .filter(|event| ids.contains(&event.normalized_event_id))
            .filter(|event| {
                history_event_type(&event.event_kind) == Some(HistoryEventType::Permission)
            })
            .filter(|event| {
                event
                    .after_state
                    .get("subject")
                    .and_then(Value::as_str)
                    .is_some_and(|candidate| candidate.eq_ignore_ascii_case(subject))
            })
            .filter(|event| event.block_number.is_some())
            .min_by_key(|event| {
                (
                    event.block_number,
                    event.log_index,
                    event.normalized_event_id,
                )
            });
        if let Some(event) = grant {
            grant_events.insert(subject.clone(), role_grant_event_value(event));
        }
    }
    Ok(grant_events)
}

pub(super) fn role_grant_event_value(event: &StorageHistoryEvent) -> Value {
    let mut object = serde_json::Map::new();
    object.insert("block_number".to_owned(), json!(event.block_number));
    if let Some(timestamp) = event.block_timestamp {
        object.insert("timestamp".to_owned(), json!(format_timestamp(timestamp)));
    }
    if let Some(transaction_hash) = event.transaction_hash.as_ref() {
        object.insert("transaction_hash".to_owned(), json!(transaction_hash));
    }
    if let Some(log_index) = event.log_index {
        object.insert("log_index".to_owned(), json!(log_index));
    }
    Value::Object(object)
}

pub(super) fn attach_role_grant_events(
    roles: Option<&mut Value>,
    grant_events: &BTreeMap<String, Value>,
) {
    let Some(items) = roles.and_then(Value::as_array_mut) else {
        return;
    };
    for item in items {
        let Some(object) = item.as_object_mut() else {
            continue;
        };
        let Some(address) = object.get("address").and_then(Value::as_str) else {
            continue;
        };
        if let Some(grant_event) = grant_events.get(&address.to_ascii_lowercase()) {
            object.insert(ROLE_GRANT_EVENT_KEY.to_owned(), grant_event.clone());
        }
    }
}
