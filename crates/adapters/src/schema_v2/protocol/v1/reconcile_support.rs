use std::collections::BTreeSet;

use uuid::Uuid;

use crate::schema_v2::model::NormalizedEvent;

pub(super) fn event_position(event: &NormalizedEvent) -> Option<(i64, i64, i64)> {
    Some((
        event.block_number?,
        event.transaction_index?,
        event.log_index?,
    ))
}

pub(super) fn is_pending_setup(
    event: &NormalizedEvent,
    namespace: &str,
    block_hash: &str,
    transaction_hash: &str,
    registration_log_index: i64,
    namehash: &str,
) -> bool {
    matches!(
        event.source_family.as_str(),
        "ens_v1_registry_l1" | "basenames_base_registry"
    ) && is_registration_window(
        event,
        namespace,
        block_hash,
        transaction_hash,
        registration_log_index,
        namehash,
    )
}

pub(super) fn is_pending_resolver_setup(
    event: &NormalizedEvent,
    namespace: &str,
    block_hash: &str,
    transaction_hash: &str,
    registration_log_index: i64,
    namehash: &str,
    first_ownership_setup_log_index: Option<i64>,
    stale_registry_resources: &BTreeSet<Uuid>,
) -> bool {
    matches!(
        event.source_family.as_str(),
        "ens_v1_resolver_l1" | "basenames_base_resolver"
    ) && event
        .resource_id
        .is_none_or(|resource| stale_registry_resources.contains(&resource))
        && first_ownership_setup_log_index.is_some_and(|setup_log_index| {
            event
                .log_index
                .is_some_and(|event_log_index| event_log_index > setup_log_index)
        })
        && is_registration_window(
            event,
            namespace,
            block_hash,
            transaction_hash,
            registration_log_index,
            namehash,
        )
}

pub(super) fn is_registry_ownership_setup(
    event: &NormalizedEvent,
    namespace: &str,
    block_hash: &str,
    transaction_hash: &str,
    registration_log_index: i64,
    namehash: &str,
) -> bool {
    matches!(
        event
            .after_state
            .get("source_event")
            .and_then(serde_json::Value::as_str),
        Some("NewOwner" | "Transfer")
    ) && is_pending_setup(
        event,
        namespace,
        block_hash,
        transaction_hash,
        registration_log_index,
        namehash,
    )
}

fn is_registration_window(
    event: &NormalizedEvent,
    namespace: &str,
    block_hash: &str,
    transaction_hash: &str,
    registration_log_index: i64,
    namehash: &str,
) -> bool {
    event.namespace == namespace
        && event.block_hash.as_deref() == Some(block_hash)
        && event.transaction_hash.as_deref() == Some(transaction_hash)
        && event
            .log_index
            .is_some_and(|index| index < registration_log_index)
        && event_target_namehash(event).is_some_and(|target| target.eq_ignore_ascii_case(namehash))
}

fn event_target_namehash(event: &NormalizedEvent) -> Option<&str> {
    event
        .after_state
        .get("child_node")
        .or_else(|| event.after_state.get("node"))
        .and_then(serde_json::Value::as_str)
}
