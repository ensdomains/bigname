use std::collections::BTreeSet;

use anyhow::{Result, bail};
use serde_json::Value;

use crate::normalized_events::NormalizedEvent;

/// The normalized-event journal resolves an event ID to its current row when workers
/// derive invalidations. A supersession therefore cannot move a row to a different
/// projection key: the old key would no longer be recoverable from that journal entry.
pub(super) fn ensure_stateless_replay_projection_identity_matches(
    existing: &NormalizedEvent,
    incoming: &NormalizedEvent,
) -> Result<()> {
    let mut differing_fields = Vec::new();
    compare_field(
        existing.namespace != incoming.namespace,
        "namespace",
        &mut differing_fields,
    );
    compare_field(
        existing.logical_name_id != incoming.logical_name_id,
        "logical_name_id",
        &mut differing_fields,
    );
    compare_field(
        existing.resource_id != incoming.resource_id,
        "resource_id",
        &mut differing_fields,
    );
    compare_field(
        existing.event_kind != incoming.event_kind,
        "event_kind",
        &mut differing_fields,
    );
    compare_field(
        existing.source_family != incoming.source_family,
        "source_family",
        &mut differing_fields,
    );
    compare_field(
        existing.chain_id != incoming.chain_id,
        "chain_id",
        &mut differing_fields,
    );
    compare_field(
        existing.derivation_kind != incoming.derivation_kind,
        "derivation_kind",
        &mut differing_fields,
    );

    let existing_state = state_projection_identity(existing);
    let incoming_state = state_projection_identity(incoming);
    compare_field(
        existing_state != incoming_state,
        "before_state/after_state projection keys",
        &mut differing_fields,
    );

    if !differing_fields.is_empty() {
        bail!(
            "stateless normalized-event replay authority for {} would change downstream projection identity (differing_fields={})",
            existing.event_identity,
            differing_fields.join(",")
        );
    }
    Ok(())
}

fn compare_field(changed: bool, field: &'static str, differences: &mut Vec<&'static str>) {
    if changed {
        differences.push(field);
    }
}

#[derive(Debug, Eq, PartialEq)]
enum StateProjectionIdentity {
    LabelPreimages {
        decoded_name: Option<String>,
        labelhashes: Option<Value>,
    },
    ProjectionKeys(BTreeSet<String>),
    ChildrenParentNode(Option<String>),
    /// Future stateless event kinds fail closed on state changes until their
    /// projection-key mapping is made explicit here.
    Unmapped {
        before_state: Value,
        after_state: Value,
    },
}

fn state_projection_identity(event: &NormalizedEvent) -> StateProjectionIdentity {
    match event.event_kind.as_str() {
        "PreimageObserved" => StateProjectionIdentity::LabelPreimages {
            decoded_name: text(&event.after_state, "decoded_name").map(str::to_owned),
            labelhashes: event.after_state.get("labelhashes").cloned(),
        },
        "ReverseChanged" => {
            StateProjectionIdentity::ProjectionKeys(primary_name_keys(event, false))
        }
        "RecordChanged" => StateProjectionIdentity::ProjectionKeys(primary_name_keys(event, true)),
        "ResolverChanged" => {
            let mut keys = resolver_keys(event);
            keys.extend(primary_name_keys(event, false));
            StateProjectionIdentity::ProjectionKeys(keys)
        }
        "SubregistryChanged" => StateProjectionIdentity::ChildrenParentNode(
            text(&event.after_state, "parent_node").map(str::to_owned),
        ),
        _ => StateProjectionIdentity::Unmapped {
            before_state: event.before_state.clone(),
            after_state: event.after_state.clone(),
        },
    }
}

fn resolver_keys(event: &NormalizedEvent) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    let Some(chain_id) = event.chain_id.as_deref() else {
        return keys;
    };
    for state in [&event.before_state, &event.after_state] {
        if let Some(resolver) = nonempty_text(state, "resolver") {
            keys.insert(format!("resolver:{chain_id}:{}", resolver.to_lowercase()));
        }
    }
    keys
}

fn primary_name_keys(event: &NormalizedEvent, require_name_record: bool) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    if event.logical_name_id.is_some() || event.resource_id.is_some() {
        return keys;
    }
    if require_name_record && text(&event.after_state, "record_key") != Some("name") {
        return keys;
    }

    for state in [&event.before_state, &event.after_state] {
        let claim_source = if event.event_kind == "ReverseChanged" {
            state
        } else {
            state.get("primary_claim_source").unwrap_or(&Value::Null)
        };
        let Some(address) = nonempty_text(claim_source, "address") else {
            continue;
        };
        let namespace =
            nonempty_text(claim_source, "namespace").unwrap_or(event.namespace.as_str());
        let Some(coin_type) = nonempty_text(claim_source, "coin_type") else {
            continue;
        };
        if namespace.is_empty() {
            continue;
        }
        keys.insert(format!(
            "primary:{}:{namespace}:{coin_type}",
            address.to_lowercase()
        ));
    }
    keys
}

fn nonempty_text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    text(value, key).filter(|value| !value.is_empty())
}

fn text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}
