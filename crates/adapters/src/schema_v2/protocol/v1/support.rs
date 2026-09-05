use serde_json::Value;

use super::super::{EventDraft, Interpreted};

pub(super) const ROOT_NODE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";
pub(super) const LLL_REGISTRY: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";

pub(super) fn single_event(
    kind: &str,
    logical_name_id: Option<String>,
    resource_id: Option<uuid::Uuid>,
    after_state: Value,
) -> Interpreted {
    let mut output = Interpreted::new();
    output.events.push(EventDraft {
        event_kind: kind.to_owned(),
        logical_name_id,
        resource_id,
        identity_suffix: kind.to_owned(),
        explicit_before: None,
        after_state,
        state_scope: String::new(),
    });
    output
}

pub(super) fn events(kinds: Vec<&str>, after_state: Value) -> Interpreted {
    let mut output = Interpreted::new();
    output
        .events
        .extend(kinds.into_iter().map(|kind| EventDraft {
            event_kind: kind.to_owned(),
            logical_name_id: None,
            resource_id: None,
            identity_suffix: kind.to_owned(),
            explicit_before: None,
            after_state: after_state.clone(),
            state_scope: String::new(),
        }));
    output
}

pub(super) fn events_linked(
    kinds: Vec<&str>,
    logical_name_id: String,
    resource_id: uuid::Uuid,
    after_state: Value,
) -> Interpreted {
    let mut output = Interpreted::new();
    output
        .events
        .extend(kinds.into_iter().map(|kind| EventDraft {
            event_kind: kind.to_owned(),
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(resource_id),
            identity_suffix: kind.to_owned(),
            explicit_before: None,
            after_state: after_state.clone(),
            state_scope: String::new(),
        }));
    output
}
