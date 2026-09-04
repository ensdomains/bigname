use serde_json::Value;

use crate::schema_v2::{model::PriorEventInput, state::State};

pub(super) fn restore_migration_marker(state: &mut State, event: &PriorEventInput) {
    if event.after_state["registry_migrated"] == true
        && let Some(namehash) = event.after_state.get("namehash").and_then(Value::as_str)
    {
        state.mark_v1_migrated(&event.namespace, namehash);
    }
    if event
        .after_state
        .get("source_event")
        .and_then(Value::as_str)
        == Some("NewOwner")
        && event
            .after_state
            .get("emitter_role")
            .and_then(Value::as_str)
            == Some("registry")
        && let Some(node) = event.after_state.get("child_node").and_then(Value::as_str)
    {
        state.mark_v1_migrated(&event.namespace, node);
    }
}
