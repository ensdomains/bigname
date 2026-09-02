use serde_json::Value;

use super::super::{model::PriorEventInput, state::State};

pub(super) fn missing_replacement_role(
    state: &State,
    emitter: &str,
    token: &str,
    event: &PriorEventInput,
    field: &str,
) -> bool {
    matches!(
        event.after_state.get("source_event").and_then(Value::as_str),
        Some("LabelRegistered" | "LabelReserved")
    ) && event.after_state.get(field).is_some_and(Value::is_null)
        && state.v2_token(emitter, token).is_none()
}
