use serde_json::Value;

use super::super::{
    V2Error, V2Result,
    vocab::{WrapperFuses, WrapperState},
};

pub(crate) fn wrapper_metadata(
    declared_summary: &Value,
) -> V2Result<Option<(WrapperState, WrapperFuses)>> {
    let state_value = declared_summary.get("wrapper_state");
    let fuses_value = declared_summary.get("wrapper_fuses");
    if state_value.is_none() && fuses_value.is_none() {
        return Ok(None);
    }
    if state_value.is_none() || fuses_value.is_none() {
        return Err(invalid_wrapper_metadata());
    }

    let state = state_value
        .and_then(Value::as_str)
        .and_then(WrapperState::from_wire)
        .ok_or_else(invalid_wrapper_metadata)?;
    let fuses =
        WrapperFuses::from_summary(declared_summary).ok_or_else(invalid_wrapper_metadata)?;
    if !lifecycle_matches_fuses(state, fuses) {
        return Err(invalid_wrapper_metadata());
    }
    Ok(Some((state, fuses)))
}

const fn lifecycle_matches_fuses(state: WrapperState, fuses: WrapperFuses) -> bool {
    match state {
        WrapperState::Wrapped => !fuses.cannot_unwrap && !fuses.parent_cannot_control,
        WrapperState::Emancipated => !fuses.cannot_unwrap && fuses.parent_cannot_control,
        WrapperState::Locked => fuses.cannot_unwrap && fuses.parent_cannot_control,
    }
}

fn invalid_wrapper_metadata() -> V2Error {
    V2Error::internal_error("stored wrapper metadata is inconsistent")
}
