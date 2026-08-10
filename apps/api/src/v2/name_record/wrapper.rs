use serde_json::Value;

use super::super::{
    V2Error, V2Result,
    vocab::{WrapperFuses, WrapperState},
};

const NON_PARENT_CONTROLLED_FUSES: u32 = 0x0000_FFFF;

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
    let has_locked_pair = fuses.cannot_unwrap && fuses.parent_cannot_control;
    // Any non-parent-controlled fuse requires both PARENT_CANNOT_CONTROL and
    // CANNOT_UNWRAP, including unnamed low-word bits.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1058-L1066 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L22 @ ens_v1@91c966f)
    if fuses.fuses & NON_PARENT_CONTROLLED_FUSES != 0 && !has_locked_pair {
        return false;
    }
    // .eth second-level wrapping always burns PARENT_CANNOT_CONTROL with
    // IS_DOT_ETH, and IS_DOT_ETH is excluded from user-settable fuses.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1013 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L24 @ ens_v1@91c966f)
    if fuses.is_dot_eth && !fuses.parent_cannot_control {
        return false;
    }

    match state {
        WrapperState::Wrapped => !fuses.cannot_unwrap && !fuses.parent_cannot_control,
        WrapperState::Emancipated => !fuses.cannot_unwrap && fuses.parent_cannot_control,
        WrapperState::Locked => has_locked_pair,
    }
}

fn invalid_wrapper_metadata() -> V2Error {
    V2Error::internal_error("stored wrapper metadata is inconsistent")
}
