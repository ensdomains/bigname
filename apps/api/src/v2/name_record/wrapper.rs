use serde_json::Value;

use super::super::{
    V2Error, V2Result,
    timestamps::ExpiryTimestamp,
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
    if !wrapper_lifecycle_matches_fuses(state, fuses) {
        return Err(invalid_wrapper_metadata());
    }
    Ok(Some((state, fuses)))
}

/// A NameWrapper entry's stored expiry word as served `wrapper_expires_at` with its
/// `wrapper_expires_at_reason`, identically on `ens_v1` and the wrapper `restrictions`: the exact
/// second, or null for the NameWrapper maximum (`no_expiry`) and zero (`not_set`). `None` when
/// the word is not a whole second.
pub(crate) fn wrapper_expiry(word: &Value) -> Option<(ExpiryTimestamp, Option<String>)> {
    let seconds = bigname_storage::UnixSeconds::from_json(word)?;
    let reason = bigname_storage::contract_expiry_reason(seconds, "ens_v1_wrapper_l1", None);
    let timestamp = match reason {
        Some(_) => ExpiryTimestamp::NoExpiry,
        None => ExpiryTimestamp::Seconds(seconds.unix_timestamp().to_string()),
    };
    Some((timestamp, reason.map(str::to_owned)))
}

/// Whether a NameWrapper lifecycle label agrees with a fuse word; shared by name detail and the
/// `restrictions` block so both reject an inconsistent projection identically.
pub(crate) const fn wrapper_lifecycle_matches_fuses(
    state: WrapperState,
    fuses: WrapperFuses,
) -> bool {
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

/// The served `manager` (docs/api-v1.md, Manager): the registry owner of a name with no
/// NameWrapper state, and the token holder, the `owner`, of a wrapped name in any wrapper state,
/// because NameWrapper authorizes record changes by token holder or approved operator with no
/// wrapper-state condition, except while a wrapped `.eth` name is in registrar grace, when no one
/// can (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L202-L222 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1082-L1089 @ ens_v1@91c966f).
/// Burned fuses can still forbid a particular change. A released name, and a wrapped name whose
/// NameWrapper state is unknown or lapsed (`wrapper_masked`), has no manager.
pub(crate) fn served_manager(
    declared_summary: &Value,
    owner: Option<&String>,
    registry_owner: Option<&String>,
) -> Option<String> {
    if declared_summary.pointer("/registration/status") == Some(&Value::from("released"))
        || declared_summary.get("wrapper_masked") == Some(&Value::Bool(true))
    {
        None
    } else if declared_summary.get("wrapper_state").is_none() {
        registry_owner.cloned()
    } else if declared_summary.get("wrapper_in_grace") == Some(&Value::Bool(true)) {
        None
    } else {
        owner.cloned()
    }
}

fn invalid_wrapper_metadata() -> V2Error {
    V2Error::internal_error("stored wrapper metadata is inconsistent")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::served_manager;

    #[test]
    fn manager_is_the_registry_owner_unwrapped_and_the_holder_outside_grace() {
        let holder = "0xholder".to_owned();
        let registry = "0xregistry".to_owned();
        assert_eq!(
            served_manager(&json!({}), Some(&holder), Some(&registry)),
            Some(registry.clone())
        );
        for state in ["wrapped", "emancipated", "locked"] {
            for (in_grace, expected) in [
                (None, Some(&holder)),
                (Some(false), Some(&holder)),
                (Some(true), None),
            ] {
                let mut summary = json!({"wrapper_state": state});
                if let Some(in_grace) = in_grace {
                    summary["wrapper_in_grace"] = json!(in_grace);
                }
                assert_eq!(
                    served_manager(&summary, Some(&holder), Some(&registry)).as_ref(),
                    expected,
                    "{summary}"
                );
            }
        }
        assert_eq!(
            served_manager(&json!({"wrapper_state": "locked"}), None, Some(&registry)),
            None
        );
    }

    #[test]
    fn a_wrapper_with_an_unknown_state_has_no_manager() {
        let registry = "0xregistry".to_owned();
        assert_eq!(
            served_manager(&json!({"wrapper_masked": true}), None, Some(&registry)),
            None
        );
    }

    #[test]
    fn a_released_name_has_no_manager() {
        let holder = "0xholder".to_owned();
        let registry = "0xregistry".to_owned();
        for summary in [
            json!({"registration": {"status": "released"}}),
            json!({"registration": {"status": "released"}, "wrapper_state": "wrapped"}),
        ] {
            assert_eq!(
                served_manager(&summary, Some(&holder), Some(&registry)),
                None,
                "{summary}"
            );
        }
    }
}
