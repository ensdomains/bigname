use alloy_primitives::U256;
use anyhow::Context;
use serde_json::json;

use crate::schema_v2::{
    catalog::Selected,
    model::RawLogInput,
    protocol::EventDraft,
    state::{State, V1NameState},
};

const ENS_GRACE_PERIOD_SECS: u64 = 90 * 24 * 60 * 60;

pub(super) fn event(
    selected: &Selected,
    state: &mut State,
    previous_active: Option<&V1NameState>,
    namehash: &str,
    raw: &RawLogInput,
    registration: bool,
) -> anyhow::Result<Option<EventDraft>> {
    if registration
        || selected.emitter_role.as_deref() != Some("wrapped_registrar_controller")
        || previous_active
            .is_none_or(|active| active.authority_source_family != "ens_v1_wrapper_l1")
    {
        return Ok(None);
    }
    let Some(registrar_expiry) = state
        .v1_registrar(&selected.source.namespace, namehash)
        .and_then(|registrar| registrar.expiry)
    else {
        return Ok(None);
    };
    let registrar_expiry = u64::try_from(registrar_expiry)?;
    let registrar_word = state
        .v1_registrar_renewal_expiry(&selected.source.namespace, namehash, raw)
        .unwrap_or_else(|| U256::from(registrar_expiry));
    let wrapper_expiry = wrapper_expiry(registrar_word)?;
    let Some((previous_expiry, wrapper)) =
        state.renew_v1_wrapper_expiry(&selected.source.namespace, namehash, wrapper_expiry)
    else {
        return Ok(None);
    };
    Ok(Some(EventDraft {
        event_kind: "ExpiryChanged".to_owned(),
        logical_name_id: Some(wrapper.logical_name_id),
        resource_id: Some(wrapper.resource_id),
        identity_suffix: "ExpiryChanged:wrapper".to_owned(),
        explicit_before: Some(json!({"expiry":previous_expiry})),
        after_state: json!({
            "source_event":"NameRenewed",
            "node":namehash,
            "expiry":wrapper_expiry,
            "registrar_expiry":registrar_expiry,
            "authority_kind":"wrapper",
            "emitter_role":"wrapped_registrar_controller",
            "token_lineage_id":wrapper.token_lineage_id.map(|id| id.to_string()),
        }),
        state_scope: String::new(),
    }))
}

// NameWrapper narrows to uint64 before its checked grace-period addition.
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L318-L337 @ ens_v1@91c966f)
fn wrapper_expiry(registrar_expiry: U256) -> anyhow::Result<u64> {
    registrar_expiry.as_limbs()[0]
        .checked_add(ENS_GRACE_PERIOD_SECS)
        .context("NameWrapper renewal expiry overflows uint64")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registrar_width_is_preserved_until_wrapper_conversion() {
        let ordinary = U256::from(1_900_000_200_u64);
        assert_eq!(wrapper_expiry(ordinary).unwrap(), 1_907_776_200);
        assert_eq!(
            wrapper_expiry((U256::from(1) << 64) + ordinary).unwrap(),
            wrapper_expiry(ordinary).unwrap()
        );
        assert_eq!(
            wrapper_expiry(U256::from(1) << 255).unwrap(),
            ENS_GRACE_PERIOD_SECS
        );
    }

    #[test]
    fn wrapper_grace_addition_uses_checked_uint64_arithmetic() {
        let boundary = u64::MAX - ENS_GRACE_PERIOD_SECS;
        assert_eq!(wrapper_expiry(U256::from(boundary)).unwrap(), u64::MAX);
        assert!(wrapper_expiry(U256::from(boundary + 1)).is_err());
    }
}
