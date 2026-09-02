use serde_json::json;

use crate::schema_v2::{
    catalog::Selected,
    protocol::EventDraft,
    state::{State, V1NameState},
};

const ENS_GRACE_PERIOD_SECS: u64 = 90 * 24 * 60 * 60;

pub(super) fn event(
    selected: &Selected,
    state: &mut State,
    previous_active: Option<&V1NameState>,
    namehash: &str,
    registrar_expiry: Option<i64>,
    registration: bool,
) -> anyhow::Result<Option<EventDraft>> {
    if registration
        || selected.emitter_role.as_deref() != Some("wrapped_registrar_controller")
        || previous_active
            .is_none_or(|active| active.authority_source_family != "ens_v1_wrapper_l1")
    {
        return Ok(None);
    }
    let Some(registrar_expiry) = registrar_expiry else {
        return Ok(None);
    };
    let registrar_expiry = u64::try_from(registrar_expiry)?;
    let wrapper_expiry = registrar_expiry
        .checked_add(ENS_GRACE_PERIOD_SECS)
        .ok_or_else(|| anyhow::anyhow!("wrapped renewal expiry exceeds uint64"))?;
    let Some((previous_expiry, wrapper)) =
        state.update_v1_wrapper_expiry(&selected.source.namespace, namehash, wrapper_expiry)
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
