use uuid::Uuid;

use crate::schema_v2::{model::PriorEventInput, state::State};

pub(super) fn restore_preimage(state: &mut State, event: &PriorEventInput) {
    if event.event_kind != "PreimageObserved" || event.logical_name_id.is_none() {
        return;
    }
    let Some(namehash) = event
        .after_state
        .get("namehash")
        .and_then(serde_json::Value::as_str)
    else {
        return;
    };
    if event
        .after_state
        .get("visibility_state")
        .and_then(serde_json::Value::as_str)
        == Some("shadow")
    {
        state.observe_v1_surface(&event.namespace, namehash);
    } else {
        state.observe_v1_active_surface(&event.namespace, namehash);
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn restore_registrar(
    state: &mut State,
    event: &PriorEventInput,
    source_event: Option<&str>,
    logical_name_id: &String,
    resource_id: Uuid,
    lineage: Option<Uuid>,
    expiry: Option<i64>,
) -> bool {
    match source_event {
        Some("NameRegistered" | "NameRenewed") if event.event_kind == "RegistrationGranted" => {
            let (Some(namehash), Some(lineage)) = (
                event
                    .after_state
                    .get("namehash")
                    .and_then(serde_json::Value::as_str),
                lineage,
            ) else {
                return true;
            };
            let registration = source_event == Some("NameRegistered");
            let current = state.v1_name(&event.namespace, namehash);
            let retained_authority_owner = event
                .after_state
                .get("authority_owner")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let registration_registry_owner = registration
                .then(|| state.v1_registry_owner(&event.namespace, namehash))
                .flatten()
                .filter(|owner| {
                    !owner.eq_ignore_ascii_case("0x0000000000000000000000000000000000000000")
                });
            let event_registrant = event
                .after_state
                .get("registrant")
                .or_else(|| event.after_state.get("owner"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let registrar_owner = retained_authority_owner
                .or(registration_registry_owner)
                .or(event_registrant);
            let make_current = current.is_none_or(|current| {
                let same_family = current.authority_source_family == event.source_family;
                current.authority_source_family != "ens_v1_wrapper_l1"
                    && (registration || same_family)
            });
            state.observe_v1_registrar(
                &event.namespace,
                namehash,
                logical_name_id.clone(),
                event
                    .after_state
                    .get("surface_known")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(true),
                resource_id,
                lineage,
                event.source_family.clone(),
                event.source_manifest_id,
                event
                    .after_state
                    .get("labelhash")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                expiry,
                registrar_owner,
                event
                    .after_state
                    .get("authority_key")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                false,
                make_current,
            );
            true
        }
        Some("NameRenewed") if event.event_kind == "RegistrationRenewed" => {
            let (Some(namehash), Some(lineage)) = (
                event
                    .after_state
                    .get("namehash")
                    .and_then(serde_json::Value::as_str),
                lineage,
            ) else {
                return true;
            };
            let make_current = state
                .v1_name(&event.namespace, namehash)
                .is_none_or(|current| current.authority_source_family == event.source_family);
            let retained = state.v1_registrar(&event.namespace, namehash);
            state.observe_v1_registrar(
                &event.namespace,
                namehash,
                logical_name_id.clone(),
                event
                    .after_state
                    .get("surface_known")
                    .and_then(serde_json::Value::as_bool)
                    .or_else(|| retained.as_ref().map(|state| state.surface_known))
                    .unwrap_or(true),
                resource_id,
                lineage,
                event.source_family.clone(),
                event.source_manifest_id,
                event
                    .after_state
                    .get("labelhash")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| retained.as_ref().and_then(|state| state.labelhash.clone())),
                expiry,
                event
                    .after_state
                    .get("registrant")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| retained.as_ref().and_then(|state| state.owner.clone())),
                event
                    .after_state
                    .get("authority_key")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        retained
                            .as_ref()
                            .and_then(|state| state.authority_key.clone())
                    }),
                false,
                make_current,
            );
            true
        }
        _ => false,
    }
}
