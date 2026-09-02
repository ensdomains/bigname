use alloy_primitives::{hex, keccak256};
use anyhow::bail;

use super::{
    Interpreted, NameDraft, ShadowNameDraft, State, admitted_label, decode, decoded_label,
    registrar_namehash, stable_uuid, wrapper_renewal,
};
use crate::schema_v2::{catalog::Selected, model::RawLogInput};

pub(super) fn name_registered(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    event(selected, raw, state, true)
}

pub(super) fn name_renewed(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    event(selected, raw, state, false)
}

fn event(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    registration: bool,
) -> anyhow::Result<Interpreted> {
    super::super::super::ensure_declared(selected, &["PreimageObserved"])?;
    let (raw_label, explicit_labelhash, _) = decode::name(selected, raw)?;
    if keccak256(&raw_label) != explicit_labelhash {
        bail!(
            "{} label does not hash to its indexed label",
            selected.event.name
        );
    }
    let namehash = registrar_namehash(selected, explicit_labelhash);
    let previous_active = state.v1_name(&selected.source.namespace, &namehash);
    let registrar = state.v1_registrar(&selected.source.namespace, &namehash);
    let mut output = Interpreted::new();
    if let Some(wrapper_event) = wrapper_renewal::event(
        selected,
        state,
        previous_active.as_ref(),
        &namehash,
        registrar.as_ref().and_then(|state| state.expiry),
        registration,
    )? {
        output.events.push(wrapper_event);
    }
    let Some(label) = admitted_label(&raw_label) else {
        output.shadow_names.push(ShadowNameDraft {
            raw_labels: vec![raw_label, b"eth".to_vec()],
            namehash,
            source_kind: format!("{}_name", selected.event.name),
        });
        return Ok(output);
    };
    let resource_id = registrar.as_ref().map(|state| state.resource_id);
    let token_lineage_id = registrar.as_ref().and_then(|state| state.token_lineage_id);
    let bind = registrar.as_ref().is_some_and(|state| !state.surface_known);
    output.names.push(NameDraft {
        labels: vec![label, "eth".to_owned()],
        namehash,
        resource_id,
        token_lineage_id,
        surface_binding_id: bind
            .then_some(registrar.as_ref())
            .flatten()
            .and_then(|state| {
                state.authority_key.as_ref().map(|authority_key| {
                    stable_uuid(&format!(
                        "binding:{authority_key}:{}",
                        raw.block_timestamp.unix_timestamp()
                    ))
                })
            }),
        bind,
        binding_kind: "declared_registry_path".to_owned(),
        authority_arm: super::super::authority_arm(&selected.source.namespace).to_owned(),
        source_kind: format!("{}_name", selected.event.name),
        preimage_metadata: Some(serde_json::json!({
            "raw_label_hex":hex::encode(&raw_label),
            "decoded_label":decoded_label(&raw_label),
            "labelhash":format!("{explicit_labelhash:#x}"),
            "surface_known":true,
        })),
    });
    Ok(output)
}
