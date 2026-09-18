use alloy_primitives::{hex, keccak256};
use anyhow::bail;

use serde_json::{Value, json};

use super::super::super::{EventDraft, SourcedEventBatch};
use super::super::authority_transition::authority_kind;
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
    let (raw_label, explicit_labelhash, observation) = decode::name(selected, raw)?;
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
        raw,
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
    let binding_target = previous_active.as_ref().or_else(|| {
        registrar.as_ref().filter(|_| {
            state
                .v1_registry_owner(&selected.source.namespace, &namehash)
                .as_deref()
                != Some(super::ZERO_ADDRESS)
        })
    });
    let resource_id = binding_target.map(|state| state.resource_id);
    let token_lineage_id = binding_target.and_then(|state| state.token_lineage_id);
    let bind = binding_target.is_some_and(|state| !state.surface_known);
    // This event names a surface its authority did not have. A resolver set
    // while the name was unknown was linked to the resource alone; it is
    // replayed onto the surface now, as a registrar event does for a
    // registry-only authority it promotes.
    if bind
        && let Some(target) = binding_target
        && let Some(source_manifest_id) = target.source_manifest_id
        && let Some(link) = state.name_v1_resolver_link(
            &selected.source.namespace,
            &namehash,
            &format!("{}:{namehash}", selected.source.namespace),
            target.resource_id,
        )
    {
        output.sourced_events.push(SourcedEventBatch {
            source_manifest_id,
            events: vec![EventDraft {
                event_kind: "ResolverChanged".to_owned(),
                logical_name_id: Some(format!("{}:{namehash}", selected.source.namespace)),
                resource_id: Some(target.resource_id),
                identity_suffix: format!(
                    "ResolverChanged:surface-materialization:{namehash}:{}:{}",
                    target.resource_id, link.resolver_address
                ),
                explicit_before: Some(json!({"resolver":Value::Null})),
                after_state: json!({
                    "state_derived":true,
                    "surface_materialization":true,
                    "source_event":selected.event.name,
                    "node":namehash,
                    "authority_kind":authority_kind(target),
                    "authority_key":target.authority_key,
                    "binding_kind":"declared_registry_path",
                    "pointer_reason":"surface_materialization_current_resolver",
                    "resolver":link.resolver_address,
                    "resolver_source_role":link.source_role,
                }),
                state_scope: format!(
                    "surface-materialization:{namehash}:{}:resolver",
                    target.resource_id
                ),
            }],
        });
    }
    output.names.push(NameDraft {
        labels: vec![label, "eth".to_owned()],
        namehash,
        resource_id,
        token_lineage_id,
        surface_binding_id: bind.then_some(binding_target).flatten().and_then(|state| {
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
        preimage_metadata: Some(super::super::registry::merge_observation(
            &observation,
            json!({
                "raw_label_hex":hex::encode(&raw_label),
                "decoded_label":decoded_label(&raw_label),
                "labelhash":format!("{explicit_labelhash:#x}"),
                "surface_known":true,
            }),
        )),
    });
    Ok(output)
}
