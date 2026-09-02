use super::{
    Interpreted, ResourceDraft, State, ZERO_ADDRESS, events_linked, new_registrar_identity,
    registrar_namehash,
};
use crate::{
    evm_abi::{address_hex, decode_event_log, saturating_u256_i64, u256_word_hex},
    schema_v2::{catalog::Selected, model::RawLogInput},
};
use alloy_primitives::B256;
use alloy_sol_types::sol;
use serde_json::json;
sol! {
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event NameRenewed(uint256 indexed id, uint256 expires);
}
pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    match selected.event.signature.as_str() {
        "NameRegistered(uint256,address,uint256)" => name_registered(selected, raw, state),
        "NameRenewed(uint256,uint256)" => name_renewed(selected, raw, state),
        signature => anyhow::bail!("unsupported BaseRegistrar lifecycle event {signature}"),
    }
}
fn name_registered(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    super::super::super::ensure_declared(selected, &["RegistrationGranted"])?;
    let event = decode_event_log::<NameRegistered>(
        &raw.topics,
        &raw.data,
        "BaseRegistrar NameRegistered log is malformed",
    )?;
    let labelhash = B256::from(event.id.to_be_bytes::<32>());
    let labelhash_hex = format!("{labelhash:#x}");
    let namehash = registrar_namehash(selected, labelhash);
    let logical_name_id = format!("{}:{namehash}", selected.source.namespace);
    let previous = state.v1_name(&selected.source.namespace, &namehash);
    let prior_registrar = state.v1_registrar(&selected.source.namespace, &namehash);
    let (token_lineage_id, resource_id, authority_key) =
        new_registrar_identity(selected, raw, &labelhash_hex);
    let expiry = saturating_u256_i64(event.expires);
    let owner = address_hex(event.owner);
    let surface_known = state.v1_active_surface_materialized(&selected.source.namespace, &namehash);
    state.observe_v1_registrar(
        &selected.source.namespace,
        &namehash,
        logical_name_id.clone(),
        surface_known,
        resource_id,
        token_lineage_id,
        selected.source.source_family.clone(),
        Some(selected.source.manifest_id),
        Some(labelhash_hex.clone()),
        Some(expiry),
        Some(owner.clone()),
        authority_key.clone(),
        false,
        true,
    );
    let after = json!({
        "source_event":"NameRegistered", "namehash":namehash, "labelhash":labelhash_hex,
        "token_id":u256_word_hex(event.id), "registrant":owner, "authority_owner":owner,
        "expiry":expiry, "surface_known":surface_known, "token_lineage_id":token_lineage_id,
        "authority_kind":"registrar", "authority_key":authority_key,
        "registration_window":"whole_transaction",
    });
    let mut output = events_linked(
        vec!["RegistrationGranted", "ExpiryChanged", "PermissionChanged"],
        surface_known.then_some(logical_name_id),
        resource_id,
        after.clone(),
    );
    output.resources.push(ResourceDraft {
        resource_id,
        token_lineage_id: Some(token_lineage_id),
    });
    output.events[0].explicit_before = Some(json!({
        "authority_kind":previous.as_ref().map(super::super::registry::authority_kind),
        "registrant":prior_registrar.as_ref().and_then(|state| state.owner.clone()),
    }));
    output.events[1].explicit_before = Some(json!({
        "expiry":prior_registrar.as_ref().and_then(|state| state.expiry),
    }));
    let (before, permission_after) = super::super::super::permissions::v1_grant_states(
        &owner,
        json!({"kind":"resource"}),
        "resource_control",
        "registrar",
        authority_key
            .as_deref()
            .expect("new registrar authority key"),
        "RegistrationGranted",
    );
    output.events[2].explicit_before = Some(before);
    output.events[2].after_state = permission_after;
    let active = state.v1_name(&selected.source.namespace, &namehash);
    super::super::registry::append_authority_transition(
        &mut output,
        super::super::authority_arm(&selected.source.namespace),
        previous.as_ref(),
        active.as_ref(),
        raw,
        &after,
        state.v1_resolver(&selected.source.namespace, &namehash),
        None,
    );
    if !surface_known {
        output
            .events
            .iter_mut()
            .for_each(|event| event.logical_name_id = None);
    }
    Ok(output)
}
fn name_renewed(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<NameRenewed>(
        &raw.topics,
        &raw.data,
        "BaseRegistrar NameRenewed log is malformed",
    )?;
    let labelhash = B256::from(event.id.to_be_bytes::<32>());
    let labelhash_hex = format!("{labelhash:#x}");
    let namehash = registrar_namehash(selected, labelhash);
    let logical_name_id = format!("{}:{namehash}", selected.source.namespace);
    let previous_active = state.v1_name(&selected.source.namespace, &namehash);
    let existing = state.v1_registrar(&selected.source.namespace, &namehash);
    let synthetic_grant = existing.is_none();
    let (token_lineage_id, resource_id, authority_key) = existing
        .as_ref()
        .map(|registrar| {
            (
                registrar.token_lineage_id.expect("registrar token lineage"),
                registrar.resource_id,
                registrar.authority_key.clone(),
            )
        })
        .unwrap_or_else(|| new_registrar_identity(selected, raw, &labelhash_hex));
    let expiry = saturating_u256_i64(event.expires);
    let surface_known = existing.as_ref().is_some_and(|state| state.surface_known)
        || state.v1_active_surface_materialized(&selected.source.namespace, &namehash);
    let owner = existing
        .as_ref()
        .and_then(|state| state.owner.clone())
        .or_else(|| Some(ZERO_ADDRESS.to_owned()));
    state.observe_v1_registrar(
        &selected.source.namespace,
        &namehash,
        logical_name_id.clone(),
        surface_known,
        resource_id,
        token_lineage_id,
        selected.source.source_family.clone(),
        Some(selected.source.manifest_id),
        Some(labelhash_hex.clone()),
        Some(expiry),
        owner.clone(),
        authority_key.clone(),
        false,
        previous_active
            .as_ref()
            .is_none_or(|current| current.authority_source_family == selected.source.source_family),
    );
    let after = json!({
        "source_event":"NameRenewed", "namehash":namehash, "labelhash":labelhash_hex,
        "token_id":u256_word_hex(event.id), "registrant":owner, "expiry":expiry,
        "surface_known":surface_known, "token_lineage_id":token_lineage_id,
        "authority_kind":"registrar", "authority_key":authority_key,
    });
    let kinds = if synthetic_grant {
        vec![
            "RegistrationGranted",
            "RegistrationRenewed",
            "ExpiryChanged",
        ]
    } else {
        vec!["RegistrationRenewed", "ExpiryChanged"]
    };
    super::super::super::ensure_declared(selected, &[kinds[0]])?;
    let mut output = events_linked(
        kinds,
        surface_known.then_some(logical_name_id),
        resource_id,
        after.clone(),
    );
    output.resources.push(ResourceDraft {
        resource_id,
        token_lineage_id: Some(token_lineage_id),
    });
    for draft in &mut output.events {
        draft.explicit_before =
            Some(json!({"expiry":existing.as_ref().and_then(|state| state.expiry)}));
    }
    if synthetic_grant {
        let active = state.v1_name(&selected.source.namespace, &namehash);
        super::super::registry::append_authority_transition(
            &mut output,
            super::super::authority_arm(&selected.source.namespace),
            previous_active.as_ref(),
            active.as_ref(),
            raw,
            &after,
            state.v1_resolver(&selected.source.namespace, &namehash),
            None,
        );
    }
    if !surface_known {
        output
            .events
            .iter_mut()
            .for_each(|event| event.logical_name_id = None);
    }
    Ok(output)
}
