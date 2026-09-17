//! Named snapshots of registrar authority already proven before a readable trigger.
use serde_json::json;

use super::super::{EventDraft, Interpreted, SourcedEventBatch, permissions::v1_grant_states};
use crate::schema_v2::{
    catalog::Selected,
    common::{hash_hex, namehash, normalization_flag},
    model::RawLogInput,
    state::State,
};

pub(in crate::schema_v2) fn materialize(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    output: &mut Interpreted,
) -> anyhow::Result<()> {
    if !matches!(
        raw.canonicality_state.as_str(),
        "canonical" | "safe" | "finalized"
    ) {
        return Ok(());
    }
    let controller_enrichment = selected.source.source_family == "ens_v1_registrar_l1"
        && selected
            .emitter_role
            .as_deref()
            .is_some_and(|role| role != "registrar")
        && matches!(
            selected.event.name.as_str(),
            "NameRegistered" | "NameRenewed"
        )
        && selected
            .event
            .normalized_events
            .iter()
            .any(|kind| kind == "PreimageObserved")
        && !selected
            .event
            .normalized_events
            .iter()
            .any(|kind| kind == "RegistrationGranted");
    let names = output
        .names
        .iter()
        .filter(|name| {
            // This producer already owns the exact registrar binding; external name drafts do not.
            let controller_owns_binding = controller_enrichment
                && name.bind
                && state
                    .v1_registrar(&selected.source.namespace, &name.namehash)
                    .is_some_and(|registrar| {
                        name.resource_id == Some(registrar.resource_id)
                            && name.token_lineage_id.is_some()
                            && name.token_lineage_id == registrar.token_lineage_id
                    });
            !controller_owns_binding
                && !name.labels.is_empty()
                && namehash(&name.labels) == name.namehash
                && name
                    .labels
                    .iter()
                    .all(|label| normalization_flag(Some(label)).normalized)
        })
        .map(|name| (name.namehash.clone(), hash_hex(name.labels[0].as_bytes())))
        .collect::<Vec<_>>();
    for (node, labelhash) in names {
        let Some((authority, resolver, evidence)) = state.disclose_retained_registrar(
            &selected.source.namespace,
            &node,
            &labelhash,
            raw.block_timestamp.unix_timestamp(),
        )?
        else {
            continue;
        };
        let source_manifest_id = authority
            .source_manifest_id
            .expect("verified original grant manifest");
        let owner = authority.owner.as_deref().expect("verified current owner");
        let authority_key = authority
            .authority_key
            .as_deref()
            .expect("verified authority key");
        let common = json!({
            "state_derived":true,"surface_materialization":true,"registrar_surface_snapshot":true,
            "source_event":"ReadableNameObserved","readable_source_event":selected.event.name,
            "readable_source_manifest_id":selected.source.manifest_id,
            "readable_source_family":selected.source.source_family,
            "node":node,"namehash":node,"labelhash":labelhash,
            "surface_known":true,"authority_kind":"registrar","authority_key":authority_key,
            "token_lineage_id":authority.token_lineage_id,"registrant":owner,"authority_owner":owner,
            "owner":owner,"owner_getter":owner,
            "registry_contract":evidence["registry_owner"]["raw_fact_ref"]["emitting_address"],
            "expiry":authority.expiry,"original_registered_at":evidence["grant"]["timestamp"],
            "registrar_surface_evidence":evidence,
        });
        let mut derived = Interpreted::new();
        super::registry::surface::append_binding(&mut derived, &authority, "ens_v1", raw, None);
        super::registry::surface::append_bound_event(&mut derived, &authority, raw, &common);
        for kind in ["RegistrationGranted", "ExpiryChanged"] {
            derived.events.push(EventDraft {
                event_kind: kind.to_owned(),
                logical_name_id: Some(authority.logical_name_id.clone()),
                resource_id: Some(authority.resource_id),
                identity_suffix: format!("{kind}:registrar-surface:{node}"),
                explicit_before: Some(json!({})),
                after_state: common.clone(),
                state_scope: format!("registrar-surface:{node}:{}", authority.resource_id),
            });
        }
        let mut permissions = vec![(json!({"kind":"resource"}), "resource_control")];
        if let Some(resolver) = resolver {
            derived.events.push(EventDraft {
                event_kind:"ResolverChanged".to_owned(),logical_name_id:Some(authority.logical_name_id.clone()),
                resource_id:Some(authority.resource_id),identity_suffix:format!("ResolverChanged:registrar-surface:{node}"),
                explicit_before:Some(json!({"resolver":null})),
                after_state:super::authority_transition::merge_observation(&common,json!({
                    "resolver":resolver.resolver_address,"resolver_source_role":resolver.source_role,
                    "pointer_reason":"surface_materialization_current_resolver",
                })),state_scope:format!("registrar-surface:{node}:resolver"),
            });
            if resolver.resolver_address != "0x0000000000000000000000000000000000000000" {
                permissions.push((
                    json!({"kind":"resolver","chain_id":raw.chain_id,
                    "resolver_address":resolver.resolver_address}),
                    "resolver_control",
                ));
            }
        }
        for (scope, power) in permissions {
            let (before, after) = v1_grant_states(
                owner,
                scope,
                power,
                "registrar",
                authority_key,
                "ReadableNameObserved",
            );
            derived.events.push(EventDraft {
                event_kind: "PermissionChanged".to_owned(),
                logical_name_id: Some(authority.logical_name_id.clone()),
                resource_id: Some(authority.resource_id),
                identity_suffix: format!("PermissionChanged:registrar-surface:{node}:{power}"),
                explicit_before: Some(before),
                after_state: super::authority_transition::merge_observation(&common, after),
                state_scope: format!("registrar-surface:{node}:permission:{power}"),
            });
        }
        output.bindings.extend(derived.bindings);
        output.sourced_events.push(SourcedEventBatch {
            source_manifest_id,
            events: derived.events,
        });
    }
    Ok(())
}
