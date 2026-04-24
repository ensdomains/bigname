use anyhow::Result;
use bigname_storage::{
    NameSurface, NormalizedEvent, Resource, SurfaceBinding, TokenLineage,
    load_name_surface_including_noncanonical, load_resource_including_noncanonical,
    load_surface_binding_including_noncanonical, load_token_lineage_including_noncanonical,
    upsert_surface_bindings,
};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{
    constants::*,
    normalized::normalized_event,
    types::{NameMetadata, ObservationRef, RegistryNameState, RegistryResourceLink},
    util::event_position_timestamp,
};

pub(super) async fn upsert_surface_bindings_close_before_open(
    pool: &PgPool,
    bindings: &[SurfaceBinding],
) -> Result<()> {
    let closed_bindings = bindings
        .iter()
        .filter(|binding| binding.active_to.is_some())
        .cloned()
        .collect::<Vec<_>>();
    let open_bindings = bindings
        .iter()
        .filter(|binding| binding.active_to.is_none())
        .cloned()
        .collect::<Vec<_>>();

    upsert_surface_bindings(pool, &closed_bindings).await?;
    upsert_surface_bindings(pool, &open_bindings).await?;
    Ok(())
}

pub(super) fn build_resource_events(
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> Vec<NormalizedEvent> {
    let mut events = Vec::new();
    events.push(normalized_event(
        &link.linked_ref,
        Some(state.name.logical_name_id.clone()),
        Some(link.resource_id),
        EVENT_KIND_TOKEN_RESOURCE_LINKED,
        json!({}),
        json!({
            "source_event": "TokenResource",
            "token_id": link.observed_token_id,
            "upstream_resource": link.upstream_resource,
            "resource_id": link.resource_id.to_string(),
            "token_lineage_id": link.token_lineage_id.to_string(),
            "current_token_id": state.token_id,
            "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
        }),
        format!("token-resource-linked:{}", link.upstream_resource),
    ));
    events.push(normalized_event(
        &link.linked_ref,
        Some(state.name.logical_name_id.clone()),
        Some(link.resource_id),
        EVENT_KIND_SURFACE_BOUND,
        json!({}),
        json!({
            "binding_kind": state.binding_kind.as_str(),
            "surface_binding_id": link.surface_binding_id.to_string(),
            "logical_name_id": state.name.logical_name_id,
            "resource_id": link.resource_id.to_string(),
            "upstream_resource": link.upstream_resource,
            "token_id": link.observed_token_id,
            "current_token_id": state.token_id,
        }),
        format!("surface-bound:{}", link.surface_binding_id),
    ));
    if state.status == "registered" {
        events.push(normalized_event(
            &state.first_ref,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_REGISTRATION_GRANTED,
            json!({}),
            json!({
                "authority_kind": "ens_v2_registry",
                "authority_key": format!(
                    "ens-v2-registry:{}:{}:{}",
                    state.first_ref.chain_id, state.registry_contract_instance_id, link.upstream_resource
                ),
                "registrant": state.owner,
                "expiry": state.expiry,
                "labelhash": state.labelhash,
                "token_id": link.observed_token_id,
                "current_token_id": state.token_id,
                "upstream_resource": link.upstream_resource,
                "status": "registered",
                "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
            }),
            format!("registration-granted:{}", link.upstream_resource),
        ));
        events.push(normalized_event(
            &state.first_ref,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_AUTHORITY_TRANSFERRED,
            json!({}),
            json!({
                "owner": state.owner,
                "token_id": link.observed_token_id,
                "current_token_id": state.token_id,
                "upstream_resource": link.upstream_resource,
            }),
            format!("authority-transferred:{}", link.upstream_resource),
        ));
    }
    if let Some(expiry) = state.expiry {
        events.push(normalized_event(
            &state.current_ref,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_EXPIRY_CHANGED,
            json!({}),
            json!({
                "expiry": expiry,
                "token_id": link.observed_token_id,
                "current_token_id": state.token_id,
                "upstream_resource": link.upstream_resource,
            }),
            format!("expiry-current:{}", link.upstream_resource),
        ));
    }
    events
}

pub(super) async fn build_name_surface(
    pool: &PgPool,
    name: &NameMetadata,
    reference: &ObservationRef,
) -> Result<NameSurface> {
    if let Some(existing) =
        load_name_surface_including_noncanonical(pool, &name.logical_name_id).await?
    {
        return Ok(NameSurface {
            logical_name_id: existing.logical_name_id,
            namespace: existing.namespace,
            input_name: existing.input_name,
            canonical_display_name: existing.canonical_display_name,
            normalized_name: existing.normalized_name,
            dns_encoded_name: existing.dns_encoded_name,
            namehash: existing.namehash,
            labelhashes: existing.labelhashes,
            normalizer_version: existing.normalizer_version,
            normalization_warnings: existing.normalization_warnings,
            normalization_errors: existing.normalization_errors,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: json!({
                "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
                "logical_name_id": name.logical_name_id,
            }),
            canonicality_state: reference.canonicality_state,
        });
    }

    Ok(NameSurface {
        logical_name_id: name.logical_name_id.clone(),
        namespace: name.namespace.clone(),
        input_name: name.input_name.clone(),
        canonical_display_name: name.canonical_display_name.clone(),
        normalized_name: name.normalized_name.clone(),
        dns_encoded_name: name.dns_encoded_name.clone(),
        namehash: name.namehash.clone(),
        labelhashes: name.labelhashes.clone(),
        normalizer_version: name.normalizer_version.clone(),
        normalization_warnings: json!([]),
        normalization_errors: json!([]),
        chain_id: reference.chain_id.clone(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
            "logical_name_id": name.logical_name_id,
        }),
        canonicality_state: reference.canonicality_state,
    })
}

pub(super) async fn build_token_lineage(
    pool: &PgPool,
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> Result<TokenLineage> {
    if let Some(existing) =
        load_token_lineage_including_noncanonical(pool, link.token_lineage_id).await?
    {
        return Ok(TokenLineage {
            token_lineage_id: existing.token_lineage_id,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: token_lineage_provenance(state, link),
            canonicality_state: link.linked_ref.canonicality_state,
        });
    }

    Ok(TokenLineage {
        token_lineage_id: link.token_lineage_id,
        chain_id: link.linked_ref.chain_id.clone(),
        block_hash: link.linked_ref.block_hash.clone(),
        block_number: link.linked_ref.block_number,
        provenance: token_lineage_provenance(state, link),
        canonicality_state: link.linked_ref.canonicality_state,
    })
}

pub(super) async fn build_resource(
    pool: &PgPool,
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> Result<Resource> {
    if let Some(existing) = load_resource_including_noncanonical(pool, link.resource_id).await? {
        return Ok(Resource {
            resource_id: existing.resource_id,
            token_lineage_id: existing.token_lineage_id.or(Some(link.token_lineage_id)),
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: resource_provenance(state, link),
            canonicality_state: link.linked_ref.canonicality_state,
        });
    }

    Ok(Resource {
        resource_id: link.resource_id,
        token_lineage_id: Some(link.token_lineage_id),
        chain_id: link.linked_ref.chain_id.clone(),
        block_hash: link.linked_ref.block_hash.clone(),
        block_number: link.linked_ref.block_number,
        provenance: resource_provenance(state, link),
        canonicality_state: link.linked_ref.canonicality_state,
    })
}

pub(super) async fn build_surface_binding(
    pool: &PgPool,
    state: &RegistryNameState,
    link: &RegistryResourceLink,
) -> Result<SurfaceBinding> {
    if let Some(existing) =
        load_surface_binding_including_noncanonical(pool, link.surface_binding_id).await?
    {
        return Ok(SurfaceBinding {
            surface_binding_id: existing.surface_binding_id,
            logical_name_id: existing.logical_name_id,
            resource_id: existing.resource_id,
            binding_kind: existing.binding_kind,
            active_from: existing.active_from,
            active_to: existing.active_to,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: existing.provenance,
            canonicality_state: link.linked_ref.canonicality_state,
        });
    }

    Ok(SurfaceBinding {
        surface_binding_id: link.surface_binding_id,
        logical_name_id: state.name.logical_name_id.clone(),
        resource_id: link.resource_id,
        binding_kind: state.binding_kind,
        active_from: event_position_timestamp(&link.linked_ref),
        active_to: None,
        chain_id: link.linked_ref.chain_id.clone(),
        block_hash: link.linked_ref.block_hash.clone(),
        block_number: link.linked_ref.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
            "binding_kind": state.binding_kind.as_str(),
            "logical_name_id": state.name.logical_name_id,
            "upstream_resource": link.upstream_resource,
            "token_id": link.observed_token_id,
            "current_token_id": state.token_id,
        }),
        canonicality_state: link.linked_ref.canonicality_state,
    })
}

fn token_lineage_provenance(state: &RegistryNameState, link: &RegistryResourceLink) -> Value {
    json!({
        "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
        "chain_id": link.linked_ref.chain_id,
        "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
        "registry_address": state.registry_address,
        "upstream_resource": link.upstream_resource,
        "token_id": link.observed_token_id,
        "current_token_id": state.token_id,
        "logical_name_id": state.name.logical_name_id,
    })
}

fn resource_provenance(state: &RegistryNameState, link: &RegistryResourceLink) -> Value {
    json!({
        "adapter": DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE,
        "chain_id": link.linked_ref.chain_id,
        "registry_contract_instance_id": state.registry_contract_instance_id.to_string(),
        "registry_address": state.registry_address,
        "upstream_resource": link.upstream_resource,
        "token_id": link.observed_token_id,
        "current_token_id": state.token_id,
        "logical_name_id": state.name.logical_name_id,
        "labelhash": state.labelhash,
        "source_family": state.source_family,
        "source_manifest_id": state.source_manifest_id,
        "manifest_version": state.manifest_version,
    })
}
