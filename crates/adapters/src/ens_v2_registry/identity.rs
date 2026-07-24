use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bigname_storage::{
    CanonicalityState, NameSurface, NormalizedEvent, Resource, SurfaceBinding, SurfaceBindingKind,
    TokenLineage, load_name_surface_including_noncanonical, load_resource_including_noncanonical,
    load_surface_binding_including_noncanonical, load_token_lineage_including_noncanonical,
    sql_row, upsert_surface_bindings,
};
use serde_json::{Value, json};
use sqlx::{PgPool, types::Uuid};

use super::{
    constants::*,
    normalized::normalized_event,
    types::{NameMetadata, ObservationRef, RegistryNameState, RegistryResourceLink},
    util::event_position_timestamp,
};

mod anchors;
use anchors::stable_row_anchor_for_reobservation;

const EXISTING_BINDING_NAME_CHUNK_SIZE: usize = 5_000;

pub(super) fn coalesce_name_surfaces_for_upsert(surfaces: &mut Vec<NameSurface>) -> Result<()> {
    let mut by_logical_name_id = BTreeMap::<String, NameSurface>::new();
    for surface in surfaces.drain(..) {
        let Some(existing) = by_logical_name_id.get_mut(&surface.logical_name_id) else {
            by_logical_name_id.insert(surface.logical_name_id.clone(), surface);
            continue;
        };
        ensure_same_name_surface_identity(existing, &surface)?;
        if (surface.block_number, surface.block_hash.as_str())
            < (existing.block_number, existing.block_hash.as_str())
        {
            *existing = surface;
        }
    }
    surfaces.extend(by_logical_name_id.into_values());
    Ok(())
}

pub(super) async fn normalize_surface_bindings_for_upsert(
    pool: &PgPool,
    bindings: &mut Vec<SurfaceBinding>,
) -> Result<()> {
    coalesce_surface_bindings(bindings)?;
    sort_and_close_incoming_bindings(bindings);

    let logical_name_ids = bindings
        .iter()
        .map(|binding| binding.logical_name_id.clone())
        .collect::<Vec<_>>();
    let existing = load_readable_surface_bindings(pool, &logical_name_ids).await?;
    trim_incoming_at_existing_successors(bindings, &existing);
    let existing_closures = existing_binding_closures(&existing, bindings);
    bindings.extend(existing_closures);

    coalesce_surface_bindings(bindings)?;
    sort_and_close_incoming_bindings(bindings);
    Ok(())
}

fn ensure_same_name_surface_identity(left: &NameSurface, right: &NameSurface) -> Result<()> {
    if left.namespace != right.namespace
        || left.normalized_name != right.normalized_name
        || left.dns_encoded_name != right.dns_encoded_name
        || left.namehash != right.namehash
        || left.labelhashes != right.labelhashes
        || left.normalization_errors != right.normalization_errors
    {
        bail!(
            "ENSv2 lifecycle produced conflicting name-surface identity for {}",
            left.logical_name_id
        );
    }
    Ok(())
}

fn coalesce_surface_bindings(bindings: &mut Vec<SurfaceBinding>) -> Result<()> {
    let mut by_id = BTreeMap::<Uuid, SurfaceBinding>::new();
    for binding in bindings.drain(..) {
        let Some(existing) = by_id.get_mut(&binding.surface_binding_id) else {
            by_id.insert(binding.surface_binding_id, binding);
            continue;
        };
        ensure_same_surface_binding_identity(existing, &binding)?;
        existing.active_to = match (existing.active_to, binding.active_to) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(active_to), None) | (None, Some(active_to)) => Some(active_to),
            (None, None) => None,
        };
        existing.canonicality_state = existing
            .canonicality_state
            .merge_observation(binding.canonicality_state);
    }
    bindings.extend(by_id.into_values());
    Ok(())
}

fn ensure_same_surface_binding_identity(
    left: &SurfaceBinding,
    right: &SurfaceBinding,
) -> Result<()> {
    if left.logical_name_id != right.logical_name_id
        || left.resource_id != right.resource_id
        || left.binding_kind != right.binding_kind
        || left.active_from != right.active_from
        || left.chain_id != right.chain_id
        || left.block_hash != right.block_hash
        || left.block_number != right.block_number
        || left.provenance != right.provenance
    {
        bail!(
            "ENSv2 lifecycle produced conflicting surface-binding identity for {}",
            left.surface_binding_id
        );
    }
    Ok(())
}

fn sort_and_close_incoming_bindings(bindings: &mut [SurfaceBinding]) {
    bindings.sort_by(|left, right| {
        left.logical_name_id
            .cmp(&right.logical_name_id)
            .then_with(|| left.active_from.cmp(&right.active_from))
            .then_with(|| left.block_number.cmp(&right.block_number))
            .then_with(|| left.surface_binding_id.cmp(&right.surface_binding_id))
    });

    let mut previous = None::<usize>;
    for index in 0..bindings.len() {
        if !binding_excludes_overlap(bindings[index].canonicality_state) {
            continue;
        }
        if let Some(previous_index) = previous
            && bindings[previous_index].logical_name_id == bindings[index].logical_name_id
        {
            let successor_start = bindings[index].active_from;
            let predecessor = &mut bindings[previous_index];
            if predecessor.active_from < successor_start
                && predecessor
                    .active_to
                    .is_none_or(|active_to| active_to > successor_start)
            {
                predecessor.active_to = Some(successor_start);
            }
        }
        previous = Some(index);
    }
}

fn trim_incoming_at_existing_successors(
    incoming: &mut [SurfaceBinding],
    existing: &[SurfaceBinding],
) {
    for binding in incoming
        .iter_mut()
        .filter(|binding| binding_excludes_overlap(binding.canonicality_state))
    {
        let close_at = existing
            .iter()
            .filter(|stored| {
                stored.logical_name_id == binding.logical_name_id
                    && stored.surface_binding_id != binding.surface_binding_id
                    && binding.active_from < stored.active_from
                    && binding
                        .active_to
                        .is_none_or(|active_to| active_to > stored.active_from)
            })
            .map(|stored| stored.active_from)
            .min();
        if let Some(close_at) = close_at {
            binding.active_to = Some(close_at);
        }
    }
}

fn existing_binding_closures(
    existing: &[SurfaceBinding],
    incoming: &[SurfaceBinding],
) -> Vec<SurfaceBinding> {
    existing
        .iter()
        .filter_map(|stored| {
            let close_at = incoming
                .iter()
                .filter(|candidate| {
                    binding_excludes_overlap(candidate.canonicality_state)
                        && candidate.logical_name_id == stored.logical_name_id
                        && candidate.surface_binding_id != stored.surface_binding_id
                        && stored.active_from < candidate.active_from
                        && stored
                            .active_to
                            .is_none_or(|active_to| active_to > candidate.active_from)
                })
                .map(|candidate| candidate.active_from)
                .min()?;
            let mut closure = stored.clone();
            closure.active_to = Some(close_at);
            Some(closure)
        })
        .collect()
}

async fn load_readable_surface_bindings(
    pool: &PgPool,
    logical_name_ids: &[String],
) -> Result<Vec<SurfaceBinding>> {
    let mut output = Vec::new();
    for chunk in logical_name_ids.chunks(EXISTING_BINDING_NAME_CHUNK_SIZE) {
        let rows = sqlx::query(
            r#"
            SELECT
                surface_binding_id,
                logical_name_id,
                resource_id,
                binding_kind,
                active_from,
                active_to,
                chain_id,
                block_hash,
                block_number,
                provenance,
                canonicality_state::TEXT AS canonicality_state
            FROM surface_bindings
            WHERE logical_name_id = ANY($1::TEXT[])
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            ORDER BY logical_name_id, active_from, surface_binding_id
            "#,
        )
        .bind(chunk)
        .fetch_all(pool)
        .await
        .context("failed to load existing ENSv2 lifecycle surface bindings")?;
        for row in rows {
            output.push(SurfaceBinding {
                surface_binding_id: sql_row::get(&row, "surface_binding_id")?,
                logical_name_id: sql_row::get(&row, "logical_name_id")?,
                resource_id: sql_row::get(&row, "resource_id")?,
                binding_kind: SurfaceBindingKind::parse(&sql_row::get::<String>(
                    &row,
                    "binding_kind",
                )?)?,
                active_from: sql_row::get(&row, "active_from")?,
                active_to: sql_row::get(&row, "active_to")?,
                chain_id: sql_row::get(&row, "chain_id")?,
                block_hash: sql_row::get(&row, "block_hash")?,
                block_number: sql_row::get(&row, "block_number")?,
                provenance: sql_row::get(&row, "provenance")?,
                canonicality_state: CanonicalityState::parse(&sql_row::get::<String>(
                    &row,
                    "canonicality_state",
                )?)?,
            });
        }
    }
    Ok(output)
}

const fn binding_excludes_overlap(state: CanonicalityState) -> bool {
    matches!(
        state,
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
    )
}

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
            "current_token_id": link.observed_token_id,
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
            "current_token_id": link.observed_token_id,
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
                "expiry": link.observed_expiry,
                "labelhash": state.labelhash,
                "token_id": link.observed_token_id,
                "current_token_id": link.observed_token_id,
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
                "current_token_id": link.observed_token_id,
                "upstream_resource": link.upstream_resource,
            }),
            format!("authority-transferred:{}", link.upstream_resource),
        ));
    }
    if let Some(expiry) = link.observed_expiry {
        events.push(normalized_event(
            &state.first_ref,
            Some(state.name.logical_name_id.clone()),
            Some(link.resource_id),
            EVENT_KIND_EXPIRY_CHANGED,
            json!({}),
            json!({
                "expiry": expiry,
                "token_id": link.observed_token_id,
                "current_token_id": link.observed_token_id,
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
        let (chain_id, block_hash, block_number) = stable_row_anchor_for_reobservation(
            existing.canonicality_state,
            &existing.chain_id,
            &existing.block_hash,
            existing.block_number,
            reference,
        );
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
            chain_id,
            block_hash,
            block_number,
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
        let (chain_id, block_hash, block_number) = stable_row_anchor_for_reobservation(
            existing.canonicality_state,
            &existing.chain_id,
            &existing.block_hash,
            existing.block_number,
            &link.linked_ref,
        );
        return Ok(TokenLineage {
            token_lineage_id: existing.token_lineage_id,
            chain_id,
            block_hash,
            block_number,
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
        let (chain_id, block_hash, block_number) = stable_row_anchor_for_reobservation(
            existing.canonicality_state,
            &existing.chain_id,
            &existing.block_hash,
            existing.block_number,
            &link.linked_ref,
        );
        return Ok(Resource {
            resource_id: existing.resource_id,
            token_lineage_id: existing.token_lineage_id.or(Some(link.token_lineage_id)),
            chain_id,
            block_hash,
            block_number,
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
        let reanchors_orphan = existing.canonicality_state == CanonicalityState::Orphaned;
        let (chain_id, block_hash, block_number) = stable_row_anchor_for_reobservation(
            existing.canonicality_state,
            &existing.chain_id,
            &existing.block_hash,
            existing.block_number,
            &link.linked_ref,
        );
        return Ok(SurfaceBinding {
            surface_binding_id: existing.surface_binding_id,
            logical_name_id: existing.logical_name_id,
            resource_id: existing.resource_id,
            binding_kind: existing.binding_kind,
            active_from: existing.active_from,
            active_to: (!reanchors_orphan).then_some(existing.active_to).flatten(),
            chain_id,
            block_hash,
            block_number,
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
            "current_token_id": link.observed_token_id,
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
        "current_token_id": link.observed_token_id,
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
        "current_token_id": link.observed_token_id,
        "logical_name_id": state.name.logical_name_id,
        "labelhash": state.labelhash,
        "source_family": state.source_family,
        "source_manifest_id": state.source_manifest_id,
        "manifest_version": state.manifest_version,
    })
}
