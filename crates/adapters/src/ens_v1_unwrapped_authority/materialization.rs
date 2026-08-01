use super::*;
mod lineage;

pub(super) use lineage::{build_resource, build_token_lineage, build_token_lineage_from_boundary};
#[cfg(test)]
pub(super) fn coalesce_name_surfaces_for_upsert(surfaces: &mut Vec<NameSurface>) {
    let mut seen = HashSet::<String>::new();
    surfaces.retain(|surface| seen.insert(surface.logical_name_id.clone()));
}

pub(super) fn normalize_surface_bindings_for_upsert(
    bindings: &mut Vec<SurfaceBinding>,
) -> Result<()> {
    if bindings.len() < 2 {
        return Ok(());
    }

    coalesce_surface_bindings_for_upsert(bindings)?;
    bindings.sort_by(|left, right| {
        left.logical_name_id
            .cmp(&right.logical_name_id)
            .then_with(|| left.active_from.cmp(&right.active_from))
            .then_with(|| left.block_number.cmp(&right.block_number))
            .then_with(|| left.surface_binding_id.cmp(&right.surface_binding_id))
    });
    drop_same_start_weaker_surface_bindings(bindings);

    let mut group_start = 0usize;
    while group_start < bindings.len() {
        let logical_name_id = bindings[group_start].logical_name_id.clone();
        let mut group_end = group_start + 1;
        while group_end < bindings.len() && bindings[group_end].logical_name_id == logical_name_id {
            group_end += 1;
        }

        close_incoming_binding_group(&mut bindings[group_start..group_end]);
        group_start = group_end;
    }

    Ok(())
}

fn drop_same_start_weaker_surface_bindings(bindings: &mut Vec<SurfaceBinding>) -> usize {
    let mut max_rank_by_boundary = BTreeMap::<(String, OffsetDateTime), u8>::new();
    for binding in bindings
        .iter()
        .filter(|binding| surface_binding_exclusion_applies(binding.canonicality_state))
    {
        let key = (binding.logical_name_id.clone(), binding.active_from);
        let rank = surface_binding_authority_rank(binding);
        max_rank_by_boundary
            .entry(key)
            .and_modify(|max_rank| *max_rank = (*max_rank).max(rank))
            .or_insert(rank);
    }

    let before_len = bindings.len();
    bindings.retain(|binding| {
        if !surface_binding_exclusion_applies(binding.canonicality_state) {
            return true;
        }
        let key = (binding.logical_name_id.clone(), binding.active_from);
        let rank = surface_binding_authority_rank(binding);
        max_rank_by_boundary
            .get(&key)
            .is_none_or(|max_rank| rank >= *max_rank)
    });

    let dropped = before_len - bindings.len();
    if dropped > 0 {
        tracing::warn!(
            adapter = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            shadowed_same_start_surface_binding_count = dropped,
            "dropped lower-precedence same-start surface bindings before upsert"
        );
    }
    dropped
}

fn coalesce_surface_bindings_for_upsert(bindings: &mut Vec<SurfaceBinding>) -> Result<()> {
    let mut by_id = BTreeMap::<Uuid, SurfaceBinding>::new();
    for binding in bindings.drain(..) {
        if let Some(existing) = by_id.get_mut(&binding.surface_binding_id) {
            ensure_same_surface_binding_identity(existing, &binding)?;
            existing.active_to =
                merge_replayed_binding_active_to(existing.active_to, binding.active_to)?;
            existing.canonicality_state = merge_replayed_canonicality(
                existing.canonicality_state,
                binding.canonicality_state,
            );
        } else {
            by_id.insert(binding.surface_binding_id, binding);
        }
    }

    bindings.extend(by_id.into_values());
    Ok(())
}

fn close_incoming_binding_group(bindings: &mut [SurfaceBinding]) {
    let mut previous_excluding_binding = None::<usize>;
    for index in 0..bindings.len() {
        if !surface_binding_exclusion_applies(bindings[index].canonicality_state) {
            continue;
        }

        if let Some(previous_index) = previous_excluding_binding {
            let next_active_from = bindings[index].active_from;
            let previous = &mut bindings[previous_index];
            if previous.active_from < next_active_from
                && previous
                    .active_to
                    .is_none_or(|active_to| active_to > next_active_from)
            {
                previous.active_to = Some(next_active_from);
            }
        }

        previous_excluding_binding = Some(index);
    }
}

fn ensure_same_surface_binding_identity(
    existing: &SurfaceBinding,
    incoming: &SurfaceBinding,
) -> Result<()> {
    if existing.logical_name_id != incoming.logical_name_id
        || existing.resource_id != incoming.resource_id
        || existing.binding_kind != incoming.binding_kind
        || existing.active_from != incoming.active_from
        || existing.chain_id != incoming.chain_id
        || existing.block_hash != incoming.block_hash
        || existing.block_number != incoming.block_number
        || existing.provenance != incoming.provenance
    {
        bail!(
            "surface binding identity mismatch for {}",
            existing.surface_binding_id
        );
    }

    Ok(())
}

fn merge_replayed_binding_active_to(
    current: Option<OffsetDateTime>,
    incoming: Option<OffsetDateTime>,
) -> Result<Option<OffsetDateTime>> {
    match (current, incoming) {
        (Some(current), Some(incoming)) => Ok(Some(current.min(incoming))),
        (Some(current), _) => Ok(Some(current)),
        (None, incoming) => Ok(incoming),
    }
}

fn merge_replayed_canonicality(
    current: CanonicalityState,
    incoming: CanonicalityState,
) -> CanonicalityState {
    match incoming {
        CanonicalityState::Orphaned => CanonicalityState::Orphaned,
        CanonicalityState::Observed => {
            if current == CanonicalityState::Orphaned {
                CanonicalityState::Observed
            } else {
                current
            }
        }
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized => {
            if current == CanonicalityState::Orphaned || incoming.rank() > current.rank() {
                incoming
            } else {
                current
            }
        }
    }
}

pub(super) async fn build_name_surface(
    _pool: &PgPool,
    name: &NameMetadata,
    reference: Option<&ObservationRef>,
) -> Result<Option<NameSurface>> {
    let Some(reference) = reference else {
        return Ok(None);
    };

    Ok(Some(name_surface_from_anchor(
        name,
        &reference.chain_id,
        &reference.block_hash,
        reference.block_number,
        reference.canonicality_state,
        "registrar_name_observation",
    )))
}

pub(super) async fn build_name_surface_from_boundary(
    _pool: &PgPool,
    name: &NameMetadata,
    reference: Option<&BoundaryRef>,
    source_event: &str,
) -> Result<Option<NameSurface>> {
    let Some(reference) = reference else {
        return Ok(None);
    };

    Ok(Some(name_surface_from_anchor(
        name,
        &reference.chain_id,
        &reference.block_hash,
        reference.block_number,
        reference.canonicality_state,
        source_event,
    )))
}

fn name_surface_from_anchor(
    name: &NameMetadata,
    chain_id: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
    source_event: &str,
) -> NameSurface {
    NameSurface {
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
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "logical_name_id": name.logical_name_id,
            "source_event": source_event,
        }),
        canonicality_state,
    }
}

pub(super) async fn build_surface_binding(
    _pool: &PgPool,
    logical_name_id: &str,
    segment: &BindingSegment,
    chain: &str,
) -> Result<SurfaceBinding> {
    Ok(SurfaceBinding {
        surface_binding_id: segment.surface_binding_id,
        logical_name_id: logical_name_id.to_owned(),
        resource_id: segment.authority.resource_id,
        binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
        active_from: segment.active_from,
        active_to: segment.active_to,
        chain_id: chain.to_owned(),
        block_hash: segment.anchor_ref.block_hash.clone(),
        block_number: segment.anchor_ref.block_number,
        provenance: json!({
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "authority_kind": segment.authority.kind.as_str(),
            "authority_key": segment.authority.authority_key,
        }),
        canonicality_state: segment.anchor_ref.canonicality_state,
    })
}

fn surface_binding_authority_rank(binding: &SurfaceBinding) -> u8 {
    match binding
        .provenance
        .get("authority_kind")
        .and_then(Value::as_str)
    {
        Some("wrapper") => 3,
        Some("registrar") => 2,
        Some("registry_only") => 1,
        _ => 0,
    }
}

fn surface_binding_exclusion_applies(canonicality_state: CanonicalityState) -> bool {
    matches!(
        canonicality_state,
        CanonicalityState::Canonical | CanonicalityState::Safe | CanonicalityState::Finalized
    )
}

#[cfg(test)]
mod tests;
