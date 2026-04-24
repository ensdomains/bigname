use super::*;

pub(super) async fn build_name_surface(
    pool: &PgPool,
    name: &NameMetadata,
    reference: Option<&ObservationRef>,
) -> Result<Option<NameSurface>> {
    let Some(reference) = reference else {
        return Ok(None);
    };

    if let Some(existing) =
        load_name_surface_including_noncanonical(pool, &name.logical_name_id).await?
    {
        return Ok(Some(NameSurface {
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
                "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
                "logical_name_id": name.logical_name_id,
            }),
            canonicality_state: reference.canonicality_state,
        }));
    }

    Ok(Some(NameSurface {
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
            "adapter": DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
            "logical_name_id": name.logical_name_id,
            "source_event": "registrar_name_observation",
        }),
        canonicality_state: reference.canonicality_state,
    }))
}

pub(super) async fn build_token_lineage(
    pool: &PgPool,
    token_lineage_id: Uuid,
    chain: &str,
    reference: &ObservationRef,
    provenance: serde_json::Value,
) -> Result<TokenLineage> {
    if let Some(existing) =
        load_token_lineage_including_noncanonical(pool, token_lineage_id).await?
    {
        return Ok(TokenLineage {
            token_lineage_id: existing.token_lineage_id,
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance,
            canonicality_state: reference.canonicality_state,
        });
    }

    Ok(TokenLineage {
        token_lineage_id,
        chain_id: chain.to_owned(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance,
        canonicality_state: reference.canonicality_state,
    })
}

pub(super) async fn build_resource(
    pool: &PgPool,
    resource_id: Uuid,
    token_lineage_id: Option<Uuid>,
    chain: &str,
    reference: &BoundaryRef,
    provenance: serde_json::Value,
) -> Result<Resource> {
    if let Some(existing) = load_resource_including_noncanonical(pool, resource_id).await? {
        return Ok(Resource {
            resource_id: existing.resource_id,
            token_lineage_id: existing.token_lineage_id.or(token_lineage_id),
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance,
            canonicality_state: reference.canonicality_state,
        });
    }

    Ok(Resource {
        resource_id,
        token_lineage_id,
        chain_id: chain.to_owned(),
        block_hash: reference.block_hash.clone(),
        block_number: reference.block_number,
        provenance,
        canonicality_state: reference.canonicality_state,
    })
}

pub(super) async fn build_surface_binding(
    pool: &PgPool,
    logical_name_id: &str,
    segment: &BindingSegment,
    chain: &str,
) -> Result<SurfaceBinding> {
    if let Some(existing) =
        load_surface_binding_including_noncanonical(pool, segment.surface_binding_id).await?
    {
        return Ok(SurfaceBinding {
            surface_binding_id: existing.surface_binding_id,
            logical_name_id: existing.logical_name_id,
            resource_id: existing.resource_id,
            binding_kind: existing.binding_kind,
            active_from: existing.active_from,
            active_to: segment.active_to.or(existing.active_to),
            chain_id: existing.chain_id,
            block_hash: existing.block_hash,
            block_number: existing.block_number,
            provenance: existing.provenance,
            canonicality_state: segment.anchor_ref.canonicality_state,
        });
    }

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
