use super::*;

pub(in crate::ens_v1_unwrapped_authority) async fn build_token_lineage(
    pool: &PgPool,
    token_lineage_id: Uuid,
    chain: &str,
    reference: &ObservationRef,
    provenance: serde_json::Value,
) -> Result<TokenLineage> {
    build_token_lineage_for_ref(
        pool,
        token_lineage_id,
        chain,
        &reference.block_hash,
        reference.block_number,
        reference.canonicality_state,
        provenance,
    )
    .await
}

pub(in crate::ens_v1_unwrapped_authority) async fn build_token_lineage_from_boundary(
    pool: &PgPool,
    token_lineage_id: Uuid,
    chain: &str,
    reference: &BoundaryRef,
    provenance: serde_json::Value,
) -> Result<TokenLineage> {
    build_token_lineage_for_ref(
        pool,
        token_lineage_id,
        chain,
        &reference.block_hash,
        reference.block_number,
        reference.canonicality_state,
        provenance,
    )
    .await
}

async fn build_token_lineage_for_ref(
    pool: &PgPool,
    token_lineage_id: Uuid,
    chain: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: CanonicalityState,
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
            canonicality_state,
        });
    }

    Ok(TokenLineage {
        token_lineage_id,
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        provenance,
        canonicality_state,
    })
}

pub(in crate::ens_v1_unwrapped_authority) async fn build_resource(
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
