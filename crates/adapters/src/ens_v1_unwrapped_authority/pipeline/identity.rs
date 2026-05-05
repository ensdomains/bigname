use super::*;

pub(super) async fn ensure_binding_authority_identity_rows(
    pool: &PgPool,
    token_lineages: &mut Vec<TokenLineage>,
    token_lineage_ids: &mut HashSet<Uuid>,
    resources: &mut Vec<Resource>,
    resource_ids: &mut HashSet<Uuid>,
    logical_name_id: &str,
    segment: &BindingSegment,
) -> Result<()> {
    let mut provenance = Map::from_iter([
        (
            "adapter".to_owned(),
            Value::String(DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY.to_owned()),
        ),
        (
            "authority_kind".to_owned(),
            Value::String(segment.authority.kind.as_str().to_owned()),
        ),
        (
            "authority_key".to_owned(),
            Value::String(segment.authority.authority_key.clone()),
        ),
        (
            "logical_name_id".to_owned(),
            Value::String(logical_name_id.to_owned()),
        ),
        (
            "source_event".to_owned(),
            Value::String("surface_binding_authority".to_owned()),
        ),
        (
            "binding_source_family".to_owned(),
            Value::String(segment.authority.binding_source_family.clone()),
        ),
        (
            "binding_manifest_version".to_owned(),
            Value::Number(segment.authority.binding_manifest_version.into()),
        ),
        (
            "binding_manifest_id".to_owned(),
            Value::Number(segment.authority.binding_manifest_id.into()),
        ),
    ]);
    if segment.authority.kind == AuthorityKind::Registrar {
        if let Some(labelhash) =
            registrar_labelhash_from_authority_key(&segment.authority.authority_key)
        {
            provenance.insert("labelhash".to_owned(), Value::String(labelhash));
        }
        if let Some(active_to) = segment.active_to {
            provenance.insert(
                "released_at".to_owned(),
                Value::Number(active_to.unix_timestamp().into()),
            );
            if let Some(expiry) = active_to
                .unix_timestamp()
                .checked_sub(ENS_GRACE_PERIOD_SECS)
            {
                provenance.insert("expiry".to_owned(), Value::Number(expiry.into()));
            }
        }
    }
    let provenance = Value::Object(provenance);

    if let Some(token_lineage_id) = segment.authority.token_lineage_id {
        push_token_lineage_once(
            token_lineages,
            token_lineage_ids,
            build_token_lineage_from_boundary(
                pool,
                token_lineage_id,
                &segment.anchor_ref.chain_id,
                &segment.anchor_ref,
                provenance.clone(),
            )
            .await?,
        );
    }

    push_resource_once(
        resources,
        resource_ids,
        build_resource(
            pool,
            segment.authority.resource_id,
            segment.authority.token_lineage_id,
            &segment.anchor_ref.chain_id,
            &segment.anchor_ref,
            provenance,
        )
        .await?,
    );

    Ok(())
}

pub(super) fn push_token_lineage_once(
    token_lineages: &mut Vec<TokenLineage>,
    token_lineage_ids: &mut HashSet<Uuid>,
    token_lineage: TokenLineage,
) {
    if token_lineage_ids.insert(token_lineage.token_lineage_id) {
        token_lineages.push(token_lineage);
    }
}

pub(super) fn push_resource_once(
    resources: &mut Vec<Resource>,
    resource_ids: &mut HashSet<Uuid>,
    resource: Resource,
) {
    if resource_ids.insert(resource.resource_id) {
        resources.push(resource);
    }
}
