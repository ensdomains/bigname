use super::*;

pub(in crate::ens_v1_unwrapped_authority) async fn load_generic_resolver_event_sources(
    pool: &PgPool,
    chain: &str,
    source_scope: Option<&[AuthorityRawLogSourceScopeTarget]>,
) -> Result<Vec<GenericResolverEventSource>> {
    let scope_ranges = match source_scope {
        Some(source_scope) => {
            let ranges = source_scope
                .iter()
                .filter(|target| is_generic_resolver_event_source_scope_target(target))
                .map(|target| {
                    (
                        target.source_family.clone(),
                        Some(target.effective_from_block),
                        Some(target.effective_to_block),
                    )
                })
                .collect::<Vec<_>>();
            if ranges.is_empty() {
                return Ok(Vec::new());
            }
            ranges
        }
        None => [
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            SOURCE_FAMILY_BASENAMES_BASE_RESOLVER,
        ]
        .into_iter()
        .map(|source_family| (source_family.to_owned(), None, None))
        .collect(),
    };

    let mut sources = Vec::new();
    for source_family in scope_ranges
        .iter()
        .map(|(source_family, _, _)| source_family.as_str())
        .collect::<HashSet<_>>()
    {
        let manifests =
            load_active_manifest_metadata_for_source_family(pool, chain, source_family).await?;
        for manifest in manifests {
            for (_, effective_from_block, effective_to_block) in scope_ranges
                .iter()
                .filter(|(scoped_family, _, _)| scoped_family == source_family)
            {
                sources.push(GenericResolverEventSource {
                    source_manifest_id: manifest.manifest_id,
                    namespace: manifest.namespace.clone(),
                    source_family: manifest.source_family.clone(),
                    manifest_version: manifest.manifest_version,
                    normalizer_version: manifest.normalizer_version.clone(),
                    effective_from_block: *effective_from_block,
                    effective_to_block: *effective_to_block,
                });
            }
        }
    }
    sources.sort_by(|left, right| {
        left.source_family.cmp(&right.source_family).then(
            left.effective_from_block
                .cmp(&right.effective_from_block)
                .then(left.effective_to_block.cmp(&right.effective_to_block))
                .then(left.source_manifest_id.cmp(&right.source_manifest_id)),
        )
    });
    Ok(sources)
}
