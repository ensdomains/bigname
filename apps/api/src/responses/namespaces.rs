fn build_namespace_metadata_response(
    namespace: String,
    snapshot: NamespaceManifestSnapshot,
) -> NamespaceMetadataResponse {
    let manifest_versions = snapshot
        .manifests
        .iter()
        .map(ManifestVersionRef::from)
        .collect::<Vec<_>>();

    NamespaceMetadataResponse {
        data: NamespaceMetadataData { namespace },
        declared_state: NamespaceMetadataDeclaredState {
            active_manifest_count: snapshot.manifests.len(),
            active_source_families: collect_unique(
                snapshot
                    .manifests
                    .iter()
                    .map(|manifest| manifest.source_family.clone()),
            ),
            chains: collect_unique(
                snapshot
                    .manifests
                    .iter()
                    .map(|manifest| manifest.chain.clone()),
            ),
            normalizer_versions: collect_unique(
                snapshot
                    .manifests
                    .iter()
                    .map(|manifest| manifest.normalizer_version.clone()),
            ),
        },
        verified_state: None,
        provenance: NamespaceMetadataProvenance {
            normalized_event_ids: Vec::new(),
            raw_fact_refs: Vec::new(),
            manifest_versions,
            execution_trace_id: None,
            derivation_kind: "declared".to_owned(),
        },
        coverage: CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["source_manifests".to_owned()],
            enumeration_basis: "active manifests for the requested namespace".to_owned(),
            unsupported_reason: None,
        },
        chain_positions: BTreeMap::new(),
        consistency: "head".to_owned(),
        last_updated: snapshot.last_updated,
    }
}

fn build_namespace_manifests_response(
    namespace: String,
    snapshot: NamespaceManifestSnapshot,
) -> NamespaceManifestsResponse {
    let manifests = snapshot
        .manifests
        .into_iter()
        .map(Into::into)
        .collect::<Vec<NamespaceManifestEntry>>();
    let manifest_versions = manifests.iter().map(ManifestVersionRef::from).collect();

    NamespaceManifestsResponse {
        data: NamespaceManifestsData { namespace },
        declared_state: NamespaceManifestsDeclaredState { manifests },
        verified_state: None,
        provenance: NamespaceManifestsProvenance {
            normalized_event_ids: Vec::new(),
            raw_fact_refs: Vec::new(),
            manifest_versions,
            execution_trace_id: None,
            derivation_kind: "declared".to_owned(),
        },
        coverage: CoverageResponse {
            status: "full".to_owned(),
            exhaustiveness: "authoritative".to_owned(),
            source_classes_considered: vec!["source_manifests".to_owned()],
            enumeration_basis: "active manifests for the requested namespace".to_owned(),
            unsupported_reason: None,
        },
        chain_positions: BTreeMap::new(),
        consistency: "head".to_owned(),
        last_updated: snapshot.last_updated,
    }
}

