use super::*;

#[test]
fn coverage_query_binds_the_same_manifest_and_admission_evidence_as_project() {
    let query = resolver_manifest_coverage_sql();
    assert!(query.contains("active.manifest_payload -> 'contracts'"));
    assert!(query.contains("jsonb_typeof(active.manifest_payload -> 'contracts') = 'array'"));
    assert!(query.contains("contracts is absent or is not an array"));
    assert!(query.contains("a contract entry has no address"));
    assert!(query.contains("a contract entry has an invalid start_block"));
    assert!(query.contains("bool_or(applicable) AS applicable"));
    assert!(query.contains("AS applicable_start_block"));
    assert!(query.contains("event.event_kind = 'SourceManifestUpdated'"));
    assert!(query.contains("event.event_kind = 'Upgraded'"));
    assert!(query.contains("upgrade.manifest_payload -> 'resolver_implementations'"));
    assert!(query.contains("= 'ens_v2_resolver_l1'"));
    assert!(query.contains("SELECT DISTINCT ON (event.source_manifest_id)"));
    assert!(query.contains("FULL OUTER JOIN latest_project_manifest_events"));
    assert!(query.contains("manifest.rollout_status IS DISTINCT FROM 'active'"));
    assert!(query.contains("latest.chain_id IS DISTINCT FROM manifest.chain_id"));
    assert!(query.contains("latest.source_family IS DISTINCT FROM manifest.source_family"));
    assert!(
        query.contains("latest.normalizer_version IS DISTINCT FROM manifest.normalizer_version")
    );
    assert!(query.contains("event.block_number AS upgrade_block_number"));
    assert!(query.contains("event.block_hash AS upgrade_block_hash"));
    assert!(query.contains("event.consumer_visibility = 'activated'"));
    assert!(query.contains("event.normalized_event_id DESC"));
    assert!(query.contains("current_project.current_block_number AS target_block_number"));
    assert!(query.contains("marker.input_content_hash = $1"));
    assert!(query.contains("marker.state = 'live'"));
    assert!(query.contains("publication_lineage.block_number = marker.current_block_number"));
    assert!(query.contains("input_phase.redo_from_block_number <= marker.current_block_number"));
    assert!(query.contains("resolver.manifest_id = expected.manifest_id"));
    assert!(query.contains("resolver.manifest_event_id = expected.manifest_event_id"));
    assert!(query.contains("manifest_event.manifest_version = expected.manifest_version"));
    assert!(query.contains("resolver.classification #>> '{upgrade,normalized_event_id}'"));
    assert!(query.contains("resolver.classification #>> '{upgrade,block_number}'"));
    assert!(query.contains("publication.current_block_number >= expected.applicable_start_block"));
    assert!(query.contains("END AS applicable"));
    assert!(query.contains("manifest.rollout_status = 'active'"));
    assert!(query.contains("'ens_v1_resolver_l1', 'ens_v2_resolver_l1'"));
    assert!(query.contains("'basenames_base_resolver'"));
}
