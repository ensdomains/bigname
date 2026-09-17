//! Classification fragments for the declared ENSv1 mirror resolver role
//! (`docs/manifests.md` § ENSv1 mirror resolver declarations). The instance stores no records:
//! it is supported when its exact same-namespace declaration applies, and `record_inventory`
//! serves names bound to it from the mirrored ENSv1 resolver's inventory.

pub(super) const MIRROR_ROLE: &str = "ensv1_mirror_resolver";

pub(super) const DIRECT_MIRROR_DECLARED: &str = r#"
EXISTS (SELECT 1 FROM project_declared_resolver_addresses direct
        WHERE direct.manifest_id = manifest.manifest_id
          AND direct.resolver_address = candidate.resolver_address
          AND direct.classification_role = 'ensv1_mirror_resolver'
          AND direct.source_family = 'ens_v2_resolver_l1')
"#;

/// Published under `declared_summary.classification.mirror`; the registry address is the
/// manifest's validated `correlation_addresses.ens_v1_registry`.
pub(super) const MIRROR_CLASSIFICATION: &str = r#"
CASE WHEN classification_role = 'ensv1_mirror_resolver' THEN jsonb_strip_nulls(jsonb_build_object(
    'mirrored_source_family', 'ens_v1_resolver_l1',
    'mirrored_registry_source_family', 'ens_v1_registry_l1',
    'mirrored_registry_address',
        lower(manifest_payload #>> '{correlation_addresses,ens_v1_registry}')
)) END
"#;
