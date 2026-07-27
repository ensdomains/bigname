/// Declared compatibility boundary for one full-corpus startup adapter.
///
/// Adapter authors must increment the affected family's `*_DECLARATION`
/// constant whenever decoding, derivation, discovery, identity materialization,
/// or normalized-event semantics change. Do not use a build SHA here:
/// unrelated code-only releases must preserve completed startup work.
///
/// A consumer's checkpoint version also composes the declared versions of
/// adapter outputs it reads. That makes a producer bump invalidate every known
/// downstream startup checkpoint automatically.
/// `scripts/check-startup-adapter-versions` enforces bumps for mapped
/// family-owned production files; changes to unmapped shared helpers still
/// require reviewer judgment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartupAdapterVersion {
    pub adapter: &'static str,
    pub declared_version: i64,
    pub semantic_version: i64,
}

const VERSION_COMPONENT_BASE: i64 = 1_000_000;

impl StartupAdapterVersion {
    const fn new(
        adapter: &'static str,
        declared_version: i64,
        dependency_versions: &[i64],
    ) -> Self {
        Self {
            adapter,
            declared_version,
            semantic_version: compose_checkpoint_version(declared_version, dependency_versions),
        }
    }
}

const fn compose_checkpoint_version(declared_version: i64, dependency_versions: &[i64]) -> i64 {
    assert!(
        declared_version > 0 && declared_version < VERSION_COMPONENT_BASE,
        "startup adapter declarations must be in 1..1_000_000"
    );
    let mut composed = declared_version;
    let mut index = 0;
    while index < dependency_versions.len() {
        let dependency = dependency_versions[index];
        assert!(
            dependency > 0 && dependency < VERSION_COMPONENT_BASE,
            "startup adapter dependency versions must be in 1..1_000_000"
        );
        assert!(
            composed <= (i64::MAX - dependency) / VERSION_COMPONENT_BASE,
            "startup adapter version composition overflow"
        );
        composed = composed * VERSION_COMPONENT_BASE + dependency;
        index += 1;
    }
    composed
}

const ENS_V1_REVERSE_CLAIM_STARTUP_VERSION_DECLARATION: i64 = 2;
const BLOCK_DERIVED_NORMALIZED_EVENTS_STARTUP_VERSION_DECLARATION: i64 = 1;
const ENS_V1_SUBREGISTRY_DISCOVERY_STARTUP_VERSION_DECLARATION: i64 = 2;
const ENS_V1_UNWRAPPED_AUTHORITY_STARTUP_VERSION_DECLARATION: i64 = 3;
const ENS_V2_REGISTRY_RESOURCE_SURFACE_STARTUP_VERSION_DECLARATION: i64 = 2;
const ENS_V2_REGISTRAR_STARTUP_VERSION_DECLARATION: i64 = 2;
const ENS_V2_RESOLVER_STARTUP_VERSION_DECLARATION: i64 = 2;
const ENS_V2_PERMISSIONS_STARTUP_VERSION_DECLARATION: i64 = 2;

pub const ENS_V1_REVERSE_CLAIM_STARTUP_VERSION: StartupAdapterVersion = StartupAdapterVersion::new(
    "ens_v1_reverse_claim",
    ENS_V1_REVERSE_CLAIM_STARTUP_VERSION_DECLARATION,
    &[],
);
pub const ENS_V1_SUBREGISTRY_DISCOVERY_STARTUP_VERSION: StartupAdapterVersion =
    StartupAdapterVersion::new(
        "ens_v1_subregistry_discovery",
        ENS_V1_SUBREGISTRY_DISCOVERY_STARTUP_VERSION_DECLARATION,
        &[],
    );
pub const ENS_V1_UNWRAPPED_AUTHORITY_STARTUP_VERSION: StartupAdapterVersion =
    StartupAdapterVersion::new(
        "ens_v1_unwrapped_authority",
        ENS_V1_UNWRAPPED_AUTHORITY_STARTUP_VERSION_DECLARATION,
        &[ENS_V1_REVERSE_CLAIM_STARTUP_VERSION_DECLARATION],
    );
pub const ENS_V2_REGISTRY_RESOURCE_SURFACE_STARTUP_VERSION: StartupAdapterVersion =
    StartupAdapterVersion::new(
        "ens_v2_registry_resource_surface",
        ENS_V2_REGISTRY_RESOURCE_SURFACE_STARTUP_VERSION_DECLARATION,
        &[],
    );
pub const ENS_V2_REGISTRAR_STARTUP_VERSION: StartupAdapterVersion = StartupAdapterVersion::new(
    "ens_v2_registrar",
    ENS_V2_REGISTRAR_STARTUP_VERSION_DECLARATION,
    &[ENS_V2_REGISTRY_RESOURCE_SURFACE_STARTUP_VERSION_DECLARATION],
);
pub const ENS_V2_RESOLVER_STARTUP_VERSION: StartupAdapterVersion = StartupAdapterVersion::new(
    "ens_v2_resolver",
    ENS_V2_RESOLVER_STARTUP_VERSION_DECLARATION,
    &[ENS_V2_REGISTRY_RESOURCE_SURFACE_STARTUP_VERSION_DECLARATION],
);
pub const ENS_V2_PERMISSIONS_STARTUP_VERSION: StartupAdapterVersion = StartupAdapterVersion::new(
    "ens_v2_permissions",
    ENS_V2_PERMISSIONS_STARTUP_VERSION_DECLARATION,
    &[BLOCK_DERIVED_NORMALIZED_EVENTS_STARTUP_VERSION_DECLARATION],
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ens_v2_registry_semantics_are_part_of_downstream_checkpoint_versions() {
        let next_registry_version =
            ENS_V2_REGISTRY_RESOURCE_SURFACE_STARTUP_VERSION_DECLARATION + 1;
        let registrar_after_registry_bump = StartupAdapterVersion::new(
            "ens_v2_registrar",
            ENS_V2_REGISTRAR_STARTUP_VERSION_DECLARATION,
            &[next_registry_version],
        );
        let resolver_after_registry_bump = StartupAdapterVersion::new(
            "ens_v2_resolver",
            ENS_V2_RESOLVER_STARTUP_VERSION_DECLARATION,
            &[next_registry_version],
        );

        assert_ne!(
            registrar_after_registry_bump.semantic_version,
            ENS_V2_REGISTRAR_STARTUP_VERSION.semantic_version,
            "a registry bump must invalidate registrar startup reuse"
        );
        assert_ne!(
            resolver_after_registry_bump.semantic_version,
            ENS_V2_RESOLVER_STARTUP_VERSION.semantic_version,
            "a registry bump must invalidate resolver startup reuse"
        );
    }

    #[test]
    fn block_derived_semantics_are_part_of_permissions_checkpoint_version() {
        let permissions_after_producer_bump = StartupAdapterVersion::new(
            "ens_v2_permissions",
            ENS_V2_PERMISSIONS_STARTUP_VERSION_DECLARATION,
            &[BLOCK_DERIVED_NORMALIZED_EVENTS_STARTUP_VERSION_DECLARATION + 1],
        );

        assert_ne!(
            permissions_after_producer_bump.semantic_version,
            ENS_V2_PERMISSIONS_STARTUP_VERSION.semantic_version,
            "a block-derived producer bump must invalidate permissions startup reuse"
        );
    }

    #[test]
    fn ens_v1_reverse_semantics_are_part_of_unwrapped_checkpoint_version() {
        let unwrapped_after_reverse_bump = StartupAdapterVersion::new(
            "ens_v1_unwrapped_authority",
            ENS_V1_UNWRAPPED_AUTHORITY_STARTUP_VERSION_DECLARATION,
            &[ENS_V1_REVERSE_CLAIM_STARTUP_VERSION_DECLARATION + 1],
        );

        assert_ne!(
            unwrapped_after_reverse_bump.semantic_version,
            ENS_V1_UNWRAPPED_AUTHORITY_STARTUP_VERSION.semantic_version,
            "a reverse-claim bump must invalidate unwrapped-authority startup reuse"
        );
    }
}
