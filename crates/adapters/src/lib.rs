//! ENSv1, ENSv2, and Basenames event normalization adapters.

#[cfg(all(test, feature = "legacy"))]
use std::sync::{Arc, OnceLock};

#[cfg(feature = "legacy")]
mod adapter_manifest;
#[cfg(feature = "legacy")]
mod block_derived_normalized_events;
#[cfg(feature = "legacy")]
mod ens_v1_reverse_claim;
#[cfg(feature = "legacy")]
mod ens_v1_unwrapped_authority;
#[cfg(feature = "legacy")]
mod ens_v2_common;
#[cfg(feature = "legacy")]
mod ens_v2_permissions;
#[cfg(feature = "legacy")]
mod ens_v2_registrar;
#[cfg(feature = "legacy")]
mod ens_v2_registry;
#[cfg(feature = "legacy")]
mod ens_v2_resolver;
#[cfg(any(feature = "legacy", feature = "schema-v2"))]
#[cfg_attr(not(feature = "legacy"), allow(dead_code))]
mod evm_abi;
#[cfg(feature = "legacy")]
mod manifest_normalized_events;
#[cfg(feature = "legacy")]
mod normalized_event_support;
#[cfg(feature = "legacy")]
mod registry_migration_cache;
#[cfg(feature = "schema-v2")]
pub mod schema_v2;

#[cfg(feature = "legacy")]
pub use block_derived_normalized_events::{
    BlockDerivedNormalizedEventKindSyncSummary, BlockDerivedNormalizedEventSyncSummary,
    sync_block_derived_normalized_events,
    sync_block_derived_normalized_events_with_scanned_log_count,
};
#[cfg(feature = "legacy")]
pub use ens_v1_reverse_claim::{
    EnsV1ReverseClaimKindSyncSummary, EnsV1ReverseClaimSyncSummary, sync_ens_v1_reverse_claim,
    sync_ens_v1_reverse_claim_range,
};
#[cfg(feature = "legacy")]
pub use ens_v1_unwrapped_authority::{
    EnsV1TextRecordChange, EnsV1UnwrappedAuthoritySyncSummary, decode_ens_v1_text_record_change,
    sync_ens_v1_unwrapped_authority, sync_ens_v1_unwrapped_authority_through_block,
};
#[cfg(feature = "legacy")]
pub use ens_v2_permissions::{
    EnsV2PermissionsKindSyncSummary, EnsV2PermissionsSyncSummary, sync_ens_v2_permissions,
    sync_ens_v2_permissions_through_block,
};
#[cfg(feature = "legacy")]
pub use ens_v2_registrar::{
    EnsV2RegistrarKindSyncSummary, EnsV2RegistrarSyncSummary, sync_ens_v2_registrar,
    sync_ens_v2_registrar_through_block,
};
#[cfg(feature = "legacy")]
pub use ens_v2_registry::{
    EnsV2RegistryResourceSurfaceSyncSummary, sync_ens_v2_registry_resource_surface,
    sync_ens_v2_registry_resource_surface_live_poll,
    sync_ens_v2_registry_resource_surface_through_block,
};
#[cfg(feature = "legacy")]
pub use ens_v2_resolver::{
    EnsV2ResolverKindSyncSummary, EnsV2ResolverSyncSummary, sync_ens_v2_resolver,
    sync_ens_v2_resolver_through_block,
};
#[cfg(feature = "legacy")]
pub use manifest_normalized_events::{
    ManifestNormalizedEventKindSyncSummary, ManifestNormalizedEventSyncSummary,
    sync_manifest_normalized_events,
};
#[cfg(feature = "schema-v2")]
pub use schema_v2::{
    AddressAdmissionInput, BatchInput as SchemaV2BatchInput, BatchOutput as SchemaV2BatchOutput,
    DiscoveryRuleInput, ManifestInput as SchemaV2ManifestInput,
    NormalizedEvent as SchemaV2NormalizedEvent, PriorEventInput,
    RawLogInput as SchemaV2RawLogInput, interpret_schema_v2_batch,
};
#[cfg(all(test, feature = "legacy"))]
static TEST_DB_SEMAPHORE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

#[cfg(all(test, feature = "legacy"))]
pub(crate) async fn acquire_test_db_permit() -> tokio::sync::OwnedSemaphorePermit {
    TEST_DB_SEMAPHORE
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone()
        .acquire_owned()
        .await
        .expect("test DB semaphore must stay open")
}
