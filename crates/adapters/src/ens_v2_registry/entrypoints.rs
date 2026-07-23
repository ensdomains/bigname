use super::*;

impl EnsV2RegistryResourceSurfaceSyncSummary {
    pub fn empty(scanned_log_count: usize) -> Self {
        Self {
            scanned_log_count,
            matched_log_count: 0,
            total_name_surface_count: 0,
            total_resource_count: 0,
            total_surface_binding_count: 0,
            total_normalized_event_count: 0,
            total_normalized_event_inserted_count: 0,
            active_discovery_observation_count: 0,
            active_edge_count: 0,
            admitted_edge_count: 0,
            inserted_edge_count: 0,
            deactivated_edge_count: 0,
            discovery_admission_epoch_bump_count: 0,
            by_kind: BTreeMap::new(),
        }
    }

    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            RawLogCanonicalityFilter::IncludeObserved,
            None,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_canonical_only(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            RawLogCanonicalityFilter::CanonicalOnly,
            None,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            RawLogCanonicalityFilter::IncludeObserved,
            None,
            None,
        )
        .await
    }

    pub async fn sync_for_block_hashes_with_source_scope_canonical_only(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v2_registry_resource_surface_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            RawLogCanonicalityFilter::CanonicalOnly,
            None,
            None,
        )
        .await
    }
}

pub async fn sync_ens_v2_registry_resource_surface(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        RawLogCanonicalityFilter::IncludeObserved,
        None,
        None,
    )
    .await
}

pub async fn sync_ens_v2_registry_resource_surface_with_progress(
    pool: &PgPool,
    chain: &str,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        RawLogCanonicalityFilter::IncludeObserved,
        None,
        Some(progress),
    )
    .await
}

pub async fn sync_ens_v2_registry_resource_surface_through_block(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        RawLogCanonicalityFilter::CanonicalOnly,
        Some(target_block_number),
        None,
    )
    .await
}

pub async fn sync_ens_v2_registry_resource_surface_through_block_with_progress(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    sync_ens_v2_registry_resource_surface_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        RawLogCanonicalityFilter::CanonicalOnly,
        Some(target_block_number),
        Some(progress),
    )
    .await
}
