use anyhow::Result;
use sqlx::PgPool;

use super::{
    DiscoveryEdgeMutation, EnsV1SubregistryDiscoverySyncSummary, checkpoint,
    sync_ens_v1_subregistry_discovery_with_scope,
};

pub async fn sync_ens_v1_subregistry_discovery(
    pool: &PgPool,
    chain: &str,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    sync_ens_v1_subregistry_discovery_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        DiscoveryEdgeMutation::Reconcile,
        None,
        None,
        None,
        checkpoint::PAGE_LIMIT,
        None,
        &mut None,
    )
    .await
    .map(|(summary, _, _)| summary)
}

/// Reconcile the complete retained ENSv1/Basenames registry source through an
/// inclusive live target. Closed historical emitter intervals are included so
/// a winning reorg can restore a canonical descendant branch. Facts after the
/// target are not replay inputs, and existing non-orphaned later edges remain
/// active.
pub async fn sync_ens_v1_subregistry_discovery_through_block(
    pool: &PgPool,
    chain: &str,
    through_block_number: i64,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    sync_ens_v1_subregistry_discovery_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        DiscoveryEdgeMutation::Reconcile,
        Some(through_block_number),
        None,
        None,
        checkpoint::PAGE_LIMIT,
        None,
        &mut None,
    )
    .await
    .map(|(summary, _, _)| summary)
}

/// Target-bounded complete-source reconciliation which accepts absence only
/// while the watched registry set remains at the epoch used by the caller's
/// current-generation coverage proof.
pub async fn sync_ens_v1_subregistry_discovery_through_block_with_expected_admission_epoch(
    pool: &PgPool,
    chain: &str,
    through_block_number: i64,
    expected_admission_epoch: i64,
) -> Result<EnsV1SubregistryDiscoverySyncSummary> {
    sync_ens_v1_subregistry_discovery_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        DiscoveryEdgeMutation::Reconcile,
        Some(through_block_number),
        Some(expected_admission_epoch),
        None,
        checkpoint::PAGE_LIMIT,
        None,
        &mut None,
    )
    .await
    .map(|(summary, _, _)| summary)
}

impl EnsV1SubregistryDiscoverySyncSummary {
    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            DiscoveryEdgeMutation::Reconcile,
            None,
            None,
            None,
            checkpoint::PAGE_LIMIT,
            None,
            &mut None,
        )
        .await
        .map(|(summary, _, _)| summary)
    }

    pub async fn sync_for_block_hashes_with_source_scope_without_discovery_reconciliation(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            DiscoveryEdgeMutation::Skip,
            None,
            None,
            None,
            checkpoint::PAGE_LIMIT,
            None,
            &mut None,
        )
        .await
        .map(|(summary, _, _)| summary)
    }

    pub async fn sync_for_block_hashes_without_discovery_reconciliation(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_ens_v1_subregistry_discovery_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            None,
            DiscoveryEdgeMutation::Skip,
            None,
            None,
            None,
            checkpoint::PAGE_LIMIT,
            None,
            &mut None,
        )
        .await
        .map(|(summary, _, _)| summary)
    }
}
