use anyhow::Result;
use bigname_adapters::{
    EnsV1UnwrappedAuthoritySyncSummary, EnsV2RegistrarSyncSummary, StartupAdapterProgress,
};

type SourceScope<'a> = &'a [(String, String, i64, i64)];

pub(super) async fn sync_ens_v1_unwrapped_authority_for_scope(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<SourceScope<'_>>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    match (source_scope, progress.as_deref_mut()) {
        (Some(source_scope), Some(progress)) => {
            EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes_with_source_scope_and_progress(
                pool,
                chain,
                block_hashes,
                source_scope,
                progress,
            )
            .await
        }
        (Some(source_scope), None) => {
            EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await
        }
        (None, Some(progress)) => {
            EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes_with_progress(
                pool,
                chain,
                block_hashes,
                progress,
            )
            .await
        }
        (None, None) => {
            EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(pool, chain, block_hashes)
                .await
        }
    }
}

pub(super) async fn sync_ens_v2_registrar_for_scope(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<SourceScope<'_>>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<EnsV2RegistrarSyncSummary> {
    match (source_scope, progress.as_deref_mut()) {
        (Some(source_scope), Some(progress)) => {
            EnsV2RegistrarSyncSummary::sync_for_block_hashes_with_source_scope_and_progress(
                pool,
                chain,
                block_hashes,
                source_scope,
                progress,
            )
            .await
        }
        (Some(source_scope), None) => {
            EnsV2RegistrarSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await
        }
        (None, Some(progress)) => {
            EnsV2RegistrarSyncSummary::sync_for_block_hashes_with_progress(
                pool,
                chain,
                block_hashes,
                progress,
            )
            .await
        }
        (None, None) => {
            EnsV2RegistrarSyncSummary::sync_for_block_hashes(pool, chain, block_hashes).await
        }
    }
}
