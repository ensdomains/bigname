use crate::StartupAdapterProgress;
use anyhow::Result;
use bigname_adapters::{EnsV1UnwrappedAuthoritySyncSummary, EnsV2RegistrarSyncSummary};

type SourceScope<'a> = &'a [(String, String, i64, i64)];

pub(super) async fn sync_ens_v1_unwrapped_authority_for_scope(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<SourceScope<'_>>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<EnsV1UnwrappedAuthoritySyncSummary> {
    let result = match source_scope {
        Some(source_scope) => {
            EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await
        }
        None => {
            EnsV1UnwrappedAuthoritySyncSummary::sync_for_block_hashes(pool, chain, block_hashes)
                .await
        }
    };
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    result
}

pub(super) async fn sync_ens_v2_registrar_for_scope(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<SourceScope<'_>>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<EnsV2RegistrarSyncSummary> {
    let result = match source_scope {
        Some(source_scope) => {
            EnsV2RegistrarSyncSummary::sync_for_block_hashes_with_source_scope(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await
        }
        None => EnsV2RegistrarSyncSummary::sync_for_block_hashes(pool, chain, block_hashes).await,
    };
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    result
}
