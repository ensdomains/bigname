use anyhow::Result;
use bigname_adapters::StartupAdapterProgress;

use crate::resolver_profile_convergence::{
    journal_resolver_profile_authority_if_epoch_changed,
    journal_resolver_profile_authority_if_epoch_changed_with_progress,
};

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

pub(super) async fn record_full_closure_progress(
    pool: &sqlx::PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}

pub(super) async fn journal_full_closure_authority_with_progress(
    pool: &sqlx::PgPool,
    chain: &str,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<crate::resolver_profile_convergence::ResolverProfileAuthorityJournalSummary> {
    match progress.as_deref_mut() {
        Some(progress) => {
            journal_resolver_profile_authority_if_epoch_changed_with_progress(pool, chain, progress)
                .await
        }
        None => journal_resolver_profile_authority_if_epoch_changed(pool, chain).await,
    }
}

pub(super) fn trim_allocator_after_full_closure_adapter(adapter: &'static str) {
    #[cfg(target_os = "linux")]
    {
        let malloc_trim_result = unsafe { malloc_trim(0) };
        tracing::info!(
            service = "indexer",
            adapter,
            malloc_trim_result,
            "allocator trim requested after full closure adapter"
        );
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = adapter;
    }
}
