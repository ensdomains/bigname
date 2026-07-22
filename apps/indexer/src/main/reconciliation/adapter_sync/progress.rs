use anyhow::Result;
use bigname_adapters::StartupAdapterProgress;

use crate::resolver_profile_convergence::{
    journal_resolver_profile_authority_if_epoch_changed,
    journal_resolver_profile_authority_if_epoch_changed_with_progress,
};

pub(super) async fn record_adapter_progress(
    pool: &sqlx::PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}

pub(super) async fn journal_authority_epoch_with_progress(
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
