use crate::StartupAdapterProgress;
use anyhow::Result;

pub(super) async fn record_adapter_progress(
    pool: &sqlx::PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}
