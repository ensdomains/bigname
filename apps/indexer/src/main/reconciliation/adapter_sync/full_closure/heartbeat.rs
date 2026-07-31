use crate::StartupAdapterProgress;
use anyhow::Result;

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
