use anyhow::Result;
use bigname_storage::sql_row;
use sqlx::{PgPool, postgres::PgRow};

use crate::checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress};

pub(crate) struct StartupManifestProgress<'a> {
    progress: &'a mut dyn StartupAdapterProgress,
}

impl<'a> StartupManifestProgress<'a> {
    pub(crate) fn new(progress: &'a mut dyn StartupAdapterProgress) -> Self {
        Self { progress }
    }
}

impl bigname_manifests::ManifestRuntimeProgress for StartupManifestProgress<'_> {
    fn record<'a>(
        &'a mut self,
        pool: &'a PgPool,
    ) -> bigname_manifests::ManifestRuntimeProgressFuture<'a> {
        self.progress.record(pool)
    }
}

pub(crate) const STARTUP_ADAPTER_PROGRESS_PAGE_ROWS: usize = 1_000;
pub(crate) const STARTUP_ADAPTER_PROGRESS_PAGE_ROWS_I64: i64 = 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RawLogPagePosition {
    pub(crate) block_number: i64,
    pub(crate) transaction_index: i64,
    pub(crate) log_index: i64,
    pub(crate) emitting_address: String,
    pub(crate) block_hash: String,
}

impl RawLogPagePosition {
    pub(crate) fn from_row(row: &PgRow) -> Result<Self> {
        Ok(Self {
            block_number: sql_row::get(row, "block_number")?,
            transaction_index: sql_row::get(row, "transaction_index")?,
            log_index: sql_row::get(row, "log_index")?,
            emitting_address: sql_row::get::<String>(row, "emitting_address")?.to_ascii_lowercase(),
            block_hash: sql_row::get(row, "block_hash")?,
        })
    }
}

pub(crate) async fn record_processed_row_progress(
    pool: &PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
    completed: usize,
    total: usize,
) -> Result<()> {
    if completed == total || completed.is_multiple_of(STARTUP_ADAPTER_PROGRESS_PAGE_ROWS) {
        record_startup_adapter_progress(pool, progress).await?;
    }
    Ok(())
}
