use bigname_storage::LabelPreimageImportSummary;

use crate::{
    database::RunnerDatabase,
    error::{RunnerError, RunnerResult},
};

pub async fn import_ens_rainbow(
    database_url: &str,
    batch_size: Option<i64>,
    limit: Option<i64>,
) -> RunnerResult<LabelPreimageImportSummary> {
    let database = RunnerDatabase::connect(database_url, 2).await?;
    let summary = bigname_storage::import_label_preimages_from_ens_names_table(
        database.pool(),
        batch_size,
        limit,
    )
    .await
    .map_err(|error| {
        RunnerError::transient(format!(
            "ENS rainbow label-preimage import failed: {error:#}"
        ))
    })?;
    tracing::info!(
        scanned_row_count = summary.scanned_row_count,
        retained_row_count = summary.retained_row_count,
        rejected_row_count = summary.rejected_row_count,
        "ENS rainbow label-preimage import completed"
    );
    Ok(summary)
}
