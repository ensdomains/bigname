//! Per-page evidence the history collections read after a page is chosen: the name each
//! primary-name event recorded, event-time token IDs and uniquely matched payment observations.
//! It is a request-scoped read of normalized events and changes
//! neither which rows a page holds nor its order, count or cursor.

use std::collections::BTreeMap;

use bigname_storage::HistoryEvent as StorageHistoryEvent;
use serde_json::Value;

use super::{HistoryInclude, V2Error, V2Result};

#[derive(Debug, Default)]
pub(crate) struct HistoryRowContext {
    primary_names: Option<BTreeMap<i64, Value>>,
    token_ids: BTreeMap<i64, String>,
    payments: BTreeMap<i64, Value>,
}

impl HistoryRowContext {
    pub(crate) fn payment(&self, row: &StorageHistoryEvent) -> Option<&Value> {
        self.payments.get(&row.normalized_event_id)
    }
    pub(crate) fn token_id(&self, row: &StorageHistoryEvent) -> Option<&str> {
        self.token_ids
            .get(&row.normalized_event_id)
            .map(String::as_str)
    }

    /// The recorded name of a primary-name row, once the page's names were loaded: `Some(None)`
    /// when the event recorded none.
    pub(crate) fn recorded_primary_name(
        &self,
        row: &StorageHistoryEvent,
    ) -> Option<Option<&Value>> {
        self.primary_names
            .as_ref()
            .map(|names| names.get(&row.normalized_event_id))
    }
}

/// Load the selected rows' supporting evidence only with `include=data`.
pub(crate) async fn load_history_row_context(
    pool: &sqlx::PgPool,
    rows: &[StorageHistoryEvent],
    include: HistoryInclude,
    block_bounds: &BTreeMap<String, i64>,
    fence: Option<&bigname_storage::InterpretRedoFence>,
) -> V2Result<HistoryRowContext> {
    if !include.data {
        return Ok(HistoryRowContext::default());
    }
    let context = read_context(pool, rows, block_bounds).await;
    #[cfg(test)]
    bigname_storage::history_anchor_read_test_hooks::run(
        pool,
        bigname_storage::history_anchor_read_test_hooks::HistoryReadHookPoint::AfterContext,
    )
    .await
    .map_err(context_error)?;
    // A replay may replace the evidence between page selection and this snapshot. Check the
    // captured generation even on errors, so a concurrent replay returns retry guidance.
    if let Some(fence) = fence {
        bigname_storage::revalidate_interpret_redo_fence(pool, fence)
            .await
            .map_err(|error| {
                super::history::map_history_page_error(error, "failed to load history row evidence")
            })?;
    }
    context
}

async fn read_context(
    pool: &sqlx::PgPool,
    rows: &[StorageHistoryEvent],
    block_bounds: &BTreeMap<String, i64>,
) -> V2Result<HistoryRowContext> {
    let claim_ids = rows
        .iter()
        .filter(|row| row.event_kind == "ReverseChanged")
        .map(|row| row.normalized_event_id)
        .collect::<Vec<_>>();
    let primary_names = bigname_storage::load_recorded_primary_names(pool, &claim_ids)
        .await
        .map_err(|error| {
            tracing::error!(error = ?error, "failed to load recorded primary names");
            V2Error::internal_error("failed to load recorded primary names")
        })?;
    let mut snapshot = bigname_storage::begin_read_snapshot(pool)
        .await
        .map_err(context_error)?;
    let token_ids = bigname_storage::load_history_token_ids(&mut *snapshot, rows, block_bounds)
        .await
        .map_err(context_error)?;
    let payments = bigname_storage::load_history_payment_values(&mut *snapshot, rows, block_bounds)
        .await
        .map_err(context_error)?;
    snapshot
        .commit()
        .await
        .map_err(|error| context_error(error.into()))?;
    Ok(HistoryRowContext {
        primary_names: Some(primary_names),
        token_ids,
        payments,
    })
}

fn context_error(error: anyhow::Error) -> V2Error {
    tracing::error!(error = ?error, "failed to load history row evidence");
    V2Error::internal_error("failed to load history row evidence")
}
