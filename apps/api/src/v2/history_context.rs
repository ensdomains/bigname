//! Per-page evidence the history collections read after a page is chosen: the name each
//! primary-name event recorded. It is a request-scoped read of normalized events and changes
//! neither which rows a page holds nor its order, count or cursor.

use std::collections::BTreeMap;

use bigname_storage::HistoryEvent as StorageHistoryEvent;
use serde_json::Value;

use super::{HistoryInclude, V2Error, V2Result};

#[derive(Debug, Default)]
pub(crate) struct HistoryRowContext {
    primary_names: Option<BTreeMap<i64, Value>>,
}

impl HistoryRowContext {
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

/// Load the context of `rows`: primary-name values, only with `include=data`.
pub(crate) async fn load_history_row_context(
    pool: &sqlx::PgPool,
    rows: &[StorageHistoryEvent],
    include: HistoryInclude,
) -> V2Result<HistoryRowContext> {
    if !include.data {
        return Ok(HistoryRowContext::default());
    }
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
    Ok(HistoryRowContext {
        primary_names: Some(primary_names),
    })
}
