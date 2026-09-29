//! Per-page evidence the history collections read after a page is chosen: the name each
//! nameless resolver record write was made for at its own position, and the name each
//! primary-name event recorded. Both are request-scoped reads of normalized events at the rows'
//! own positions; neither changes which rows a page holds, its order, count or cursor.

use std::collections::BTreeMap;

use bigname_storage::HistoryEvent as StorageHistoryEvent;
use serde_json::Value;

use super::{HistoryInclude, V2Error, V2Result};

const RECORD_EVENT_KINDS: [&str; 2] = ["RecordChanged", "RecordVersionChanged"];

#[derive(Debug, Default)]
pub(crate) struct HistoryRowContext {
    record_names: BTreeMap<i64, String>,
    primary_names: Option<BTreeMap<i64, Value>>,
}

impl HistoryRowContext {
    /// The logical name a nameless record write was made for, when exactly one name's pointer and
    /// links selected it at its position.
    pub(crate) fn record_name(&self, row: &StorageHistoryEvent) -> Option<&str> {
        self.record_names
            .get(&row.normalized_event_id)
            .map(String::as_str)
    }

    /// The logical names this context adds to a page.
    pub(crate) fn record_logical_name_ids(&self) -> impl Iterator<Item = &String> {
        self.record_names.values()
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

/// Load the context of `rows`. Record names are loaded always, since they fill the lean `name`;
/// primary-name values only with `include=data`.
pub(crate) async fn load_history_row_context(
    pool: &sqlx::PgPool,
    rows: &[StorageHistoryEvent],
    include: HistoryInclude,
) -> V2Result<HistoryRowContext> {
    let record_ids = rows
        .iter()
        .filter(|row| {
            row.logical_name_id.is_none() && RECORD_EVENT_KINDS.contains(&row.event_kind.as_str())
        })
        .map(|row| row.normalized_event_id)
        .collect::<Vec<_>>();
    let record_names = bigname_storage::load_positional_record_names(pool, &record_ids)
        .await
        .map_err(|error| {
            if bigname_storage::families::name::is_publication_unavailable(&error) {
                return V2Error::stale(
                    "history resolver classification is temporarily unavailable",
                );
            }
            tracing::error!(error = ?error, "failed to load record event names");
            V2Error::internal_error("failed to load record event names")
        })?
        .into_iter()
        .filter_map(|(event_id, names)| {
            let mut names = names.into_iter();
            match (names.next(), names.next()) {
                (Some(name), None) => Some((event_id, name)),
                _ => None,
            }
        })
        .collect();
    let primary_names = if include.data {
        let claim_ids = rows
            .iter()
            .filter(|row| row.event_kind == "ReverseChanged")
            .map(|row| row.normalized_event_id)
            .collect::<Vec<_>>();
        Some(
            bigname_storage::load_recorded_primary_names(pool, &claim_ids)
                .await
                .map_err(|error| {
                    tracing::error!(error = ?error, "failed to load recorded primary names");
                    V2Error::internal_error("failed to load recorded primary names")
                })?,
        )
    } else {
        None
    };
    Ok(HistoryRowContext {
        record_names,
        primary_names,
    })
}
