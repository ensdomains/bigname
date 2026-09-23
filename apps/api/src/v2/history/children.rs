//! `include=child_registrations` on name history: the name's direct child registrations merged
//! into its history (docs/api-v2-routes.md, "Direct child registrations").

use bigname_storage::{
    HistoryCursor, HistoryPageOptions, HistoryScope, HistorySubject, HistorySummary,
    HistorySummaryMode, InterpretRedoFence,
};
use serde::{Deserialize, Serialize};
use sqlx::types::Uuid;

use crate::AppState;
use crate::v2::{HistoryInclude, V2Error, V2Result, history_include};

use super::{HistoryEvent, build_history_event, map_history_page_error};

const CHILD_REGISTRATIONS_INCLUDE: &str = "child_registrations";
/// The cursor filter that records the option; absent without it, so cursors issued without the
/// option keep their exact bytes.
const CHILDREN_FILTER_KEY: &str = "children";
const CHILDREN_FILTER_VALUE: &str = "registrations";

/// Names whose children the option refuses: every second-level registration of the `.eth` or
/// Basenames registrar is a child of one of them. Project stores no rows for them either
/// (`bigname_project::EXCLUDED_CHILD_REGISTRATION_PARENTS`).
pub(super) const CHILD_REGISTRATION_REFUSED_PARENTS: &[&str] = &["eth", "base.eth"];

/// A row's relation to the requested name, present only with the option.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HistoryRowSubject {
    Name,
    Child,
}

/// Name history's `include`: the shared payload and count flags plus `child_registrations`,
/// which address history and `/v1/events` do not accept.
pub(super) fn name_history_include(include: &[String]) -> V2Result<(HistoryInclude, bool)> {
    let child_registrations = include
        .iter()
        .any(|value| value == CHILD_REGISTRATIONS_INCLUDE);
    let shared = include
        .iter()
        .filter(|value| *value != CHILD_REGISTRATIONS_INCLUDE)
        .cloned()
        .collect::<Vec<_>>();
    let parsed = history_include(&shared).map_err(|_| {
        V2Error::invalid_input(
            "include must contain only data, raw, total_count, or child_registrations",
        )
    })?;
    Ok((parsed, child_registrations))
}

/// Refuse the option for `eth` and `base.eth`, compared by resolved name identity.
pub(super) fn refuse_registrar_root(
    child_registrations: bool,
    namespace: &str,
    logical_name_id: &str,
) -> V2Result<()> {
    if child_registrations
        && CHILD_REGISTRATION_REFUSED_PARENTS.iter().any(|name| {
            bigname_storage::logical_name_id_for_name(namespace, name) == logical_name_id
        })
    {
        return Err(V2Error::invalid_input(
            "include=child_registrations is not supported for eth or base.eth: \
             their children are every second-level registration",
        ));
    }
    Ok(())
}

pub(super) fn insert_children_filter_key(
    filters: &mut std::collections::BTreeMap<String, String>,
    child_registrations: bool,
) {
    if child_registrations {
        filters.insert(
            CHILDREN_FILTER_KEY.to_owned(),
            CHILDREN_FILTER_VALUE.to_owned(),
        );
    }
}

/// One served page, with or without the option.
pub(super) struct NameHistoryPageData {
    pub(super) rows: Vec<HistoryEvent>,
    pub(super) next_cursor: Option<HistoryCursor>,
    pub(super) summary: Option<HistorySummary>,
}

pub(super) struct PageRequest<'a> {
    pub(super) logical_name_id: &'a str,
    pub(super) resource_ids: &'a [Uuid],
    pub(super) scope: HistoryScope,
    pub(super) cursor: Option<&'a HistoryCursor>,
    pub(super) page_size: u64,
    pub(super) summary_mode: HistorySummaryMode,
    pub(super) options: &'a HistoryPageOptions,
    pub(super) interpret_redo_fence: &'a InterpretRedoFence,
    pub(super) anchor_name: &'a str,
    pub(super) include: HistoryInclude,
}

/// Load a name history page; with the option, the page also holds the direct child
/// registrations and every row carries `subject`.
pub(super) async fn load_page(
    state: &AppState,
    request: PageRequest<'_>,
    child_registrations: bool,
) -> V2Result<NameHistoryPageData> {
    if !child_registrations {
        let page = bigname_storage::load_name_history_page(
            &state.pool,
            request.logical_name_id,
            request.resource_ids,
            request.scope,
            true,
            request.cursor,
            request.page_size,
            request.summary_mode,
            request.options,
            Some(request.interpret_redo_fence),
        )
        .await
        .map_err(|error| map_history_page_error(error, "failed to load name history"))?;
        return Ok(NameHistoryPageData {
            rows: page
                .rows
                .iter()
                .filter_map(|row| build_history_event(row, request.anchor_name, request.include))
                .collect(),
            next_cursor: page.next_cursor,
            summary: page.summary,
        });
    }
    let page = bigname_storage::load_name_history_page_with_child_registrations(
        &state.pool,
        request.logical_name_id,
        request.resource_ids,
        request.scope,
        request.cursor,
        request.page_size,
        request.summary_mode,
        request.options,
        Some(request.interpret_redo_fence),
    )
    .await
    .map_err(|error| map_history_page_error(error, "failed to load name history"))?;
    let rows = page
        .rows
        .iter()
        .filter_map(|row| {
            let mut event = build_history_event(&row.event, request.anchor_name, request.include)?;
            event.subject = Some(match row.subject {
                HistorySubject::Name => HistoryRowSubject::Name,
                HistorySubject::Child => {
                    event.name = row.child_name.clone()?;
                    HistoryRowSubject::Child
                }
            });
            Some(event)
        })
        .collect();
    Ok(NameHistoryPageData {
        rows,
        next_cursor: page.next_cursor,
        summary: page.summary,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_name_history_parses_the_child_option() {
        let values = |values: &[&str]| {
            values
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            name_history_include(&values(&["child_registrations", "data"])).expect("valid"),
            (
                HistoryInclude {
                    data: true,
                    raw: false
                },
                true
            )
        );
        assert_eq!(
            name_history_include(&values(&["raw", "total_count"])).expect("valid"),
            (
                HistoryInclude {
                    data: false,
                    raw: true
                },
                false
            )
        );
        assert!(name_history_include(&values(&["children"])).is_err());
        // The shared parser behind address history and `/v1/events` keeps refusing it.
        assert!(history_include(&values(&["child_registrations"])).is_err());
    }

    #[test]
    fn the_refused_parents_are_the_ones_project_never_stores() {
        assert_eq!(
            CHILD_REGISTRATION_REFUSED_PARENTS,
            bigname_project::EXCLUDED_CHILD_REGISTRATION_PARENTS
        );
    }

    #[test]
    fn registrar_roots_are_refused_by_identity_in_every_namespace() {
        for namespace in ["ens", "basenames"] {
            for name in ["eth", "base.eth"] {
                let id = bigname_storage::logical_name_id_for_name(namespace, name);
                assert!(refuse_registrar_root(true, namespace, &id).is_err());
                assert!(refuse_registrar_root(false, namespace, &id).is_ok());
            }
            for name in ["alice.eth", "alice.base.eth", "ethereum.eth"] {
                let id = bigname_storage::logical_name_id_for_name(namespace, name);
                assert!(refuse_registrar_root(true, namespace, &id).is_ok());
            }
        }
    }
}
