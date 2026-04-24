use uuid::Uuid;

use super::HistoryScope;

#[derive(Clone, Debug)]
pub(super) enum HistorySelector {
    None,
    LogicalNames(Vec<String>),
    Resources(Vec<Uuid>),
    LogicalNamesOrResources {
        logical_name_ids: Vec<String>,
        resource_ids: Vec<Uuid>,
    },
}

impl HistorySelector {
    pub(super) fn logical_names(logical_name_ids: Vec<String>) -> Self {
        if logical_name_ids.is_empty() {
            Self::None
        } else {
            Self::LogicalNames(logical_name_ids)
        }
    }

    pub(super) fn resources(resource_ids: Vec<Uuid>) -> Self {
        if resource_ids.is_empty() {
            Self::None
        } else {
            Self::Resources(resource_ids)
        }
    }

    pub(super) fn logical_names_or_resources(
        logical_name_ids: Vec<String>,
        resource_ids: Vec<Uuid>,
    ) -> Self {
        match (logical_name_ids.is_empty(), resource_ids.is_empty()) {
            (true, true) => Self::None,
            (false, true) => Self::LogicalNames(logical_name_ids),
            (true, false) => Self::Resources(resource_ids),
            (false, false) => Self::LogicalNamesOrResources {
                logical_name_ids,
                resource_ids,
            },
        }
    }
}

pub(super) fn name_history_selector(
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
) -> HistorySelector {
    let logical_name_ids = vec![logical_name_id.to_owned()];
    let resource_ids = resource_ids.to_vec();

    match scope {
        HistoryScope::Surface => HistorySelector::logical_names(logical_name_ids),
        HistoryScope::Resource => HistorySelector::resources(resource_ids),
        HistoryScope::Both => {
            HistorySelector::logical_names_or_resources(logical_name_ids, resource_ids)
        }
    }
}

pub(super) fn resource_history_selector(
    resource_id: Uuid,
    logical_name_ids: &[String],
    scope: HistoryScope,
) -> HistorySelector {
    let logical_name_ids = logical_name_ids.to_vec();
    let resource_ids = vec![resource_id];

    match scope {
        HistoryScope::Surface => HistorySelector::logical_names(logical_name_ids),
        HistoryScope::Resource => HistorySelector::resources(resource_ids),
        HistoryScope::Both => {
            HistorySelector::logical_names_or_resources(logical_name_ids, resource_ids)
        }
    }
}
