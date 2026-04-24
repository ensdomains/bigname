use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct HistoryQuery {
    pub(crate) scope: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct PermissionsQuery {
    pub(crate) subject: Option<String>,
    pub(crate) scope: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ChildrenQuery {
    pub(crate) surface_classes: Option<String>,
    pub(crate) include: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct AddressNamesQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) dedupe_by: Option<String>,
    pub(crate) include: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AddressNamesIncludeOptions {
    pub(crate) role_summary: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct AddressHistoryQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) scope: Option<String>,
    pub(crate) cursor: Option<String>,
    pub(crate) page_size: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ExactNameSnapshotQuery {
    pub(crate) at: Option<String>,
    pub(crate) chain_positions: Option<String>,
    pub(crate) consistency: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ResolutionQuery {
    pub(crate) at: Option<String>,
    pub(crate) chain_positions: Option<String>,
    pub(crate) consistency: Option<String>,
    pub(crate) mode: Option<String>,
    pub(crate) records: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct InferredResolutionQuery {
    pub(crate) mode: Option<String>,
    pub(crate) records: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct ResolutionExecutionExplainQuery {
    pub(crate) records: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct PrimaryNameQuery {
    pub(crate) namespace: Option<String>,
    pub(crate) coin_type: Option<String>,
    pub(crate) mode: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResolutionMode {
    Declared,
    Verified,
    Both,
}

impl ResolutionMode {
    pub(crate) fn includes_declared(self) -> bool {
        matches!(self, Self::Declared | Self::Both)
    }

    pub(crate) fn includes_verified(self) -> bool {
        matches!(self, Self::Verified | Self::Both)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolutionRecordKey {
    pub(crate) record_key: String,
    pub(crate) record_family: String,
    pub(crate) selector_key: Option<String>,
}
