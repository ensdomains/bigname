use std::collections::BTreeMap;

/// Per-kind sync counts for the gas-sponsorship adapter.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EntrypointUserOperationKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

/// One gas-sponsorship adapter sync pass over stored raw logs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EntrypointUserOperationSyncSummary {
    pub scanned_log_count: usize,
    pub matched_log_count: usize,
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, EntrypointUserOperationKindSyncSummary>,
}

pub(super) fn empty_summary(scanned_log_count: usize) -> EntrypointUserOperationSyncSummary {
    EntrypointUserOperationSyncSummary {
        scanned_log_count,
        ..EntrypointUserOperationSyncSummary::default()
    }
}
