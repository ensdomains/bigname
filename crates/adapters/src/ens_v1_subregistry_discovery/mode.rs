use bigname_storage::NormalizedEventReplayAuthoritySummary;

use super::EnsV1SubregistryDiscoverySyncSummary;

pub(super) type EnsV1SubregistryDiscoverySyncOutcome = (
    EnsV1SubregistryDiscoverySyncSummary,
    bool,
    NormalizedEventReplayAuthoritySummary,
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DiscoveryEdgeMutation {
    Reconcile,
    Skip,
}

impl DiscoveryEdgeMutation {
    pub(super) const fn reconciles(self) -> bool {
        matches!(self, Self::Reconcile)
    }
}
