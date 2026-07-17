use uuid::Uuid;

pub(super) struct StoredActiveRoot {
    pub(super) manifest_id: i64,
    pub(super) chain: String,
    pub(super) _contract_instance_id: Uuid,
    pub(super) address: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct StoredActiveContract {
    pub(super) manifest_id: i64,
    pub(super) chain: String,
    pub(super) role: String,
    pub(super) contract_instance_id: Uuid,
    pub(super) address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StoredDiscoveryRule {
    pub(super) edge_kind: String,
    pub(super) from_role: String,
    pub(super) admission: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscoveryCandidate<'a> {
    pub chain: &'a str,
    pub from_address: &'a str,
    pub to_address: &'a str,
    pub edge_kind: &'a str,
    pub discovery_source: &'a str,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AdmittedDiscoveryEdge {
    pub source_manifest_id: i64,
    pub chain: String,
    pub from_contract_instance_id: Uuid,
    pub to_contract_instance_id: Option<Uuid>,
    pub from_address: String,
    pub to_address: String,
    pub edge_kind: String,
    pub discovery_source: String,
    pub admission: String,
    pub from_role: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryObservation {
    pub chain: String,
    pub from_address: String,
    pub to_address: String,
    pub edge_kind: String,
    pub discovery_source: String,
    pub active_from_block_number: Option<i64>,
    pub active_from_block_hash: Option<String>,
    pub active_to_block_number: Option<i64>,
    pub active_to_block_hash: Option<String>,
    pub provenance: serde_json::Value,
}

impl DiscoveryObservation {
    pub fn candidate(&self) -> DiscoveryCandidate<'_> {
        DiscoveryCandidate {
            chain: &self.chain,
            from_address: &self.from_address,
            to_address: &self.to_address,
            edge_kind: &self.edge_kind,
            discovery_source: &self.discovery_source,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryPersistenceSummary {
    pub admitted_edge_count: usize,
    pub inserted_edge_count: usize,
    pub admitted_edges: Vec<AdmittedDiscoveryEdge>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryReconciliationSummary {
    /// Total active edges for the reconciled discovery source after this call.
    /// A scoped call limits which assignments may change, not the reported source total.
    pub active_edge_count: usize,
    pub admitted_edge_count: usize,
    /// Edges which became active during reconciliation, whether by inserting
    /// a new epoch or reactivating the exact retained historical epoch.
    pub inserted_edge_count: usize,
    pub deactivated_edge_count: usize,
    /// Exact number of admission-epoch increments committed by this call.
    pub admission_epoch_bump_count: usize,
    pub admitted_edges: Vec<AdmittedDiscoveryEdge>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct EvmEventPosition {
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct ReconciledDiscoveryEdgeSpec {
    pub(super) observation_key: String,
    pub(super) chain: String,
    pub(super) edge_kind: String,
    pub(super) from_contract_instance_id: Uuid,
    pub(super) to_contract_instance_id: Uuid,
    pub(super) discovery_source: String,
    pub(super) source_manifest_id: i64,
    pub(super) admission: String,
    pub(super) active_from_block_number: Option<i64>,
    pub(super) active_from_block_hash: Option<String>,
    pub(super) active_from_event_position: Option<EvmEventPosition>,
    pub(super) provenance_json: String,
}

#[derive(Clone, Debug)]
pub(super) struct ExistingReconciledDiscoveryEdge {
    pub(super) discovery_edge_id: i64,
    pub(super) spec: ReconciledDiscoveryEdgeSpec,
    pub(super) to_address: String,
    pub(super) active_from_block_is_orphaned: bool,
}

#[derive(Clone, Debug)]
pub(super) struct ObservationTerminalState {
    pub(super) chain: String,
    pub(super) block_number: Option<i64>,
    pub(super) block_hash: Option<String>,
    pub(super) event_position: Option<EvmEventPosition>,
}
