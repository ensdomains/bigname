#[path = "discovery/admission.rs"]
mod admission;
#[path = "discovery/admission_epoch.rs"]
mod admission_epoch;
#[path = "discovery/loading.rs"]
mod loading;
#[path = "discovery/persistence.rs"]
mod persistence;
#[path = "discovery/provenance.rs"]
mod provenance;
#[path = "discovery/reconciliation.rs"]
mod reconciliation;
#[path = "discovery/types.rs"]
mod types;

pub use admission::DiscoveryAdmissionState;
pub use admission_epoch::load_discovery_admission_epoch;
pub(crate) use admission_epoch::{
    bump_discovery_admission_epochs, fence_discovery_admission_epoch_writes,
};
pub use loading::load_discovery_admission_state;
pub use persistence::persist_discovery_observation;
pub use provenance::discovery_observation_evm_event_position;
pub use reconciliation::{
    ExpectedDiscoveryAdmissionEpoch, FullDiscoveryReconciliationOptions,
    reconcile_discovery_observations, reconcile_scoped_discovery_observation_transitions,
    reconcile_scoped_discovery_observations,
};
pub use types::{
    AdmittedDiscoveryEdge, DiscoveryCandidate, DiscoveryObservation, DiscoveryPersistenceSummary,
    DiscoveryReconciliationSummary,
};
