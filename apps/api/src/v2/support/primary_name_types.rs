use bigname_storage::{PrimaryNameCurrentRow, SelectedSnapshot};
use serde_json::Value as JsonValue;
use sqlx::types::time::OffsetDateTime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PrimaryNameTupleState {
    ProjectionUnavailable,
    TupleMissing,
    TuplePresent(PrimaryNameCurrentRow),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PrimaryNameLookupState {
    pub(crate) tuple_state: PrimaryNameTupleState,
    pub(crate) normalized_claim_name: Option<String>,
    pub(crate) claim_name_is_normalized: bool,
    pub(crate) on_demand_claim: OnDemandPrimaryNameClaimState,
    pub(crate) on_demand_verified: OnDemandPrimaryNameVerificationState,
    pub(crate) persisted_verified: Option<PersistedPrimaryNameVerifiedReadback>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OnDemandPrimaryNameClaimState {
    NotAttempted,
    Unavailable,
    NotFound,
    InvalidName(OnDemandPrimaryNameInvalidClaim),
    Found(OnDemandPrimaryNameClaim),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OnDemandPrimaryNameInvalidClaim {
    pub(crate) raw_name: String,
    pub(crate) resolver_address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OnDemandPrimaryNameClaim {
    pub(crate) raw_name: String,
    pub(crate) normalized_name: String,
    pub(crate) resolver_address: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OnDemandPrimaryNameVerificationState {
    NotAttempted,
    ClaimNotNormalized,
    Verified(JsonValue),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistedPrimaryNameVerifiedReadback {
    pub(crate) verified_primary_name: JsonValue,
    pub(crate) provenance: JsonValue,
    pub(crate) finished_at: OffsetDateTime,
    pub(crate) route_local_claim: Option<OnDemandPrimaryNameClaimState>,
    pub(crate) forward_call_attempted: bool,
}

pub(crate) struct PrimaryNameRouteRead {
    pub(crate) lookup_state: PrimaryNameLookupState,
    pub(crate) selected_snapshot: Option<SelectedSnapshot>,
}
