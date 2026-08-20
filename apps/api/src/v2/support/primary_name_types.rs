use bigname_storage::{PrimaryNameCurrentRow, SelectedSnapshot};
use serde_json::Value as JsonValue;

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
    /// The claimed name's selected exact-name authority is not one this deployment can resolve
    /// through, so the route dispatched no forward resolver call.
    AuthorityUnsupported(String),
    Verified(JsonValue),
}

pub(crate) struct PrimaryNameRouteRead {
    pub(crate) lookup_state: PrimaryNameLookupState,
    pub(crate) selected_snapshot: Option<SelectedSnapshot>,
}
