//! Pure public fields of an already-composed name. No authority selection or database reads.
//! Project stores these fields; API adapters use the same pure shaping and serialization.
mod ens_v1;
mod registration;
mod types;
pub mod values;
mod wrapper;
mod wrapper_fuses;
mod wrapper_state;

use serde::{Deserialize, Serialize};

pub use ens_v1::{EnsV1, ens_v1};
pub use registration::{
    RegistrationFields, chain_positions_created_at, classify_registration_status,
    declared_created_at, declared_expires_at, declared_grace_ends_at, declared_owner,
    declared_registered_at, declared_registration, declared_registry_owner, has_current_control,
    has_registration_identity, registration_fields,
};
pub use types::{ExpiryTimestamp, RegistrationStatus};
pub use wrapper::{
    InvalidWrapperMetadata, served_manager, wrapper_expiry, wrapper_lifecycle_matches_fuses,
    wrapper_metadata,
};
pub use wrapper_fuses::WrapperFuses;
pub use wrapper_state::WrapperState;

/// The compact registration subset; identity, owner and authority remain separate columns.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SearchFields {
    #[serde(flatten)]
    pub registration: RegistrationFields,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ens_v1: Option<EnsV1>,
}

/// Resolve only the timestamp fallback; callers supply the positions admitted on their snapshot.
pub fn resolve_created_at(declared: Option<&str>, positions: &serde_json::Value) -> Option<String> {
    declared
        .map(str::to_owned)
        .or_else(|| chain_positions_created_at(positions))
}

#[cfg(test)]
mod tests;
