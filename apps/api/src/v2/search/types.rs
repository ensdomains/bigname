use super::RegistrationStatus;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct SearchName {
    pub(crate) name: String,
    pub(crate) display_name: String,
    pub(crate) namespace: String,
    pub(crate) namehash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) manager: Option<String>,
    pub(crate) status: RegistrationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registered_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at: Option<crate::v2::timestamps::ExpiryTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) expires_at_reason: Option<String>,
    /// Zero-based request-window membership, omitted on scalar expiry requests and search.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) expires_window_index: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) grace_ends_window_index: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) grace_ends_at: Option<crate::v2::timestamps::ExpiryTimestamp>,
    /// The `authority` the name's detail serves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) authority: Option<crate::v2::vocab::Authority>,
    /// Present while `authority` is `ens_v1` or `ens_v0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) ens_v1: Option<crate::v2::name_record::EnsV1>,
    /// On `GET /v1/names` rows only: the last holder of a released registration
    /// (`name_record::lapsed_registration`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lapsed_registration: Option<crate::v2::name_record::LapsedRegistration>,
}
