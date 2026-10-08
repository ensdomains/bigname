use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistrationStatus {
    Active,
    Expired,
    Released,
    Unregistered,
}

/// An expiry with registration context: a finite exact second, or explicit null with a reason.
/// The enclosing optional field still omits expiry when there is no registration context.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ExpiryTimestamp {
    Seconds(String),
    NoExpiry,
}

/// Preserve a present JSON null as a present expiry when reading stored fields.
pub(super) fn present_expiry<'de, D>(deserializer: D) -> Result<Option<ExpiryTimestamp>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    ExpiryTimestamp::deserialize(deserializer).map(Some)
}
