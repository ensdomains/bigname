use super::values::{
    json_address_at_paths, json_timestamp_at_paths, json_value_present, object_field,
};
use super::{ExpiryTimestamp, RegistrationStatus, served_manager};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RegistrationFields {
    #[serde(skip)]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manager: Option<String>,
    pub status: RegistrationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at_declared: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "super::types::present_expiry"
    )]
    pub expires_at: Option<ExpiryTimestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_reason: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "super::types::present_expiry"
    )]
    pub grace_ends_at: Option<ExpiryTimestamp>,
}

pub fn registration_fields(
    namespace: &str,
    summary: &Value,
    has_binding: bool,
) -> RegistrationFields {
    let owner = declared_owner(summary);
    RegistrationFields {
        manager: served_manager(
            summary,
            owner.as_ref(),
            declared_registry_owner(summary).as_ref(),
        ),
        status: classify_registration_status(
            namespace,
            declared_registration(summary),
            owner.as_deref(),
            has_binding,
        ),
        registered_at: declared_registered_at(summary),
        created_at_declared: declared_created_at(summary),
        expires_at: declared_expires_at(summary),
        expires_at_reason: summary
            .pointer("/registration/expires_at_reason")
            .and_then(Value::as_str)
            .map(str::to_owned),
        grace_ends_at: declared_grace_ends_at(summary),
        owner,
    }
}

pub fn classify_registration_status(
    namespace: &str,
    registration: Option<&Value>,
    owner: Option<&str>,
    has_binding: bool,
) -> RegistrationStatus {
    if let Some(status) = registration
        .and_then(|value| value.get("lifecycle_status"))
        .and_then(Value::as_str)
    {
        return match status {
            "active" => RegistrationStatus::Active,
            "expired" => RegistrationStatus::Expired,
            "released" => RegistrationStatus::Released,
            _ => RegistrationStatus::Unregistered,
        };
    }
    control_status(namespace, registration, owner, has_binding)
}

// Preserve the former registration-identity and current-control rules independently of the
// canonical clock. In particular a reservation must not acquire an audit handle from status.
fn control_status(
    namespace: &str,
    registration: Option<&Value>,
    owner: Option<&str>,
    has_binding: bool,
) -> RegistrationStatus {
    if !has_binding {
        return RegistrationStatus::Unregistered;
    }
    if registration.is_some_and(|value| {
        value.get("released_at").is_some_and(json_value_present)
            || value.get("status").and_then(Value::as_str) == Some("released")
    }) {
        return RegistrationStatus::Released;
    }
    match registration
        .and_then(|value| value.get("authority_kind"))
        .and_then(Value::as_str)
    {
        Some("registrar") => RegistrationStatus::Active,
        Some("wrapper") if namespace != crate::BASENAMES_NAMESPACE => RegistrationStatus::Active,
        Some("registry_only" | "ens_v2_registry")
            if owner.is_some_and(|owner| !owner.trim().is_empty()) =>
        {
            RegistrationStatus::Active
        }
        _ => RegistrationStatus::Unregistered,
    }
}

pub fn has_registration_identity(namespace: &str, summary: &Value, has_binding: bool) -> bool {
    // Explicit absence belongs to the selected canonical allocation; the control binding
    // may still be a different registration's tombstone.
    if let Some(identity) = summary.pointer("/registration/identity_resource_id") {
        return identity.is_string();
    }
    control_status(
        namespace,
        declared_registration(summary),
        declared_owner(summary).as_deref(),
        has_binding,
    ) != RegistrationStatus::Unregistered
}

/// Current control deliberately ignores lifecycle_status: canonical grace can continue after
/// the ENSv2 control path ended, and wrapper-only release need not clear its plain token owner.
pub fn has_current_control(namespace: &str, summary: &Value, has_binding: bool) -> bool {
    control_status(
        namespace,
        declared_registration(summary),
        declared_owner(summary).as_deref(),
        has_binding,
    ) == RegistrationStatus::Active
}

pub fn declared_registration(summary: &Value) -> Option<&Value> {
    object_field(summary, "registration")
}

pub fn declared_owner(summary: &Value) -> Option<String> {
    json_address_at_paths(summary, &[&["control", "owner"]])
}

pub fn declared_registry_owner(summary: &Value) -> Option<String> {
    json_address_at_paths(summary, &[&["control", "registry_owner"]])
}

pub fn declared_registered_at(summary: &Value) -> Option<String> {
    json_timestamp_at_paths(
        summary,
        &[
            &["registration", "registered_at"],
            &["registration", "registration_date"],
        ],
    )
}

pub fn declared_created_at(summary: &Value) -> Option<String> {
    json_timestamp_at_paths(
        summary,
        &[&["registration", "created_at"], &["history", "created_at"]],
    )
}

pub fn declared_expires_at(summary: &Value) -> Option<super::ExpiryTimestamp> {
    use super::ExpiryTimestamp;
    if summary
        .pointer("/registration/expires_at_reason")
        .and_then(Value::as_str)
        .is_some()
    {
        return Some(ExpiryTimestamp::NoExpiry);
    }
    json_timestamp_at_paths(
        summary,
        &[
            &["registration", "expires_at"],
            &["registration", "expiry_date"],
            &["registration", "expiry"],
            &["control", "expires_at"],
            &["control", "expiry_date"],
            &["control", "expiry"],
        ],
    )
    .map(ExpiryTimestamp::Seconds)
}

/// When the registration's renewal grace ends: its expiry plus the grace period of the registrar
/// the expiry comes from (90 days for an ENSv1 `.eth` lease and a Basenames name, the ENSv2
/// `ETHRegistrar` grace for an ENSv2 `.eth` entry), or the expiry itself where no registrar grace
/// applies.
pub fn declared_grace_ends_at(summary: &Value) -> Option<super::ExpiryTimestamp> {
    use super::ExpiryTimestamp;
    if summary
        .pointer("/registration/expires_at_reason")
        .and_then(Value::as_str)
        .is_some()
    {
        return Some(ExpiryTimestamp::NoExpiry);
    }
    json_timestamp_at_paths(summary, &[&["registration", "grace_ends_at"]])
        .map(ExpiryTimestamp::Seconds)
}

pub fn chain_positions_created_at(chain_positions: &Value) -> Option<String> {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(_, position)| json_timestamp_at_paths(position, &[&["timestamp"]]))
        .min_by_key(|value| value.parse::<crate::UnixSeconds>().ok())
}
