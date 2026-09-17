//! Readers for the `declared_summary` Project writes on a current name row.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::Uuid;

use super::values::{json_address_at_paths, json_timestamp_at_paths, object_field, string_field};

/// The holder and authority a released ENSv1 lease had when it lapsed. Project writes the
/// block only on a [released v1 authority](../../../../../docs/glossary.md#released-v1-authority)
/// tombstone, so its presence means the name has no current registrant. It is never current
/// data: no relation, permission or current field is derived from it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct LapsedRegistration {
    /// The lapsed lease's last holder.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registrant: Option<String>,
    /// What held the lapsed lease: `registrar` for an unwrapped lease, `wrapper` for one held
    /// through the NameWrapper.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) authority: Option<String>,
    /// Block time of the first block at or past the end of the lease's grace period.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) released_at: Option<String>,
}

pub(crate) fn lapsed_registration(summary: &Value) -> Option<LapsedRegistration> {
    let lapsed = object_field(declared_registration(summary)?, "lapsed_registration")?;
    Some(LapsedRegistration {
        registrant: json_address_at_paths(lapsed, &[&["registrant"]]),
        authority: string_field(lapsed.get("authority_kind")),
        released_at: json_timestamp_at_paths(lapsed, &[&["released_at"]]),
    })
}

/// The registration resource Project selected for the name. For a `.eth` second-level name
/// this is its BaseRegistrar lease, also while the name is wrapped.
pub(crate) fn projected_registration_resource_id(summary: &Value) -> Option<&str> {
    declared_registration(summary)?.get("resource_id")?.as_str()
}

/// A name's `registration_id`: the projected registration resource, else the bound resource
/// (a wrapped subname, an ENSv2 registration, or a row projected before the field existed).
pub(crate) fn registration_id(summary: &Value, resource_id: Option<Uuid>) -> Option<String> {
    projected_registration_resource_id(summary)
        .map(str::to_owned)
        .or_else(|| resource_id.map(|value| value.to_string()))
}

pub(super) fn declared_registration(summary: &Value) -> Option<&Value> {
    object_field(summary, "registration")
}

pub(super) fn declared_owner(summary: &Value) -> Option<String> {
    json_address_at_paths(
        summary,
        &[&["control", "owner"], &["control", "registry_owner"]],
    )
}

pub(super) fn declared_registrant(summary: &Value) -> Option<String> {
    json_address_at_paths(
        summary,
        &[&["registration", "registrant"], &["control", "registrant"]],
    )
}

pub(super) fn declared_registered_at(summary: &Value) -> Option<String> {
    json_timestamp_at_paths(
        summary,
        &[
            &["registration", "registered_at"],
            &["registration", "registration_date"],
        ],
    )
}

pub(super) fn declared_created_at(summary: &Value) -> Option<String> {
    json_timestamp_at_paths(
        summary,
        &[&["registration", "created_at"], &["history", "created_at"]],
    )
}

pub(super) fn declared_expires_at(summary: &Value) -> Option<String> {
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
}

pub(super) fn chain_positions_created_at(chain_positions: &Value) -> Option<String> {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(_, position)| json_timestamp_at_paths(position, &[&["timestamp"]]))
        .min()
}
