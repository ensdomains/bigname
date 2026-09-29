//! Readers for the `declared_summary` Project writes on a current name row.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::Uuid;

use super::values::{json_address_at_paths, json_timestamp_at_paths, object_field, string_field};

/// The holder and contract of an ended registration. Project writes this historical block
/// for released ENSv1 leases and supported ENSv2 expiry/unregister releases. It supplies the
/// `former_registrant` relation, never current authority relations or permissions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct LapsedRegistration {
    /// The lapsed lease's last holder.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registrant: Option<String>,
    /// What held the lapsed lease. Not named `authority`: the record's top-level `authority`
    /// is the protocol arm (`ens_v1` / `ens_v2`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) held_through: Option<LapsedHeldThrough>,
    /// Canonical block time at which Project recorded the release. For ENSv1 leases this is
    /// the first observed block strictly past expiry plus the BaseRegistrar's 90-day grace,
    /// when the registrar treats the name as available again.
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) released_at: Option<String>,
    /// How the registration ended: `expired` (an ENSv1 lease past grace, or an ENSv2 registry
    /// path expiry) or `unregistered` (an explicit ENSv2 unregister). Expiry alone does not
    /// imply lost renewal eligibility: the ENSv2 `.eth` registrar admits an available name
    /// with a retained latest owner during grace. Unregister burns an existing owner token.
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registrar/ETHRegistrar.sol:L270-L292 @ ens_v2_sepolia_20260916@366de741)
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L224-L235 @ ens_v2_sepolia_20260916@366de741)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) release_kind: Option<LapsedReleaseKind>,
}

/// How a lapsed registration ended.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LapsedReleaseKind {
    Expired,
    Unregistered,
}

/// The contract an ended registration was held through.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LapsedHeldThrough {
    /// An unwrapped lease, held as the BaseRegistrar token.
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
    Registrar,
    /// A lease held through the NameWrapper.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240-L305 @ ens_v1@91c966f)
    Wrapper,
    /// An ENSv2 registration, held as the registry's token.
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L353-L367 @ ens_v2_sepolia_20260916@366de741)
    Registry,
}

impl LapsedHeldThrough {
    /// Project's other authority kinds (`registry_only`, the ENSv2 kinds) never hold an ENSv1
    /// lease, so they are omitted rather than passed through.
    fn from_authority_kind(authority_kind: &str) -> Option<Self> {
        match authority_kind {
            "registrar" => Some(Self::Registrar),
            "wrapper" => Some(Self::Wrapper),
            _ => None,
        }
    }
}

pub(crate) fn lapsed_registration(summary: &Value) -> Option<LapsedRegistration> {
    let lapsed = object_field(declared_registration(summary)?, "lapsed_registration")?;
    Some(LapsedRegistration {
        registrant: json_address_at_paths(lapsed, &[&["registrant"]]),
        held_through: match string_field(lapsed.get("held_through")).as_deref() {
            Some("registry") => Some(LapsedHeldThrough::Registry),
            _ => string_field(lapsed.get("authority_kind"))
                .as_deref()
                .and_then(LapsedHeldThrough::from_authority_kind),
        },
        released_at: json_timestamp_at_paths(lapsed, &[&["released_at"]]),
        release_kind: match string_field(lapsed.get("release_kind")).as_deref() {
            Some("expired") => Some(LapsedReleaseKind::Expired),
            Some("unregistered") => Some(LapsedReleaseKind::Unregistered),
            _ => None,
        },
    })
}

/// The registration resource Project selected for the name. For a `.eth` second-level name
/// this is its BaseRegistrar lease, also while the name is wrapped.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L270 @ ens_v1@91c966f)
pub(crate) fn projected_registration_resource_id(summary: &Value) -> Option<&str> {
    declared_registration(summary)?.get("resource_id")?.as_str()
}

/// A name's `registration_id`: the projected registration resource, else the bound resource
/// (a wrapped or registry-owned subname, an ENSv2 registration, or a row projected before
/// the field existed).
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

pub(super) fn declared_expires_at(
    summary: &Value,
) -> Option<crate::v2::timestamps::ExpiryTimestamp> {
    use crate::v2::timestamps::ExpiryTimestamp;
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
pub(super) fn declared_grace_ends_at(
    summary: &Value,
) -> Option<crate::v2::timestamps::ExpiryTimestamp> {
    use crate::v2::timestamps::ExpiryTimestamp;
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

pub(super) fn chain_positions_created_at(chain_positions: &Value) -> Option<String> {
    chain_positions
        .as_object()
        .into_iter()
        .flatten()
        .filter_map(|(_, position)| json_timestamp_at_paths(position, &[&["timestamp"]]))
        .min_by_key(|value| value.parse::<bigname_storage::UnixSeconds>().ok())
}
