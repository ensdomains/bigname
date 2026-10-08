//! Public ENSv1 fields shaped from an already-selected name authority.
use super::values::json_timestamp_at_paths;
use super::{ExpiryTimestamp, WrapperFuses, WrapperState, wrapper_expiry, wrapper_metadata};
use crate::wrapper_expiry::{self, WrapperExpiryKey};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EnsV1 {
    /// The BaseRegistrar lease's own expiry, null without a lease (a subname). It is the
    /// registrar's `expiries[id]`, which a renewal advances in place
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L96-L98 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L161-L167 @ ens_v1@91c966f).
    /// After the Universal Resolver cutover the Universal Resolver reads only ENSv2 registries,
    /// so the top-level `expires_at` of a `.eth` name with a live ENSv2 entry is that entry's
    /// registry expiry instead
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L55-L63 @ ens_v2_sepolia_20260916@366de741)
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/registry/PermissionedRegistry.sol:L326-L329 @ ens_v2_sepolia_20260916@366de741).
    /// A premigration reservation expires 62 days (the continuity bonus) after the lease date,
    /// so the two dates differ even when both are live
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L38-L42 @ ens_v2_sepolia_20260916@366de741)
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L250 @ ens_v2_sepolia_20260916@366de741).
    ///
    /// The outer `None` omits the field: a registry child named only under a label that fails
    /// normalization claims no lifecycle (the API registry-child adapter).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub expires_at: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wrapper_state: Option<WrapperState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wrapper_fuses: Option<WrapperFuses>,
    /// The NameWrapper entry's own stored expiry, on a row with a wrapper state and on one whose
    /// emancipated or locked wrapper has lapsed past it, when NameWrapper reports no owner and no
    /// fuses for it
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L843-L856 @ ens_v1@91c966f).
    /// It is the expiry NameWrapper's `getData` reads from the token's own word
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L143-L154 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L128-L135 @ ens_v1@91c966f),
    /// so a renewal through a controller that calls only `BaseRegistrar.renew` leaves it
    /// behind the lease
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L352-L368 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312-L337 @ ens_v1@91c966f).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "super::types::present_expiry"
    )]
    pub wrapper_expires_at: Option<ExpiryTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wrapper_expires_at_reason: Option<String>,
    /// A wrapper expiry the response has yet to read; serializing an
    /// object that still holds one fails.
    #[serde(
        default,
        skip_deserializing,
        skip_serializing_if = "Option::is_none",
        serialize_with = "unfilled",
        rename = "unfilled_wrapper_expiry"
    )]
    pub pending_wrapper_expiry: Option<PendingExpiry>,
}

fn unfilled<S: serde::Serializer>(_: &Option<PendingExpiry>, _: S) -> Result<S::Ok, S::Error> {
    Err(serde::ser::Error::custom(
        "ens_v1 wrapper expiry was never read",
    ))
}

/// The `ens_v1` object of a row whose served authority is `authority`. Stored wrapper metadata
/// is validated whatever the authority.
pub fn ens_v1(authority: Option<&str>, declared_summary: &Value) -> Result<Option<EnsV1>> {
    let wrapper = wrapper_metadata(declared_summary)?;
    let (expiry, pending_wrapper_expiry) =
        stored_wrapper_expiry(declared_summary, wrapper.is_some())?;
    if !matches!(authority, Some("ens_v0" | "ens_v1")) {
        return Ok(None);
    }
    let (wrapper_state, wrapper_fuses) = wrapper.map_or_else(
        || {
            (
                declared_summary
                    .get("wrapper_state")
                    .and_then(Value::as_str)
                    .and_then(WrapperState::from_wire),
                None,
            )
        },
        |(state, fuses)| (Some(state), Some(fuses)),
    );
    let (wrapper_expires_at, wrapper_expires_at_reason) = expiry
        .map_or((None, None), |(timestamp, reason)| {
            (Some(timestamp), reason)
        });
    Ok(Some(EnsV1 {
        expires_at: Some(json_timestamp_at_paths(
            declared_summary,
            &[&["registration", "ens_v1_expiry"]],
        )),
        wrapper_state,
        wrapper_fuses,
        wrapper_expires_at,
        wrapper_expires_at_reason,
        pending_wrapper_expiry,
    }))
}

type ServedExpiry = (ExpiryTimestamp, Option<String>);
type PendingExpiry = (WrapperExpiryKey, bool);

/// The stored NameWrapper expiry the composed read attached (`wrapper_expiry_seconds`), or the
/// wrapper whose expiry it left for the response to read (`wrapper_expiry_pending`). One of them
/// is required beside a wrapper state; without a state only a masked (lapsed or unknown) wrapper
/// may carry either. An attached JSON null is a wrapper that was unwrapped, which serves no
/// expiry.
fn stored_wrapper_expiry(
    declared_summary: &Value,
    backed: bool,
) -> Result<(Option<ServedExpiry>, Option<PendingExpiry>)> {
    let masked = declared_summary.get("wrapper_masked") == Some(&Value::Bool(true));
    let stored = declared_summary.get(wrapper_expiry::WRAPPER_EXPIRY_KEY);
    let pending = declared_summary.get(wrapper_expiry::WRAPPER_EXPIRY_PENDING_KEY);
    if (stored.is_some() || pending.is_some()) && !backed && !masked {
        return Err(inconsistent_wrapper_expiry());
    }
    match (stored, pending) {
        (Some(_), Some(_)) => Err(inconsistent_wrapper_expiry()),
        // The wrapper was unwrapped: the name has no current entry to serve the expiry of.
        (Some(Value::Null), None) => Ok((None, None)),
        (Some(word), None) => wrapper_expiry(word)
            .map(|served| (Some(served), None))
            .ok_or_else(inconsistent_wrapper_expiry),
        (None, Some(marker)) => wrapper_expiry::parse_pending_marker(marker)
            .filter(|(_, marked_backed)| *marked_backed == backed)
            .map(|pending| (None, Some(pending)))
            .ok_or_else(inconsistent_wrapper_expiry),
        (None, None) if backed => Err(inconsistent_wrapper_expiry()),
        (None, None) => Ok((None, None)),
    }
}

fn inconsistent_wrapper_expiry() -> anyhow::Error {
    anyhow::anyhow!("stored wrapper expiry is inconsistent")
}

/// Deserialize a present `expires_at`, null included, as `Some`; an absent one stays `None`.
fn present<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}
