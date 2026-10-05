//! The `ens_v1` object of a name-shaped row: what only ENSv1 holds about a name while ENSv1
//! decides it (`authority` `ens_v1` or `ens_v0`).
use std::collections::BTreeMap;

use bigname_storage::{
    NameCurrentRow,
    wrapper_expiry::{self, WrapperExpiryKey},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::super::{
    V2Error, V2Result,
    timestamps::ExpiryTimestamp,
    vocab::{Authority, WrapperFuses, WrapperState},
};
use super::{values::json_timestamp_at_paths, wrapper_expiry, wrapper_metadata};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct EnsV1 {
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
    /// normalization claims no lifecycle ([`ens_v1_of_registry_child`]).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub(crate) expires_at: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wrapper_state: Option<WrapperState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wrapper_fuses: Option<WrapperFuses>,
    /// The NameWrapper entry's own stored expiry, on a row with a wrapper state and on one whose
    /// emancipated or locked wrapper has lapsed past it. It is what NameWrapper's `getData`
    /// reads, so a renewal through a controller that calls only `BaseRegistrar.renew` leaves it
    /// behind the lease
    /// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L352-L368 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312-L337 @ ens_v1@91c966f).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wrapper_expires_at: Option<ExpiryTimestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wrapper_expires_at_reason: Option<String>,
    /// A wrapper expiry the response has yet to read ([`fill_wrapper_expiries`]); serializing an
    /// object that still holds one fails.
    #[serde(
        default,
        skip_deserializing,
        skip_serializing_if = "Option::is_none",
        serialize_with = "unfilled",
        rename = "unfilled_wrapper_expiry"
    )]
    pub(crate) pending_wrapper_expiry: Option<PendingExpiry>,
}

fn unfilled<S: serde::Serializer>(_: &Option<PendingExpiry>, _: S) -> Result<S::Ok, S::Error> {
    Err(serde::ser::Error::custom(
        "ens_v1 wrapper expiry was never read",
    ))
}

/// The `ens_v1` object of a row whose served authority is `authority`. Stored wrapper metadata
/// is validated whatever the authority.
pub(crate) fn ens_v1(
    authority: Option<Authority>,
    declared_summary: &Value,
) -> V2Result<Option<EnsV1>> {
    let wrapper = wrapper_metadata(declared_summary)?;
    let (expiry, pending_wrapper_expiry) =
        stored_wrapper_expiry(declared_summary, wrapper.is_some())?;
    if !matches!(authority, Some(Authority::EnsV0 | Authority::EnsV1)) {
        return Ok(None);
    }
    let (wrapper_state, wrapper_fuses) =
        wrapper.map_or((None, None), |(state, fuses)| (Some(state), Some(fuses)));
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
) -> V2Result<(Option<ServedExpiry>, Option<PendingExpiry>)> {
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

/// Reads the pending wrapper expiries of `objects` and serves them: one read per chain for the
/// whole response, on `db`, which must be the snapshot the rows were composed on where the route
/// holds one.
pub(crate) async fn fill_wrapper_expiries<'a>(
    db: impl Into<bigname_storage::ReadDb<'_>>,
    objects: impl IntoIterator<Item = &'a mut EnsV1>,
) -> V2Result<()> {
    let mut objects: Vec<&mut EnsV1> = objects
        .into_iter()
        .filter(|object| object.pending_wrapper_expiry.is_some())
        .collect();
    if objects.is_empty() {
        return Ok(());
    }
    let wanted: BTreeMap<WrapperExpiryKey, bool> = objects
        .iter()
        .filter_map(|object| object.pending_wrapper_expiry.clone())
        .collect();
    let served = wrapper_expiry::load_wrapper_expiries(db, &wanted)
        .await
        .map_err(|error| {
            tracing::error!(service = "api", error = ?error, "failed to read wrapper expiries");
            inconsistent_wrapper_expiry()
        })?;
    for object in &mut objects {
        let Some((key, _)) = object.pending_wrapper_expiry.take() else {
            continue;
        };
        if let Some(word) = served.get(&key).filter(|word| !word.is_null()) {
            let (timestamp, reason) =
                wrapper_expiry(word).ok_or_else(inconsistent_wrapper_expiry)?;
            object.wrapper_expires_at = Some(timestamp);
            object.wrapper_expires_at_reason = reason;
        }
    }
    Ok(())
}

fn inconsistent_wrapper_expiry() -> V2Error {
    V2Error::internal_error("stored wrapper expiry is inconsistent")
}

/// The `ens_v1` object of an ENSv1 registry child with no name row, which serves `authority` from
/// its registry.
///
/// A child no label-bearing event named holds only a null expiry. A lease is the BaseRegistrar's
/// `expiries[id]` and token, which a registry `setSubnodeOwner` child never gets
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L142-L147 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f).
///
/// A child whose only name surface is a shadow one (the surface bigname records, but keeps out of
/// name reads, for a name that fails ENSIP-15 normalization) has no name row either, but it can
/// be wrapped or leased. NameWrapper's `setSubnodeOwner` and `setSubnodeRecord` take any label
/// bytes, `_addLabel` checks only the length, and `_wrap` emits `NameWrapped` with that name
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L585 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L596-L630 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L865-L876 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f).
/// For a name that fails normalization the wrapper adapter writes a shadow surface instead of a
/// name (crates/adapters/src/schema_v2/protocol/v1/wrapper.rs:133 and :258-267), so its wrapper
/// state, and a `.eth` name's lease, are projected without a composed name. When a NameWrapper or
/// ENSv1 registrar event observed the shadow (`lifecycle_shadow`) the object omits `expires_at`
/// and the wrapper fields rather than claim the child has no lease or wrapper state. A shadow
/// only another observer wrote, such as a resolver `NameChanged`, brings no lifecycle, so that
/// child keeps the null expiry.
pub(crate) fn ens_v1_of_registry_child(
    authority: Option<Authority>,
    lifecycle_shadow: bool,
) -> V2Result<Option<EnsV1>> {
    let ens_v1 = ens_v1(authority, &Value::Object(serde_json::Map::new()))?;
    Ok(ens_v1.map(|ens_v1| {
        if lifecycle_shadow {
            EnsV1 {
                expires_at: None,
                ..ens_v1
            }
        } else {
            ens_v1
        }
    }))
}

/// Deserialize a present `expires_at`, null included, as `Some`; an absent one stays `None`.
fn present<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

/// The `ens_v1` object of a composed name row, for the rows that serve one.
pub(crate) fn ens_v1_of_row(row: Option<&NameCurrentRow>) -> V2Result<Option<EnsV1>> {
    row.map(|row| {
        ens_v1(
            Authority::from_provenance(&row.provenance),
            &row.declared_summary,
        )
    })
    .transpose()
    .map(Option::flatten)
}
