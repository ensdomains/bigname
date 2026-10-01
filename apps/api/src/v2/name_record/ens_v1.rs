//! The `ens_v1` object of a name-shaped row: what only ENSv1 holds about a name while ENSv1
//! decides it (`authority` `ens_v1` or `ens_v0`).
use bigname_storage::NameCurrentRow;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::super::{
    V2Result,
    vocab::{Authority, WrapperFuses, WrapperState},
};
use super::{values::json_timestamp_at_paths, wrapper_metadata};

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
}

/// The `ens_v1` object of a row whose served authority is `authority`. Stored wrapper metadata
/// is validated whatever the authority.
pub(crate) fn ens_v1(
    authority: Option<Authority>,
    declared_summary: &Value,
) -> V2Result<Option<EnsV1>> {
    let wrapper = wrapper_metadata(declared_summary)?;
    if !matches!(authority, Some(Authority::EnsV0 | Authority::EnsV1)) {
        return Ok(None);
    }
    let (wrapper_state, wrapper_fuses) =
        wrapper.map_or((None, None), |(state, fuses)| (Some(state), Some(fuses)));
    Ok(Some(EnsV1 {
        expires_at: Some(json_timestamp_at_paths(
            declared_summary,
            &[&["registration", "ens_v1_expiry"]],
        )),
        wrapper_state,
        wrapper_fuses,
    }))
}

/// The `ens_v1` object of an ENSv1 registry child with no name row, which serves `authority` from
/// its registry.
///
/// A child no label-bearing event named holds only a null expiry. A lease is the BaseRegistrar's
/// `expiries[id]` and token, which a registry `setSubnodeOwner` child never gets
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L142-L147 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f).
///
/// A child whose only name surface is a shadow one (`shadow_surface`: the surface bigname records,
/// but keeps out of name reads, for a name that fails ENSIP-15 normalization) has no name row
/// either, but it can be wrapped or leased. NameWrapper's `setSubnodeOwner` and `setSubnodeRecord` take any label
/// bytes, `_addLabel` checks only the length, and `_wrap` emits `NameWrapped` with that name
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L565-L585 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L596-L630 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L865-L876 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L903 @ ens_v1@91c966f).
/// For a name that fails normalization the wrapper adapter writes a shadow surface instead of a
/// name (crates/adapters/src/schema_v2/protocol/v1/wrapper.rs:133 and :258-267), so its wrapper
/// state, and a `.eth` name's lease, are projected without a composed name. Its object then
/// omits `expires_at` and the wrapper fields rather than claim it has no lease or wrapper state.
pub(crate) fn ens_v1_of_registry_child(
    authority: Option<Authority>,
    shadow_surface: bool,
) -> V2Result<Option<EnsV1>> {
    let ens_v1 = ens_v1(authority, &Value::Object(serde_json::Map::new()))?;
    Ok(ens_v1.map(|ens_v1| {
        if shadow_surface {
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
