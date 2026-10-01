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
    /// The BaseRegistrar lease's own expiry, null without a lease (a subname). After the
    /// Universal Resolver cutover the top-level `expires_at` of a reserved `.eth` name is its
    /// ENSv2 entry's instead.
    pub(crate) expires_at: Option<String>,
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
        expires_at: json_timestamp_at_paths(
            declared_summary,
            &[&["registration", "ens_v1_expiry"]],
        ),
        wrapper_state,
        wrapper_fuses,
    }))
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
