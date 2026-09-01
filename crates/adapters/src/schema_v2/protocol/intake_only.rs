use alloy_sol_types::sol;

use super::Interpreted;
use crate::{
    evm_abi::decode_event_log,
    schema_v2::{catalog::Selected, model::RawLogInput},
};

sol! {
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
    event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
    event Approved(address owner, bytes32 indexed node, address indexed delegate, bool indexed approved);
}

pub(super) fn approval(
    selected: &Selected,
    raw: &RawLogInput,
) -> anyhow::Result<Option<Interpreted>> {
    if !bigname_manifests::is_address_scoped_approval(
        &selected.source.source_family,
        &selected.event.signature,
    ) {
        return Ok(None);
    }
    match selected.event.signature.as_str() {
        bigname_manifests::APPROVAL_FOR_ALL_SIGNATURE => {
            decode_event_log::<ApprovalForAll>(
                &raw.topics,
                &raw.data,
                "ApprovalForAll log is malformed",
            )?;
        }
        bigname_manifests::APPROVAL_SIGNATURE => {
            decode_event_log::<Approval>(&raw.topics, &raw.data, "Approval log is malformed")?;
        }
        bigname_manifests::APPROVED_SIGNATURE => {
            decode_event_log::<Approved>(&raw.topics, &raw.data, "Approved log is malformed")?;
        }
        _ => unreachable!("closed approval watch policy admitted an unknown signature"),
    }
    Ok(Some(Interpreted::new()))
}
