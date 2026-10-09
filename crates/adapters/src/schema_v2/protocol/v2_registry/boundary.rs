//! ENSv2 name transitions settled at a block boundary. A crossed expiry has no log, so the
//! drafts are built against a stand-in log that carries the block and no transaction.
use super::topology::append_v2_name_transitions;
use crate::schema_v2::{
    RawBlockInput, RawLogInput, protocol::Interpreted, state::V2NameTransition,
};

const SOURCE_EVENT: &str = "RegistryPathExpired";

fn boundary_raw(block: &RawBlockInput, registry: &str) -> RawLogInput {
    RawLogInput {
        chain_id: block.chain_id.clone(),
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
        block_timestamp: block.block_timestamp,
        canonicality_state: block.canonicality_state.clone(),
        transaction_hash: format!("block-boundary:{}", block.block_hash),
        transaction_index: -1,
        log_index: -1,
        emitting_address: registry.to_owned(),
        topics: Vec::new(),
        data: Vec::new(),
    }
}

pub(in crate::schema_v2) fn boundary_reassertion(
    transition: &V2NameTransition,
    block: &RawBlockInput,
) -> Option<Interpreted> {
    if transition.current.is_none()
        || transition.previous != transition.current
        || transition.previous_shadow != transition.current_shadow
    {
        return None;
    }
    let raw = boundary_raw(block, &transition.registry);
    let mut output = Interpreted::new();
    append_v2_name_transitions(
        &mut output,
        vec![transition.clone()],
        &raw,
        SOURCE_EVENT,
        None,
    );
    Some(output)
}

/// The two halves of a boundary transition that is not a reassertion. `released` is what the
/// expiry ends: the token's old path, or the token itself when it had no name. `granted`
/// holds the drafts that name the token under its new path, with the stand-in log they were
/// built against.
pub(in crate::schema_v2) struct BoundaryMove {
    pub released: Option<V2NameTransition>,
    pub granted: Option<(Interpreted, RawLogInput)>,
}

/// Splits a boundary transition that names a token under another path. An expiry can do that
/// in two ways. The parent token that named the token's registry expires while another
/// parent token still points at the registry. Or a claimed parent token expires and the
/// registry falls back to a mount path. A transition that leaves the token unnamed is
/// released whole and grants nothing.
pub(in crate::schema_v2) fn split_boundary_move(
    transition: V2NameTransition,
    block: &RawBlockInput,
) -> BoundaryMove {
    if transition.current.is_none() && transition.current_shadow.is_none() {
        return BoundaryMove {
            released: Some(transition),
            granted: None,
        };
    }
    let raw = boundary_raw(block, &transition.registry);
    let named_before = transition.previous.is_some() || transition.previous_shadow.is_some();
    let released = named_before.then(|| V2NameTransition {
        current: None,
        current_shadow: None,
        ..transition.clone()
    });
    let mut granted = Interpreted::new();
    let grant = V2NameTransition {
        previous: None,
        previous_shadow: None,
        ..transition
    };
    append_v2_name_transitions(&mut granted, vec![grant], &raw, SOURCE_EVENT, None);
    BoundaryMove {
        released,
        granted: Some((granted, raw)),
    }
}
