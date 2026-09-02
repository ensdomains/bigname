use alloy_primitives::{B256, keccak256};

pub(super) fn child_node(parent: B256, labelhash: B256) -> String {
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(parent.as_slice());
    input[32..].copy_from_slice(labelhash.as_slice());
    format!("{:#x}", keccak256(input))
}
