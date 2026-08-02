use alloy_primitives::{B256, keccak256};

use crate::schema_v2::{
    catalog::Selected,
    common::{namehash, stable_uuid},
    model::RawLogInput,
};

pub(super) fn registrar_namehash(selected: &Selected, labelhash: B256) -> String {
    let suffix = if selected.source.source_family == "basenames_base_registrar" {
        vec!["base".to_owned(), "eth".to_owned()]
    } else {
        vec!["eth".to_owned()]
    };
    let parent = namehash(&suffix)
        .parse::<B256>()
        .expect("namehash helper returns a bytes32 value");
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(parent.as_slice());
    input[32..].copy_from_slice(labelhash.as_slice());
    format!("{:#x}", keccak256(input))
}

pub(super) fn new_registrar_identity(
    selected: &Selected,
    raw: &RawLogInput,
    labelhash: &str,
) -> (uuid::Uuid, uuid::Uuid, Option<String>) {
    let authority_key = format!(
        "registrar:{}:{}:{}:{}:{}",
        raw.chain_id, selected.source.manifest_id, labelhash, raw.block_hash, raw.log_index,
    );
    let token_lineage_id = stable_uuid(&format!("token-lineage:{authority_key}"));
    let resource_id = stable_uuid(&format!("resource:{authority_key}"));
    (token_lineage_id, resource_id, Some(authority_key))
}
