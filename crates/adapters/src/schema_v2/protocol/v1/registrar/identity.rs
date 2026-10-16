use alloy_primitives::{B256, keccak256};

use crate::schema_v2::{
    catalog::Selected,
    common::{namehash, stable_uuid},
    model::RawLogInput,
};

pub(in crate::schema_v2) fn registrar_namehash(selected: &Selected, labelhash: B256) -> String {
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

/// The node of a registration log and, for a numeric BaseRegistrar `NameRegistered`, its
/// registrant. A controller's `NameRegistered` comes after every registry write of its
/// registration, so it has no registrant here. Block reconciliation moves the registry-only
/// grants of the writes each one takes over to its lease.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L301-L341 @ ens_v1@91c966f)
pub(in crate::schema_v2) fn registration(
    selected: &Selected,
    raw: &RawLogInput,
) -> Option<(String, Option<String>)> {
    let lifecycle = selected
        .event
        .normalized_events
        .iter()
        .any(|event| event == "RegistrationGranted");
    if !lifecycle || selected.event.name != "NameRegistered" {
        return None;
    }
    match selected.source.source_family.as_str() {
        "ens_v1_registrar_l1"
            if selected.emitter_role.as_deref() == Some("registrar")
                && selected.event.signature == "NameRegistered(uint256,address,uint256)" =>
        {
            super::base::registration_namehash(selected, raw)
                .ok()
                .flatten()
                .map(|(namehash, owner)| (namehash, Some(owner)))
        }
        "ens_v1_registrar_l1" | "basenames_base_registrar" => super::decode::name(selected, raw)
            .ok()
            .map(|(_, labelhash, _)| (registrar_namehash(selected, labelhash), None)),
        _ => None,
    }
}
