use crate::{
    evm_abi::{address_hex, decode_event_log_tolerant_address_word},
    schema_v2::{catalog::Selected, model::RawLogInput},
};

use super::{NewOwner, Transfer, child_node, unmasked_word};

pub(in crate::schema_v2) fn registration_setup_node(
    selected: &Selected,
    raw: &RawLogInput,
) -> anyhow::Result<Option<(String, String)>> {
    if selected.emitter_role.as_deref() != Some("registry") {
        return Ok(None);
    }
    let tolerate_unmasked_words = selected.source.source_family == "ens_v1_registry_l1";
    match selected.event.name.as_str() {
        "NewOwner" => {
            let decoded = unmasked_word::decode_registry_event::<NewOwner>(
                tolerate_unmasked_words,
                &raw.topics,
                &raw.data,
                "NewOwner log is malformed",
                decode_event_log_tolerant_address_word::<NewOwner>,
            )?;
            Ok(Some((
                child_node(decoded.event.node, decoded.event.label),
                address_hex(decoded.event.owner),
            )))
        }
        "Transfer" => {
            let decoded = unmasked_word::decode_registry_event::<Transfer>(
                tolerate_unmasked_words,
                &raw.topics,
                &raw.data,
                "registry Transfer log is malformed",
                decode_event_log_tolerant_address_word::<Transfer>,
            )?;
            Ok(Some((
                format!("{:#x}", decoded.event.node),
                address_hex(decoded.event.owner),
            )))
        }
        _ => Ok(None),
    }
}
