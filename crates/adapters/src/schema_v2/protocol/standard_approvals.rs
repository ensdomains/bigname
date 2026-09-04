use alloy_sol_types::sol;
use serde_json::json;

use super::{EventDraft, Interpreted};
use crate::{
    evm_abi::{address_hex, decode_event_log},
    schema_v2::{catalog::Selected, model::RawLogInput},
};

sol! {
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
    event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
    event Approved(address owner, bytes32 indexed node, address indexed delegate, bool indexed approved);
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
) -> anyhow::Result<Option<Interpreted>> {
    if !bigname_manifests::is_address_scoped_approval(
        &selected.source.source_family,
        &selected.event.signature,
    ) {
        return Ok(None);
    }
    let event = match selected.event.signature.as_str() {
        bigname_manifests::APPROVAL_FOR_ALL_SIGNATURE => {
            let event = decode_event_log::<ApprovalForAll>(
                &raw.topics,
                &raw.data,
                "ApprovalForAll log is malformed",
            )?;
            registry_operator_event(selected, raw, event)
        }
        bigname_manifests::APPROVAL_SIGNATURE => {
            decode_event_log::<Approval>(&raw.topics, &raw.data, "Approval log is malformed")?;
            None
        }
        bigname_manifests::APPROVED_SIGNATURE => {
            decode_event_log::<Approved>(&raw.topics, &raw.data, "Approved log is malformed")?;
            None
        }
        _ => unreachable!("closed approval watch policy admitted an unknown signature"),
    };
    let mut output = Interpreted::new();
    output.events.extend(event);
    Ok(Some(output))
}

fn registry_operator_event(
    selected: &Selected,
    raw: &RawLogInput,
    event: ApprovalForAll,
) -> Option<EventDraft> {
    if !matches!(
        selected.source.source_family.as_str(),
        "ens_v1_registry_l1" | "basenames_base_registry"
    ) || !matches!(
        selected.emitter_role.as_deref(),
        Some("registry" | "registry_old")
    ) {
        return None;
    }
    let owner = address_hex(event.owner);
    let subject = address_hex(event.operator);
    let authority_contract = raw.emitting_address.to_ascii_lowercase();
    let source = json!({"kind": "raw_log", "source_event": "ApprovalForAll"});
    let (powers, grant_source, revocation_source) = if event.approved {
        (json!(["registry_control"]), source, serde_json::Value::Null)
    } else {
        (json!([]), json!({}), source)
    };
    Some(EventDraft {
        event_kind: "AccountPermissionChanged".to_owned(),
        logical_name_id: None,
        resource_id: None,
        identity_suffix: format!("AccountPermissionChanged:{owner}:{subject}"),
        explicit_before: None,
        state_scope: format!(
            "registry-operator:{}:{authority_contract}:{owner}:{subject}",
            raw.chain_id
        ),
        after_state: json!({
            "subject": subject,
            "relation_kind": "operator",
            "approved": event.approved,
            "scope": {
                "kind": "account",
                "chain_id": raw.chain_id,
                "authority_kind": "registry",
                "authority_contract": authority_contract,
                "authority_contract_instance_id": selected.contract_instance_id.to_string(),
                "owner": owner,
            },
            "effective_powers": powers,
            "grant_source": grant_source,
            "revocation_source": revocation_source,
            "inheritance_path": [],
            "transfer_behavior": {
                "mode": "owner_scoped",
                "on_registry_owner_change": "ceases_to_apply"
            },
            "source_event": "ApprovalForAll"
        }),
    })
}
