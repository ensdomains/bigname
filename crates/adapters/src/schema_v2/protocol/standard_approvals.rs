use alloy_primitives::Address;
use alloy_sol_types::sol;
use serde_json::json;

use super::{EventDraft, Interpreted, v1};
use crate::{
    evm_abi::{address_hex, decode_event_log, u256_word_hex},
    schema_v2::{catalog::Selected, model::RawLogInput, state::State},
};

sol! {
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
    event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
    event Approved(address owner, bytes32 indexed node, address indexed delegate, bool indexed approved);
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Option<Interpreted>> {
    if !bigname_manifests::is_address_scoped_approval(
        &selected.source.source_family,
        &selected.event.signature,
    ) {
        return Ok(None);
    }
    let events = match selected.event.signature.as_str() {
        bigname_manifests::APPROVAL_FOR_ALL_SIGNATURE => {
            let event = decode_event_log::<ApprovalForAll>(
                &raw.topics,
                &raw.data,
                "ApprovalForAll log is malformed",
            )?;
            operator_event(selected, raw, event).into_iter().collect()
        }
        bigname_manifests::APPROVAL_SIGNATURE => {
            let event =
                decode_event_log::<Approval>(&raw.topics, &raw.data, "Approval log is malformed")?;
            wrapper_token_approval(selected, raw, state, event)
        }
        bigname_manifests::APPROVED_SIGNATURE => {
            decode_event_log::<Approved>(&raw.topics, &raw.data, "Approved log is malformed")?;
            Vec::new()
        }
        _ => unreachable!("closed approval watch policy admitted an unknown signature"),
    };
    let mut output = Interpreted::new();
    output.events.extend(events);
    Ok(Some(output))
}

/// Owner-wide operators: an ENSv1/Basenames registry operator controls the owner's registry
/// entries, and a NameWrapper operator passes `canModifyName` and the ERC-1155-fuse approve and
/// transfer checks exactly as the holder does. Project fans only the wrapper relation out.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L108-L118 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L222 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L105-L117 @ ens_v1@91c966f)
fn operator_event(
    selected: &Selected,
    raw: &RawLogInput,
    event: ApprovalForAll,
) -> Option<EventDraft> {
    let (authority_kind, power, owner_change) = match (
        selected.source.source_family.as_str(),
        selected.emitter_role.as_deref(),
    ) {
        ("ens_v1_registry_l1" | "basenames_base_registry", Some("registry" | "registry_old")) => {
            ("registry", "registry_control", "on_registry_owner_change")
        }
        ("ens_v1_wrapper_l1", Some("name_wrapper")) => {
            ("wrapper", "wrapper_control", "on_holder_change")
        }
        _ => return None,
    };
    let owner = address_hex(event.owner);
    let subject = address_hex(event.operator);
    let authority_contract = raw.emitting_address.to_ascii_lowercase();
    let source = json!({"kind": "raw_log", "source_event": "ApprovalForAll"});
    let (powers, grant_source, revocation_source) = if event.approved {
        (json!([power]), source, serde_json::Value::Null)
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
            "{authority_kind}-operator:{}:{authority_contract}:{owner}:{subject}",
            raw.chain_id
        ),
        after_state: json!({
            "subject": subject,
            "relation_kind": "operator",
            "approved": event.approved,
            "scope": {
                "kind": "account",
                "chain_id": raw.chain_id,
                "authority_kind": authority_kind,
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
                owner_change: "ceases_to_apply"
            },
            "source_event": "ApprovalForAll"
        }),
    })
}

/// The NameWrapper per-token approval. `_approve` names `ownerOf(tokenId)` as the owner, an
/// approval to the zero address clears it, and the approved address only passes
/// `canExtendSubnames`.
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L375-L378 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L127-L136 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L228-L238 @ ens_v1@91c966f)
fn wrapper_token_approval(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    event: Approval,
) -> Vec<EventDraft> {
    if selected.source.source_family != "ens_v1_wrapper_l1"
        || selected.emitter_role.as_deref() != Some("name_wrapper")
    {
        return Vec::new();
    }
    let namespace = &selected.source.namespace;
    let namehash = u256_word_hex(event.tokenId);
    let Some(name) = state
        .v1_name(namespace, &namehash)
        .filter(|name| name.authority_source_family == selected.source.source_family)
    else {
        return Vec::new();
    };
    let approved = (event.approved != Address::ZERO).then(|| address_hex(event.approved));
    let previous = state.set_v1_wrapper_delegate(namespace, &namehash, approved.clone());
    if previous.is_some() && previous == approved {
        return Vec::new();
    }
    let context = v1::WrapperPermissionContext {
        name: &name,
        resolver: None,
        chain_id: &raw.chain_id,
        wrapper: raw.emitting_address.to_ascii_lowercase(),
        source_event_kind: "Approval",
        identity_suffix: "Approval",
    };
    let mut output = Interpreted::new();
    if let Some(previous) = previous {
        v1::append_delegate_permission(&mut output, &context, &previous, false);
    }
    if let Some(approved) = approved {
        v1::append_delegate_permission(&mut output, &context, &approved, true);
    }
    output.events
}
