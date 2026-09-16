use alloy_primitives::{U256, hex, keccak256};
use alloy_sol_types::sol;
use serde_json::{Value, json};
use uuid::Uuid;

use super::super::{
    Interpreted, ResourceDraft, ensure_declared,
    permissions::{V2PermissionState, V2Vocabulary, v2_states},
    v2_resolver::single_event,
};
use crate::{
    evm_abi::{address_hex, decode_event_log, hex_string, u256_word_hex},
    schema_v2::{
        catalog::Selected,
        common::{ens_v2_resolver_resource_id, event_string_selector},
        model::{PriorEventInput, RawLogInput},
        state::State,
    },
};

sol! {
    event ResourceArgument(uint256 indexed resource, bytes arg);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
}

pub(super) fn argument(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let e = decode_event_log::<ResourceArgument>(
        &raw.topics,
        &raw.data,
        "ResourceArgument log is malformed",
    )?;
    if e.resource == U256::ZERO || U256::from_be_bytes(*keccak256(&e.arg)) != e.resource {
        return Ok(Interpreted::new());
    }
    ensure_declared(selected, &["ResolverPermissionArgument"])?;
    let resource = u256_word_hex(e.resource);
    state.v2_resolver_arguments.insert(
        (selected.contract_instance_id, resource.clone()),
        e.arg.to_vec(),
    );
    let mut output = single_event(
        "ResolverPermissionArgument",
        None,
        None,
        json!({
            "source_event":"ResourceArgument", "resolver":raw.emitting_address,
            "resolver_contract_instance_id":selected.contract_instance_id.to_string(),
            "upstream_resource":resource, "argument_hex":hex_string(e.arg),
        }),
    );
    output.events[0].state_scope = format!("{}:argument:{resource}", selected.contract_instance_id);
    Ok(output)
}

pub(in crate::schema_v2) fn restore(state: &mut State, event: &PriorEventInput) {
    if event.source_family != "ens_v2_resolver_l1"
        || event.event_kind != "ResolverPermissionArgument"
    {
        return;
    }
    let after = &event.after_state;
    if let (Some(id), Some(resource), Some(arg)) = (
        after
            .get("resolver_contract_instance_id")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<Uuid>().ok()),
        after.get("upstream_resource").and_then(Value::as_str),
        after
            .get("argument_hex")
            .and_then(Value::as_str)
            .and_then(|s| hex::decode(s).ok()),
    ) && u256_word_hex(U256::from_be_bytes(*keccak256(&arg))) == resource
    {
        state
            .v2_resolver_arguments
            .insert((id, resource.to_owned()), arg);
    }
}

pub(super) fn permission(
    selected: &Selected,
    raw: &RawLogInput,
    state: &State,
) -> anyhow::Result<Interpreted> {
    let e = decode_event_log::<EACRolesChanged>(
        &raw.topics,
        &raw.data,
        "EACRolesChanged log is malformed",
    )?;
    ensure_declared(selected, &["PermissionChanged"])?;
    let upstream_resource = u256_word_hex(e.resource);
    let resource_id = ens_v2_resolver_resource_id(
        &raw.chain_id,
        selected.contract_instance_id,
        &upstream_resource,
    );
    let arg = state
        .v2_resolver_arguments
        .get(&(selected.contract_instance_id, upstream_resource.clone()));
    let selector = arg.map_or_else(
        || json!({"kind":"resource", "key":Value::Null, "hash":Value::Null}),
        |arg| argument_selector(arg, e.oldRoleBitmap | e.newRoleBitmap, &upstream_resource),
    );
    let (before, after) = v2_states(
        selected,
        raw,
        V2Vocabulary::RecordResolver,
        V2PermissionState {
            upstream_resource: &upstream_resource,
            account: address_hex(e.account),
            old_bitmap: e.oldRoleBitmap,
            new_bitmap: e.newRoleBitmap,
            root_resource: e.resource == U256::ZERO,
            selector,
        },
    );
    let mut output = single_event("PermissionChanged", None, Some(resource_id), after);
    output.events[0].explicit_before = Some(before);
    output.events[0].state_scope = format!(
        "{}:permission:{upstream_resource}:{}",
        selected.contract_instance_id,
        address_hex(e.account)
    );
    output.resources.push(ResourceDraft {
        resource_id,
        token_lineage_id: None,
    });
    Ok(output)
}

// An argument may authorize several setters; preserve every applicable selector without a name.
// (upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/PermissionedResolver.sol:L307 @ ens_v2_sepolia_20260903@5da83f6a)
fn argument_selector(arg: &[u8], bitmap: U256, resource: &str) -> Value {
    let mut selectors = Vec::new();
    for (bit, family) in [
        (0, "addr"),
        (4, "text"),
        (12, "abi"),
        (16, "interface"),
        (24, "data"),
    ] {
        if !bitmap.bit(bit) && !bitmap.bit(bit + 128) {
            continue;
        }
        let selector = match family {
            "text" | "data" => {
                let s = event_string_selector(family, arg);
                let mut value = json!({"kind":family, "key":s.selector_key, "hash":resource});
                s.retain_raw_selector(&mut value);
                value
            }
            "addr" | "abi" if arg.len() == 32 => {
                json!({"kind":if family == "addr" {"address"} else {"abi"},
                "key":U256::from_be_slice(arg).to_string(), "hash":resource})
            }
            "interface" if arg.len() == 4 => {
                json!({"kind":"interface", "key":hex_string(arg), "hash":resource})
            }
            _ => continue,
        };
        selectors.push(selector);
    }
    if selectors.len() == 1 {
        return selectors.remove(0);
    }
    json!({"kind":"argument", "key":Value::Null, "hash":resource, "argument_hex":hex_string(arg), "selectors":selectors})
}

// (upstream: .refs/ens_v2_sepolia_20260903/contracts/src/resolver/libraries/PermissionedResolverLib.sol:L10 @ ens_v2_sepolia_20260903@5da83f6a)
pub(in crate::schema_v2::protocol) const ROLE_BITS: &[(usize, &str)] = &[
    (0, "set_addr"),
    (4, "set_text"),
    (8, "set_contenthash"),
    (12, "set_abi"),
    (16, "set_interface"),
    (20, "set_name"),
    (24, "set_data"),
    (28, "link"),
    (120, "can_name"),
    (124, "upgrade"),
    (128, "admin_set_addr"),
    (132, "admin_set_text"),
    (136, "admin_set_contenthash"),
    (140, "admin_set_abi"),
    (144, "admin_set_interface"),
    (148, "admin_set_name"),
    (152, "admin_set_data"),
    (156, "admin_link"),
    (248, "admin_can_name"),
    (252, "admin_upgrade"),
];
