use alloy_sol_types::sol;
use anyhow::bail;
use serde_json::json;

use super::super::{Interpreted, NameDraft, ResourceDraft, ShadowNameDraft, ensure_declared};
use super::registry::{append_authority_transition, authority_kind};
use super::support::{events_linked, single_event};
use crate::evm_abi::{address_hex, decode_event_log, hex_string};
use crate::schema_v2::{
    catalog::Selected,
    common::{decode_dns_labels, namehash_raw, stable_uuid, surface_labels},
    model::RawLogInput,
    state::State,
};

sol! {
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
    event ExpiryExtended(bytes32 indexed node, uint64 expiry);
    event FusesSet(bytes32 indexed node, uint32 fuses);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
}

mod permissions;
mod transfer;
use permissions::append_holder_permissions;
pub(in crate::schema_v2) use permissions::{WrapperPermissionContext, append_delegate_permission};
use transfer::{transfer_batch, transfer_single};

const CANNOT_UNWRAP: u32 = 1;
const CANNOT_APPROVE: u32 = 64;
const PARENT_CANNOT_CONTROL: u32 = 1 << 16;

fn wrapper_state(fuses: u32) -> &'static str {
    if fuses & CANNOT_UNWRAP != 0 {
        "locked"
    } else if fuses & PARENT_CANNOT_CONTROL != 0 {
        "emancipated"
    } else {
        "wrapped"
    }
}

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    match selected.event.name.as_str() {
        "NameWrapped" => name_wrapped(selected, raw, state),
        "NameUnwrapped" => name_unwrapped(selected, raw, state),
        "ExpiryExtended" => {
            let event = decode_event_log::<ExpiryExtended>(
                &raw.topics,
                &raw.data,
                "ExpiryExtended log is malformed",
            )?;
            ensure_declared(selected, &["ExpiryChanged"])?;
            let node = hex_string(event.node);
            let transition =
                state.update_v1_wrapper_expiry(&selected.source.namespace, &node, event.expiry);
            let explicit_before = transition.as_ref().map(|(expiry, linked)| {
                json!({
                    "authority_key":linked.authority_key,
                    "authority_kind":authority_kind(linked),
                    "node":node,
                    "expiry":expiry,
                })
            });
            let linked = transition.map(|(_, linked)| linked);
            let mut output = single_event(
                "ExpiryChanged",
                linked.as_ref().map(|state| state.logical_name_id.clone()),
                linked.as_ref().map(|state| state.resource_id),
                json!({"source_event":"ExpiryExtended","node":node,"expiry":event.expiry}),
            );
            output.events[0].explicit_before = explicit_before;
            Ok(output)
        }
        "FusesSet" => {
            let event =
                decode_event_log::<FusesSet>(&raw.topics, &raw.data, "FusesSet log is malformed")?;
            ensure_declared(selected, &["PermissionScopeChanged"])?;
            let node = hex_string(event.node);
            let linked = state.v1_name(&selected.source.namespace, &node);
            let transition =
                state.set_v1_wrapper_fuses(&selected.source.namespace, &node, event.fuses);
            let previous = transition.map(|(previous, _)| previous);
            let expiry = transition.map(|(_, data)| data.expiry);
            let mut output = single_event(
                "PermissionScopeChanged",
                linked.as_ref().map(|state| state.logical_name_id.clone()),
                linked.as_ref().map(|state| state.resource_id),
                json!({
                    "source_event":"FusesSet",
                    "node":node,
                    "fuses":event.fuses,
                    "wrapper_state":wrapper_state(event.fuses),
                    "expiry":expiry,
                }),
            );
            output.events[0].explicit_before = previous.map(|data| {
                json!({
                    "authority_key":linked.as_ref().and_then(|state| state.authority_key.clone()),
                    "authority_kind":linked.as_ref().map(authority_kind),
                    "node":node,
                    "fuses":data.fuses,
                    "wrapper_state":wrapper_state(data.fuses),
                    "expiry":data.expiry,
                })
            });
            Ok(output)
        }
        "TransferSingle" => transfer_single(selected, raw, state),
        "TransferBatch" => transfer_batch(selected, raw, state),
        name => bail!("unsupported wrapper event {name}"),
    }
}

fn name_wrapped(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event =
        decode_event_log::<NameWrapped>(&raw.topics, &raw.data, "NameWrapped log is malformed")?;
    let raw_labels = decode_dns_labels(&event.name)?;
    let raw_namehash = namehash_raw(raw_labels.iter().map(Vec::as_slice));
    if raw_namehash != hex_string(event.node) {
        bail!("NameWrapped DNS name does not match its node");
    }
    let labels = surface_labels(&raw_labels);
    let surface_known = labels.is_some();
    let authority_key = format!(
        "wrapper:{}:{}:{}:{}:{}",
        raw.chain_id, selected.source.manifest_id, raw_namehash, raw.block_hash, raw.log_index,
    );
    let resource_id = stable_uuid(&format!("resource:{authority_key}"));
    let token_lineage_id = stable_uuid(&format!("token-lineage:{authority_key}"));
    let logical_name_id = format!("{}:{raw_namehash}", selected.source.namespace);
    let previous = state.v1_name(&selected.source.namespace, &raw_namehash);
    // A freshly minted token carries no approval: `_burn` cleared any earlier one.
    // (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L275 @ ens_v1@91c966f)
    state.set_v1_wrapper_delegate(&selected.source.namespace, &raw_namehash, None);
    state.set_v1_wrapper_burnt(&selected.source.namespace, &raw_namehash, false);
    let wrapper_data = state.wrap_v1_name(
        &selected.source.namespace,
        &raw_namehash,
        event.fuses,
        event.expiry,
        raw.block_timestamp.unix_timestamp(),
    );
    state.observe_v1_name(
        &selected.source.namespace,
        &raw_namehash,
        logical_name_id.clone(),
        surface_known,
        resource_id,
        Some(token_lineage_id),
        selected.source.source_family.clone(),
        Some(i64::try_from(wrapper_data.expiry).unwrap_or(i64::MAX)),
        Some(address_hex(event.owner)),
        Some(authority_key.clone()),
    );
    let kinds = vec![
        "TokenControlTransferred",
        "ExpiryChanged",
        "PermissionScopeChanged",
    ];
    ensure_declared(selected, &["TokenControlTransferred"])?;
    let after = json!({"source_event":"NameWrapped","node":raw_namehash,"owner":address_hex(event.owner),"fuses":wrapper_data.fuses,"wrapper_state":wrapper_state(wrapper_data.fuses),"expiry":wrapper_data.expiry,"token_lineage_id":token_lineage_id.to_string(),"authority_kind":"wrapper","authority_key":authority_key.clone(),"surface_known":surface_known});
    let mut output = events_linked(kinds, logical_name_id, resource_id, after.clone());
    if let Some(transfer) = output
        .events
        .iter_mut()
        .find(|event| event.event_kind == "TokenControlTransferred")
    {
        transfer.explicit_before = Some(json!({
            "from": previous.as_ref().and_then(|state| state.owner.clone()),
            "authority_kind": previous.as_ref().map(|state| {
                if state.authority_source_family == "ens_v1_wrapper_l1" {
                    "wrapper"
                } else {
                    "registrar"
                }
            }),
        }));
        let after = transfer
            .after_state
            .as_object_mut()
            .expect("wrapper transfer state is an object");
        after.insert("to".to_owned(), json!(event.owner));
        after.remove("owner");
    }
    if let Some(expiry) = output
        .events
        .iter_mut()
        .find(|event| event.event_kind == "ExpiryChanged")
    {
        expiry.explicit_before = Some(json!({
            "expiry":previous.as_ref().and_then(|state| state.expiry),
        }));
    }
    append_authority_transition(
        &mut output,
        super::authority_arm(&selected.source.namespace),
        previous.as_ref(),
        state
            .v1_name(&selected.source.namespace, &raw_namehash)
            .as_ref(),
        None,
        raw,
        &after,
        state.v1_resolver_link(&selected.source.namespace, &raw_namehash),
        None,
    );
    if let Some(name) = state.v1_name(&selected.source.namespace, &raw_namehash) {
        let context = WrapperPermissionContext {
            name: &name,
            resolver: state.v1_resolver(&selected.source.namespace, &raw_namehash),
            chain_id: &raw.chain_id,
            wrapper: raw.emitting_address.to_ascii_lowercase(),
            source_event_kind: "NameWrapped",
            identity_suffix: "NameWrapped",
        };
        append_holder_permissions(&mut output, &context, &address_hex(event.owner), true);
    }
    if let Some(labels) = labels {
        output.names.push(NameDraft {
            labels,
            namehash: raw_namehash,
            resource_id: Some(resource_id),
            token_lineage_id: Some(token_lineage_id),
            surface_binding_id: Some(stable_uuid(&format!(
                "binding:{authority_key}:{}",
                raw.block_timestamp.unix_timestamp()
            ))),
            bind: false,
            binding_kind: "observed_only".to_owned(),
            authority_arm: "ens_v1".to_owned(),
            source_kind: "NameWrapped_name".to_owned(),
            preimage_metadata: None,
        });
    } else {
        output.shadow_names.push(ShadowNameDraft {
            raw_labels,
            namehash: raw_namehash,
            source_kind: "NameWrapped_name".to_owned(),
        });
        output.resources.push(ResourceDraft {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
        });
    }
    Ok(output)
}

fn name_unwrapped(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<NameUnwrapped>(
        &raw.topics,
        &raw.data,
        "NameUnwrapped log is malformed",
    )?;
    ensure_declared(selected, &["SurfaceUnbound"])?;
    let namehash = hex_string(event.node);
    let wrapper_resolver = state.v1_resolver(&selected.source.namespace, &namehash);
    let delegate = state.set_v1_wrapper_delegate(&selected.source.namespace, &namehash, None);
    let holder_revoked_by_burn =
        state.set_v1_wrapper_burnt(&selected.source.namespace, &namehash, false);
    state.note_v1_unwrap(
        &selected.source.namespace,
        &namehash,
        &raw.emitting_address,
        raw,
    );
    let linked = state.release_v1_name(&selected.source.namespace, &namehash);
    let reactivated = state.reactivate_v1_registrar(
        &selected.source.namespace,
        &namehash,
        raw.block_timestamp.unix_timestamp(),
    );
    let resolver = state.v1_resolver_for_activation(
        &selected.source.namespace,
        &namehash,
        reactivated.as_ref(),
    );
    let after = json!({
        "source_event":"NameUnwrapped",
        "node":namehash,
        "owner":address_hex(event.owner),
        "unwrapped_at":raw.block_timestamp.unix_timestamp(),
        "reactivated_resource_id":reactivated.as_ref().map(|state| state.resource_id.to_string()),
        "reactivated_token_lineage_id":reactivated.as_ref().and_then(|state| state.token_lineage_id).map(|id| id.to_string()),
    });
    let mut output = Interpreted::new();
    append_authority_transition(
        &mut output,
        super::authority_arm(&selected.source.namespace),
        linked.as_ref(),
        reactivated.as_ref(),
        None,
        raw,
        &after,
        resolver,
        None,
    );
    if let Some(name) = linked
        .as_ref()
        .filter(|name| name.authority_source_family == selected.source.source_family)
    {
        let context = WrapperPermissionContext {
            name,
            resolver: wrapper_resolver,
            chain_id: &raw.chain_id,
            wrapper: raw.emitting_address.to_ascii_lowercase(),
            source_event_kind: "NameUnwrapped",
            identity_suffix: "NameUnwrapped",
        };
        if let Some(owner) = name.owner.as_deref()
            && !holder_revoked_by_burn
        {
            append_holder_permissions(&mut output, &context, owner, false);
        }
        if let Some(delegate) = delegate {
            append_delegate_permission(&mut output, &context, &delegate, false);
        }
    }
    Ok(output)
}
