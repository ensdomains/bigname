//! ERC-1155 transfers and burns of wrapped names: holder rows follow the token, the per-token
//! approval is cleared unless CANNOT_APPROVE is burnt, and a burn records itself so the
//! NameUnwrapped that follows it revokes nothing twice.
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L137-L197 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L815-L840 @ ens_v1@91c966f)

use alloy_primitives::{Address, U256};
use anyhow::bail;
use serde_json::json;

use super::permissions::{
    WrapperPermissionContext, append_delegate_permission, append_holder_permissions,
};
use super::{CANNOT_APPROVE, TransferBatch, TransferSingle};
use crate::evm_abi::{address_hex, decode_event_log, u256_word_hex};
use crate::schema_v2::protocol::v1::support::single_event;
use crate::schema_v2::protocol::{Interpreted, ensure_declared};
use crate::schema_v2::{catalog::Selected, model::RawLogInput, state::State};

pub(super) fn transfer_single(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<TransferSingle>(
        &raw.topics,
        &raw.data,
        "TransferSingle log is malformed",
    )?;
    ensure_declared(selected, &["TokenControlTransferred"])?;
    Ok(transfer_item(
        selected,
        raw,
        state,
        event.operator,
        event.from,
        event.to,
        event.id,
        event.value,
        "TransferSingle".to_owned(),
    ))
}

pub(super) fn transfer_batch(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    let event = decode_event_log::<TransferBatch>(
        &raw.topics,
        &raw.data,
        "TransferBatch log is malformed",
    )?;
    if event.ids.len() != event.values.len() {
        bail!("TransferBatch ids and values differ in length");
    }
    ensure_declared(selected, &["TokenControlTransferred"])?;
    let mut output = Interpreted::new();
    for (index, (id, value)) in event.ids.into_iter().zip(event.values).enumerate() {
        let mut item = transfer_item(
            selected,
            raw,
            state,
            event.operator,
            event.from,
            event.to,
            id,
            value,
            format!("TransferBatch:{index}"),
        );
        output.events.append(&mut item.events);
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn transfer_item(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    operator: Address,
    from: Address,
    to: Address,
    id: U256,
    value: U256,
    identity_suffix: String,
) -> Interpreted {
    if value != U256::from(1) || from == Address::ZERO {
        return Interpreted::new();
    }
    let namehash = u256_word_hex(id);
    let namespace = &selected.source.namespace;
    let source_event_kind = identity_suffix
        .split(':')
        .next()
        .unwrap_or("TransferSingle")
        .to_owned();
    let wrapper = raw.emitting_address.to_ascii_lowercase();
    if to == Address::ZERO {
        return burn(
            selected,
            raw,
            state,
            &namehash,
            wrapper,
            &source_event_kind,
            &identity_suffix,
        );
    }
    let Some((before, linked)) = state.transfer_v1_wrapper_owner(
        namespace,
        &namehash,
        &selected.source.source_family,
        address_hex(to),
    ) else {
        return Interpreted::new();
    };
    let mut output = single_event(
        "TokenControlTransferred",
        Some(linked.logical_name_id.clone()),
        Some(linked.resource_id),
        json!({
            "source_event": identity_suffix.split(':').next().unwrap_or("TransferSingle"),
            "operator": address_hex(operator),
            "to": address_hex(to),
            "id": namehash,
            "namehash": namehash,
            "value": value.to_string(),
        }),
    );
    output.events[0].explicit_before = Some(json!({"from": address_hex(from)}));
    output.events[0].identity_suffix = format!("{identity_suffix}:{namehash}");
    let context = WrapperPermissionContext {
        name: &linked,
        resolver: state.v1_resolver(namespace, &namehash),
        chain_id: &raw.chain_id,
        wrapper,
        source_event_kind: &source_event_kind,
        identity_suffix: &identity_suffix,
    };
    // `_beforeTransfer` deletes the token approval unless CANNOT_APPROVE is burnt, judged on the
    // expiry-cleared fuse word. The revocation is emitted even when the delegate is the recipient,
    // so a restore that rebuilt the delegate from these rows replays identically, and it is
    // emitted BEFORE the holder rows: Project folds rows by (resource, subject, scope) and keeps
    // the newest, so the recipient's holder grant must be the later row when it is the delegate.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L837-L840 @ ens_v1@91c966f)
    let fuses = state
        .v1_wrapper_effective_fuses(namespace, &namehash, raw.block_timestamp.unix_timestamp())
        .unwrap_or(0);
    if fuses & CANNOT_APPROVE == 0
        && let Some(delegate) = state.set_v1_wrapper_delegate(namespace, &namehash, None)
    {
        append_delegate_permission(&mut output, &context, &delegate, false);
    }
    if let (Some(from_owner), Some(to_owner)) = (before.owner.as_deref(), linked.owner.as_deref())
        && !from_owner.eq_ignore_ascii_case(to_owner)
    {
        append_holder_permissions(&mut output, &context, from_owner, false);
        append_holder_permissions(&mut output, &context, to_owner, true);
        // A delegate retained under CANNOT_APPROVE who was also the outgoing holder keeps
        // `getApproved` naming it, so its token-approval grant is re-emitted after the holder
        // revocation; otherwise the empty revocation would be its newest row.
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L108-L121 @ ens_v1@91c966f)
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L228-L238 @ ens_v1@91c966f)
        if let Some(delegate) = state.v1_wrapper_delegate(namespace, &namehash)
            && delegate.eq_ignore_ascii_case(from_owner)
        {
            append_delegate_permission(&mut output, &context, &delegate, true);
        }
    }
    output
}

// Every burn clears the token approval, and every burn except the un-admitted upgrade path is
// followed by NameUnwrapped; revoking here keeps an upgraded name from retaining a live holder,
// and the recorded burn keeps the following NameUnwrapped from revoking the holder twice.
// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269-L278 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L483-L509 @ ens_v1@91c966f)
fn burn(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    namehash: &str,
    wrapper: String,
    source_event_kind: &str,
    identity_suffix: &str,
) -> Interpreted {
    let mut output = Interpreted::new();
    let namespace = &selected.source.namespace;
    let Some(name) = state
        .v1_name(namespace, namehash)
        .filter(|name| name.authority_source_family == selected.source.source_family)
    else {
        return output;
    };
    let context = WrapperPermissionContext {
        name: &name,
        resolver: state.v1_resolver(namespace, namehash),
        chain_id: &raw.chain_id,
        wrapper,
        source_event_kind,
        identity_suffix,
    };
    if let Some(owner) = name.owner.as_deref() {
        append_holder_permissions(&mut output, &context, owner, false);
        state.set_v1_wrapper_burnt(namespace, namehash, true);
    }
    if let Some(delegate) = state.set_v1_wrapper_delegate(namespace, namehash, None) {
        append_delegate_permission(&mut output, &context, &delegate, false);
    }
    output
}
