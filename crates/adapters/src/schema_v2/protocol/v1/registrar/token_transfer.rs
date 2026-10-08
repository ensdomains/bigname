//! Registrar token movement and its authority handoff, including a complete wrapper callback.
use super::*;

pub(super) fn interpret(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
) -> anyhow::Result<Interpreted> {
    ensure_declared(selected, &["TokenControlTransferred"])?;
    let event = decode_event_log::<transfer::Transfer>(
        &raw.topics,
        &raw.data,
        "registrar Transfer log is malformed",
    )?;
    let from = address_hex(event.from);
    let to = address_hex(event.to);
    if from == ZERO_ADDRESS || to == ZERO_ADDRESS {
        return Ok(Interpreted::new());
    }
    let labelhash = B256::from(event.tokenId.to_be_bytes::<32>());
    let raw_namehash = registrar_namehash(selected, labelhash);
    let previous_active = state.v1_name(&selected.source.namespace, &raw_namehash);
    let mut wrapper_fallback = false;
    let fallback_active_from =
        state.matching_v1_unwrap_time(&selected.source.namespace, &raw_namehash, &from, raw);
    if state
        .v1_registrar(&selected.source.namespace, &raw_namehash)
        .is_none()
        && state.v1_surface_materialized(&selected.source.namespace, &raw_namehash)
        && fallback_active_from.is_some()
        && let Some(expiry) =
            state.v1_registrar_expiry_from_wrapper(&selected.source.namespace, &raw_namehash)
    {
        let (token_lineage_id, resource_id, authority_key) =
            new_registrar_identity(selected, raw, &format!("{labelhash:#x}"));
        state.observe_v1_registrar(
            &selected.source.namespace,
            &raw_namehash,
            format!("{}:{raw_namehash}", selected.source.namespace),
            true,
            resource_id,
            token_lineage_id,
            selected.source.source_family.clone(),
            Some(selected.source.manifest_id),
            Some(format!("{labelhash:#x}")),
            Some(expiry),
            Some(from.clone()),
            authority_key,
            true,
            false,
        );
        wrapper_fallback = true;
    }
    let Some((_, linked)) =
        state.transfer_v1_registrar_owner(&selected.source.namespace, &raw_namehash, to.clone())
    else {
        return Ok(Interpreted::new());
    };
    let mut active_after = state.converge_v1_registrar_transfer(
        &selected.source.namespace,
        &raw_namehash,
        raw.block_timestamp.unix_timestamp(),
    );
    registry_only_fallback::activate_registry_only_authority(
        selected,
        raw,
        state,
        &raw_namehash,
        labelhash,
        &linked,
        &mut active_after,
    );
    let linked = state
        .v1_registrar(&selected.source.namespace, &raw_namehash)
        .context("transferred registrar remains retained")?;
    let mut after = json!({
        "source_event": "Transfer",
        "to": to,
        "token_id": u256_word_hex(event.tokenId),
        "namehash": raw_namehash,
        "token_lineage_id": linked.token_lineage_id.map(|id| id.to_string()),
    });
    // A fallback-created registrar identity must be recoverable from the latest transfer row
    // alone. Until numeric BaseRegistrar lifecycle refreshes it, every transfer repeats the marker
    // and uses that transfer's sender as restore-time owner; controllers only enrich plaintext.
    if wrapper_fallback || linked.wrapper_fallback {
        after["fallback_from_wrapper"] = json!(true);
        after["fallback_from"] = json!(from);
        after["surface_known"] = json!(linked.surface_known);
        after["labelhash"] = json!(linked.labelhash);
        after["expiry"] = json!(linked.expiry);
        after["authority_key"] = json!(linked.authority_key);
        after["authority_source_manifest_id"] = json!(linked.source_manifest_id);
    }
    let mut output = single_event(
        "TokenControlTransferred",
        linked.surface_known.then(|| linked.logical_name_id.clone()),
        Some(linked.resource_id),
        after,
    );
    output.events[0].explicit_before = Some(json!({"from": from}));
    output.resources.push(ResourceDraft {
        resource_id: linked.resource_id,
        token_lineage_id: linked.token_lineage_id,
    });
    append_transfer_permissions(
        &mut output,
        &from,
        &linked,
        previous_active.as_ref(),
        active_after.as_ref(),
        state.v1_resolver(&selected.source.namespace, &raw_namehash),
        &raw.chain_id,
    );
    let linked_resolver = state.v1_resolver_for_activation(
        &selected.source.namespace,
        &raw_namehash,
        active_after.as_ref(),
    );
    let registry_binding = state.v1_registry_binding(&selected.source.namespace, &raw_namehash);
    let mut observation = json!({"source_event":"Transfer"});
    if let (Some(active), Some((owner, _))) = (active_after.as_ref(), registry_binding.as_ref())
        && active.token_lineage_id.is_none()
        && matches!(
            active.authority_source_family.as_str(),
            "ens_v1_registry_l1" | "basenames_base_registry"
        )
        && !owner.eq_ignore_ascii_case(ZERO_ADDRESS)
        && active
            .owner
            .as_deref()
            .is_some_and(|selected_owner| selected_owner.eq_ignore_ascii_case(owner))
    {
        observation["registry_owner"] = json!(owner);
        // A proved same-transaction unwrap may split the live lease holder from the registry
        // manager. Keep that lease link when the already-readable name gets its registry binding.
        // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
        if fallback_active_from.is_some()
            && linked
                .expiry
                .is_some_and(|expiry| expiry > raw.block_timestamp.unix_timestamp())
        {
            observation["callback_retained_registrar_resource_id"] = json!(linked.resource_id);
            observation["node"] = json!(raw_namehash);
        }
    }
    append_authority_transition(
        &mut output,
        super::super::authority_arm(&selected.source.namespace),
        previous_active.as_ref(),
        active_after.as_ref(),
        registry_binding,
        raw,
        &observation,
        linked_resolver,
        fallback_active_from,
    );
    Ok(output)
}
