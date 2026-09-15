use super::*;

mod transfer {
    use super::*;
    sol! { event Transfer(address indexed from, address indexed to, uint256 indexed tokenId); }
}

pub(super) fn transfer(
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
    let mut fallback_active_from = None;
    if state
        .v1_registrar(&selected.source.namespace, &raw_namehash)
        .is_none()
        && state.v1_surface_materialized(&selected.source.namespace, &raw_namehash)
        && let Some(unwrapped_at) =
            state.matching_v1_unwrap_time(&selected.source.namespace, &raw_namehash, &from, raw)
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
        fallback_active_from = Some(unwrapped_at);
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
    if active_after.is_none()
        && state
            .v1_registry_owner(&selected.source.namespace, &raw_namehash)
            .is_some_and(|owner| !owner.eq_ignore_ascii_case(ZERO_ADDRESS))
    {
        let registry_owner = state
            .v1_registry_owner(&selected.source.namespace, &raw_namehash)
            .expect("checked registry owner");
        let authority = V1NameState {
            logical_name_id: linked.logical_name_id.clone(),
            surface_known: linked.surface_known,
            resource_id: stable_uuid(&format!(
                "resource:registry-only:{}:{raw_namehash}",
                raw.chain_id
            )),
            token_lineage_id: None,
            authority_source_family: if selected.source.source_family == "basenames_base_registrar"
            {
                "basenames_base_registry"
            } else {
                "ens_v1_registry_l1"
            }
            .to_owned(),
            source_manifest_id: None,
            labelhash: Some(format!("{labelhash:#x}")),
            expiry: None,
            owner: Some(registry_owner),
            registry_contract: None,
            authority_key: Some(format!("registry-only:{}:{raw_namehash}", raw.chain_id)),
            wrapper_fallback: false,
        };
        state.remember_v1_registry_authority(
            &selected.source.namespace,
            &raw_namehash,
            authority.clone(),
        );
        state.activate_v1_authority(
            &selected.source.namespace,
            &raw_namehash,
            Some(authority.clone()),
        );
        active_after = Some(authority);
    }
    let mut after = json!({
        "source_event": "Transfer",
        "to": to,
        "token_id": u256_word_hex(event.tokenId),
        "namehash": raw_namehash,
        "token_lineage_id": linked.token_lineage_id.map(|id| id.to_string()),
    });
    // A fallback-created registrar identity must be recoverable from the latest transfer row
    // alone. Until a label-bearing registrar-controller registration or renewal replaces it,
    // every transfer repeats the marker and uses that transfer's sender as the restore-time owner.
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
        Some(linked.logical_name_id.clone()),
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
    append_authority_transition(
        &mut output,
        super::super::authority_arm(&selected.source.namespace),
        previous_active.as_ref(),
        active_after.as_ref(),
        state.v1_registry_binding(&selected.source.namespace, &raw_namehash),
        raw,
        &json!({"source_event":"Transfer"}),
        linked_resolver,
        fallback_active_from,
    );
    Ok(output)
}
