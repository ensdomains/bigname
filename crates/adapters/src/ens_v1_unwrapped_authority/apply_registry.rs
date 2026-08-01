use super::*;

pub(super) fn apply_registry_owner_changed(
    history: &mut NameHistory,
    event: RegistryOwnerObservation,
) -> Result<()> {
    let before_anchor = active_anchor_for_observation(history, &event.reference);
    let before_owner = history.current_registry_owner.clone();
    let registry_owner_restores_registrar =
        registry_owner_restores_superseded_registrar(history, &event);
    let registry_owner_supersedes_registrar = history.current_wrapper_key.is_none()
        && nonzero_address(Some(event.owner.as_str())).is_some()
        && history
            .current_registration
            .as_ref()
            .is_some_and(|lease| !event.owner.eq_ignore_ascii_case(&lease.registrant));
    if registry_owner_restores_registrar {
        emit_registry_owner_revokes_before_registrar_restore(
            history,
            before_anchor.as_ref(),
            &event.reference,
            EVENT_KIND_AUTHORITY_TRANSFERRED,
        );
    }
    history.current_registry_owner = Some(event.owner.clone());
    history.latest_registry_owner_ref = Some(event.reference.clone());
    history
        .registry_resource_anchor
        .get_or_insert_with(|| event.reference.as_boundary_ref());
    if registry_owner_supersedes_registrar {
        history.superseded_registration = history.current_registration.take();
    }
    if registry_owner_restores_registrar && let Some(lease) = history.superseded_registration.take()
    {
        history.current_registration = Some(lease);
    }

    let after_anchor = active_anchor_for_observation(history, &event.reference);
    if before_owner != history.current_registry_owner
        && let (Some(name), Some(after)) = (history.name.as_ref(), after_anchor.as_ref())
    {
        let identity_prefix = if after.kind == AuthorityKind::RegistryOnly {
            "registry-transfer".to_owned()
        } else {
            format!("registry-active-transfer:{}", after.authority_key)
        };
        history.events.push(build_registry_owner_transfer_event(
            &event.reference,
            &name.logical_name_id,
            after,
            before_owner.as_deref(),
            history.current_registry_owner.as_deref(),
            &event.labelhash,
            &identity_prefix,
        ));
    }
    if let Some(name) = history.name.clone() {
        match (before_anchor.as_ref(), after_anchor.as_ref()) {
            (Some(before), Some(after))
                if before.kind == AuthorityKind::RegistryOnly
                    && after.kind == AuthorityKind::RegistryOnly =>
            {
                emit_observation_permission_subject_change(
                    &mut history.events,
                    &event.reference,
                    &name.logical_name_id,
                    after,
                    PermissionSubjectChange {
                        before_subject: before_owner.as_deref(),
                        after_subject: history.current_registry_owner.as_deref(),
                        resolver: history.current_resolver.as_deref(),
                        source_event_kind: EVENT_KIND_AUTHORITY_TRANSFERRED,
                        identity_suffix: None,
                    },
                );
            }
            (_, Some(after)) if after.kind == AuthorityKind::RegistryOnly => {
                if let Some(subject) = nonzero_address(history.current_registry_owner.as_deref()) {
                    emit_observation_permission_grants(
                        &mut history.events,
                        &event.reference,
                        &name.logical_name_id,
                        after,
                        &subject,
                        history.current_resolver.as_deref(),
                        EVENT_KIND_AUTHORITY_TRANSFERRED,
                    );
                }
            }
            _ => {}
        }
    }
    transition_authority(
        history,
        before_anchor,
        after_anchor,
        &event.reference.as_boundary_ref(),
        event.reference.block_timestamp,
    )?;
    Ok(())
}

pub(super) fn build_registry_child_changed_event(
    event: &RegistryOwnerObservation,
) -> Result<NormalizedEvent> {
    let parent_node = event
        .parent_node
        .as_deref()
        .context("registry child change is missing its parent node")?;
    let child_node = event
        .namehash
        .as_deref()
        .context("registry child change is missing its child node")?;
    let tombstone = event.owner.eq_ignore_ascii_case(ZERO_ADDRESS);
    let transaction_hash = event
        .reference
        .transaction_hash
        .as_deref()
        .unwrap_or_default();
    let log_index = event.reference.log_index.unwrap_or_default();

    Ok(NormalizedEvent {
        event_identity: format!(
            "{}:{}:{}:{}:{}:{}",
            DERIVATION_KIND_ENS_V1_SUBREGISTRY_CHANGED,
            event.reference.source_manifest_id,
            event.reference.block_hash,
            transaction_hash,
            log_index,
            event.emitting_address
        ),
        namespace: event.reference.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_SUBREGISTRY_CHANGED.to_owned(),
        source_family: event.reference.source_family.clone(),
        manifest_version: event.reference.manifest_version,
        source_manifest_id: source_manifest_id_if_known(event.reference.source_manifest_id),
        chain_id: Some(event.reference.chain_id.clone()),
        block_number: Some(event.reference.block_number),
        block_hash: Some(event.reference.block_hash.clone()),
        transaction_hash: event.reference.transaction_hash.clone(),
        log_index: event.reference.log_index,
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": event.reference.chain_id,
            "block_hash": event.reference.block_hash,
            "block_number": event.reference.block_number,
            "transaction_hash": event.reference.transaction_hash,
            "transaction_index": event.reference.transaction_index,
            "log_index": event.reference.log_index,
            "emitting_address": event.emitting_address,
        }),
        derivation_kind: DERIVATION_KIND_ENS_V1_SUBREGISTRY_CHANGED.to_owned(),
        canonicality_state: event.reference.canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "source_event": "NewOwner",
            "parent_node": parent_node,
            "labelhash": event.labelhash,
            "child_node": child_node,
            "owner": event.owner,
            "emitting_address": event.emitting_address,
            "tombstone": tombstone,
            // This retained field name is consumed by the transitional child
            // projection. It means the child assignment is active; no owner
            // address is admitted to `discovery_edges`.
            "active_edge": !tombstone,
        }),
    })
}

fn registry_owner_restores_superseded_registrar(
    history: &NameHistory,
    event: &RegistryOwnerObservation,
) -> bool {
    if history.current_wrapper_key.is_some() || history.current_registration.is_some() {
        return false;
    }
    let Some(lease) = history.superseded_registration.as_ref() else {
        return false;
    };
    if !event.labelhash.is_empty() && !lease.labelhash.eq_ignore_ascii_case(&event.labelhash) {
        return false;
    }
    if registration_released_at_or_before(lease, event.reference.block_timestamp) {
        return false;
    }
    nonzero_address(Some(event.owner.as_str()))
        .is_some_and(|owner| owner.eq_ignore_ascii_case(&lease.registrant))
}

fn build_registry_owner_transfer_event(
    reference: &ObservationRef,
    logical_name_id: &str,
    anchor: &AuthorityAnchor,
    before_owner: Option<&str>,
    after_owner: Option<&str>,
    labelhash: &str,
    identity_prefix: &str,
) -> NormalizedEvent {
    build_normalized_event(
        reference,
        Some(logical_name_id.to_owned()),
        Some(anchor.resource_id),
        EVENT_KIND_AUTHORITY_TRANSFERRED,
        json!({
            "owner": before_owner,
        }),
        json!({
            "owner": after_owner,
            "labelhash": labelhash,
        }),
        format!(
            "{}:{}:{}:{}",
            identity_prefix,
            reference.block_hash,
            reference.transaction_hash.as_deref().unwrap_or_default(),
            reference.log_index.unwrap_or_default()
        ),
    )
}
