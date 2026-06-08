use super::*;

pub(super) fn apply_registry_owner_changed(
    history: &mut NameHistory,
    event: RegistryOwnerObservation,
) -> Result<()> {
    let before_anchor = active_anchor_for_observation(history, &event.reference);
    let before_owner = history.current_registry_owner.clone();
    let registry_owner_supersedes_registrar = history.current_wrapper_key.is_none()
        && nonzero_address(Some(event.owner.as_str())).is_some()
        && history
            .current_registration
            .as_ref()
            .is_some_and(|lease| !event.owner.eq_ignore_ascii_case(&lease.registrant));
    history.current_registry_owner = Some(event.owner.clone());
    history.latest_registry_owner_ref = Some(event.reference.clone());
    history
        .registry_resource_anchor
        .get_or_insert_with(|| event.reference.as_boundary_ref());
    if registry_owner_supersedes_registrar {
        history.superseded_registration = history.current_registration.take();
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
