use super::*;

pub(super) fn apply_registry_owner_changed(
    history: &mut NameHistory,
    event: RegistryOwnerObservation,
) -> Result<()> {
    let before_anchor = active_anchor_for_history(history, &event.reference.chain_id);
    let before_owner = history.current_registry_owner.clone();
    history.current_registry_owner = Some(event.owner.clone());
    history.latest_registry_owner_ref = Some(event.reference.clone());
    history
        .registry_resource_anchor
        .get_or_insert_with(|| event.reference.as_boundary_ref());

    let after_anchor = active_anchor_for_history(history, &event.reference.chain_id);
    if matches!(
        (&before_anchor, &after_anchor),
        (Some(left), Some(right))
            if left.kind == AuthorityKind::RegistryOnly
                && right.kind == AuthorityKind::RegistryOnly
                && before_owner != history.current_registry_owner
    ) && let Some(name) = history.name.as_ref()
    {
        history.events.push(build_normalized_event(
            &event.reference,
            Some(name.logical_name_id.clone()),
            after_anchor.as_ref().map(|value| value.resource_id),
            EVENT_KIND_AUTHORITY_TRANSFERRED,
            json!({
                "owner": before_owner,
            }),
            json!({
                "owner": history.current_registry_owner,
                "labelhash": event.labelhash,
            }),
            format!(
                "registry-transfer:{}:{}:{}",
                event.reference.block_hash,
                event
                    .reference
                    .transaction_hash
                    .as_deref()
                    .unwrap_or_default(),
                event.reference.log_index.unwrap_or_default()
            ),
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
