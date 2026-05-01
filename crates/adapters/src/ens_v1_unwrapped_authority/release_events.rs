use super::*;

pub(super) fn emit_registration_released_event(
    history: &mut NameHistory,
    lease: &RegistrationLease,
    release_ref: &BoundaryRef,
) -> Result<()> {
    let Some(name) = history.name.as_ref() else {
        return Ok(());
    };
    history.events.push(build_boundary_event(
        release_ref,
        BoundaryEventPayload {
            logical_name_id: Some(name.logical_name_id.clone()),
            resource_id: Some(deterministic_uuid(&format!(
                "resource:{}",
                lease.authority_key
            ))),
            event_kind: EVENT_KIND_REGISTRATION_RELEASED,
            before_state: json!({
                "registrant": lease.registrant,
                "expiry": lease.expiry.unix_timestamp(),
            }),
            after_state: json!({
                "released_at": release_ref.block_timestamp.unix_timestamp(),
                "labelhash": lease.labelhash,
            }),
            identity_suffix: format!(
                "release:{}:{}:{}",
                release_ref.block_hash, name.logical_name_id, lease.authority_key
            ),
        },
        BoundaryEventSource {
            source_family: lease.start_ref.source_family.clone(),
            manifest_version: lease.start_ref.manifest_version,
            source_manifest_id: source_manifest_id_if_known(lease.start_ref.source_manifest_id),
            canonicality_state: release_ref.canonicality_state,
        },
    ));
    Ok(())
}
