use super::*;

/// When a registrar transfer leaves no active authority but the registry still names a nonzero
/// owner, activate the registry-only authority for the name.
pub(super) fn activate_registry_only_authority(
    selected: &Selected,
    raw: &RawLogInput,
    state: &mut State,
    raw_namehash: &str,
    labelhash: B256,
    linked: &V1NameState,
    active_after: &mut Option<V1NameState>,
) {
    if active_after.is_none()
        && state
            .v1_registry_owner(&selected.source.namespace, raw_namehash)
            .is_some_and(|owner| !owner.eq_ignore_ascii_case(ZERO_ADDRESS))
    {
        let registry_owner = state
            .v1_registry_owner(&selected.source.namespace, raw_namehash)
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
            raw_namehash,
            authority.clone(),
        );
        state.activate_v1_authority(
            &selected.source.namespace,
            raw_namehash,
            Some(authority.clone()),
        );
        *active_after = Some(authority);
    }
}
