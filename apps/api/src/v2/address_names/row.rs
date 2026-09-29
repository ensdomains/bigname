//! One `GET /v1/addresses/{address}/names` row from its grouped entry and composed name row.
use bigname_storage::{AddressNameCurrentEntry, NameCurrentRow};

use super::{AddressName, AddressNameRoleSummary, relation_from_storage};
use crate::v2::{
    Authority,
    name_record::{name_registration_fields, registration_id},
};

pub(crate) fn build_address_name(
    entry: &AddressNameCurrentEntry,
    name_row: Option<&NameCurrentRow>,
    primary_name: Option<&str>,
    migrated_at: Option<String>,
    subname_count: Option<u64>,
    record_count: Option<u64>,
    role_summary: Option<Vec<AddressNameRoleSummary>>,
) -> AddressName {
    let registration = name_registration_fields(name_row, &entry.namespace);

    // A surface-less ENSv1 registry child has no name row: it serves what its parent's subnames
    // route serves for it (`subnames::build_subname` with no name row), its registry owner and
    // the registration fields of no name row, on its registry-only resource.
    AddressName {
        name: entry.normalized_name.clone(),
        display_name: entry.canonical_display_name.clone(),
        namespace: entry.namespace.clone(),
        namehash: entry.namehash.clone(),
        permission_resource_id: Some(permission_resource_handle(name_row, entry.resource_id)),
        owner: registration.owner.or_else(|| entry.served_owner.clone()),
        registrant: registration.registrant,
        registration_status: registration.registration_status,
        registered_at: registration.registered_at,
        created_at: registration.created_at,
        expires_at: registration.expires_at,
        expires_at_reason: registration.expires_at_reason,
        grace_ends_at: registration.grace_ends_at,
        authority: name_row.and_then(|row| Authority::from_provenance(&row.provenance)),
        migrated_at,
        relations: entry
            .relations
            .iter()
            .copied()
            .map(relation_from_storage)
            .collect(),
        is_primary: !entry.is_registry_child()
            && primary_name == Some(entry.normalized_name.as_str()),
        resolution: None,
        resolutions: None,
        lapsed_registration: None,
        subname_count,
        record_count,
        role_summary,
        restrictions: None,
    }
}

/// The value `GET /v1/permissions?registration_id=` resolves to this row's permission resource:
/// the registration the name currently serves (its BaseRegistrar lease for a wrapped `.eth`
/// name, whose rows live on the NameWrapper resource), or the resource itself when no
/// current registration claims it. Unsupported name coverage does not erase a retained handle.
/// A wrapped subname has no lease, so it keeps its NameWrapper resource.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L240-L305 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L390-L414 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
pub(crate) fn permission_resource_handle(
    name_row: Option<&NameCurrentRow>,
    resource_id: sqlx::types::Uuid,
) -> String {
    name_row
        .filter(|row| crate::v2::permissions::registration_row(row))
        .and_then(|row| registration_id(&row.declared_summary, None))
        .unwrap_or_else(|| resource_id.to_string())
}
