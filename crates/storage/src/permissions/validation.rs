use anyhow::{Result, bail};

use super::types::{PermissionScope, PermissionsCurrentRow};

pub(super) fn validate_permissions_current_row(row: &PermissionsCurrentRow) -> Result<()> {
    if row.subject.trim().is_empty() {
        bail!(
            "permissions_current row for resource_id {} must include subject",
            row.resource_id
        );
    }
    if row.manifest_version <= 0 {
        bail!(
            "permissions_current row for resource_id {} subject {} must include positive manifest_version",
            row.resource_id,
            row.subject
        );
    }

    match &row.scope {
        PermissionScope::Resolver {
            chain_id,
            resolver_address,
        } => {
            if chain_id.trim().is_empty() {
                bail!("resolver permission scope must include chain_id");
            }
            if resolver_address.trim().is_empty() {
                bail!("resolver permission scope must include resolver_address");
            }
        }
        PermissionScope::RecordManager {
            chain_id,
            manager_address,
        } => {
            if chain_id.trim().is_empty() {
                bail!("record_manager permission scope must include chain_id");
            }
            if manager_address.trim().is_empty() {
                bail!("record_manager permission scope must include manager_address");
            }
        }
        PermissionScope::TransportDerived { transport } => {
            if transport.trim().is_empty() {
                bail!("transport_derived permission scope must include transport");
            }
        }
        PermissionScope::Root
        | PermissionScope::Registry
        | PermissionScope::Resource
        | PermissionScope::MigrationDerived { .. } => {}
    }

    Ok(())
}
