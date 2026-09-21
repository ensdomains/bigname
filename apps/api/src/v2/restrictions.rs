use bigname_storage::PermissionsCurrentResourceSummary;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use super::history::format_timestamp;
use super::name_record::wrapper_lifecycle_matches_fuses;
use super::vocab::{WrapperFuses, WrapperState};
use super::{V2Error, V2Result};

/// Registration-level constraints served as `restrictions`; see
/// [resource restrictions](../../../../docs/api-v2.md#resource-restrictions).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ResourceRestrictions {
    /// The ENSv1 NameWrapper position: lifecycle label, expiry-effective fuse word, and entry
    /// expiry.
    EnsV1Wrapper {
        registration_id: String,
        wrapper_state: WrapperState,
        wrapper_fuses: WrapperFuses,
        #[serde(skip_serializing_if = "Option::is_none")]
        wrapper_expires_at: Option<String>,
    },
    /// The ENSv2 registration whose token-scoped roles can no longer change.
    EnsV2Registry {
        registration_id: String,
        locked_roles: Vec<String>,
    },
}

impl ResourceRestrictions {
    /// Maps the Project-owned `resource_restrictions` block of a per-resource permission
    /// summary. `None` when the registration has no resource-level constraint model.
    pub(crate) fn from_summary(
        summary: &PermissionsCurrentResourceSummary,
    ) -> V2Result<Option<Self>> {
        let Some(block) = summary.resource_restrictions.as_ref() else {
            return Ok(None);
        };
        let registration_id = summary.resource_id.to_string();
        match block.get("kind").and_then(Value::as_str) {
            Some("ens_v1_wrapper") => wrapper_restrictions(registration_id, block).map(Some),
            Some("ens_v2_registry") => registry_restrictions(registration_id, block).map(Some),
            _ => Err(invalid_restrictions()),
        }
    }
}

impl ResourceRestrictions {
    /// Serve the restrictions under the name's `registration_id`. The constraint model of a
    /// wrapped `.eth` name lives on its NameWrapper resource while the registration is
    /// identified by its BaseRegistrar lease.
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L143-L153 @ ens_v1@91c966f)
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L278 @ ens_v1@91c966f)
    pub(crate) fn for_registration(mut self, id: Option<String>) -> Self {
        let (Self::EnsV1Wrapper {
            registration_id, ..
        }
        | Self::EnsV2Registry {
            registration_id, ..
        }) = &mut self;
        if let Some(id) = id {
            *registration_id = id;
        }
        self
    }
}

fn wrapper_restrictions(registration_id: String, block: &Value) -> V2Result<ResourceRestrictions> {
    let wrapper_state = block
        .get("wrapper_state")
        .and_then(Value::as_str)
        .and_then(WrapperState::from_wire)
        .ok_or_else(invalid_restrictions)?;
    let fuses = block
        .get("fuses")
        .and_then(Value::as_u64)
        .and_then(|fuses| u32::try_from(fuses).ok())
        .ok_or_else(invalid_restrictions)?;
    let wrapper_fuses = WrapperFuses::from_word(fuses);
    if !wrapper_lifecycle_matches_fuses(wrapper_state, wrapper_fuses) {
        return Err(invalid_restrictions());
    }
    let wrapper_expires_at = match block.get("expiry_seconds") {
        None | Some(Value::Null) => None,
        Some(expiry) => wrapper_expiry(expiry)?,
    };
    Ok(ResourceRestrictions::EnsV1Wrapper {
        registration_id,
        wrapper_state,
        wrapper_fuses,
        wrapper_expires_at,
    })
}

/// NameWrapper expiries are `uint64` seconds; a value with no calendar representation (for
/// example the `type(uint64).max` sentinel some parents set) is served without a timestamp.
fn wrapper_expiry(expiry: &Value) -> V2Result<Option<String>> {
    let seconds = match expiry {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().map(|_| i64::MAX))
            .or_else(|| number.as_f64().map(|value| value as i64)),
        Value::String(text) => text
            .parse::<i64>()
            .ok()
            .or_else(|| text.parse::<u64>().ok().map(|_| i64::MAX)),
        _ => None,
    }
    .ok_or_else(invalid_restrictions)?;
    Ok(OffsetDateTime::from_unix_timestamp(seconds)
        .ok()
        .map(format_timestamp))
}

fn registry_restrictions(registration_id: String, block: &Value) -> V2Result<ResourceRestrictions> {
    let locked_roles = block
        .get("locked_roles")
        .and_then(Value::as_array)
        .ok_or_else(invalid_restrictions)?
        .iter()
        .map(|role| {
            role.as_str()
                .filter(|role| LOCKABLE_ROLES.contains(role))
                .map(str::to_owned)
                .ok_or_else(invalid_restrictions)
        })
        .collect::<V2Result<Vec<_>>>()?;
    Ok(ResourceRestrictions::EnsV2Registry {
        registration_id,
        locked_roles,
    })
}

/// The token-scoped registry roles whose assignment can lock; see the registry role table.
/// (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L24-L45 @ ens_v2@a971bd64)
const LOCKABLE_ROLES: &[&str] = &[
    "unregister",
    "renew",
    "set_subregistry",
    "set_resolver",
    "transfer",
];

fn invalid_restrictions() -> V2Error {
    V2Error::internal_error("stored resource restrictions are inconsistent")
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::types::Uuid;

    use super::*;
    use crate::v2::ErrorCode;
    use bigname_storage::ResourcePermissionCoverage;

    const PROJECTION: &str =
        include_str!("../../../../crates/project/src/builders/permissions/resource_summary.rs");
    const V2_ROLE_TABLES: &str =
        include_str!("../../../../crates/adapters/src/schema_v2/protocol/permissions.rs");

    /// `(role, admin)` pairs from the projection's `VALUES (ordinal, 'role', 'admin')` list.
    fn projected_lock_pairs() -> Vec<(String, String)> {
        let start = PROJECTION
            .find("AS locked_roles")
            .expect("projection must build locked_roles");
        let end = start
            + PROJECTION[start..]
                .find(") role(ordinality, name, admin)")
                .expect("projection must close its role list");
        PROJECTION[start..end]
            .lines()
            .filter_map(|line| {
                let quoted = line.trim().strip_prefix('(')?.split_once(", '")?.1;
                let (role, rest) = quoted.split_once("', '")?;
                Some((role.to_owned(), rest.split('\'').next()?.to_owned()))
            })
            .collect()
    }

    fn registry_role_names() -> Vec<String> {
        let start = V2_ROLE_TABLES
            .find("REGISTRY_ROLE_BITS: &[(usize, &str)] = &[")
            .expect("registry role table must exist");
        let end = start
            + V2_ROLE_TABLES[start..]
                .find("];")
                .expect("table must close");
        V2_ROLE_TABLES[start..end]
            .lines()
            .filter_map(|line| line.trim().split_once(", \"")?.1.strip_suffix("\"),"))
            .map(str::to_owned)
            .collect()
    }

    /// The served `locked_roles` names, the projection's role-to-admin pairs, and the interpreter's
    /// registry role vocabulary describe the same five token-scoped roles.
    #[test]
    fn lockable_roles_bind_the_projection_and_the_registry_role_vocabulary() {
        let pairs = projected_lock_pairs();
        let names = registry_role_names();
        assert_eq!(
            pairs
                .iter()
                .map(|(role, _)| role.as_str())
                .collect::<Vec<_>>(),
            LOCKABLE_ROLES
        );
        for (role, admin) in &pairs {
            assert!(names.contains(admin), "{admin} must be a registry role");
            if role == "transfer" {
                assert_eq!(admin, "can_transfer_admin");
            } else {
                assert!(names.contains(role), "{role} must be a registry role");
                assert_eq!(*admin, format!("admin_{role}"));
            }
        }
    }

    fn summary(restrictions: Option<Value>) -> PermissionsCurrentResourceSummary {
        PermissionsCurrentResourceSummary {
            resource_id: Uuid::from_u128(0x77),
            authority_kind: Some("wrapper".to_owned()),
            root_resource_id: None,
            coverage:
                ResourcePermissionCoverage::wrapper_parent_and_resolver_delegation_not_projected(),
            resource_restrictions: restrictions,
            provenance: json!({}),
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn wrapper_restrictions_serve_state_fuses_and_expiry() {
        let mapped = ResourceRestrictions::from_summary(&summary(Some(json!({
            "kind": "ens_v1_wrapper",
            "wrapper_state": "locked",
            "fuses": 196_609,
            "expiry_seconds": 1_800_000_000,
        }))))
        .expect("consistent block must map")
        .expect("block must be present");
        let serialized = serde_json::to_value(&mapped).expect("must serialize");
        assert_eq!(serialized["kind"], json!("ens_v1_wrapper"));
        assert_eq!(
            serialized["registration_id"],
            json!(Uuid::from_u128(0x77).to_string())
        );
        assert_eq!(serialized["wrapper_state"], json!("locked"));
        assert_eq!(serialized["wrapper_fuses"]["fuses"], json!(196_609));
        assert_eq!(serialized["wrapper_fuses"]["cannot_unwrap"], json!(true));
        assert_eq!(
            serialized["wrapper_fuses"]["parent_cannot_control"],
            json!(true)
        );
        assert_eq!(serialized["wrapper_fuses"]["is_dot_eth"], json!(true));
        assert_eq!(
            serialized["wrapper_expires_at"],
            json!("2027-01-15T08:00:00Z")
        );
    }

    #[test]
    fn wrapper_restrictions_omit_an_unrepresentable_expiry() {
        let mapped = ResourceRestrictions::from_summary(&summary(Some(json!({
            "kind": "ens_v1_wrapper",
            "wrapper_state": "wrapped",
            "fuses": 0,
            "expiry_seconds": 18_446_744_073_709_551_615_u64,
        }))))
        .expect("consistent block must map")
        .expect("block must be present");
        let serialized = serde_json::to_value(&mapped).expect("must serialize");
        assert!(serialized.get("wrapper_expires_at").is_none());
    }

    #[test]
    fn inconsistent_wrapper_block_is_an_internal_error() {
        let error = ResourceRestrictions::from_summary(&summary(Some(json!({
            "kind": "ens_v1_wrapper",
            "wrapper_state": "locked",
            "fuses": 0,
        }))))
        .expect_err("locked without CANNOT_UNWRAP must fail");
        assert_eq!(error.code(), ErrorCode::InternalError);
    }

    #[test]
    fn registry_restrictions_serve_locked_roles_and_reject_unknown_ones() {
        let mapped = ResourceRestrictions::from_summary(&summary(Some(json!({
            "kind": "ens_v2_registry",
            "locked_roles": ["renew", "transfer"],
        }))))
        .expect("known roles must map")
        .expect("block must be present");
        assert_eq!(
            serde_json::to_value(&mapped).expect("must serialize"),
            json!({
                "kind": "ens_v2_registry",
                "registration_id": Uuid::from_u128(0x77).to_string(),
                "locked_roles": ["renew", "transfer"],
            })
        );
        assert!(
            ResourceRestrictions::from_summary(&summary(Some(json!({
                "kind": "ens_v2_registry",
                "locked_roles": ["admin_renew"],
            }))))
            .is_err()
        );
        assert_eq!(
            ResourceRestrictions::from_summary(&summary(None)).expect("absent block maps"),
            None
        );
    }
}
