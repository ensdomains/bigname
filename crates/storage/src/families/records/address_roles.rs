//! Names an address manages through an ENSv2 registry role on the name's own registration.
//!
//! ENSv2 `PermissionedRegistry.setResolver` and `setSubregistry` accept any account holding
//! `ROLE_SET_RESOLVER` or `ROLE_SET_SUBREGISTRY` on the name's token resource, so such a role
//! holder manages the name without owning it.
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L142-L155 @ ens_v2@a971bd64)
//! (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L33-L41 @ ens_v2@a971bd64)
//!
//! A holder qualifies when a served permission row of the name's selected resource
//! (`GET /v1/permissions`, the F8 grant after the read-time masks of `masked_grants`) has the
//! registry scope and holds one of [`MANAGEMENT_POWERS`]. Other roles (renewing, unregistering,
//! admin roles), roles held on the registry root, and ENSv2 registry operators do not add a
//! name: a root role reaches every name of the registry, and operators are not permission rows.
//!
//! The grants are read at request time, so the membership follows the published grant rows
//! with no Project index of its own. The candidate names come from the grant's resource through
//! the F1 binding candidates; the composed name keeps a holder only while that resource is its
//! selected resource.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;

use super::FamilyPosition;
use crate::families::control::{
    permissions::{GrantRow, ServedGrant, masked_grants},
    rows::{Maxima, WrapperRow, text},
};

/// The ENSv2 registry roles whose holder manages the name: its resolver and its subregistry.
pub(crate) const MANAGEMENT_POWERS: [&str; 2] = ["set_resolver", "set_subregistry"];

/// One role holder of a name's selected resource, with its grant row's position.
#[derive(Clone, Debug)]
pub(crate) struct RoleManager {
    pub(crate) subject: String,
    pub(crate) position: FamilyPosition,
}

/// The (chain, name) pairs bound, now or before, to a resource on which `address` holds a
/// management role grant. A superset: the composed name decides.
pub(super) async fn role_name_candidates(
    conn: &mut PgConnection,
    address: &str,
    namespace: Option<&str>,
) -> Result<Vec<(String, String)>> {
    sqlx::query_as(
        "/* storage:families.records.address_role_candidates */
         SELECT DISTINCT grant_row.chain_id, candidate.logical_name_id
         FROM bigname_phase.project_grant grant_row
         JOIN bigname_phase.project_binding_candidate candidate
           ON candidate.chain_id = grant_row.chain_id
          AND candidate.resource_id = grant_row.resource_id
         WHERE grant_row.subject = lower($1) AND grant_row.scope_kind = 'registry'
           AND grant_row.effective_powers ?| $3::text[]
           AND ($2::text IS NULL OR candidate.namespace = $2)",
    )
    .bind(address)
    .bind(namespace)
    .bind(MANAGEMENT_POWERS.as_slice())
    .fetch_all(&mut *conn)
    .await
    .with_context(|| format!("failed to load the registry role names of {address}"))
}

/// The management role holders of each resource at `clock_seconds`, as the permission read
/// serves them.
pub(super) async fn role_managers(
    conn: &mut PgConnection,
    chain_id: &str,
    resources: &[String],
    wrappers: &BTreeMap<String, WrapperRow>,
    clock_seconds: i64,
) -> Result<BTreeMap<String, Vec<RoleManager>>> {
    if resources.is_empty() {
        return Ok(BTreeMap::new());
    }
    let grants: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.records.address_role_grants */ SELECT to_jsonb(grant_row)
         FROM bigname_phase.project_grant grant_row
         WHERE grant_row.chain_id = $1 AND grant_row.resource_id = ANY($2::uuid[])
           AND grant_row.scope_kind = 'registry' AND grant_row.effective_powers ?| $3::text[]",
    )
    .bind(chain_id)
    .bind(resources)
    .bind(MANAGEMENT_POWERS.as_slice())
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the registry role grants")?;
    let mut by_resource: BTreeMap<String, Vec<GrantRow>> = BTreeMap::new();
    for grant in grants.iter().filter_map(GrantRow::from_row) {
        by_resource
            .entry(grant.resource_id.clone())
            .or_default()
            .push(grant);
    }
    if by_resource.is_empty() {
        return Ok(BTreeMap::new());
    }
    let granted: Vec<&String> = by_resource.keys().collect();
    let key_states: BTreeMap<String, Maxima> = sqlx::query_scalar::<_, Value>(
        "/* storage:families.records.address_role_key_states */ SELECT to_jsonb(state)
         FROM bigname_phase.project_lifecycle_key_state state
         WHERE state.chain_id = $1 AND state.resource_id = ANY($2::uuid[])",
    )
    .bind(chain_id)
    .bind(&granted)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the registry role key states")?
    .iter()
    .filter_map(|row| Some((text(row, "resource_id")?, Maxima::from_row(row))))
    .collect();
    let mut out = BTreeMap::new();
    for (resource, grants) in &by_resource {
        let served = masked_grants(
            grants,
            wrappers.get(resource),
            key_states.get(resource),
            clock_seconds,
        );
        let managers: Vec<RoleManager> = served
            .iter()
            .filter(|grant| grant.scope_kind.as_deref() == Some("registry") && manages(grant))
            .filter_map(|grant| {
                let row = grants
                    .iter()
                    .find(|row| row.subject == grant.subject && row.scope == grant.scope)?;
                Some(RoleManager {
                    subject: grant.subject.to_ascii_lowercase(),
                    position: FamilyPosition {
                        block_number: row.position.block_number,
                        transaction_index: row.position.transaction_index,
                        log_index: row.position.log_index,
                        event_identity: row.position.event_identity.clone(),
                    },
                })
            })
            .collect();
        if !managers.is_empty() {
            out.insert(resource.clone(), managers);
        }
    }
    Ok(out)
}

fn manages(grant: &ServedGrant) -> bool {
    grant
        .effective_powers
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|power| MANAGEMENT_POWERS.contains(&power))
}
