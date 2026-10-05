//! The `role_holder` relation: names on which an address holds an ENSv2 registry role.
//!
//! An ENSv2 `PermissionedRegistry` keeps per-account role bitmaps on each registration's token
//! resource (`EACRolesChanged`), and a holder can act on the name within those roles without
//! owning it, for example `setResolver` and `setSubregistry` check `ROLE_SET_RESOLVER` and
//! `ROLE_SET_SUBREGISTRY` on the token resource.
//! (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L142-L155 @ ens_v2@a971bd64)
//! (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L7-L63 @ ens_v2@a971bd64)
//! Holding a role does not make the address the name's manager (the effective controller).
//!
//! An address is a role holder of a name when a served permission row of the name's selected
//! resource (`GET /v1/permissions`, the F8 grant after the read-time masks of `masked_grants`)
//! has the registry scope and holds at least one role. The `was_reserved` marker alone is not a
//! role: it authorizes nothing.
//! (upstream: .refs/ens_v2/contracts/src/registry/libraries/RegistryRolesLib.sol:L47-L48 @ ens_v2@a971bd64)
//! Roles held on the registry root reach every name of the registry and add no name; ENSv2
//! registry operators are not permission rows and add none either.
//!
//! Address-list reads load only the requested subject's grants. The history catalogue also
//! uses this fold at publication, loading all holders of the touched resources once; both paths
//! use the same permission masks ([`RoleHolderLoad`]).
//! The candidate names come from the grant's resource through the
//! F1 binding candidates; the composed name keeps a holder only while that resource is its
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

/// The ENSv2 registry role bit that marks a registration made from a reservation; not a role.
const MARKER: &str = "was_reserved";

/// One role holder of a name's selected resource, with its grant row's position.
#[derive(Clone, Debug)]
pub(crate) struct RoleHolder {
    pub(crate) subject: String,
    pub(crate) position: FamilyPosition,
}

/// The (chain, name) pairs bound, now or before, to a resource on which `address` holds a
/// registry-scope grant with some power. A superset: the composed name decides.
pub(super) async fn role_name_candidates(
    conn: &mut PgConnection,
    address: &str,
    namespace: Option<&str>,
) -> Result<Vec<(String, String)>> {
    super::seams::note_role_read("candidates");
    sqlx::query_as(
        "/* storage:families.records.address_role_candidates */
         SELECT DISTINCT grant_row.chain_id, candidate.logical_name_id
         FROM bigname_phase.project_grant grant_row
         JOIN bigname_phase.project_binding_candidate candidate
           ON candidate.chain_id = grant_row.chain_id
          AND candidate.resource_id = grant_row.resource_id
         WHERE grant_row.subject = lower($1) AND grant_row.scope_kind = 'registry'
           AND NOT grant_row.revoked
           AND ($2::text IS NULL OR candidate.namespace = $2)",
    )
    .bind(address)
    .bind(namespace)
    .fetch_all(&mut *conn)
    .await
    .with_context(|| format!("failed to load the registry role names of {address}"))
}

/// Whose registry roles a relations load reads.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RoleHolderLoad<'a> {
    /// Only this address's grants. The address-name and address-history reads relate one
    /// address, so the grants of every other subject on the same resources are never read: an
    /// untrusted subregistry can grant roles to any number of accounts on one registration.
    Subject(&'a str),
    /// No grants. Used when an explicit relation set excludes `role_holder`, and by identity
    /// composition for reverse lookup, which does not serve that relation.
    Skip,
    /// Project stores all exact relations for the touched names, once per publication.
    All,
}

/// The registry-scope grants of one subject on a batch of resources. The primary key
/// `(chain_id, resource_id, subject, scope)` answers it with one probe per resource.
pub(crate) const ROLE_GRANTS_SQL: &str = "/* storage:families.records.address_role_grants */
     SELECT to_jsonb(grant_row)
     FROM bigname_phase.project_grant grant_row
     WHERE grant_row.chain_id = $1 AND grant_row.resource_id = ANY($2::uuid[])
       AND grant_row.subject = lower($3)
       AND grant_row.scope_kind = 'registry' AND NOT grant_row.revoked";

/// The role holders of each resource at `clock_seconds` among the subjects `load` names, as
/// the permission read serves them.
pub(super) async fn role_holders(
    conn: &mut PgConnection,
    chain_id: &str,
    resources: &[String],
    wrappers: &BTreeMap<String, WrapperRow>,
    clock_seconds: i64,
    load: RoleHolderLoad<'_>,
) -> Result<BTreeMap<String, Vec<RoleHolder>>> {
    if matches!(load, RoleHolderLoad::Skip) {
        return Ok(BTreeMap::new());
    }
    if resources.is_empty() {
        return Ok(BTreeMap::new());
    }
    super::seams::note_role_read("grants");
    let all = "/* storage:families.records.publication_role_grants */
        SELECT to_jsonb(grant_row) FROM bigname_phase.project_grant grant_row
        WHERE grant_row.chain_id = $1 AND grant_row.resource_id = ANY($2::uuid[])
          AND grant_row.scope_kind = 'registry' AND NOT grant_row.revoked";
    let mut query = sqlx::query_scalar(match load {
        RoleHolderLoad::Subject(_) => ROLE_GRANTS_SQL,
        _ => all,
    })
    .bind(chain_id)
    .bind(resources);
    if let RoleHolderLoad::Subject(subject) = load {
        query = query.bind(subject);
    }
    let grants: Vec<Value> = query
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
        // Each grant is masked on its own, as `masked_grants` masks every grant independently,
        // so the served row keeps its grant's position without a search.
        let mut holders: Vec<RoleHolder> = grants
            .iter()
            .filter(|grant| {
                masked_grants(
                    std::slice::from_ref(*grant),
                    wrappers.get(resource),
                    key_states.get(resource),
                    clock_seconds,
                )
                .first()
                .is_some_and(|served| {
                    served.scope_kind.as_deref() == Some("registry") && holds_role(served)
                })
            })
            .map(|grant| RoleHolder {
                subject: grant.subject.to_ascii_lowercase(),
                position: FamilyPosition {
                    block_number: grant.position.block_number,
                    transaction_index: grant.position.transaction_index,
                    log_index: grant.position.log_index,
                    event_identity: grant.position.event_identity.clone(),
                },
            })
            .collect();
        holders.sort_by(|left, right| left.subject.cmp(&right.subject));
        holders.dedup_by(|left, right| left.subject == right.subject);
        if !holders.is_empty() {
            out.insert(resource.clone(), holders);
        }
    }
    Ok(out)
}

fn holds_role(grant: &ServedGrant) -> bool {
    grant
        .effective_powers
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|power| power != MARKER)
}

#[cfg(test)]
#[path = "address_roles_tests.rs"]
mod tests;
