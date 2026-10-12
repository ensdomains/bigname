//! The registry-operator rows of the effective-permission page (permissions/effective.rs
//! `OPERATOR_COLUMNS`): an account approved as operator by a resource's registry owner on the
//! registry contract of the resource's registry binding. The binding is the F2c one
//! (`registry::registry_bindings`) over the observations that can reach the resource, with the
//! attribution of name-keyed observations read from the composed name rows, which stand where
//! the served build reads the name row it builds (permission_resources.rs).
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use super::ServedApproval;
use crate::families::{
    control::{
        lifecycle::AuthoritySelection,
        registry::{NameAttribution, Observation, RegistryBinding, registry_bindings},
    },
    name::{CoverageShape, load_names_on},
};

/// The observations on chain `$1` that can reach one of the resources `$2`. Each arm is a keyed
/// lookup: by target resource, by own resource, and by the primary key for the names the
/// resources' binding candidates carry.
///
/// The caller binds only resources of chain `$1`. The candidates are then keyed by chain as well
/// as resource, which drops none. A resource id is the primary key of `resources`, so a resource
/// is on one chain. The foreign key of `surface_bindings` on `(chain_id, resource_id)` puts a
/// binding on its resource's chain, and Project writes a chain's candidates from that chain's
/// own bindings (`crates/project/src/families/identity.rs`).
///
/// The rows come in primary-key order, so the order does not depend on the plan.
pub(super) const BINDING_OBSERVATIONS_SQL: &str =
    "/* storage:families.control.permissions.binding_observations */
     SELECT to_jsonb(observation)
     FROM (
         SELECT by_target.*
         FROM bigname_phase.project_registry_binding_observation by_target
         WHERE by_target.chain_id = $1 AND by_target.target_resource_id = ANY($2::uuid[])
         UNION
         SELECT by_resource.*
         FROM bigname_phase.project_registry_binding_observation by_resource
         WHERE by_resource.chain_id = $1 AND by_resource.resource_id = ANY($2::uuid[])
         UNION
         SELECT by_name.*
         FROM bigname_phase.project_binding_candidate candidate
         JOIN bigname_phase.project_registry_binding_observation by_name
           ON by_name.chain_id = candidate.chain_id
          AND by_name.observation_identity = candidate.logical_name_id
         WHERE candidate.chain_id = $1 AND candidate.resource_id = ANY($2::uuid[])
     ) observation
     ORDER BY observation.chain_id COLLATE \"C\", observation.observation_identity COLLATE \"C\"";

/// The registry binding of each of `resources` on `chain_id`: every observation that can reach
/// one of them (its target, its own resource, or a name bound to it), attributed as the served
/// build does, then the latest per resource.
pub(super) async fn bindings_for(
    conn: &mut PgConnection,
    chain_id: &str,
    resources: &[Uuid],
) -> Result<BTreeMap<Uuid, RegistryBinding>> {
    if resources.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows: Vec<Value> = sqlx::query_scalar(BINDING_OBSERVATIONS_SQL)
        .bind(chain_id)
        .bind(resources)
        .fetch_all(&mut *conn)
        .await
        .context("failed to load the registry-binding observations of permission resources")?;
    let observations: Vec<Observation> = rows.iter().filter_map(Observation::from_row).collect();
    let names = attributions(conn, &observations).await?;
    let wanted: BTreeSet<String> = resources.iter().map(Uuid::to_string).collect();
    registry_bindings(&observations, &names)
        .into_iter()
        .filter(|(resource, _)| wanted.contains(resource))
        .map(|(resource, binding)| Ok((resource.parse()?, binding)))
        .collect()
}

/// The current resource and authority arm of every name a name-attributed observation carries,
/// from its composed row; a name with no composed row keeps the observation's stored target.
async fn attributions(
    conn: &mut PgConnection,
    observations: &[Observation],
) -> Result<BTreeMap<String, NameAttribution>> {
    let names: Vec<String> = observations
        .iter()
        .filter(|observation| observation.attributed_via == "name")
        .filter_map(|observation| observation.logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let rows = load_names_on(conn, &names, CoverageShape::Plain).await?;
    Ok(rows
        .into_iter()
        .map(|(name, row)| {
            (
                name,
                NameAttribution {
                    current_resource_id: row.resource_id.map(|id| id.to_string()),
                    authority_arm: AuthoritySelection::from_provenance(&row.provenance)
                        .authority_arm,
                },
            )
        })
        .collect())
}

/// The registry approvals of `(authority_contract, owner)` on `chain_id` with relation operator.
pub(super) async fn registry_approvals(
    conn: &mut PgConnection,
    chain_id: &str,
    authority_contract: &str,
    owner: &str,
    subjects: &[String],
) -> Result<Vec<ServedApproval>> {
    let rows: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.control.permissions.registry_approvals */
         SELECT to_jsonb(approval)
         FROM bigname_phase.project_account_approval approval
         WHERE approval.chain_id = $1 AND approval.authority_kind = 'registry'
           AND approval.authority_contract = $2 AND approval.owner = $3
           AND approval.relation_kind = 'operator'
           AND approval.subject = ANY($4::text[])",
    )
    .bind(chain_id)
    .bind(authority_contract)
    .bind(owner)
    .bind(subjects)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the registry operator approvals")?;
    Ok(rows.iter().filter_map(ServedApproval::from_row).collect())
}

#[cfg(test)]
#[path = "operators_tests.rs"]
mod tests;
