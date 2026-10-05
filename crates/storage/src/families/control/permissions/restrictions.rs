//! Summary-only restriction reads. Wrapper restrictions need no grant fan-out; ENSv2
//! restrictions need only whether a grant survives path expiry and the union of admin powers.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use serde_json::Value;
use sqlx::PgConnection;

use super::{ResourceInput, resource_restrictions, rows_for, wrapper_unwrapped};
use crate::families::control::{
    lifecycle::{Clock, view::registration_lapsed},
    rows::{Maxima, text},
    wrapper::load_wrapper_rows,
};

pub(super) async fn load(
    conn: &mut PgConnection,
    chain_id: &str,
    clock: &Clock,
    resources: &[ResourceInput],
) -> Result<BTreeMap<String, Value>> {
    let ids: Vec<String> = resources
        .iter()
        .flat_map(|input| {
            std::iter::once(input.resource_id.clone()).chain(input.root_resource_id.clone())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let states: BTreeMap<String, Maxima> = rows_for(
        conn,
        "/* storage:families.control.permissions.summary_key_states */ SELECT to_jsonb(state)
         FROM bigname_phase.project_lifecycle_key_state state
         WHERE state.chain_id = $1 AND state.resource_id = ANY($2::uuid[])",
        chain_id,
        &ids,
    )
    .await?
    .iter()
    .filter_map(|row| Some((text(row, "resource_id")?, Maxima::from_row(row))))
    .collect();
    let active = |resource: &str| !states.get(resource).is_some_and(registration_lapsed);
    // Collapse the stored holder map in SQL: only the distinct powers are needed by a summary.
    let admins: BTreeMap<String, Vec<String>> = sqlx::query_as(
        "/* storage:families.control.permissions.summary_admin_powers */
         SELECT aggregate.resource_id::text,
                ARRAY(SELECT DISTINCT power FROM jsonb_each(aggregate.admin_powers) holder,
                    jsonb_array_elements_text(holder.value) power ORDER BY power)
         FROM bigname_phase.project_resource_admin_aggregate aggregate
         WHERE aggregate.chain_id = $1 AND aggregate.resource_id = ANY($2::uuid[])",
    )
    .bind(chain_id)
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect();
    let admins_of = |resource: &str| -> &[String] {
        if active(resource) {
            admins.get(resource).map_or(&[], Vec::as_slice)
        } else {
            &[]
        }
    };
    let wrappers: BTreeMap<String, _> = load_wrapper_rows(&mut *conn, chain_id, &ids)
        .await?
        .into_iter()
        .map(|row| (row.resource_id.clone(), row))
        .collect();
    let (registry_ids, root_ids): (Vec<String>, Vec<Option<String>>) = resources
        .iter()
        .filter(|input| {
            input.authority_kind.as_deref() == Some("ens_v2_registry") && active(&input.resource_id)
        })
        .map(|input| (input.resource_id.clone(), input.root_resource_id.clone()))
        .unzip();
    // ENSv2 registry grants have no wrapper mask. Account approval expansion cannot create a
    // row unless a holder grant already survives, so its fan-out never affects this existence.
    // A read bound to the resource also serves its registry's root grants (`ens_v2.rs`), so a
    // root grant is a served row.
    let has_grants: BTreeSet<String> = sqlx::query_scalar(
        "/* storage:families.control.permissions.summary_has_grants */
         SELECT pair.resource FROM unnest($2::text[], $3::text[]) pair(resource, root)
         WHERE EXISTS (SELECT 1 FROM bigname_phase.project_grant grant_row
             WHERE grant_row.chain_id = $1 AND grant_row.resource_id = pair.resource::uuid
               AND jsonb_typeof(grant_row.effective_powers) = 'array'
               AND grant_row.effective_powers <> '[]'::jsonb)
            OR EXISTS (SELECT 1 FROM bigname_phase.project_grant root_grant
             WHERE root_grant.chain_id = $1 AND root_grant.resource_id = pair.root::uuid
               AND root_grant.scope = 'root'
               AND jsonb_typeof(root_grant.effective_powers) = 'array'
               AND root_grant.effective_powers <> '[]'::jsonb)",
    )
    .bind(chain_id)
    .bind(&registry_ids)
    .bind(&root_ids)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect();
    Ok(resources
        .iter()
        .filter_map(|input| {
            let wrapper = wrappers.get(&input.resource_id);
            resource_restrictions(
                input.authority_kind.as_deref(),
                wrapper,
                wrapper_unwrapped(wrapper),
                clock.timestamp_seconds,
                has_grants.contains(&input.resource_id),
                admins_of(&input.resource_id),
                input.root_resource_id.as_deref().map_or(&[], admins_of),
            )
            .map(|restrictions| (input.resource_id.clone(), restrictions))
        })
        .collect())
}
