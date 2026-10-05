//! WrapperRegistry's parent-owner replacement, computed from the same publication as F8/F9/F16.
//! Stored direct root grants remain intact, including while a derived bitmap replaces them.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

use super::{
    GrantRow, OperatorRow,
    candidates::Key,
    facts::authority_facts,
    masked_grants,
    page::{direct_row, operator_row},
    registry_support::{self, Model},
};
use crate::{
    EffectivePermissionRow, PermissionGrantRelation,
    families::{
        control::{
            lifecycle::{Clock, view::registration_lapsed},
            rows::{Maxima, text},
        },
        name::FamilyPublication,
    },
};

// Start address candidates from ownership/approvals, then the reverse parent-entry index.
// The virtual parent's nonempty stored grant supplies the already-persisted root resource id.
// Recognition happens once per selected W/P pair in compose(), before page rows are retained.
const PARENT_ROOT: &str = "JOIN bigname_phase.project_ens_v2_registry_parent parent
      ON parent.chain_id = entry.chain_id AND parent.parent = entry.registry
     AND parent.parent_entry_key = entry.entry_key
    JOIN bigname_phase.project_grant virtual_grant
      ON virtual_grant.chain_id = parent.chain_id AND virtual_grant.subject = parent.parent
     AND virtual_grant.scope = 'root'
     AND virtual_grant.scope_detail ->> 'registry_address' = parent.registry
    JOIN bigname_phase.project_family_marker marker ON marker.chain_id = entry.chain_id
    WHERE entry.status = 'registered' AND entry.owner IS NOT NULL
      AND entry.expiry > extract(epoch FROM marker.block_timestamp)
      AND ($9::text IS NULL OR entry.chain_id = $9)
      AND ($2::uuid IS NULL OR virtual_grant.resource_id = COALESCE($8::uuid, $2::uuid))";
const ROOT_RESOURCE: &str = "CASE WHEN $8::uuid = virtual_grant.resource_id
        THEN $2::uuid ELSE virtual_grant.resource_id END";

pub(super) fn owner_candidates() -> String {
    format!(
        "SELECT entry.owner AS subject, {ROOT_RESOURCE} AS resource_id, 'root'::text AS scope
        FROM bigname_phase.project_ens_v2_entry_owner entry {PARENT_ROOT}
          AND ($1::text IS NULL OR entry.owner = $1)"
    )
}

pub(super) fn operator_candidates() -> String {
    format!(
        "SELECT approval.subject, {ROOT_RESOURCE} AS resource_id,
        concat('account:', approval.chain_id, ':ens_v2_registry:',
            approval.authority_contract, ':', approval.owner) AS scope
        FROM bigname_phase.project_account_approval approval
        JOIN bigname_phase.project_ens_v2_entry_owner entry
          ON entry.chain_id = approval.chain_id AND entry.registry = approval.authority_contract
         AND entry.owner = approval.owner
        {PARENT_ROOT}
          AND approval.authority_kind = 'ens_v2_registry'
          AND approval.relation_kind = 'operator' AND approval.approved
          AND approval.subject <> approval.owner
          AND ($1::text IS NULL OR approval.subject = $1)"
    )
}

/// Root and token reads share this compositor. It suppresses each qualifying account's direct
/// W-root row first, even when the parent's bitmap is empty, then emits only selected derived
/// keys. The owner needs no parent role grant; approvals belong to P, while powers belong to W.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L273-L287 @ ens_v2_sepolia_20261001@07e55a05)
pub(super) async fn compose(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    clock: &Clock,
    keys: &[Key],
    rows: &mut Vec<EffectivePermissionRow>,
) -> Result<()> {
    let resources: Vec<Uuid> = keys
        .iter()
        .map(|key| key.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let facts = authority_facts(
        conn,
        &publication.chain_id,
        publication.block_number,
        &resources,
    )
    .await?;
    // Token facts name their registry root. A root's own grants need not carry
    // authority_kind, so also try the selected resource itself; the resource-keyed
    // root grant and exact admitted-root UUID check below must both confirm it.
    let roots: BTreeMap<Uuid, Uuid> = resources
        .iter()
        .map(|resource| {
            (
                *resource,
                facts
                    .get(resource)
                    .and_then(|facts| facts.root_resource_id)
                    .unwrap_or(*resource),
            )
        })
        .collect();
    if roots.is_empty() {
        return Ok(());
    }
    let root_ids: Vec<Uuid> = roots
        .values()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let subjects: Vec<String> = keys
        .iter()
        .map(|key| key.subject.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    // Resolve each root's address from one of its stored, resource-keyed root grants. The
    // recognition check below verifies that this is the admitted instance's actual root id.
    let parents: Vec<(Uuid, String, String, String)> = sqlx::query_as(
        "/* storage:families.control.permissions.wrapper_parents */
         SELECT root.resource_id, parent.registry, parent.parent, parent.parent_entry_key
         FROM unnest($2::uuid[]) root(resource_id)
         JOIN LATERAL (
             SELECT grant_row.scope_detail ->> 'registry_address' AS registry
             FROM bigname_phase.project_grant grant_row
             WHERE grant_row.chain_id = $1 AND grant_row.resource_id = root.resource_id
               AND grant_row.scope = 'root'
             ORDER BY grant_row.subject COLLATE \"C\" LIMIT 1
         ) registry ON true
         JOIN bigname_phase.project_ens_v2_registry_parent parent
           ON parent.chain_id = $1 AND parent.registry = registry.registry
         WHERE parent.parent IS NOT NULL AND parent.parent_entry_key IS NOT NULL",
    )
    .bind(&publication.chain_id)
    .bind(&root_ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load current WrapperRegistry parents")?;
    let mut supported = BTreeMap::new();
    for (_, registry, parent, _) in &parents {
        for address in [registry, parent] {
            if !supported.contains_key(address) {
                let support = registry_support::load(conn, publication, address).await?;
                supported.insert(address.clone(), support);
            }
        }
    }
    let states: BTreeMap<Uuid, Maxima> = sqlx::query_scalar::<_, Value>(
        "/* storage:families.control.permissions.wrapper_key_states */
         SELECT to_jsonb(state) FROM bigname_phase.project_lifecycle_key_state state
         WHERE state.chain_id = $1 AND state.resource_id = ANY($2::uuid[])",
    )
    .bind(&publication.chain_id)
    .bind(&resources)
    .fetch_all(&mut *conn)
    .await?
    .iter()
    .filter_map(|row| {
        Some((
            text(row, "resource_id")?.parse().ok()?,
            Maxima::from_row(row),
        ))
    })
    .collect();
    for (root, registry, parent, entry_key) in parents {
        let (Some(Some(wrapper)), Some(Some(parent_model))) =
            (supported.get(&registry), supported.get(&parent))
        else {
            continue;
        };
        if wrapper.model != Model::Wrapper
            || wrapper.root != root
            || wrapper.namespace != parent_model.namespace
        {
            continue;
        }
        let owner: Option<String> = sqlx::query_scalar(
            "/* storage:families.control.permissions.wrapper_owner */
             SELECT owner FROM bigname_phase.project_ens_v2_entry_owner
             WHERE chain_id = $1 AND registry = $2 AND entry_key = $3
               AND status = 'registered' AND owner IS NOT NULL AND expiry > $4::numeric",
        )
        .bind(&publication.chain_id)
        .bind(&parent)
        .bind(&entry_key)
        .bind(clock.timestamp_seconds)
        .fetch_optional(&mut *conn)
        .await?;
        let Some(owner) = owner else {
            continue;
        };
        let approvals: Vec<Value> = sqlx::query_scalar(
            "/* storage:families.control.permissions.wrapper_approvals */
             SELECT to_jsonb(approval) FROM bigname_phase.project_account_approval approval
             WHERE approval.chain_id = $1 AND approval.authority_kind = 'ens_v2_registry'
               AND approval.authority_contract = $2 AND approval.owner = $3
               AND approval.relation_kind = 'operator' AND approval.approved
               AND approval.subject = ANY($4::text[]) AND approval.subject <> approval.owner",
        )
        .bind(&publication.chain_id)
        .bind(&parent)
        .bind(&owner)
        .bind(&subjects)
        .fetch_all(&mut *conn)
        .await?;
        let approved: BTreeMap<String, &Value> = approvals
            .iter()
            .filter_map(|approval| Some((text(approval, "subject")?, approval)))
            .collect();
        // Qualification, not a nonempty replacement, decides whether a direct grant is dormant.
        rows.retain(|row| {
            !(roots.get(&row.resource_id) == Some(&root)
                && row.scope.storage_key() == "root"
                && (row.subject == owner || approved.contains_key(&row.subject)))
        });
        let raw: Option<Value> = sqlx::query_scalar(
            "/* storage:families.control.permissions.wrapper_virtual_grant */
             SELECT to_jsonb(grant_row) FROM bigname_phase.project_grant grant_row
             WHERE grant_row.chain_id = $1 AND grant_row.resource_id = $2
               AND grant_row.subject = $3 AND grant_row.scope = 'root'",
        )
        .bind(&publication.chain_id)
        .bind(root)
        .bind(&parent)
        .fetch_optional(&mut *conn)
        .await?;
        let Some(grant) = raw.as_ref().and_then(GrantRow::from_row) else {
            continue;
        };
        let Some(grant) = masked_grants(&[grant], None, None, clock.timestamp_seconds).pop() else {
            continue;
        };
        let account_scope = format!(
            "account:{}:ens_v2_registry:{parent}:{owner}",
            publication.chain_id
        );
        for key in keys
            .iter()
            .filter(|key| roots.get(&key.resource_id) == Some(&root))
        {
            if states
                .get(&key.resource_id)
                .is_some_and(registration_lapsed)
            {
                continue;
            }
            if key.subject == owner && key.scope == "root" {
                let mut holder = grant.clone();
                holder.resource_id = key.resource_id.to_string();
                holder.subject.clone_from(&owner);
                let mut row = direct_row(&holder, publication)?;
                row.grant_relation = Some(PermissionGrantRelation::Holder);
                rows.push(row);
            } else if key.scope == account_scope
                && let Some(approval) = approved.get(&key.subject)
            {
                let derived = operator_row(
                    &OperatorRow {
                        resource_id: key.resource_id.to_string(),
                        subject: key.subject.clone(),
                        scope: account_scope.clone(),
                        scope_detail: json!({"chain_id": publication.chain_id,
                        "authority_kind": "ens_v2_registry", "authority_contract": parent, "owner": owner}),
                        effective_powers: grant.effective_powers.clone(),
                        grant_source: approval.get("grant_source").cloned().unwrap_or(Value::Null),
                        inheritance_path: grant.inheritance_path.clone(),
                        transfer_behavior: grant.transfer_behavior.clone(),
                    },
                    publication,
                )?;
                // If W is its own current parent, one approval can supply both the token's
                // roles and the root bitmap under the same account key. Serve one combined row.
                if let Some(existing) = rows.iter_mut().find(|row| {
                    row.resource_id == derived.resource_id
                        && row.subject == derived.subject
                        && row.scope.storage_key() == derived.scope.storage_key()
                }) {
                    if let (Some(powers), Some(additional)) = (
                        existing.effective_powers.as_array_mut(),
                        derived.effective_powers.as_array(),
                    ) {
                        for power in additional {
                            if !powers.contains(power) {
                                powers.push(power.clone());
                            }
                        }
                    }
                    if let (Some(path), Some(additional)) = (
                        existing.inheritance_path.as_array_mut(),
                        derived.inheritance_path.as_array(),
                    ) {
                        for step in additional {
                            if !path.contains(step) {
                                path.push(step.clone());
                            }
                        }
                    }
                } else {
                    rows.push(derived);
                }
            }
        }
    }
    Ok(())
}
