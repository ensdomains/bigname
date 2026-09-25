//! F8 and F9, raw permissions: the shadow of `permissions_current`, the restriction block and
//! registry binding of `permissions_current_resource_summary`, `account_permission_state_current`
//! and the registry-operator rows the effective-permission reader adds (permissions/effective.rs
//! :63-72).
mod grants;
mod summary;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgPool;

pub use grants::{
    GrantRow, ServedGrant, WrapperApproval, masked_grants, masked_powers, with_operators,
};
pub use summary::{admin_powers, locked_roles, resource_restrictions, wrapper_unwrapped};

use super::{
    lifecycle::Clock,
    registry::RegistryBinding,
    rows::{LifecycleEvent, Maxima, flag, lower, text},
    wrapper::load_wrapper_rows,
};

/// One resource to read, with the summary facts the families do not hold: the authority kind
/// today's summary derives from the resource's whole event history (resource_summary.rs:53-126,
/// :218-237) and the registry root from the identity table.
#[derive(Clone, Debug, Default)]
pub struct ResourceInput {
    pub resource_id: String,
    pub authority_kind: Option<String>,
    pub root_resource_id: Option<String>,
}

/// The shadow of one resource's permission rows and restriction block.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShadowPermissions {
    pub grants: Vec<ServedGrant>,
    pub admin_powers: Vec<String>,
    pub restrictions: Option<Value>,
}

async fn rows_for(
    pool: &PgPool,
    sql: &str,
    chain_id: &str,
    resources: &[String],
) -> Result<Vec<Value>> {
    if resources.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar(sql)
        .bind(chain_id)
        .bind(resources)
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to run {}", sql.lines().next().unwrap_or(sql)))
}

/// Load and evaluate the permission shadow of `resources` at `clock`.
pub async fn load_shadow_permissions(
    pool: &PgPool,
    chain_id: &str,
    clock: &Clock,
    resources: &[ResourceInput],
) -> Result<BTreeMap<String, ShadowPermissions>> {
    let mut ids: BTreeSet<String> = resources
        .iter()
        .map(|input| input.resource_id.clone())
        .collect();
    ids.extend(
        resources
            .iter()
            .filter_map(|input| input.root_resource_id.clone()),
    );
    let ids: Vec<String> = ids.into_iter().collect();
    let grants: Vec<GrantRow> = rows_for(
        pool,
        "/* storage:families.control.permissions.grants */ SELECT to_jsonb(grant_row)
         FROM bigname_phase.project_grant grant_row
         WHERE grant_row.chain_id = $1 AND grant_row.resource_id = ANY($2::uuid[])",
        chain_id,
        &ids,
    )
    .await?
    .iter()
    .filter_map(GrantRow::from_row)
    .collect();
    let aggregates: BTreeMap<String, Value> = rows_for(
        pool,
        "/* storage:families.control.permissions.admin_aggregates */ SELECT to_jsonb(aggregate)
         FROM bigname_phase.project_resource_admin_aggregate aggregate
         WHERE aggregate.chain_id = $1 AND aggregate.resource_id = ANY($2::uuid[])",
        chain_id,
        &ids,
    )
    .await?
    .iter()
    .filter_map(|row| Some((text(row, "resource_id")?, row.get("admin_powers")?.clone())))
    .collect();
    let key_states: BTreeMap<String, Maxima> = rows_for(
        pool,
        "/* storage:families.control.permissions.key_states */ SELECT to_jsonb(state)
         FROM bigname_phase.project_lifecycle_key_state state
         WHERE state.chain_id = $1 AND state.resource_id = ANY($2::uuid[])",
        chain_id,
        &ids,
    )
    .await?
    .iter()
    .filter_map(|row| Some((text(row, "resource_id")?, Maxima::from_row(row))))
    .collect();
    let mints: Vec<LifecycleEvent> = rows_for(
        pool,
        "/* storage:families.control.permissions.wrapper_mints */ SELECT to_jsonb(event)
         FROM bigname_phase.project_lifecycle_event event
         WHERE event.chain_id = $1 AND event.state_kind = 'resource' AND event.state_key = ANY($2)
           AND event.event_kind = 'TokenControlTransferred'
           AND event.source_family = 'ens_v1_wrapper_l1'",
        chain_id,
        &ids,
    )
    .await?
    .iter()
    .filter_map(LifecycleEvent::from_row)
    .collect();
    let wrappers: BTreeMap<String, _> = load_wrapper_rows(pool, chain_id, &ids)
        .await?
        .into_iter()
        .map(|row| (row.resource_id.clone(), row))
        .collect();
    let holders: Vec<String> = grants
        .iter()
        .filter(|grant| {
            grant
                .grant_source
                .get("relation_kind")
                .and_then(Value::as_str)
                == Some("holder")
        })
        .map(|grant| grant.subject.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let approvals: Vec<WrapperApproval> = rows_for(
        pool,
        "/* storage:families.control.permissions.wrapper_approvals */ SELECT to_jsonb(approval)
         FROM bigname_phase.project_account_approval approval
         WHERE approval.chain_id = $1 AND approval.authority_kind = 'wrapper'
           AND approval.owner = ANY($2)",
        chain_id,
        &holders,
    )
    .await?
    .iter()
    .filter_map(|row| {
        Some(WrapperApproval {
            authority_contract: lower(row, "authority_contract")?,
            owner: lower(row, "owner")?,
            subject: lower(row, "subject")?,
            approved: flag(row, "approved")?,
        })
    })
    .collect();

    let served = |resource: &str| -> Vec<ServedGrant> {
        let own: Vec<GrantRow> = grants
            .iter()
            .filter(|grant| grant.resource_id == resource)
            .cloned()
            .collect();
        masked_grants(
            &own,
            wrappers.get(resource),
            key_states.get(resource),
            clock.timestamp_seconds,
        )
    };
    let admins = |resource: &str| -> Vec<String> {
        // The admin rows are served rows: a resource whose registration lapsed by path expiry
        // serves none (resource_summary.rs:272-297 reads the staged permission rows).
        if key_states
            .get(resource)
            .is_some_and(crate::families::control::lifecycle::view::registration_lapsed)
        {
            return Vec::new();
        }
        aggregates
            .get(resource)
            .map(admin_powers)
            .unwrap_or_default()
    };
    let mut out = BTreeMap::new();
    for input in resources {
        let resource = input.resource_id.as_str();
        let rows = with_operators(served(resource), &approvals);
        let own_admins = admins(resource);
        let root_admins = input
            .root_resource_id
            .as_deref()
            .map(admins)
            .unwrap_or_default();
        let unwrapped = wrapper_unwrapped(resource, &mints, &grants);
        let restrictions = resource_restrictions(
            input.authority_kind.as_deref(),
            wrappers.get(resource),
            unwrapped,
            clock.timestamp_seconds,
            !rows.is_empty(),
            &own_admins,
            &root_admins,
        );
        out.insert(
            input.resource_id.clone(),
            ShadowPermissions {
                grants: rows,
                admin_powers: own_admins,
                restrictions,
            },
        );
    }
    Ok(out)
}

/// One served account approval (`account_permission_state_current`), in the columns the
/// comparison reads; the whole-history evidence arrays are dropped (design F9 row).
#[derive(Clone, Debug, PartialEq)]
pub struct ServedApproval {
    pub authority_kind: String,
    pub authority_contract: String,
    pub authority_contract_instance_id: Option<String>,
    pub owner: String,
    pub subject: String,
    pub relation_kind: String,
    pub approved: bool,
    pub effective_powers: Value,
    pub grant_source: Value,
    pub revocation_source: Value,
    pub inheritance_path: Value,
    pub transfer_behavior: Value,
}

impl ServedApproval {
    fn from_row(row: &Value) -> Option<Self> {
        let column = |field: &str| row.get(field).cloned().unwrap_or(Value::Null);
        Some(Self {
            authority_kind: text(row, "authority_kind")?,
            authority_contract: lower(row, "authority_contract")?,
            authority_contract_instance_id: text(row, "authority_contract_instance_id"),
            owner: lower(row, "owner")?,
            subject: lower(row, "subject")?,
            relation_kind: text(row, "relation_kind")?,
            approved: flag(row, "approved")?,
            effective_powers: column("effective_powers"),
            grant_source: column("grant_source"),
            revocation_source: column("revocation_source"),
            inheritance_path: column("inheritance_path"),
            transfer_behavior: column("transfer_behavior"),
        })
    }

    /// The key the served table uses.
    pub fn key(&self) -> (String, String, String, String, String) {
        (
            self.authority_kind.clone(),
            self.authority_contract.clone(),
            self.owner.clone(),
            self.subject.clone(),
            self.relation_kind.clone(),
        )
    }
}

/// Every F9 approval of the chain.
pub async fn load_shadow_approvals(pool: &PgPool, chain_id: &str) -> Result<Vec<ServedApproval>> {
    let rows: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.control.permissions.approvals */ SELECT to_jsonb(approval)
         FROM bigname_phase.project_account_approval approval
         WHERE approval.chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("failed to load account approvals")?;
    Ok(rows.iter().filter_map(ServedApproval::from_row).collect())
}

/// One registry-operator row the effective-permission reader adds for a resource.
#[derive(Clone, Debug, PartialEq)]
pub struct OperatorRow {
    pub resource_id: String,
    pub subject: String,
    pub scope: String,
    pub effective_powers: Value,
    pub grant_source: Value,
    pub inheritance_path: Value,
    pub transfer_behavior: Value,
}

/// The approved registry operators of a resource's registry binding (permissions/effective.rs
/// :63-72): approvals of the binding's registry contract and owner, relation operator.
pub fn effective_operator_rows(
    chain_id: &str,
    resource_id: &str,
    binding: &RegistryBinding,
    approvals: &[ServedApproval],
) -> Vec<OperatorRow> {
    let (Some(owner), Some(contract)) = (&binding.registry_owner, &binding.registry_contract)
    else {
        return Vec::new();
    };
    let mut rows: Vec<OperatorRow> = approvals
        .iter()
        .filter(|approval| {
            approval.approved
                && approval.authority_kind == "registry"
                && approval.relation_kind == "operator"
                && &approval.authority_contract == contract
                && &approval.owner == owner
        })
        .map(|approval| OperatorRow {
            resource_id: resource_id.to_owned(),
            subject: approval.subject.clone(),
            scope: format!(
                "account:{chain_id}:{}:{}:{}",
                approval.authority_kind, approval.authority_contract, approval.owner
            ),
            effective_powers: approval.effective_powers.clone(),
            grant_source: approval.grant_source.clone(),
            inheritance_path: approval.inheritance_path.clone(),
            transfer_behavior: approval.transfer_behavior.clone(),
        })
        .collect();
    rows.sort_by(|left, right| (&left.subject, &left.scope).cmp(&(&right.subject, &right.scope)));
    rows
}

/// A served grant as the JSON the harness compares.
pub fn grant_json(grant: &ServedGrant) -> Value {
    json!({
        "resource_id": grant.resource_id,
        "subject": grant.subject,
        "scope": grant.scope,
        "scope_kind": grant.scope_kind,
        "scope_detail": grant.scope_detail,
        "effective_powers": grant.effective_powers,
        "grant_source": grant.grant_source,
        "revocation_source": grant.revocation_source,
        "inheritance_path": grant.inheritance_path,
        "transfer_behavior": grant.transfer_behavior,
    })
}
