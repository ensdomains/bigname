//! `GET /v1/permissions` from the owned key families (TYR-36 step 7b slice 4): the
//! effective-permission page and the resource summaries the route reads, with the served
//! readers' keyset, sort, filters and shapes (permissions/effective.rs, resource_summary.rs).
//!
//! - Direct rows are each resource's served permission rows computed from its F8 grants
//!   ([`load_shadow_permissions_on`]: the wrapper fuse and grace masks at the publication's block
//!   time, the ENSv2 path-expiry drop, the empty-row drop and the NameWrapper operator fan-out).
//! - Registry-operator rows are the F9 approvals of the resource's F2c registry binding
//!   (`operators.rs`).
//! - The summary's authority kind, registry root and readability come from the identity and
//!   event inputs by key (`facts.rs`); the restriction block is the family one.
//!
//! Each read runs in one read-only REPEATABLE READ snapshot and describes the family marker's
//! publication of every chain it touches; a chain whose marker is not servable (a rebuild in
//! flight, or another build's) fails the read with [`FamilyPublicationUnavailable`], which the
//! API answers with the stale 409. The served filter's other two predicates, the row's own
//! canonicality and its publication lineage, have no family counterpart: the family undo
//! removes what a dropped block wrote (ruling J13).
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    OperatorRow, ResourceInput, ServedGrant, effective_operator_rows,
    facts::{authority_facts, in_namespace, readable_resources},
    load_shadow_permissions_on,
    operators::{approvals_of_subject, bindings_for, registry_approvals, resources_bound_to},
};
use crate::{
    EffectivePermissionRow, EffectivePermissionScope, EffectivePermissionsAccountResourcePage,
    PermissionGrantRelation, PermissionScope, PermissionsCurrentAccountResourceCursor,
    PermissionsCurrentResourceSummary, ResourcePermissionCoverage,
    families::{
        control::{lifecycle::Clock, position::EventOrder},
        name::{FamilyPublication, FamilyPublicationUnavailable, publication_on, read_snapshot},
    },
    projection_helpers::{checked_page_limit_i64, checked_page_size_usize, split_keyset_page},
};

/// The served page of [`crate::load_effective_permissions_account_resource_page`], read from the
/// families: the rows of `subject` and/or `resource_id` after `cursor`, ordered by subject,
/// resource and scope key as bytes (the served `COLLATE "C"` order).
pub async fn load_family_effective_permissions_page(
    pool: &PgPool,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    namespace: Option<&str>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    page_size: u64,
) -> Result<EffectivePermissionsAccountResourcePage> {
    if subject.is_none() && resource_id.is_none() {
        bail!("effective permissions require subject or resource_id")
    }
    checked_page_limit_i64(
        page_size,
        "effective permissions page_size must be positive",
        "effective permissions page_size is too large",
    )?;
    let size = checked_page_size_usize(
        page_size,
        "effective permissions page_size must be positive",
        "effective permissions page_size must fit usize",
    )?;
    let mut snapshot = read_snapshot(pool).await?;
    let mut rows = effective_rows(&mut snapshot, subject, resource_id, namespace).await?;
    snapshot.commit().await?;
    let key = |row: &EffectivePermissionRow| {
        (
            row.subject.clone().into_bytes(),
            row.resource_id,
            row.scope.storage_key().into_bytes(),
        )
    };
    if let Some(cursor) = cursor {
        let after = (
            cursor.subject.clone().into_bytes(),
            cursor.resource_id,
            cursor.scope.clone().into_bytes(),
        );
        rows.retain(|row| key(row) > after);
    }
    rows.sort_by_key(key);
    rows.truncate(size.saturating_add(1));
    let (rows, next_cursor) = split_keyset_page(rows, size, |row| {
        PermissionsCurrentAccountResourceCursor::from(row)
    });
    Ok(EffectivePermissionsAccountResourcePage {
        rows,
        next_cursor,
        summary: None,
    })
}

/// The served summaries of [`crate::load_permissions_current_resource_summaries`], read from the
/// families: one per readable resource of `resource_ids` at or below its chain's publication.
pub async fn load_family_permission_summaries(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, PermissionsCurrentResourceSummary>> {
    if resource_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut snapshot = read_snapshot(pool).await?;
    let mut out = BTreeMap::new();
    for (publication, resources) in published(&mut snapshot, resource_ids).await? {
        let chain_id = publication.chain_id.as_str();
        let facts = authority_facts(
            &mut snapshot,
            chain_id,
            publication.block_number,
            &resources,
        )
        .await?;
        let inputs: Vec<ResourceInput> = resources
            .iter()
            .map(|resource| {
                let facts = facts.get(resource).cloned().unwrap_or_default();
                ResourceInput {
                    resource_id: resource.to_string(),
                    authority_kind: facts.authority_kind,
                    root_resource_id: facts.root_resource_id.map(|root| root.to_string()),
                }
            })
            .collect();
        let shadows = load_shadow_permissions_on(
            &mut snapshot,
            chain_id,
            &clock(&publication),
            &inputs,
            &EventOrder::Canonical,
        )
        .await?;
        for input in inputs {
            let resource: Uuid = input.resource_id.parse()?;
            let restrictions = shadows
                .get(&input.resource_id)
                .and_then(|shadow| shadow.restrictions.clone());
            out.insert(
                resource,
                PermissionsCurrentResourceSummary {
                    resource_id: resource,
                    coverage: coverage(input.authority_kind.as_deref()),
                    authority_kind: input.authority_kind,
                    root_resource_id: input
                        .root_resource_id
                        .as_deref()
                        .map(str::parse)
                        .transpose()?,
                    resource_restrictions: restrictions,
                    provenance: json!({"chain_id": chain_id}),
                    chain_positions: publication_positions(&publication),
                    canonicality_summary: canonicality(&publication),
                    manifest_version: 1,
                    last_recomputed_at: publication.block_timestamp,
                },
            );
        }
    }
    snapshot.commit().await?;
    Ok(out)
}

/// The summary coverage by authority kind: the served summary is always unsupported, with the
/// reason its authority kind gives (resource_summary.rs), and the reader maps each reason to
/// its typed coverage (permissions/resource_summary.rs `SUMMARY_SELECT_COLUMNS`).
fn coverage(authority_kind: Option<&str>) -> ResourcePermissionCoverage {
    match authority_kind {
        Some("wrapper") => {
            ResourcePermissionCoverage::wrapper_parent_and_resolver_delegation_not_projected()
        }
        Some(
            "registrar" | "registry" | "registry_only" | "registry_owner" | "registrant"
            | "resolver" | "ens_v2_registry",
        ) => ResourcePermissionCoverage::operator_approval_surfaces_not_ingested(),
        _ => ResourcePermissionCoverage::resource_authority_not_projected(),
    }
}

/// `resource_ids` that are readable and at or below their chain's publication, grouped by that
/// publication. A chain with no servable marker fails the read.
async fn published(
    conn: &mut PgConnection,
    resource_ids: &[Uuid],
) -> Result<Vec<(FamilyPublication, Vec<Uuid>)>> {
    let readable = readable_resources(conn, resource_ids).await?;
    let mut by_chain: BTreeMap<String, Vec<(Uuid, i64)>> = BTreeMap::new();
    for (resource, fact) in readable {
        by_chain
            .entry(fact.chain_id)
            .or_default()
            .push((resource, fact.block_number));
    }
    let mut out = Vec::new();
    for (chain_id, resources) in by_chain {
        let Some(publication) = publication_on(conn, &chain_id).await? else {
            return Err(FamilyPublicationUnavailable { chain_id }.into());
        };
        let resources: Vec<Uuid> = resources
            .into_iter()
            .filter(|(_, block)| *block <= publication.block_number)
            .map(|(resource, _)| resource)
            .collect();
        if !resources.is_empty() {
            out.push((publication, resources));
        }
    }
    Ok(out)
}

fn clock(publication: &FamilyPublication) -> Clock {
    Clock {
        block_number: publication.block_number,
        timestamp_seconds: publication.timestamp_seconds(),
    }
}

fn publication_positions(publication: &FamilyPublication) -> Value {
    json!({
        "target_block_number": publication.block_number,
        "target_block_hash": publication.block_hash,
    })
}

fn canonicality(publication: &FamilyPublication) -> Value {
    json!({
        "state": "canonical_lineage",
        "target_block_number": publication.block_number,
        "target_block_hash": publication.block_hash,
    })
}

/// Every effective row of `subject` and/or `resource_id`, unordered.
async fn effective_rows(
    conn: &mut PgConnection,
    subject: Option<&str>,
    resource_id: Option<Uuid>,
    namespace: Option<&str>,
) -> Result<Vec<EffectivePermissionRow>> {
    let (direct_candidates, operator_approvals) = match (resource_id, subject) {
        (Some(resource), _) => (vec![resource], None),
        (None, Some(subject)) => (
            subject_resources(conn, subject).await?,
            Some(approvals_of_subject(conn, subject, "registry").await?),
        ),
        (None, None) => bail!("effective permissions require subject or resource_id"),
    };
    let mut rows = Vec::new();
    for (publication, resources) in published(conn, &direct_candidates).await? {
        let chain_id = publication.chain_id.clone();
        let inputs: Vec<ResourceInput> = resources
            .iter()
            .map(|resource| ResourceInput {
                resource_id: resource.to_string(),
                ..ResourceInput::default()
            })
            .collect();
        let shadows = load_shadow_permissions_on(
            conn,
            &chain_id,
            &clock(&publication),
            &inputs,
            &EventOrder::Canonical,
        )
        .await?;
        for shadow in shadows.values() {
            for grant in &shadow.grants {
                if subject.is_none_or(|subject| grant.subject == subject) {
                    rows.push(direct_row(grant, &publication)?);
                }
            }
        }
        if resource_id.is_some() {
            for resource in &resources {
                rows.extend(
                    resource_operator_rows(conn, &publication, *resource)
                        .await?
                        .into_iter()
                        .filter(|row| subject.is_none_or(|subject| row.subject == subject)),
                );
            }
        }
    }
    if let Some(approvals) = operator_approvals {
        rows.extend(subject_operator_rows(conn, &approvals).await?);
    }
    if let Some(namespace) = namespace {
        let ids: Vec<Uuid> = rows
            .iter()
            .map(|row| row.resource_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let members = in_namespace(conn, &ids, namespace).await?;
        rows.retain(|row| members.contains(&row.resource_id));
    }
    Ok(rows)
}

/// The resources whose served rows can name `subject`: its own grants, and the wrapper holder
/// grants of every holder that approved it as a NameWrapper operator on that wrapper contract
/// (the fan-out of `with_operators`).
async fn subject_resources(conn: &mut PgConnection, subject: &str) -> Result<Vec<Uuid>> {
    let resources: Vec<Uuid> = sqlx::query_scalar(
        "/* storage:families.control.permissions.subject_resources */
         SELECT grant_row.resource_id
         FROM bigname_phase.project_grant grant_row
         WHERE grant_row.subject = $1
         UNION
         SELECT grant_row.resource_id
         FROM bigname_phase.project_account_approval approval
         JOIN bigname_phase.project_grant grant_row
           ON grant_row.chain_id = approval.chain_id AND grant_row.subject = approval.owner
         WHERE approval.subject = $1 AND approval.authority_kind = 'wrapper' AND approval.approved
           AND grant_row.grant_source ->> 'authority_kind' = 'wrapper'
           AND grant_row.grant_source ->> 'relation_kind' = 'holder'
           AND lower(grant_row.grant_source ->> 'authority_contract') =
               approval.authority_contract",
    )
    .bind(subject)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the resources of an account's permissions")?;
    Ok(resources)
}

/// The registry-operator rows of one resource under its registry binding.
async fn resource_operator_rows(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    resource: Uuid,
) -> Result<Vec<EffectivePermissionRow>> {
    let chain_id = publication.chain_id.as_str();
    let bindings = bindings_for(conn, chain_id, &[resource]).await?;
    let Some(binding) = bindings.get(&resource) else {
        return Ok(Vec::new());
    };
    let (Some(contract), Some(owner)) = (&binding.registry_contract, &binding.registry_owner)
    else {
        return Ok(Vec::new());
    };
    let approvals = registry_approvals(conn, chain_id, contract, owner).await?;
    effective_operator_rows(chain_id, &resource.to_string(), binding, &approvals)
        .iter()
        .map(|row| operator_row(row, publication))
        .collect()
}

/// The registry-operator rows of a subject's approvals: for each approved `(chain, registry
/// contract, owner)`, every published resource whose registry binding is that pair.
async fn subject_operator_rows(
    conn: &mut PgConnection,
    approvals: &[(String, super::ServedApproval)],
) -> Result<Vec<EffectivePermissionRow>> {
    let mut by_pair: BTreeMap<(String, String, String), Vec<super::ServedApproval>> =
        BTreeMap::new();
    for (chain_id, approval) in approvals {
        by_pair
            .entry((
                chain_id.clone(),
                approval.authority_contract.clone(),
                approval.owner.clone(),
            ))
            .or_default()
            .push(approval.clone());
    }
    let mut rows = Vec::new();
    for ((chain_id, contract, owner), approvals) in by_pair {
        let candidates = resources_bound_to(conn, &chain_id, &contract, &owner).await?;
        for (publication, resources) in published(conn, &candidates).await? {
            if publication.chain_id != chain_id {
                continue;
            }
            let bindings = bindings_for(conn, &chain_id, &resources).await?;
            for (resource, binding) in bindings {
                if binding.registry_contract.as_deref() != Some(contract.as_str())
                    || binding.registry_owner.as_deref() != Some(owner.as_str())
                {
                    continue;
                }
                for row in
                    effective_operator_rows(&chain_id, &resource.to_string(), &binding, &approvals)
                {
                    rows.push(operator_row(&row, &publication)?);
                }
            }
        }
    }
    Ok(rows)
}

/// A family permission row in the served effective row's shape. The provenance, coverage,
/// positions and recompute time describe the publication; the route reads none of them.
fn direct_row(
    grant: &ServedGrant,
    publication: &FamilyPublication,
) -> Result<EffectivePermissionRow> {
    let kind = grant
        .scope_kind
        .as_deref()
        .with_context(|| format!("family grant {} has no scope kind", grant.scope))?;
    let scope = PermissionScope::parse(kind, &grant.scope_detail)?;
    if scope.storage_key() != grant.scope {
        bail!(
            "family grant scope mismatch: stored {}, decoded {}",
            grant.scope,
            scope.storage_key()
        );
    }
    Ok(EffectivePermissionRow {
        resource_id: grant.resource_id.parse()?,
        subject: grant.subject.clone(),
        scope: EffectivePermissionScope::Direct(scope),
        record_resource_selector: grant.scope_detail.get("resource_selector").cloned(),
        grant_relation: None,
        effective_powers: grant.effective_powers.clone(),
        grant_source: grant.grant_source.clone(),
        revocation_source: (!grant.revocation_source.is_null())
            .then(|| grant.revocation_source.clone()),
        inheritance_path: grant.inheritance_path.clone(),
        transfer_behavior: grant.transfer_behavior.clone(),
        provenance: json!({"chain_id": publication.chain_id}),
        coverage: json!({"status": "projected", "exhaustiveness": "not_asserted"}),
        chain_positions: publication_positions(publication),
        canonicality_summary: canonicality(publication),
        manifest_version: 1,
        last_recomputed_at: recomputed(publication),
    })
}

fn operator_row(
    row: &OperatorRow,
    publication: &FamilyPublication,
) -> Result<EffectivePermissionRow> {
    let text = |field: &str| -> Result<String> {
        row.scope_detail
            .get(field)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .with_context(|| format!("operator scope has no {field}"))
    };
    Ok(EffectivePermissionRow {
        resource_id: row.resource_id.parse()?,
        subject: row.subject.clone(),
        scope: EffectivePermissionScope::Account {
            chain_id: text("chain_id")?,
            authority_kind: text("authority_kind")?,
            authority_contract: text("authority_contract")?,
            owner: text("owner")?,
        },
        record_resource_selector: None,
        grant_relation: Some(PermissionGrantRelation::Operator),
        effective_powers: row.effective_powers.clone(),
        grant_source: row.grant_source.clone(),
        revocation_source: None,
        inheritance_path: row.inheritance_path.clone(),
        transfer_behavior: row.transfer_behavior.clone(),
        provenance: json!({"chain_id": publication.chain_id}),
        coverage: json!({"status": "projected", "exhaustiveness": "not_asserted"}),
        chain_positions: publication_positions(publication),
        canonicality_summary: canonicality(publication),
        manifest_version: 1,
        last_recomputed_at: recomputed(publication),
    })
}

fn recomputed(publication: &FamilyPublication) -> OffsetDateTime {
    publication.block_timestamp
}
