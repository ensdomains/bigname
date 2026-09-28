//! `GET /v1/permissions` from the owned key families: the effective-permission page and the
//! resource summaries the route reads, with the keyset, sort, filters and shapes of
//! permissions/effective.rs and resource_summary.rs.
//!
//! - Direct rows are each resource's permission rows computed from its F8 grants
//!   ([`super::load_shadow_permissions_on`]: the wrapper fuse and grace masks at the publication's block
//!   time, the ENSv2 path-expiry drop, the empty-row drop and the NameWrapper operator fan-out).
//! - Registry-operator rows are the F9 approvals of the resource's F2c registry binding
//!   (`operators.rs`).
//! - The summary's authority kind, registry root and readability come from the identity and
//!   event inputs by key (`facts.rs`); the restriction block is the family one.
//!
//! Each read runs in one read-only REPEATABLE READ snapshot and describes the family marker's
//! publication of every chain it touches; a chain whose marker is not servable (a rebuild in
//! flight, or another build's) fails the read with [`FamilyPublicationUnavailable`], which the
//! API answers with the stale 409. No per-row canonicality or lineage check is needed: the
//! family undo removes what a dropped block wrote.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    OperatorRow, ResourceInput, ServedGrant, effective_operator_rows,
    facts::{authority_facts, in_namespace, readable_resources},
    load_permissions_on,
    operators::{bindings_for, registry_approvals},
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
    let rows = page_rows(
        &mut snapshot,
        subject,
        resource_id,
        namespace,
        cursor,
        size + 1,
    )
    .await?;
    snapshot.commit().await?;
    let (rows, next_cursor) = split_keyset_page(rows, size, |row| {
        PermissionsCurrentAccountResourceCursor::from(row)
    });
    Ok(EffectivePermissionsAccountResourcePage {
        rows,
        next_cursor,
        summary: None,
    })
}

/// A sentinel-bounded inline expansion for address `include=role_summary`, in one read snapshot.
pub async fn load_family_bounded_permissions(
    pool: &PgPool,
    resource_ids: &[Uuid],
    namespace: Option<&str>,
    max_rows: u64,
) -> Result<Vec<EffectivePermissionRow>> {
    let limit = checked_page_limit_i64(
        max_rows,
        "positive grant budget required",
        "grant budget too large",
    )? as usize;
    if resource_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut snapshot = read_snapshot(pool).await?;
    let mut rows = Vec::new();
    for resource in resource_ids.iter().copied().collect::<BTreeSet<_>>() {
        rows.extend(
            page_rows(
                &mut snapshot,
                None,
                Some(resource),
                namespace,
                None,
                limit - rows.len(),
            )
            .await?,
        );
        if rows.len() == limit {
            break;
        }
    }
    snapshot.commit().await?;
    Ok(rows)
}

async fn page_rows(
    conn: &mut PgConnection,
    subject: Option<&str>,
    resource: Option<Uuid>,
    namespace: Option<&str>,
    cursor: Option<&PermissionsCurrentAccountResourceCursor>,
    limit: usize,
) -> Result<Vec<EffectivePermissionRow>> {
    if let Some(resource) = resource {
        if let Some(namespace) = namespace
            && !in_namespace(conn, &[resource], namespace)
                .await?
                .contains(&resource)
        {
            return Ok(Vec::new());
        }
        published(conn, &[resource]).await?;
    }
    let mut after = cursor.cloned();
    let mut rows = Vec::new();
    while rows.len() < limit {
        let batch_size = (limit - rows.len()).min(64);
        let keys = super::candidates::page(
            conn,
            subject,
            resource,
            namespace,
            after.as_ref(),
            batch_size as i64,
        )
        .await?;
        let exhausted = keys.len() < batch_size;
        after = keys
            .last()
            .map(PermissionsCurrentAccountResourceCursor::from);
        rows.extend(effective_rows(conn, &keys).await?);
        if exhausted {
            break;
        }
    }
    Ok(rows)
}

/// The permission summaries `load_serving_permission_summaries` serves, read from the families: one
/// per readable resource of `resource_ids` at or below its chain's publication.
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
        let restrictions =
            super::restrictions::load(&mut snapshot, chain_id, &clock(&publication), &inputs)
                .await?;
        for input in inputs {
            let resource: Uuid = input.resource_id.parse()?;
            let restrictions = restrictions.get(&input.resource_id).cloned();
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

/// Compose only a bounded batch of keys; wrapper holder inputs are retained just for the
/// selected operators. The cursor and namespace predicate have already run in the candidate SQL.
async fn effective_rows(
    conn: &mut PgConnection,
    keys: &[super::candidates::Key],
) -> Result<Vec<EffectivePermissionRow>> {
    let resources: Vec<Uuid> = keys
        .iter()
        .map(|key| key.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut rows = Vec::new();
    for (publication, resources) in published(conn, &resources).await? {
        let inputs: Vec<ResourceInput> = resources
            .iter()
            .map(|resource| ResourceInput {
                resource_id: resource.to_string(),
                ..ResourceInput::default()
            })
            .collect();
        let shadows = load_permissions_on(
            conn,
            &publication.chain_id,
            &clock(&publication),
            &inputs,
            &EventOrder::Canonical,
            Some(keys),
        )
        .await?;
        for shadow in shadows.values() {
            for grant in &shadow.grants {
                rows.push(direct_row(grant, &publication)?);
            }
        }
        let bindings = bindings_for(conn, &publication.chain_id, &resources).await?;
        for (resource, binding) in bindings {
            let (Some(contract), Some(owner)) =
                (&binding.registry_contract, &binding.registry_owner)
            else {
                continue;
            };
            let subjects: Vec<String> = keys
                .iter()
                .filter(|key| key.resource_id == resource)
                .map(|key| key.subject.clone())
                .collect();
            let approvals =
                registry_approvals(conn, &publication.chain_id, contract, owner, &subjects).await?;
            for row in effective_operator_rows(
                &publication.chain_id,
                &resource.to_string(),
                &binding,
                &approvals,
            ) {
                rows.push(operator_row(&row, &publication)?);
            }
        }
    }
    rows.retain(|row| {
        keys.iter().any(|key| {
            key.resource_id == row.resource_id
                && key.subject == row.subject
                && key.scope == row.scope.storage_key()
        })
    });
    rows.sort_by_key(|row| {
        (
            row.subject.clone(),
            row.resource_id,
            row.scope.storage_key(),
        )
    });
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
