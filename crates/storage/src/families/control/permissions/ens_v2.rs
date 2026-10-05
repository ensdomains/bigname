//! The ENSv2 registry rows the effective-permission page derives when it reads, from F8 grants,
//! F9 approvals and the F16 entry rows. Nothing here is stored per token.
//!
//! - Operator rows. A registry adds the roles its current token owner holds on a token's own
//!   resource to every account that owner approved on that registry. It reads the owner's stored
//!   roles on that resource only, so the owner's root roles and the owner's own operator
//!   approvals do not follow, and the owner of an expired entry is the zero address, which has
//!   no operators. An operator row therefore joins an approved `ens_v2_registry` approval to the
//!   entry rows its owner currently holds in that registry and carries the powers of the owner's
//!   served grant on the entry's current resource. It is absent when the entry's own expiry is
//!   at or before the publication's block time, and returns when a renewal moves the expiry.
//!   (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L622-L636 @ ens_v2_sepolia_20261001@07e55a05)
//!   (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L350-L352 @ ens_v2_sepolia_20261001@07e55a05)
//! - Root-holder rows. A role check on a token reads the caller's roles on the registry root
//!   together with its roles on the token, so every root holder acts on every token of the
//!   registry. A read bound to one token resource lists the registry's root grants as rows of
//!   that resource, with every root power. `can_transfer_admin` held on the root does not let
//!   its holder transfer a token or make one transferable, because a transfer checks that role
//!   only among the token owner's own roles on the token; it does let the holder revoke that
//!   role from an account on a live token, since revocable roles are computed from the root and
//!   token roles together.
//!   (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L454-L465 @ ens_v2_sepolia_20261001@07e55a05)
//!   (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L528-L543 @ ens_v2_sepolia_20261001@07e55a05)
//!   (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L408-L417 @ ens_v2_sepolia_20261001@07e55a05)
//!   (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/access-control/EnhancedAccessControl.sol:L444-L451 @ ens_v2_sepolia_20261001@07e55a05)
//!   A registration whose rows the path-expiry drop removes lists no root holders either. A
//!   root `renew` holder can still revive an expired entry; that holder is a row of the root.
//!   (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L243-L258 @ ens_v2_sepolia_20261001@07e55a05)
//!   (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L643-L654 @ ens_v2_sepolia_20261001@07e55a05)
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

use super::{
    GrantRow, OperatorRow, ServedGrant, candidates::Key, facts::authority_facts, masked_grants,
    rows_for,
};
use crate::families::{
    control::{
        lifecycle::Clock,
        rows::{Maxima, json as column, lower, text},
    },
    name::FamilyPublication,
};

/// The scope key of a root grant, on the root resource and on a token resource alike.
const ROOT_SCOPE: &str = "root";
/// The scope key of a token owner's own roles on the token's resource.
const TOKEN_SCOPE: &str = "registry";
/// The candidate keys of ENSv2 registry operators: each approved operator with every entry its
/// approver currently owns in the approving registry, where the owner has a grant on the entry's
/// resource. Composition applies the entry's expiry and the path-expiry drop. `$9` is the chain
/// of the resource a read is bound to, null on an address read.
pub(super) const OPERATOR_CANDIDATES: &str = "SELECT approval.subject, entry.resource_id,
        concat('account:', approval.chain_id, ':ens_v2_registry:',
            approval.authority_contract, ':', approval.owner) AS scope
    FROM bigname_phase.project_account_approval approval
    JOIN bigname_phase.project_ens_v2_entry_owner entry
      ON entry.chain_id = approval.chain_id AND entry.owner = approval.owner
     AND entry.registry = approval.authority_contract
    WHERE approval.authority_kind = 'ens_v2_registry'
      AND approval.relation_kind = 'operator' AND approval.approved
      AND approval.subject <> approval.owner
      AND entry.status = 'registered' AND entry.resource_id IS NOT NULL
      AND ($9::text IS NULL OR entry.chain_id = $9)
      AND EXISTS (SELECT 1 FROM bigname_phase.project_grant owner_grant
          WHERE owner_grant.chain_id = entry.chain_id
            AND owner_grant.resource_id = entry.resource_id
            AND owner_grant.subject = entry.owner AND owner_grant.scope = 'registry')";

/// The candidate keys of a token resource's root holders: the root grants of the registry root
/// `$8`, keyed under the token resource `$2` the read is bound to. Empty without a root.
pub(super) const ROOT_HOLDER_CANDIDATES: &str = "SELECT grant_row.subject,
        $2::uuid AS resource_id, grant_row.scope
    FROM bigname_phase.project_grant grant_row
    WHERE grant_row.resource_id = $8::uuid AND grant_row.scope = 'root'
      AND $2::uuid IS NOT NULL AND $2::uuid <> $8::uuid";

/// The registry root whose holders a read bound to `resource` lists with it: the root of an ENSv2
/// registry token resource, as the resource summary derives it. A root resource has none.
pub(super) async fn token_registry_root(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    resource: Uuid,
) -> Result<Option<Uuid>> {
    Ok(token_registry_roots(conn, publication, &[resource])
        .await?
        .remove(&resource))
}

async fn token_registry_roots(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    resources: &[Uuid],
) -> Result<BTreeMap<Uuid, Uuid>> {
    let facts = authority_facts(
        conn,
        &publication.chain_id,
        publication.block_number,
        resources,
    )
    .await?;
    Ok(facts
        .into_iter()
        .filter_map(|(resource, facts)| Some((resource, facts.root_resource_id?)))
        .filter(|(resource, root)| resource != root)
        .collect())
}

/// The F2a key states of `resources`, which carry the path-expiry drop.
async fn key_states(
    conn: &mut PgConnection,
    chain_id: &str,
    resources: &[Uuid],
) -> Result<BTreeMap<String, Maxima>> {
    let ids: Vec<String> = resources.iter().map(Uuid::to_string).collect();
    Ok(rows_for(
        conn,
        "/* storage:families.control.permissions.ens_v2_key_states */ SELECT to_jsonb(state)
         FROM bigname_phase.project_lifecycle_key_state state
         WHERE state.chain_id = $1 AND state.resource_id = ANY($2::uuid[])",
        chain_id,
        &ids,
    )
    .await?
    .iter()
    .filter_map(|row| Some((text(row, "resource_id")?, Maxima::from_row(row))))
    .collect())
}

/// The root-holder rows of the selected `root` keys whose resource is a token resource: each
/// root grant of the token's registry, served as a row of the token resource. Every holder of a
/// resource whose registration lapsed by path expiry has no row.
pub(super) async fn root_holder_rows(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    clock: &Clock,
    keys: &[Key],
) -> Result<Vec<ServedGrant>> {
    let wanted: Vec<&Key> = keys.iter().filter(|key| key.scope == ROOT_SCOPE).collect();
    if wanted.is_empty() {
        return Ok(Vec::new());
    }
    let resources: Vec<Uuid> = distinct(wanted.iter().map(|key| key.resource_id));
    let roots = token_registry_roots(conn, publication, &resources).await?;
    if roots.is_empty() {
        return Ok(Vec::new());
    }
    let tokens: Vec<Uuid> = roots.keys().copied().collect();
    let states = key_states(conn, &publication.chain_id, &tokens).await?;
    let root_ids: Vec<Uuid> = distinct(roots.values().copied());
    let subjects: Vec<String> = distinct(wanted.iter().map(|key| key.subject.clone()));
    let grants: Vec<GrantRow> = sqlx::query_scalar::<_, Value>(
        "/* storage:families.control.permissions.ens_v2_root_grants */ SELECT to_jsonb(grant_row)
         FROM bigname_phase.project_grant grant_row
         WHERE grant_row.chain_id = $1 AND grant_row.resource_id = ANY($2::uuid[])
           AND grant_row.scope = 'root' AND grant_row.subject = ANY($3::text[])",
    )
    .bind(&publication.chain_id)
    .bind(&root_ids)
    .bind(&subjects)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the registry root grants of token resources")?
    .iter()
    .filter_map(GrantRow::from_row)
    .collect();
    let mut rows = Vec::new();
    for (token, root) in &roots {
        let token = token.to_string();
        let root = root.to_string();
        let of_root: Vec<GrantRow> = grants
            .iter()
            .filter(|grant| grant.resource_id == root)
            .cloned()
            .collect();
        // The token's key state decides: a lapsed registration serves no rows at all.
        for mut grant in masked_grants(&of_root, None, states.get(&token), clock.timestamp_seconds)
        {
            grant.resource_id.clone_from(&token);
            rows.push(grant);
        }
    }
    Ok(rows)
}

/// The operator rows of the selected `account:<chain>:ens_v2_registry:…` keys.
pub(super) async fn operator_rows(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    clock: &Clock,
    keys: &[Key],
) -> Result<Vec<OperatorRow>> {
    let chain_id = publication.chain_id.as_str();
    let prefix = format!("account:{chain_id}:ens_v2_registry:");
    let wanted: Vec<&Key> = keys
        .iter()
        .filter(|key| key.scope.starts_with(&prefix))
        .collect();
    if wanted.is_empty() {
        return Ok(Vec::new());
    }
    let resources: Vec<Uuid> = distinct(wanted.iter().map(|key| key.resource_id));
    let subjects: Vec<String> = distinct(wanted.iter().map(|key| key.subject.clone()));
    // `expiry > clock` is the registry's own test: an entry is expired from the block whose
    // time reaches its expiry. An entry no log has stated an expiry for cannot be shown live.
    let found: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.control.permissions.ens_v2_operators */
         SELECT jsonb_build_object('resource_id', entry.resource_id, 'approval',
             to_jsonb(approval), 'owner_grant', to_jsonb(owner_grant))
         FROM bigname_phase.project_ens_v2_entry_owner entry
         JOIN bigname_phase.project_account_approval approval
           ON approval.chain_id = entry.chain_id
          AND approval.authority_kind = 'ens_v2_registry'
          AND approval.authority_contract = entry.registry AND approval.owner = entry.owner
          AND approval.relation_kind = 'operator' AND approval.approved
         JOIN bigname_phase.project_grant owner_grant
           ON owner_grant.chain_id = entry.chain_id
          AND owner_grant.resource_id = entry.resource_id
          AND owner_grant.subject = entry.owner AND owner_grant.scope = $5
         WHERE entry.chain_id = $1 AND entry.resource_id = ANY($2::uuid[])
           AND entry.status = 'registered' AND entry.expiry > $4::numeric
           AND approval.subject = ANY($3::text[]) AND approval.subject <> entry.owner",
    )
    .bind(chain_id)
    .bind(&resources)
    .bind(&subjects)
    .bind(clock.timestamp_seconds)
    .bind(TOKEN_SCOPE)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the ENSv2 registry operators of token resources")?;
    let states = key_states(conn, chain_id, &resources).await?;
    let mut rows = Vec::new();
    for row in &found {
        let (Some(resource), Some(approval), Some(owner_grant)) = (
            text(row, "resource_id"),
            row.get("approval"),
            row.get("owner_grant").and_then(GrantRow::from_row),
        ) else {
            continue;
        };
        let (Some(registry), Some(owner), Some(subject)) = (
            lower(approval, "authority_contract"),
            lower(approval, "owner"),
            lower(approval, "subject"),
        ) else {
            continue;
        };
        // The owner's grant as its own row serves it: nothing when the registration lapsed.
        let Some(served) = masked_grants(
            &[owner_grant],
            None,
            states.get(&resource),
            clock.timestamp_seconds,
        )
        .into_iter()
        .next() else {
            continue;
        };
        rows.push(OperatorRow {
            resource_id: resource,
            subject,
            scope: format!("{prefix}{registry}:{owner}"),
            scope_detail: json!({
                "chain_id": chain_id,
                "authority_kind": "ens_v2_registry",
                "authority_contract": registry,
                "owner": owner,
            }),
            effective_powers: served.effective_powers,
            grant_source: column(approval, "grant_source"),
            inheritance_path: column(approval, "inheritance_path"),
            transfer_behavior: column(approval, "transfer_behavior"),
        });
    }
    Ok(rows)
}

fn distinct<T: Ord>(items: impl Iterator<Item = T>) -> Vec<T> {
    items.collect::<BTreeSet<_>>().into_iter().collect()
}
