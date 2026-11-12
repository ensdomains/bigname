//! `GET /v1/resolvers?role_holder=`: the resolvers on which an address holds ENSv2 Enhanced
//! Access Control (EAC) roles, one row per resolver, grouped from the holder's F8 grants
//! (`project_grant`).
//!
//! A row counts the holder's live EAC grants on one resolver, root resource included, and
//! unions their powers. An EAC grant is told apart from the derived ENSv1 and Basenames
//! resolver authority that shares scope kind `resolver` by its grant source: only an ENSv2
//! resolver's `EACRolesChanged` log writes `source_event = 'EACRolesChanged'`
//! (crates/adapters/src/schema_v2/protocol/permissions.rs, `v2_states`), and Project stores that
//! source as given (crates/project/src/families/permissions.rs, `apply_grant`). The stored row is
//! the served one: an EAC grant keeps the latest bitmap, `revoked` is set exactly when it is
//! empty, and none of the read-time masks of `load_shadow_permissions_on` reaches an EAC
//! resolver resource, which has no wrapper row and no lifecycle key state. So grouping in SQL is
//! the whole read: a resolver is listed when its `/roles` would list the holder with a power,
//! wherever `/roles` supports the resolver's enumeration.
//!
//! The page is in scope order (`resolver:<chain>:<address>` as bytes), so by chain slug, then by
//! lower-cased resolver address, over the chains the request spans. Its range is the cursor's
//! scope, or the start of the one chain or of every chain, up to the end of that chain or of
//! every chain.
//! `project_grant_resolver_role_holder_idx` carries the predicate below as its own, so its index
//! condition holds the holder and the range under both plan modes.
//!
//! `GET /v1/registries?role_holder=` is read here too, by [`load_registries_by_role_holder`]:
//! the registries on which an address holds ENSv2 registry root roles.
use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use serde_json::Value;

use crate::families::name::ensure_published;

/// One resolver on which the holder holds live EAC grants.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoleHolderResolver {
    /// The resolver's chain slug.
    pub chain_id: String,
    /// The lower-cased resolver address.
    pub resolver_address: String,
    /// The holder's live EAC grants on the resolver: one per EAC resource.
    pub grant_count: u64,
    /// The union of those grants' powers, sorted.
    pub powers: Vec<String>,
}

/// The statement of [`load_resolvers_by_role_holder`]. The grouped scope is the page key. The
/// predicate is `project_grant_resolver_role_holder_idx`'s, as literals.
pub(crate) const STATEMENT: &str = r#"/* storage:families.topology.resolvers_by_role_holder */
SELECT grant_row.scope COLLATE "C" AS scope, count(*) AS grant_count,
       jsonb_agg(grant_row.effective_powers) AS powers
FROM bigname_phase.project_grant grant_row
WHERE grant_row.subject COLLATE "C" = $1 COLLATE "C"
  AND grant_row.scope COLLATE "C" > $2 COLLATE "C"
  AND grant_row.scope COLLATE "C" < $3 COLLATE "C"
  AND grant_row.scope_kind = 'resolver'
  AND NOT grant_row.revoked
  AND grant_row.grant_source ->> 'source_event' = 'EACRolesChanged'
  AND grant_row.chain_id = ANY($5::text[])
GROUP BY 1
ORDER BY 1
LIMIT $4"#;

/// The scope range of a page: after `after` (a chain slug and resolver address), within the
/// one chain of `chains` when there is one, else within every chain. Both ends are exclusive;
/// neither is a scope, since a scope ends in an address and `;` follows `:` in byte order.
fn page_range(chains: &[String], after: Option<(&str, &str)>) -> (String, String) {
    let (start, end) = match chains {
        [chain] => (format!("resolver:{chain}:"), format!("resolver:{chain};")),
        _ => ("resolver:".to_owned(), "resolver;".to_owned()),
    };
    let start = match after {
        Some((chain, address)) => {
            let position = format!("resolver:{chain}:{}", address.to_ascii_lowercase());
            position.max(start)
        }
        None => start,
    };
    (start, end)
}

/// The resolvers on `chains` after `after` on which `holder` holds live EAC grants, at most
/// `limit`, in page order. Every chain's publication is checked on the same snapshot, and a
/// chain whose marker is not servable fails the read with `FamilyPublicationUnavailable`.
pub async fn load_resolvers_by_role_holder(
    db: impl Into<crate::ReadDb<'_>>,
    holder: &str,
    chains: &[String],
    after: Option<(&str, &str)>,
    limit: i64,
) -> Result<Vec<RoleHolderResolver>> {
    if limit < 1 {
        bail!("the role-holder page limit must be positive");
    }
    if chains.is_empty() {
        return Ok(Vec::new());
    }
    let holder = holder.to_ascii_lowercase();
    let (start, end) = page_range(chains, after);
    let mut snapshot = db.into().snapshot().await?;
    let rows: Vec<(String, i64, Value)> = sqlx::query_as(STATEMENT)
        .bind(&holder)
        .bind(&start)
        .bind(&end)
        .bind(limit)
        .bind(chains)
        .fetch_all(&mut *snapshot)
        .await
        .with_context(|| format!("failed to load the resolvers of role holder {holder}"))?;
    let page = rows
        .into_iter()
        .map(|(scope, count, powers)| row(&scope, count, &powers))
        .collect::<Result<Vec<_>>>()?;
    ensure_published(&mut snapshot, chains).await?;
    snapshot.close().await?;
    Ok(page)
}

fn row(scope: &str, count: i64, powers: &Value) -> Result<RoleHolderResolver> {
    let Some((chain_id, resolver_address)) = scope
        .strip_prefix("resolver:")
        .and_then(|rest| rest.rsplit_once(':'))
    else {
        bail!("grant scope {scope} is not a resolver scope");
    };
    Ok(RoleHolderResolver {
        chain_id: chain_id.to_owned(),
        resolver_address: resolver_address.to_owned(),
        grant_count: u64::try_from(count)?,
        powers: powers_union(powers, scope)?,
    })
}

/// The sorted union of a group's `jsonb_agg` of power arrays.
fn powers_union(powers: &Value, group: &str) -> Result<Vec<String>> {
    let mut union = BTreeSet::new();
    for grant in powers.as_array().into_iter().flatten() {
        for power in grant.as_array().into_iter().flatten() {
            let Some(power) = power.as_str() else {
                bail!("grant scope {group} holds a power that is not a string");
            };
            union.insert(power.to_owned());
        }
    }
    Ok(union.into_iter().collect())
}

/// One registry on which the holder holds a live ENSv2 root grant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoleHolderRegistry {
    /// The registry's chain slug.
    pub chain_id: String,
    /// The lower-cased registry address.
    pub registry_address: String,
    /// The holder's live root grants at the address: one per registry contract instance.
    pub grant_count: u64,
    /// The union of those grants' powers, sorted.
    pub powers: Vec<String>,
}

/// The statement of [`load_registries_by_role_holder`], for one chain. Only an ENSv2
/// registry's `EACRolesChanged` on its root resource writes scope `root`
/// (crates/adapters/src/schema_v2/protocol/permissions.rs, `v2_states`, and
/// crates/project/src/families/keys.rs, `grant_scope`), so `scope = 'root' AND NOT revoked` is
/// the live root EAC grants. That is `project_grant_root_role_holder_idx`'s predicate, as
/// literals, and the holder, the chain and the address bound are its index condition, so the
/// page reads the holder's live root grants on the chain from the bound, in page order, under
/// both plan modes. The page key and its bound compare as bytes, whatever the database
/// collation.
pub(crate) const REGISTRY_STATEMENT: &str = r#"/* storage:families.topology.registries_by_role_holder */
SELECT lower(grant_row.scope_detail ->> 'registry_address') COLLATE "C" AS registry_address,
       count(*) AS grant_count,
       jsonb_agg(grant_row.effective_powers) AS powers
FROM bigname_phase.project_grant grant_row
WHERE grant_row.subject COLLATE "C" = $1 COLLATE "C"
  AND grant_row.chain_id COLLATE "C" = $2 COLLATE "C"
  AND lower(grant_row.scope_detail ->> 'registry_address') COLLATE "C" > $3 COLLATE "C"
  AND grant_row.scope = 'root'
  AND NOT grant_row.revoked
GROUP BY 1
ORDER BY 1
LIMIT $4"#;

/// The registries on `chains` after `after` (a chain slug and registry address) on which
/// `holder` holds live ENSv2 root grants, at most `limit`, in page order: by chain slug, then by
/// lower-cased registry address, both as bytes. It reads one chain at a time, from the cursor's
/// chain on, until the page is full. A row groups the root grants at one address, whatever
/// letter case stored it. Every chain's publication is checked on the same snapshot, and a chain
/// whose marker is not servable fails the read with `FamilyPublicationUnavailable`.
///
/// The stored row is the served one, as `GET /v1/permissions?registry=` composes a root
/// resource: no wrapper row or lifecycle key state keys it, and an empty bitmap is `revoked`.
/// Two compositions are not applied: the root rows a WrapperRegistry derives for its parent
/// name's owner and that owner's operators are not stored, and the stored root grants it
/// replaces for those accounts are listed with their own powers. A grant on a registry instance
/// that no longer holds its address is listed too.
pub async fn load_registries_by_role_holder(
    db: impl Into<crate::ReadDb<'_>>,
    holder: &str,
    chains: &[String],
    after: Option<(&str, &str)>,
    limit: i64,
) -> Result<Vec<RoleHolderRegistry>> {
    if limit < 1 {
        bail!("the role-holder page limit must be positive");
    }
    if chains.is_empty() {
        return Ok(Vec::new());
    }
    let holder = holder.to_ascii_lowercase();
    let mut ordered: Vec<&str> = chains.iter().map(String::as_str).collect();
    ordered.sort_unstable();
    ordered.dedup();
    let mut snapshot = db.into().snapshot().await?;
    let mut page = Vec::new();
    for chain in ordered {
        let bound = match after {
            Some((after_chain, _)) if chain < after_chain => continue,
            Some((after_chain, address)) if chain == after_chain => address.to_ascii_lowercase(),
            _ => String::new(),
        };
        let remaining = limit - i64::try_from(page.len())?;
        if remaining < 1 {
            break;
        }
        // Planned for this chain's bound each time, never as a cached generic plan.
        let rows: Vec<(String, i64, Value)> = sqlx::query_as(REGISTRY_STATEMENT)
            .bind(&holder)
            .bind(chain)
            .bind(&bound)
            .bind(remaining)
            .persistent(false)
            .fetch_all(&mut *snapshot)
            .await
            .with_context(|| format!("failed to load the registries of role holder {holder}"))?;
        for (registry_address, count, powers) in rows {
            page.push(RoleHolderRegistry {
                chain_id: chain.to_owned(),
                powers: powers_union(&powers, &format!("root:{chain}:{registry_address}"))?,
                registry_address,
                grant_count: u64::try_from(count)?,
            });
        }
    }
    ensure_published(&mut snapshot, chains).await?;
    snapshot.close().await?;
    Ok(page)
}

#[cfg(test)]
#[path = "role_holder_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "role_holder_registry_tests.rs"]
mod registry_tests;
