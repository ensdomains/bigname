//! The served child types and the children relation's read filter. The subnames page, the
//! registry labels page and the child counts read the stored child listing (`listing_page.rs`);
//! the relation here, read at the family marker's block, spells a page's healed names and
//! serves an address's surface-less registry children (`registry_children.rs`), with the page
//! semantics of crates/storage/src/children/page.rs and reads.rs: the optional prefix or
//! substring filter, the expiry fence with its null treatment, and a registry's labels as the
//! ENSv2 children its subregistry holds, optionally narrowed by the owner each serves
//! (`project_name_summary.owner`). Expiry filtering uses the canonical status stored at the
//! publication clock. A caller's fixed `evaluated_at` adds an expiry fence; neither uses the
//! database's transaction time.
//!
//! The registration and expiry times the fence uses, the released status it checks and the
//! owner the labels' owner filter reads are the child's name summary (`project_name_summary`),
//! which the family step writes from the child's composed `declared_summary`; a child with no
//! name surface is released when its node's registrar lease is ([`released_lease`]), and its
//! lease's holder, when the registrar retains one, is
//! [`token_holder`](child_flags::token_holder).
use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder};

use crate::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentPageFilter, RegistryLabelOwnerFilter, UnixSeconds,
    families::name::ensure_published,
};

use super::{
    children::{CHILD_DISPLAY_NAME, Parents, Relation, push_listed},
    listing_page::{count, page},
};
use child_flags::released_lease;

pub(super) mod child_flags;

/// One served child, the wire fields of the subnames route (docs/api-v1-routes.md, subnames).
/// The row carries no per-row provenance, chain positions or target blocks: those are not family
/// facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyChildRow {
    pub parent_logical_name_id: String,
    pub child_logical_name_id: String,
    pub namespace: String,
    pub canonical_display_name: String,
    pub namehash: String,
    pub labelhash: Option<String>,
    pub owner: Option<String>,
    pub registrant: Option<String>,
    /// The `authority` the child serves when it has no name row (`ens_v1` or `ens_v0`).
    pub registry_authority: Option<String>,
    /// The child's only surface is a shadow one at or below the clock (its label fails
    /// normalization, so no name row composes for it) and a NameWrapper or ENSv1 registrar event
    /// observed that name, so wrapper state or a lease can be projected for it without a name
    /// row. A shadow only another observer wrote, such as a resolver `NameChanged`, does not
    /// count ([`LIFECYCLE_SHADOW`](child_flags::LIFECYCLE_SHADOW)).
    pub lifecycle_shadow: bool,
    /// A lifecycle shadow whose registry owner is the NameWrapper that observed it: the wrapper
    /// holds the node for a token holder bigname does not record
    /// ([`WRAPPER_HELD`](child_flags::WRAPPER_HELD))
    /// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L579-L581 @ ens_v1@91c966f).
    pub wrapper_held: bool,
    /// The child has no name surface and its node's registrar lease has been released
    /// ([`released_lease`]).
    pub released_lease: bool,
    /// The holder of the node's registrar lease when the child has no name surface and the
    /// registrar retains one ([`token_holder`](child_flags::token_holder)).
    pub token_holder: Option<String>,
}

/// A registry's labels: the ENSv2 children whose registration `registry` emitted, narrowed by
/// the owner each serves when `owner` is given. `registration_limit` keeps at most that many of
/// the registry's registrations under the parent (`push_selected`); the label counts are stored
/// (`listing_page.rs`), so no reader sets it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegistryLabels<'a> {
    pub(crate) registry: &'a str,
    pub(crate) owner: Option<RegistryLabelOwnerFilter<'a>>,
    pub(crate) registration_limit: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyChildrenPage {
    /// Exact number of children the filters admit, before the cursor, or `cap + 1` above a cap.
    pub total_count: u64,
    pub rows: Vec<FamilyChildRow>,
    pub next_cursor: Option<ChildrenCurrentKeysetCursor>,
}

/// One page of `parent_logical_name_id`'s children from the child edge families, with the
/// children route's filters, order and cursor semantics.
pub async fn load_children_shadow_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<FamilyChildrenPage> {
    let mut conn = pool
        .acquire()
        .await
        .context("failed to acquire a connection")?;
    page(
        &mut conn,
        parent_logical_name_id,
        filter,
        None,
        cursor,
        page_size,
        None,
    )
    .await
}

/// The exact unfiltered child count of each parent, in `parent_logical_name_ids` order, the
/// count `load_children_current_summaries` serves; one count per parent, all in one read-only
/// snapshot.
pub async fn count_children_shadow(
    pool: &PgPool,
    parent_logical_name_ids: &[String],
) -> Result<Vec<(String, u64)>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open the count snapshot")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to fix the count snapshot")?;
    let mut counts = Vec::with_capacity(parent_logical_name_ids.len());
    for parent in parent_logical_name_ids {
        counts.push((parent.clone(), count(&mut transaction, parent, None).await?));
    }
    transaction
        .commit()
        .await
        .context("failed to close the count snapshot")?;
    Ok(counts)
}

/// Fails with [`FamilyPublicationUnavailable`] when a surface of one of `parent_logical_name_ids`
/// is on a chain whose family marker the publication fence would refuse (missing, not `live`,
/// another build's, or on a block a reorg orphaned): the composed name reads' check
/// (`ensure_published`), so a child read under the switch answers the stale 409 instead of an
/// empty list while a rebuild populates the families. Run it in the caller's snapshot, before
/// the reads.
pub(crate) async fn require_publication(
    conn: &mut PgConnection,
    parent_logical_name_ids: &[String],
) -> Result<()> {
    let chains: Vec<String> = sqlx::query_scalar(
        "/* storage:families.topology.children_publication */
         SELECT DISTINCT surface.chain_id FROM bigname_phase.name_surfaces surface
         WHERE surface.logical_name_id = ANY($1)
         ORDER BY surface.chain_id",
    )
    .bind(parent_logical_name_ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to read the chains of the child reads' parents")?;
    ensure_published(conn, &chains).await?;
    Ok(())
}

/// The selected children CTEs and the `children` relation (left open, closed by the caller)
/// after the read filter, the prefix, the expiry fence and, for a registry's labels, the registry
/// and owner filters, with each child's served fields and `sort_timestamp`; `children` narrows
/// the relation to those child nodes (`push_selected`).
pub(super) fn push_children<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    parents: Parents<'a>,
    filter: &ChildrenCurrentPageFilter<'a>,
    registry: Option<RegistryLabels<'a>>,
    children: Option<&'a [String]>,
) {
    push_listed(
        builder,
        parents,
        registry.map(|labels| labels.registry),
        children,
        registry.and_then(|labels| labels.registration_limit),
        Relation::Served,
    );
    if let Some(labels) = registry {
        builder.push(" AND selected.registry_address = ");
        builder.push_bind(labels.registry);
        // The owner a label serves is its composed name row's (apps/api/src/v2/subnames.rs,
        // `build_subname`); the ENSv2 arm's child row carries none, so a label without a summary
        // or a summary owner is ownerless and only the exclusion admits it.
        match labels.owner {
            Some(RegistryLabelOwnerFilter::Owner(owner)) => {
                builder.push(" AND summary.owner = ");
                builder.push_bind(owner);
            }
            Some(RegistryLabelOwnerFilter::ExcludeOwner(owner)) => {
                builder.push(" AND summary.owner IS DISTINCT FROM ");
                builder.push_bind(owner);
            }
            None => {}
        }
    }
    if let Some(q) = filter.q {
        builder.push(format!(" AND {CHILD_DISPLAY_NAME} LIKE "));
        builder.push_bind(q.like_pattern());
        builder.push(" ESCAPE '\\'");
    }
    if !filter.include_expired {
        // A child with no name summary or no registration remains listable. The stored scalar
        // uses the publication clock and the protocol's own equality boundary.
        builder.push(format!(
            " AND COALESCE(summary.registration_status, '') NOT IN ('expired','released') AND NOT {released_lease}",
            released_lease = released_lease(),
        ));
        // Preserve the caller's additional fixed-time fence; API snapshots pass the same
        // publication time, while direct storage callers may deliberately evaluate later.
        if let Some(evaluated_at) = filter.evaluated_at {
            builder.push(" AND COALESCE(summary.expires_at >= ");
            builder.push_bind(UnixSeconds::from(evaluated_at));
            builder.push(", TRUE)");
        }
    }
}

#[cfg(test)]
pub(crate) mod listing_support;
#[cfg(test)]
mod listing_tests;
#[cfg(test)]
mod narrowed_plan_tests;
#[cfg(test)]
mod performance_tests;
#[cfg(test)]
mod registry_labels_tests;
#[cfg(test)]
pub(crate) mod served_page;
