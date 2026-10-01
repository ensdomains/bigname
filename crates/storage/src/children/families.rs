//! The declared child reads: the subnames page, the registry labels page and count, and the per-parent child
//! counts, read from the family child relation (`families::topology`) with the children's name
//! summaries, in the served readers' shapes. Every read runs in one read-only repeatable-read
//! snapshot that first checks the parents' chains have a servable family marker
//! ([`require_publication`]): without one the read fails with `FamilyPublicationUnavailable`,
//! which the API answers with the stale 409.
//!
//! A family child row carries the wire fields only: the provenance, chain positions,
//! canonicality summary, manifest version and legacy recompute time are not
//! family facts, so the rows and summaries built here leave them empty (no route reads them).
use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction, types::time::OffsetDateTime};

use crate::families::{
    name::{ensure_published, read_snapshot},
    topology::{
        FamilyChildRow, RegistryLabels, children_page_on, count_children_of_parents_on,
        count_children_on, require_publication,
    },
};

use super::{
    DECLARED_SURFACE_CLASS,
    types::{
        ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentPageFilter,
        ChildrenCurrentRow, ChildrenCurrentSummary, RegistryChildrenPage, RegistryLabelOwnerFilter,
    },
};

/// The snapshot a child read runs in, once its parents' publication is checked.
async fn snapshot(
    pool: &PgPool,
    parent_logical_name_ids: &[String],
) -> Result<Transaction<'static, Postgres>> {
    let mut transaction = read_snapshot(pool).await?;
    require_publication(&mut transaction, parent_logical_name_ids).await?;
    Ok(transaction)
}

async fn close(transaction: Transaction<'static, Postgres>) -> Result<()> {
    transaction
        .commit()
        .await
        .context("failed to close the children snapshot")
}

pub(super) async fn page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<ChildrenCurrentPage> {
    let mut transaction = snapshot(pool, &[parent_logical_name_id.to_owned()]).await?;
    let page = children_page_on(
        &mut transaction,
        parent_logical_name_id,
        filter,
        None,
        cursor,
        page_size,
    )
    .await?;
    let child_count = if filter.admits_every_child() {
        page.total_count
    } else {
        count_children_on(&mut transaction, parent_logical_name_id, None).await?
    };
    close(transaction).await?;
    Ok(ChildrenCurrentPage {
        total_count: page.total_count,
        rows: page.rows.into_iter().map(row).collect(),
        next_cursor: page.next_cursor,
        summary: summary(parent_logical_name_id, child_count)?,
    })
}

pub(super) async fn registry_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    registry_address: &str,
    owner: Option<RegistryLabelOwnerFilter<'_>>,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<RegistryChildrenPage> {
    let registry = registry_address.to_ascii_lowercase();
    let mut transaction = snapshot(pool, &[parent_logical_name_id.to_owned()]).await?;
    let page = children_page_on(
        &mut transaction,
        parent_logical_name_id,
        &ChildrenCurrentPageFilter::default(),
        Some(RegistryLabels {
            registry: &registry,
            owner,
        }),
        cursor,
        page_size,
    )
    .await?;
    close(transaction).await?;
    Ok(RegistryChildrenPage {
        rows: page.rows.into_iter().map(row).collect(),
        next_cursor: page.next_cursor,
        label_count: i64::try_from(page.total_count).context("registry label count overflow")?,
    })
}

/// The registry's representative pointing name: the earliest name whose current ENSv2
/// subregistry pointer targets it, the overview's `name` rule. With no reverse index on
/// `project_parent_subregistry` this reads the chain's pointer rows.
const SERVING_PARENT: &str = "/* storage:children.registry_serving_parent */
    SELECT pointer.logical_name_id
    FROM bigname_phase.project_parent_subregistry pointer
    JOIN bigname_phase.name_surfaces surface
      ON surface.logical_name_id = pointer.logical_name_id
     AND surface.visibility_state = 'active'
    WHERE pointer.chain_id = $1 AND pointer.subregistry_address = $2
    ORDER BY pointer.block_number, pointer.transaction_index NULLS LAST,
             pointer.log_index NULLS LAST, pointer.event_identity
    LIMIT 1";

/// The labels-route total of `registry_address` at the current family publication of
/// `chain_id`: its labels under its representative pointing name ([`SERVING_PARENT`]) in that
/// same snapshot, or zero when no name points at it. The name is resolved here, not passed in,
/// so the count never anchors to a name read from an earlier publication.
pub(super) async fn registry_count_current(
    pool: &PgPool,
    chain_id: &str,
    registry_address: &str,
) -> Result<i64> {
    let registry = registry_address.to_ascii_lowercase();
    let mut transaction = read_snapshot(pool).await?;
    ensure_published(&mut transaction, &[chain_id.to_owned()]).await?;
    let parent: Option<String> = sqlx::query_scalar(SERVING_PARENT)
        .bind(chain_id)
        .bind(&registry)
        .fetch_optional(&mut *transaction)
        .await
        .context("failed to read the name a registry serves")?;
    let count = match parent {
        Some(parent) => count_children_on(&mut transaction, &parent, Some(&registry)).await?,
        None => 0,
    };
    close(transaction).await?;
    i64::try_from(count).context("registry label count overflow")
}

pub(super) async fn summaries(
    pool: &PgPool,
    parent_logical_name_ids: &[String],
) -> Result<Vec<ChildrenCurrentSummary>> {
    let mut transaction = snapshot(pool, parent_logical_name_ids).await?;
    // One statement for every parent, as the served summaries aggregate them in one query.
    let counts = count_children_of_parents_on(&mut transaction, parent_logical_name_ids).await?;
    close(transaction).await?;
    parent_logical_name_ids
        .iter()
        .map(|parent| summary(parent, counts.get(parent).copied().unwrap_or(0)))
        .collect()
}

fn summary(parent_logical_name_id: &str, child_count: u64) -> Result<ChildrenCurrentSummary> {
    Ok(ChildrenCurrentSummary {
        parent_logical_name_id: parent_logical_name_id.to_owned(),
        child_count: i64::try_from(child_count).context("child count overflow")?,
        provenance_inputs: Vec::new(),
        chain_positions: Vec::new(),
        canonicality_summaries: Vec::new(),
        last_recomputed_at: None,
    })
}

fn row(child: FamilyChildRow) -> ChildrenCurrentRow {
    ChildrenCurrentRow {
        parent_logical_name_id: child.parent_logical_name_id,
        child_logical_name_id: child.child_logical_name_id,
        surface_class: DECLARED_SURFACE_CLASS.to_owned(),
        namespace: child.namespace,
        normalized_name: child.canonical_display_name.clone(),
        canonical_display_name: child.canonical_display_name,
        namehash: child.namehash,
        labelhash: child.labelhash,
        owner: child.owner,
        registrant: child.registrant,
        registry_authority: child.registry_authority,
        shadow_surface: child.shadow_surface,
        provenance: json!({}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 0,
        last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
    }
}

#[cfg(test)]
#[path = "serving_parent_tests.rs"]
mod serving_parent_tests;
