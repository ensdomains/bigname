//! The anchors of `GET /v1/events?address=` from the stored address listing
//! (`project_address_listing`): the names and resources whose relation to the address has a
//! cited event at or below the read's bound on the row's chain, and whose name was attached to
//! the row's resource at or below that bound.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use uuid::Uuid;

use crate::{AddressNameRelation, ReadDb};

/// The relations a request asks about, as the three flags: every relation when none is named.
pub(super) fn wanted(relations: Option<&[AddressNameRelation]>) -> [bool; 3] {
    let relations = relations.filter(|relations| !relations.is_empty());
    let has = |relation| relations.is_none_or(|relations| relations.contains(&relation));
    [
        has(AddressNameRelation::TokenHolder),
        has(AddressNameRelation::EffectiveController),
        has(AddressNameRelation::RoleHolder),
    ]
}

/// The stored anchors of `address` at `published`.
pub async fn load_stored_address_anchors(
    db: impl Into<ReadDb<'_>>,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    published: &BTreeMap<String, i64>,
) -> Result<BTreeSet<(String, Uuid)>> {
    let [token_holder, effective_controller, role_holder] = wanted(relations);
    let (chains, blocks): (Vec<String>, Vec<i64>) = published
        .iter()
        .map(|(chain, block)| (chain.clone(), *block))
        .unzip();
    let mut conn = db.into().acquire().await?;
    let rows: Vec<(String, Uuid)> = sqlx::query_as(
        "/* storage:address_names.listing_anchors */
         SELECT l.logical_name_id, l.resource_id
         FROM bigname_phase.project_address_listing l
         JOIN unnest($6::text[], $7::bigint[]) AS bound(chain_id, block_number)
           ON bound.chain_id = l.chain_id
         WHERE l.address = lower($1) AND l.listed
           AND ($2::text IS NULL OR l.namespace = $2)
           AND (($3 AND l.token_holder_block <= bound.block_number)
                OR ($4 AND l.effective_controller_block <= bound.block_number)
                OR ($5 AND l.role_holder_block <= bound.block_number))
           AND EXISTS (
               SELECT 1 FROM bigname_phase.surface_bindings attached
               WHERE attached.logical_name_id = l.logical_name_id
                 AND attached.resource_id = l.resource_id
                 AND attached.chain_id = l.chain_id
                 AND attached.block_number <= bound.block_number
                 AND attached.canonicality_state IN (
                     'canonical'::bigname_phase.canonicality_state,
                     'safe'::bigname_phase.canonicality_state,
                     'finalized'::bigname_phase.canonicality_state))",
    )
    .bind(address)
    .bind(namespace)
    .bind(token_holder)
    .bind(effective_controller)
    .bind(role_holder)
    .bind(&chains)
    .bind(&blocks)
    .fetch_all(&mut *conn)
    .await
    .with_context(|| format!("failed to load the stored anchors of {address}"))?;
    Ok(rows.into_iter().collect())
}

/// The anchors today's composed read gives `address` at `published`
/// (`load_address_names_current_at_bound`), for tests to compare with the stored ones.
#[cfg(any(test, feature = "test-support"))]
pub async fn load_composed_address_anchors(
    pool: &sqlx::PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    published: &BTreeMap<String, i64>,
) -> Result<BTreeSet<(String, Uuid)>> {
    let rows = super::super::load_address_names_current_at_bound(
        pool, address, namespace, relations, false, published,
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.logical_name_id, row.resource_id))
        .collect())
}
