//! The child listing cases' shared steps (`families_child_listing*.rs`): ENSv1 edges and label
//! preimages written directly, the stored rows and counts of a parent, today's read-time answer
//! (`load_children_shadow_page`) and a block's journal.
use anyhow::{Result, ensure};
use bigname_storage::{ChildrenCurrentPageFilter, families::topology::load_children_shadow_page};
use serde_json::json;

use super::{CHAIN, Fixture};

pub const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
pub const OWNER: &str = "0x00000000000000000000000000000000000000a1";
pub const ZERO: &str = "0x0000000000000000000000000000000000000000";
pub const CHAIN_KIND: &str = "PreimageObserved";

pub fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

pub fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// An ENSv1 `NewOwner` of `child` under `parent` with label hash `label`, at `block`.
pub async fn edge(
    fixture: &Fixture,
    (block, log): (i64, i64),
    parent: u64,
    child: u64,
    label: &str,
    owner: &str,
) -> Result<()> {
    fixture
        .write(
            block,
            log,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"source_event": "NewOwner", "node": node(parent), "child_node": node(child),
                   "labelhash": label, "owner": owner}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

/// The after state of a label-only preimage observation of `label`: no name, the label hash
/// carried directly, as the adapter writes one for a label no name of its log represents
/// (`crates/adapters/src/schema_v2/identity.rs`, `materialize_from`).
pub fn label_state(label: &str) -> serde_json::Value {
    json!({"source_event": "LabelRegistered",
           "raw_label_hex": alloy_primitives::hex::encode(label),
           "decoded_label": label,
           "labelhash": format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()))})
}

/// A chain-observed label preimage.
pub async fn preimage(fixture: &Fixture, labelhash: &str, label: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority)
         VALUES ($1, convert_to($2, 'UTF8'), $2, $3, TRUE, $4, 0)",
    )
    .bind(labelhash)
    .bind(label)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN_KIND)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// The stored rows of `parent` as (child, display name, display key), in child order.
pub async fn listed(fixture: &Fixture, parent: &str) -> Result<Vec<(String, String, String)>> {
    Ok(sqlx::query_as(
        "SELECT child_logical_name_id, display_name, display_key FROM project_child_listing
         WHERE chain_id = $1 AND parent_logical_name_id = $2 ORDER BY child_logical_name_id",
    )
    .bind(CHAIN)
    .bind(parent)
    .fetch_all(&fixture.pool)
    .await?)
}

/// The stored counts of `parent` as (registry, count).
pub async fn counted(fixture: &Fixture, parent: &str) -> Result<Vec<(String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT registry_address, child_count FROM project_child_listing_count
         WHERE chain_id = $1 AND parent_logical_name_id = $2 ORDER BY registry_address",
    )
    .bind(CHAIN)
    .bind(parent)
    .fetch_all(&fixture.pool)
    .await?)
}

/// Today's read-time answer for `parent`: every child the served relation lists, as (child,
/// served name), in child order, and its exact total.
pub async fn served(fixture: &Fixture, parent: &str) -> Result<(Vec<(String, String)>, u64)> {
    let page = load_children_shadow_page(
        &fixture.pool,
        parent,
        &ChildrenCurrentPageFilter::default(),
        None,
        10_000,
    )
    .await?;
    let mut rows: Vec<(String, String)> = page
        .rows
        .into_iter()
        .map(|row| (row.child_logical_name_id, row.canonical_display_name))
        .collect();
    rows.sort();
    Ok((rows, page.total_count))
}

/// The stored rows of `parent` equal today's read-time rows, names included, and its stored
/// count equals the served total.
pub async fn assert_served(fixture: &Fixture, parent: &str) -> Result<()> {
    let stored: Vec<(String, String)> = listed(fixture, parent)
        .await?
        .into_iter()
        .map(|(child, display, _)| (child, display))
        .collect();
    let (rows, total) = served(fixture, parent).await?;
    ensure!(
        stored == rows,
        "{parent} stores {stored:?}, serves {rows:?}"
    );
    let count = counted(fixture, parent)
        .await?
        .into_iter()
        .find(|(registry, _)| registry.is_empty())
        .map_or(0, |(_, count)| count);
    ensure!(
        u64::try_from(count)? == total,
        "{parent} counts {count}, serves a total of {total}"
    );
    Ok(())
}

/// `kept` and `now`, two `Fixture::exact` snapshots, agree on every family table but the child
/// listing and the address listing, whose healed names an undo keeps: it derives them again at
/// the restored block.
pub fn ensure_equal_but_listing(kept: &[(String, String)], now: &[(String, String)]) -> Result<()> {
    for ((table, was), (_, is)) in kept.iter().zip(now) {
        if table == "project_child_listing" || table == "project_address_listing" {
            continue;
        }
        ensure!(was == is, "the undo left {table} as {is}, not {was}");
    }
    Ok(())
}

/// The undo rows block `block` journalled for `family`: (rows, rows with a before-image).
pub async fn journalled(fixture: &Fixture, block: i64, family: &str) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT count(*), count(before_image) FROM project_family_undo
         WHERE chain_id = $1 AND block_number = $2 AND family = $3",
    )
    .bind(CHAIN)
    .bind(block)
    .bind(family)
    .fetch_one(&fixture.pool)
    .await?)
}
