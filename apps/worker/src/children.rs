use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, ChildrenCurrentRow, clear_children_current, delete_children_current,
    load_canonical_declared_child_sources, load_raw_block, upsert_children_current_rows,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    types::time::{OffsetDateTime, UtcOffset},
};

const DECLARED_SURFACE_CLASS: &str = "declared";
const CHILDREN_CURRENT_DERIVATION_KIND: &str = "children_current_rebuild";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChildrenCurrentRebuildSummary {
    pub requested_parent_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

pub async fn rebuild_children_current(
    pool: &PgPool,
    parent_logical_name_id: Option<&str>,
) -> Result<ChildrenCurrentRebuildSummary> {
    match parent_logical_name_id {
        Some(parent_logical_name_id) => rebuild_one_parent(pool, parent_logical_name_id).await,
        None => rebuild_all_parents(pool).await,
    }
}

async fn rebuild_all_parents(pool: &PgPool) -> Result<ChildrenCurrentRebuildSummary> {
    let sources = load_canonical_declared_child_sources(pool, None).await?;
    let requested_parent_count = sources
        .iter()
        .map(|source| source.parent_logical_name_id.clone())
        .collect::<BTreeSet<_>>()
        .len();
    let rows = build_children_rows(pool, &sources).await?;
    let upserted_row_count = upsert_children_current_rows(pool, &rows).await?.len();
    let deleted_row_count = delete_stale_children_current_rows(pool, &rows).await?;

    Ok(ChildrenCurrentRebuildSummary {
        requested_parent_count,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_parent(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<ChildrenCurrentRebuildSummary> {
    let sources = load_canonical_declared_child_sources(pool, Some(parent_logical_name_id)).await?;
    let rows = build_children_rows(pool, &sources).await?;
    let upserted_row_count = upsert_children_current_rows(pool, &rows).await?.len();
    let deleted_row_count =
        delete_stale_children_current_rows_for_parent(pool, parent_logical_name_id, &rows).await?;

    Ok(ChildrenCurrentRebuildSummary {
        requested_parent_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn build_children_rows(
    pool: &PgPool,
    sources: &[bigname_storage::DeclaredChildEventSource],
) -> Result<Vec<ChildrenCurrentRow>> {
    let mut block_cache = BTreeMap::new();
    let mut rows = Vec::with_capacity(sources.len());

    for source in sources {
        rows.push(build_children_row(pool, source, &mut block_cache).await?);
    }

    Ok(rows)
}

async fn delete_stale_children_current_rows(
    pool: &PgPool,
    rows: &[ChildrenCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return clear_children_current(pool).await;
    }

    let parent_logical_name_ids = rows
        .iter()
        .map(|row| row.parent_logical_name_id.clone())
        .collect::<Vec<_>>();
    let child_logical_name_ids = rows
        .iter()
        .map(|row| row.child_logical_name_id.clone())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM children_current current
        WHERE current.surface_class = $1
          AND NOT EXISTS (
            SELECT 1
            FROM UNNEST($2::TEXT[], $3::TEXT[]) AS replacement(
                parent_logical_name_id,
                child_logical_name_id
            )
            WHERE replacement.parent_logical_name_id = current.parent_logical_name_id
              AND replacement.child_logical_name_id = current.child_logical_name_id
          )
        "#,
    )
    .bind(DECLARED_SURFACE_CLASS)
    .bind(&parent_logical_name_ids)
    .bind(&child_logical_name_ids)
    .execute(pool)
    .await
    .context("failed to delete stale children_current rows after rebuild")
    .map(|result| result.rows_affected())
}

async fn delete_stale_children_current_rows_for_parent(
    pool: &PgPool,
    parent_logical_name_id: &str,
    rows: &[ChildrenCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return delete_children_current(pool, parent_logical_name_id).await;
    }

    let child_logical_name_ids = rows
        .iter()
        .map(|row| row.child_logical_name_id.clone())
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM children_current current
        WHERE current.parent_logical_name_id = $1
          AND current.surface_class = $2
          AND NOT EXISTS (
            SELECT 1
            FROM UNNEST($3::TEXT[]) AS replacement(child_logical_name_id)
            WHERE replacement.child_logical_name_id = current.child_logical_name_id
          )
        "#,
    )
    .bind(parent_logical_name_id)
    .bind(DECLARED_SURFACE_CLASS)
    .bind(&child_logical_name_ids)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete stale children_current rows for parent_logical_name_id {parent_logical_name_id}"
        )
    })
    .map(|result| result.rows_affected())
}

async fn build_children_row(
    pool: &PgPool,
    source: &bigname_storage::DeclaredChildEventSource,
    block_cache: &mut BTreeMap<(String, String), bigname_storage::RawBlock>,
) -> Result<ChildrenCurrentRow> {
    let block = load_source_block(pool, source, block_cache).await?;

    Ok(ChildrenCurrentRow {
        parent_logical_name_id: source.parent_logical_name_id.clone(),
        child_logical_name_id: source.child_logical_name_id.clone(),
        surface_class: DECLARED_SURFACE_CLASS.to_owned(),
        namespace: source.namespace.clone(),
        canonical_display_name: source.canonical_display_name.clone(),
        normalized_name: source.normalized_name.clone(),
        namehash: source.namehash.clone(),
        provenance: build_provenance(source),
        chain_positions: build_chain_positions(source, &block),
        canonicality_summary: build_canonicality_summary(source, block.canonicality_state),
        manifest_version: source.manifest_version,
        last_recomputed_at: block.block_timestamp,
    })
}

async fn load_source_block(
    pool: &PgPool,
    source: &bigname_storage::DeclaredChildEventSource,
    block_cache: &mut BTreeMap<(String, String), bigname_storage::RawBlock>,
) -> Result<bigname_storage::RawBlock> {
    let cache_key = (source.chain_id.clone(), source.block_hash.clone());
    if let Some(block) = block_cache.get(&cache_key) {
        return Ok(block.clone());
    }

    let block = load_raw_block(pool, &source.chain_id, &source.block_hash)
        .await
        .with_context(|| {
            format!(
                "failed to load raw block for child source {} on chain {} block {}",
                source.event_identity, source.chain_id, source.block_hash
            )
        })?
        .with_context(|| {
            format!(
                "missing raw block for child source {} on chain {} block {}",
                source.event_identity, source.chain_id, source.block_hash
            )
        })?;

    block_cache.insert(cache_key, block.clone());
    Ok(block)
}

fn build_provenance(source: &bigname_storage::DeclaredChildEventSource) -> Value {
    json!({
        "normalized_event_ids": source.normalized_event_ids.clone(),
        "raw_fact_refs": source.raw_fact_refs.clone(),
        "manifest_versions": source.manifest_versions.clone(),
        "execution_trace_id": Value::Null,
        "derivation_kind": CHILDREN_CURRENT_DERIVATION_KIND,
    })
}

fn build_chain_positions(
    source: &bigname_storage::DeclaredChildEventSource,
    block: &bigname_storage::RawBlock,
) -> Value {
    json!({
        chain_slot(&source.chain_id): {
            "chain_id": source.chain_id,
            "block_number": source.block_number,
            "block_hash": source.block_hash,
            "timestamp": format_timestamp(block.block_timestamp),
        }
    })
}

fn build_canonicality_summary(
    source: &bigname_storage::DeclaredChildEventSource,
    state: CanonicalityState,
) -> Value {
    json!({
        "status": state.as_str(),
        "chains": {
            source.chain_id.clone(): state.as_str(),
        }
    })
}

fn chain_slot(chain_id: &str) -> &str {
    match chain_id {
        "ethereum-mainnet" => "ethereum",
        "base-mainnet" => "base",
        _ => chain_id,
    }
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

#[cfg(test)]
mod tests;
