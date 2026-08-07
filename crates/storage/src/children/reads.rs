use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};

use crate::projection_helpers::{
    checked_page_limit_i64, checked_page_size_usize, split_keyset_page, take_json_array,
};

use super::{
    DECLARED_SURFACE_CLASS, DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS,
    DEFAULT_CHILDREN_CURRENT_READ_FILTER,
    types::{
        ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentRow,
        ChildrenCurrentSummary,
    },
};

/// A registry edge proves a child node and its labelhash but not the label itself, so Project
/// writes the name columns null until a preimage arrives. The read path names such a child by the
/// documented `[<labelhash-without-0x>].<parent-name>` placeholder rather than returning null into
/// a mandatory field. The parent name comes from a subquery so the expression holds on the
/// audit path too, which omits the identity joins.
const CHILD_DISPLAY_NAME_EXPR: &str = r#"COALESCE(
    cc.decoded_name,
    encode(cc.raw_name, 'escape'),
    '[' || substring(lower(cc.labelhash) FROM 3) || '].' || (
        SELECT parent_surface.raw_name
        FROM bigname_phase.name_surfaces parent_surface
        WHERE parent_surface.logical_name_id = cc.parent_logical_name_id
    )
)"#;

fn child_select() -> String {
    format!(
        r#"
    SELECT cc.parent_logical_name_id, cc.child_logical_name_id, cc.surface_class,
           cc.namespace,
           {CHILD_DISPLAY_NAME_EXPR} AS canonical_display_name,
           lower({CHILD_DISPLAY_NAME_EXPR}) AS normalized_name,
           cc.namehash, cc.labelhash, cc.owner, cc.registrant, cc.provenance,
           cc.chain_positions, cc.canonicality_summary, cc.manifest_version,
           cc.last_recomputed_at
    FROM bigname_phase.children_current cc
"#
    )
}

pub async fn load_children_current(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<Vec<ChildrenCurrentRow>> {
    load_children_current_internal(pool, parent_logical_name_id, false).await
}

pub async fn load_children_current_including_noncanonical(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<Vec<ChildrenCurrentRow>> {
    load_children_current_internal(pool, parent_logical_name_id, true).await
}

pub async fn load_children_current_page(
    pool: &PgPool,
    parent_logical_name_id: &str,
    cursor: Option<&ChildrenCurrentKeysetCursor>,
    page_size: u64,
) -> Result<ChildrenCurrentPage> {
    let limit = checked_page_limit_i64(
        page_size,
        "children_current page_size must be positive",
        "children_current page_size is too large",
    )?;
    let page_size = checked_page_size_usize(
        page_size,
        "children_current page_size must be positive",
        "children_current page_size does not fit in usize",
    )?;
    let mut builder = QueryBuilder::<Postgres>::new(child_select());
    builder.push(DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS);
    builder.push(" WHERE cc.parent_logical_name_id = ");
    builder.push_bind(parent_logical_name_id);
    builder.push(" AND cc.surface_class = ");
    builder.push_bind(DECLARED_SURFACE_CLASS);
    builder.push(DEFAULT_CHILDREN_CURRENT_READ_FILTER);
    if let Some(cursor) = cursor {
        builder.push(format!(
            " AND ({CHILD_DISPLAY_NAME_EXPR}, cc.child_logical_name_id) > ("
        ));
        builder.push_bind(&cursor.canonical_display_name);
        builder.push(", ");
        builder.push_bind(&cursor.child_logical_name_id);
        builder.push(")");
    }
    builder.push(format!(
        " ORDER BY {CHILD_DISPLAY_NAME_EXPR}, cc.child_logical_name_id LIMIT "
    ));
    builder.push_bind(limit);
    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to load phase children_current page")?
        .into_iter()
        .map(decode_children_current_row)
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size, |row| {
        ChildrenCurrentKeysetCursor::from(row)
    });
    let summary = load_children_current_summary(pool, parent_logical_name_id).await?;
    Ok(ChildrenCurrentPage {
        rows,
        next_cursor,
        summary,
    })
}

pub async fn load_children_current_summaries(
    pool: &PgPool,
    parent_logical_name_ids: &[String],
) -> Result<Vec<ChildrenCurrentSummary>> {
    if parent_logical_name_ids.is_empty() {
        return Ok(Vec::new());
    }
    // The summary annotates the page, so it has to admit exactly the rows the page admits —
    // including the projection-target lineage fence that fails closed on an orphaned target whose
    // identity anchors are still canonical.
    let rows = sqlx::query(&format!(
        r#"
        WITH requested AS (
            SELECT parent_logical_name_id, ordinal
            FROM unnest($1::text[]) WITH ORDINALITY
              AS input(parent_logical_name_id, ordinal)
        ),
        readable_children AS (
            SELECT cc.*
            FROM bigname_phase.children_current cc
            {DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS}
            WHERE cc.surface_class = $2
            {DEFAULT_CHILDREN_CURRENT_READ_FILTER}
        )
        SELECT requested.parent_logical_name_id,
               COUNT(cc.child_logical_name_id)::bigint AS child_count,
               COALESCE(jsonb_agg(cc.provenance ORDER BY cc.raw_name, cc.child_logical_name_id)
                        FILTER (WHERE cc.child_logical_name_id IS NOT NULL), '[]'::jsonb)
                    AS provenance_inputs,
               COALESCE(jsonb_agg(cc.chain_positions ORDER BY cc.raw_name, cc.child_logical_name_id)
                        FILTER (WHERE cc.child_logical_name_id IS NOT NULL), '[]'::jsonb)
                    AS chain_positions,
               COALESCE(jsonb_agg(cc.canonicality_summary ORDER BY cc.raw_name, cc.child_logical_name_id)
                        FILTER (WHERE cc.child_logical_name_id IS NOT NULL), '[]'::jsonb)
                    AS canonicality_summaries,
               MAX(cc.last_recomputed_at) AS last_recomputed_at
        FROM requested
        LEFT JOIN readable_children cc
          ON cc.parent_logical_name_id = requested.parent_logical_name_id
        GROUP BY requested.ordinal, requested.parent_logical_name_id
        ORDER BY requested.ordinal
        "#
    ))
    .bind(parent_logical_name_ids)
    .bind(DECLARED_SURFACE_CLASS)
    .fetch_all(pool)
    .await
    .context("failed to load phase children_current summaries")?;
    rows.into_iter()
        .map(decode_children_current_summary)
        .collect()
}

async fn load_children_current_summary(
    pool: &PgPool,
    parent_logical_name_id: &str,
) -> Result<ChildrenCurrentSummary> {
    load_children_current_summaries(pool, &[parent_logical_name_id.to_owned()])
        .await?
        .into_iter()
        .next()
        .context("phase children summary must preserve its requested key")
}

async fn load_children_current_internal(
    pool: &PgPool,
    parent_logical_name_id: &str,
    include_noncanonical: bool,
) -> Result<Vec<ChildrenCurrentRow>> {
    let mut query = child_select();
    if !include_noncanonical {
        query.push_str(DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS);
    }
    query.push_str(" WHERE cc.parent_logical_name_id = $1 AND cc.surface_class = $2");
    if !include_noncanonical {
        query.push_str(DEFAULT_CHILDREN_CURRENT_READ_FILTER);
    }
    query.push_str(&format!(
        " ORDER BY {CHILD_DISPLAY_NAME_EXPR}, cc.child_logical_name_id"
    ));
    let rows = sqlx::query(&query)
        .bind(parent_logical_name_id)
        .bind(DECLARED_SURFACE_CLASS)
        .fetch_all(pool)
        .await
        .context("failed to load phase children_current rows")?;
    rows.into_iter().map(decode_children_current_row).collect()
}

fn decode_children_current_row(row: PgRow) -> Result<ChildrenCurrentRow> {
    let surface_class: String = crate::sql_row::get(&row, "surface_class")?;
    if surface_class != DECLARED_SURFACE_CLASS {
        bail!("children_current row has unsupported surface_class {surface_class}");
    }
    Ok(ChildrenCurrentRow {
        parent_logical_name_id: crate::sql_row::get(&row, "parent_logical_name_id")?,
        child_logical_name_id: crate::sql_row::get(&row, "child_logical_name_id")?,
        surface_class,
        namespace: crate::sql_row::get(&row, "namespace")?,
        canonical_display_name: crate::sql_row::get(&row, "canonical_display_name")?,
        normalized_name: crate::sql_row::get(&row, "normalized_name")?,
        namehash: crate::sql_row::get(&row, "namehash")?,
        labelhash: crate::sql_row::get(&row, "labelhash")?,
        owner: crate::sql_row::get(&row, "owner")?,
        registrant: crate::sql_row::get(&row, "registrant")?,
        provenance: crate::sql_row::get(&row, "provenance")?,
        chain_positions: crate::sql_row::get(&row, "chain_positions")?,
        canonicality_summary: crate::sql_row::get(&row, "canonicality_summary")?,
        manifest_version: crate::sql_row::get(&row, "manifest_version")?,
        last_recomputed_at: crate::sql_row::get(&row, "last_recomputed_at")?,
    })
}

fn decode_children_current_summary(row: PgRow) -> Result<ChildrenCurrentSummary> {
    Ok(ChildrenCurrentSummary {
        parent_logical_name_id: row.try_get("parent_logical_name_id")?,
        child_count: row.try_get("child_count")?,
        provenance_inputs: take_json_array(row.try_get("provenance_inputs")?, || {
            "children summary provenance_inputs must be a JSON array".to_owned()
        })?,
        chain_positions: take_json_array(row.try_get("chain_positions")?, || {
            "children summary chain_positions must be a JSON array".to_owned()
        })?,
        canonicality_summaries: take_json_array(row.try_get("canonicality_summaries")?, || {
            "children summary canonicality_summaries must be a JSON array".to_owned()
        })?,
        last_recomputed_at: row.try_get("last_recomputed_at")?,
    })
}
