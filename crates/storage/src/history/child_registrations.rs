//! Name history with the requested name's direct child registrations
//! (`include=child_registrations`, docs/api-v2-routes.md).
//!
//! The collection is the set union, by event identity, of the name arm (the rows the ordinary
//! name history selects) and the child arm (`child_arm.rs`). An event in both is a child row.
//! Page rows, counts, and cursor validation all use that one rule: the name arm drops every row
//! the child arm holds, so the arms are disjoint and each is paged by its own index.

use anyhow::{Context, Result, bail};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder, postgres::PgRow};
use uuid::Uuid;

use super::{
    EventHistoryReadFilter, HistoryCursor, HistoryEvent, HistoryPageOptions, HistoryScope,
    HistorySummary, HistorySummaryMode, InvalidHistoryCursor,
    child_arm::{
        ChildArm, ChildArmBound, push_child_arm_order, push_child_arm_source,
        push_not_child_arm_row,
    },
    columns::push_history_columns,
    decoders::decode_history_event,
    duplicates::push_product_history_duplicate_filter,
    keyset::{push_history_cursor_after, push_history_cursor_cte},
    paging::{push_history_filters, push_history_order, push_history_order_terms},
    redo::{InterpretRedoFence, ensure_interpret_redo_fence},
    selectors::{HistorySelector, name_history_selector},
    source::push_history_source_for_filter,
};
use crate::projection_helpers::{
    checked_page_limit_i64_from_usize, checked_page_size_usize, split_keyset_page,
};

/// How a row of name history relates to the requested name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistorySubject {
    /// A row of the requested name itself, as name history selects it without the option.
    Name,
    /// A registration of one of the requested name's direct children.
    Child,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameHistoryRow {
    pub event: HistoryEvent,
    pub subject: HistorySubject,
    /// The child's name as its surface spells it, read in the page transaction; `None` on a
    /// `Name` row.
    pub child_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameHistoryPage {
    pub rows: Vec<NameHistoryRow>,
    pub next_cursor: Option<HistoryCursor>,
    pub summary: Option<HistorySummary>,
    pub interpret_redo_fence: Option<InterpretRedoFence>,
}

/// One keyset page of a name's history merged with its direct child registrations.
/// `summary_mode` supports counts only.
#[allow(clippy::too_many_arguments)]
pub async fn load_name_history_page_with_child_registrations(
    pool: &PgPool,
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
    options: &HistoryPageOptions,
    interpret_redo_fence: Option<&InterpretRedoFence>,
) -> Result<NameHistoryPage> {
    #[cfg(any(test, feature = "test-support"))]
    super::history_anchor_read_test_hooks::run_if(pool, interpret_redo_fence.is_some()).await?;
    let filter = name_history_filter(logical_name_id, resource_ids, scope, options);
    let page_size = checked_page_size_usize(
        page_size,
        "history page_size must be positive",
        "history page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        page_size,
        "history page_size is too large",
        "history page_size exceeds SQL limit",
    )?;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin name history page transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to configure name history page transaction")?;
    if let Some(fence) = interpret_redo_fence {
        ensure_interpret_redo_fence(&mut transaction, fence)
            .await
            .context("name history page refused during Interpret redo")?;
    }
    if let Some(cursor) = cursor {
        ensure_cursor_in_collection(&mut transaction, logical_name_id, &filter, cursor).await?;
    }
    let arm = ChildArm::resolve(&mut transaction, logical_name_id, &filter).await?;
    let summary = match summary_mode {
        HistorySummaryMode::None => None,
        HistorySummaryMode::Count => {
            Some(count(&mut transaction, &filter, arm.as_ref(), None).await?)
        }
        HistorySummaryMode::CappedCount(cap) => {
            Some(count(&mut transaction, &filter, arm.as_ref(), Some(cap)).await?)
        }
        HistorySummaryMode::Full => bail!("name history with child registrations counts only"),
    };
    let bound = child_arm_bound(&mut transaction, arm.as_ref(), cursor, filter.order).await?;

    let rows = if has_empty_selector(&filter) && arm.is_none() {
        Vec::new()
    } else {
        let mut builder = QueryBuilder::<Postgres>::new("");
        push_page_query(
            &mut builder,
            &filter,
            arm.as_ref(),
            cursor,
            &bound,
            page_limit,
        );
        builder
            .build()
            .fetch_all(&mut *transaction)
            .await
            .context("failed to fetch name history page with child registrations")?
            .into_iter()
            .map(decode_row)
            .collect::<Result<Vec<_>>>()?
    };
    let (rows, next_cursor) =
        split_keyset_page(rows, page_size, |row: &NameHistoryRow| HistoryCursor {
            normalized_event_id: row.event.normalized_event_id,
            event_identity: row.event.event_identity.clone(),
        });
    transaction
        .commit()
        .await
        .context("failed to commit name history page transaction")?;
    Ok(NameHistoryPage {
        rows,
        next_cursor,
        summary,
        interpret_redo_fence: interpret_redo_fence.cloned(),
    })
}

fn name_history_filter(
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    options: &HistoryPageOptions,
) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        selectors: vec![name_history_selector(logical_name_id, resource_ids, scope)],
        ..EventHistoryReadFilter::default()
    }
    .with_page_options(options)
}

async fn child_arm_bound(
    connection: &mut PgConnection,
    arm: Option<&ChildArm>,
    cursor: Option<&HistoryCursor>,
    order: super::HistoryOrder,
) -> Result<ChildArmBound> {
    Ok(match (arm, cursor) {
        (Some(arm), Some(cursor)) => {
            let position = ChildArmBound::load_cursor(connection, arm, &cursor.event_identity)
                .await?
                .ok_or(InvalidHistoryCursor)?;
            ChildArmBound::after(&position, order)
        }
        _ => ChildArmBound::Unbounded,
    })
}

/// `EXPLAIN (ANALYZE, FORMAT JSON)` of the page query `load_name_history_page_with_child_registrations`
/// runs for the same arguments, so tests can check which index each arm reads and how many rows.
#[cfg(any(test, feature = "test-support"))]
pub async fn explain_name_history_page_with_child_registrations_for_test(
    pool: &PgPool,
    logical_name_id: &str,
    resource_ids: &[Uuid],
    scope: HistoryScope,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    options: &HistoryPageOptions,
) -> Result<serde_json::Value> {
    let filter = name_history_filter(logical_name_id, resource_ids, scope, options);
    let page_limit = checked_page_limit_i64_from_usize(
        checked_page_size_usize(
            page_size,
            "page_size must be positive",
            "page_size too large",
        )?,
        "history page_size is too large",
        "history page_size exceeds SQL limit",
    )?;
    let mut transaction = pool.begin().await?;
    let arm = ChildArm::resolve(&mut transaction, logical_name_id, &filter).await?;
    let bound = child_arm_bound(&mut transaction, arm.as_ref(), cursor, filter.order).await?;
    let mut builder = QueryBuilder::<Postgres>::new("EXPLAIN (ANALYZE, FORMAT JSON) ");
    push_page_query(
        &mut builder,
        &filter,
        arm.as_ref(),
        cursor,
        &bound,
        page_limit,
    );
    let plan = builder
        .build_query_scalar::<serde_json::Value>()
        .fetch_one(&mut *transaction)
        .await?;
    transaction.rollback().await?;
    Ok(plan)
}

/// `[WITH cursor] SELECT … FROM ((name arm LIMIT n) UNION ALL (child arm LIMIT n)) ne ORDER BY
/// … LIMIT n`. Each arm is ordered and limited on its own index before the merge.
fn push_page_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    arm: Option<&'a ChildArm>,
    cursor: Option<&'a HistoryCursor>,
    bound: &'a ChildArmBound,
    page_limit: i64,
) {
    if let Some(cursor) = cursor {
        push_history_cursor_cte(builder, cursor);
    }
    builder.push(" SELECT * FROM (");
    let name_arm = !has_empty_selector(filter);
    if name_arm {
        builder.push("(");
        push_history_columns(builder, true, false);
        builder.push(", 'name'::text AS history_subject, NULL::text AS child_name");
        push_history_source_for_filter(builder, filter, true, cursor.is_some(), false);
        push_name_arm_filters(builder, filter, arm);
        if cursor.is_some() {
            builder.push(" AND ");
            push_history_cursor_after(builder, filter.order);
        }
        push_history_order(builder, filter.order);
        builder.push(" LIMIT ");
        builder.push_bind(page_limit);
        builder.push(")");
    }
    if let Some(arm) = arm {
        if name_arm {
            builder.push(" UNION ALL ");
        }
        builder.push("(");
        push_history_columns(builder, true, false);
        builder.push(", 'child'::text AS history_subject, child_surface.raw_name AS child_name");
        push_child_arm_source(builder, arm, Some(&filter.event_kinds), true);
        bound.push(builder);
        push_child_arm_order(builder, filter.order);
        builder.push(" LIMIT ");
        builder.push_bind(page_limit);
        builder.push(")");
    }
    builder.push(") ne ORDER BY ");
    push_history_order_terms(builder, filter.order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);
}

fn push_name_arm_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    arm: Option<&'a ChildArm>,
) {
    push_history_filters(builder, filter, true);
    push_product_history_duplicate_filter(builder, filter, true);
    if let Some(arm) = arm {
        push_not_child_arm_row(builder, arm);
    }
}

fn has_empty_selector(filter: &EventHistoryReadFilter) -> bool {
    filter
        .selectors
        .iter()
        .any(|selector| matches!(selector, HistorySelector::None))
}

fn decode_row(row: PgRow) -> Result<NameHistoryRow> {
    let subject: String = crate::sql_row::get(&row, "history_subject")?;
    let child_name: Option<String> = crate::sql_row::get(&row, "child_name")?;
    let subject = match subject.as_str() {
        "name" => HistorySubject::Name,
        "child" => HistorySubject::Child,
        other => bail!("unexpected name history subject {other}"),
    };
    Ok(NameHistoryRow {
        event: decode_history_event(row)?,
        subject,
        child_name,
    })
}

/// The combined collection's size: name-arm rows the child arm does not hold, plus child-arm
/// rows. With a cap, each arm stops after `cap + 1` rows, so the sum is exact at or below the cap
/// and above it otherwise.
async fn count(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    arm: Option<&ChildArm>,
    cap: Option<u64>,
) -> Result<HistorySummary> {
    let limit = cap
        .map(|cap| i64::try_from(cap.saturating_add(1)))
        .transpose()
        .context("history total_count cap exceeds SQL limit")?;
    let mut builder = QueryBuilder::<Postgres>::new("SELECT COUNT(*)::BIGINT FROM (");
    let mut arms = 0;
    if !has_empty_selector(filter) {
        builder.push("(SELECT 1");
        push_history_source_for_filter(&mut builder, filter, true, false, false);
        push_name_arm_filters(&mut builder, filter, arm);
        if let Some(limit) = limit {
            builder.push(" LIMIT ");
            builder.push_bind(limit);
        }
        builder.push(")");
        arms += 1;
    }
    if let Some(arm) = arm {
        if arms > 0 {
            builder.push(" UNION ALL ");
        }
        builder.push("(SELECT 1");
        push_child_arm_source(&mut builder, arm, Some(&filter.event_kinds), true);
        if let Some(limit) = limit {
            builder.push(" LIMIT ");
            builder.push_bind(limit);
        }
        builder.push(")");
        arms += 1;
    }
    if arms == 0 {
        builder.push("SELECT 1 WHERE FALSE");
    }
    builder.push(") counted");
    let total_count = builder
        .build_query_scalar::<i64>()
        .fetch_one(&mut *connection)
        .await
        .context("failed to count name history with child registrations")?;
    Ok(HistorySummary {
        total_count: u64::try_from(total_count).context("negative name history total_count")?,
        normalized_event_ids: Vec::new(),
        raw_fact_refs: Vec::new(),
        manifest_versions: Vec::new(),
        chain_position_samples: Vec::new(),
        last_updated: None,
    })
}

/// The cursor's event must still be in the collection: in the name arm or the child arm. As on
/// ordinary history, only an explicit type set binds the anchor to event kinds.
async fn ensure_cursor_in_collection(
    connection: &mut PgConnection,
    parent: &str,
    filter: &EventHistoryReadFilter,
    cursor: &HistoryCursor,
) -> Result<()> {
    let mut cursor_filter = filter.clone();
    if !cursor_filter.bind_cursor_anchor_to_event_kinds {
        cursor_filter.event_kinds.clear();
    }
    let arm = ChildArm::resolve(&mut *connection, parent, &cursor_filter).await?;
    let mut builder = QueryBuilder::<Postgres>::new("SELECT EXISTS (SELECT 1");
    push_history_source_for_filter(&mut builder, &cursor_filter, true, false, false);
    push_history_filters(&mut builder, &cursor_filter, true);
    push_product_history_duplicate_filter(&mut builder, &cursor_filter, true);
    builder.push(" AND ne.event_identity = ");
    builder.push_bind(&cursor.event_identity);
    builder.push(")");
    if let Some(arm) = arm.as_ref() {
        builder.push(" OR EXISTS (SELECT 1");
        push_child_arm_source(&mut builder, arm, None, true);
        builder.push(" AND ne.event_identity = ");
        builder.push_bind(&cursor.event_identity);
        builder.push(")");
    }
    let exists = builder
        .build_query_scalar::<bool>()
        .fetch_one(&mut *connection)
        .await
        .context("failed to validate name history cursor")?;
    if exists {
        Ok(())
    } else {
        Err(InvalidHistoryCursor.into())
    }
}
