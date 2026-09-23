use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::redo::{InterpretRedoFence, ensure_interpret_redo_fence};
use super::{
    EventHistoryReadFilter, HistoryCursor, HistoryEvent, HistoryOrder, HistoryPage,
    HistorySummaryMode,
    columns::push_history_select,
    decoders::decode_history_event,
    duplicates::push_product_history_duplicate_filter,
    filters::{push_history_block_window, push_selector_filter, push_string_filter},
    keyset::{
        HistoryKeyset, history_cursor_from_row, load_history_keyset, push_history_cursor_after,
        push_history_cursor_block_bound, push_history_cursor_cte,
    },
    registration_identity::push_registration_filter,
    selectors::HistorySelector,
    source::push_history_canonicality_filter,
    summary::load_history_summary,
};
use crate::projection_helpers::{
    checked_page_limit_i64_from_usize, checked_page_size_usize, split_keyset_page,
};

pub(super) async fn load_history(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history_internal(
        pool,
        EventHistoryReadFilter {
            selectors: vec![selector],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        false,
    )
    .await
}

pub(super) async fn load_history_head(
    pool: &PgPool,
    selector: HistorySelector,
    canonical_only: bool,
) -> Result<Option<HistoryEvent>> {
    let mut rows = load_history_internal(
        pool,
        EventHistoryReadFilter {
            selectors: vec![selector],
            ..EventHistoryReadFilter::default()
        },
        canonical_only,
        true,
    )
    .await?;
    Ok(rows.drain(..).next())
}

pub(super) async fn load_event_history_rows(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
) -> Result<Vec<HistoryEvent>> {
    load_history_internal(pool, filter, canonical_only, false).await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn load_history_page(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: HistorySummaryMode,
    include_candidates: bool,
    interpret_redo_fence: Option<&InterpretRedoFence>,
) -> Result<HistoryPage> {
    let mut transaction = begin_history_snapshot(pool, "page").await?;

    if let Some(interpret_redo_fence) = interpret_redo_fence {
        ensure_interpret_redo_fence(&mut transaction, interpret_redo_fence)
            .await
            .context("normalized-event history page refused during Interpret redo")?;
    }
    let filter = filter.with_attributed_records(&mut transaction).await?;

    let keyset = match cursor {
        Some(cursor) => Some(
            load_history_keyset(
                &mut transaction,
                &filter,
                canonical_only,
                cursor,
                include_candidates,
            )
            .await?,
        ),
        None => None,
    };

    let summary =
        load_history_summary(&mut transaction, &filter, canonical_only, summary_mode).await?;

    if filter
        .selectors
        .iter()
        .any(|selector| matches!(selector, HistorySelector::None))
    {
        transaction
            .commit()
            .await
            .context("failed to commit normalized-event history page transaction")?;
        return Ok(HistoryPage {
            rows: Vec::new(),
            next_cursor: None,
            summary,
            interpret_redo_fence: interpret_redo_fence.cloned(),
        });
    }

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

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_page_query(
        &mut builder,
        &filter,
        canonical_only,
        keyset.as_ref(),
        include_candidates,
        page_limit,
    );

    // An unanchored page is sent unprepared, so PostgreSQL plans it with its bound block
    // values. A prepared statement may switch to a generic plan after five runs, which guesses
    // 0.5% of the rows for a range between two unknown block bounds (an oldest-first cursor, or
    // a newest-first window with a lower bound) and 33% for each lone `<=` bound. On the test
    // fixture the generic plan still reads the order index from the cursor block
    // (`generic_plans_still_read_the_order_index_from_the_cursor_block`); planning with the
    // real values keeps that choice from resting on the guesses. Only an unanchored page is
    // driven by the order index, so only it pays the planning, about 16 to 19 ms per page on
    // the test machine; anchored pages keep their cached plans.
    // sqlx looks its statement cache up by SQL text before it consults `persistent`, so a
    // persistent execution of byte-identical text on the same connection would make this one
    // reuse that prepared statement.
    let rows = builder
        .build()
        .persistent(!order_index_drives_page(&filter))
        .fetch_all(&mut *transaction)
        .await
        .context("failed to fetch normalized-event history page")?;
    let rows = rows
        .into_iter()
        .map(decode_history_event)
        .collect::<Result<Vec<_>>>()?;
    let (rows, next_cursor) = split_keyset_page(rows, page_size, history_cursor_from_row);

    transaction
        .commit()
        .await
        .context("failed to commit normalized-event history page transaction")?;

    Ok(HistoryPage {
        rows,
        next_cursor,
        summary,
        interpret_redo_fence: interpret_redo_fence.cloned(),
    })
}

/// Whether nothing anchors the page to a name, registration, address, resolver or contract,
/// so the chain-position order index, not an anchor's index, drives the read.
pub(super) fn order_index_drives_page(filter: &EventHistoryReadFilter) -> bool {
    filter.selectors.is_empty()
        && filter.registration_id.is_none()
        && filter.resolver.is_none()
        && filter.contract_address.is_none()
}

/// One keyset page of history rows: the rows after the keyset's cursor in `filter.order`, at
/// most `page_limit` of them.
pub(super) fn push_history_page_query<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    keyset: Option<&HistoryKeyset<'a>>,
    include_candidates: bool,
    page_limit: i64,
) {
    if let Some(keyset) = keyset {
        push_history_cursor_cte(builder, keyset.cursor);
    }
    push_history_select(
        builder,
        filter,
        canonical_only,
        keyset.is_some(),
        include_candidates,
    );
    push_history_filters(builder, filter, canonical_only);
    if !include_candidates {
        push_product_history_duplicate_filter(builder, filter, canonical_only);
    }

    if let Some(keyset) = keyset {
        builder.push(" AND ");
        push_history_cursor_after(builder, filter.order);
        push_history_cursor_block_bound(builder, filter, keyset);
    }

    push_history_order(builder, filter.order);
    builder.push(" LIMIT ");
    builder.push_bind(page_limit);
}

async fn load_history_internal(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    canonical_only: bool,
    head_only: bool,
) -> Result<Vec<HistoryEvent>> {
    if filter
        .selectors
        .iter()
        .any(|selector| matches!(selector, HistorySelector::None))
    {
        return Ok(Vec::new());
    }
    // The attribution reads and the row read share one snapshot, so a publication between them
    // cannot filter rows against attribution from another database state.
    let mut transaction = begin_history_snapshot(pool, "row").await?;
    let filter = filter.with_attributed_records(&mut transaction).await?;

    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_select(&mut builder, &filter, canonical_only, false, false);
    push_history_filters(&mut builder, &filter, canonical_only);
    push_product_history_duplicate_filter(&mut builder, &filter, canonical_only);
    push_history_order(&mut builder, filter.order);

    if head_only {
        builder.push(" LIMIT 1");
    }

    let rows = builder
        .build()
        .fetch_all(&mut *transaction)
        .await
        .context("failed to fetch normalized-event history rows")?;
    transaction
        .commit()
        .await
        .context("failed to commit normalized-event history row transaction")?;

    rows.into_iter().map(decode_history_event).collect()
}

/// A read-only transaction with one snapshot for every statement of a history read: the
/// attribution reads, the cursor and summary reads, and the row read.
async fn begin_history_snapshot(
    pool: &PgPool,
    read: &str,
) -> Result<sqlx::Transaction<'static, Postgres>> {
    let mut transaction = pool
        .begin()
        .await
        .with_context(|| format!("failed to begin normalized-event history {read} transaction"))?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .with_context(|| {
            format!("failed to configure normalized-event history {read} transaction")
        })?;
    Ok(transaction)
}

pub(super) fn push_history_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
) {
    for selector in &filter.selectors {
        builder.push(" AND ");
        push_selector_filter(builder, selector, &filter.attributed_records);
    }

    if let Some(namespace) = filter.namespace.as_ref() {
        builder.push(" AND ne.namespace = ");
        builder.push_bind(namespace);
    }

    if let Some(contract_address) = filter.contract_address.as_ref() {
        builder.push(" AND lower(ne.raw_fact_ref ->> 'emitting_address') = ");
        builder.push_bind(contract_address);
    }

    push_registration_filter(builder, filter, canonical_only);

    if !filter.event_kinds.is_empty() {
        builder.push(" AND ");
        push_string_filter(builder, "ne.event_kind", &filter.event_kinds);
    }

    if let Some(from_block) = filter.from_block {
        builder.push(" AND ne.block_number >= ");
        builder.push_bind(from_block);
    }

    if let Some(to_block) = filter.to_block {
        builder.push(" AND ne.block_number <= ");
        builder.push_bind(to_block);
    }

    if let Some(window) = filter.block_window.as_ref() {
        push_history_block_window(builder, window);
    }

    if let Some(resolver) = filter.resolver.as_ref() {
        builder.push(" AND ne.chain_id = ");
        builder.push_bind(&resolver.chain_id);
        builder.push(" AND (lower(ne.raw_fact_ref ->> 'emitting_address') = ");
        builder.push_bind(&resolver.address);
        builder.push(
            " OR (ne.event_kind = 'ResolverChanged' AND (lower(ne.after_state ->> 'resolver') = ",
        );
        builder.push_bind(&resolver.address);
        builder.push(" OR lower(ne.before_state ->> 'resolver') = ");
        builder.push_bind(&resolver.address);
        builder.push(")))");
    }

    push_history_canonicality_filter(builder, canonical_only);
}

pub(super) async fn load_history_events_by_ids(
    pool: &PgPool,
    ids: &[i64],
) -> Result<Vec<HistoryEvent>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let filter = EventHistoryReadFilter::default();
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_select(&mut builder, &filter, true, false, false);
    builder.push(" AND ne.normalized_event_id = ANY(");
    builder.push_bind(ids);
    builder.push("::bigint[])");
    push_history_canonicality_filter(&mut builder, true);
    push_history_order(&mut builder, HistoryOrder::Desc);

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to fetch normalized events by id")?;
    rows.into_iter().map(decode_history_event).collect()
}

pub(super) fn push_history_order(builder: &mut QueryBuilder<'_, Postgres>, order: HistoryOrder) {
    builder.push(" ORDER BY ");
    push_history_order_terms(builder, order);
}

/// `Asc` is the exact reverse of the canonical `Desc` sort, including null
/// placement, so the keyset predicate can be derived by swapping the compared
/// rows. For a read bound to one chain, `normalized_events_chain_block_number_desc_idx`
/// on `(chain_id, block_number DESC NULLS LAST)` serves the leading key of both:
/// `Desc` reads it forward and `Asc` reads it backward (`ASC NULLS FIRST`). The
/// remaining keys only break ties within a block. The ascending
/// `normalized_events_chain_block_number_idx` read backward gives `DESC NULLS FIRST`,
/// which does not match, so it cannot serve these pages.
pub(super) fn push_history_order_terms(
    builder: &mut QueryBuilder<'_, Postgres>,
    order: HistoryOrder,
) {
    match order {
        HistoryOrder::Desc => builder.push(
            r#"
            ne.block_number DESC NULLS LAST,
            ne.chain_id ASC NULLS LAST,
            ne.block_hash DESC NULLS LAST,
            ne.transaction_hash DESC NULLS LAST,
            ne.log_index DESC NULLS LAST,
            ne.event_identity DESC
        "#,
        ),
        HistoryOrder::Asc => builder.push(
            r#"
            ne.block_number ASC NULLS FIRST,
            ne.chain_id DESC NULLS FIRST,
            ne.block_hash ASC NULLS FIRST,
            ne.transaction_hash ASC NULLS FIRST,
            ne.log_index ASC NULLS FIRST,
            ne.event_identity ASC
        "#,
        ),
    };
}

#[cfg(test)]
#[path = "paging_tests.rs"]
mod tests;
