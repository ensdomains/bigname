use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::redo::{InterpretRedoFence, ensure_interpret_redo_fence};
use super::{
    EventHistoryReadFilter, HistoryCursor, HistoryEvent, HistoryOrder, HistoryPage,
    HistorySummaryMode,
    decoders::decode_history_event,
    duplicates::push_product_history_duplicate_filter,
    filters::{push_history_block_window, push_selector_filter, push_string_filter},
    keyset::{
        HistoryKeyset, history_cursor_from_row, load_history_keyset, push_history_cursor_after,
        push_history_cursor_block_bound, push_history_cursor_cte,
    },
    registration_identity::{push_product_registration_id, push_registration_filter},
    selectors::HistorySelector,
    source::{push_history_canonicality_filter, push_history_source_for_filter},
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
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin normalized-event history page transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to configure normalized-event history page transaction")?;

    if let Some(interpret_redo_fence) = interpret_redo_fence {
        ensure_interpret_redo_fence(&mut transaction, interpret_redo_fence)
            .await
            .context("normalized-event history page refused during Interpret redo")?;
    }

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

    // Unprepared, so PostgreSQL plans every page with its bound values. A prepared statement
    // may switch to a generic plan after five runs, and without the values PostgreSQL guesses
    // that a block range between the cursor and the window bound holds 0.5% of the rows, which
    // can make sorting every matching row look cheaper than reading the order index.
    let rows = builder
        .build()
        .persistent(false)
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
        .fetch_all(pool)
        .await
        .context("failed to fetch normalized-event history rows")?;

    rows.into_iter().map(decode_history_event).collect()
}

pub(super) fn push_history_select<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
) {
    builder.push(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_identity,
            ne.namespace,
            ne.logical_name_id,
            ne.resource_id,
        "#,
    );
    push_product_registration_id(builder, canonical_only);
    builder.push(
        r#" AS registration_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.transaction_hash,
            ne.log_index,
            ne.raw_fact_ref,
            ne.derivation_kind,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.before_state,
            ne.after_state,
        "#,
    );
    if include_candidates {
        builder.push(
            r#"
            ne.migration_correlation_ids,
            ne.consumer_visibility,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'migration_correlation_ids',
                            ARRAY[association.migration_correlation_id],
                            'correlation_kind', association.correlation_kind,
                            'consumer_visibility', association.consumer_visibility
                        )
                        ORDER BY association.migration_correlation_id,
                                 association.correlation_kind,
                                 association.consumer_visibility
                    )
                    FROM migration_event_associations AS association
                    WHERE association.event_identity = ne.event_identity
                ),
                '[]'::jsonb
            ) AS migration_associations,
            "#,
        );
    } else {
        builder.push(
            r#"
            ARRAY[]::text[] AS migration_correlation_ids,
            'activated'::text AS consumer_visibility,
            '[]'::jsonb AS migration_associations,
            "#,
        );
    }
    builder.push(
        r#"
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'provenance') = 'object'
                        THEN ne.after_state -> 'provenance'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'provenance') = 'object'
                        THEN ne.before_state -> 'provenance'
                END,
                '{}'::jsonb
            ) AS provenance,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'coverage') = 'object'
                        THEN ne.after_state -> 'coverage'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'coverage') = 'object'
                        THEN ne.before_state -> 'coverage'
                END,
                '{}'::jsonb
            ) AS coverage
        "#,
    );
    push_history_source_for_filter(
        builder,
        filter,
        canonical_only,
        include_cursor_row,
        include_candidates,
    );
}

pub(super) fn push_history_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
) {
    for selector in &filter.selectors {
        builder.push(" AND ");
        push_selector_filter(builder, selector);
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
