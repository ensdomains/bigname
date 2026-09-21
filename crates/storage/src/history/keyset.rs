//! Keyset continuation for history pages: the cursor row, its existence check, and the
//! predicates that start a page strictly after it in the requested order.

use anyhow::{Context, Result};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter, HistoryCursor, HistoryEvent, HistoryOrder, InvalidHistoryCursor,
    duplicates::push_product_history_duplicate_filter, paging::push_history_filters,
    source::push_history_source_for_filter,
};

/// A validated continuation point: the cursor and the block number of its row.
pub(super) struct HistoryKeyset<'a> {
    pub(super) cursor: &'a HistoryCursor,
    pub(super) block_number: Option<i64>,
}

/// Check that the cursor row is still one of the rows this filter reads, and return its block
/// number for [`push_history_cursor_block_bound`].
pub(super) async fn load_history_keyset<'a>(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
    cursor: &'a HistoryCursor,
    include_candidates: bool,
) -> Result<HistoryKeyset<'a>> {
    let mut builder = QueryBuilder::<Postgres>::new(" SELECT ne.block_number ");
    let mut cursor_filter = filter.clone();
    if !cursor_filter.bind_cursor_anchor_to_event_kinds {
        cursor_filter.event_kinds.clear();
    }
    push_history_source_for_filter(
        &mut builder,
        &cursor_filter,
        canonical_only,
        false,
        include_candidates,
    );
    push_history_filters(&mut builder, &cursor_filter, canonical_only);
    if !include_candidates {
        push_product_history_duplicate_filter(&mut builder, &cursor_filter, canonical_only);
    }
    builder.push(" AND ne.event_identity = ");
    builder.push_bind(&cursor.event_identity);
    builder.push(" LIMIT 1");

    let block_number = builder
        .build_query_scalar::<Option<i64>>()
        .fetch_optional(&mut *connection)
        .await
        .context("failed to validate normalized-event history cursor")?
        .ok_or(InvalidHistoryCursor)?;
    Ok(HistoryKeyset {
        cursor,
        block_number,
    })
}

pub(super) fn push_history_cursor_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a HistoryCursor,
) {
    builder.push(
        r#"
        WITH history_cursor_row AS (
            SELECT
                block_number,
                chain_id,
                block_hash,
                transaction_hash,
                log_index,
                event_identity
            FROM normalized_events
            WHERE event_identity =
        "#,
    );
    builder.push_bind(&cursor.event_identity);
    builder.push(") ");
}

/// A plain bound at the cursor's block, implied by [`push_history_cursor_after`]: newest-first
/// pages continue at or below the cursor block, oldest-first pages at or above it. The nested
/// continuation predicate compares whole rows and cannot bound an index scan, so without this
/// PostgreSQL reads the order index from the first row of the window on every page. The block
/// is bound as a value rather than read from `history_cursor_row` so the planner estimates the
/// remaining rows from the column statistics instead of a fixed guess.
///
/// It is added only when the filter already excludes rows without a block, as every block
/// window and block bound does. A row without a block sorts after every block newest first,
/// and the comparison would drop it. The cursor row passed the same filter in
/// [`load_history_keyset`], so its block is known too.
pub(super) fn push_history_cursor_block_bound(
    builder: &mut QueryBuilder<'_, Postgres>,
    filter: &EventHistoryReadFilter,
    keyset: &HistoryKeyset<'_>,
) {
    let requires_block =
        filter.block_window.is_some() || filter.from_block.is_some() || filter.to_block.is_some();
    let Some(block_number) = keyset.block_number.filter(|_| requires_block) else {
        return;
    };
    builder.push(match filter.order {
        HistoryOrder::Desc => " AND ne.block_number <= ",
        HistoryOrder::Asc => " AND ne.block_number >= ",
    });
    builder.push_bind(block_number);
}

/// Keyset continuation predicate. `later` sorts after `earlier` in the canonical
/// descending order; the ascending direction swaps the two rows because it is
/// the exact reverse of that order.
pub(super) fn push_history_cursor_after(
    builder: &mut QueryBuilder<'_, Postgres>,
    order: HistoryOrder,
) {
    let (later, earlier) = match order {
        HistoryOrder::Desc => ("ne", "cursor_row"),
        HistoryOrder::Asc => ("cursor_row", "ne"),
    };
    builder.push(format!(
        r#"
        (
            CASE WHEN {later}.block_number IS NULL THEN 1 ELSE 0 END
                > CASE WHEN {earlier}.block_number IS NULL THEN 1 ELSE 0 END
            OR (
                CASE WHEN {later}.block_number IS NULL THEN 1 ELSE 0 END
                    = CASE WHEN {earlier}.block_number IS NULL THEN 1 ELSE 0 END
                AND (
                    {later}.block_number < {earlier}.block_number
                    OR (
                        {later}.block_number IS NOT DISTINCT FROM {earlier}.block_number
                        AND (
                            CASE WHEN {later}.chain_id IS NULL THEN 1 ELSE 0 END
                                > CASE WHEN {earlier}.chain_id IS NULL THEN 1 ELSE 0 END
                            OR (
                                CASE WHEN {later}.chain_id IS NULL THEN 1 ELSE 0 END
                                    = CASE WHEN {earlier}.chain_id IS NULL THEN 1 ELSE 0 END
                                AND (
                                    {later}.chain_id > {earlier}.chain_id
                                    OR (
                                        {later}.chain_id IS NOT DISTINCT FROM {earlier}.chain_id
                                        AND (
                                            CASE WHEN {later}.block_hash IS NULL THEN 1 ELSE 0 END
                                                > CASE WHEN {earlier}.block_hash IS NULL THEN 1 ELSE 0 END
                                            OR (
                                                CASE WHEN {later}.block_hash IS NULL THEN 1 ELSE 0 END
                                                    = CASE WHEN {earlier}.block_hash IS NULL THEN 1 ELSE 0 END
                                                AND (
                                                    {later}.block_hash < {earlier}.block_hash
                                                    OR (
                                                        {later}.block_hash IS NOT DISTINCT FROM {earlier}.block_hash
                                                        AND (
                                                            CASE WHEN {later}.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                                > CASE WHEN {earlier}.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                            OR (
                                                                CASE WHEN {later}.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                                    = CASE WHEN {earlier}.transaction_hash IS NULL THEN 1 ELSE 0 END
                                                                AND (
                                                                    {later}.transaction_hash < {earlier}.transaction_hash
                                                                    OR (
                                                                        {later}.transaction_hash IS NOT DISTINCT FROM {earlier}.transaction_hash
                                                                        AND (
                                                                            COALESCE({later}.log_index, -1) < COALESCE({earlier}.log_index, -1)
                                                                            OR (
                                                                                COALESCE({later}.log_index, -1) = COALESCE({earlier}.log_index, -1)
                                                                                AND {later}.event_identity < {earlier}.event_identity
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
        "#,
    ));
}

pub(super) fn history_cursor_from_row(row: &HistoryEvent) -> HistoryCursor {
    HistoryCursor {
        normalized_event_id: row.normalized_event_id,
        event_identity: row.event_identity.clone(),
    }
}
