//! Keyset continuation for history pages: the cursor's position in the history order, and the
//! predicates that start a page strictly after it in the requested order. A cursor that carries
//! its position continues after it whether or not its anchor row still exists; a cursor without
//! one resumes through its anchor row, which must still be one of the rows the filter reads.

use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter, HistoryCursor, HistoryEvent, HistoryOrder, HistoryPosition,
    InvalidHistoryCursor, duplicates::push_product_history_duplicate_filter,
    paging::push_history_filters, source::push_history_source_for_filter,
};

/// A validated continuation point: the cursor and the block number of its row.
pub(super) struct HistoryKeyset<'a> {
    pub(super) cursor: &'a HistoryCursor,
    pub(super) block_number: Option<i64>,
}

/// The cursor's block number for [`push_history_cursor_block_bound`]. A cursor without its
/// position must name a row that is still one of the rows this filter reads.
pub(super) async fn load_history_keyset<'a>(
    connection: &mut PgConnection,
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
    cursor: &'a HistoryCursor,
    include_candidates: bool,
) -> Result<HistoryKeyset<'a>> {
    if let Some(position) = cursor.position.as_ref() {
        return Ok(HistoryKeyset {
            cursor,
            block_number: position.block_number,
        });
    }
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

/// `WITH history_cursor_row AS (…)`: the cursor's ordering values, bound as given when the cursor
/// carries its position, else read from its anchor row.
pub(super) fn push_history_cursor_cte<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    cursor: &'a HistoryCursor,
) {
    if let Some(position) = cursor.position.as_ref() {
        builder.push(" WITH history_cursor_row AS (SELECT ");
        builder.push_bind(position.block_number);
        builder.push("::bigint AS block_number, ");
        builder.push_bind(position.chain_id.as_deref());
        builder.push("::text AS chain_id, ");
        builder.push_bind(position.block_hash.as_deref());
        builder.push("::text AS block_hash, ");
        builder.push_bind(position.transaction_index);
        builder.push("::bigint AS transaction_index, ");
        builder.push_bind(position.log_index);
        builder.push("::bigint AS log_index, ");
        builder.push_bind(&cursor.event_identity);
        builder.push("::text AS event_identity) ");
        return;
    }
    builder.push(
        r#"
        WITH history_cursor_row AS (
            SELECT
                block_number,
                chain_id,
                block_hash,
                transaction_index,
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
/// window and block bound does, and the cursor has a block. A row without a block sorts after
/// every block newest first, and the comparison would drop it.
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
/// the exact reverse of that order. Within a block the transaction index and the
/// log index compare with a missing value as -1, the placement the order gives a
/// null: after every transaction newest first, before every one oldest first.
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
                                                            COALESCE({later}.transaction_index, -1) < COALESCE({earlier}.transaction_index, -1)
                                                            OR (
                                                                COALESCE({later}.transaction_index, -1) = COALESCE({earlier}.transaction_index, -1)
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
        "#,
    ));
}

pub(super) fn history_cursor_from_row(row: &HistoryEvent) -> HistoryCursor {
    HistoryCursor {
        normalized_event_id: Some(row.normalized_event_id),
        event_identity: row.event_identity.clone(),
        position: Some(HistoryPosition {
            block_number: row.block_number,
            chain_id: row.chain_id.clone(),
            block_hash: row.block_hash.clone(),
            transaction_index: row.transaction_index,
            transaction_hash: None,
            log_index: row.log_index,
        }),
    }
}

/// The history position of the event named `event_identity`, for a cursor that carries only its
/// anchor, or `None` when no such event exists. It reads under the same Interpret redo check as a
/// history page, in one snapshot: during a redo it fails with `InterpretRedoInProgress` instead of
/// reporting an anchor the redo may have removed.
pub async fn load_history_anchor_position(
    pool: &PgPool,
    event_identity: &str,
) -> Result<Option<HistoryPosition>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin a history cursor anchor read")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to configure a history cursor anchor read")?;
    super::redo::ensure_interpret_not_redo(&mut transaction).await?;
    let row = sqlx::query(
        "SELECT block_number, chain_id, block_hash, transaction_index, log_index
             FROM bigname_phase.normalized_events
             WHERE event_identity = $1",
    )
    .bind(event_identity)
    .fetch_optional(&mut *transaction)
    .await
    .context("failed to load a history cursor anchor")?;
    transaction
        .commit()
        .await
        .context("failed to commit a history cursor anchor read")?;
    row.map(|row| {
        Ok(HistoryPosition {
            block_number: sqlx::Row::try_get(&row, "block_number")?,
            chain_id: sqlx::Row::try_get(&row, "chain_id")?,
            block_hash: sqlx::Row::try_get(&row, "block_hash")?,
            transaction_index: sqlx::Row::try_get(&row, "transaction_index")?,
            transaction_hash: None,
            log_index: sqlx::Row::try_get(&row, "log_index")?,
        })
    })
    .transpose()
}

/// The transaction index a cursor issued before the history order compared transaction indexes
/// continues from: `position` carries its anchor's transaction hash and no index. The index is
/// read from the anchor row, or, when that row is gone, from any other event of the same
/// transaction in the same block, because the index belongs to the transaction. `None` when
/// neither exists. It reads under the same Interpret redo check as
/// [`load_history_anchor_position`].
pub async fn load_history_transaction_index(
    pool: &PgPool,
    event_identity: &str,
    position: &HistoryPosition,
) -> Result<Option<i64>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin a history cursor transaction read")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to configure a history cursor transaction read")?;
    super::redo::ensure_interpret_not_redo(&mut transaction).await?;
    // The second arm reads one block's rows through `normalized_events_block_idx`.
    let index = sqlx::query_scalar::<_, i64>(
        "SELECT transaction_index FROM (
             (SELECT transaction_index, 0 AS preference
              FROM bigname_phase.normalized_events
              WHERE event_identity = $1 AND transaction_index IS NOT NULL)
             UNION ALL
             (SELECT transaction_index, 1 AS preference
              FROM bigname_phase.normalized_events
              WHERE chain_id = $2 AND block_hash = $3 AND transaction_hash = $4
                AND transaction_index IS NOT NULL
              LIMIT 1)
         ) found
         ORDER BY preference
         LIMIT 1",
    )
    .bind(event_identity)
    .bind(position.chain_id.as_deref())
    .bind(position.block_hash.as_deref())
    .bind(position.transaction_hash.as_deref())
    .fetch_optional(&mut *transaction)
    .await
    .context("failed to load a history cursor transaction index")?;
    transaction
        .commit()
        .await
        .context("failed to commit a history cursor transaction read")?;
    Ok(index)
}
