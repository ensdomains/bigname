use anyhow::{Context, Result, bail};
use tracing::debug;

use super::{
    client::{CoinbaseSqlClient, CoinbaseSqlQueryResponse},
    query::{CoinbaseSqlFilterPack, build_query},
    rows::CoinbaseSqlLogRow,
};
use crate::backfill::CoinbaseSqlFetchStats;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CoinbaseSqlLogCursor {
    pub(super) block_number: i64,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
}

impl From<&CoinbaseSqlLogRow> for CoinbaseSqlLogCursor {
    fn from(row: &CoinbaseSqlLogRow) -> Self {
        Self {
            block_number: row.block_number,
            transaction_index: row.transaction_index,
            log_index: row.log_index,
        }
    }
}

impl CoinbaseSqlLogCursor {
    fn strictly_after(self, previous: Self) -> bool {
        (self.block_number, self.transaction_index, self.log_index)
            > (
                previous.block_number,
                previous.transaction_index,
                previous.log_index,
            )
    }
}

pub(super) struct CoinbaseSqlFetchedPages {
    pub(super) rows: Vec<CoinbaseSqlLogRow>,
    pub(super) stats: CoinbaseSqlFetchStats,
}

pub(super) async fn fetch_all_pages(
    client: &CoinbaseSqlClient,
    pack: &CoinbaseSqlFilterPack,
    page_limit: usize,
    sql_char_limit: usize,
) -> Result<CoinbaseSqlFetchedPages> {
    let mut cursor = None;
    let mut rows = Vec::new();
    let mut stats = CoinbaseSqlFetchStats::default();
    let mut previous_cursor = None;

    loop {
        let sql = build_query(pack, cursor, page_limit)?;
        if sql.len() > sql_char_limit {
            bail!(
                "Coinbase SQL query length {} exceeds configured character limit {}",
                sql.len(),
                sql_char_limit
            );
        }
        let response = client.run_query(&sql).await?;
        record_response_stats(&mut stats, &response);
        let page_len = response.rows.len();

        append_page_rows(&mut rows, &mut previous_cursor, response.rows, &mut stats)?;

        if page_len < page_limit {
            break;
        }
        ensure_full_page_advanced_cursor(cursor, previous_cursor)?;
        cursor = previous_cursor;
        if cursor.is_none() {
            bail!("Coinbase SQL returned a full page without a cursor row");
        }
    }

    Ok(CoinbaseSqlFetchedPages { rows, stats })
}

/// Fold one page of rows into the accumulated result, enforcing strict cursor
/// ordering. The query is a UNION ALL of a decoded-logs arm and an
/// encoded-logs arm ordered only by (block_number, transaction_index,
/// log_index); the arms are meant to be disjoint, but decode-pipeline lag can
/// transiently surface the same physical log in both, delivering the same
/// cursor tuple twice in arbitrary adjacent order. Such benign duplicates are
/// dropped (preferring the decoded shape); anything else out of order still
/// fails the fetch. State persists across pages so a duplicate arriving at a
/// page head is reconciled against the previous page's tail — page cursors
/// are exclusive lower bounds, so equal tuples are normally never re-fetched.
pub(super) fn append_page_rows(
    rows: &mut Vec<CoinbaseSqlLogRow>,
    previous_cursor: &mut Option<CoinbaseSqlLogCursor>,
    page_rows: Vec<CoinbaseSqlLogRow>,
    stats: &mut CoinbaseSqlFetchStats,
) -> Result<()> {
    for row in page_rows {
        let row_cursor = CoinbaseSqlLogCursor::from(&row);
        if let Some(previous) = *previous_cursor {
            if row_cursor == previous {
                let kept = rows
                    .last_mut()
                    .context("Coinbase SQL duplicate row arrived before any kept row")?;
                reconcile_union_duplicate(kept, row, stats)?;
                continue;
            }
            if !row_cursor.strictly_after(previous) {
                bail!(
                    "Coinbase SQL rows were not strictly ordered at block {}, transaction index {}, log index {}",
                    row.block_number,
                    row.transaction_index,
                    row.log_index
                );
            }
        }
        *previous_cursor = Some(row_cursor);
        rows.push(row);
    }

    Ok(())
}

/// Resolve a row that repeats the previously kept row's cursor tuple. Byte-
/// identical repeats and encoded/decoded shapes of the same physical log are
/// benign union duplicates: keep the decoded shape (it carries the event
/// signature and synthesized data) regardless of arrival order. Any other
/// repeat is genuine corruption and fails exactly like disorder.
fn reconcile_union_duplicate(
    kept: &mut CoinbaseSqlLogRow,
    duplicate: CoinbaseSqlLogRow,
    stats: &mut CoinbaseSqlFetchStats,
) -> Result<()> {
    if *kept == duplicate {
        stats.union_duplicate_count += 1;
        debug!(
            block_number = duplicate.block_number,
            transaction_index = duplicate.transaction_index,
            log_index = duplicate.log_index,
            "dropped identical Coinbase SQL union duplicate row"
        );
        return Ok(());
    }

    if rows_are_same_underlying_log(kept, &duplicate) {
        // The decoded arm carries an event_signature; the encoded arm selects
        // NULL for it (and NULL parameters, so its data is synthesized later
        // from the validation provider).
        match (
            kept.event_signature.is_some(),
            duplicate.event_signature.is_some(),
        ) {
            (true, false) => {
                stats.union_duplicate_count += 1;
                debug!(
                    block_number = duplicate.block_number,
                    transaction_index = duplicate.transaction_index,
                    log_index = duplicate.log_index,
                    "dropped encoded Coinbase SQL union duplicate row; decoded row already kept"
                );
                return Ok(());
            }
            (false, true) => {
                stats.union_duplicate_count += 1;
                debug!(
                    block_number = duplicate.block_number,
                    transaction_index = duplicate.transaction_index,
                    log_index = duplicate.log_index,
                    "replaced encoded Coinbase SQL union duplicate row with its decoded row"
                );
                *kept = duplicate;
                return Ok(());
            }
            _ => {}
        }
    }

    bail!(
        "Coinbase SQL rows were not strictly ordered at block {}, transaction index {}, log index {}",
        duplicate.block_number,
        duplicate.transaction_index,
        duplicate.log_index
    )
}

/// Whether two rows sharing a cursor tuple describe the same physical log:
/// everything that identifies the log on-chain must match; only the
/// decode-dependent fields (event_signature and the data synthesized from
/// parameters) may differ between the union arms.
fn rows_are_same_underlying_log(a: &CoinbaseSqlLogRow, b: &CoinbaseSqlLogRow) -> bool {
    a.block_hash == b.block_hash
        && a.transaction_hash == b.transaction_hash
        && a.emitting_address == b.emitting_address
        && a.topics == b.topics
}

/// A full page whose rows all reconciled as duplicates of the previous tail
/// leaves the pagination cursor unmoved, and the next request would be
/// byte-identical — an infinite loop against a paid API. Fail the fetch
/// instead; a healthy warehouse can only produce this by returning the same
/// duplicate row page_limit times.
pub(super) fn ensure_full_page_advanced_cursor(
    previous_query_cursor: Option<CoinbaseSqlLogCursor>,
    next_query_cursor: Option<CoinbaseSqlLogCursor>,
) -> Result<()> {
    if let Some(cursor) = next_query_cursor
        && Some(cursor) == previous_query_cursor
    {
        bail!(
            "Coinbase SQL returned a full page without advancing the pagination cursor past block {}, transaction index {}, log index {}; refusing to re-issue an identical query",
            cursor.block_number,
            cursor.transaction_index,
            cursor.log_index
        );
    }
    Ok(())
}

fn record_response_stats(stats: &mut CoinbaseSqlFetchStats, response: &CoinbaseSqlQueryResponse) {
    stats.query_count += 1;
    stats.retry_count += response.retry_count;
    stats.record_page(response.rows.len());
}
