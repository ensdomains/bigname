use anyhow::{Context, Result};
use sqlx::{Postgres, QueryBuilder};

use super::{
    ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, summary::push_history_count_query,
};

/// Counts the product-visible events one contract emitted on one chain through `as_of_block`:
/// the rows `GET /v1/events?contract_address=` counts on that chain, built by the same
/// statement, in every namespace the contract emitted into. With `cap`, the count stops after
/// `cap + 1` rows, so a result above `cap` means only "more than the cap".
pub async fn count_contract_events(
    db: impl Into<crate::ReadDb<'_>>,
    chain_id: &str,
    address: &str,
    event_kinds: &[String],
    as_of_block: Option<i64>,
    cap: Option<u64>,
) -> Result<u64> {
    let limit = cap
        .map(|cap| i64::try_from(cap.saturating_add(1)))
        .transpose()
        .context("contract event count cap exceeds SQL limit")?;
    let mut conn = db.into().acquire().await?;
    let filter = contract_count_filter(chain_id, address, event_kinds, as_of_block);
    #[cfg(any(test, feature = "test-support"))]
    if super::query_plan::COUNT_SQL_DUMP_ON.load(std::sync::atomic::Ordering::SeqCst) {
        let mut render = QueryBuilder::<Postgres>::new("");
        push_history_count_query(&mut render, &filter, true, limit);
        super::query_plan::record_count_statement(&mut *conn, render).await?;
    }
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_count_query(&mut builder, &filter, true, limit);
    let count = builder
        .build_query_scalar::<i64>()
        .fetch_one(&mut *conn)
        .await
        .with_context(|| format!("failed to count events emitted by {chain_id}:{address}"))?;
    u64::try_from(count).context("negative contract event count")
}

/// The feed's filter for `contract_address` on one chain, bounded at `as_of_block`.
pub(super) fn contract_count_filter(
    chain_id: &str,
    address: &str,
    event_kinds: &[String],
    as_of_block: Option<i64>,
) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        contract_address: Some(address.to_ascii_lowercase()),
        event_kinds: event_kinds.to_vec(),
        match_no_events: event_kinds.is_empty(),
        block_window: Some(HistoryBlockWindow {
            ranges: vec![ChainBlockRange {
                chain_id: chain_id.to_owned(),
                from_block: None,
                to_block: as_of_block,
            }],
        }),
        ..EventHistoryReadFilter::default()
    }
}
