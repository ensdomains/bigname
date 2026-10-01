use anyhow::{Context, Result};
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{
    ChainBlockRange, EventHistoryReadFilter, HistoryBlockWindow, summary::push_history_count_query,
};

/// Counts the product-visible events one contract emitted on one chain through `as_of_block`:
/// the rows `GET /v1/events?contract_address=` counts, built by the same statement.
pub async fn count_contract_events(
    pool: &PgPool,
    namespace: &str,
    chain_id: &str,
    address: &str,
    event_kinds: &[String],
    as_of_block: Option<i64>,
) -> Result<u64> {
    let filter = contract_count_filter(namespace, chain_id, address, event_kinds, as_of_block);
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_history_count_query(&mut builder, &filter, true, None);
    let count = builder
        .build_query_scalar::<i64>()
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to count events emitted by {chain_id}:{address}"))?;
    u64::try_from(count).context("negative contract event count")
}

/// The feed's filter for `contract_address` on one chain, bounded at `as_of_block`.
pub(super) fn contract_count_filter(
    namespace: &str,
    chain_id: &str,
    address: &str,
    event_kinds: &[String],
    as_of_block: Option<i64>,
) -> EventHistoryReadFilter {
    EventHistoryReadFilter {
        namespace: Some(namespace.to_owned()),
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
