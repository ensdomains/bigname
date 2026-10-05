//! Bounded address-history reads. Candidate keys and witnesses stay in a SQL cursor; current
//! membership and record attribution are validated in fixed batches on the same snapshot.

mod duplicates;
mod entry;
pub use entry::load_address_history_page_for_relations;
mod matches;
#[cfg(test)]
mod plan_tests;
pub(crate) mod seams;
mod source;
mod walk;

use std::collections::BTreeMap;

use sqlx::FromRow;
use uuid::Uuid;

use super::{HistoryCursor, HistoryScope};
use crate::AddressNameRelation;

struct AddressRead<'a> {
    address: &'a str,
    namespace: Option<&'a str>,
    relations: Option<&'a [AddressNameRelation]>,
    scope: HistoryScope,
    canonical_only: bool,
    published: Option<&'a BTreeMap<String, i64>>,
}

/// A single possible reason an event belongs to an address. This carries no event payload,
/// composed summary, or complete list of the event's possible witnesses.
#[derive(Debug, FromRow)]
struct Witness {
    normalized_event_id: i64,
    event_identity: String,
    chain_id: Option<String>,
    block_number: Option<i64>,
    block_hash: Option<String>,
    node: Option<String>,
    witness_kind: i32,
    current_chain: Option<String>,
    current_name: Option<String>,
    witness_resource: Option<Uuid>,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn load_page(
    pool: &sqlx::PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    scope: HistoryScope,
    cursor: Option<&HistoryCursor>,
    page_size: u64,
    summary_mode: super::HistorySummaryMode,
    options: &super::HistoryPageOptions,
    fence: Option<&super::InterpretRedoFence>,
) -> anyhow::Result<super::HistoryPage> {
    use super::{EventHistoryReadFilter, HistorySummaryMode, keyset::HistoryKeyset};
    use crate::projection_helpers::{checked_page_limit_i64_from_usize, checked_page_size_usize};
    use anyhow::Context;

    let mut transaction = super::paging::begin_history_snapshot(pool, "address page").await?;
    if let Some(fence) = fence {
        super::redo::ensure_interpret_redo_fence(&mut transaction, fence).await?;
    }
    let read = AddressRead {
        address,
        namespace,
        relations,
        scope,
        canonical_only: true,
        published: options.publication_block_bounds.as_ref(),
    };
    let filter = EventHistoryReadFilter::default().with_page_options(options);
    let mut membership = matches::Membership::new();
    // Position cursors remain usable after their row disappears. A legacy row-only cursor
    // still has to identify an eligible, unsuppressed member of this exact collection.
    let keyset = if let Some(cursor) = cursor {
        let block_number = if let Some(position) = &cursor.position {
            position.block_number
        } else {
            let mut cursor_filter = filter.clone();
            if !cursor_filter.bind_cursor_anchor_to_event_kinds {
                cursor_filter.event_kinds.clear();
            }
            let mut anchor = walk::Accumulator::new(1, Some(0));
            walk::collect(
                &mut transaction,
                &read,
                &cursor_filter,
                None,
                Some(&cursor.event_identity),
                &mut membership,
                &mut anchor,
            )
            .await?;
            let id = anchor.ids.first().ok_or(super::InvalidHistoryCursor)?;
            sqlx::query_scalar::<_, Option<i64>>(
                "SELECT block_number FROM normalized_events WHERE normalized_event_id = $1",
            )
            .bind(id)
            .fetch_one(&mut *transaction)
            .await?
        };
        Some(HistoryKeyset {
            cursor,
            block_number,
        })
    } else {
        None
    };
    let size = checked_page_size_usize(
        page_size,
        "history page_size must be positive",
        "history page_size does not fit in usize",
    )?;
    let page_limit = checked_page_limit_i64_from_usize(
        size,
        "history page_size is too large",
        "history page_size exceeds SQL limit",
    )? as usize;
    let count_limit = match summary_mode {
        HistorySummaryMode::None => Some(0),
        HistorySummaryMode::Count => None,
        HistorySummaryMode::CappedCount(cap) => Some(u64::try_from(
            i64::try_from(cap.saturating_add(1))
                .context("history total_count cap exceeds SQL limit")?,
        )?),
        HistorySummaryMode::Full => {
            anyhow::bail!("bounded address history requires count-only summary")
        }
    };
    let mut output = walk::Accumulator::new(
        page_limit,
        if keyset.is_some() {
            Some(0)
        } else {
            count_limit
        },
    );
    walk::collect(
        &mut transaction,
        &read,
        &filter,
        keyset.as_ref(),
        None,
        &mut membership,
        &mut output,
    )
    .await?;
    let total = if keyset.is_some() && summary_mode != HistorySummaryMode::None {
        let mut count = walk::Accumulator::new(0, count_limit);
        walk::collect(
            &mut transaction,
            &read,
            &filter,
            None,
            None,
            &mut membership,
            &mut count,
        )
        .await?;
        count.count
    } else {
        output.count
    };
    let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new("");
    super::columns::push_history_select(&mut query, &filter, true, false, false);
    let has_more = output.ids.len() > size;
    let page_ids = &output.ids[..output.ids.len().min(size)];
    query
        .push(" AND ne.normalized_event_id = ANY(")
        .push_bind(page_ids)
        .push("::bigint[])");
    super::paging::push_history_order(&mut query, filter.order);
    let rows = query.build().fetch_all(&mut *transaction).await?;
    let _payloads = seams::Live::new("page_payloads", rows.len());
    let rows = rows
        .into_iter()
        .map(super::decoders::decode_history_event)
        .collect::<anyhow::Result<Vec<_>>>()?;
    let next_cursor = has_more
        .then(|| rows.last().map(super::keyset::history_cursor_from_row))
        .flatten();
    transaction.commit().await?;
    Ok(super::HistoryPage {
        rows,
        next_cursor,
        summary: (summary_mode != HistorySummaryMode::None).then_some(super::HistorySummary {
            total_count: total,
            normalized_event_ids: Vec::new(),
            raw_fact_refs: Vec::new(),
            manifest_versions: Vec::new(),
            chain_position_samples: Vec::new(),
            last_updated: None,
        }),
        interpret_redo_fence: fence.cloned(),
    })
}
