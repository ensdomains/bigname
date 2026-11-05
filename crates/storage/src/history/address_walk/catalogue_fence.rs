//! The fence around each catalogue probe's read of `normalized_events`.

use sqlx::{Postgres, QueryBuilder};

use super::{
    AddressRead,
    catalogue_source::{Bucket, block_window, push_bucket},
};
use crate::history::{
    EventHistoryReadFilter, HistoryOrder, catalogue_contract as contract,
    keyset::{HistoryKeyset, push_history_cursor_block_bound},
    paging::push_history_order_terms,
};

/// The `FROM` item of one probe: `normalized_events` itself, or behind the fence.
///
/// The planner estimates any block range above the `block_number` histogram of
/// `normalized_events` at about one row, and the newest blocks are always above it. Unfenced, a
/// probe of a bucket near the head was planned as a scan of the whole bucket on
/// `normalized_events_chain_block_number_desc_idx`, with the probe's key only a filter, once per
/// source. The fence holds the key, the readable terms each key index is partial on, the bucket
/// range, the block bounds the window, the cursor and the selected event imply, and the order.
/// `OFFSET 0` keeps the outer filters, the `LIMIT` and the outer chain equality out of it. The
/// chain-and-block index then has no leading key inside, so the probe's own index is the only
/// cheap path whatever the estimate. The node and record-id keys carry the chain because their
/// indexes lead with it. The order lets a `LIMIT 1` seek stop at the first readable row of a
/// source with a long history. Every term inside is also outside, and the outer statement still
/// applies every filter, so the rows are the unfenced statement's rows. The registrar-grant
/// lookup in `history/wrapped_registrar.rs` is fenced the same way.
#[allow(clippy::too_many_arguments)]
pub(super) fn push_event_rows<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'_>>,
    bucket: Bucket,
    identity: Option<&'a str>,
    fence: bool,
    push_key: impl FnOnce(&mut QueryBuilder<'a, Postgres>),
) {
    if !fence {
        query.push("normalized_events ne");
        return;
    }
    query.push("(SELECT ne.* FROM normalized_events ne WHERE ne.consumer_visibility = 'activated'");
    if read.canonical_only {
        query.push(" AND ne.canonicality_state IN ('canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state)");
    }
    query.push(" AND ");
    push_key(query);
    let (minimum, maximum) = block_window(filter);
    match (bucket, filter.order, minimum) {
        // The window's lower bound excludes unpositioned events, so the descending seek's
        // `IS NULL` arm is dead here. Without it the planner has no `OR` to serve with a
        // bitmap of the whole history; the probe's outer bucket term keeps it.
        (Bucket::After(after), HistoryOrder::Desc, Some(_)) if after != contract::NULL_BUCKET => {
            query
                .push(" AND ne.block_number < ")
                .push_bind(after.saturating_mul(contract::BUCKET_BLOCKS));
        }
        _ => push_bucket(query, bucket, filter.order),
    }
    if let Some(minimum) = minimum {
        query.push(" AND ne.block_number >= ").push_bind(minimum);
    }
    if let Some(maximum) = maximum {
        query.push(" AND ne.block_number <= ").push_bind(maximum);
    }
    if let Some(keyset) = keyset {
        push_history_cursor_block_bound(query, filter, keyset);
    }
    if let Some(identity) = identity {
        query.push(" AND ne.event_identity = ").push_bind(identity);
    }
    query.push(" ORDER BY ");
    push_history_order_terms(query, filter.order);
    query.push(" OFFSET 0) ne");
}
