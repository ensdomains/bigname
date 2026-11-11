//! The source of a history page filtered by one record key that no name, registration or
//! address anchors: for each chain the page can read, one arm with the key's `RecordChanged`
//! rows and one with the `RecordVersionChanged` rows, each read through its own partial index in
//! page order and stopped after the page. A `resolver` or `contract_address` scope is the
//! emitter equality inside both arms. Only page reads reach this file. The read's other
//! statements keep their source and apply the key in `push_history_filters`.

use sqlx::{Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter, HistoryOrder,
    duplicates::push_product_history_duplicate_filter_for_alias,
    filters::push_string_filter,
    keyset::push_history_cursor_after_for_alias,
    resolver_source::{CANONICAL_ROW, ResolverPageBound},
    source::push_history_lineage_join,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecordArm {
    /// Writes and clears of the key, read through `normalized_events_record_key_history_idx`,
    /// or `normalized_events_record_emitter_key_history_idx` under an emitter scope.
    Write,
    /// Record-version resets, which carry no key and match every key, read through
    /// `normalized_events_record_reset_history_idx`. An emitter scope filters its rows.
    Reset,
}

impl RecordArm {
    fn event_kind(self) -> &'static str {
        match self {
            Self::Write => "RecordChanged",
            Self::Reset => "RecordVersionChanged",
        }
    }
}

/// The chains the arms of a key page read, when the page takes them. Only a canonical,
/// activated-only page with a key and no selector or registration does, because the indexes
/// hold only canonical, activated rows and the conditions an anchor adds cannot be pushed into
/// an arm. Each arm reads one chain, so the page must name its chains: the chains of its block
/// window, narrowed to the resolver's chain under a resolver scope, or the resolver's chain
/// when there is no window. An empty list is a page that can read nothing. `None` leaves the
/// page to the other sources.
pub(super) fn record_arm_chains(
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
    include_candidates: bool,
) -> Option<Vec<&str>> {
    filter.record_key.as_ref()?;
    if !canonical_only
        || include_candidates
        || filter.diagnostics
        || !filter.selectors.is_empty()
        || filter.registration_id.is_some()
    {
        return None;
    }
    let resolver_chain = filter
        .resolver
        .as_ref()
        .map(|resolver| resolver.chain_id.as_str());
    let mut chains: Vec<&str> = match filter.block_window.as_ref() {
        Some(window) => window
            .ranges
            .iter()
            .map(|range| range.chain_id.as_str())
            .filter(|chain| resolver_chain.is_none_or(|resolver| resolver == *chain))
            .collect(),
        None => vec![resolver_chain?],
    };
    chains.sort_unstable();
    chains.dedup();
    Some(chains)
}

/// `FROM (<arms>) ne … WHERE <visibility>` for a key page on `chains`. The arms are disjoint by
/// event kind, so `UNION ALL` gives the rows of the key condition with no de-duplication step.
/// An arm is left out when the read's kinds exclude it.
pub(super) fn push_record_key_source<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    chains: &[&'a str],
    include_cursor_row: bool,
    bound: ResolverPageBound,
) {
    builder.push(" FROM (");
    let mut arms = 0;
    for &chain in chains {
        for arm in [RecordArm::Write, RecordArm::Reset] {
            if !filter.event_kinds.is_empty()
                && !filter
                    .event_kinds
                    .iter()
                    .any(|kind| kind == arm.event_kind())
            {
                continue;
            }
            if arms > 0 {
                builder.push("\nUNION ALL\n");
            }
            push_record_arm(builder, filter, chain, arm, bound);
            arms += 1;
        }
    }
    if arms == 0 {
        builder
            .push("SELECT candidate.* FROM bigname_phase.normalized_events candidate WHERE FALSE");
    }
    builder.push(") ne ");
    if include_cursor_row {
        builder.push(" CROSS JOIN history_cursor_row cursor_row ");
    }
    push_history_lineage_join(builder);
    builder.push(" WHERE ne.consumer_visibility = 'activated' ");
}

fn push_record_arm<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    chain: &'a str,
    arm: RecordArm,
    bound: ResolverPageBound,
) {
    builder.push(
        "(SELECT candidate.*\n FROM bigname_phase.normalized_events candidate \
         LEFT JOIN bigname_phase.chain_lineage candidate_lineage \
         ON candidate_lineage.chain_id = candidate.chain_id \
         AND candidate_lineage.block_hash = candidate.block_hash ",
    );
    if bound.has_cursor {
        builder.push("CROSS JOIN history_cursor_row cursor_row ");
    }
    builder.push("\nWHERE candidate.chain_id = ");
    builder.push_bind(chain);
    match arm {
        RecordArm::Write => {
            builder.push(
                " AND candidate.event_kind = 'RecordChanged' \
                 AND (candidate.after_state ->> 'record_key') = ",
            );
            builder.push_bind(filter.record_key.as_deref());
        }
        RecordArm::Reset => {
            builder.push(" AND candidate.event_kind = 'RecordVersionChanged'");
        }
    }
    // The emitter scopes. A resolver's pointer rows are `ResolverChanged`, which the key
    // condition excludes, so under a key the resolver condition is its emitter and its chain,
    // and `record_arm_chains` already chose only that chain.
    if let Some(contract_address) = filter.contract_address.as_ref() {
        builder.push(" AND lower(candidate.raw_fact_ref ->> 'emitting_address') = ");
        builder.push_bind(contract_address);
    }
    if let Some(resolver) = filter.resolver.as_ref() {
        builder.push(" AND lower(candidate.raw_fact_ref ->> 'emitting_address') = ");
        builder.push_bind(&resolver.address);
    }
    builder.push(" AND candidate.consumer_visibility = 'activated'");
    builder.push(CANONICAL_ROW);
    push_record_arm_filters(builder, filter, chain);
    if let Some(block_number) = bound.cursor_block {
        builder.push(match bound.order {
            HistoryOrder::Desc => " AND candidate.block_number <= ",
            HistoryOrder::Asc => " AND candidate.block_number >= ",
        });
        builder.push_bind(block_number);
    }
    if bound.has_cursor {
        builder.push(" AND ");
        push_history_cursor_after_for_alias(builder, bound.order, "candidate");
    }
    // The page order without its chain term, which the arm fixes. Each index supplies the
    // block number, newest first read forward and oldest first with blocks absent first read
    // backward, so only the rows of one block are sorted before the limit.
    builder.push(match bound.order {
        HistoryOrder::Desc => {
            " ORDER BY candidate.block_number DESC NULLS LAST, \
             candidate.block_hash DESC NULLS LAST, \
             candidate.transaction_index DESC NULLS LAST, \
             candidate.log_index DESC NULLS LAST, \
             candidate.event_identity DESC"
        }
        HistoryOrder::Asc => {
            " ORDER BY candidate.block_number ASC NULLS FIRST, \
             candidate.block_hash ASC NULLS FIRST, \
             candidate.transaction_index ASC NULLS FIRST, \
             candidate.log_index ASC NULLS FIRST, \
             candidate.event_identity ASC"
        }
    });
    builder.push(" LIMIT ");
    builder.push_bind(bound.limit);
    builder.push(")");
}

/// Every other outer page condition that can drop a `candidate` row, pushed before the arm
/// limit, so no row the outer page keeps is cut in favour of one it drops. The arms serve no
/// read with a selector or a registration (`record_arm_chains`), the conditions not pushed
/// here.
fn push_record_arm_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    chain: &str,
) {
    if let Some(namespace) = filter.namespace.as_ref() {
        builder.push(" AND candidate.namespace = ");
        builder.push_bind(namespace);
    }
    if filter.match_no_events {
        builder.push(" AND FALSE");
    }
    if !filter.event_kinds.is_empty() {
        builder.push(" AND ");
        push_string_filter(builder, "candidate.event_kind", &filter.event_kinds);
    }
    if let Some(from_block) = filter.from_block {
        builder.push(" AND candidate.block_number >= ");
        builder.push_bind(from_block);
    }
    if let Some(to_block) = filter.to_block {
        builder.push(" AND candidate.block_number <= ");
        builder.push_bind(to_block);
    }
    if let Some(window) = filter.block_window.as_ref() {
        // The arm's chain has at least one range, or `record_arm_chains` would not name it.
        // Its outermost bounds bound the index scan. The ranges themselves still decide each
        // row.
        let ranges: Vec<_> = window
            .ranges
            .iter()
            .filter(|range| range.chain_id == chain)
            .collect();
        builder.push(" AND (FALSE");
        for range in &ranges {
            builder.push(" OR (TRUE");
            if let Some(from_block) = range.from_block {
                builder.push(" AND candidate.block_number >= ");
                builder.push_bind(from_block);
            }
            if let Some(to_block) = range.to_block {
                builder.push(" AND candidate.block_number <= ");
                builder.push_bind(to_block);
            }
            builder.push(" AND candidate.block_number IS NOT NULL)");
        }
        builder.push(")");
        let lowest = ranges
            .iter()
            .map(|range| range.from_block)
            .collect::<Option<Vec<i64>>>()
            .and_then(|bounds| bounds.into_iter().min());
        let highest = ranges
            .iter()
            .map(|range| range.to_block)
            .collect::<Option<Vec<i64>>>()
            .and_then(|bounds| bounds.into_iter().max());
        if let Some(lowest) = lowest {
            builder.push(" AND candidate.block_number >= ");
            builder.push_bind(lowest);
        }
        if let Some(highest) = highest {
            builder.push(" AND candidate.block_number <= ");
            builder.push_bind(highest);
        }
    }
    builder.push(
        " AND (candidate.block_hash IS NULL \
         OR candidate_lineage.canonicality_state IN (\n\
         'canonical'::bigname_phase.canonicality_state,\n\
         'safe'::bigname_phase.canonicality_state,\n\
         'finalized'::bigname_phase.canonicality_state\n\
         ))",
    );
    push_product_history_duplicate_filter_for_alias(builder, filter, true, "candidate");
}
