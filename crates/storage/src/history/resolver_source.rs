//! The source of a resolver event read: the resolver's own rows, read through its own indexes,
//! for the request shape that has them. Every other read keeps [`source`]'s source and the
//! resolver condition in `push_history_filters`. Only serving reads reach this file.

use sqlx::{Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter, EventHistoryResolverFilter, HistoryOrder,
    duplicates::push_product_history_duplicate_filter_for_alias,
    filters::push_string_filter,
    keyset::push_history_cursor_after_for_alias,
    source::{self, push_history_lineage_join},
};

const CANONICAL_ROW: &str = " AND candidate.canonicality_state IN (\n\
     'canonical'::bigname_phase.canonicality_state,\n\
     'safe'::bigname_phase.canonicality_state,\n\
     'finalized'::bigname_phase.canonicality_state\n\
     )";

/// How far one page read may run into each resolver candidate arm. Each arm
/// keeps at most `limit` rows with a block and at most `limit` rows without
/// one, in the page order and after the page cursor. So the three arms
/// together hand the outer page at most six page bounds of rows to sort.
/// `cursor_block` is the plain block bound the outer page also pushes, set
/// only when the read already excludes rows without a block.
#[derive(Clone, Copy)]
pub(super) struct ResolverPageBound {
    pub(super) order: HistoryOrder,
    pub(super) limit: i64,
    pub(super) has_cursor: bool,
    pub(super) cursor_block: Option<i64>,
}

/// Where a statement applies the resolver. Only [`push_history_source_for_page`]
/// returns `Arms`, so a statement whose source is not the arms always gets the
/// resolver condition in `push_history_filters`. A caller that drops the value
/// could pair the arms with a second resolver condition, or the plain source
/// with none.
#[must_use]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ResolverSource {
    Arms,
    Condition,
}

/// The resolver whose own rows every statement of a request draws from, when
/// the request takes the arms. Only a canonical, activated-only request with no
/// selector and no registration does. The pointer indexes hold only canonical,
/// activated rows and the emitter index only canonical ones, so in another mode
/// an arm has no index to read. A selector cannot be pushed inside an arm before
/// its limit, so with one the arms would take no page bound, and each arm would
/// have to finish its match against the selector before the page could stop.
/// `event_history_read_filter` sets `filter.diagnostics` for a request that
/// includes candidate rows. So a statement such as the summary count, which
/// reads activated rows whatever the request, still follows the request.
fn resolver_arm_source(
    filter: &EventHistoryReadFilter,
    canonical_only: bool,
    include_candidates: bool,
) -> Option<&EventHistoryResolverFilter> {
    filter.resolver.as_ref().filter(|_| {
        canonical_only
            && !include_candidates
            && !filter.diagnostics
            && filter.selectors.is_empty()
            && filter.registration_id.is_none()
    })
}

/// [`source::push_history_source_for_filter`], or the resolver's own rows for
/// a request [`resolver_arm_source`] admits.
pub(super) fn push_history_source_for_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
) -> ResolverSource {
    push_history_source_for_page(
        builder,
        filter,
        canonical_only,
        include_cursor_row,
        include_candidates,
        None,
    )
}

/// The same source for one page read, with each resolver arm bounded by
/// `resolver_page_bound`. See [`ResolverPageBound`].
pub(super) fn push_history_source_for_page<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
    resolver_page_bound: Option<ResolverPageBound>,
) -> ResolverSource {
    let Some(resolver) = resolver_arm_source(filter, canonical_only, include_candidates) else {
        source::push_history_source_for_filter(
            builder,
            filter,
            canonical_only,
            include_cursor_row,
            include_candidates,
        );
        return ResolverSource::Condition;
    };
    // The resolver's own rows only: what it emitted, the pointers set to it, and the
    // pointers cleared from it. The arms are mutually exclusive by priority:
    // the set arm excludes rows the emitted arm claims, and the cleared arm
    // excludes rows both earlier arms claim. So `UNION ALL` gives the same
    // rows as the resolver condition, with no de-duplication step. Each arm
    // carries the chain, visibility, and canonicality conditions, so each reads
    // through its own index.
    builder.push(" FROM (");
    for (index, arm) in [
        ResolverArm::Emitted,
        ResolverArm::PointerSet,
        ResolverArm::PointerCleared,
    ]
    .into_iter()
    .enumerate()
    {
        if index > 0 {
            builder.push("\nUNION ALL\n");
        }
        push_resolver_candidate_arm(builder, filter, resolver, arm, resolver_page_bound);
    }
    builder.push(") ne ");
    if include_cursor_row {
        builder.push(" CROSS JOIN history_cursor_row cursor_row ");
    }
    push_history_lineage_join(builder);
    builder.push(" WHERE ne.consumer_visibility = 'activated' ");
    ResolverSource::Arms
}

#[derive(Clone, Copy, PartialEq)]
enum ResolverArm {
    Emitted,
    PointerSet,
    PointerCleared,
}

#[derive(Clone, Copy, PartialEq)]
enum ResolverBlockPart {
    Any,
    WithBlock,
    WithoutBlock,
}

fn push_resolver_candidate_arm<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    resolver: &'a EventHistoryResolverFilter,
    arm: ResolverArm,
    page_bound: Option<ResolverPageBound>,
) {
    match page_bound {
        None => push_resolver_candidate_select(
            builder,
            filter,
            resolver,
            arm,
            ResolverBlockPart::Any,
            None,
        ),
        Some(bound) => {
            // Rows with a block and rows without one sit at opposite ends of the
            // page order. The pointer indexes order the block ascending, so in
            // either direction they place block nulls at the other end from the
            // page. Bounding the two parts on their own keeps the page order
            // while each part orders in a way its index can serve. The emitter
            // index orders the block descending with nulls last, which is the
            // page's own null placement. The emitted arm splits the same way,
            // and each of its parts still keeps the page order.
            builder.push("(");
            push_resolver_candidate_select(
                builder,
                filter,
                resolver,
                arm,
                ResolverBlockPart::WithBlock,
                Some(bound),
            );
            builder.push("\nUNION ALL\n");
            push_resolver_candidate_select(
                builder,
                filter,
                resolver,
                arm,
                ResolverBlockPart::WithoutBlock,
                Some(bound),
            );
            builder.push(")");
        }
    }
}

fn push_resolver_candidate_select<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    resolver: &'a EventHistoryResolverFilter,
    arm: ResolverArm,
    block_part: ResolverBlockPart,
    page_bound: Option<ResolverPageBound>,
) {
    if page_bound.is_some() {
        builder.push("(");
    }
    builder.push("SELECT candidate.*\n FROM bigname_phase.normalized_events candidate ");
    if page_bound.is_some() {
        builder.push(
            "LEFT JOIN bigname_phase.chain_lineage candidate_lineage \
             ON candidate_lineage.chain_id = candidate.chain_id \
             AND candidate_lineage.block_hash = candidate.block_hash ",
        );
    }
    if page_bound.is_some_and(|bound| bound.has_cursor) {
        builder.push("CROSS JOIN history_cursor_row cursor_row ");
    }
    builder.push("\nWHERE candidate.chain_id = ");
    builder.push_bind(&resolver.chain_id);
    match arm {
        ResolverArm::Emitted => {
            builder.push(" AND lower(candidate.raw_fact_ref ->> 'emitting_address') = ");
            builder.push_bind(&resolver.address);
        }
        ResolverArm::PointerSet => {
            builder.push(
                " AND candidate.event_kind = 'ResolverChanged' \
                 AND lower(candidate.after_state ->> 'resolver') = ",
            );
            builder.push_bind(&resolver.address);
        }
        ResolverArm::PointerCleared => {
            builder.push(
                " AND candidate.event_kind = 'ResolverChanged' \
                 AND lower(candidate.before_state ->> 'resolver') = ",
            );
            builder.push_bind(&resolver.address);
        }
    }
    // Priority exclusions: the set arm drops rows the emitted arm claims, and
    // the cleared arm drops rows either earlier arm claims. `IS DISTINCT FROM`
    // keeps a row whose other resolver value is missing.
    match arm {
        ResolverArm::Emitted => {}
        ResolverArm::PointerSet => {
            builder.push(
                " AND lower(candidate.raw_fact_ref ->> 'emitting_address') \
                 IS DISTINCT FROM ",
            );
            builder.push_bind(&resolver.address);
        }
        ResolverArm::PointerCleared => {
            builder.push(
                " AND lower(candidate.raw_fact_ref ->> 'emitting_address') \
                 IS DISTINCT FROM ",
            );
            builder.push_bind(&resolver.address);
            builder.push(" AND lower(candidate.after_state ->> 'resolver') IS DISTINCT FROM ");
            builder.push_bind(&resolver.address);
        }
    }
    builder.push(match block_part {
        ResolverBlockPart::Any => "",
        ResolverBlockPart::WithBlock => " AND candidate.block_number IS NOT NULL",
        ResolverBlockPart::WithoutBlock => " AND candidate.block_number IS NULL",
    });
    builder.push(" AND candidate.consumer_visibility = 'activated'");
    builder.push(CANONICAL_ROW);
    let Some(bound) = page_bound else {
        return;
    };
    push_resolver_bounded_filters(builder, filter);
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
    builder.push(" ORDER BY ");
    push_resolver_arm_order_terms(builder, bound.order, arm, block_part);
    builder.push(" LIMIT ");
    builder.push_bind(bound.limit);
    builder.push(")");
}

/// The page order within one arm part. The arm fixes `chain_id` to one value,
/// so the page order's chain term cannot break a tie there and is left out. In
/// the part with blocks, no row has a null block, so the null placement of the
/// block terms cannot change the order of the rows, which is the page order. The
/// pointer arms take the placement of their index, whose keys include the block
/// hash. The emitted arm keeps the page's placement, which its index's block
/// number shares. Its index has no block hash key. Each arm's index supplies
/// only a prefix of this order: the block number for the emitted arm, and the
/// block number and block hash for the pointer arms. So an arm sorts any group
/// of its rows that share that prefix before its limit. In the part without a
/// block, every row shares the prefix.
fn push_resolver_arm_order_terms(
    builder: &mut QueryBuilder<'_, Postgres>,
    order: HistoryOrder,
    arm: ResolverArm,
    block_part: ResolverBlockPart,
) {
    if block_part == ResolverBlockPart::WithoutBlock {
        // Every row here has the arm's resolver value and no block, so these
        // leading keys are one value each. For the pointer arms they repeat
        // the index's keys after the chain, which the arm fixes, in the
        // index's own direction. For the emitted arm only the address matches
        // the index, whose block key is descending. Every row shares these
        // keys, so in every arm the part still sorts all of its rows.
        builder.push(match arm {
            ResolverArm::Emitted => {
                "lower(candidate.raw_fact_ref ->> 'emitting_address'), \
                 candidate.block_number, "
            }
            ResolverArm::PointerSet => {
                "lower(candidate.after_state ->> 'resolver'), \
                 candidate.block_number, "
            }
            ResolverArm::PointerCleared => {
                "lower(candidate.before_state ->> 'resolver'), \
                 candidate.block_number, "
            }
        });
    }
    builder.push(match (order, block_part) {
        (_, ResolverBlockPart::WithoutBlock) => match order {
            HistoryOrder::Desc => {
                "candidate.block_hash DESC NULLS LAST, \
                     candidate.transaction_index DESC NULLS LAST, \
                     candidate.log_index DESC NULLS LAST, \
                     candidate.event_identity DESC"
            }
            HistoryOrder::Asc => {
                "candidate.block_hash ASC NULLS FIRST, \
                     candidate.transaction_index ASC NULLS FIRST, \
                     candidate.log_index ASC NULLS FIRST, \
                     candidate.event_identity ASC"
            }
        },
        (HistoryOrder::Desc, _) if arm == ResolverArm::Emitted => {
            "candidate.block_number DESC NULLS LAST, \
             candidate.block_hash DESC NULLS LAST, \
             candidate.transaction_index DESC NULLS LAST, \
             candidate.log_index DESC NULLS LAST, \
             candidate.event_identity DESC"
        }
        (HistoryOrder::Asc, _) if arm == ResolverArm::Emitted => {
            "candidate.block_number ASC NULLS FIRST, \
             candidate.block_hash ASC NULLS FIRST, \
             candidate.transaction_index ASC NULLS FIRST, \
             candidate.log_index ASC NULLS FIRST, \
             candidate.event_identity ASC"
        }
        (HistoryOrder::Desc, _) => {
            "candidate.block_number DESC NULLS FIRST, \
             candidate.block_hash DESC NULLS FIRST, \
             candidate.transaction_index DESC NULLS LAST, \
             candidate.log_index DESC NULLS LAST, \
             candidate.event_identity DESC"
        }
        (HistoryOrder::Asc, _) => {
            "candidate.block_number ASC NULLS LAST, \
             candidate.block_hash ASC NULLS LAST, \
             candidate.transaction_index ASC NULLS FIRST, \
             candidate.log_index ASC NULLS FIRST, \
             candidate.event_identity ASC"
        }
    });
}

/// Every outer page condition that can drop a `candidate` row, pushed before
/// the arm limit. The arms serve no read with a selector or a registration
/// ([`resolver_arm_source`]), the two conditions that cannot be pushed here.
/// So no row that this part drops could have passed the outer page, and no
/// passing row is cut by the limit in favour of a dropped one.
fn push_resolver_bounded_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
) {
    if let Some(namespace) = filter.namespace.as_ref() {
        builder.push(" AND candidate.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(contract_address) = filter.contract_address.as_ref() {
        builder.push(" AND lower(candidate.raw_fact_ref ->> 'emitting_address') = ");
        builder.push_bind(contract_address);
    }
    if filter.match_no_events {
        builder.push(" AND FALSE");
    }
    if let Some(key) = filter.record_key.as_ref() {
        builder.push(
            " AND (candidate.event_kind = 'RecordVersionChanged' \
             OR (candidate.event_kind = 'RecordChanged' \
             AND candidate.after_state ->> 'record_key' = ",
        );
        builder.push_bind(key);
        builder.push("))");
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
        if window.ranges.is_empty() {
            builder.push(" AND FALSE");
        } else {
            builder.push(" AND (");
            for (index, range) in window.ranges.iter().enumerate() {
                if index > 0 {
                    builder.push(" OR ");
                }
                builder.push("(candidate.chain_id = ");
                builder.push_bind(&range.chain_id);
                if let Some(from_block) = range.from_block {
                    builder.push(" AND candidate.block_number >= ");
                    builder.push_bind(from_block);
                }
                if let Some(to_block) = range.to_block {
                    builder.push(" AND candidate.block_number <= ");
                    builder.push_bind(to_block);
                }
                if range.from_block.is_none() && range.to_block.is_none() {
                    builder.push(" AND candidate.block_number IS NOT NULL");
                }
                builder.push(")");
            }
            builder.push(")");
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
