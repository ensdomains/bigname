use sqlx::{Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter, EventHistoryResolverFilter, HistoryOrder,
    duplicates::push_product_history_duplicate_filter_for_alias,
    filters::{push_attributed_record_filter, push_string_filter},
    keyset::push_history_cursor_after_for_alias,
};

/// How far one page read may run into each resolver candidate arm. Each arm
/// keeps at most `limit` rows with a block and at most `limit` rows without
/// one, in the page order and after the page cursor. So the three arms
/// together hand the outer page at most six page bounds of rows to
/// de-duplicate and sort, however many rows the resolver has in total.
/// `cursor_block` is the plain block bound the outer page also pushes, set
/// only when the read already excludes rows without a block.
#[derive(Clone, Copy)]
pub(super) struct ResolverPageBound {
    pub(super) order: HistoryOrder,
    pub(super) limit: i64,
    pub(super) has_cursor: bool,
    pub(super) cursor_block: Option<i64>,
}

/// Push `FROM … WHERE <visibility>` for a history read. A registration-scoped product read
/// draws its rows from a bounded candidate set (the registration's names, its resources, and
/// the record writes attributed to those resources) so each arm keeps an index-keyed scan.
/// A resolver read does the same from its three arms below. Every other read draws from
/// `normalized_events` directly.
pub(super) fn push_history_source_for_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
) {
    push_history_source_impl(
        builder,
        filter,
        canonical_only,
        include_cursor_row,
        include_candidates,
        None,
    );
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
) {
    push_history_source_impl(
        builder,
        filter,
        canonical_only,
        include_cursor_row,
        include_candidates,
        resolver_page_bound,
    );
}

fn push_history_source_impl<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
    resolver_page_bound: Option<ResolverPageBound>,
) {
    if let Some((logical_name_ids, resource_ids)) = filter.product_registration() {
        builder.push(" FROM (");
        if !logical_name_ids.is_empty() {
            builder.push(
                "SELECT candidate.*\n\
                 FROM bigname_phase.normalized_events candidate\n\
                 WHERE ",
            );
            push_string_filter(builder, "candidate.logical_name_id", logical_name_ids);
            push_bounded_candidate_canonicality(builder, canonical_only);
            builder.push("\nUNION ALL\n");
        }
        builder.push(
            "SELECT candidate.*\n\
             FROM bigname_phase.normalized_events candidate\n\
             WHERE candidate.resource_id = ANY(",
        );
        builder.push_bind(resource_ids);
        builder.push(")");
        push_bounded_candidate_canonicality(builder, canonical_only);
        push_not_in_name_arm(builder, logical_name_ids);
        // Node-keyed record writes carry neither a name nor a resource; a resolver pointer at or
        // below the read's published block attributes them to a resource (`attribution.rs`).
        builder.push(
            "\nUNION ALL\n\
             SELECT candidate.*\n\
             FROM bigname_phase.normalized_events candidate\n\
             WHERE candidate.resource_id IS NULL AND (FALSE",
        );
        push_attributed_record_filter(
            builder,
            "candidate",
            &filter.attributed_records,
            resource_ids,
        );
        builder.push(")");
        push_bounded_candidate_canonicality(builder, canonical_only);
        push_not_in_name_arm(builder, logical_name_ids);
        builder.push(") ne ");
    } else if let Some(resolver) = filter.resolver.as_ref() {
        // The resolver's own rows only: what it emitted, the pointers set to it, and the
        // pointers cleared from it. The arms are mutually exclusive by priority:
        // the set arm excludes rows the emitted arm claims, and the cleared arm
        // excludes rows both earlier arms claim. So `UNION ALL` gives the same
        // rows as the old `UNION`, as the old `OR` did, with no de-duplication
        // step. Each arm carries the chain, visibility, and canonicality
        // conditions, so each reads through its own index.
        builder.push(" FROM (");
        push_resolver_candidate_arm(
            builder,
            filter,
            resolver,
            ResolverArm::Emitted,
            canonical_only,
            include_candidates,
            resolver_page_bound,
        );
        builder.push("\nUNION ALL\n");
        push_resolver_candidate_arm(
            builder,
            filter,
            resolver,
            ResolverArm::PointerSet,
            canonical_only,
            include_candidates,
            resolver_page_bound,
        );
        builder.push("\nUNION ALL\n");
        push_resolver_candidate_arm(
            builder,
            filter,
            resolver,
            ResolverArm::PointerCleared,
            canonical_only,
            include_candidates,
            resolver_page_bound,
        );
        builder.push(") ne ");
    } else {
        builder.push(" FROM normalized_events ne ");
    }
    if include_cursor_row {
        builder.push(" CROSS JOIN history_cursor_row cursor_row ");
    }
    push_history_lineage_join(builder);
    if include_candidates {
        builder.push(" WHERE TRUE ");
    } else {
        builder.push(" WHERE ne.consumer_visibility = 'activated' ");
    }
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
    canonical_only: bool,
    include_candidates: bool,
    page_bound: Option<ResolverPageBound>,
) {
    match page_bound {
        None => push_resolver_candidate_select(
            builder,
            filter,
            resolver,
            arm,
            ResolverBlockPart::Any,
            canonical_only,
            include_candidates,
            None,
        ),
        Some(bound) => {
            // Rows with a block and rows without one sit at opposite ends of the
            // page order, and each arm's index places block nulls at the other
            // end from the page. Bounding the two parts on their own keeps the
            // page order while each part orders in a way its index can serve.
            builder.push("(");
            push_resolver_candidate_select(
                builder,
                filter,
                resolver,
                arm,
                ResolverBlockPart::WithBlock,
                canonical_only,
                include_candidates,
                Some(bound),
            );
            builder.push("\nUNION ALL\n");
            push_resolver_candidate_select(
                builder,
                filter,
                resolver,
                arm,
                ResolverBlockPart::WithoutBlock,
                canonical_only,
                include_candidates,
                Some(bound),
            );
            builder.push(")");
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn push_resolver_candidate_select<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    resolver: &'a EventHistoryResolverFilter,
    arm: ResolverArm,
    block_part: ResolverBlockPart,
    canonical_only: bool,
    include_candidates: bool,
    page_bound: Option<ResolverPageBound>,
) {
    if page_bound.is_some() {
        builder.push("(");
    }
    builder.push("SELECT candidate.*\n FROM bigname_phase.normalized_events candidate ");
    if page_bound.is_some() && canonical_only {
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
    push_resolver_candidate_conditions(builder, canonical_only, include_candidates);
    if page_bound.is_some() {
        push_resolver_bounded_filters(builder, filter, canonical_only, include_candidates);
    }
    if let Some(bound) = page_bound {
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
}

/// The page order within one arm part. The arm fixes `chain_id` to one value,
/// so the page order's chain term cannot break a tie there and is left out. In
/// the part with blocks, no row has a null block, so the block terms take the
/// null placement of that arm's index instead of the page's. The order of the
/// rows themselves is the page order either way, and each arm's index starts
/// with these block keys, so it can serve the order instead of the part
/// sorting every match.
fn push_resolver_arm_order_terms(
    builder: &mut QueryBuilder<'_, Postgres>,
    order: HistoryOrder,
    arm: ResolverArm,
    block_part: ResolverBlockPart,
) {
    if block_part == ResolverBlockPart::WithoutBlock {
        // Every row here has the arm's resolver value and no block, so these
        // leading keys are one value each. They are the arm index's own keys,
        // which keeps that index the ordered path for this part.
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

fn push_resolver_candidate_conditions(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
    include_candidates: bool,
) {
    if !include_candidates {
        builder.push(" AND candidate.consumer_visibility = 'activated'");
    }
    push_bounded_candidate_canonicality(builder, canonical_only);
}

/// Every outer page condition that can drop a `candidate` row, pushed before
/// the arm limit. The caller only bounds a page when the conditions it cannot
/// push here (a selector, a registration) are absent, so no row that this
/// part drops could have passed the outer page, and no passing row is cut
/// by the limit in favour of a dropped one.
fn push_resolver_bounded_filters<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_candidates: bool,
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
    if canonical_only {
        builder.push(
            " AND (candidate.block_hash IS NULL \
             OR candidate_lineage.canonicality_state IN (\n\
             'canonical'::bigname_phase.canonicality_state,\n\
             'safe'::bigname_phase.canonicality_state,\n\
             'finalized'::bigname_phase.canonicality_state\n\
             ))",
        );
    }
    if !include_candidates {
        push_product_history_duplicate_filter_for_alias(
            builder,
            filter,
            canonical_only,
            "candidate",
        );
    }
}

fn push_not_in_name_arm<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    logical_name_ids: &'a [String],
) {
    if !logical_name_ids.is_empty() {
        builder.push(" AND (candidate.logical_name_id IS NULL OR NOT (");
        push_string_filter(builder, "candidate.logical_name_id", logical_name_ids);
        builder.push("))");
    }
}

fn push_bounded_candidate_canonicality(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
) {
    if canonical_only {
        builder.push(
            " AND candidate.canonicality_state IN (\n\
             'canonical'::bigname_phase.canonicality_state,\n\
             'safe'::bigname_phase.canonicality_state,\n\
             'finalized'::bigname_phase.canonicality_state\n\
             )",
        );
    }
}

pub(super) fn push_history_lineage_join(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        r#"
        LEFT JOIN bigname_phase.chain_lineage rb
          ON rb.chain_id = ne.chain_id
         AND rb.block_hash = ne.block_hash
        "#,
    );
}

pub(super) fn push_history_canonicality_filter(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
) {
    if canonical_only {
        builder.push(
            r#"
            AND ne.canonicality_state IN (
                'canonical'::bigname_phase.canonicality_state,
                'safe'::bigname_phase.canonicality_state,
                'finalized'::bigname_phase.canonicality_state
            )
            AND (
                ne.block_hash IS NULL
                OR rb.canonicality_state IN (
                    'canonical'::bigname_phase.canonicality_state,
                    'safe'::bigname_phase.canonicality_state,
                    'finalized'::bigname_phase.canonicality_state
                )
            )
            "#,
        );
    }
}

pub(super) fn push_readable_anchored_row_filter(
    builder: &mut QueryBuilder<'_, Postgres>,
    row_alias: &str,
    lineage_alias: &str,
) {
    builder.push(format!(
        r#"
        AND {row_alias}.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
        AND {lineage_alias}.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
        "#,
    ));
}
