use sqlx::{Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter, count_contract, paging::push_history_filters,
    resolver_source::push_history_source_for_filter,
};

pub(super) fn push_product_history_duplicate_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
) {
    push_product_history_duplicate_filter_for_alias(builder, filter, canonical_only, "ne");
}

/// The same filter for a row aliased `row_alias`. The handoff subquery keeps
/// its own inner `ne`, so only the outer row references change.
pub(super) fn push_product_history_duplicate_filter_for_alias<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    row_alias: &str,
) {
    push_fixed_product_history_duplicate_filter_for_alias(builder, row_alias);
    // Retain the sole clear in a resource-scoped request and keep selection before paging.
    count_contract::push_handoff_head(builder, row_alias, "");
    let resolver_source =
        push_history_source_for_filter(builder, filter, canonical_only, false, false);
    count_contract::push_handoff_group(builder);
    push_history_filters(builder, filter, canonical_only, resolver_source);
    builder.push("))");
}

/// Duplicate rules independent of the selected anchor set. Handoff copies require exact
/// address membership first, so the address walk applies that part after bounded matching.
pub(super) fn push_fixed_product_history_duplicate_filter(
    builder: &mut QueryBuilder<'_, Postgres>,
) {
    push_fixed_product_history_duplicate_filter_for_alias(builder, "ne");
}

pub(super) fn push_fixed_product_history_duplicate_filter_for_alias(
    builder: &mut QueryBuilder<'_, Postgres>,
    row_alias: &str,
) {
    count_contract::push_fixed_duplicate_filter(builder, row_alias);
}

#[cfg(test)]
#[path = "duplicates_tests.rs"]
mod tests;
