use sqlx::{Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter, paging::push_history_filters, source::push_history_source_for_filter,
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
    // Handoffs have no separate original. Pick one matching copy, retaining the
    // sole clear in a resource-scoped request and keeping selection before paging.
    // NOT LIKE also exposes marker selectivity to the global lineage-join planner.
    builder.push(aliased(
        r#"
        AND (@ROW@.event_identity NOT LIKE '%:ResolverChanged:registry-fallback-handoff:%'
             OR @ROW@.event_identity = (
                 WITH handoff AS (
                     SELECT @ROW@.chain_id, @ROW@.block_number, @ROW@.block_hash,
                            @ROW@.after_state ->> 'node' AS node,
                            split_part(@ROW@.event_identity,
                                ':ResolverChanged:registry-fallback-handoff:', 1) AS origin
                 )
                 SELECT min(ne.event_identity)
        "#,
        row_alias,
    ));
    // Handoffs come from RawLogInput, whose chain and block number are required.
    // Equality bounds use the existing chain/block index before matching origin.
    push_history_source_for_filter(builder, filter, canonical_only, false, false);
    builder.push(
        r#"
        AND ne.event_kind = 'ResolverChanged'
        AND strpos(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:') > 0
        AND ne.chain_id = (SELECT chain_id FROM handoff)
        AND ne.block_number = (SELECT block_number FROM handoff)
        AND ne.block_hash IS NOT DISTINCT FROM (SELECT block_hash FROM handoff)
        AND ne.after_state ->> 'node' IS NOT DISTINCT FROM (SELECT node FROM handoff)
        AND split_part(ne.event_identity, ':ResolverChanged:registry-fallback-handoff:', 1)
            = (SELECT origin FROM handoff)
        "#,
    );
    push_history_filters(builder, filter, canonical_only);
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
    // A named registrar snapshot describes retained state, not another on-chain action.
    // Filter before keyset pagination; diagnostics bypasses this product-only filter.
    builder.push(aliased(" AND NOT (@ROW@.after_state @> '{\"state_derived\":true,\"registrar_surface_snapshot\":true}'::jsonb)", row_alias));
    // An ENSv2 registry `ExpiryUpdated` whose token the adapter holds no state for is kept for
    // the registry entry row only: it has no name and no resource to describe. Product reads
    // neither list nor count it; diagnostics bypasses this filter.
    builder.push(aliased(
        " AND NOT (@ROW@.event_kind = 'ExpiryChanged' AND @ROW@.logical_name_id IS NULL \
           AND @ROW@.resource_id IS NULL \
           AND @ROW@.after_state @> '{\"token_state_absent\":true}'::jsonb)",
        row_alias,
    ));
    // Wrapping has a canonical authority row, including a positively matched mint. Its
    // derived transfer is an ownership effect only when both owners are retained, nonzero
    // and different; independent ERC-1155 transfers keep their existing behavior.
    builder.push(aliased(
        r#" AND NOT (@ROW@.event_kind = 'TokenControlTransferred'
        AND @ROW@.source_family = 'ens_v1_wrapper_l1'
        AND (@ROW@.after_state @> '{"source_event":"NameWrapped"}'::jsonb
             OR @ROW@.after_state @> '{"source_event":"TransferSingle","wrapper_mint":true,
                "matched_wrapper_completion":{"source_event":"NameWrapped"}}'::jsonb)
        AND NOT COALESCE(
          @ROW@.before_state ->> 'from' ~ '^0x[0-9a-fA-F]{40}$'
          AND @ROW@.after_state ->> 'to' ~ '^0x[0-9a-fA-F]{40}$'
          AND lower(@ROW@.before_state ->> 'from') <> '0x0000000000000000000000000000000000000000'
          AND lower(@ROW@.after_state ->> 'to') <> '0x0000000000000000000000000000000000000000'
          AND lower(@ROW@.before_state ->> 'from') <> lower(@ROW@.after_state ->> 'to'), false))"#,
        row_alias,
    ));
    super::address_pairs::push_address_pair_filter_for_alias(builder, row_alias);
    // Registry read copies retain the original control-resource representation.
    // NOT LIKE rather than strpos(...) = 0: PostgreSQL has no statistics for the strpos
    // result and estimates the equality at 0.5% of rows, so a page read looked 200 times
    // smaller than it is and the planner sorted every matching row instead of reading the
    // order index up to the page limit. The marker has no LIKE wildcard or escape character.
    builder.push(aliased(
        " AND @ROW@.event_identity NOT LIKE '%:ResolverChanged:registry-read:%'",
        row_alias,
    ));
}

fn aliased(sql: &str, row_alias: &str) -> String {
    sql.replace("@ROW@.", &format!("{row_alias}."))
}

#[cfg(test)]
#[path = "duplicates_tests.rs"]
mod tests;
