//! The source of a registration-anchored product read. Its candidate arms are
//! [`super::source`]'s, unchanged. Only the lineage join differs: the registration filter puts
//! subqueries on the candidate rows, and with a plain join the planner then scanned or hashed the
//! whole lineage of the chain instead of probing it per candidate row. So the join here is
//! `push_history_lineage_probe`.

use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use super::{
    EventHistoryReadFilter,
    filters::{push_attributed_record_filter, push_string_filter},
    source::push_history_lineage_join,
};

/// Push `FROM … WHERE <visibility>` for a read with a product registration selector.
pub(super) fn push_registration_history_source<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    logical_name_ids: &'a [String],
    resource_ids: &'a [Uuid],
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
) {
    builder.push(
        " FROM (SELECT ne.*, rb.block_timestamp AS rb_block_timestamp, \
         rb.canonicality_state AS rb_canonicality_state FROM (",
    );
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
    push_history_lineage_join(builder);
    builder.push(" OFFSET 0) ne ");
    if include_cursor_row {
        builder.push(" CROSS JOIN history_cursor_row cursor_row ");
    }
    builder.push(
        " CROSS JOIN LATERAL (SELECT ne.rb_block_timestamp AS block_timestamp, \
         ne.rb_canonicality_state AS canonicality_state) rb ",
    );
    if include_candidates {
        builder.push(" WHERE TRUE ");
    } else {
        builder.push(" WHERE ne.consumer_visibility = 'activated' ");
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
