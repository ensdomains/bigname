use sqlx::{Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter,
    filters::{push_attributed_record_filter, push_string_filter},
};

/// Push `FROM … WHERE <visibility>` for a history read. A registration-scoped product read
/// draws its rows from a bounded candidate set (the registration's names, its resources, and
/// the record writes attributed to those resources) so each arm keeps an index-keyed scan;
/// every other read draws from `normalized_events` directly.
pub(super) fn push_history_source_for_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
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
