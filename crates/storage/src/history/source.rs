use sqlx::{Postgres, QueryBuilder};

use super::{EventHistoryReadFilter, selectors::HistorySelector};

pub(super) fn push_history_source_for_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
) {
    if let Some((logical_name_ids, registration_id)) =
        filter.selectors.iter().find_map(|selector| match selector {
            HistorySelector::ProductRegistration {
                logical_name_ids,
                registration_id,
            } => Some((logical_name_ids, registration_id)),
            _ => None,
        })
    {
        builder.push(" FROM (");
        if !logical_name_ids.is_empty() {
            builder.push(
                "SELECT candidate.*\n\
                 FROM bigname_phase.normalized_events candidate\n\
                 WHERE ",
            );
            push_string_filter(builder, "candidate.logical_name_id", logical_name_ids);
            push_bounded_candidate_canonicality(builder, canonical_only);
            builder.push(
                "\nUNION ALL\n\
                 SELECT candidate.*\n\
                 FROM bigname_phase.normalized_events candidate\n\
                 WHERE candidate.resource_id = ",
            );
        } else {
            builder.push(
                "SELECT candidate.*\n\
                 FROM bigname_phase.normalized_events candidate\n\
                 WHERE candidate.resource_id = ",
            );
        }
        builder.push_bind(registration_id);
        push_bounded_candidate_canonicality(builder, canonical_only);
        if !logical_name_ids.is_empty() {
            builder.push(" AND (candidate.logical_name_id IS NULL OR NOT (");
            push_string_filter(builder, "candidate.logical_name_id", logical_name_ids);
            builder.push("))");
        }
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

fn push_string_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [String],
) {
    builder.push(column);
    builder.push(" IN (");
    let mut separated = builder.separated(", ");
    for value in values {
        separated.push_bind(value);
    }
    separated.push_unseparated(")");
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
