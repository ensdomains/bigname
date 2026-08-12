use sqlx::{Postgres, QueryBuilder};

pub(super) fn push_history_source(
    builder: &mut QueryBuilder<'_, Postgres>,
    include_cursor_row: bool,
) {
    push_history_source_with_visibility(builder, include_cursor_row, false);
}

pub(super) fn push_history_source_with_visibility(
    builder: &mut QueryBuilder<'_, Postgres>,
    include_cursor_row: bool,
    include_candidates: bool,
) {
    builder.push(" FROM normalized_events ne ");
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
