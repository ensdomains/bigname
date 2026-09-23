//! The row shape every normalized-event history read selects.

use sqlx::{Postgres, QueryBuilder};

use super::{
    EventHistoryReadFilter, registration_identity::push_product_registration_id,
    source::push_history_source_for_filter,
};

/// Push the history row columns and the read's `FROM … WHERE <visibility>`.
pub(super) fn push_history_select<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
    include_cursor_row: bool,
    include_candidates: bool,
) {
    push_history_columns(builder, canonical_only, include_candidates);
    push_history_source_for_filter(
        builder,
        filter,
        canonical_only,
        include_cursor_row,
        include_candidates,
    );
}

/// Push `SELECT <history row columns>` over a normalized event aliased `ne` and its block's
/// lineage row aliased `rb`, for a read that supplies its own `FROM`.
pub(super) fn push_history_columns(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
    include_candidates: bool,
) {
    builder.push(
        r#"
        SELECT
            ne.normalized_event_id,
            ne.event_identity,
            ne.namespace,
            ne.logical_name_id,
            ne.resource_id,
        "#,
    );
    push_product_registration_id(builder, canonical_only);
    builder.push(
        r#" AS registration_id,
            ne.event_kind,
            ne.source_family,
            ne.manifest_version,
            ne.source_manifest_id,
            ne.chain_id,
            ne.block_number,
            ne.block_hash,
            rb.block_timestamp,
            ne.transaction_hash,
            ne.log_index,
            ne.raw_fact_ref,
            ne.derivation_kind,
            ne.canonicality_state::TEXT AS canonicality_state,
            ne.before_state,
            ne.after_state,
        "#,
    );
    if include_candidates {
        builder.push(
            r#"
            ne.migration_correlation_ids,
            ne.consumer_visibility,
            COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'migration_correlation_ids',
                            ARRAY[association.migration_correlation_id],
                            'correlation_kind', association.correlation_kind,
                            'consumer_visibility', association.consumer_visibility
                        )
                        ORDER BY association.migration_correlation_id,
                                 association.correlation_kind,
                                 association.consumer_visibility
                    )
                    FROM migration_event_associations AS association
                    WHERE association.event_identity = ne.event_identity
                ),
                '[]'::jsonb
            ) AS migration_associations,
            "#,
        );
    } else {
        builder.push(
            r#"
            ARRAY[]::text[] AS migration_correlation_ids,
            'activated'::text AS consumer_visibility,
            '[]'::jsonb AS migration_associations,
            "#,
        );
    }
    builder.push(
        r#"
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'provenance') = 'object'
                        THEN ne.after_state -> 'provenance'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'provenance') = 'object'
                        THEN ne.before_state -> 'provenance'
                END,
                '{}'::jsonb
            ) AS provenance,
            COALESCE(
                CASE
                    WHEN jsonb_typeof(ne.after_state -> 'coverage') = 'object'
                        THEN ne.after_state -> 'coverage'
                END,
                CASE
                    WHEN jsonb_typeof(ne.before_state -> 'coverage') = 'object'
                        THEN ne.before_state -> 'coverage'
                END,
                '{}'::jsonb
            ) AS coverage
        "#,
    );
}
