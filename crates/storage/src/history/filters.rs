use std::collections::BTreeMap;

use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use super::{
    HistoryBlockWindow, attribution::AttributedRecords, lineage::same_fork_predicate,
    selectors::HistorySelector,
};

/// Keep `alias` at or below the published block of its chain. A row on a chain the bounds do
/// not name lies above every publication, as does every row when the bounds are empty; without
/// bounds the read is not bound to a publication and nothing is pushed.
pub(super) fn push_publication_bound(
    builder: &mut QueryBuilder<'_, Postgres>,
    alias: &str,
    published: Option<&BTreeMap<String, i64>>,
) {
    let Some(bounds) = published else {
        return;
    };
    if bounds.is_empty() {
        builder.push(" AND FALSE");
        return;
    }
    builder.push(" AND (");
    for (index, (chain_id, block_number)) in bounds.iter().enumerate() {
        if index > 0 {
            builder.push(" OR ");
        }
        builder.push(format!("({alias}.chain_id = "));
        builder.push_bind(chain_id.clone());
        builder.push(format!(" AND {alias}.block_number <= "));
        builder.push_bind(*block_number);
        builder.push(")");
    }
    builder.push(")");
}

/// One inclusive block range per chain; a window without ranges matches nothing.
pub(super) fn push_history_block_window<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    window: &'a HistoryBlockWindow,
) {
    if window.ranges.is_empty() {
        builder.push(" AND FALSE");
        return;
    }
    builder.push(" AND (");
    for (index, range) in window.ranges.iter().enumerate() {
        if index > 0 {
            builder.push(" OR ");
        }
        builder.push("(ne.chain_id = ");
        builder.push_bind(&range.chain_id);
        if let Some(from_block) = range.from_block {
            builder.push(" AND ne.block_number >= ");
            builder.push_bind(from_block);
        }
        if let Some(to_block) = range.to_block {
            builder.push(" AND ne.block_number <= ");
            builder.push_bind(to_block);
        }
        if range.from_block.is_none() && range.to_block.is_none() {
            builder.push(" AND ne.block_number IS NOT NULL");
        }
        builder.push(")");
    }
    builder.push(")");
}

pub(super) fn push_selector_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    selector: &'a HistorySelector,
    attributed: &AttributedRecords,
) {
    match selector {
        HistorySelector::LogicalNames(logical_name_ids) => {
            push_string_filter(builder, "ne.logical_name_id", logical_name_ids);
        }
        HistorySelector::Resources(resource_ids) => {
            builder.push("(");
            push_uuid_filter(builder, "ne.resource_id", resource_ids);
            push_attributed_record_filter(builder, "ne", attributed, resource_ids);
            builder.push(")");
        }
        HistorySelector::LogicalNamesOrResources {
            logical_name_ids,
            resource_ids,
        } => {
            builder.push("(");
            push_string_filter(builder, "ne.logical_name_id", logical_name_ids);
            builder.push(" OR ");
            push_uuid_filter(builder, "ne.resource_id", resource_ids);
            push_attributed_record_filter(builder, "ne", attributed, resource_ids);
            builder.push(")");
        }
        // The history source already holds exactly this selector's candidate rows.
        HistorySelector::ProductRegistration { .. } => {
            builder.push("TRUE");
        }
        HistorySelector::None => {
            builder.push("FALSE");
        }
    }
}

/// Node-keyed record observations carry no logical name or resource of their own; a resolver
/// pointer attributes them to a resource. Resource-scoped history lists the writes attributed to
/// its resources at the read's published block (`attribution.rs`), so a name's history lists the
/// writes its records served.
///
/// The read loads the ids before the statement and binds them as an array. PostgreSQL can then
/// key this branch on the primary key and combine it with index scans for the other branches of
/// the selector's OR; an `IN (SELECT ...)` branch inside an OR cannot be an index condition and
/// forces a scan of every candidate row.
pub(super) fn push_attributed_record_filter(
    builder: &mut QueryBuilder<'_, Postgres>,
    row_alias: &str,
    attributed: &AttributedRecords,
    resource_ids: &[Uuid],
) {
    builder.push(" OR ");
    builder.push(row_alias);
    builder.push(".normalized_event_id = ANY(");
    builder.push_bind(attributed.event_ids_for(resource_ids));
    builder.push("::bigint[])");
}

/// [`push_attributed_record_filter`] with a further predicate on the attributing resource
/// (`attribution.resource_id`), for readers that admit attribution through some of the candidate
/// resources only. The predicate may refer to the outer row, so the subquery stays a correlated
/// `IN`.
pub(super) fn push_attributed_record_filter_where<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    row_alias: &str,
    attributed: &'a AttributedRecords,
    resource_ids: &'a [Uuid],
    push_attribution_predicate: impl FnOnce(&mut QueryBuilder<'a, Postgres>),
) {
    builder.push(" OR ");
    builder.push(row_alias);
    builder.push(
        ".normalized_event_id IN (
            SELECT attribution.normalized_event_id
            FROM unnest(",
    );
    builder.push_bind(attributed.resource_ids());
    builder.push("::uuid[], ");
    builder.push_bind(attributed.event_ids());
    builder.push(
        "::bigint[]) AS attribution(resource_id, normalized_event_id)
            WHERE attribution.resource_id = ANY(",
    );
    builder.push_bind(resource_ids);
    builder.push("::uuid[])");
    push_attribution_predicate(builder);
    builder.push(")");
}

/// The attribution predicate of a registration-scoped read: the resource that attributes a write
/// proves membership only when it is the registration itself, or a NameWrapper resource whose
/// `NameWrapped` row on the write's fork recorded this registration as the lease it wrapped. The
/// `NameWrapped` row must lie at or below the read's published block.
pub(super) fn push_attributing_resource_is_registration(
    builder: &mut QueryBuilder<'_, Postgres>,
    registration_id: Uuid,
    canonical_only: bool,
    published: Option<&BTreeMap<String, i64>>,
) {
    builder.push(" AND (attribution.resource_id = ");
    builder.push_bind(registration_id);
    // The derived table keeps the lookup keyed by the attributing resource; without it the
    // planner rewrites the EXISTS into a per-row hash of every NameWrapped row on the chain.
    builder.push(
        " OR EXISTS (
            SELECT 1
            FROM (
                SELECT * FROM bigname_phase.normalized_events wrapper_binding
                WHERE wrapper_binding.resource_id = attribution.resource_id
                  AND wrapper_binding.resource_id IS NOT NULL
                  AND wrapper_binding.consumer_visibility = 'activated'",
    );
    if canonical_only {
        builder
            .push(" AND wrapper_binding.canonicality_state IN ('canonical', 'safe', 'finalized')");
    }
    push_publication_bound(builder, "wrapper_binding", published);
    builder.push(
        " OFFSET 0
            ) wrapper_binding
            LEFT JOIN bigname_phase.chain_lineage wrapper_lineage
              ON wrapper_lineage.chain_id = wrapper_binding.chain_id
             AND wrapper_lineage.block_hash = wrapper_binding.block_hash
            WHERE wrapper_binding.chain_id = ne.chain_id
              AND wrapper_binding.event_kind = 'SurfaceBound'
              AND wrapper_binding.source_family = 'ens_v1_wrapper_l1'
              AND (wrapper_binding.after_state ->> 'wrapped_registrar_resource_id')::uuid = ",
    );
    builder.push_bind(registration_id);
    if canonical_only {
        builder.push(
            " AND (wrapper_binding.block_hash IS NULL
                   OR wrapper_lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))",
        );
    }
    builder.push(" AND ");
    builder.push(same_fork_predicate("wrapper_binding", "ne", canonical_only));
    builder.push("))");
}

pub(super) fn push_string_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [String],
) {
    builder.push(column);
    push_string_filter_tail(builder, values);
}

fn push_string_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [String]) {
    builder.push(" = ANY(");
    builder.push_bind(values);
    builder.push("::text[])");
}

fn push_uuid_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    values: &'a [Uuid],
) {
    builder.push(column);
    push_uuid_filter_tail(builder, values);
}

fn push_uuid_filter_tail<'a>(builder: &mut QueryBuilder<'a, Postgres>, values: &'a [Uuid]) {
    builder.push(" = ANY(");
    builder.push_bind(values);
    builder.push("::uuid[])");
}
