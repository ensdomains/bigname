use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use super::{HistoryBlockWindow, selectors::HistorySelector};

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
) {
    match selector {
        HistorySelector::LogicalNames(logical_name_ids) => {
            push_string_filter(builder, "ne.logical_name_id", logical_name_ids);
        }
        HistorySelector::Resources(resource_ids) => {
            builder.push("(");
            push_uuid_filter(builder, "ne.resource_id", resource_ids);
            push_attributed_record_filter(builder, "ne", resource_ids);
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
            push_attributed_record_filter(builder, "ne", resource_ids);
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

/// Node-keyed record observations carry no logical name or resource of their own; Project
/// attributes them to a resource through its selected resolver pointer and publishes the
/// attributed event ids in the record inventory provenance. Resource-scoped history reads them
/// back through that provenance so a name's history lists the same writes its records serve.
pub(super) fn push_attributed_record_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    row_alias: &str,
    resource_ids: &'a [Uuid],
) {
    push_attributed_record_filter_where(builder, row_alias, resource_ids, |_| {});
}

/// [`push_attributed_record_filter`] with a further predicate on the attributing `inventory`
/// row, for readers that admit attribution through some of the candidate resources only.
pub(super) fn push_attributed_record_filter_where<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    row_alias: &str,
    resource_ids: &'a [Uuid],
    push_inventory_predicate: impl FnOnce(&mut QueryBuilder<'a, Postgres>),
) {
    builder.push(" OR ");
    builder.push(row_alias);
    builder.push(
        r#".normalized_event_id IN (
            SELECT attributed.event_id::bigint
            FROM bigname_phase.record_inventory_current inventory
            CROSS JOIN LATERAL jsonb_array_elements_text(
                CASE WHEN jsonb_typeof(inventory.provenance -> 'attributed_event_ids') = 'array'
                     THEN inventory.provenance -> 'attributed_event_ids'
                     ELSE '[]'::jsonb END
            ) attributed(event_id)
            WHERE inventory.resource_id = ANY("#,
    );
    builder.push_bind(resource_ids);
    builder.push(
        r#"::uuid[])
              AND attributed.event_id ~ '^[0-9]+$'"#,
    );
    push_inventory_predicate(builder);
    builder.push(")");
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
