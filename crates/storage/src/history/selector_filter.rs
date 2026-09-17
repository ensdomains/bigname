use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use super::selectors::HistorySelector;

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
            push_attributed_record_filter(builder, resource_ids);
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
            push_attributed_record_filter(builder, resource_ids);
            builder.push(")");
        }
        HistorySelector::ProductRegistration {
            logical_name_ids: _,
            resource_ids: _,
        } => {
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
fn push_attributed_record_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource_ids: &'a [Uuid],
) {
    builder.push(
        r#"
        OR ne.normalized_event_id IN (
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
              AND attributed.event_id ~ '^[0-9]+$'
        )"#,
    );
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
