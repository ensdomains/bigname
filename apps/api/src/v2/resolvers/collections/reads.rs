use std::collections::BTreeMap;

use super::read_error;
use crate::v2::{HistoryEventType, V2Result, history_event_type};
use serde_json::{Value, json};
use sqlx::Row;

pub(super) async fn page(
    pool: &sqlx::PgPool,
    chain: &str,
    address: &str,
    section: &str,
    (height, publication_block_bounds): (i64, &BTreeMap<String, i64>),
    key: Option<&(String, String)>,
    page_size: u64,
) -> V2Result<(Vec<(String, String, Value)>, u64)> {
    let source = if section == "roles" {
        format!(
            r#"WITH items AS (
            SELECT pc.subject AS key1, pc.resource_id::text AS key2,
                jsonb_strip_nulls(jsonb_build_object('address', pc.subject,
                    'registration_id', pc.resource_id, 'powers', pc.effective_powers,
                    'record_resource_selector', pc.scope_detail -> 'resource_selector',
                    'event_ids',
                    COALESCE(pc.provenance -> 'normalized_event_ids', '[]'::jsonb))) AS item
            FROM bigname_phase.permissions_current pc
            WHERE pc.scope_kind = 'resolver'
              AND pc.scope_detail ->> 'chain_id' = $1
              AND lower(pc.scope_detail ->> 'resolver_address') = $2
              AND (pc.chain_positions ->> 'target_block_number')::bigint <= $3
              AND jsonb_array_length(pc.effective_powers) > 0
              {}
        )"#,
            bigname_storage::DEFAULT_PERMISSIONS_CURRENT_READ_FILTER
        )
    } else {
        include_str!("aliases.sql")
            .replace(
                "{{name_lineage_joins}}",
                bigname_storage::DEFAULT_NAME_CURRENT_LINEAGE_JOINS,
            )
            .replace(
                "{{name_read_filter}}",
                bigname_storage::DEFAULT_NAME_CURRENT_READ_FILTER,
            )
    };
    let query = format!(
        r#"{source}, selected_page AS (
        SELECT key1, key2, item FROM items
        WHERE $4::text IS NULL OR (key1, key2) > ($4, $5)
        ORDER BY key1, key2 LIMIT $6
    ) SELECT (SELECT count(*) FROM items) AS total,
        COALESCE((SELECT jsonb_agg(jsonb_build_object('key1', key1, 'key2', key2, 'item', item)
                    ORDER BY key1, key2) FROM selected_page), '[]'::jsonb) AS rows"#
    );
    let row = sqlx::query(&query)
        .bind(chain)
        .bind(address)
        .bind(height)
        .bind(key.map(|k| k.0.as_str()))
        .bind(key.map(|k| k.1.as_str()))
        .bind(page_size.saturating_add(1) as i64)
        .fetch_one(pool)
        .await
        .map_err(|error| {
            tracing::error!(?error, "resolver collection read failed");
            read_error()
        })?;
    let total: i64 = row.try_get("total").map_err(|_| read_error())?;
    let rows: Value = row.try_get("rows").map_err(|_| read_error())?;
    let mut result = rows
        .as_array()
        .ok_or_else(read_error)?
        .iter()
        .map(|row| {
            Ok((
                row["key1"].as_str().ok_or_else(read_error)?.to_owned(),
                row["key2"].as_str().ok_or_else(read_error)?.to_owned(),
                row["item"].clone(),
            ))
        })
        .collect::<V2Result<Vec<_>>>()?;
    if section == "roles" {
        attach_grants(pool, &mut result, height, publication_block_bounds).await?;
    }
    Ok((result, total as u64))
}

async fn attach_grants(
    pool: &sqlx::PgPool,
    rows: &mut [(String, String, Value)],
    height: i64,
    publication_block_bounds: &BTreeMap<String, i64>,
) -> V2Result<()> {
    let ids = rows
        .iter()
        .flat_map(|(_, _, item)| {
            item["event_ids"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_i64)
        })
        .collect::<Vec<_>>();
    let events = bigname_storage::load_history_events_by_ids(pool, &ids)
        .await
        .map_err(|_| read_error())?;
    let registrations = rows
        .iter()
        .map(|(_, id, _)| id.parse::<sqlx::types::Uuid>().map_err(|_| read_error()))
        .collect::<V2Result<Vec<_>>>()?;
    let names = bigname_storage::load_current_names_by_resource_ids(pool, &registrations)
        .await
        .map_err(|_| read_error())?;
    let nameless = registrations
        .iter()
        .filter(|id| !names.contains_key(id))
        .copied()
        .collect::<Vec<_>>();
    let leases = bigname_storage::load_registry_permission_registration_map(
        pool,
        &nameless,
        None,
        publication_block_bounds,
    )
    .await
    .map_err(|_| read_error())?;
    for ((_, _, item), registration) in rows.iter_mut().zip(registrations) {
        if let Some(name) = names.get(&registration) {
            item["name"] = json!(name.normalized_name);
            item["registration_id"] = json!(crate::v2::address_names::permission_resource_handle(
                Some(name),
                registration,
            ));
        } else if let Some(lease) = leases.get(&registration) {
            item["registration_id"] = json!(lease);
        }
        let ids = item
            .as_object_mut()
            .ok_or_else(read_error)?
            .remove("event_ids")
            .unwrap_or(json!([]));
        let address = item["address"].as_str().ok_or_else(read_error)?;
        let grant = events
            .iter()
            .filter(|event| {
                ids.as_array()
                    .is_some_and(|ids| ids.contains(&json!(event.normalized_event_id)))
                    && history_event_type(&event.event_kind) == Some(HistoryEventType::Permission)
                    && event
                        .after_state
                        .get("subject")
                        .and_then(Value::as_str)
                        .is_some_and(|subject| subject.eq_ignore_ascii_case(address))
                    && event.block_number.is_some_and(|number| number <= height)
            })
            .min_by_key(|event| {
                (
                    event.block_number,
                    event.log_index,
                    event.normalized_event_id,
                )
            });
        if let Some(grant) = grant {
            item["grant_event"] = super::super::role_grants::role_grant_event_value(grant);
        }
    }
    Ok(())
}
