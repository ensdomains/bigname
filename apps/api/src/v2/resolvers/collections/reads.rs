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
    family_page(
        pool,
        chain,
        address,
        section,
        (height, publication_block_bounds),
        key,
        page_size,
    )
    .await
}

/// The page under the publication switch: the collection readers over the owned key families
/// (`bigname_storage::families::topology`), which read at the family marker's publication, the
/// only position the switch serves (ruling J5), with the same keys, items and totals; `/roles`
/// then attaches names, registrations and `grant_event` exactly as the served page does.
async fn family_page(
    pool: &sqlx::PgPool,
    chain: &str,
    address: &str,
    section: &str,
    (height, publication_block_bounds): (i64, &BTreeMap<String, i64>),
    key: Option<&(String, String)>,
    page_size: u64,
) -> V2Result<(Vec<(String, String, Value)>, u64)> {
    use bigname_storage::families::topology::{
        load_resolver_aliases_shadow, load_resolver_links_shadow, load_resolver_roles_shadow,
    };
    let limit = page_size.saturating_add(1) as i64;
    let loaded = match section {
        "links" => {
            let namespace = super::super::resolver_namespace(chain)?;
            load_resolver_links_shadow(pool, chain, address, namespace, key, limit).await
        }
        "roles" => load_resolver_roles_shadow(pool, chain, address, key, limit).await,
        _ => load_resolver_aliases_shadow(pool, chain, address, key, limit).await,
    }
    .map_err(crate::v2::name_rows_error(
        crate::v2::SnapshotReadResource::Resolver,
        |error| {
            tracing::error!(?error, "resolver collection read failed");
            read_error()
        },
    ))?;
    let mut rows = loaded.rows;
    if section == "roles" {
        attach_grants(pool, &mut rows, height, publication_block_bounds).await?;
    }
    Ok((rows, loaded.total_count))
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
        .map_err(crate::v2::name_rows_error(
            crate::v2::SnapshotReadResource::Resolver,
            |_| read_error(),
        ))?;
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
