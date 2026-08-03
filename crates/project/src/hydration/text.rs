use bigname_execution::{
    ChainRpcUrls, EnsTextRecordMulticallBlock, EnsTextRecordMulticallRequest,
    EnsTextRecordMulticallResult, MULTICALL3_ADDRESS, execute_ens_text_record_multicall,
};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{ETHEREUM, HYDRATION_KEY, hydration_provenance};
use crate::{Marker, ProjectError, Result};

const TEXT_VALUE_MISSING: &str = "value_not_retained_in_normalized_events";
const BATCH_SIZE: usize = 250;

pub(super) struct TextRow {
    pub(super) resource_id: String,
    pub(super) boundary_key: String,
    resolver_address: String,
    namehash: String,
    pub(super) entries: Value,
    pub(super) calls: Vec<(usize, String)>,
    pub(super) changed: bool,
}

pub(super) async fn load_candidates(pool: &PgPool) -> Result<Vec<TextRow>> {
    let rows = sqlx::query_as::<_, (String, String, String, String, Value)>(
        r#"
        SELECT row.resource_id::text,
               row.record_version_boundary_key,
               lower(row.provenance ->> 'resolver_address'),
               surface.namehash,
               row.entries
        FROM record_inventory_current row
        JOIN name_surfaces surface
          ON surface.logical_name_id = row.provenance ->> 'logical_name_id'
        WHERE row.provenance ->> 'chain_id' = $1
          AND row.support_status = 'supported'
          AND EXISTS (
              SELECT 1 FROM jsonb_array_elements(row.entries) entry
              WHERE entry ->> 'record_family' = 'text'
                AND (
                    entry ->> 'unsupported_reason' = $2
                    OR entry ? $3
                )
          )
        ORDER BY row.resource_id, row.record_version_boundary_key
        "#,
    )
    .bind(ETHEREUM)
    .bind(TEXT_VALUE_MISSING)
    .bind(HYDRATION_KEY)
    .fetch_all(pool)
    .await
    .map_err(|error| ProjectError::database("failed to load text hydration candidates", error))?;
    rows.into_iter()
        .map(
            |(resource_id, boundary_key, resolver_address, namehash, entries)| {
                let calls = entries
                    .as_array()
                    .ok_or_else(|| ProjectError::data_integrity("record entries are not an array"))?
                    .iter()
                    .enumerate()
                    .filter_map(|(index, entry)| text_key(entry).map(|key| (index, key.to_owned())))
                    .collect();
                Ok(TextRow {
                    resource_id,
                    boundary_key,
                    resolver_address,
                    namehash,
                    entries,
                    calls,
                    changed: false,
                })
            },
        )
        .collect()
}

fn text_key(entry: &Value) -> Option<&str> {
    if entry.get("record_family")?.as_str()? != "text" {
        return None;
    }
    let key = entry.get("selector_key")?.as_str()?;
    let expected = format!("text:{key}");
    let eligible = entry.get("unsupported_reason").and_then(Value::as_str)
        == Some(TEXT_VALUE_MISSING)
        || entry.get(HYDRATION_KEY).is_some();
    (eligible && !key.trim().is_empty() && entry.get("record_key")?.as_str()? == expected)
        .then_some(key)
}

pub(super) async fn hydrate(
    rpc_urls: &ChainRpcUrls,
    head: &Marker,
    rows: &mut [TextRow],
) -> Result<usize> {
    let block = EnsTextRecordMulticallBlock {
        block_number: head.number,
        block_hash: head.hash.clone(),
    };
    let references = rows
        .iter()
        .enumerate()
        .flat_map(|(row_index, row)| {
            row.calls.iter().map(move |(entry_index, key)| {
                (
                    row_index,
                    *entry_index,
                    EnsTextRecordMulticallRequest {
                        resolver_address: row.resolver_address.clone(),
                        namehash: row.namehash.clone(),
                        text_key: key.clone(),
                    },
                )
            })
        })
        .collect::<Vec<_>>();
    let mut failures = 0usize;
    for chunk in references.chunks(BATCH_SIZE) {
        let requests = chunk
            .iter()
            .map(|(_, _, call)| call.clone())
            .collect::<Vec<_>>();
        let results = match execute_ens_text_record_multicall(
            rpc_urls,
            ETHEREUM,
            MULTICALL3_ADDRESS,
            &block,
            &requests,
        )
        .await
        {
            Ok(results) => results,
            Err(error) => {
                let message = format!("text-record hydration multicall failed: {error:#}");
                requests
                    .iter()
                    .map(|_| EnsTextRecordMulticallResult::Failed {
                        message: message.clone(),
                    })
                    .collect()
            }
        };
        for ((row_index, entry_index, _), result) in chunk.iter().zip(results) {
            let Some(entry) = rows[*row_index]
                .entries
                .as_array_mut()
                .and_then(|entries| entries.get_mut(*entry_index))
            else {
                return Err(ProjectError::data_integrity(
                    "text hydration entry reference is no longer valid",
                ));
            };
            match update_entry(entry, result, head)? {
                EntryUpdate::Changed => rows[*row_index].changed = true,
                EntryUpdate::Failed { changed } => {
                    failures += 1;
                    rows[*row_index].changed |= changed;
                }
            }
        }
    }
    Ok(failures)
}

enum EntryUpdate {
    Changed,
    Failed { changed: bool },
}

fn update_entry(
    entry: &mut Value,
    result: EnsTextRecordMulticallResult,
    head: &Marker,
) -> Result<EntryUpdate> {
    let baseline = entry
        .get(HYDRATION_KEY)
        .and_then(|value| value.get("baseline"))
        .cloned()
        .unwrap_or_else(|| {
            let mut baseline = entry.clone();
            if let Some(object) = baseline.as_object_mut() {
                object.remove(HYDRATION_KEY);
            }
            baseline
        });
    if matches!(result, EnsTextRecordMulticallResult::Failed { .. }) {
        let changed = entry.get(HYDRATION_KEY).is_some();
        if changed {
            *entry = baseline;
        }
        return Ok(EntryUpdate::Failed { changed });
    }
    let object = entry.as_object_mut().ok_or_else(|| {
        ProjectError::data_integrity("text hydration projection entry is not an object")
    })?;
    match result {
        EnsTextRecordMulticallResult::Success { value } => {
            object.insert("status".to_owned(), json!("success"));
            object.insert("value".to_owned(), json!(value));
        }
        EnsTextRecordMulticallResult::NotFound => {
            object.insert("status".to_owned(), json!("not_found"));
            object.remove("value");
        }
        EnsTextRecordMulticallResult::Failed { .. } => unreachable!("handled above"),
    }
    object.remove("unsupported_reason");
    let mut provenance = hydration_provenance(head, None, None);
    provenance
        .as_object_mut()
        .expect("hydration provenance is an object")
        .insert("baseline".to_owned(), baseline);
    object.insert(HYDRATION_KEY.to_owned(), provenance);
    Ok(EntryUpdate::Changed)
}
