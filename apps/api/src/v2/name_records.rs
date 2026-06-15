use std::collections::BTreeMap;

use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ExecutionOutcome, ResolutionRecordKey, build_resolution_verified_state};

use super::{
    Resolver, Status, V2Error, V2Result,
    name_record::{
        record_addresses, record_content_hash, record_text_records, record_value_string, resolver,
        string_field, value_to_string,
    },
    name_records_inventory::{
        RecordInventory, inventory_item_for_record, inventory_summary, unsupported_family_reason,
    },
};

const INDEXED_INVENTORY_UNAVAILABLE_REASON: &str = "inventory_not_available";
const VERIFIED_NOT_SUPPORTED_REASON: &str = "verified_records_not_supported";
const VERIFIED_NOT_AVAILABLE_REASON: &str = "verified_records_not_available";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NameRecords {
    pub(crate) resolver: Option<Resolver>,
    pub(crate) addresses: BTreeMap<String, String>,
    pub(crate) text_records: BTreeMap<String, String>,
    pub(crate) content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) records: Option<BTreeMap<String, RecordAnswer>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) inventory: Option<RecordInventory>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RecordAnswer {
    pub(crate) status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unsupported_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
}

pub(crate) enum VerifiedRecordLookup {
    Found(Box<ExecutionOutcome>),
    CacheMiss,
    NotSupported,
}

pub(crate) fn build_indexed_name_records(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: Option<&[ResolutionRecordKey]>,
    include_inventory: bool,
) -> NameRecords {
    let record_answers = requested_records.map(|records| {
        records
            .iter()
            .map(|record| {
                (
                    record.record_key.clone(),
                    indexed_record_answer(record_inventory, record),
                )
            })
            .collect::<BTreeMap<_, _>>()
    });
    let values = match requested_records {
        Some(records) => RecordValues::from_answers(
            records,
            record_answers
                .as_ref()
                .expect("requested indexed records must build an answer map"),
        ),
        None => RecordValues {
            addresses: record_addresses(record_inventory),
            text_records: record_text_records(record_inventory),
            content_hash: record_content_hash(record_inventory),
        },
    };

    NameRecords {
        resolver: resolver(&row.declared_summary),
        addresses: values.addresses,
        text_records: values.text_records,
        content_hash: values.content_hash,
        records: record_answers,
        inventory: include_inventory
            .then(|| inventory_summary(record_inventory, requested_records)),
    }
}

pub(crate) fn indexed_records_satisfy_request(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: &[ResolutionRecordKey],
) -> bool {
    if requested_records.is_empty() {
        return true;
    }
    if terminal_no_declared_resolver(row) {
        return true;
    }
    if !indexed_inventory_is_authoritative(record_inventory) {
        return false;
    }

    requested_records.iter().all(|record| {
        let answer = indexed_record_answer(record_inventory, record);
        matches!(answer.status, Status::Ok | Status::NotFound)
    })
}

pub(crate) fn build_verified_name_records(
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    requested_records: Option<&[ResolutionRecordKey]>,
    verified_lookup: Option<VerifiedRecordLookup>,
    include_inventory: bool,
) -> V2Result<NameRecords> {
    let records = requested_records
        .map(|records| verified_record_answers(row, records, verified_lookup))
        .transpose()?;
    let values = requested_records
        .zip(records.as_ref())
        .map(|(records, answers)| RecordValues::from_answers(records, answers))
        .unwrap_or_default();

    Ok(NameRecords {
        resolver: resolver(&row.declared_summary),
        addresses: values.addresses,
        text_records: values.text_records,
        content_hash: values.content_hash,
        records,
        inventory: include_inventory
            .then(|| inventory_summary(record_inventory, requested_records)),
    })
}

#[derive(Default)]
struct RecordValues {
    addresses: BTreeMap<String, String>,
    text_records: BTreeMap<String, String>,
    content_hash: Option<String>,
}

impl RecordValues {
    fn from_answers(
        records: &[ResolutionRecordKey],
        answers: &BTreeMap<String, RecordAnswer>,
    ) -> Self {
        let mut values = Self::default();
        for record in records {
            let Some(value) = answers
                .get(&record.record_key)
                .filter(|answer| answer.status == Status::Ok)
                .and_then(|answer| answer.value.as_ref())
                .and_then(value_to_string)
            else {
                continue;
            };

            match record.record_family.as_str() {
                "addr" => {
                    if let Some(coin_type) = record.selector_key.clone() {
                        values.addresses.insert(coin_type, value);
                    }
                }
                "text" => {
                    if let Some(key) = record.selector_key.clone() {
                        values.text_records.insert(key, value);
                    }
                }
                "avatar" => {
                    values.text_records.insert("avatar".to_owned(), value);
                }
                "contenthash" => {
                    values.content_hash = Some(value);
                }
                _ => {}
            }
        }
        values
    }
}

fn indexed_record_answer(
    record_inventory: Option<&RecordInventoryCurrentRow>,
    record: &ResolutionRecordKey,
) -> RecordAnswer {
    let Some(record_inventory) = record_inventory else {
        return unsupported_answer(INDEXED_INVENTORY_UNAVAILABLE_REASON);
    };

    if let Some(entry) = indexed_entry_for_record(record_inventory, record) {
        let answer = answer_from_inventory_entry(entry);
        if record.record_key != "avatar" || answer.status == Status::Ok {
            return answer;
        }
    }

    if record.record_key == "avatar" {
        let text_avatar = ResolutionRecordKey {
            record_key: "text:avatar".to_owned(),
            record_family: "text".to_owned(),
            selector_key: Some("avatar".to_owned()),
        };
        if let Some(entry) = indexed_entry_for_record(record_inventory, &text_avatar) {
            let answer = answer_from_inventory_entry(entry);
            if answer.status == Status::Ok {
                return answer;
            }
        }
    }

    if let Some(gap) = inventory_item_for_record(&record_inventory.explicit_gaps, record) {
        return RecordAnswer {
            status: Status::NotFound,
            value: None,
            unsupported_reason: None,
            failure_reason: string_field(gap.get("gap_reason")),
        };
    }

    if let Some(reason) = unsupported_family_reason(record_inventory, &record.record_family) {
        return unsupported_answer(&reason);
    }

    not_found_answer(None)
}

fn indexed_entry_for_record<'a>(
    record_inventory: &'a RecordInventoryCurrentRow,
    record: &ResolutionRecordKey,
) -> Option<&'a Value> {
    inventory_item_for_record(&record_inventory.entries, record)
}

fn answer_from_inventory_entry(entry: &Value) -> RecordAnswer {
    let status = string_field(entry.get("status")).unwrap_or_else(|| "unsupported".to_owned());
    match status.as_str() {
        "success" => RecordAnswer {
            status: Status::Ok,
            value: record_value_string(entry).map(Value::String),
            unsupported_reason: None,
            failure_reason: None,
        },
        "not_found" => not_found_answer(string_field(entry.get("failure_reason"))),
        "unsupported" => unsupported_answer(
            &string_field(entry.get("unsupported_reason"))
                .unwrap_or_else(|| "record_not_supported".to_owned()),
        ),
        "execution_failed" | "failed" => failed_answer(
            string_field(entry.get("failure_reason"))
                .unwrap_or_else(|| "record_read_failed".to_owned()),
        ),
        _ => failed_answer("record_read_failed"),
    }
}

fn verified_record_answers(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
    verified_lookup: Option<VerifiedRecordLookup>,
) -> V2Result<BTreeMap<String, RecordAnswer>> {
    match verified_lookup {
        Some(VerifiedRecordLookup::Found(outcome)) => {
            let state = build_resolution_verified_state(row, records, Some(outcome.as_ref()))
                .map_err(|error| V2Error::internal_error(error.to_string()))?;
            Ok(verified_queries_from_state(&state, records))
        }
        Some(VerifiedRecordLookup::CacheMiss) => {
            let supported = supported_verified_record_keys(row, records);
            Ok(records
                .iter()
                .map(|record| {
                    let answer = if supported.contains(&record.record_key) {
                        failed_answer(VERIFIED_NOT_AVAILABLE_REASON)
                    } else {
                        unsupported_answer(VERIFIED_NOT_SUPPORTED_REASON)
                    };
                    (record.record_key.clone(), answer)
                })
                .collect())
        }
        Some(VerifiedRecordLookup::NotSupported) | None => Ok(records
            .iter()
            .map(|record| {
                (
                    record.record_key.clone(),
                    unsupported_answer(VERIFIED_NOT_SUPPORTED_REASON),
                )
            })
            .collect()),
    }
}

fn verified_queries_from_state(
    state: &Value,
    records: &[ResolutionRecordKey],
) -> BTreeMap<String, RecordAnswer> {
    let queries = state
        .get("verified_queries")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|query| {
            let record_key = string_field(query.get("record_key"))?;
            Some((record_key, verified_answer_from_query(query)))
        })
        .collect::<BTreeMap<_, _>>();

    records
        .iter()
        .map(|record| {
            (
                record.record_key.clone(),
                queries
                    .get(&record.record_key)
                    .cloned()
                    .unwrap_or_else(|| unsupported_answer(VERIFIED_NOT_SUPPORTED_REASON)),
            )
        })
        .collect()
}

fn verified_answer_from_query(query: &Value) -> RecordAnswer {
    let status = string_field(query.get("status")).unwrap_or_else(|| "unsupported".to_owned());
    match status.as_str() {
        "success" => RecordAnswer {
            status: Status::Ok,
            value: query
                .get("value")
                .and_then(verified_value_string)
                .map(Value::String),
            unsupported_reason: None,
            failure_reason: None,
        },
        "not_found" => not_found_answer(string_field(query.get("failure_reason"))),
        "unsupported" => unsupported_answer(
            &string_field(query.get("unsupported_reason"))
                .unwrap_or_else(|| VERIFIED_NOT_SUPPORTED_REASON.to_owned()),
        ),
        "execution_failed" | "failed" => failed_answer(
            string_field(query.get("failure_reason"))
                .unwrap_or_else(|| "verified_record_read_failed".to_owned()),
        ),
        _ => failed_answer("verified_record_read_failed"),
    }
}

fn verified_value_string(value: &Value) -> Option<String> {
    value
        .get("value")
        .and_then(value_to_string)
        .or_else(|| value_to_string(value))
}

fn supported_verified_record_keys(
    row: &NameCurrentRow,
    records: &[ResolutionRecordKey],
) -> std::collections::BTreeSet<String> {
    bigname_storage::supported_resolution_verified_readback_records(row, records)
        .into_iter()
        .map(|record| record.record_key)
        .collect()
}

fn indexed_inventory_is_authoritative(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> bool {
    let Some(record_inventory) = record_inventory else {
        return false;
    };
    string_field(record_inventory.coverage.get("unsupported_reason")).is_none()
        && string_field(record_inventory.coverage.get("status")).as_deref() == Some("full")
}

fn terminal_no_declared_resolver(row: &NameCurrentRow) -> bool {
    let Some(resolver) = row
        .declared_summary
        .get("resolver")
        .filter(|value| value.is_object())
    else {
        return false;
    };
    if string_field(resolver.get("status")).as_deref() == Some("unsupported") {
        return false;
    }

    string_field(resolver.get("chain_id")).is_none()
        && string_field(resolver.get("address")).is_none()
}

fn not_found_answer(failure_reason: Option<String>) -> RecordAnswer {
    RecordAnswer {
        status: Status::NotFound,
        value: None,
        unsupported_reason: None,
        failure_reason,
    }
}

fn unsupported_answer(reason: &str) -> RecordAnswer {
    RecordAnswer {
        status: Status::Unsupported,
        value: None,
        unsupported_reason: Some(reason.to_owned()),
        failure_reason: None,
    }
}

fn failed_answer(reason: impl Into<String>) -> RecordAnswer {
    RecordAnswer {
        status: Status::Failed,
        value: None,
        unsupported_reason: None,
        failure_reason: Some(reason.into()),
    }
}
