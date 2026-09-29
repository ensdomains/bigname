use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot};

use crate::AppState;
use crate::v2::support::{
    PROFILE_FALLBACK_RECORD_KEYS, ResolutionRecordKey, parse_resolution_record_key,
};
use crate::v2::vocab::{
    MISSING_UNSUPPORTED_REASON, downgrades_unsupported_name, projected_row_product_reason,
};

use super::super::{
    SnapshotReadResource, Source, Status, V2Result, default_requested_records,
    name_records::{
        RecordAnswer, RecordSelection, VERIFIED_NOT_SUPPORTED_REASON, build_verified_name_records,
        ensure_default_record_limit, load_verified_record_lookup_for_resource,
    },
};
use super::{NameRecord, build_name_record, row_has_current_registration, string_field};

#[path = "verified/record_values.rs"]
mod record_values;
use record_values::VerifiedRecordValues;

pub(super) async fn build_name_record_for_source(
    state: &AppState,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    chain_id: Option<u64>,
    selected_snapshot: &mut SelectedSnapshot,
    source: Source,
) -> V2Result<NameRecord> {
    if let Some(record) = unsupported_name_record(row)? {
        return Ok(record);
    }
    match source {
        Source::Indexed => build_name_record(row, record_inventory, chain_id, Status::Ok),
        Source::Verified => {
            build_verified_name_record(state, row, record_inventory, chain_id, selected_snapshot)
                .await
        }
    }
}

fn unsupported_name_record(row: &NameCurrentRow) -> V2Result<Option<NameRecord>> {
    if string_field(row.coverage.get("status")).as_deref() != Some("unsupported") {
        return Ok(None);
    }
    let reason = string_field(row.coverage.get("unsupported_reason"))
        .filter(|reason| !reason.trim().is_empty())
        .unwrap_or_else(|| MISSING_UNSUPPORTED_REASON.to_owned());
    if !downgrades_unsupported_name(&reason) {
        return Ok(None);
    }
    let reason = projected_row_product_reason(
        &reason,
        "rejected exact-name reason containing pipeline vocabulary",
        "failed to map exact-name reason vocabulary",
    );
    Ok(Some(NameRecord {
        registration_id: None,
        token_id: None,
        owner: None,
        manager: None,
        registrant: None,
        registered_at: None,
        created_at: None,
        subregistry: None,
        expires_at: None,
        registration_status: None,
        wrapper_state: None,
        wrapper_fuses: None,
        authority: None,
        lapsed_registration: None,
        migrated_at: None,
        name: row.normalized_name.clone(),
        display_name: row.canonical_display_name.clone(),
        namespace: row.namespace.clone(),
        namehash: row.namehash.clone(),
        resolver: None,
        addresses: None,
        text_records: None,
        content_hash: None,
        primary_name: None,
        primary_address: None,
        chain_id: None,
        network: None,
        subname_count: None,
        record_count: None,
        status: Status::Unsupported,
        unsupported_reason: Some(reason),
        failure_reason: None,
        unsupported_fields: Vec::new(),
    }))
}

async fn build_verified_name_record(
    state: &AppState,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    chain_id: Option<u64>,
    selected_snapshot: &mut SelectedSnapshot,
) -> V2Result<NameRecord> {
    // Mirror build_name_record's serving guard before deriving requested records: only a
    // current registration or classified ownerless registry read path may steer lookup.
    let has_current_registration = row_has_current_registration(row);
    let record_inventory = record_inventory.filter(|_| has_current_registration);
    // A name that serves no resolver requests nothing: the lookup below refuses it before any
    // key is read.
    let requested_records = if has_current_registration {
        profile_verified_requested_records(record_inventory)?
    } else {
        Vec::new()
    };
    let verified_lookup = load_verified_record_lookup_for_resource(
        state,
        row,
        &requested_records,
        selected_snapshot,
        SnapshotReadResource::Name,
    )
    .await?;
    let verified_records = build_verified_name_records(
        row,
        record_inventory,
        RecordSelection::requested(&requested_records),
        verified_lookup,
        false,
        false,
    )?;
    let answers = &verified_records.records;

    let mut record = build_name_record(row, record_inventory, chain_id, Status::Ok)?;
    // The flat fields reflect the verified answers, never indexed values.
    let VerifiedRecordValues {
        addresses,
        text_records,
        content_hash,
    } = VerifiedRecordValues::from_answers(&requested_records, answers);
    let primary_address = addresses.get("60").cloned();
    let addresses_unserved = field_could_not_serve(&requested_records, answers, is_address_record);
    let text_records_unserved = field_could_not_serve(&requested_records, answers, is_text_record);
    let content_hash_unserved =
        field_could_not_serve(&requested_records, answers, is_content_hash_record);
    let primary_address_unserved =
        field_could_not_serve(&requested_records, answers, is_primary_address_record);

    let unsupported_fields = verified_unsupported_fields(
        addresses_unserved,
        text_records_unserved,
        content_hash_unserved,
        primary_address_unserved,
    );
    let status = verified_profile_status(answers, &unsupported_fields);

    record.addresses = (!addresses_unserved)
        .then(|| dictionary_field(addresses, &requested_records, answers, is_address_record))
        .flatten();
    record.text_records = (!text_records_unserved)
        .then(|| dictionary_field(text_records, &requested_records, answers, is_text_record))
        .flatten();
    record.content_hash = (!content_hash_unserved).then_some(content_hash).flatten();
    record.primary_address = (!primary_address_unserved)
        .then_some(primary_address)
        .flatten();
    record.status = status;
    record.unsupported_reason = verified_profile_unsupported_reason(answers, status);
    record.failure_reason = verified_profile_failure_reason(answers, status);
    record.unsupported_fields = unsupported_fields;
    Ok(record)
}

/// The record keys verified name detail executes: every key the name's record inventory lists,
/// plus `addr:60` for `primary_address`; or, when the inventory is missing or lists no key, the
/// bounded profile set [`PROFILE_FALLBACK_RECORD_KEYS`]. Inventory coverage decides neither: an
/// unsupported inventory still names its keys, and whether each getter executes is the lookup
/// engine's answer, reported per key. The set is not an enumeration of every text key or coin
/// type the resolver may hold.
fn profile_verified_requested_records(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> V2Result<Vec<ResolutionRecordKey>> {
    let mut records = default_requested_records(record_inventory)
        .into_iter()
        .map(|record| (record.record_key.clone(), record))
        .collect::<BTreeMap<_, _>>();
    let requested_records = records.values().cloned().collect::<Vec<_>>();
    ensure_default_record_limit(&requested_records)?;
    let fallbacks = if records.is_empty() {
        profile_fallback_requested_records()
    } else {
        vec![
            parse_resolution_record_key("addr:60")
                .expect("primary profile address selector must be valid"),
        ]
    };
    for record in fallbacks {
        records.entry(record.record_key.clone()).or_insert(record);
    }
    Ok(records.into_values().collect())
}

fn profile_fallback_requested_records() -> Vec<ResolutionRecordKey> {
    PROFILE_FALLBACK_RECORD_KEYS
        .iter()
        .map(|record_key| {
            parse_resolution_record_key(record_key)
                .expect("profile fallback record selector must be valid")
        })
        .collect()
}

fn dictionary_field(
    values: BTreeMap<String, String>,
    requested_records: &[ResolutionRecordKey],
    answers: &BTreeMap<String, RecordAnswer>,
    predicate: fn(&ResolutionRecordKey) -> bool,
) -> Option<BTreeMap<String, String>> {
    if !values.is_empty() || field_has_served_answer(requested_records, answers, predicate) {
        Some(values)
    } else {
        None
    }
}

fn verified_unsupported_fields(
    addresses_unserved: bool,
    text_records_unserved: bool,
    content_hash_unserved: bool,
    primary_address_unserved: bool,
) -> Vec<String> {
    let mut fields = BTreeSet::new();

    if addresses_unserved {
        fields.insert("addresses".to_owned());
    }
    if content_hash_unserved {
        fields.insert("content_hash".to_owned());
    }
    if primary_address_unserved {
        fields.insert("primary_address".to_owned());
    }
    if text_records_unserved {
        fields.insert("text_records".to_owned());
    }

    fields.into_iter().collect()
}

fn field_could_not_serve(
    requested_records: &[ResolutionRecordKey],
    answers: &BTreeMap<String, RecordAnswer>,
    predicate: fn(&ResolutionRecordKey) -> bool,
) -> bool {
    let mut has_relevant_record = false;
    let mut has_problem_answer = false;
    for record in requested_records.iter().filter(|record| predicate(record)) {
        has_relevant_record = true;
        match answers.get(&record.record_key) {
            Some(answer) if answer_is_problem(answer) => has_problem_answer = true,
            Some(_) => {}
            None => has_problem_answer = true,
        }
    }

    !has_relevant_record || has_problem_answer
}

fn field_has_served_answer(
    requested_records: &[ResolutionRecordKey],
    answers: &BTreeMap<String, RecordAnswer>,
    predicate: fn(&ResolutionRecordKey) -> bool,
) -> bool {
    requested_records
        .iter()
        .filter(|record| predicate(record))
        .filter_map(|record| answers.get(&record.record_key))
        .any(answer_is_served)
}

fn verified_profile_status(
    answers: &BTreeMap<String, RecordAnswer>,
    unsupported_fields: &[String],
) -> Status {
    if answers
        .values()
        .any(|answer| answer.status == Status::Stale)
    {
        Status::Stale
    } else if answers
        .values()
        .any(|answer| answer.status == Status::Failed)
    {
        Status::Failed
    } else if !unsupported_fields.is_empty()
        && (answers.is_empty()
            || answers
                .values()
                .any(|answer| answer.status == Status::Unsupported))
    {
        Status::Unsupported
    } else {
        Status::Ok
    }
}

fn verified_profile_failure_reason(
    answers: &BTreeMap<String, RecordAnswer>,
    status: Status,
) -> Option<String> {
    match status {
        Status::Failed | Status::Stale => answers
            .values()
            .find(|answer| answer.status == status)
            .and_then(|answer| answer.failure_reason.clone()),
        _ => None,
    }
}

fn verified_profile_unsupported_reason(
    answers: &BTreeMap<String, RecordAnswer>,
    status: Status,
) -> Option<String> {
    if status != Status::Unsupported {
        return None;
    }

    Some(
        answers
            .values()
            .find(|answer| answer.status == Status::Unsupported)
            .and_then(|answer| answer.unsupported_reason.clone())
            .unwrap_or_else(|| VERIFIED_NOT_SUPPORTED_REASON.to_owned()),
    )
}

fn answer_is_served(answer: &RecordAnswer) -> bool {
    matches!(answer.status, Status::Ok | Status::NotFound)
}

fn answer_is_problem(answer: &RecordAnswer) -> bool {
    matches!(
        answer.status,
        Status::Unsupported | Status::Stale | Status::Failed
    )
}

fn is_address_record(record: &ResolutionRecordKey) -> bool {
    record.record_family == "addr"
}

fn is_primary_address_record(record: &ResolutionRecordKey) -> bool {
    record.record_key == "addr:60"
}

fn is_text_record(record: &ResolutionRecordKey) -> bool {
    matches!(record.record_family.as_str(), "text" | "avatar")
}

fn is_content_hash_record(record: &ResolutionRecordKey) -> bool {
    record.record_key == "contenthash"
}

#[cfg(test)]
mod tests {
    use bigname_storage::RecordInventoryCurrentRow;
    use serde_json::json;
    use sqlx::types::time::OffsetDateTime;

    use super::*;
    use crate::v2::name_records::MAX_RECORD_KEYS;

    #[test]
    fn synthetic_primary_address_does_not_reject_maximum_inventory() {
        let selectors = (0..MAX_RECORD_KEYS)
            .map(|index| {
                json!({
                    "record_key": format!("text:key{index}"),
                    "record_family": "text",
                    "selector_key": format!("key{index}"),
                    "cacheable": true
                })
            })
            .collect::<Vec<_>>();
        let inventory = inventory(selectors, json!({"status":"projected"}));

        let records = profile_verified_requested_records(Some(&inventory))
            .expect("the synthetic primary selector is exempt from the client record limit");
        assert_eq!(records.len(), MAX_RECORD_KEYS + 1);
        assert!(records.iter().any(|record| record.record_key == "addr:60"));
    }

    fn inventory(
        selectors: Vec<serde_json::Value>,
        coverage: serde_json::Value,
    ) -> RecordInventoryCurrentRow {
        RecordInventoryCurrentRow {
            resource_id: "00000000-0000-0000-0000-000000000606"
                .parse()
                .expect("test resource id"),
            record_version_boundary: json!({}),
            enumeration_basis: json!({}),
            selectors: serde_json::Value::Array(selectors),
            explicit_gaps: json!([]),
            unsupported_families: json!([]),
            last_change: None,
            entries: json!([]),
            provenance: json!({}),
            coverage,
            chain_positions: json!({}),
            canonicality_summary: json!({}),
            manifest_version: 1,
            last_recomputed_at: OffsetDateTime::from_unix_timestamp(1_717_171_719)
                .expect("test timestamp"),
        }
    }

    fn keys(records: Vec<ResolutionRecordKey>) -> Vec<String> {
        records
            .into_iter()
            .map(|record| record.record_key)
            .collect()
    }

    #[test]
    fn a_name_without_usable_inventory_keys_requests_the_profile_set() {
        let profile = PROFILE_FALLBACK_RECORD_KEYS
            .iter()
            .map(|key| (*key).to_owned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let missing = profile_verified_requested_records(None).expect("profile set");
        assert_eq!(keys(missing), profile);
        // Unsupported coverage says nothing about whether a getter executes.
        let unsupported = inventory(
            Vec::new(),
            json!({"status":"unsupported","unsupported_reason":"resolver_profile_not_admitted"}),
        );
        let unsupported =
            profile_verified_requested_records(Some(&unsupported)).expect("profile set");
        assert_eq!(keys(unsupported), profile);
    }

    #[test]
    fn a_nonempty_inventory_requests_its_keys_and_the_primary_address() {
        let listed = inventory(
            vec![json!({
                "record_key": "text:com.twitter",
                "record_family": "text",
                "selector_key": "com.twitter"
            })],
            json!({"status":"unsupported","unsupported_reason":"resolver_profile_not_admitted"}),
        );
        let records = profile_verified_requested_records(Some(&listed)).expect("inventory keys");
        assert_eq!(keys(records), ["addr:60", "text:com.twitter"]);
    }
}
