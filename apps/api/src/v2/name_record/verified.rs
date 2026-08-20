use std::collections::{BTreeMap, BTreeSet};

use bigname_storage::{NameCurrentRow, RecordInventoryCurrentRow, SelectedSnapshot};

use crate::AppState;
use crate::v2::support::{
    PROFILE_FALLBACK_RECORD_KEYS, ResolutionRecordKey, parse_resolution_record_key,
};
use crate::v2::vocab::{
    MISSING_UNSUPPORTED_REASON, downgrades_unsupported_name, shared_product_reason,
};

use super::super::{
    SnapshotReadResource, Source, Status, V2Result, default_requested_records,
    name_records::{
        RecordAnswer, VERIFIED_NOT_SUPPORTED_REASON, build_verified_name_records,
        ensure_verified_record_limit, load_verified_record_lookup_for_resource,
    },
    vocab::RegistrationStatus,
};
use super::{NameRecord, build_name_record, name_registration_fields, string_field};

pub(super) struct VerifiedNameRecord {
    pub(super) record: NameRecord,
}

pub(super) async fn build_name_record_for_source(
    state: &AppState,
    row: &NameCurrentRow,
    record_inventory: Option<&RecordInventoryCurrentRow>,
    chain_id: Option<u64>,
    selected_snapshot: &mut SelectedSnapshot,
    source: Source,
) -> V2Result<VerifiedNameRecord> {
    if let Some(record) = unsupported_name_record(row)? {
        return Ok(VerifiedNameRecord { record });
    }
    match source {
        Source::Indexed => Ok(VerifiedNameRecord {
            record: build_name_record(row, record_inventory, chain_id, Status::Ok)?,
        }),
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
    let reason = shared_product_reason(
        &reason,
        "rejected exact-name reason containing pipeline vocabulary",
        "failed to map exact-name reason vocabulary",
    )?;
    Ok(Some(NameRecord {
        registration_id: None,
        token_id: None,
        owner: None,
        manager: None,
        registrant: None,
        registered_at: None,
        created_at: None,
        expires_at: None,
        registration_status: None,
        wrapper_state: None,
        wrapper_fuses: None,
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
) -> V2Result<VerifiedNameRecord> {
    // Mirror build_name_record's released-tombstone strip before deriving the
    // requested records: retained inventory or resolver state must not steer a
    // provider lookup for a released name, so the path collapses to the same
    // outcome as the canonical inventory-less tombstone.
    let record_inventory = record_inventory.filter(|_| {
        name_registration_fields(Some(row), &row.namespace).registration_status
            != RegistrationStatus::Released
    });
    let requested_records = profile_verified_requested_records(record_inventory)?;
    let verified_lookup = load_verified_record_lookup_for_resource(
        state,
        row,
        record_inventory,
        &requested_records,
        selected_snapshot,
        SnapshotReadResource::Name,
    )
    .await?;
    let mut verified_records = build_verified_name_records(
        row,
        record_inventory,
        Some(&requested_records),
        verified_lookup,
        false,
    )?;
    let answers = verified_records
        .records
        .as_ref()
        .expect("verified profile requested records must produce an answer map");

    let mut record = build_name_record(row, record_inventory, chain_id, Status::Ok)?;
    let addresses = std::mem::take(&mut verified_records.addresses);
    let text_records = std::mem::take(&mut verified_records.text_records);
    let content_hash = verified_records.content_hash.take();
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
    Ok(VerifiedNameRecord { record })
}

fn profile_verified_requested_records(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> V2Result<Vec<ResolutionRecordKey>> {
    let records = default_requested_records(record_inventory);
    let records = if !records.is_empty() || !should_use_profile_fallback_records(record_inventory) {
        records
    } else {
        profile_fallback_requested_records()
    };
    ensure_verified_record_limit(&records)?;
    Ok(records)
}

fn should_use_profile_fallback_records(
    record_inventory: Option<&RecordInventoryCurrentRow>,
) -> bool {
    let Some(record_inventory) = record_inventory else {
        return false;
    };
    string_field(record_inventory.coverage.get("status")).as_deref() != Some("unsupported")
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
