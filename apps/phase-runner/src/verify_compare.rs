use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use bigname_ingest::{VerificationBatch, VerificationLog};

use crate::verify_store::StoredVerificationBatch;

pub(crate) struct VerificationMismatch {
    block_number: i64,
    field: String,
    ours: String,
    reference: String,
}

impl fmt::Display for VerificationMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "block {} field {}: ours={}, reference={}",
            self.block_number, self.field, self.ours, self.reference
        )
    }
}

pub(crate) fn compare(
    stored: &StoredVerificationBatch,
    reference: &VerificationBatch,
) -> Option<VerificationMismatch> {
    if stored.end.number != reference.end.number {
        return Some(mismatch(
            stored.end.number,
            "block_number",
            stored.end.number,
            reference.end.number,
        ));
    }
    if !stored.end.hash.eq_ignore_ascii_case(&reference.end.hash) {
        return Some(mismatch(
            stored.end.number,
            "block_hash",
            &stored.end.hash,
            &reference.end.hash,
        ));
    }

    let stored = logs_by_position(&stored.logs);
    let reference = logs_by_position(&reference.logs);
    let positions = stored
        .keys()
        .chain(reference.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    for position in positions {
        let ours = stored.get(&position);
        let theirs = reference.get(&position);
        match (ours, theirs) {
            (Some(ours), Some(theirs)) => {
                if let Some(difference) = compare_log(ours, theirs) {
                    return Some(difference);
                }
            }
            (Some(ours), None) => {
                return Some(VerificationMismatch {
                    block_number: position.0,
                    field: format!("raw_logs[{}].presence", position.1),
                    ours: describe_log(ours),
                    reference: "<missing>".to_owned(),
                });
            }
            (None, Some(theirs)) => {
                return Some(VerificationMismatch {
                    block_number: position.0,
                    field: format!("raw_logs[{}].presence", position.1),
                    ours: "<missing>".to_owned(),
                    reference: describe_log(theirs),
                });
            }
            (None, None) => {}
        }
    }
    None
}

fn logs_by_position(logs: &[VerificationLog]) -> BTreeMap<(i64, i64), &VerificationLog> {
    logs.iter()
        .map(|log| ((log.block_number, log.log_index), log))
        .collect()
}

fn compare_log(
    ours: &VerificationLog,
    reference: &VerificationLog,
) -> Option<VerificationMismatch> {
    let block = ours.block_number;
    let prefix = format!("raw_logs[{}]", ours.log_index);
    for (field, ours, reference) in [
        (
            "block_hash",
            ours.block_hash.as_str(),
            reference.block_hash.as_str(),
        ),
        (
            "transaction_hash",
            ours.transaction_hash.as_str(),
            reference.transaction_hash.as_str(),
        ),
        (
            "emitting_address",
            ours.address.as_str(),
            reference.address.as_str(),
        ),
    ] {
        if !ours.eq_ignore_ascii_case(reference) {
            return Some(mismatch(
                block,
                format!("{prefix}.{field}"),
                ours,
                reference,
            ));
        }
    }
    for (field, ours, reference) in [
        (
            "transaction_index",
            ours.transaction_index,
            reference.transaction_index,
        ),
        ("log_index", ours.log_index, reference.log_index),
    ] {
        if ours != reference {
            return Some(mismatch(
                block,
                format!("{prefix}.{field}"),
                ours,
                reference,
            ));
        }
    }
    let ours_topics = ours
        .topics
        .iter()
        .map(|topic| topic.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let reference_topics = reference
        .topics
        .iter()
        .map(|topic| topic.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if ours_topics != reference_topics {
        return Some(mismatch(
            block,
            format!("{prefix}.topics"),
            format!("{ours_topics:?}"),
            format!("{reference_topics:?}"),
        ));
    }
    if ours.data != reference.data {
        return Some(mismatch(
            block,
            format!("{prefix}.data"),
            bytes_hex(&ours.data),
            bytes_hex(&reference.data),
        ));
    }
    None
}

fn mismatch(
    block_number: i64,
    field: impl Into<String>,
    ours: impl fmt::Display,
    reference: impl fmt::Display,
) -> VerificationMismatch {
    VerificationMismatch {
        block_number,
        field: field.into(),
        ours: bounded(ours.to_string()),
        reference: bounded(reference.to_string()),
    }
}

fn describe_log(log: &VerificationLog) -> String {
    bounded(format!(
        "{{block_hash={}, transaction_hash={}, transaction_index={}, log_index={}, address={}, \
         topics={:?}, data={}}}",
        log.block_hash,
        log.transaction_hash,
        log.transaction_index,
        log.log_index,
        log.address,
        log.topics,
        bytes_hex(&log.data)
    ))
}

fn bytes_hex(bytes: &[u8]) -> String {
    let mut output = String::from("0x");
    for byte in bytes.iter().take(128) {
        use fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    if bytes.len() > 128 {
        output.push('…');
    }
    output
}

fn bounded(value: String) -> String {
    const LIMIT: usize = 1_024;
    if value.len() <= LIMIT {
        return value;
    }
    let mut end = LIMIT;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}
