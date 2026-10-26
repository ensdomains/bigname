//! The default records read, with no `source` parameter, answers a key whose indexed answer is
//! `value_not_retained` through the verified read: the resolver's event named the key, but
//! bigname retained no value for it. Only those keys are read, through the same lookup and RPC
//! timeouts `source=auto` uses for explicit keys. Every other key keeps its indexed answer, and a
//! key the verified read does not answer `ok` or `not_found` keeps `value_not_retained`. A failed
//! read leaves the indexed response as it was. A response with a filled key reports
//! `meta.source=verified` and carries no cache headers (`v2::cache_headers`).
use bigname_storage::{NameCurrentRow, SelectedSnapshot};
use tracing::warn;

use super::{NameRecords, Source, Status, VerifiedRecordLookup, load_verified_record_lookup};
use crate::AppState;
use crate::v2::support::{AliasPath, ResolutionRecordKey};

const VALUE_NOT_RETAINED: &str = "value_not_retained";

/// Whether the request named no `source`, or only a blank one, which is the default read. The
/// request bounds classify the same requests as verified execution
/// (`bounds::classification::is_verified_execution_request`), so the fill runs under the
/// verified in-flight ceiling and client rate limit.
pub(super) fn default_source(raw_query: Option<&str>) -> bool {
    form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes())
        .filter(|(key, _)| key == "source")
        .all(|(_, value)| value.trim().is_empty())
}

type Reads<'a> = (
    &'a AppState,
    &'a NameCurrentRow,
    Option<&'a AliasPath>,
    &'a [ResolutionRecordKey],
);

/// Fills `data`'s `value_not_retained` answers from the verified read on the default read, and
/// returns the response's source with it: `verified` once any answer came from the verified
/// read, as `source=auto` does.
pub(super) async fn fill(
    default_source: bool,
    reads: Reads<'_>,
    selected_snapshot: &mut SelectedSnapshot,
    mut data: NameRecords,
) -> (Source, NameRecords) {
    let source = fill_unretained(default_source, reads, selected_snapshot, &mut data).await;
    (source, data)
}

async fn fill_unretained(
    default_source: bool,
    reads: Reads<'_>,
    selected_snapshot: &mut SelectedSnapshot,
    data: &mut NameRecords,
) -> Source {
    let (state, row, alias, requested) = reads;
    if !default_source {
        return Source::Indexed;
    }
    let keys: Vec<ResolutionRecordKey> = requested
        .iter()
        .filter(|record| {
            data.records.get(&record.record_key).is_some_and(|answer| {
                answer.unsupported_reason.as_deref() == Some(VALUE_NOT_RETAINED)
                    || answer.failure_reason.as_deref() == Some(VALUE_NOT_RETAINED)
            })
        })
        .cloned()
        .collect();
    if keys.is_empty() {
        return Source::Indexed;
    }
    let lookup =
        match load_verified_record_lookup(state, row, &keys, selected_snapshot, alias).await {
            Ok(Some(VerifiedRecordLookup::Found { response })) => response,
            Ok(_) => return Source::Indexed,
            Err(error) => {
                warn!(
                    service = "api",
                    name = %row.logical_name_id,
                    error = ?error,
                    "verified read for value_not_retained records failed; serving indexed answers"
                );
                return Source::Indexed;
            }
        };
    let Ok(answers) = super::build::verified_answers_from_results(&lookup.records, &keys) else {
        return Source::Indexed;
    };
    let mut filled = false;
    for (key, answer) in answers {
        if matches!(answer.status, Status::Ok | Status::NotFound) {
            data.records.insert(key, answer);
            filled = true;
        }
    }
    if filled {
        Source::Verified
    } else {
        Source::Indexed
    }
}
