//! Keyset cursors for the history collections, `/v1/events`, name history, and address history
//! (docs/api-v1-routes.md, "Shared Route Rules"). A cursor holds its anchor's position in the
//! history order and continues after it against whatever is published when the next page is
//! read, whether or not the anchor row still exists. A cursor issued before this layout names
//! only its anchor row; it resumes from that row's position and restarts once when the row is
//! gone.

use std::collections::BTreeMap;

use bigname_storage::{HistoryCursor, HistoryPosition};

use crate::AppState;

use super::collection_snapshot::restart_required;
use super::cursor::invalid_cursor_error;
use super::{CursorPayload, V2Result, map_history_page_error};

const EVENT_IDENTITY_KEY: &str = "event_identity";
const NORMALIZED_EVENT_ID_KEY: &str = "normalized_event_id";
const BLOCK_NUMBER_KEY: &str = "block_number";
const CHAIN_ID_KEY: &str = "chain_id";
const BLOCK_HASH_KEY: &str = "block_hash";
const TRANSACTION_HASH_KEY: &str = "transaction_hash";
const LOG_INDEX_KEY: &str = "log_index";

/// A decoded history cursor, before a legacy one is resolved through its anchor row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RequestCursor {
    Position(HistoryCursor),
    /// A cursor issued before keyset history cursors: its anchor's identity only.
    Legacy(String),
}

/// The cursor's `last_item`: the anchor's identity and each ordering value it has.
fn last_item(cursor: &HistoryCursor) -> BTreeMap<String, String> {
    let mut item = BTreeMap::from([(EVENT_IDENTITY_KEY.to_owned(), cursor.event_identity.clone())]);
    if let Some(position) = cursor.position.as_ref() {
        let mut insert = |key: &str, value: Option<String>| {
            if let Some(value) = value {
                item.insert(key.to_owned(), value);
            }
        };
        insert(
            BLOCK_NUMBER_KEY,
            position.block_number.map(|value| value.to_string()),
        );
        insert(CHAIN_ID_KEY, position.chain_id.clone());
        insert(BLOCK_HASH_KEY, position.block_hash.clone());
        insert(TRANSACTION_HASH_KEY, position.transaction_hash.clone());
        insert(
            LOG_INDEX_KEY,
            position.log_index.map(|value| value.to_string()),
        );
    }
    item
}

/// A history collection's cursor: its sort token, the request filters it binds, and the
/// anchor's position. It carries no publication token and no evaluation time.
pub(crate) fn cursor_payload(
    cursor: &HistoryCursor,
    sort: &str,
    filters: BTreeMap<String, String>,
) -> CursorPayload {
    CursorPayload::new(sort, filters, last_item(cursor), None)
}

/// Decode a history collection's cursor for a request with this sort token and these filters.
/// A publication token or evaluation time that a cursor issued before this layout carries is
/// ignored.
pub(crate) fn decode_cursor(
    payload: &CursorPayload,
    sort: &str,
    filters: &BTreeMap<String, String>,
) -> V2Result<RequestCursor> {
    if payload.sort != sort || &payload.filters != filters {
        return Err(invalid_cursor_error());
    }
    decode_last_item(payload)
}

/// Decode `last_item`. A cursor issued before this layout holds exactly the anchor's numeric id
/// and identity; any other key set must be the identity plus ordering values, where a missing
/// value is a null one.
fn decode_last_item(payload: &CursorPayload) -> V2Result<RequestCursor> {
    let item = &payload.last_item;
    let event_identity = item
        .get(EVENT_IDENTITY_KEY)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(invalid_cursor_error)?
        .clone();
    if item.len() == 2
        && let Some(id) = item.get(NORMALIZED_EVENT_ID_KEY)
    {
        id.parse::<i64>().map_err(|_| invalid_cursor_error())?;
        return Ok(RequestCursor::Legacy(event_identity));
    }
    let known = [
        EVENT_IDENTITY_KEY,
        BLOCK_NUMBER_KEY,
        CHAIN_ID_KEY,
        BLOCK_HASH_KEY,
        TRANSACTION_HASH_KEY,
        LOG_INDEX_KEY,
    ];
    if item.keys().any(|key| !known.contains(&key.as_str())) {
        return Err(invalid_cursor_error());
    }
    let integer = |key: &str| -> V2Result<Option<i64>> {
        item.get(key)
            .map(|value| value.parse::<i64>().map_err(|_| invalid_cursor_error()))
            .transpose()
    };
    let text = |key: &str| -> V2Result<Option<String>> {
        match item.get(key) {
            Some(value) if value.is_empty() => Err(invalid_cursor_error()),
            value => Ok(value.cloned()),
        }
    };
    Ok(RequestCursor::Position(HistoryCursor {
        normalized_event_id: None,
        event_identity,
        position: Some(HistoryPosition {
            block_number: integer(BLOCK_NUMBER_KEY)?,
            chain_id: text(CHAIN_ID_KEY)?,
            block_hash: text(BLOCK_HASH_KEY)?,
            transaction_hash: text(TRANSACTION_HASH_KEY)?,
            log_index: integer(LOG_INDEX_KEY)?,
        }),
    }))
}

/// The storage cursor for a decoded request cursor. A legacy cursor continues from its anchor
/// row's position; when that row is gone the client restarts without the cursor. The anchor is
/// read behind the Interpret redo check, so a redo refuses the page with its retry before a row
/// the redo removed can turn into a restart.
pub(crate) async fn resolve(state: &AppState, cursor: RequestCursor) -> V2Result<HistoryCursor> {
    match cursor {
        RequestCursor::Position(cursor) => Ok(cursor),
        RequestCursor::Legacy(event_identity) => {
            let position =
                bigname_storage::load_history_anchor_position(&state.pool, &event_identity)
                    .await
                    .map_err(|error| map_history_page_error(error, "failed to load history"))?
                    .ok_or_else(restart_required)?;
            Ok(HistoryCursor {
                normalized_event_id: None,
                event_identity,
                position: Some(position),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(item: &[(&str, &str)]) -> CursorPayload {
        CursorPayload::new(
            "sort",
            BTreeMap::new(),
            item.iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
            None,
        )
    }

    #[test]
    fn history_cursor_round_trips_its_position() {
        let cursor = HistoryCursor {
            normalized_event_id: Some(7),
            event_identity: "event:7".to_owned(),
            position: Some(HistoryPosition {
                block_number: Some(100),
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_hash: Some("0xabc".to_owned()),
                transaction_hash: None,
                log_index: Some(3),
            }),
        };
        let encoded = CursorPayload::new("sort", BTreeMap::new(), last_item(&cursor), None);
        assert!(!encoded.last_item.contains_key(NORMALIZED_EVENT_ID_KEY));
        assert_eq!(
            decode_last_item(&encoded).expect("decodes"),
            RequestCursor::Position(HistoryCursor {
                normalized_event_id: None,
                ..cursor
            })
        );
    }

    #[test]
    fn history_cursor_decodes_legacy_and_rejects_malformed_items() {
        assert_eq!(
            decode_last_item(&payload(&[
                ("normalized_event_id", "42"),
                ("event_identity", "event:42")
            ]))
            .expect("legacy"),
            RequestCursor::Legacy("event:42".to_owned())
        );
        for item in [
            vec![("normalized_event_id", "x"), ("event_identity", "event:42")],
            vec![("block_number", "1")],
            vec![("event_identity", "")],
            vec![("event_identity", "  ")],
            vec![("normalized_event_id", "42"), ("event_identity", " ")],
            vec![("event_identity", "e"), ("block_number", "one")],
            vec![("event_identity", "e"), ("chain_id", "")],
            vec![("event_identity", "e"), ("unknown", "1")],
            vec![
                ("event_identity", "e"),
                ("normalized_event_id", "1"),
                ("block_number", "1"),
            ],
        ] {
            assert!(decode_last_item(&payload(&item)).is_err(), "{item:?}");
        }
    }
}
