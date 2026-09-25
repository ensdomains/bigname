//! Keyset cursors for the history collections, `/v1/events`, name history, and address history
//! (docs/api-v1-routes.md, "Shared Route Rules"). A cursor holds its anchor's position in the
//! history order (block number, chain, block hash, transaction index, log index, then the
//! anchor's identity) and continues after it against whatever is published when the next page
//! is read, whether or not the anchor row still exists. Two older layouts are still read. A
//! cursor that names only its anchor row resumes from that row's position and restarts once
//! when the row is gone. A positional cursor issued while the order compared transaction hashes
//! carries its anchor's transaction hash and no index; the index is read from the anchor row or
//! another event of its transaction, and it restarts once when neither exists. The payload
//! version stays 1 because the `last_item` key set tells the layouts apart.

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
/// Read from cursors issued before the order compared transaction indexes; never written.
const TRANSACTION_HASH_KEY: &str = "transaction_hash";
const TRANSACTION_INDEX_KEY: &str = "transaction_index";
const LOG_INDEX_KEY: &str = "log_index";

/// A decoded history cursor, before a legacy one is resolved through its anchor row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RequestCursor {
    Position(HistoryCursor),
    /// A positional cursor issued before the order compared transaction indexes: its position
    /// holds the anchor's transaction hash and no transaction index.
    TransactionHash(HistoryCursor),
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
        insert(
            TRANSACTION_INDEX_KEY,
            position.transaction_index.map(|value| value.to_string()),
        );
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

/// Decode `last_item`. A cursor issued before keyset cursors holds exactly the anchor's numeric
/// id and identity; any other key set must be the identity plus ordering values, where a missing
/// value is a null one. A transaction index makes the cursor current and any transaction hash
/// beside it is ignored. A transaction hash without an index marks a cursor issued before the
/// order compared indexes; when it also has no log index, its anchor has no transaction index
/// either (normalized events hold both or neither), so its position is already complete.
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
        TRANSACTION_INDEX_KEY,
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
    let mut position = HistoryPosition {
        block_number: integer(BLOCK_NUMBER_KEY)?,
        chain_id: text(CHAIN_ID_KEY)?,
        block_hash: text(BLOCK_HASH_KEY)?,
        transaction_index: integer(TRANSACTION_INDEX_KEY)?,
        transaction_hash: text(TRANSACTION_HASH_KEY)?,
        log_index: integer(LOG_INDEX_KEY)?,
    };
    let needs_transaction_index = position.transaction_index.is_none()
        && position.transaction_hash.is_some()
        && position.log_index.is_some();
    if !needs_transaction_index {
        position.transaction_hash = None;
    }
    let cursor = HistoryCursor {
        normalized_event_id: None,
        event_identity,
        position: Some(position),
    };
    Ok(if needs_transaction_index {
        RequestCursor::TransactionHash(cursor)
    } else {
        RequestCursor::Position(cursor)
    })
}

/// The storage cursor for a decoded request cursor. A legacy cursor continues from its anchor
/// row's position; when that row is gone the client restarts without the cursor. A cursor with a
/// transaction hash continues from its own position with the transaction index of its anchor
/// row, or of another event of that transaction in the same block; when neither exists the
/// client restarts. Both reads run behind the Interpret redo check, so a redo refuses the page
/// with its retry before a row the redo removed can turn into a restart.
pub(crate) async fn resolve(state: &AppState, cursor: RequestCursor) -> V2Result<HistoryCursor> {
    match cursor {
        RequestCursor::Position(cursor) => Ok(cursor),
        RequestCursor::TransactionHash(mut cursor) => {
            let Some(position) = cursor.position.as_mut() else {
                return Err(invalid_cursor_error());
            };
            let transaction_index = bigname_storage::load_history_transaction_index(
                &state.pool,
                &cursor.event_identity,
                position,
            )
            .await
            .map_err(|error| map_history_page_error(error, "failed to load history"))?
            .ok_or_else(restart_required)?;
            position.transaction_index = Some(transaction_index);
            position.transaction_hash = None;
            Ok(cursor)
        }
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
                transaction_index: Some(2),
                transaction_hash: None,
                log_index: Some(3),
            }),
        };
        let encoded = CursorPayload::new("sort", BTreeMap::new(), last_item(&cursor), None);
        assert!(!encoded.last_item.contains_key(NORMALIZED_EVENT_ID_KEY));
        assert_eq!(
            encoded
                .last_item
                .get(TRANSACTION_INDEX_KEY)
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            decode_last_item(&encoded).expect("decodes"),
            RequestCursor::Position(HistoryCursor {
                normalized_event_id: None,
                ..cursor
            })
        );
    }

    #[test]
    fn history_cursor_never_writes_a_transaction_hash() {
        let cursor = HistoryCursor {
            normalized_event_id: Some(7),
            event_identity: "event:7".to_owned(),
            position: Some(HistoryPosition {
                block_number: Some(100),
                chain_id: Some("ethereum-mainnet".to_owned()),
                block_hash: Some("0xabc".to_owned()),
                transaction_index: Some(2),
                transaction_hash: Some("0xtx".to_owned()),
                log_index: Some(3),
            }),
        };
        assert!(!last_item(&cursor).contains_key(TRANSACTION_HASH_KEY));
    }

    fn position(transaction_index: Option<i64>, log_index: Option<i64>) -> HistoryPosition {
        HistoryPosition {
            block_number: Some(100),
            chain_id: Some("ethereum-mainnet".to_owned()),
            block_hash: Some("0xabc".to_owned()),
            transaction_index,
            transaction_hash: None,
            log_index,
        }
    }

    fn positional(position: HistoryPosition) -> HistoryCursor {
        HistoryCursor {
            normalized_event_id: None,
            event_identity: "e".to_owned(),
            position: Some(position),
        }
    }

    #[test]
    fn history_cursor_reads_each_positional_layout() {
        let base = [
            ("event_identity", "e"),
            ("block_number", "100"),
            ("chain_id", "ethereum-mainnet"),
            ("block_hash", "0xabc"),
        ];
        let with = |extra: &[(&'static str, &'static str)]| {
            let mut item = base.to_vec();
            item.extend_from_slice(extra);
            payload(&item)
        };
        // Current: the transaction index.
        assert_eq!(
            decode_last_item(&with(&[("transaction_index", "2"), ("log_index", "3")])).unwrap(),
            RequestCursor::Position(positional(position(Some(2), Some(3))))
        );
        // Both keys: read as current, the hash ignored.
        assert_eq!(
            decode_last_item(&with(&[
                ("transaction_index", "2"),
                ("transaction_hash", "0xtx"),
                ("log_index", "3"),
            ]))
            .unwrap(),
            RequestCursor::Position(positional(position(Some(2), Some(3))))
        );
        // Issued while the order compared hashes: the index is still to be found.
        assert_eq!(
            decode_last_item(&with(&[("transaction_hash", "0xtx"), ("log_index", "3")])).unwrap(),
            RequestCursor::TransactionHash(positional(HistoryPosition {
                transaction_hash: Some("0xtx".to_owned()),
                ..position(None, Some(3))
            }))
        );
        // A hash without a log index names an event without a transaction index.
        assert_eq!(
            decode_last_item(&with(&[("transaction_hash", "0xtx")])).unwrap(),
            RequestCursor::Position(positional(position(None, None)))
        );
        // No transaction at all: an event derived at the block boundary.
        assert_eq!(
            decode_last_item(&with(&[])).unwrap(),
            RequestCursor::Position(positional(position(None, None)))
        );
        for malformed in [
            with(&[("transaction_index", "two")]),
            with(&[("transaction_hash", "")]),
        ] {
            assert!(decode_last_item(&malformed).is_err(), "{malformed:?}");
        }
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
