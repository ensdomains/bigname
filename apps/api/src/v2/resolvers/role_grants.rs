//! `grant_event` provenance for resolver `/roles` rows.

use bigname_storage::HistoryEvent as StorageHistoryEvent;
use serde_json::{Value, json};

use crate::v2::format_timestamp;

pub(super) fn role_grant_event_value(event: &StorageHistoryEvent) -> Value {
    let mut object = serde_json::Map::new();
    object.insert("block_number".to_owned(), json!(event.block_number));
    if let Some(timestamp) = event.block_timestamp {
        object.insert("timestamp".to_owned(), json!(format_timestamp(timestamp)));
    }
    if let Some(transaction_hash) = event.transaction_hash.as_ref() {
        object.insert("transaction_hash".to_owned(), json!(transaction_hash));
    }
    if let Some(log_index) = event.log_index {
        object.insert("log_index".to_owned(), json!(log_index));
    }
    Value::Object(object)
}
