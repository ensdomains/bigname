//! Shadow readers for step 4 of TYR-36: the resource resolver pointer (F5), the registry-node
//! pointer and the ENSv1 mirror walk over it (F4), the record inventory assembled from the
//! node-keyed and record-id record families (F6, F7), the link selection, the inverse address
//! index (F14) and the reverse claim (F12).
//!
//! Every reader returns the value today's reader serves for the same key, built from the family
//! rows instead of the served tables, so the harness can compare the two field by field. Every
//! production response is still served from today's tables; nothing outside the harness calls
//! these readers. Events are ordered in the canonical event order of the families (block number,
//! transaction index, log index, then the event identity as bytes, with a synthesised event's
//! missing positions first), so a same-position tie can resolve differently from today's readers,
//! which break it by the generated event id.
mod links;
mod pointer;

use serde_json::Value;
use sqlx::{Row, postgres::PgRow};

pub use links::{
    DEFAULT_RECORD_NODE, FamilyAliasSourcePointer, FamilyLink, FamilyWildcardSource, LinkSelection,
    load_family_alias_source_pointer, load_family_link_selection, load_family_wildcard_source,
};
pub use pointer::{FamilyResourcePointer, load_family_resource_pointer};

/// The resolver address a clear writes: the zero address, or the empty string for a pointer event
/// without a resolver.
pub(crate) const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Whether a stored resolver address is a clear.
pub(crate) fn is_cleared(address: Option<&str>) -> bool {
    address.is_none_or(|address| address.is_empty() || address == ZERO_ADDRESS)
}

/// A position in the canonical event order. The derived ordering compares block number,
/// transaction index and log index with a missing index first, then the event identity by its
/// bytes.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FamilyPosition {
    pub block_number: i64,
    pub transaction_index: Option<i64>,
    pub log_index: Option<i64>,
    pub event_identity: String,
}

impl FamilyPosition {
    /// A secondary position stored as a JSON object beside a row's own position.
    pub fn from_json(value: &Value) -> Option<Self> {
        Some(Self {
            block_number: value.get("block_number")?.as_i64()?,
            transaction_index: value.get("transaction_index").and_then(Value::as_i64),
            log_index: value.get("log_index").and_then(Value::as_i64),
            event_identity: value.get("event_identity")?.as_str()?.to_owned(),
        })
    }

    /// The four position columns every family row carries for its last owning event.
    pub(crate) fn from_row(row: &PgRow) -> anyhow::Result<Self> {
        Ok(Self {
            block_number: row.try_get("block_number")?,
            transaction_index: row.try_get("transaction_index")?,
            log_index: row.try_get("log_index")?,
            event_identity: row.try_get("event_identity")?,
        })
    }
}
