//! Names that resolve to an address, read over the families instead of
//! `address_records_current` (address_records.rs and storage address_names/resolves_to.rs).
//!
//! The candidates are the inverse address index (F14) rows and every retained address value that
//! names the address (`candidates.rs`), a superset of the (resolver, node) and (resolver, record
//! id) keys that can serve it. The read goes on from each key to the resources that serve records
//! through it (F5 pointers at that resolver and node, pointers at a mirror resolver for that node,
//! pointers at a resolver whose link selects that record id), assembles each resource's family
//! record inventory to apply the arms, the combined boundary, the link selection and the mirror
//! substitution, keeps the entries that still resolve to the address, and joins today's
//! `name_current` for name eligibility (the family read model for it is step 3). Exact entries
//! shadow the ENSIP-19 default address as the forward read does: the resolvers read the default
//! only when the coin's own stored bytes are empty and the coin is an EVM coin.
//! (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L80-L85 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v2/contracts/src/resolver/AbstractRecordResolver.sol:L172-L178 @ ens_v2@a971bd64)
//! The page is a keyset over the result order with no publication binding.
use std::collections::BTreeSet;

use bigname_domain::resolver_read::ensip19_default_fallback_target;
use serde_json::{Value, json};

use uuid::Uuid;

use super::{inventory::FamilyRecordInventory, payload::strip_nulls};
use crate::ENSIP19_DEFAULT_ADDRESS_RECORD_KEY;

const ZERO: &str = "0x0000000000000000000000000000000000000000";

/// One `address_records_current`-shaped row the families derive for a resource.
pub(super) struct RecordRow {
    pub(super) record_resource_id: Uuid,
    pub(super) record_key: String,
    pub(super) coin_type: String,
    pub(super) provenance: Value,
    pub(super) chain_positions: Value,
}

pub(super) fn may_fall_back(coin_type: &str) -> bool {
    coin_type
        .parse::<u64>()
        .is_ok_and(ensip19_default_fallback_target)
}

/// An `addr` entry's coin type and address, when it answers with an EVM address.
fn entry_address(entry: &Value) -> Option<(String, String)> {
    if entry.get("record_family").and_then(Value::as_str) != Some("addr")
        || entry.get("status").and_then(Value::as_str) != Some("success")
    {
        return None;
    }
    let selector = entry.get("selector_key").and_then(Value::as_str)?;
    if selector.is_empty() || selector.len() > 30 || !selector.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let coin_type = selector.trim_start_matches('0');
    let coin_type = if coin_type.is_empty() { "0" } else { coin_type };
    let value = entry.get("value")?;
    let address = match value {
        Value::String(text) => text.clone(),
        _ => value
            .get("value")
            .or_else(|| value.get("bytes"))
            .and_then(Value::as_str)?
            .to_owned(),
    }
    .to_ascii_lowercase();
    let evm = address.len() == 42
        && address.starts_with("0x")
        && address[2..].bytes().all(|b| b.is_ascii_hexdigit());
    (evm && address != ZERO).then(|| (coin_type.to_owned(), address))
}

/// The rows `address_records.rs` publishes for one family inventory row that resolve to `address`.
pub(super) fn record_rows(
    chain_id: &str,
    inventory: &FamilyRecordInventory,
    address: &str,
) -> Vec<RecordRow> {
    let row = &inventory.row;
    if row.coverage.get("status").and_then(Value::as_str) != Some("projected")
        || row
            .provenance
            .get("record_serving")
            .is_some_and(|serving| serving == false)
    {
        return Vec::new();
    }
    let entries = row.entries.as_array().cloned().unwrap_or_default();
    let exact_absent = row
        .provenance
        .get("exact_nonempty_not_found_record_keys")
        .and_then(Value::as_array)
        .is_some_and(|keys| keys.iter().any(|key| key == "addr:60"));
    let mut shadowed = BTreeSet::new();
    for entry in &entries {
        let key = entry
            .get("record_key")
            .and_then(Value::as_str)
            .unwrap_or("");
        let selector = entry
            .get("selector_key")
            .and_then(Value::as_str)
            .unwrap_or("");
        if entry.get("record_family").and_then(Value::as_str) != Some("addr")
            || key == ENSIP19_DEFAULT_ADDRESS_RECORD_KEY
            || selector.is_empty()
            || selector.len() > 30
            || !selector.bytes().all(|b| b.is_ascii_digit())
        {
            continue;
        }
        let status = entry.get("status").and_then(Value::as_str);
        if status != Some("not_found") || (key == "addr:60" && exact_absent) {
            let coin = selector.trim_start_matches('0');
            shadowed.insert(if coin.is_empty() { "0" } else { coin }.to_owned());
        }
    }
    let default_rule = row
        .provenance
        .get("read_rules")
        .and_then(Value::as_array)
        .is_some_and(|rules| {
            rules.iter().any(|rule| {
                rule.get("kind").and_then(Value::as_str) == Some("ensip19_default_address")
                    && rule.get("source_record_key").and_then(Value::as_str)
                        == Some(ENSIP19_DEFAULT_ADDRESS_RECORD_KEY)
            })
        });
    let mut rows = Vec::new();
    for entry in &entries {
        let Some((coin_type, entry_address)) = entry_address(entry) else {
            continue;
        };
        if entry_address != address {
            continue;
        }
        let record_key = entry["record_key"].as_str().unwrap_or_default().to_owned();
        let mut provenance = strip_nulls(json!({
            "chain_id": chain_id,
            "resolver_address": row.provenance.get("resolver_address"),
            "record_version_boundary_key": inventory.record_version_boundary_key,
            "normalized_event_id": row.last_change.as_ref()
                .and_then(|change| change.get("normalized_event_id")),
            "coverage": {"status": "projected", "exhaustiveness": "not_asserted"},
        }));
        if record_key == ENSIP19_DEFAULT_ADDRESS_RECORD_KEY && default_rule {
            provenance["ensip19_default_address"] = json!(true);
            provenance["shadowed_coin_types"] = json!(shadowed);
        }
        rows.push(RecordRow {
            record_resource_id: row.resource_id,
            record_key,
            coin_type,
            provenance,
            chain_positions: strip_nulls(json!({
                "block_number": row.chain_positions.get("block_number"),
                "block_hash": row.chain_positions.get("block_hash"),
            })),
        });
    }
    rows
}
