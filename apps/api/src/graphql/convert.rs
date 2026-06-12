use bigname_storage::{
    NameCurrentListRow, RecordInventoryCurrentRow, resolution_record_inventory_lookup_key_any_chain,
};
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use super::objects::{AddressRecord, Domain, Resolver};

/// Non-null `owner` fallback for ownerless names (all-zero address).
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Mirrors the REST row→fields mapping (`responses/app_facing/names_collection.rs`) so GraphQL and
/// REST agree on the derived `owner`/`tokenId`/dates/`resolver`. `owner` resolves the non-null
/// `Account!` fallback chain here so the resolver stays trivial.
impl From<NameCurrentListRow> for Domain {
    fn from(row: NameCurrentListRow) -> Self {
        // The any-chain key: the verified-resolution REST surface scopes record reads to the
        // mainnet profiles, but the subgraph endpoint serves declared record inventory on whatever
        // chain the deployment indexes (Sepolia v2 here).
        let record_inventory_key = resolution_record_inventory_lookup_key_any_chain(&row.row);
        let owner_id = non_empty(row.owner)
            .or_else(|| non_empty(row.registrant))
            .unwrap_or_else(|| ZERO_ADDRESS.to_owned());
        Self {
            id: row.row.namehash,
            name: Some(row.row.canonical_display_name),
            normalized_name: Some(row.row.normalized_name),
            token_id: non_empty(row.token_id),
            // The Manager codegen pins `createdAt` non-null (`Int!`); the storage value coalesces
            // registration/history timestamps with the surface block timestamp, so a missing value
            // is a degenerate row — surface it as epoch rather than break the contract with null.
            created_at: row.created_at.map(unix_seconds_i32).unwrap_or(0),
            expiry_date: row.expiry_date.map(unix_seconds_i32),
            resolver_address: non_empty(row.resolver_address),
            owner_id,
            record_inventory_key,
        }
    }
}

/// Build the subgraph `Resolver` from the resolver address plus the name's
/// `record_inventory_current` row, mirroring the REST derivations
/// (`responses/app_facing/records_declared_values.rs` / `records_declared_inventory.rs`):
/// `texts` are the text-family selector keys ever observed (subgraph semantics — keys, not
/// values); `addresses` are the addr-family cache entries whose values were retained
/// (`status == "success"`); `contentHash` is the retained `contenthash` entry value. A name with
/// no inventory row serves the empty shapes.
pub(super) fn resolver_from_store(
    address: String,
    inventory: Option<&RecordInventoryCurrentRow>,
) -> Resolver {
    let texts = inventory
        .map(|row| {
            json_items(&row.selectors)
                .filter(|selector| json_str(selector, "record_family") == Some("text"))
                .filter_map(|selector| json_str(selector, "selector_key"))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let addresses = inventory
        .map(|row| {
            successful_entries(&row.entries, "addr")
                .filter_map(|(entry, value)| {
                    let coin_type = json_str(entry, "selector_key")?.parse::<u32>().ok()?;
                    Some(AddressRecord {
                        coin_type,
                        address: value,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let content_hash = inventory.and_then(|row| {
        successful_entries(&row.entries, "contenthash")
            .map(|(_, value)| value)
            .next()
    });

    Resolver {
        id: address.clone(),
        address,
        texts: Some(texts),
        content_hash,
        addresses: Some(addresses),
    }
}

/// Cache entries of a record family whose value was retained (`status == "success"`), paired with
/// the retained value. Values may arrive wrapped (`{"value": …}`) on some projection paths —
/// unwrap one level, matching REST's `compact_record_value`.
fn successful_entries<'a>(
    entries: &'a Value,
    record_family: &'a str,
) -> impl Iterator<Item = (&'a Value, String)> + 'a {
    json_items(entries)
        .filter(move |entry| json_str(entry, "record_family") == Some(record_family))
        .filter(|entry| json_str(entry, "status") == Some("success"))
        .filter_map(|entry| {
            let value = entry.get("value")?;
            let value = value.get("value").unwrap_or(value);
            Some((entry, value.as_str()?.to_owned()))
        })
}

fn json_items(value: &Value) -> impl Iterator<Item = &Value> {
    value.as_array().into_iter().flatten()
}

fn json_str<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

/// Subgraph `createdAt`/`expiryDate` are codegen-pinned `Int`. Saturating to `i32::MAX` keeps the
/// dashboard rendering for the Sepolia test scope; far-future (post-2038) expiries would need a
/// wider Manager scalar — out of scope per the plan.
fn unix_seconds_i32(timestamp: OffsetDateTime) -> i32 {
    i32::try_from(timestamp.unix_timestamp()).unwrap_or(i32::MAX)
}
