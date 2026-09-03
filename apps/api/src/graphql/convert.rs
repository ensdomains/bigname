use anyhow::Result;
use async_graphql::ID;
use bigname_storage::NameCurrentListRow;
use serde_json::Value;

use super::record_inventory_query::PhaseGraphqlRecordInventoryRow;
use super::{
    objects::{AddressRecord, Domain, Resolver},
    scalars::{BigInt, Bytes},
};

/// Non-null `owner` fallback for ownerless names (all-zero address).
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Mirrors the REST row→fields mapping (`responses/app_facing/names_collection.rs`) so GraphQL and
/// REST agree on the derived `owner`/`tokenId`/dates/`resolver`. `owner` resolves the non-null
/// `Account!` fallback chain here so the resolver stays trivial.
impl From<NameCurrentListRow> for Domain {
    fn from(row: NameCurrentListRow) -> Self {
        let record_inventory_key = row.row.record_serving_resource_id().map(|resource_id| {
            let boundary = row
                .row
                .declared_summary
                .pointer("/topology/version_boundaries/record_version_boundary")
                .cloned();
            (resource_id, boundary)
        });
        let owner_id = non_empty(row.owner)
            .or_else(|| non_empty(row.registrant))
            .unwrap_or_else(|| ZERO_ADDRESS.to_owned());
        Self {
            id: ID(row.row.namehash),
            name: Some(row.row.canonical_display_name),
            normalized_name: Some(row.row.normalized_name),
            token_id: non_empty(row.token_id),
            // The GraphQL SDL pins `createdAt` non-null (`BigInt!`), while the phase projection has
            // no legacy surface-creation timestamp. Preserve the response shape with epoch zero
            // when neither declared registration nor history supplies a timestamp.
            created_at: BigInt::from_i64(
                row.created_at
                    .map(|value| value.unix_timestamp())
                    .unwrap_or(0),
            ),
            // The projection already maps max or otherwise unrepresentable ENSv2 expiry values to
            // null; keep that documented divergence while removing the former i32 saturation.
            // (upstream: .refs/ens_v2/contracts/src/reverse-registrar/StandaloneReverseRegistrar.sol:L175-L176 @ ens_v2@a971bd64)
            expiry_date: row
                .expiry_date
                .map(|value| BigInt::from_i64(value.unix_timestamp())),
            resolver_address: non_empty(row.resolver_address),
            owner_id,
            record_inventory_key,
            served_head: None,
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
    namehash: &str,
    inventory: Option<&PhaseGraphqlRecordInventoryRow>,
) -> Result<Resolver> {
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
                        coin_type_big: coin_type.to_string(),
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

    let address = Bytes::parse_string(address)
        .map_err(|message| anyhow::anyhow!("projected resolver address is not bytes: {message}"))?;
    Ok(Resolver {
        id: ID(composite_resolver_id(address.as_str(), namehash)),
        address,
        texts: Some(texts),
        content_hash,
        addresses: Some(addresses),
    })
}

pub(super) fn composite_resolver_id(address: &str, namehash: &str) -> String {
    format!(
        "{}-{}",
        address.to_ascii_lowercase(),
        namehash.to_ascii_lowercase()
    )
}

/// Cache entries of a record family whose value was retained (`status == "success"`), paired with
/// the retained value flattened to its wire string.
fn successful_entries<'a>(
    entries: &'a Value,
    record_family: &'a str,
) -> impl Iterator<Item = (&'a Value, String)> + 'a {
    json_items(entries)
        .filter(move |entry| json_str(entry, "record_family") == Some(record_family))
        .filter(|entry| json_str(entry, "status") == Some("success"))
        .filter_map(|entry| Some((entry, entry_value_string(entry.get("value")?)?)))
}

/// Flatten a retained cache value to its wire string. Addr values arrive as a bare hex string;
/// contenthash values arrive as `{"encoding":"hex","bytes":"0x…"}`; some projection paths wrap the
/// value as `{"value": …}`. Unwrap one `value` level, then accept a bare string or the `bytes` hex
/// — the raw on-chain hex the subgraph `addresses`/`contentHash` fields expose verbatim.
fn entry_value_string(value: &Value) -> Option<String> {
    let value = value.get("value").unwrap_or(value);
    if let Some(text) = value.as_str() {
        return Some(text.to_owned());
    }
    value
        .get("bytes")
        .and_then(Value::as_str)
        .map(str::to_owned)
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
