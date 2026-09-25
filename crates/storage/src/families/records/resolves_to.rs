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
//! `name_current` for name eligibility (the family read model for it is step 3). Exact entries shadow the ENSIP-19 default address as the
//! forward read does. The page is a keyset over the result order with no publication binding.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_domain::resolver_read::ensip19_default_fallback_target;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    candidates::candidate_resources,
    inventory::{FamilyAttribution, FamilyRecordInventory, load_family_record_inventory_detail},
    payload::strip_nulls,
};
use crate::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedCursorValue,
    AddressRecordCurrentEntry, AddressRecordsCurrentPage, ENSIP19_DEFAULT_ADDRESS_RECORD_KEY,
};

const ZERO: &str = "0x0000000000000000000000000000000000000000";

/// One `address_records_current`-shaped row the families derive for a resource.
struct RecordRow {
    record_resource_id: Uuid,
    record_key: String,
    coin_type: String,
    provenance: Value,
    chain_positions: Value,
}

fn may_fall_back(coin_type: &str) -> bool {
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
fn record_rows(chain_id: &str, inventory: &FamilyRecordInventory, address: &str) -> Vec<RecordRow> {
    let row = &inventory.row;
    if row.coverage.get("status").and_then(Value::as_str) != Some("projected") {
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

/// A family page of names resolving to an address, with the entries the derived address index
/// alone would not have found: each is (record resource, record key) of an entry whose resource
/// the index did not reach through that record key's coin type.
#[derive(Clone, Debug)]
pub struct FamilyAddressRecordsPage {
    pub page: AddressRecordsCurrentPage,
    pub index_misses: Vec<(Uuid, String)>,
}

/// Load a page of names whose `addr:<coin_type>` record resolves to `address`, over the families.
/// The arguments are those of `load_address_records_current_page`; this shadow reader supports the
/// name sort without an authority filter, which is what the harness compares.
#[allow(clippy::too_many_arguments)]
pub async fn load_family_address_records_page(
    pool: &PgPool,
    address: &str,
    coin_type: &str,
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
    authority: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressRecordsCurrentPage> {
    Ok(load_family_address_records_page_detail(
        pool, address, coin_type, namespaces, dedupe_by, q, authority, sort, order, cursor,
        page_size,
    )
    .await?
    .page)
}

/// [`load_family_address_records_page`] with the entries only the retained values found.
#[allow(clippy::too_many_arguments)]
pub async fn load_family_address_records_page_detail(
    pool: &PgPool,
    address: &str,
    coin_type: &str,
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
    authority: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<FamilyAddressRecordsPage> {
    if sort != AddressNamesCurrentSort::Name || authority.is_some() {
        bail!("the family address reader supports the name sort without an authority filter");
    }
    let address = address.to_ascii_lowercase();
    let mut coin_types = vec![coin_type.to_owned()];
    if may_fall_back(coin_type) {
        coin_types.push(ENSIP19_DEFAULT_ADDRESS_RECORD_KEY["addr:".len()..].to_owned());
    }
    let candidates = candidate_resources(pool, &address, &coin_types).await?;
    let mut records = Vec::new();
    let mut indexed = BTreeMap::new();
    for ((chain_id, resource_id), candidate) in candidates {
        if indexed.contains_key(&resource_id) {
            continue;
        }
        indexed.insert(resource_id, candidate.indexed_coin_types);
        let Some(inventory) = load_family_record_inventory_detail(
            pool,
            &chain_id,
            resource_id,
            FamilyAttribution::Given(BTreeSet::new()),
        )
        .await?
        else {
            continue;
        };
        records.extend(record_rows(&chain_id, &inventory, &address));
    }
    let page = page(
        pool, &address, coin_type, &records, namespaces, dedupe_by, q, order, cursor, page_size,
    )
    .await?;
    let index_misses = page
        .entries
        .iter()
        .filter(|entry| {
            let coin = entry.record_key.trim_start_matches("addr:");
            indexed
                .get(&entry.record_resource_id)
                .is_none_or(|coins: &BTreeSet<String>| !coins.contains(coin))
        })
        .map(|entry| (entry.record_resource_id, entry.record_key.clone()))
        .collect();
    Ok(FamilyAddressRecordsPage { page, index_misses })
}

#[allow(clippy::too_many_arguments)]
async fn page(
    pool: &PgPool,
    address: &str,
    coin_type: &str,
    records: &[RecordRow],
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressRecordsCurrentPage> {
    let limit = i64::try_from(page_size).context("page size too large")?;
    let mut builder = QueryBuilder::<Postgres>::new(
        "WITH records AS (
             SELECT * FROM unnest(",
    );
    builder.push_bind(
        records
            .iter()
            .map(|r| r.record_resource_id)
            .collect::<Vec<_>>(),
    );
    builder.push("::uuid[], ");
    builder.push_bind(
        records
            .iter()
            .map(|r| r.record_key.clone())
            .collect::<Vec<_>>(),
    );
    builder.push("::text[], ");
    builder.push_bind(
        records
            .iter()
            .map(|r| r.coin_type.clone())
            .collect::<Vec<_>>(),
    );
    builder.push("::text[], ");
    builder.push_bind(
        records
            .iter()
            .map(|r| r.provenance.clone())
            .collect::<Vec<_>>(),
    );
    builder.push("::jsonb[], ");
    builder.push_bind(
        records
            .iter()
            .map(|r| r.chain_positions.clone())
            .collect::<Vec<_>>(),
    );
    builder.push(
        "::jsonb[]) AS record (record_resource_id, record_key, coin_type, provenance,
                                   chain_positions)
         ),
         filtered AS (
             SELECT name.logical_name_id, name.namespace, name.raw_name AS canonical_display_name,
                    name.namehash, name.surface_binding_id, name.resource_id AS authority_resource_id,
                    COALESCE(name.resource_id, record.record_resource_id) AS resource_id,
                    record.record_resource_id, name.binding_kind, record.record_key,
                    record.provenance || jsonb_build_object('logical_name_id', name.logical_name_id)
                        AS provenance,
                    record.chain_positions,
                    CASE WHEN record.coin_type = ",
    );
    builder.push_bind(coin_type.to_owned());
    builder.push(
        " THEN 0 ELSE 1 END AS record_rank
             FROM records record
             JOIN bigname_phase.name_current name
               ON COALESCE(name.serving_resource_id, name.resource_id) = record.record_resource_id
              AND (name.serving_resource_id IS NOT NULL
                   OR (name.surface_binding_id IS NOT NULL AND name.resource_id IS NOT NULL
                       AND name.binding_kind IS NOT NULL
                       AND name.declared_summary #>> '{control,status}'
                           IS DISTINCT FROM 'unregistered'))
             JOIN bigname_phase.name_surfaces surface
               ON surface.logical_name_id = name.logical_name_id
              AND surface.canonicality_state IN ('canonical', 'safe', 'finalized')
             WHERE (record.coin_type = ",
    );
    builder.push_bind(coin_type.to_owned());
    builder.push(" OR (");
    builder.push_bind(may_fall_back(coin_type));
    builder.push(" AND record.record_key = ");
    builder.push_bind(ENSIP19_DEFAULT_ADDRESS_RECORD_KEY);
    builder.push(
        " AND record.provenance ->> 'ensip19_default_address' = 'true'
          AND NOT COALESCE(record.provenance -> 'shadowed_coin_types' ? ",
    );
    builder.push_bind(coin_type.to_owned());
    builder.push(", false)))");
    if let Some(namespaces) = namespaces {
        builder.push(" AND name.namespace = ANY(");
        builder.push_bind(namespaces.to_vec());
        builder.push(")");
    }
    if let Some(prefix) = q {
        let escaped = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        builder.push(" AND name.raw_name LIKE ");
        builder.push_bind(format!("{escaped}%"));
        builder.push(" ESCAPE '\\'");
    }
    let dedupe = match dedupe_by {
        AddressNamesCurrentDedupe::Surface => "logical_name_id",
        AddressNamesCurrentDedupe::Resource => "resource_id",
    };
    builder.push(format!(
        "),
         entries AS (
             SELECT DISTINCT ON ({dedupe}) * FROM filtered
             ORDER BY {dedupe}, record_rank ASC, canonical_display_name ASC, logical_name_id ASC
         )
         SELECT * FROM entries WHERE TRUE"
    ));
    let direction = match order {
        AddressNamesCurrentOrder::Asc => ("> ", "ASC"),
        AddressNamesCurrentOrder::Desc => ("< ", "DESC"),
    };
    if let Some(cursor) = cursor {
        let AddressNamesCurrentSortedCursorValue::Name(value) = &cursor.sort_value else {
            bail!("the family address reader's cursor must carry a name");
        };
        builder.push(" AND (canonical_display_name ");
        builder.push(direction.0);
        builder.push_bind(value.clone());
        builder.push(" OR (canonical_display_name = ");
        builder.push_bind(value.clone());
        builder.push(" AND (logical_name_id, resource_id::TEXT) > (");
        builder.push_bind(cursor.logical_name_id.clone());
        builder.push(", ");
        builder.push_bind(cursor.resource_id.to_string());
        builder.push(")))");
    }
    builder.push(format!(
        " ORDER BY canonical_display_name {}, logical_name_id ASC, resource_id::TEXT ASC LIMIT ",
        direction.1
    ));
    builder.push_bind(limit + 1);
    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .with_context(|| format!("failed to page the family address records of {address}"))?;
    let mut entries = rows
        .into_iter()
        .map(|row| {
            let name: String = row.try_get("canonical_display_name")?;
            Ok(AddressRecordCurrentEntry {
                address: address.to_owned(),
                logical_name_id: row.try_get("logical_name_id")?,
                namespace: row.try_get("namespace")?,
                canonical_display_name: name.clone(),
                normalized_name: name,
                namehash: row.try_get("namehash")?,
                surface_binding_id: row.try_get("surface_binding_id")?,
                resource_id: row.try_get("authority_resource_id")?,
                record_resource_id: row.try_get("record_resource_id")?,
                binding_kind: crate::sql_row::get(&row, "binding_kind")?,
                coin_type: coin_type.to_owned(),
                record_key: row.try_get("record_key")?,
                provenance: row.try_get("provenance")?,
                coverage: json!({"status": "projected", "exhaustiveness": "not_asserted"}),
                chain_positions: row.try_get("chain_positions")?,
                canonicality_summary: json!({"state": "canonical_lineage"}),
                manifest_version: 1,
                last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let page_size = usize::try_from(page_size).unwrap_or(usize::MAX);
    let next_cursor = (entries.len() > page_size).then(|| {
        entries.truncate(page_size);
        entries.last().map(|entry| AddressNamesCurrentSortedCursor {
            sort_value: AddressNamesCurrentSortedCursorValue::Name(
                entry.canonical_display_name.clone(),
            ),
            logical_name_id: entry.logical_name_id.clone(),
            resource_id: entry.resource_id.unwrap_or(entry.record_resource_id),
        })
    });
    Ok(AddressRecordsCurrentPage {
        entries,
        next_cursor: next_cursor.flatten(),
    })
}
