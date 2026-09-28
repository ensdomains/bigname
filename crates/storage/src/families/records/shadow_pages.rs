//! The production page comparisons of TYR-36 step 7b slice 3, beside the step 4 reads in
//! `shadow.rs`: every address either side knows paged through the served address-names reader
//! and the F13 reader (`address_names.rs`) in the served sorts, orders, dedupe modes and filters;
//! the resolves_to pages of the production reader (F14 candidates only, `resolves_to_serving.rs`)
//! for one coin type and for `coin_type=evm`; and every address's primary-name claims read in one
//! batch. Each side is paged with its own cursors; the entries, the continuations and the grouped
//! total must agree.
//!
//! The address-names entry comparison leaves out what the composed rows do not carry and no
//! route reads (`address_names.rs`): the served event attribution (`provenance`), the relation's
//! own block (`chain_positions`), the effective-controller support status (`coverage`),
//! `manifest_version` and `last_recomputed_at`. A resolves_to key the index alone misses
//! (`address_index_misses`), or whose step 4 comparison already differs, and a reverse tuple whose
//! single-tuple comparison differs, is already a finding under its own key and is not read again
//! here (nor is the EVM page of such an address).
//!
//! Each read is timed, served and family, so a run reports the composed latency per route.
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{
    Difference,
    address_names::load_family_address_names_page,
    compare::{TARGET, address_entry, diff, without},
    compare_primary_name,
    resolves_to_serving::{load_family_resolves_to_evm_page, load_family_resolves_to_page},
    shadow::ShadowReport,
};
use crate::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentDedupe,
    AddressNamesCurrentOrder, AddressNamesCurrentSort, AddressNamesCurrentSortedCursor,
    AddressRecordEvmEntry,
    address_names::{
        RowSource, load_address_names_page_from, load_address_records_evm_page_from,
        load_address_records_page_from,
    },
    primary_name::load_served_primary_name_current_snapshots,
};

/// Pages a reader may return for one key before the comparison gives up on it.
const MAX_PAGES: usize = 100_000;

/// One served page shape: sort, order, dedupe, relation filter and authority filter.
type Shape = (
    AddressNamesCurrentSort,
    AddressNamesCurrentOrder,
    AddressNamesCurrentDedupe,
    Option<&'static [AddressNameRelation]>,
    Option<&'static str>,
);

const SHAPES: [Shape; 8] = [
    (
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        AddressNamesCurrentDedupe::Surface,
        None,
        None,
    ),
    (
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Desc,
        AddressNamesCurrentDedupe::Surface,
        None,
        None,
    ),
    (
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        AddressNamesCurrentDedupe::Resource,
        None,
        None,
    ),
    (
        AddressNamesCurrentSort::ExpiresAt,
        AddressNamesCurrentOrder::Asc,
        AddressNamesCurrentDedupe::Surface,
        None,
        None,
    ),
    (
        AddressNamesCurrentSort::RegisteredAt,
        AddressNamesCurrentOrder::Desc,
        AddressNamesCurrentDedupe::Surface,
        None,
        None,
    ),
    (
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        AddressNamesCurrentDedupe::Surface,
        Some(&[AddressNameRelation::Registrant]),
        None,
    ),
    (
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        AddressNamesCurrentDedupe::Surface,
        Some(&[AddressNameRelation::EffectiveController]),
        None,
    ),
    (
        AddressNamesCurrentSort::Name,
        AddressNamesCurrentOrder::Asc,
        AddressNamesCurrentDedupe::Surface,
        None,
        Some("ens_v1"),
    ),
];

fn shape_label(shape: &Shape) -> String {
    format!(
        "sort {} order {} dedupe {:?} relations {:?} authority {:?}",
        shape.0.as_str(),
        shape.1.as_str(),
        shape.2,
        shape.3,
        shape.4
    )
}

/// Time a served and a family read under `label`.
fn time(report: &mut ShadowReport, label: &'static str, served_us: u128, family_us: u128) {
    let entry = report.read_timings.entry(label).or_default();
    entry.0 += served_us;
    entry.1 += family_us;
    entry.2 += 1;
}

pub(super) async fn production_pages(
    pool: &PgPool,
    chain_id: &str,
    page_size: u64,
    excused: &BTreeSet<String>,
    report: &mut ShadowReport,
) -> Result<()> {
    address_names(pool, chain_id, page_size, excused, report).await?;
    resolves_to(pool, chain_id, page_size, excused, report).await?;
    primary_batches(pool, chain_id, report).await
}

/// Compare the two sides of one key, `{"entries": [...], "pages": [...], ...}` each. When an
/// excused name is listed on either side, its composed row differs from the served one by a cause
/// the control comparison decides and may be placed or filtered differently, so the entries are
/// compared as one sequence without the excused names and the pages are not compared
/// (`listing_excused`, as the listing comparisons of the topology harness do).
fn compare_sides(
    report: &mut ShadowReport,
    key: String,
    mut sides: Vec<Value>,
    excused: &BTreeSet<String>,
) {
    let listed = |side: &Value| -> bool {
        side["entries"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|entry| {
                let name = entry
                    .get("logical_name_id")
                    .or_else(|| entry.pointer("/entry/logical_name_id"))
                    .and_then(Value::as_str);
                name.is_some_and(|name| excused.contains(name))
            })
    };
    if sides.iter().any(listed) {
        report.listing_excused += 1;
        for side in &mut sides {
            let kept: Vec<Value> = side["entries"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|entry| {
                    let name = entry
                        .get("logical_name_id")
                        .or_else(|| entry.pointer("/entry/logical_name_id"))
                        .and_then(Value::as_str);
                    !name.is_some_and(|name| excused.contains(name))
                })
                .cloned()
                .collect();
            *side = json!({"entries": kept});
        }
    }
    let mut differences = Vec::new();
    diff(&mut differences, "", &sides[0], &sides[1]);
    if !differences.is_empty() {
        report.differences.push((key, differences));
    }
}

fn address_name_view(entry: &AddressNameCurrentEntry) -> Value {
    json!({
        "address": entry.address,
        "logical_name_id": entry.logical_name_id,
        "namespace": entry.namespace,
        "canonical_display_name": entry.canonical_display_name,
        "normalized_name": entry.normalized_name,
        "namehash": entry.namehash,
        "surface_binding_id": entry.surface_binding_id.to_string(),
        "resource_id": entry.resource_id.to_string(),
        "token_lineage_id": entry.token_lineage_id.map(|id| id.to_string()),
        "binding_kind": entry.binding_kind.as_str(),
        "relations": entry.relations.iter().map(|relation| relation.as_str()).collect::<Vec<_>>(),
        "canonicality_summary": without(&entry.canonicality_summary, &TARGET),
    })
}

fn continuation(cursor: Option<&AddressNamesCurrentSortedCursor>) -> String {
    format!("{cursor:?}")
}

async fn address_names(
    pool: &PgPool,
    chain_id: &str,
    page_size: u64,
    excused: &BTreeSet<String>,
    report: &mut ShadowReport,
) -> Result<()> {
    let addresses: Vec<String> = sqlx::query_scalar(
        "SELECT address FROM bigname_phase.address_names_current
         WHERE provenance ->> 'chain_id' = $1
         UNION
         SELECT address FROM bigname_phase.project_address_name_index WHERE chain_id = $1
         ORDER BY 1",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("failed to list the addresses whose names to compare")?;
    for address in addresses {
        report.address_name_addresses += 1;
        for shape in SHAPES {
            let mut sides = Vec::new();
            for family in [false, true] {
                let (mut entries, mut pages, mut totals, mut cursor) =
                    (Vec::new(), Vec::new(), BTreeSet::new(), None);
                loop {
                    let started = Instant::now();
                    let page = if family {
                        load_family_address_names_page(
                            pool,
                            &address,
                            None,
                            shape.3,
                            shape.2,
                            None,
                            shape.4,
                            None,
                            shape.0,
                            shape.1,
                            cursor.as_ref(),
                            page_size,
                        )
                        .await?
                    } else {
                        let mut conn = pool.acquire().await?;
                        load_address_names_page_from(
                            &mut conn,
                            RowSource::Served,
                            &address,
                            None,
                            shape.3,
                            shape.2,
                            None,
                            shape.4,
                            None,
                            shape.0,
                            shape.1,
                            cursor.as_ref(),
                            page_size,
                        )
                        .await?
                    };
                    let elapsed = started.elapsed().as_micros();
                    if family {
                        time(report, "address_names", 0, elapsed);
                    } else {
                        time(report, "address_names", elapsed, 0);
                        report.address_name_pages += 1;
                        report.address_name_entries += page.entries.len();
                    }
                    entries.extend(page.entries.iter().map(address_name_view));
                    pages.push(continuation(page.next_cursor.as_ref()));
                    totals.insert(page.summary.grouped_entry_count);
                    ensure!(pages.len() < MAX_PAGES, "the pages of {address} do not end");
                    match page.next_cursor {
                        Some(next) => cursor = Some(next),
                        None => break,
                    }
                }
                sides.push(json!({"entries": entries, "pages": pages, "totals": totals}));
            }
            compare_sides(
                report,
                format!("address_names {address} {}", shape_label(&shape)),
                sides,
                excused,
            );
        }
    }
    // A timing entry counts one pair of reads.
    if let Some(entry) = report.read_timings.get_mut("address_names") {
        entry.2 /= 2;
    }
    Ok(())
}

fn evm_view(entry: &AddressRecordEvmEntry) -> Value {
    json!({
        "entry": address_entry(&entry.entry),
        "resolutions": entry.resolutions.iter()
            .map(|resolution| json!([resolution.coin_type, resolution.record_key]))
            .collect::<Vec<_>>(),
        "representative_coin_types": entry.representative_coin_types,
        "matched_coin_type_count": entry.matched_coin_type_count,
    })
}

async fn resolves_to(
    pool: &PgPool,
    chain_id: &str,
    page_size: u64,
    excused: &BTreeSet<String>,
    report: &mut ShadowReport,
) -> Result<()> {
    let keys: Vec<(String, String)> = sqlx::query_as(
        "SELECT address, coin_type FROM bigname_phase.address_records_current
         WHERE provenance ->> 'chain_id' = $1
         UNION
         SELECT address, coin_type FROM bigname_phase.project_address_record_node_index
         WHERE chain_id = $1
         UNION
         SELECT address, coin_type FROM bigname_phase.project_address_record_id_index
         WHERE chain_id = $1
         ORDER BY 1, 2",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("failed to list the resolves_to keys to compare")?;
    let missed: BTreeSet<String> = report
        .address_index_misses
        .iter()
        .filter_map(|miss| {
            let mut words = miss.split(' ');
            Some(format!("{} {}", words.nth(1)?, words.nth(1)?))
        })
        .collect();
    let reported: BTreeSet<String> = report
        .differences
        .iter()
        .map(|(key, _)| key.clone())
        .collect();
    let mut addresses = BTreeSet::new();
    let mut settled = BTreeSet::new();
    for (address, coin_type) in keys {
        if missed.contains(&format!("{address} {coin_type}"))
            || reported.contains(&format!("resolves_to {address} coin {coin_type}"))
        {
            settled.insert(address.clone());
        }
        addresses.insert(address.clone());
        // A key the index misses, or whose step 4 comparison already differs, is a finding
        // already reported under its own key; the production pages would only repeat it.
        if missed.contains(&format!("{address} {coin_type}"))
            || reported.contains(&format!("resolves_to {address} coin {coin_type}"))
        {
            continue;
        }
        for order in [
            AddressNamesCurrentOrder::Asc,
            AddressNamesCurrentOrder::Desc,
        ] {
            let mut sides = Vec::new();
            for family in [false, true] {
                let (mut entries, mut pages, mut cursor) = (Vec::new(), Vec::new(), None);
                loop {
                    let started = Instant::now();
                    let page = if family {
                        load_family_resolves_to_page(
                            pool,
                            &address,
                            &coin_type,
                            None,
                            AddressNamesCurrentDedupe::Surface,
                            None,
                            None,
                            AddressNamesCurrentSort::Name,
                            order,
                            cursor.as_ref(),
                            page_size,
                        )
                        .await?
                    } else {
                        let mut conn = pool.acquire().await?;
                        load_address_records_page_from(
                            &mut conn,
                            RowSource::Served,
                            &address,
                            &coin_type,
                            None,
                            AddressNamesCurrentDedupe::Surface,
                            None,
                            None,
                            AddressNamesCurrentSort::Name,
                            order,
                            cursor.as_ref(),
                            page_size,
                        )
                        .await?
                    };
                    let elapsed = started.elapsed().as_micros();
                    let (served_us, family_us) = if family { (0, elapsed) } else { (elapsed, 0) };
                    time(report, "resolves_to", served_us, family_us);
                    report.production_address_pages += usize::from(!family);
                    entries.extend(page.entries.iter().map(address_entry));
                    pages.push(continuation(page.next_cursor.as_ref()));
                    ensure!(pages.len() < MAX_PAGES, "the pages of {address} do not end");
                    match page.next_cursor {
                        Some(next) => cursor = Some(next),
                        None => break,
                    }
                }
                sides.push(json!({"entries": entries, "pages": pages}));
            }
            compare_sides(
                report,
                format!(
                    "resolves_to_page {address} coin {coin_type} {}",
                    order.as_str()
                ),
                sides,
                excused,
            );
        }
    }
    for address in addresses.difference(&settled) {
        let address = address.clone();
        for dedupe in [
            AddressNamesCurrentDedupe::Surface,
            AddressNamesCurrentDedupe::Resource,
        ] {
            let mut sides = Vec::new();
            for family in [false, true] {
                let (mut entries, mut pages, mut cursor) = (Vec::new(), Vec::new(), None);
                loop {
                    let started = Instant::now();
                    let page = if family {
                        load_family_resolves_to_evm_page(
                            pool,
                            &address,
                            None,
                            dedupe,
                            None,
                            None,
                            AddressNamesCurrentSort::Name,
                            AddressNamesCurrentOrder::Asc,
                            cursor.as_ref(),
                            page_size,
                        )
                        .await?
                    } else {
                        let mut conn = pool.acquire().await?;
                        load_address_records_evm_page_from(
                            &mut conn,
                            RowSource::Served,
                            &address,
                            None,
                            dedupe,
                            None,
                            None,
                            AddressNamesCurrentSort::Name,
                            AddressNamesCurrentOrder::Asc,
                            cursor.as_ref(),
                            page_size,
                        )
                        .await?
                    };
                    let elapsed = started.elapsed().as_micros();
                    let (served_us, family_us) = if family { (0, elapsed) } else { (elapsed, 0) };
                    time(report, "resolves_to_evm", served_us, family_us);
                    report.evm_pages += usize::from(!family);
                    entries.extend(page.entries.iter().map(evm_view));
                    pages.push(continuation(page.next_cursor.as_ref()));
                    ensure!(pages.len() < MAX_PAGES, "the pages of {address} do not end");
                    match page.next_cursor {
                        Some(next) => cursor = Some(next),
                        None => break,
                    }
                }
                sides.push(json!({"entries": entries, "pages": pages}));
            }
            compare_sides(
                report,
                format!("resolves_to_evm {address} dedupe {dedupe:?}"),
                sides,
                excused,
            );
        }
    }
    for label in ["resolves_to", "resolves_to_evm"] {
        if let Some(entry) = report.read_timings.get_mut(label) {
            entry.2 /= 2;
        }
    }
    Ok(())
}

/// Every address's claims in one batch read per side, as the resolves_to EVM route reads them.
async fn primary_batches(pool: &PgPool, chain_id: &str, report: &mut ShadowReport) -> Result<()> {
    let tuples: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT address, namespace, coin_type FROM bigname_phase.primary_names_current
         WHERE claim_provenance ->> 'chain_id' = $1
         UNION
         SELECT address, namespace, coin_type FROM bigname_phase.project_reverse_tuple
         WHERE chain_id = $1 AND reverse_position IS NOT NULL",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("failed to list the reverse tuples to batch")?;
    let mut by_address: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for (address, namespace, coin_type) in tuples {
        by_address
            .entry(address)
            .or_default()
            .push((namespace, coin_type));
    }
    let reported: BTreeSet<String> = report
        .differences
        .iter()
        .map(|(key, _)| key.clone())
        .collect();
    for (address, keys) in by_address {
        report.primary_batches += 1;
        let started = Instant::now();
        let today = load_served_primary_name_current_snapshots(pool, &address, &keys).await?;
        let served_us = started.elapsed().as_micros();
        let started = Instant::now();
        let family =
            super::primary::load_family_primary_name_snapshots(pool, &address, &keys).await?;
        time(
            report,
            "primary_names",
            served_us,
            started.elapsed().as_micros(),
        );
        for key in &keys {
            // A tuple whose single-tuple comparison already differs is reported there.
            if reported.contains(&format!("primary_name {address} {} {}", key.0, key.1)) {
                continue;
            }
            let differences: Vec<Difference> =
                compare_primary_name(today.get(key), family.get(key));
            if !differences.is_empty() {
                report.differences.push((
                    format!("primary_batch {address} {} {}", key.0, key.1),
                    differences,
                ));
            }
        }
    }
    Ok(())
}
