//! The shadow comparison the harness runs at a publication: every record inventory row, the
//! complete sequence of names resolving to each address either side knows, and every reverse
//! claim, read through today's readers and through the family readers, compared field by field.
//! It runs only when the family marker equals the served marker; otherwise the families lag and
//! the comparison is not evidence. Every difference is reported; nothing is set apart. The
//! diagnostics beside them (index misses, node claims at another resolver, classification
//! fallbacks) say why a family read took another path, and never hold a difference.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    Difference, FamilyAttribution, check_compatibility_pairs, compare_address_results,
    compare_primary_name, compare_record_inventory, load_family_address_records,
    load_family_record_inventory_detail, load_family_reverse_claim, page_family_address_records,
    pair_oracle,
};
use crate::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    AddressNamesCurrentSortedCursor, AddressRecordCurrentEntry, load_address_records_current_page,
    load_bounded_record_attribution, load_primary_name_current_snapshot,
    load_record_inventory_current,
};

/// What one shadow comparison saw. `differences` holds every key whose family read differs from
/// today's read outside the rules the comparison excludes, each with its differing fields.
#[derive(Clone, Debug, Default)]
pub struct ShadowReport {
    pub family_marker: Option<(i64, String)>,
    pub served_marker: Option<(i64, String)>,
    pub inventory_rows: usize,
    pub compatibility_pairs: usize,
    pub address_pages: usize,
    pub address_entries: usize,
    pub primary_tuples: usize,
    pub differences: Vec<(String, Vec<Difference>)>,
    /// Diagnostic: the reverse tuples whose node has a claim in the family only at another
    /// resolver than its current one (`primary_name <address> <namespace> <coin type>`); any
    /// difference on such a tuple is still in `differences`.
    pub node_claims_at_other_resolver: Vec<String>,
    /// Diagnostic, not an exclusion: every family entry of a name resolving to an address that
    /// the derived address index alone would not have found, as `resolves_to <address> coin
    /// <coin> resource <id> <record key>`. The family read finds these from the retained values.
    /// The index is meant to be a superset, so a non-empty list is itself a finding about the
    /// index; a test lists the ones it expects and the end-to-end fixture requires none.
    pub address_index_misses: Vec<String>,
    /// Diagnostic: the resolvers the classification read took from `resolver_current` because
    /// F3 has no served row for them (`resolver <address>`). With F3 written block by block this
    /// is empty; a test that expects none can require it.
    pub classification_fallbacks: Vec<String>,
}

impl ShadowReport {
    /// Whether the families stood at the served marker, so the comparison ran.
    pub fn current(&self) -> bool {
        self.family_marker.is_some() && self.family_marker == self.served_marker
    }
}

/// Compare every shadow read on `chain_id`. `served` is the served marker; `None` reads it from
/// the Project row of `chain_phase_state`. `page_size` is the address page size, small enough that
/// an address with several names spans pages.
pub async fn compare_family_reads(
    pool: &PgPool,
    chain_id: &str,
    served: Option<(i64, String)>,
    page_size: u64,
) -> Result<ShadowReport> {
    let family_marker: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT current_block_number, current_block_hash
         FROM bigname_phase.project_family_marker WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .context("failed to read the family marker")?;
    let served = match served {
        Some(served) => Some(served),
        None => sqlx::query_as::<_, (Option<i64>, Option<String>)>(
            "SELECT current_block_number, current_block_hash
             FROM bigname_phase.chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'project'",
        )
        .bind(chain_id)
        .fetch_optional(pool)
        .await
        .context("failed to read the served marker")?
        .and_then(|(number, hash)| number.zip(hash)),
    };
    let mut report = ShadowReport {
        family_marker: family_marker.and_then(|(number, hash)| number.zip(hash)),
        served_marker: served,
        ..ShadowReport::default()
    };
    if !report.current() {
        return Ok(report);
    }
    inventory(pool, chain_id, &mut report).await?;
    addresses(pool, chain_id, page_size, &mut report).await?;
    primary(pool, chain_id, &mut report).await?;
    report.classification_fallbacks = sqlx::query_scalar(
        "SELECT 'resolver ' || resolver.resolver_address
         FROM bigname_phase.resolver_current resolver
         WHERE resolver.chain_id = $1
           AND NOT EXISTS (
               SELECT 1 FROM bigname_phase.project_resolver_classification family
               WHERE family.chain_id = resolver.chain_id
                 AND family.resolver_address = resolver.resolver_address
                 AND family.unsupported_reason
                     IS DISTINCT FROM 'resolver_manifest_not_active'
           )
         ORDER BY 1",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("failed to list the resolvers classified without F3")?;
    Ok(report)
}

async fn inventory(pool: &PgPool, chain_id: &str, report: &mut ShadowReport) -> Result<()> {
    let resources: Vec<Uuid> = sqlx::query_scalar(
        "SELECT ric.resource_id FROM bigname_phase.record_inventory_current ric
         JOIN bigname_phase.resources resource USING (resource_id)
         WHERE resource.chain_id = $1
         UNION
         SELECT resource_id FROM bigname_phase.project_resource_pointer WHERE chain_id = $1
         ORDER BY 1",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("failed to list the resources to compare")?;
    // The attribution Project publishes at its target, which the bounded reader reproduces at the
    // served block.
    let bound = report
        .served_marker
        .as_ref()
        .map(|(block, _)| BTreeMap::from([(chain_id.to_owned(), *block)]));
    let mut attribution = load_bounded_record_attribution(pool, &resources, bound.as_ref()).await?;
    for resource_id in resources {
        let family = load_family_record_inventory_detail(
            pool,
            chain_id,
            resource_id,
            FamilyAttribution::Given(attribution.remove(&resource_id).unwrap_or_default()),
        )
        .await?;
        // Today's readable row at the family row's boundary, else its first readable row.
        let boundaries: Vec<(String, Value)> = sqlx::query_as(
            "SELECT record_version_boundary_key, record_version_boundary
             FROM bigname_phase.record_inventory_current
             WHERE resource_id = $1
             ORDER BY record_version_boundary_key = $2 DESC, record_version_boundary_key",
        )
        .bind(resource_id)
        .bind(
            family
                .as_ref()
                .map(|family| family.record_version_boundary_key.clone()),
        )
        .fetch_all(pool)
        .await?;
        let mut today = None;
        for (_, boundary) in &boundaries {
            if let Some(row) = load_record_inventory_current(pool, resource_id, boundary).await? {
                today = Some(row);
                break;
            }
        }
        report.inventory_rows += usize::from(today.is_some() || family.is_some());
        let mut differences =
            compare_record_inventory(today.as_ref(), family.as_ref().map(|family| &family.row));
        if let Some(family) = &family {
            report.compatibility_pairs += family.compatibility_pairs.len();
            let served = report.served_marker.as_ref().map(|(block, _)| *block);
            let expected =
                pair_oracle::expected_pairs(pool, chain_id, served, today.as_ref()).await?;
            differences.extend(check_compatibility_pairs(family, &expected));
        }
        if !differences.is_empty() {
            report
                .differences
                .push((format!("record_inventory {resource_id}"), differences));
        }
    }
    Ok(())
}

/// The ENSIP-19 default coin type (`0x80000000`).
const DEFAULT_COIN_TYPE: &str = "2147483648";
/// Coins the default answers that the comparison adds: ETH, and Base (`0x80000000 | 8453`).
const FALLBACK_COIN_TYPES: [&str; 2] = ["60", "2147492101"];

async fn addresses(
    pool: &PgPool,
    chain_id: &str,
    page_size: u64,
    report: &mut ShadowReport,
) -> Result<()> {
    let keys: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT address, coin_type FROM bigname_phase.address_records_current
         WHERE provenance ->> 'chain_id' = $1
         UNION
         SELECT address, coin_type FROM bigname_phase.project_address_record_node_index
         WHERE chain_id = $1
         UNION
         SELECT address, coin_type FROM bigname_phase.project_address_record_id_index
         WHERE chain_id = $1
         UNION
         SELECT address, coin_type FROM (
             {}
             UNION ALL
             {}
         ) retained
         WHERE address ~ '^0x[0-9a-f]{{40}}$'
           AND address <> '0x0000000000000000000000000000000000000000'
         ORDER BY 1, 2",
        retained_addresses("project_node_record_value", true),
        retained_addresses("project_record_id_value", false),
    ))
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("failed to list the addresses to compare")?;
    // A default-address row answers every eligible EVM coin, which no stored row names, so for
    // each address with one the comparison adds coin 60 and Base's coin to exercise the fallback.
    let mut keys: BTreeSet<(String, String)> = keys.into_iter().collect();
    let defaults: Vec<String> = keys
        .iter()
        .filter(|(_, coin_type)| coin_type == DEFAULT_COIN_TYPE)
        .map(|(address, _)| address.clone())
        .collect();
    for address in defaults {
        for coin_type in FALLBACK_COIN_TYPES {
            keys.insert((address.clone(), coin_type.to_owned()));
        }
    }
    for (address, coin_type) in keys {
        let (today, today_pages) = all_today_pages(pool, &address, &coin_type, page_size).await?;
        let (family, family_pages, misses) =
            all_family_pages(pool, &address, &coin_type, page_size).await?;
        report.address_pages += today_pages.len();
        report.address_entries += today.len();
        report
            .address_index_misses
            .extend(misses.iter().map(|(resource, record_key)| {
                format!("resolves_to {address} coin {coin_type} resource {resource} {record_key}")
            }));
        let differences = compare_address_results(&today, &today_pages, &family, &family_pages);
        if !differences.is_empty() {
            report.differences.push((
                format!("resolves_to {address} coin {coin_type}"),
                differences,
            ));
        }
    }
    Ok(())
}

/// Every retained address value of a value table as (address, coin type) rows: the row's value,
/// its raw address bytes, and its coin-60 pair sibling's value and raw address bytes.
fn retained_addresses(table: &str, with_sibling: bool) -> String {
    let text = |column: &str| {
        format!(
            "lower(CASE WHEN jsonb_typeof({column}) = 'string' THEN {column} #>> '{{}}'
                        ELSE COALESCE({column} ->> 'value', {column} ->> 'bytes') END)"
        )
    };
    let (sibling, sibling_bytes) = if with_sibling {
        (
            text("value.sibling_value"),
            "lower(value.sibling_address_bytes_hex)",
        )
    } else {
        ("NULL".to_owned(), "NULL")
    };
    format!(
        "SELECT candidate.address, value.selector_key::numeric::text AS coin_type
         FROM bigname_phase.{table} value
         CROSS JOIN LATERAL (VALUES ({}), ({sibling}), (lower(value.address_bytes_hex)),
                                    ({sibling_bytes})) candidate (address)
         WHERE value.chain_id = $1 AND value.record_family = 'addr'
           AND value.selector_key ~ '^[0-9]{{1,30}}$' AND candidate.address IS NOT NULL",
        text("value.value"),
    )
}

/// A page's continuation, for the page comparison.
fn continuation(cursor: Option<&AddressNamesCurrentSortedCursor>) -> String {
    format!("{cursor:?}")
}

/// Pages a reader may return for one address before the comparison gives up on it.
const MAX_PAGES: usize = 100_000;

async fn all_today_pages(
    pool: &PgPool,
    address: &str,
    coin_type: &str,
    page_size: u64,
) -> Result<(Vec<AddressRecordCurrentEntry>, Vec<String>)> {
    let (mut entries, mut pages, mut cursor) = (Vec::new(), Vec::new(), None);
    loop {
        let page = load_address_records_current_page(
            pool,
            address,
            coin_type,
            None,
            AddressNamesCurrentDedupe::Surface,
            None,
            None,
            AddressNamesCurrentSort::Name,
            AddressNamesCurrentOrder::Asc,
            cursor.as_ref(),
            page_size,
        )
        .await?;
        entries.extend(page.entries);
        pages.push(continuation(page.next_cursor.as_ref()));
        ensure!(
            pages.len() < MAX_PAGES,
            "today's pages of {address} do not end"
        );
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok((entries, pages)),
        }
    }
}

type FamilyPages = (
    Vec<AddressRecordCurrentEntry>,
    Vec<String>,
    Vec<(Uuid, String)>,
);

async fn all_family_pages(
    pool: &PgPool,
    address: &str,
    coin_type: &str,
    page_size: u64,
) -> Result<FamilyPages> {
    // The candidates and their inventories are loaded once; every page reads the same rows.
    let records = load_family_address_records(pool, address, coin_type).await?;
    let (mut entries, mut pages, mut misses, mut cursor) =
        (Vec::new(), Vec::new(), Vec::new(), None);
    loop {
        let page = page_family_address_records(
            pool,
            &records,
            None,
            AddressNamesCurrentDedupe::Surface,
            None,
            None,
            AddressNamesCurrentSort::Name,
            AddressNamesCurrentOrder::Asc,
            cursor.as_ref(),
            page_size,
        )
        .await?;
        misses.extend(page.index_misses);
        entries.extend(page.page.entries);
        pages.push(continuation(page.page.next_cursor.as_ref()));
        ensure!(
            pages.len() < MAX_PAGES,
            "the family pages of {address} do not end"
        );
        match page.page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok((entries, pages, misses)),
        }
    }
}

async fn primary(pool: &PgPool, chain_id: &str, report: &mut ShadowReport) -> Result<()> {
    let tuples: BTreeSet<(String, String, String)> = sqlx::query_as(
        "SELECT address, namespace, coin_type FROM bigname_phase.primary_names_current
         WHERE claim_provenance ->> 'chain_id' = $1
         UNION
         SELECT address, namespace, coin_type FROM bigname_phase.project_reverse_tuple
         WHERE chain_id = $1 AND reverse_position IS NOT NULL",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await
    .context("failed to list the reverse tuples to compare")?
    .into_iter()
    .collect();
    for (address, namespace, coin_type) in tuples {
        report.primary_tuples += 1;
        let today =
            load_primary_name_current_snapshot(pool, &address, &namespace, &coin_type).await?;
        let family =
            load_family_reverse_claim(pool, chain_id, &address, &namespace, &coin_type).await?;
        let differences = compare_primary_name(
            today.as_ref(),
            family.as_ref().map(|family| &family.snapshot),
        );
        let key = format!("primary_name {address} {namespace} {coin_type}");
        if family.is_some_and(|family| family.node_claim_at_other_resolver) {
            report.node_claims_at_other_resolver.push(key.clone());
        }
        if !differences.is_empty() {
            report.differences.push((key, differences));
        }
    }
    Ok(())
}
