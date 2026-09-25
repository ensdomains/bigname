//! The shadow comparison the harness runs at a publication: every record inventory row, every
//! page of names resolving to each indexed address, and every reverse claim, read through today's
//! readers and through the family readers, compared field by field. It runs only when the family
//! marker equals the served marker; otherwise the families lag and the comparison is not evidence.
use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    Difference, FamilyAttribution, check_compatibility_pairs, compare_address_records,
    compare_primary_name, compare_record_inventory, load_family_address_records_page_detail,
    load_family_record_inventory_detail, load_family_reverse_claim,
};
use crate::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    load_address_records_current_page, load_bounded_record_attribution,
    load_primary_name_current_snapshot, load_record_inventory_current,
};

/// What one shadow comparison saw. `differences` holds every key whose family read differs from
/// today's read outside the rules the comparison excludes; `node_claim_findings` holds the reverse
/// claims the node claim family cannot represent (one row per node, see
/// [`super::FamilyReverseClaim`]), kept apart as a known step 2 gap.
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
    pub node_claim_findings: Vec<(String, Vec<Difference>)>,
    /// Diagnostic, not an exclusion: every family entry of a name resolving to an address that
    /// the derived address index alone would not have found, as `resolves_to <address> coin
    /// <coin> resource <id> <record key>`. The family read finds these from the retained values;
    /// each names a step 2 index gap, and a test that expects none can require the list empty.
    pub address_index_misses: Vec<String>,
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
        .map(|(block, _)| std::collections::BTreeMap::from([(chain_id.to_owned(), *block)]));
    let mut attribution = load_bounded_record_attribution(pool, &resources, bound.as_ref()).await?;
    for resource_id in resources {
        let boundaries: Vec<Value> = sqlx::query_scalar(
            "SELECT record_version_boundary FROM bigname_phase.record_inventory_current
             WHERE resource_id = $1",
        )
        .bind(resource_id)
        .fetch_all(pool)
        .await?;
        let mut today = None;
        for boundary in &boundaries {
            if let Some(row) = load_record_inventory_current(pool, resource_id, boundary).await? {
                today = Some(row);
                break;
            }
        }
        let family = load_family_record_inventory_detail(
            pool,
            chain_id,
            resource_id,
            FamilyAttribution::Given(attribution.remove(&resource_id).unwrap_or_default()),
        )
        .await?;
        report.inventory_rows += usize::from(today.is_some() || family.is_some());
        let mut differences =
            compare_record_inventory(today.as_ref(), family.as_ref().map(|family| &family.row));
        if let Some(family) = &family {
            report.compatibility_pairs += family.compatibility_pairs.len();
            differences.extend(check_compatibility_pairs(family));
        }
        if !differences.is_empty() {
            report
                .differences
                .push((format!("record_inventory {resource_id}"), differences));
        }
    }
    Ok(())
}

async fn addresses(
    pool: &PgPool,
    chain_id: &str,
    page_size: u64,
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
    .context("failed to list the addresses to compare")?;
    for (address, coin_type) in keys {
        let (mut today_cursor, mut family_cursor) = (None, None);
        loop {
            let today = load_address_records_current_page(
                pool,
                &address,
                &coin_type,
                None,
                AddressNamesCurrentDedupe::Surface,
                None,
                None,
                AddressNamesCurrentSort::Name,
                AddressNamesCurrentOrder::Asc,
                today_cursor.as_ref(),
                page_size,
            )
            .await?;
            let family = load_family_address_records_page_detail(
                pool,
                &address,
                &coin_type,
                None,
                AddressNamesCurrentDedupe::Surface,
                None,
                None,
                AddressNamesCurrentSort::Name,
                AddressNamesCurrentOrder::Asc,
                family_cursor.as_ref(),
                page_size,
            )
            .await?;
            report
                .address_index_misses
                .extend(family.index_misses.iter().map(|(resource, record_key)| {
                    format!(
                        "resolves_to {address} coin {coin_type} resource {resource} {record_key}"
                    )
                }));
            let family = family.page;
            report.address_pages += 1;
            report.address_entries += today.entries.len();
            let mut differences = compare_address_records(&today.entries, &family.entries);
            if today.next_cursor != family.next_cursor {
                differences.push(Difference {
                    field: "next_cursor".to_owned(),
                    today: Value::String(format!("{:?}", today.next_cursor)),
                    family: Value::String(format!("{:?}", family.next_cursor)),
                });
            }
            let done = today.next_cursor.is_none() || !differences.is_empty();
            if !differences.is_empty() {
                report.differences.push((
                    format!(
                        "resolves_to {address} coin {coin_type} page {}",
                        report.address_pages
                    ),
                    differences,
                ));
            }
            if done {
                break;
            }
            today_cursor = today.next_cursor;
            family_cursor = family.next_cursor;
        }
    }
    Ok(())
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
        if differences.is_empty() {
            continue;
        }
        let key = format!("primary_name {address} {namespace} {coin_type}");
        if family.is_some_and(|family| family.node_claim_at_other_resolver) {
            report.node_claim_findings.push((key, differences));
        } else {
            report.differences.push((key, differences));
        }
    }
    Ok(())
}
