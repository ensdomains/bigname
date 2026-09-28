//! `GET /v1/addresses/{address}/resolves_to` (both the single coin type and `coin_type=evm`) over
//! the families under the publication switch (TYR-36 step 7b).
//!
//! The candidate resources come from the derived inverse address index (F14) alone,
//! with mirror resolvers from the family classification alone (`candidates.rs`,
//! `CandidateSource::Index`). The retained-value scan the harness adds stays a harness check:
//! the switch requires `address_index_misses` 0 first. Each candidate's family record inventory
//! is assembled (`inventory.rs`) and its entries that resolve to the address become
//! `address_records_current`-shaped rows, one per name the resource serves records for. Those
//! names are composed at read (`families::name`) and kept under the served builder's rule
//! (address_records.rs: the record resource is the serving resource, else the bound resource of a
//! bound, registered name). The rows are bound into the served page statements
//! (`address_names::source`), so the coin-type match, the ENSIP-19 default-address fallback,
//! dedupe, the authority filter, sorts, cursors and the EVM aggregation are the served SQL.
//!
//! A page is read in one snapshot (`read_snapshot`).
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::{
    address_names::{name_row, publication_stamps},
    candidates::{CandidateSource, candidate_resources_from},
    inventory::{FamilyAttribution, load_family_record_inventory_detail_on},
    resolves_to::{RecordRow, may_fall_back, record_rows},
};
use crate::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    AddressNamesCurrentSortedCursor, AddressRecordsCurrentEvmPage, AddressRecordsCurrentPage,
    ENSIP19_DEFAULT_ADDRESS_RECORD_KEY, NameCurrentRow,
    address_names::{
        RowSource, load_address_records_evm_page_from, load_address_records_page_from,
    },
    families::name::{
        CoverageShape, FamilyPublication, all_servable_publications, load_composed,
        servable_publication,
    },
};

/// `load_address_records_current_page` over the families.
#[allow(clippy::too_many_arguments)]
pub async fn load_family_resolves_to_page(
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
    let mut coin_types = vec![coin_type.to_owned()];
    if may_fall_back(coin_type) {
        coin_types.push(ENSIP19_DEFAULT_ADDRESS_RECORD_KEY["addr:".len()..].to_owned());
    }
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let (rows, names) = compose_address_record_rows(&mut snapshot, address, &coin_types).await?;
    let page = load_address_records_page_from(
        &mut snapshot,
        RowSource::Composed {
            rows: &rows,
            names: &names,
        },
        address,
        coin_type,
        namespaces,
        dedupe_by,
        q,
        authority,
        sort,
        order,
        cursor,
        page_size,
    )
    .await?;
    snapshot.commit().await?;
    Ok(page)
}

/// `load_address_records_current_evm_page` over the families.
#[allow(clippy::too_many_arguments)]
pub async fn load_family_resolves_to_evm_page(
    pool: &PgPool,
    address: &str,
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<&str>,
    authority: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressRecordsCurrentEvmPage> {
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    // Every coin type the index holds for the address; the page statement keeps the EVM ones.
    let coin_types: Vec<String> = sqlx::query_scalar(
        "/* storage:families.records.address_index_coin_types */
         SELECT coin_type FROM bigname_phase.project_address_record_node_index
         WHERE address = lower($1)
         UNION
         SELECT coin_type FROM bigname_phase.project_address_record_id_index
         WHERE address = lower($1)",
    )
    .bind(address)
    .fetch_all(&mut *snapshot)
    .await
    .with_context(|| format!("failed to load the indexed coin types of {address}"))?;
    let (rows, names) = compose_address_record_rows(&mut snapshot, address, &coin_types).await?;
    let page = load_address_records_evm_page_from(
        &mut snapshot,
        RowSource::Composed {
            rows: &rows,
            names: &names,
        },
        address,
        namespaces,
        dedupe_by,
        q,
        authority,
        sort,
        order,
        cursor,
        page_size,
    )
    .await?;
    snapshot.commit().await?;
    Ok(page)
}

/// The composed `address_records_current` rows of `address` for `coin_types`, and the composed
/// name rows they read, as JSON record sets.
async fn compose_address_record_rows(
    conn: &mut PgConnection,
    address: &str,
    coin_types: &[String],
) -> Result<(Value, Value)> {
    let address = address.to_ascii_lowercase();
    let candidates = if coin_types.is_empty() {
        BTreeMap::new()
    } else {
        candidate_resources_from(conn, &address, coin_types, CandidateSource::Index).await?
    };
    if candidates.is_empty() {
        all_servable_publications(conn).await?;
        return Ok((json!([]), json!([])));
    }
    let mut by_chain: BTreeMap<String, Vec<Uuid>> = BTreeMap::new();
    for (chain_id, resource_id) in candidates.into_keys() {
        by_chain.entry(chain_id).or_default().push(resource_id);
    }
    let (mut rows, mut names) = (Vec::new(), Vec::new());
    for (chain_id, resources) in by_chain {
        let publication = servable_publication(conn, &chain_id).await?;
        let mut records: BTreeMap<Uuid, Vec<RecordRow>> = BTreeMap::new();
        for resource_id in resources {
            let Some(inventory) = load_family_record_inventory_detail_on(
                conn,
                &chain_id,
                resource_id,
                FamilyAttribution::Given(BTreeSet::new()),
            )
            .await?
            else {
                continue;
            };
            let found = record_rows(&chain_id, &inventory, &address);
            if !found.is_empty() {
                records.insert(resource_id, found);
            }
        }
        if records.is_empty() {
            continue;
        }
        let resources: Vec<Uuid> = records.keys().copied().collect();
        let ids = names_reaching(conn, &chain_id, &resources).await?;
        let composed = load_composed(conn, &ids, CoverageShape::Plain).await?;
        for row in composed.values() {
            let Some(record_resource) = serves_records_through(row) else {
                continue;
            };
            let Some(found) = records.get(&record_resource) else {
                continue;
            };
            let mut coin_types = BTreeSet::new();
            for record in found {
                // DISTINCT ON (address, coin_type, logical_name_id).
                if coin_types.insert(record.coin_type.clone()) {
                    rows.push(address_record_row(&address, record, row, &publication));
                }
            }
            names.push(name_row(row));
        }
    }
    Ok((Value::Array(rows), Value::Array(names)))
}

/// The resource a composed name serves records through, under the served builder's rule
/// (address_records.rs, `names`).
fn serves_records_through(row: &NameCurrentRow) -> Option<Uuid> {
    let bound = row.surface_binding_id.is_some()
        && row.resource_id.is_some()
        && row.binding_kind.is_some()
        && row
            .declared_summary
            .pointer("/control/status")
            .and_then(Value::as_str)
            != Some("unregistered");
    if row.serving_resource_id.is_some() || bound {
        row.record_serving_resource_id()
    } else {
        None
    }
}

/// Every name that may serve records through one of `resources`: the names bound to it (F1
/// binding candidates) and the names its pointers carry (F5 resource pointers and F4 registry
/// pointers on it, and an ENSv2 root-registry pointer's `namespace:namehash`), which is where a
/// composed row's serving resource comes from (`families::name::serving`). A superset: the
/// composed rows decide.
async fn names_reaching(
    conn: &mut PgConnection,
    chain_id: &str,
    resources: &[Uuid],
) -> Result<Vec<String>> {
    sqlx::query_scalar(
        "/* storage:families.records.names_reaching_resources */
         SELECT candidate.logical_name_id
         FROM bigname_phase.project_binding_candidate candidate
         WHERE candidate.chain_id = $1 AND candidate.resource_id = ANY($2::uuid[])
         UNION
         SELECT event.logical_name_id
         FROM bigname_phase.project_resource_pointer pointer
         JOIN bigname_phase.normalized_events event
           ON event.event_identity = pointer.pointer_position ->> 'event_identity'
         WHERE pointer.chain_id = $1 AND pointer.resource_id = ANY($2::uuid[])
           AND event.logical_name_id IS NOT NULL
         UNION
         SELECT pointer.namespace || ':' || lower(pointer.namehash)
         FROM bigname_phase.project_resource_pointer pointer
         WHERE pointer.chain_id = $1 AND pointer.resource_id = ANY($2::uuid[])
           AND pointer.source_family = 'ens_v2_root_l1'
           AND pointer.namespace IS NOT NULL AND pointer.namehash IS NOT NULL
         UNION
         SELECT event.logical_name_id
         FROM bigname_phase.project_registry_pointer pointer
         JOIN bigname_phase.normalized_events event
           ON event.event_identity = pointer.event_identity
         WHERE pointer.chain_id = $1 AND pointer.resource_id = ANY($2::uuid[])
           AND event.logical_name_id IS NOT NULL",
    )
    .bind(chain_id)
    .bind(resources)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the names reaching the record resources")
}

fn address_record_row(
    address: &str,
    record: &RecordRow,
    row: &NameCurrentRow,
    publication: &FamilyPublication,
) -> Value {
    let (_, target, canonicality_summary) = publication_stamps(publication);
    let mut provenance = record.provenance.clone();
    provenance["logical_name_id"] = json!(row.logical_name_id);
    let mut chain_positions = record.chain_positions.clone();
    if let (Value::Object(positions), Value::Object(target)) = (&mut chain_positions, target) {
        positions.extend(target);
    }
    json!({
        "address": address,
        "coin_type": record.coin_type,
        "logical_name_id": row.logical_name_id,
        "namespace": row.namespace,
        "raw_name": row.normalized_name,
        "namehash": row.namehash,
        "surface_binding_id": row.surface_binding_id,
        "resource_id": row.resource_id,
        "record_resource_id": record.record_resource_id,
        "binding_kind": row.binding_kind.map(|kind| kind.as_str()),
        "record_key": record.record_key,
        "support_status": "supported",
        "unsupported_reason": null,
        "provenance": provenance,
        "chain_positions": chain_positions,
        "canonicality_summary": canonicality_summary,
        "manifest_version": row.manifest_version,
        "last_recomputed_at": crate::time::format_timestamp(row.last_recomputed_at),
    })
}
