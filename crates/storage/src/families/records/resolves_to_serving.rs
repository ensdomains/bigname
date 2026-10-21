//! `GET /v1/addresses/{address}/resolves_to` (both the single coin type and `coin_type=evm`) over
//! the families at the family publication.
//!
//! The candidate resources come from the derived inverse address index (F14) alone, with mirror
//! resolvers from the family classification alone (`candidates.rs`, `candidate_resources_from`).
//! Each candidate's family record inventory is assembled (`inventory.rs`) and its entries that
//! resolve to the address become `address_records_current`-shaped rows, one per name the resource
//! serves records for. Those names are composed at read (`families::name`) and kept when the record
//! resource is the name's serving resource, else the bound resource of a bound, registered name.
//! The rows are bound into the served page statements (`address_names::source`), so the coin-type
//! match, the ENSIP-19 default-address fallback, dedupe, the authority filter, sorts, cursors and
//! the EVM aggregation are the served SQL.
//!
//! An address with more than `seams::exact_total_cap` candidate names (the names that may
//! serve records through a candidate resource) is not read in full. The page walks the
//! candidates in its own order as the ownership relations' page does (`address_names::walk`):
//! each step composes its names a chunk at a time, assembles the inventories of the candidate
//! resources those names serve records through and filters the step's rows before the page
//! statement runs over what is gathered. With `dedupe=registration` a step that reaches a
//! group first also composes the other names that can fall in it, because the group's entry is
//! its exact-record member when it has one. Any disagreement with the walk order gives way to
//! the full read.
//!
//! A page is read in one snapshot (`read_snapshot`).
use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgConnection;
use uuid::Uuid;

use super::{
    address_names::{compose_base_chunk, name_row, publication_stamps},
    candidates::candidate_resources_from,
    inventory::{FamilyAttribution, load_family_record_inventories_on},
    resolves_to::{RecordRow, may_fall_back, record_rows},
    seams,
};
use crate::{
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    AddressNamesCurrentSortedCursor, AddressRecordsCurrentEvmPage, AddressRecordsCurrentPage,
    ENSIP19_DEFAULT_ADDRESS_RECORD_KEY, NameCurrentRow,
    address_names::{
        AddressRecordsQuery, Composed, EvmCoinsPage, OrderRequest, SingleCoinPage, WalkPage,
        WalkRequest, WalkSource, name_kinds, read_every_candidate, walk, walk_order,
    },
    families::name::{FamilyPublication, servable_publication},
};

/// `load_address_records_current_page` over the families.
#[allow(clippy::too_many_arguments)]
pub async fn load_family_resolves_to_page(
    db: impl Into<crate::ReadDb<'_>>,
    address: &str,
    coin_type: &str,
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<crate::NameQuery<'_>>,
    authority: Option<&[&str]>,
    parent: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressRecordsCurrentPage> {
    let mut coin_types = vec![coin_type.to_owned()];
    if may_fall_back(coin_type) {
        coin_types.push(ENSIP19_DEFAULT_ADDRESS_RECORD_KEY["addr:".len()..].to_owned());
    }
    let query = AddressRecordsQuery {
        address,
        coin_type: Some(coin_type),
        namespaces,
        dedupe_by,
        q,
        authority,
        parent,
        sort,
        order,
        cursor,
        page_size,
    };
    let mut snapshot = db.into().snapshot().await?;
    let (entries, next_cursor) = read_page(
        &mut snapshot,
        &query,
        &coin_types,
        &SingleCoinPage {
            query: &query,
            coin_type,
        },
    )
    .await?;
    snapshot.close().await?;
    Ok(AddressRecordsCurrentPage {
        entries,
        next_cursor,
    })
}

/// `load_address_records_current_evm_page` over the families.
#[allow(clippy::too_many_arguments)]
pub async fn load_family_resolves_to_evm_page(
    db: impl Into<crate::ReadDb<'_>>,
    address: &str,
    namespaces: Option<&[String]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<crate::NameQuery<'_>>,
    authority: Option<&[&str]>,
    parent: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressRecordsCurrentEvmPage> {
    let mut snapshot = db.into().snapshot().await?;
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
    let query = AddressRecordsQuery {
        address,
        coin_type: None,
        namespaces,
        dedupe_by,
        q,
        authority,
        parent,
        sort,
        order,
        cursor,
        page_size,
    };
    let (entries, next_cursor) =
        read_page(&mut snapshot, &query, &coin_types, &EvmCoinsPage(&query)).await?;
    snapshot.close().await?;
    Ok(AddressRecordsCurrentEvmPage {
        entries,
        next_cursor,
    })
}

/// The page of `query`: read in full when the address has at most the cap of candidate names,
/// else walked.
async fn read_page<P: WalkPage>(
    conn: &mut PgConnection,
    query: &AddressRecordsQuery<'_>,
    coin_types: &[String],
    page: &P,
) -> Result<(Vec<P::Entry>, Option<AddressNamesCurrentSortedCursor>)> {
    let address = query.address.to_ascii_lowercase();
    let candidates = if coin_types.is_empty() {
        BTreeMap::new()
    } else {
        candidate_resources_from(conn, &address, coin_types).await?
    };
    let mut resources: BTreeMap<String, Vec<Uuid>> = BTreeMap::new();
    for (chain_id, resource_id) in candidates.into_keys() {
        resources.entry(chain_id).or_default().push(resource_id);
    }
    // Resolve the requested name scope before a different chain's rebuild can veto it: a chain
    // with no name in scope is not read.
    let mut by_chain: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (chain_id, chain_resources) in &resources {
        let ids = names_reaching(conn, chain_id, chain_resources, query.namespaces).await?;
        if !ids.is_empty() {
            by_chain.insert(chain_id.clone(), ids.into_iter().collect());
        }
    }
    if by_chain.values().map(BTreeSet::len).sum::<usize>() > seams::exact_total_cap() {
        if let Some(found) = walk_page(conn, query, &address, &resources, &by_chain, page).await? {
            seams::note_address_name_path("walk");
            return Ok(found);
        }
        seams::note_address_name_path("fallback");
    } else {
        seams::note_address_name_path("exact");
    }
    // The route's namespace publication fence also covers an empty candidate set.
    let (rows, names) = compose_address_record_rows(conn, &address, &resources, &by_chain).await?;
    page.page(conn, &rows, &names).await
}

/// The walked page, or `None` when it must give way to the full read.
async fn walk_page<P: WalkPage>(
    conn: &mut PgConnection,
    query: &AddressRecordsQuery<'_>,
    address: &str,
    resources: &BTreeMap<String, Vec<Uuid>>,
    by_chain: &BTreeMap<String, BTreeSet<String>>,
    page: &P,
) -> Result<Option<(Vec<P::Entry>, Option<AddressNamesCurrentSortedCursor>)>> {
    // Every chain is checked as the full read checks it, so the same chain answers stale.
    let mut publications = BTreeMap::new();
    for chain_id in by_chain.keys() {
        publications.insert(
            chain_id.clone(),
            servable_publication(conn, chain_id).await?,
        );
    }
    let Some(kinds) = name_kinds(by_chain) else {
        return Ok(None);
    };
    let source = RecordsWalk {
        query,
        address,
        publications,
        resources: resources
            .iter()
            .map(|(chain_id, resources)| (chain_id.as_str(), resources.iter().copied().collect()))
            .collect(),
    };
    let registration = query.dedupe_by == AddressNamesCurrentDedupe::Resource;
    let created = if query.sort == AddressNamesCurrentSort::CreatedAt {
        read_every_candidate(conn, &source, by_chain, registration, true)
            .await?
            .created
    } else {
        None
    };
    let order_request = OrderRequest {
        sort: query.sort,
        order: query.order,
        cursor: query.cursor,
    };
    let Some(order) = walk_order(
        conn,
        order_request,
        &source.publications,
        by_chain,
        &[],
        created,
    )
    .await?
    else {
        return Ok(None);
    };
    let request = WalkRequest {
        order: order_request,
        page_size: query.page_size,
        registration,
    };
    walk(conn, &source, page, &request, by_chain, &kinds, &order).await
}

/// The resolves-to candidates: names whose records may resolve to the address.
struct RecordsWalk<'a> {
    query: &'a AddressRecordsQuery<'a>,
    /// Lowercase.
    address: &'a str,
    publications: BTreeMap<String, FamilyPublication>,
    resources: BTreeMap<&'a str, HashSet<Uuid>>,
}

impl WalkSource for RecordsWalk<'_> {
    async fn compose(
        &self,
        conn: &mut PgConnection,
        chain_id: &str,
        ids: &[String],
    ) -> Result<Vec<Composed>> {
        let composed = compose_base_chunk(conn, ids).await?;
        let candidates = &self.resources[chain_id];
        let serving: Vec<Uuid> = composed
            .values()
            .filter_map(serves_records_through)
            .filter(|resource| candidates.contains(resource))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let records = load_record_rows(conn, chain_id, &serving, self.address).await?;
        let publication = &self.publications[chain_id];
        let mut out = Vec::new();
        for row in composed.values() {
            let Some(found) = serves_records_through(row).and_then(|r| records.get(&r)) else {
                continue;
            };
            out.push(Composed {
                logical_name_id: row.logical_name_id.clone(),
                display: row.canonical_display_name.clone(),
                // The page statement's group key, COALESCE(resource_id, record_resource_id).
                group: row
                    .resource_id
                    .or(found.first().map(|record| record.record_resource_id)),
                rows: address_record_rows(self.address, found, row, publication),
                name: Some(name_row(row)),
            });
        }
        Ok(out)
    }

    async fn group_members(
        &self,
        conn: &mut PgConnection,
        chain_id: &str,
        groups: &[Uuid],
    ) -> Result<Vec<String>> {
        names_reaching(conn, chain_id, groups, self.query.namespaces).await
    }

    async fn kept(
        &self,
        conn: &mut PgConnection,
        rows: &Value,
        names: &Value,
    ) -> Result<Vec<String>> {
        self.query.kept(conn, rows, names).await
    }
}

/// The record rows of `resources` on `chain_id` that resolve to `address`, by resource; a
/// resource with none has no entry.
async fn load_record_rows(
    conn: &mut PgConnection,
    chain_id: &str,
    resources: &[Uuid],
    address: &str,
) -> Result<BTreeMap<Uuid, Vec<RecordRow>>> {
    let inventories = load_family_record_inventories_on(
        conn,
        chain_id,
        resources,
        FamilyAttribution::Given(BTreeSet::new()),
    )
    .await?;
    let mut records = BTreeMap::new();
    for (resource_id, inventory) in inventories {
        let found = record_rows(chain_id, &inventory, address);
        if !found.is_empty() {
            records.insert(resource_id, found);
        }
    }
    Ok(records)
}

/// The composed `address_records_current` rows of `address` for every candidate name, and the
/// composed name rows they read, as JSON record sets.
async fn compose_address_record_rows(
    conn: &mut PgConnection,
    address: &str,
    resources: &BTreeMap<String, Vec<Uuid>>,
    by_chain: &BTreeMap<String, BTreeSet<String>>,
) -> Result<(Value, Value)> {
    let (mut rows, mut names) = (Vec::new(), Vec::new());
    for (chain_id, ids) in by_chain {
        // Chunks in id order emit the rows in the order one composition of every id did.
        let ids: Vec<String> = ids.iter().cloned().collect();
        let publication = servable_publication(conn, chain_id).await?;
        let records = load_record_rows(conn, chain_id, &resources[chain_id], address).await?;
        if records.is_empty() {
            continue;
        }
        for chunk in ids.chunks(seams::compose_chunk()) {
            let composed = compose_base_chunk(conn, chunk).await?;
            for row in composed.values() {
                let Some(found) = serves_records_through(row).and_then(|r| records.get(&r)) else {
                    continue;
                };
                rows.extend(address_record_rows(address, found, row, &publication));
                names.push(name_row(row));
            }
        }
    }
    Ok((Value::Array(rows), Value::Array(names)))
}

/// A composed name's rows for the records its record resource holds, one per coin type.
fn address_record_rows(
    address: &str,
    found: &[RecordRow],
    row: &NameCurrentRow,
    publication: &FamilyPublication,
) -> Vec<Value> {
    let mut coin_types = BTreeSet::new();
    found
        .iter()
        // DISTINCT ON (address, coin_type, logical_name_id).
        .filter(|record| coin_types.insert(record.coin_type.clone()))
        .map(|record| address_record_row(address, record, row, publication))
        .collect()
}

/// The resource a composed name serves records through: its serving resource, else the bound
/// resource of a bound, registered name.
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
    namespaces: Option<&[String]>,
) -> Result<Vec<String>> {
    sqlx::query_scalar(
        "/* storage:families.records.names_reaching_resources */
         SELECT names.logical_name_id FROM (
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
           AND event.logical_name_id IS NOT NULL
         ) names
         WHERE $3::text[] IS NULL OR EXISTS (
             SELECT 1 FROM bigname_phase.name_surfaces surface
             WHERE surface.logical_name_id = names.logical_name_id
               AND surface.namespace = ANY($3::text[]))",
    )
    .bind(chain_id)
    .bind(resources)
    .bind(namespaces)
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
        "raw_name": row.canonical_display_name,
        "normalized_name": row.normalized_name,
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
