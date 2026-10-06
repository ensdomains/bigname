//! `GET /v1/addresses/{address}/names` over the families (F13): the names the
//! address index (`project_address_name_index`) lists for the address, composed at read
//! (`families::name`), with each name's relations recomputed at its publication
//! (`address_relations.rs`). The index holds every address a relation can take under some
//! admission and mask, so the read only removes rows. Names on which the address holds an
//! ENSv2 registry role come from the served grant rows instead (`address_roles.rs`). The rows
//! are bound in the served `address_names_current` shape into the served page statements
//! (`address_names::source`), so the grouping, dedupe, filters, sorts, cursors and totals are
//! the served SQL.
//!
//! The rows carry what a route reads: identity, relations, the publication's position. They do
//! not carry the served event attribution (`provenance.normalized_event_id`, the relation's own
//! block in `chain_positions`, `manifest_version`) or the effective-controller support status
//! from the permission resource summary; no route reads them.
//!
//! The page adds the ENSv1 registry children with no name surface that the address owns
//! (`registry_children.rs`), which no name row composes for.
//!
//! A page is read in one snapshot (`read_snapshot`).
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgConnection;

use super::{
    address_relation_inputs::ChainInputs, address_relations::relations,
    address_roles::RoleHolderLoad,
};
use crate::{
    AddressNameRelation, AddressNamesCurrentDedupe, AddressNamesCurrentOrder,
    AddressNamesCurrentSort, AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedPage,
    NameCurrentRow,
    address_names::{RowSource, load_address_names_page_from},
    families::name::{CoverageShape, FamilyPublication, load_composed_base, servable_publication},
};

/// `load_address_names_current_page_filtered` over the families.
#[allow(clippy::too_many_arguments)]
pub async fn load_family_address_names_page(
    db: impl Into<crate::ReadDb<'_>>,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<crate::NameQuery<'_>>,
    authority: Option<&[&str]>,
    is_migrated: Option<bool>,
    parent: Option<&str>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    page_size: u64,
) -> Result<AddressNamesCurrentSortedPage> {
    let mut snapshot = db.into().snapshot().await?;
    let (mut rows, names) =
        compose_address_name_rows(&mut snapshot, address, namespace, relations, false).await?;
    // The surface-less ENSv1 registry children the address owns, which compose no name row.
    let children =
        super::registry_children::compose_registry_child_rows(&mut snapshot, address, namespace)
            .await?;
    if let Value::Array(rows) = &mut rows {
        rows.extend(children);
    }
    let page = load_address_names_page_from(
        &mut snapshot,
        RowSource::Composed {
            rows: &rows,
            names: &names,
            parent,
        },
        address,
        namespace,
        relations,
        dedupe_by,
        q,
        authority,
        is_migrated,
        sort,
        order,
        cursor,
        page_size,
    )
    .await?;
    snapshot.close().await?;
    Ok(page)
}

/// The composed `address_names_current` rows of `address` and the composed name rows they read,
/// as JSON record sets.
pub(crate) async fn compose_address_name_rows(
    conn: &mut PgConnection,
    address: &str,
    namespace: Option<&str>,
    requested_relations: Option<&[AddressNameRelation]>,
    with_history_evidence: bool,
) -> Result<(Value, Value)> {
    let include_roles = includes_roles(requested_relations);
    let by_chain = address_name_candidates(conn, address, namespace, include_roles).await?;
    let composer = AddressComposer {
        address,
        include_roles,
        with_history_evidence,
    };
    compose_candidate_rows(conn, &composer, &by_chain).await
}

/// Whether a relation set lists `role_holder`, explicitly or by default.
pub(crate) fn includes_roles(requested_relations: Option<&[AddressNameRelation]>) -> bool {
    requested_relations
        .filter(|relations| !relations.is_empty())
        .is_none_or(|relations| relations.contains(&AddressNameRelation::RoleHolder))
}

/// The names an address read composes, by chain: the names the address index lists for the
/// address and, with `include_roles`, the names on which it holds an ENSv2 registry role, which
/// the index does not list (`address_roles.rs`).
pub(crate) async fn address_name_candidates(
    conn: &mut PgConnection,
    address: &str,
    namespace: Option<&str>,
    include_roles: bool,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut indexed: Vec<(String, String)> = sqlx::query_as(
        "/* storage:families.records.address_name_index */
         SELECT DISTINCT indexed.chain_id, indexed.logical_name_id
         FROM bigname_phase.project_address_name_index indexed
         WHERE indexed.address = lower($1)
           AND ($2::text IS NULL OR EXISTS (
               SELECT 1 FROM bigname_phase.name_surfaces surface
               WHERE surface.logical_name_id = indexed.logical_name_id
                 AND surface.namespace = $2))",
    )
    .bind(address)
    .bind(namespace)
    .fetch_all(&mut *conn)
    .await
    .with_context(|| format!("failed to load the address index of {address}"))?;
    if include_roles {
        indexed.extend(
            super::address_roles::role_name_candidates(&mut *conn, address, namespace).await?,
        );
    }
    let mut by_chain: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (chain_id, name) in indexed {
        by_chain.entry(chain_id).or_default().insert(name);
    }
    Ok(by_chain)
}

/// Every candidate's page rows, composed a chunk at a time.
pub(crate) async fn compose_candidate_rows(
    conn: &mut PgConnection,
    composer: &AddressComposer<'_>,
    by_chain: &BTreeMap<String, BTreeSet<String>>,
) -> Result<(Value, Value)> {
    // With no candidates the route's own namespace publication check stands: no rows identify
    // another chain to read here, and unrelated markers cannot veto that scope.
    let (mut rows, mut names) = (Vec::new(), Vec::new());
    for (chain_id, ids) in by_chain {
        let publication = servable_publication(conn, chain_id).await?;
        let ids: Vec<String> = ids.iter().cloned().collect();
        // Each chunk's composed rows are reduced to the page's JSON rows before the next chunk
        // composes. A name's relations read only its own rows, so chunking changes no row.
        for chunk in ids.chunks(super::seams::compose_chunk()) {
            for name in composer.compose(conn, &publication, chunk).await? {
                rows.extend(name.rows);
                names.push(name.name);
            }
        }
    }
    Ok((Value::Array(rows), Value::Array(names)))
}

/// One composed name the address relates to: its relation rows for the address and its name row.
pub(crate) struct ComposedName {
    pub(crate) logical_name_id: String,
    pub(crate) canonical_display_name: String,
    pub(crate) resource_id: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) name: Value,
}

/// Composes names and keeps the page rows of those `address` relates to.
pub(crate) struct AddressComposer<'a> {
    pub(crate) address: &'a str,
    pub(crate) include_roles: bool,
    pub(crate) with_history_evidence: bool,
}

impl AddressComposer<'_> {
    /// The names among `ids`, of `publication`'s chain, the address relates to, in id order.
    pub(crate) async fn compose(
        &self,
        conn: &mut PgConnection,
        publication: &FamilyPublication,
        ids: &[String],
    ) -> Result<Vec<ComposedName>> {
        let wanted = self.address.to_ascii_lowercase();
        let clock_seconds = publication.timestamp_seconds();
        let composed = compose_base_chunk(conn, ids).await?;
        let roles = if self.include_roles {
            RoleHolderLoad::Subject(self.address)
        } else {
            RoleHolderLoad::Skip
        };
        let inputs = ChainInputs::load(
            conn,
            &publication.chain_id,
            composed.values(),
            clock_seconds,
            roles,
        )
        .await?;
        let mut out = Vec::new();
        for row in composed.values() {
            let input = inputs.input(row, clock_seconds);
            let mut rows = Vec::new();
            for (related, relation) in relations(&input) {
                if related != wanted {
                    continue;
                }
                let mut relation_row = address_name_row(&related, relation, row, publication);
                if self.with_history_evidence
                    && let Some(position) =
                        super::address_relations::relation_position(&input, &related, relation)
                {
                    relation_row["provenance"]["event_identity"] = json!(position.event_identity);
                    relation_row["chain_positions"]["block_number"] = json!(position.block_number);
                }
                rows.push(relation_row);
            }
            if !rows.is_empty() {
                out.push(ComposedName {
                    logical_name_id: row.logical_name_id.clone(),
                    canonical_display_name: row.canonical_display_name.clone(),
                    resource_id: row.resource_id.map(|resource| resource.to_string()),
                    rows,
                    name: name_row(row),
                });
            }
        }
        Ok(out)
    }
}

/// The composed rows of one chunk of an address read's names, without the declared resolution
/// topology and the record inventories it reads: no address read serves either.
pub(super) async fn compose_base_chunk(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<BTreeMap<String, NameCurrentRow>> {
    super::seams::note_composed_batch(ids.len());
    let mut rows = load_composed_base(conn, ids, CoverageShape::Plain).await?;
    crate::rendered_name::enrich(conn, &mut rows).await?;
    Ok(rows)
}

/// The name columns the page's authority and migration filters and timestamp sorts read:
/// `declared_summary.registration`, `.control` and `.history.created_at`, and
/// `provenance.authority_selection`. Nothing else is kept, so a page holds no composed summary
/// beyond what its statements read.
pub(super) fn name_row(row: &NameCurrentRow) -> Value {
    let mut summary = serde_json::Map::new();
    for key in ["registration", "control"] {
        if let Some(value) = row.declared_summary.get(key) {
            summary.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(history) = row.declared_summary.get("history") {
        let slim = match history.get("created_at") {
            Some(created_at) => json!({"created_at": created_at}),
            None if history.is_object() => json!({}),
            None => history.clone(),
        };
        summary.insert("history".to_owned(), slim);
    }
    let mut provenance = serde_json::Map::new();
    if let Some(selection) = row.provenance.get("authority_selection") {
        provenance.insert("authority_selection".to_owned(), selection.clone());
    }
    json!({
        "logical_name_id": row.logical_name_id,
        "declared_summary": summary,
        "provenance": provenance,
    })
}

/// The publication stamps a composed relation row carries: the served read filter checks that
/// the target block is on canonical lineage.
pub(super) fn publication_stamps(publication: &FamilyPublication) -> (Value, Value, Value) {
    (
        json!({
            "chain_id": publication.chain_id,
            "coverage": {"status": "projected", "exhaustiveness": "not_asserted"},
        }),
        json!({
            "target_block_number": publication.block_number,
            "target_block_hash": publication.block_hash,
        }),
        json!({
            "state": "canonical_lineage",
            "target_block_number": publication.block_number,
            "target_block_hash": publication.block_hash,
        }),
    )
}

fn address_name_row(
    address: &str,
    relation: &str,
    row: &NameCurrentRow,
    publication: &FamilyPublication,
) -> Value {
    let (provenance, chain_positions, canonicality_summary) = publication_stamps(publication);
    json!({
        "address": address,
        "logical_name_id": row.logical_name_id,
        "relation": relation,
        "namespace": row.namespace,
        "raw_name": row.canonical_display_name,
        "normalized_name": row.normalized_name,
        "namehash": row.namehash,
        "surface_binding_id": row.surface_binding_id,
        "resource_id": row.resource_id,
        "token_lineage_id": row.token_lineage_id,
        "binding_kind": row.binding_kind.map(|kind| kind.as_str()),
        "support_status": "supported",
        "unsupported_reason": null,
        "provenance": provenance,
        "chain_positions": chain_positions,
        "canonicality_summary": canonicality_summary,
        "manifest_version": row.manifest_version,
        "last_recomputed_at": crate::time::format_timestamp(row.last_recomputed_at),
    })
}

/// Address relations for composed names on the caller's snapshot, used by batch lookup. They
/// hold no `role_holder` relations: reverse lookup does not serve it ([`RoleHolderLoad::Skip`]).
/// Exact lookup ownership/control from the supplied publication. Project calls this before
/// the marker advances; role holders remain excluded by the same lookup relation fold.
pub(crate) async fn name_relations_at(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    names: &[&NameCurrentRow],
) -> Result<BTreeMap<String, Vec<crate::IdentityAddressRelationRow>>> {
    let chain = &publication.chain_id;
    let mut out = BTreeMap::new();
    let clock_seconds = publication.timestamp_seconds();
    let roles = RoleHolderLoad::Skip;
    let inputs =
        ChainInputs::load(conn, &chain, names.iter().copied(), clock_seconds, roles).await?;
    for row in names {
        let input = inputs.input(row, clock_seconds);
        let related = relations(&input)
            .into_iter()
            .map(|(address, relation)| {
                let relation = match relation {
                    "token_holder" => AddressNameRelation::TokenHolder,
                    "effective_controller" => AddressNameRelation::EffectiveController,
                    "role_holder" => AddressNameRelation::RoleHolder,
                    _ => unreachable!("family relations have three defined kinds"),
                };
                crate::IdentityAddressRelationRow {
                    address,
                    logical_name_id: row.logical_name_id.clone(),
                    relation,
                    chain_positions: row.chain_positions.clone(),
                }
            })
            .collect();
        out.insert(row.logical_name_id.clone(), related);
    }
    Ok(out)
}
