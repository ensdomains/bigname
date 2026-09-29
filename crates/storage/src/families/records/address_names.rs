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
use sqlx::{PgConnection, PgPool, Row};

use super::{
    FamilyPosition,
    address_relations::{ControllerCandidate, NameRelationsInput, relations},
    address_roles::{RoleHolder, RoleHolderLoad, role_holders},
};
use crate::{
    AddressNameRelation, AddressNamesCurrentDedupe, AddressNamesCurrentOrder,
    AddressNamesCurrentSort, AddressNamesCurrentSortedCursor, AddressNamesCurrentSortedPage,
    NameCurrentRow,
    address_names::{RowSource, load_address_names_page_from},
    families::{
        control::{
            rows::{BindingCandidate, WrapperRow},
            wrapper::load_wrapper_rows,
        },
        name::{CoverageShape, FamilyPublication, load_composed, servable_publication},
    },
};

/// `load_address_names_current_page_filtered` over the families.
#[allow(clippy::too_many_arguments)]
pub async fn load_family_address_names_page(
    pool: &PgPool,
    address: &str,
    namespace: Option<&str>,
    relations: Option<&[AddressNameRelation]>,
    dedupe_by: AddressNamesCurrentDedupe,
    q: Option<crate::NameQuery<'_>>,
    authority: Option<&[&str]>,
    is_migrated: Option<bool>,
    sort: AddressNamesCurrentSort,
    order: AddressNamesCurrentOrder,
    cursor: Option<&AddressNamesCurrentSortedCursor>,
    expected_registry_children_digest: Option<&str>,
    page_size: u64,
) -> Result<AddressNamesCurrentSortedPage> {
    let mut snapshot = crate::families::read_snapshot(pool).await?;
    let (mut rows, names) =
        compose_address_name_rows(&mut snapshot, address, namespace, relations, false).await?;
    // The surface-less ENSv1 registry children the address owns, which compose no name row.
    let (children, registry_children_digest) =
        super::registry_children::compose_registry_child_rows(&mut snapshot, address, namespace)
            .await?;
    // Before the page validates the cursor's anchor, which a renamed child no longer matches.
    if expected_registry_children_digest
        .is_some_and(|expected| expected != registry_children_digest)
    {
        return Err(crate::AddressNamesRegistryChildrenChanged.into());
    }
    if let Value::Array(rows) = &mut rows {
        rows.extend(children);
    }
    let mut page = load_address_names_page_from(
        &mut snapshot,
        RowSource::Composed {
            rows: &rows,
            names: &names,
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
    page.registry_children_digest = registry_children_digest;
    snapshot.commit().await?;
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
    let indexed: Vec<(String, String)> = sqlx::query_as(
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
    // The names on which the address holds an ENSv2 registry role, which the index does not
    // list (`address_roles.rs`).
    let include_roles = requested_relations
        .filter(|relations| !relations.is_empty())
        .is_none_or(|relations| relations.contains(&AddressNameRelation::RoleHolder));
    let mut indexed = indexed;
    if include_roles {
        indexed.extend(
            super::address_roles::role_name_candidates(&mut *conn, address, namespace).await?,
        );
    }
    if indexed.is_empty() {
        // The route captures and revalidates its requested namespace publication. No rows
        // identify another chain to read here; unrelated markers cannot veto that scope.
        return Ok((json!([]), json!([])));
    }
    let mut by_chain: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (chain_id, name) in indexed {
        by_chain.entry(chain_id).or_default().insert(name);
    }
    let wanted = address.to_ascii_lowercase();
    let (mut rows, mut names) = (Vec::new(), Vec::new());
    for (chain_id, ids) in by_chain {
        let publication = servable_publication(conn, &chain_id).await?;
        let ids: Vec<String> = ids.into_iter().collect();
        let composed = load_composed(conn, &ids, CoverageShape::Plain).await?;
        let clock_seconds = publication.timestamp_seconds();
        let roles = if include_roles {
            RoleHolderLoad::Subject(address)
        } else {
            RoleHolderLoad::Skip
        };
        let inputs =
            ChainInputs::load(conn, &chain_id, composed.values(), clock_seconds, roles).await?;
        for row in composed.values() {
            let input = inputs.input(row, clock_seconds);
            let mut listed = false;
            for (related, relation) in relations(&input) {
                if related == wanted {
                    let mut relation_row = address_name_row(&related, relation, row, &publication);
                    if with_history_evidence
                        && let Some(position) =
                            super::address_relations::relation_position(&input, &related, relation)
                    {
                        relation_row["provenance"]["event_identity"] =
                            json!(position.event_identity);
                        relation_row["chain_positions"]["block_number"] =
                            json!(position.block_number);
                    }
                    rows.push(relation_row);
                    listed = true;
                }
            }
            if listed {
                names.push(name_row(row));
            }
        }
    }
    Ok((Value::Array(rows), Value::Array(names)))
}

/// The name columns the page's authority and migration filters and timestamp sorts read.
pub(super) fn name_row(row: &NameCurrentRow) -> Value {
    json!({
        "logical_name_id": row.logical_name_id,
        "declared_summary": row.declared_summary,
        "provenance": row.provenance,
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

/// The per-chain family rows the relations of a batch of composed names read.
struct ChainInputs {
    candidates: BTreeMap<String, Vec<ControllerCandidate>>,
    bindings: BTreeMap<String, BindingCandidate>,
    wrappers: BTreeMap<String, WrapperRow>,
    role_holders: BTreeMap<String, Vec<RoleHolder>>,
}

impl ChainInputs {
    async fn load<'a>(
        conn: &mut PgConnection,
        chain_id: &str,
        composed: impl IntoIterator<Item = &'a NameCurrentRow>,
        clock_seconds: i64,
        roles: RoleHolderLoad<'_>,
    ) -> Result<Self> {
        let composed: Vec<&NameCurrentRow> = composed.into_iter().collect();
        let ids: Vec<String> = composed
            .iter()
            .map(|row| row.logical_name_id.clone())
            .collect();
        let rows = sqlx::query(
            "/* storage:families.records.address_controller_candidates */
             SELECT logical_name_id, block_number, transaction_index, log_index, event_identity,
                    resource_id::text AS resource_id, event_kind, source_family, action, subject
             FROM bigname_phase.project_address_controller_candidate
             WHERE chain_id = $1 AND logical_name_id = ANY($2)",
        )
        .bind(chain_id)
        .bind(&ids)
        .fetch_all(&mut *conn)
        .await
        .context("failed to load the address controller candidates")?;
        let mut candidates: BTreeMap<String, Vec<ControllerCandidate>> = BTreeMap::new();
        for row in &rows {
            let candidate = ControllerCandidate {
                logical_name_id: row.try_get("logical_name_id")?,
                position: FamilyPosition::from_row(row)?,
                resource_id: row.try_get("resource_id")?,
                event_kind: row.try_get("event_kind")?,
                source_family: row.try_get("source_family")?,
                set: row.try_get::<String, _>("action")? == "set",
                subject: row.try_get("subject")?,
            };
            candidates
                .entry(candidate.logical_name_id.clone())
                .or_default()
                .push(candidate);
        }
        let selected: Vec<_> = composed
            .iter()
            .filter_map(|row| row.surface_binding_id)
            .collect();
        let bindings: Vec<Value> = sqlx::query_scalar(
            "/* storage:families.records.address_selected_bindings */
             SELECT to_jsonb(candidate) FROM bigname_phase.project_binding_candidate candidate
             WHERE candidate.chain_id = $1 AND candidate.surface_binding_id = ANY($2::uuid[])",
        )
        .bind(chain_id)
        .bind(&selected)
        .fetch_all(&mut *conn)
        .await
        .context("failed to load the selected binding candidates")?;
        let bindings = bindings
            .iter()
            .filter_map(BindingCandidate::from_row)
            .map(|binding| (binding.surface_binding_id.clone(), binding))
            .collect();
        let resources: Vec<String> = composed
            .iter()
            .filter_map(|row| row.resource_id.map(|id| id.to_string()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let wrappers = load_wrapper_rows(&mut *conn, chain_id, &resources)
            .await?
            .into_iter()
            .map(|wrapper| (wrapper.resource_id.clone(), wrapper))
            .collect();
        let role_holders = role_holders(
            &mut *conn,
            chain_id,
            &resources,
            &wrappers,
            clock_seconds,
            roles,
        )
        .await?;
        Ok(Self {
            candidates,
            bindings,
            wrappers,
            role_holders,
        })
    }

    /// The relations input of one composed name of the batch.
    fn input<'a>(&'a self, row: &'a NameCurrentRow, clock_seconds: i64) -> NameRelationsInput<'a> {
        let resource = row.resource_id.map(|resource| resource.to_string());
        NameRelationsInput {
            row,
            candidates: self
                .candidates
                .get(&row.logical_name_id)
                .map_or(&[][..], Vec::as_slice),
            binding: self.selected_binding(row),
            wrapper: resource
                .as_ref()
                .and_then(|resource| self.wrappers.get(resource)),
            clock_seconds,
            role_holders: resource
                .as_ref()
                .and_then(|resource| self.role_holders.get(resource))
                .map_or(&[][..], Vec::as_slice),
        }
    }

    fn selected_binding(&self, row: &NameCurrentRow) -> Option<&BindingCandidate> {
        self.bindings
            .get(&row.surface_binding_id?.to_string())
            .filter(|binding| binding.logical_name_id == row.logical_name_id)
    }
}

/// Address relations for composed names on the caller's snapshot, used by batch lookup. They
/// hold no `role_holder` relations: reverse lookup does not serve it ([`RoleHolderLoad::Skip`]).
pub(crate) async fn name_relations_on(
    conn: &mut PgConnection,
    composed: &BTreeMap<String, NameCurrentRow>,
) -> Result<BTreeMap<String, Vec<crate::IdentityAddressRelationRow>>> {
    let mut chains: BTreeMap<String, Vec<&NameCurrentRow>> = BTreeMap::new();
    for row in composed.values() {
        let chain = row.provenance["chain_id"]
            .as_str()
            .context("composed name has no chain")?;
        chains.entry(chain.to_owned()).or_default().push(row);
    }
    let mut out = BTreeMap::new();
    for (chain, names) in chains {
        let publication = servable_publication(conn, &chain).await?;
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
                        "registrant" => AddressNameRelation::Registrant,
                        "token_holder" => AddressNameRelation::TokenHolder,
                        "effective_controller" => AddressNameRelation::EffectiveController,
                        "role_holder" => AddressNameRelation::RoleHolder,
                        _ => unreachable!("family relations have four defined kinds"),
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
    }
    Ok(out)
}
