//! The capped address-names page: `GET /v1/addresses/{address}/names` on the ownership
//! relations without composing every name the address may hold.
//!
//! An address with at most `seams::exact_total_cap` candidate names (the names the address
//! index lists, the ENSv2 registry-role names and the surface-less registry children), or a
//! request for the exact total, is read as `families::records::address_names` reads it: every
//! candidate composed, a chunk at a time, with an exact total. Above the cap the page walks the
//! candidates in the page's own order and its total is null.
//!
//! The walk orders every candidate by its sort key without composing it: the served name,
//! `ens_beautify` of the stored surface spelling as composition computes it, or the stored name
//! summary's expiry or registration time (`project_name_summary`, written with the page's own
//! timestamp expressions), then the name id. `sort=created_at` has no stored equivalent:
//! composition can date a name from an earlier registrar event that only a later binding named,
//! which the name history (`project_name_history`) never backdates. Above the cap it first
//! composes every candidate a chunk at a time and keeps each kept name's served time, then
//! walks in that order (`relation::read_every_candidate`). `include=total_count` above the cap
//! reads every candidate the same way for its count, then walks. Registry children sort by the
//! name they serve and have no timestamp.
//! Postgres orders the keys, so the order is the page statement's under the database
//! collation. For name grouping the walk starts with about a page and grows its steps
//! (`batch`). Registration grouping retains the old four-page first step and doubling. It runs
//! the unchanged page statement over the rows gathered so far. A group whose first member the
//! walk has not reached sorts at or after the walk position, so the page is final once its
//! last row is a group the walk passed, with one more row after it, or once every candidate is
//! gathered. With `dedupe=registration` a group spans every name bound to its resource; when the
//! walk first reaches a resource it also composes the other candidates bound to it (F1 binding
//! candidates), so every gathered group is whole. The step loop is `relation`'s, which
//! `relation=resolves_to` shares.
//!
//! The walk trusts the stored keys of the names it has not composed. Every name it composes is
//! checked against its stored key, and the page's rows against the walk order; on any
//! disagreement, such as a name summary composition no longer matches, the page is read in
//! full instead, with the exact total when `include=total_count` asks for it.
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

mod batch;
mod order;
mod relation;

pub(crate) use order::{OrderRequest, walk_order};
pub(crate) use relation::{
    Composed, Kind, WalkPage, WalkRequest, WalkSource, name_kinds, read_every_candidate, walk,
};

use super::{
    RowSource, load_address_names_filtered_ids_from, load_address_names_page_entries_from,
    load_address_names_page_from,
};
use crate::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentCappedPage,
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    AddressNamesCurrentSortedCursor, NameQuery,
    families::{
        name::{FamilyPublication, servable_publication},
        records::{
            AddressComposer, ComposedName, address_name_candidates, compose_candidate_rows,
            includes_roles, load_registry_children, registry_child_rows, seams,
        },
        topology::RegistryChildRow,
    },
};

/// The request an address-names page answers.
#[derive(Clone, Copy, Debug)]
pub struct AddressNamesPageRequest<'a> {
    pub address: &'a str,
    pub namespace: Option<&'a str>,
    pub relations: Option<&'a [AddressNameRelation]>,
    pub dedupe_by: AddressNamesCurrentDedupe,
    pub q: Option<NameQuery<'a>>,
    pub authority: Option<&'a [&'a str]>,
    pub is_migrated: Option<bool>,
    pub parent: Option<&'a str>,
    pub sort: AddressNamesCurrentSort,
    pub order: AddressNamesCurrentOrder,
    pub cursor: Option<&'a AddressNamesCurrentSortedCursor>,
    pub page_size: u64,
}

/// The page of `request`, with an exact total when `exact_total` asks for it or the address has
/// at most the cap of candidate names, else a null total.
pub async fn load_family_address_names_capped_page(
    db: impl Into<crate::ReadDb<'_>>,
    request: &AddressNamesPageRequest<'_>,
    exact_total: bool,
) -> Result<AddressNamesCurrentCappedPage> {
    let mut snapshot = db.into().snapshot().await?;
    let include_roles = includes_roles(request.relations);
    let by_chain = address_name_candidates(
        &mut snapshot,
        request.address,
        request.namespace,
        include_roles,
    )
    .await?;
    // The surface-less ENSv1 registry children the address owns, which compose no name row.
    // They are held as keys; the walk builds rows only for those it reaches.
    let children =
        load_registry_children(&mut snapshot, request.address, request.namespace).await?;
    let mut candidates: HashSet<&str> = by_chain.values().flatten().map(String::as_str).collect();
    candidates.extend(
        children
            .iter()
            .flat_map(|(_, children)| children)
            .map(|child| child.logical_name_id.as_str()),
    );
    let composer = AddressComposer {
        address: request.address,
        include_roles,
        with_history_evidence: false,
    };
    let capped = candidates.len() > seams::exact_total_cap();
    drop(candidates);
    if capped {
        if let Some(page) = walk_page(
            &mut snapshot,
            request,
            &composer,
            &by_chain,
            &children,
            exact_total,
        )
        .await?
        {
            seams::note_address_name_path("walk");
            snapshot.close().await?;
            return Ok(page);
        }
        seams::note_address_name_path("fallback");
    } else {
        seams::note_address_name_path("exact");
    }
    let (rows, names) = compose_candidate_rows(&mut snapshot, &composer, &by_chain).await?;
    let mut rows = match rows {
        Value::Array(rows) => rows,
        _ => unreachable!("composed rows are an array"),
    };
    for (publication, children) in &children {
        for child in children {
            rows.extend(registry_child_rows(request.address, publication, child));
        }
    }
    let page = load_address_names_page_from(
        &mut snapshot,
        RowSource::Composed {
            rows: &Value::Array(rows),
            names: &names,
            parent: request.parent,
        },
        request.address,
        request.namespace,
        request.relations,
        request.dedupe_by,
        request.q,
        request.authority,
        request.is_migrated,
        request.sort,
        request.order,
        request.cursor,
        request.page_size,
    )
    .await?;
    snapshot.close().await?;
    Ok(AddressNamesCurrentCappedPage {
        entries: page.entries,
        next_cursor: page.next_cursor,
        total_count: (!capped || exact_total).then_some(page.summary.grouped_entry_count),
    })
}

/// A surface-less registry child the walk can reach, with its chain's publication.
struct WalkChild<'a> {
    publication: &'a FamilyPublication,
    row: &'a RegistryChildRow,
}

/// The ownership relations' candidates: names composed for the address and registry children.
struct OwnershipWalk<'a> {
    request: &'a AddressNamesPageRequest<'a>,
    composer: &'a AddressComposer<'a>,
    publications: BTreeMap<String, FamilyPublication>,
    children: HashMap<&'a str, WalkChild<'a>>,
}

impl OwnershipWalk<'_> {
    fn child(&self, id: &str, child: &WalkChild<'_>) -> Composed {
        Composed {
            logical_name_id: id.to_owned(),
            display: child.row.display_name.clone(),
            group: Some(child.row.resource_id),
            rows: registry_child_rows(self.request.address, child.publication, child.row),
            name: None,
        }
    }
}

impl WalkSource for OwnershipWalk<'_> {
    async fn compose(
        &self,
        conn: &mut PgConnection,
        chain_id: &str,
        ids: &[String],
    ) -> Result<Vec<Composed>> {
        let publication = &self.publications[chain_id];
        let names: Vec<ComposedName> = self.composer.compose(conn, publication, ids).await?;
        names
            .into_iter()
            .map(|name| {
                Ok(Composed {
                    group: name.resource_id.as_deref().map(str::parse).transpose()?,
                    logical_name_id: name.logical_name_id,
                    display: name.canonical_display_name,
                    rows: name.rows,
                    name: Some(name.name),
                })
            })
            .collect()
    }

    async fn group_members(
        &self,
        conn: &mut PgConnection,
        chain_id: &str,
        groups: &[Uuid],
    ) -> Result<Vec<String>> {
        sqlx::query_scalar(
            "/* storage:families.records.address_name_walk_closure */
             SELECT DISTINCT logical_name_id FROM bigname_phase.project_binding_candidate
             WHERE chain_id = $1 AND resource_id = ANY($2::uuid[])",
        )
        .bind(chain_id)
        .bind(groups)
        .fetch_all(&mut *conn)
        .await
        .context("failed to load the names bound to the walked resources")
    }

    async fn kept(
        &self,
        conn: &mut PgConnection,
        rows: &Value,
        names: &Value,
    ) -> Result<Vec<String>> {
        let request = self.request;
        load_address_names_filtered_ids_from(
            conn,
            RowSource::Composed {
                rows,
                names,
                parent: request.parent,
            },
            request.address,
            request.namespace,
            request.relations,
            request.dedupe_by,
            request.q,
            request.authority,
            request.is_migrated,
        )
        .await
    }

    fn prebuilt_ids(&self) -> Vec<&str> {
        self.children.keys().copied().collect()
    }

    fn prebuilt(&self, id: &str) -> Option<Composed> {
        self.children.get(id).map(|child| self.child(id, child))
    }

    fn prebuilt_in(&self, groups: &[Uuid]) -> Vec<Composed> {
        self.children
            .iter()
            .filter(|(_, child)| groups.contains(&child.row.resource_id))
            .map(|(id, child)| self.child(id, child))
            .collect()
    }
}

impl WalkPage for OwnershipWalk<'_> {
    type Entry = AddressNameCurrentEntry;

    async fn page(
        &self,
        conn: &mut PgConnection,
        rows: &Value,
        names: &Value,
    ) -> Result<(Vec<Self::Entry>, Option<AddressNamesCurrentSortedCursor>)> {
        let request = self.request;
        load_address_names_page_entries_from(
            conn,
            RowSource::Composed {
                rows,
                names,
                parent: request.parent,
            },
            request.address,
            request.namespace,
            request.relations,
            request.dedupe_by,
            request.q,
            request.authority,
            request.is_migrated,
            request.sort,
            request.order,
            request.cursor,
            request.page_size,
        )
        .await
    }

    fn entry_name(entry: &Self::Entry) -> &str {
        &entry.logical_name_id
    }
}

/// The walk's page, or `None` when it must give way to the full read.
async fn walk_page(
    conn: &mut PgConnection,
    request: &AddressNamesPageRequest<'_>,
    composer: &AddressComposer<'_>,
    by_chain: &BTreeMap<String, BTreeSet<String>>,
    children: &[(FamilyPublication, Vec<RegistryChildRow>)],
    exact_total: bool,
) -> Result<Option<AddressNamesCurrentCappedPage>> {
    // Every chain is checked as the full read checks it, so the same chain answers stale.
    let mut publications = BTreeMap::new();
    for chain_id in by_chain.keys() {
        publications.insert(
            chain_id.clone(),
            servable_publication(conn, chain_id).await?,
        );
    }
    let mut walk_children: HashMap<&str, WalkChild<'_>> = HashMap::new();
    for (publication, rows) in children {
        for row in rows {
            let child = WalkChild { publication, row };
            if walk_children.insert(&row.logical_name_id, child).is_some() {
                return Ok(None);
            }
        }
    }
    let source = OwnershipWalk {
        request,
        composer,
        publications,
        children: walk_children,
    };
    let Some(mut kinds) = name_kinds(by_chain) else {
        return Ok(None);
    };
    let registration = request.dedupe_by == AddressNamesCurrentDedupe::Resource;
    let created_at = request.sort == AddressNamesCurrentSort::CreatedAt;
    let mut every = if exact_total || created_at {
        Some(read_every_candidate(conn, &source, by_chain, registration, created_at).await?)
    } else {
        None
    };
    let order_request = OrderRequest {
        sort: request.sort,
        order: request.order,
        cursor: request.cursor,
    };
    let child_keys: Vec<(&str, &str)> = source
        .children
        .iter()
        .map(|(id, child)| (*id, child.row.display_name.as_str()))
        .collect();
    let created = every.as_mut().and_then(|every| every.created.take());
    let Some(order) = walk_order(
        conn,
        order_request,
        &source.publications,
        by_chain,
        &child_keys,
        created,
    )
    .await?
    else {
        return Ok(None);
    };
    // A surface-less child the index also lists composes nothing as a name. A name with a
    // published surface is never such a child; if one were, the order would list it twice.
    for id in source.children.keys() {
        kinds.insert(id, Kind::Prebuilt);
    }
    let walk_request = WalkRequest {
        order: order_request,
        page_size: request.page_size,
        registration,
    };
    let Some((entries, next_cursor)) = walk(
        conn,
        &source,
        &source,
        &walk_request,
        by_chain,
        &kinds,
        &order,
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(AddressNamesCurrentCappedPage {
        entries,
        next_cursor,
        total_count: every.filter(|_| exact_total).map(|every| every.groups),
    }))
}
