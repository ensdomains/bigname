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
//! timestamp expressions) or the name history's first observation (`project_name_history`),
//! then the name id. Registry children sort by the name they serve and have no timestamp.
//! Postgres orders the keys, so the order is the page statement's under the database
//! collation. The walk composes the candidates in that order, in batches that double, and runs
//! the unchanged page statement over the rows gathered so far. A group whose first member the
//! walk has not reached sorts at or after the walk position, so the page is final once its
//! last row is a group the walk passed, with one more row after it, or once every candidate is
//! gathered. With `dedupe=registration` a group spans every name bound to its resource; when the
//! walk first reaches a resource it also composes the other candidates bound to it (F1 binding
//! candidates), so every gathered group is whole.
//!
//! The walk trusts the stored keys of the names it has not composed. Every name it composes is
//! checked against its stored key, and the page's rows against the walk order; on any
//! disagreement, such as a name summary composition no longer matches, the page is read in
//! full instead, still with a null total.
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

mod order;

use order::{WalkOrder, timestamps_agree, walk_order};

use super::{RowSource, load_address_names_page_entries_from, load_address_names_page_from};
use crate::{
    AddressNameCurrentEntry, AddressNameRelation, AddressNamesCurrentCappedPage,
    AddressNamesCurrentDedupe, AddressNamesCurrentOrder, AddressNamesCurrentSort,
    AddressNamesCurrentSortedCursor, NameQuery,
    families::{
        name::{FamilyPublication, servable_publication},
        records::{
            AddressComposer, ComposedName, address_name_candidates, compose_candidate_rows,
            compose_registry_child_rows, includes_roles, seams,
        },
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

type PageEntries = (
    Vec<AddressNameCurrentEntry>,
    Option<AddressNamesCurrentSortedCursor>,
);

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
    let children =
        compose_registry_child_rows(&mut snapshot, request.address, request.namespace).await?;
    let mut candidates: HashSet<&str> = by_chain.values().flatten().map(String::as_str).collect();
    candidates.extend(
        children
            .iter()
            .filter_map(|row| row["logical_name_id"].as_str()),
    );
    let composer = AddressComposer {
        address: request.address,
        include_roles,
        with_history_evidence: false,
    };
    let walk = !exact_total && candidates.len() > seams::exact_total_cap();
    drop(candidates);
    if walk {
        if let Some((entries, next_cursor)) =
            walk_page(&mut snapshot, request, &composer, &by_chain, &children).await?
        {
            seams::note_address_name_path("walk");
            snapshot.close().await?;
            return Ok(AddressNamesCurrentCappedPage {
                entries,
                next_cursor,
                total_count: None,
            });
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
    rows.extend(children);
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
        total_count: (!walk).then_some(page.summary.grouped_entry_count),
    })
}

/// A walk candidate: a name to compose on its index chain, or a registry child's prebuilt rows.
enum Candidate {
    Name(String),
    Child,
}

/// The rows the walk has gathered, the names they come from and the resources whose
/// `dedupe=registration` groups they hold whole.
#[derive(Default)]
struct Gathered {
    rows: Vec<Value>,
    names: Vec<Value>,
    ids: HashSet<String>,
    resources: HashSet<Uuid>,
}

/// One walk step's additions.
#[derive(Default)]
struct Step {
    rows: Vec<Value>,
    names: Vec<Value>,
    opened: Vec<Uuid>,
}

impl Gathered {
    /// Adds a registry child's rows unless they are already gathered; false when a row has no
    /// resource to group by.
    fn admit_child(&mut self, step: &mut Step, id: &str, rows: &[&Value]) -> Result<bool> {
        if !self.ids.insert(id.to_owned()) {
            return Ok(true);
        }
        for row in rows {
            let Some(resource) = row["resource_id"].as_str() else {
                return Ok(false);
            };
            let resource: Uuid = resource.parse()?;
            if self.resources.insert(resource) {
                step.opened.push(resource);
            }
            step.rows.push((*row).clone());
        }
        Ok(true)
    }

    /// Adds a walked name's rows unless they are already gathered.
    fn admit_name(&mut self, step: &mut Step, name: ComposedName) -> Result<bool> {
        if self.ids.contains(&name.logical_name_id) {
            return Ok(true);
        }
        let Some(resource) = name.resource_id.as_deref() else {
            return Ok(false);
        };
        let resource: Uuid = resource.parse()?;
        if self.resources.insert(resource) {
            step.opened.push(resource);
        }
        self.ids.insert(name.logical_name_id);
        step.rows.extend(name.rows);
        step.names.push(name.name);
        Ok(true)
    }
}

/// What the walk reads its candidates from.
struct WalkInputs<'a> {
    composer: &'a AddressComposer<'a>,
    publications: BTreeMap<String, FamilyPublication>,
    by_chain: &'a BTreeMap<String, BTreeSet<String>>,
    child_rows: HashMap<&'a str, Vec<&'a Value>>,
}

/// The walk's page, or `None` when it must give way to the full read.
async fn walk_page(
    conn: &mut PgConnection,
    request: &AddressNamesPageRequest<'_>,
    composer: &AddressComposer<'_>,
    by_chain: &BTreeMap<String, BTreeSet<String>>,
    children: &[Value],
) -> Result<Option<PageEntries>> {
    // Every chain is checked as the full read checks it, so the same chain answers stale.
    let mut publications = BTreeMap::new();
    for chain_id in by_chain.keys() {
        publications.insert(
            chain_id.clone(),
            servable_publication(conn, chain_id).await?,
        );
    }
    let mut child_rows: HashMap<&str, Vec<&Value>> = HashMap::new();
    for row in children {
        let id = row["logical_name_id"]
            .as_str()
            .context("registry child has no id")?;
        child_rows.entry(id).or_default().push(row);
    }
    let inputs = WalkInputs {
        composer,
        publications,
        by_chain,
        child_rows,
    };
    let mut kinds: HashMap<&str, Candidate> = HashMap::new();
    for (chain_id, ids) in by_chain {
        for id in ids {
            if kinds
                .insert(id, Candidate::Name(chain_id.clone()))
                .is_some()
            {
                return Ok(None);
            }
        }
    }
    let Some(order) = walk_order(conn, request, &inputs).await? else {
        return Ok(None);
    };
    // A surface-less child the index also lists composes nothing as a name. A name with a
    // published surface is never such a child; if one were, the order would list it twice.
    for id in inputs.child_rows.keys() {
        kinds.insert(id, Candidate::Child);
    }
    let rank: HashMap<&str, usize> = order
        .ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect();
    if rank.len() != order.ids.len() {
        return Ok(None);
    }
    let registration = request.dedupe_by == AddressNamesCurrentDedupe::Resource;
    let mut batch = (4 * usize::try_from(request.page_size)?.saturating_add(1)).max(32);
    let mut position = order.start;
    let mut gathered = Gathered::default();
    loop {
        let end = position.saturating_add(batch).min(order.ids.len());
        let mut step = Step::default();
        let mut names: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for id in &order.ids[position..end] {
            match kinds.get(id.as_str()) {
                Some(Candidate::Name(chain_id)) => {
                    names.entry(chain_id).or_default().push(id.clone())
                }
                Some(Candidate::Child) => {
                    let rows = inputs
                        .child_rows
                        .get(id.as_str())
                        .map_or(&[][..], Vec::as_slice);
                    if !gathered.admit_child(&mut step, id, rows)? {
                        return Ok(None);
                    }
                }
                None => return Ok(None),
            }
        }
        for (chain_id, ids) in names {
            let publication = &inputs.publications[chain_id];
            for chunk in ids.chunks(seams::compose_chunk()) {
                for name in composer.compose(conn, publication, chunk).await? {
                    if !order.agrees(request.sort, &name)
                        || !gathered.admit_name(&mut step, name)?
                    {
                        return Ok(None);
                    }
                }
            }
        }
        if registration
            && !close_groups(
                conn,
                &inputs,
                &order,
                request.sort,
                &mut gathered,
                &mut step,
            )
            .await?
        {
            return Ok(None);
        }
        if request.sort.is_timestamp() && !timestamps_agree(conn, request.sort, &step.names).await?
        {
            return Ok(None);
        }
        gathered.rows.append(&mut step.rows);
        gathered.names.append(&mut step.names);
        position = end;
        batch = batch.saturating_mul(2);
        let rows = Value::Array(std::mem::take(&mut gathered.rows));
        let names = Value::Array(std::mem::take(&mut gathered.names));
        let page = load_address_names_page_entries_from(
            conn,
            RowSource::Composed {
                rows: &rows,
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
        .await;
        if let (Value::Array(rows), Value::Array(names)) = (rows, names) {
            gathered.rows = rows;
            gathered.names = names;
        }
        let (entries, next_cursor) = page?;
        // The page's groups come in walk order unless a sort key disagrees.
        let mut last = None;
        for entry in &entries {
            let Some(&at) = rank.get(entry.logical_name_id.as_str()) else {
                return Ok(None);
            };
            if last.is_some_and(|last| at <= last) {
                return Ok(None);
            }
            last = Some(at);
        }
        let passed = last.is_some_and(|last| last < position);
        if position >= order.ids.len() || (next_cursor.is_some() && passed) {
            return Ok(Some((entries, next_cursor)));
        }
    }
}

/// Composes the candidates bound to each resource the step reached first and keeps those whose
/// own resource is one the walk reached, so every gathered `dedupe=registration` group holds
/// all its members. Registry children on those resources join too.
async fn close_groups(
    conn: &mut PgConnection,
    inputs: &WalkInputs<'_>,
    order: &WalkOrder,
    sort: AddressNamesCurrentSort,
    gathered: &mut Gathered,
    step: &mut Step,
) -> Result<bool> {
    if step.opened.is_empty() {
        return Ok(true);
    }
    for (chain_id, candidates) in inputs.by_chain {
        let bound: Vec<String> = sqlx::query_scalar(
            "/* storage:families.records.address_name_walk_closure */
             SELECT DISTINCT logical_name_id FROM bigname_phase.project_binding_candidate
             WHERE chain_id = $1 AND resource_id = ANY($2::uuid[])",
        )
        .bind(chain_id)
        .bind(&step.opened)
        .fetch_all(&mut *conn)
        .await
        .context("failed to load the names bound to the walked resources")?;
        let mut ids: Vec<String> = bound
            .into_iter()
            .filter(|id| candidates.contains(id) && !gathered.ids.contains(id))
            .collect();
        ids.sort_unstable();
        for chunk in ids.chunks(seams::compose_chunk()) {
            let publication = &inputs.publications[chain_id];
            for name in inputs.composer.compose(conn, publication, chunk).await? {
                let Some(resource) = name.resource_id.as_deref() else {
                    return Ok(false);
                };
                if !gathered.resources.contains(&resource.parse::<Uuid>()?) {
                    continue;
                }
                if !order.agrees(sort, &name) {
                    return Ok(false);
                }
                gathered.ids.insert(name.logical_name_id);
                step.rows.extend(name.rows);
                step.names.push(name.name);
            }
        }
    }
    for (id, rows) in &inputs.child_rows {
        if gathered.ids.contains(*id) {
            continue;
        }
        let bound = rows.iter().any(|row| {
            row["resource_id"]
                .as_str()
                .and_then(|resource| resource.parse::<Uuid>().ok())
                .is_some_and(|resource| step.opened.contains(&resource))
        });
        if bound {
            gathered.ids.insert((*id).to_owned());
            step.rows.extend(rows.iter().map(|row| (*row).clone()));
        }
    }
    Ok(true)
}
