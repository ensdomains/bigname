//! The walk's step loop, shared by the ownership relations (`super`) and `relation=resolves_to`
//! (`families::records::resolves_to_serving`). A relation supplies how it composes a name's rows
//! ([`WalkSource`]) and its page statement ([`WalkPage`]); the step sizes (`super::batch`), the
//! registration closure, the filters, the rank check and the stopping rule are this file's.
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::Result;
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

use super::{
    batch,
    order::{OrderRequest, WalkOrder},
};
use crate::{AddressNamesCurrentSortedCursor, families::records::seams};

/// One composed candidate: its page rows, its name row, its served name and the
/// `dedupe=registration` group its rows fall in. A surface-less registry child has no name row.
pub(crate) struct Composed {
    pub(crate) logical_name_id: String,
    pub(crate) display: String,
    pub(crate) group: Option<Uuid>,
    pub(crate) rows: Vec<Value>,
    pub(crate) name: Option<Value>,
}

/// How a relation reads its candidates' rows.
pub(crate) trait WalkSource {
    /// The names among `ids`, on `chain_id`, with rows for the page.
    async fn compose(
        &self,
        conn: &mut PgConnection,
        chain_id: &str,
        ids: &[String],
    ) -> Result<Vec<Composed>>;

    /// Every name on `chain_id` whose rows can fall in one of `groups`: a superset, the
    /// composed rows decide.
    async fn group_members(
        &self,
        conn: &mut PgConnection,
        chain_id: &str,
        groups: &[Uuid],
    ) -> Result<Vec<String>>;

    /// The names among `rows` with a row the page's filters keep.
    async fn kept(
        &self,
        conn: &mut PgConnection,
        rows: &Value,
        names: &Value,
    ) -> Result<Vec<String>>;

    /// The candidates whose rows are built without composition, by name id.
    fn prebuilt_ids(&self) -> Vec<&str> {
        Vec::new()
    }

    fn prebuilt(&self, _id: &str) -> Option<Composed> {
        None
    }

    /// The prebuilt candidates in `groups`.
    fn prebuilt_in(&self, _groups: &[Uuid]) -> Vec<Composed> {
        Vec::new()
    }
}

/// A relation's page statement over gathered rows.
pub(crate) trait WalkPage {
    type Entry;

    async fn page(
        &self,
        conn: &mut PgConnection,
        rows: &Value,
        names: &Value,
    ) -> Result<(Vec<Self::Entry>, Option<AddressNamesCurrentSortedCursor>)>;

    fn entry_name(entry: &Self::Entry) -> &str;
}

/// How the walk reaches a candidate in its order.
pub(crate) enum Kind<'a> {
    /// Composed on its chain.
    Name(&'a str),
    Prebuilt,
}

/// Each candidate's kind, or `None` when a name is listed on two chains.
pub(crate) fn name_kinds(
    by_chain: &BTreeMap<String, BTreeSet<String>>,
) -> Option<HashMap<&str, Kind<'_>>> {
    let mut kinds = HashMap::new();
    for (chain_id, ids) in by_chain {
        for id in ids {
            if kinds.insert(id.as_str(), Kind::Name(chain_id)).is_some() {
                return None;
            }
        }
    }
    Some(kinds)
}

/// The page request the steps answer.
pub(crate) struct WalkRequest<'a> {
    pub(crate) order: OrderRequest<'a>,
    pub(crate) page_size: u64,
    /// `dedupe=registration`: every gathered group must hold all its members.
    pub(crate) registration: bool,
}

/// Rows and name rows.
#[derive(Default)]
struct Rows {
    rows: Vec<Value>,
    names: Vec<Value>,
}

/// The rows the walk has gathered, the names they come from and the groups it holds whole.
#[derive(Default)]
struct Gathered {
    rows: Rows,
    ids: HashSet<String>,
    groups: HashSet<Uuid>,
}

/// One walk step's additions and the groups it reached first.
#[derive(Default)]
struct Step {
    rows: Rows,
    opened: Vec<Uuid>,
}

impl Gathered {
    /// Adds a walked name's rows unless they are already gathered. A name with no group gives
    /// way to the full read.
    fn admit(&mut self, step: &mut Step, name: Composed) -> bool {
        if self.ids.contains(&name.logical_name_id) {
            return true;
        }
        let Some(group) = name.group else {
            return false;
        };
        self.open(step, group);
        self.ids.insert(name.logical_name_id);
        step.rows.rows.extend(name.rows);
        step.rows.names.extend(name.name);
        true
    }

    fn admit_prebuilt(&mut self, step: &mut Step, name: Composed) {
        if !self.ids.insert(name.logical_name_id) {
            return;
        }
        if let Some(group) = name.group {
            self.open(step, group);
        }
        step.rows.rows.extend(name.rows);
    }

    fn open(&mut self, step: &mut Step, group: Uuid) {
        if self.groups.insert(group) {
            step.opened.push(group);
        }
    }
}

/// The page, or `None` when the walk must give way to the full read.
pub(crate) async fn walk<S: WalkSource, P: WalkPage>(
    conn: &mut PgConnection,
    source: &S,
    page: &P,
    request: &WalkRequest<'_>,
    by_chain: &BTreeMap<String, BTreeSet<String>>,
    kinds: &HashMap<&str, Kind<'_>>,
    order: &WalkOrder,
) -> Result<Option<(Vec<P::Entry>, Option<AddressNamesCurrentSortedCursor>)>> {
    let sort = request.order.sort;
    let rank: HashMap<&str, usize> = order
        .ids
        .iter()
        .enumerate()
        .map(|(index, id)| (id.as_str(), index))
        .collect();
    if rank.len() != order.ids.len() {
        return Ok(None);
    }
    // Registration closure depends on the partitions, so preserve its complete old sequence.
    let old_batches = request.registration || seams::legacy_walk_batches();
    let mut steps = batch::Steps::first(
        usize::try_from(request.page_size)?,
        request.order.cursor.is_some(),
        old_batches,
    );
    let mut position = order.start;
    let mut gathered = Gathered::default();
    loop {
        let end = position.saturating_add(steps.size()).min(order.ids.len());
        let mut step = Step::default();
        let mut names: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for id in &order.ids[position..end] {
            match kinds.get(id.as_str()) {
                Some(Kind::Name(chain_id)) => names.entry(chain_id).or_default().push(id.clone()),
                Some(Kind::Prebuilt) => {
                    let Some(name) = source.prebuilt(id) else {
                        return Ok(None);
                    };
                    gathered.admit_prebuilt(&mut step, name);
                }
                None => return Ok(None),
            }
        }
        for (chain_id, ids) in names {
            for chunk in ids.chunks(seams::compose_chunk()) {
                for name in source.compose(conn, chain_id, chunk).await? {
                    if !order.agrees(sort, &name) || !gathered.admit(&mut step, name) {
                        return Ok(None);
                    }
                }
            }
        }
        let mut closed = Rows::default();
        if request.registration
            && !close(
                conn,
                source,
                request,
                by_chain,
                order,
                &mut gathered,
                &mut step,
                &mut closed,
            )
            .await?
        {
            return Ok(None);
        }
        if !order.keys_agree(conn, sort, &step.rows.names).await? {
            return Ok(None);
        }
        keep_filtered(conn, source, &mut step.rows).await?;
        for rows in [step.rows, closed] {
            gathered.rows.rows.extend(rows.rows);
            gathered.rows.names.extend(rows.names);
        }
        position = end;
        let rows = Value::Array(std::mem::take(&mut gathered.rows.rows));
        let names = Value::Array(std::mem::take(&mut gathered.rows.names));
        let read = page.page(conn, &rows, &names).await;
        if let (Value::Array(rows), Value::Array(names)) = (rows, names) {
            gathered.rows = Rows { rows, names };
        }
        let (entries, next_cursor) = read?;
        // The page's groups come in walk order unless a sort key disagrees.
        let mut last = None;
        for entry in &entries {
            let Some(&at) = rank.get(P::entry_name(entry)) else {
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
        let found = entries.len() + usize::from(next_cursor.is_some());
        steps.grow(position - order.start, found);
    }
}

/// Composes the candidates that can join each group the step reached first and keeps those in
/// a group the walk reached, so every gathered `dedupe=registration` group holds all its
/// members. They are composed and filtered a chunk at a time: a group holds the rows the
/// filters keep, not every member's. Prebuilt candidates in those groups join the step.
#[allow(clippy::too_many_arguments)]
async fn close<S: WalkSource>(
    conn: &mut PgConnection,
    source: &S,
    request: &WalkRequest<'_>,
    by_chain: &BTreeMap<String, BTreeSet<String>>,
    order: &WalkOrder,
    gathered: &mut Gathered,
    step: &mut Step,
    closed: &mut Rows,
) -> Result<bool> {
    if step.opened.is_empty() {
        return Ok(true);
    }
    let sort = request.order.sort;
    for (chain_id, candidates) in by_chain {
        let members = source.group_members(conn, chain_id, &step.opened).await?;
        let mut ids: Vec<String> = members
            .into_iter()
            .filter(|id| candidates.contains(id) && !gathered.ids.contains(id))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        for chunk in ids.chunks(seams::compose_chunk()) {
            let mut rows = Rows::default();
            for name in source.compose(conn, chain_id, chunk).await? {
                let Some(group) = name.group else {
                    return Ok(false);
                };
                if !gathered.groups.contains(&group) {
                    continue;
                }
                if !order.agrees(sort, &name) {
                    return Ok(false);
                }
                gathered.ids.insert(name.logical_name_id);
                rows.rows.extend(name.rows);
                rows.names.extend(name.name);
            }
            keep_filtered(conn, source, &mut rows).await?;
            if !order.keys_agree(conn, sort, &rows.names).await? {
                return Ok(false);
            }
            closed.rows.append(&mut rows.rows);
            closed.names.append(&mut rows.names);
        }
    }
    for name in source.prebuilt_in(&step.opened) {
        if gathered.ids.insert(name.logical_name_id) {
            step.rows.rows.extend(name.rows);
        }
    }
    Ok(true)
}

/// Drops the rows of names the page's filters keep no row of: they add nothing to any group,
/// so the gathered rows stay about as large as the rows the page can serve. Returns the kept
/// names.
async fn keep_filtered<S: WalkSource>(
    conn: &mut PgConnection,
    source: &S,
    rows: &mut Rows,
) -> Result<HashSet<String>> {
    if rows.rows.is_empty() {
        return Ok(HashSet::new());
    }
    let id = |row: &Value| row["logical_name_id"].as_str().map(str::to_owned);
    seams::note_walk_filter(
        rows.rows
            .iter()
            .filter_map(id)
            .collect::<HashSet<_>>()
            .len(),
    );
    let all = Value::Array(std::mem::take(&mut rows.rows));
    let names = Value::Array(std::mem::take(&mut rows.names));
    let kept = source.kept(conn, &all, &names).await;
    let (Value::Array(all), Value::Array(names)) = (all, names) else {
        unreachable!("the rows are arrays")
    };
    let kept: HashSet<String> = kept?.into_iter().collect();
    let kept_row = |row: &Value| {
        row["logical_name_id"]
            .as_str()
            .is_some_and(|id| kept.contains(id))
    };
    rows.rows = all.into_iter().filter(|row| kept_row(row)).collect();
    rows.names = names.into_iter().filter(|row| kept_row(row)).collect();
    Ok(kept)
}

/// What reading every candidate a chunk at a time found (`read_every_candidate`).
pub(crate) struct EveryCandidate {
    /// The page's group count, as the summary statement counts `entries`.
    pub(crate) groups: u64,
    /// With `sort=created_at`, every name with a row the filters keep, and its served time.
    pub(crate) created: Option<HashMap<String, Option<String>>>,
}

/// Composes every candidate a chunk at a time, filters each chunk with the page's own filter
/// statement and keeps only what the walk needs of the kept names: their groups for the exact
/// total and, with `created_at`, their served first-observation times. The memory held is one
/// chunk of rows and these keys, whatever the candidate count. It takes as long as composing
/// every candidate.
pub(crate) async fn read_every_candidate<S: WalkSource>(
    conn: &mut PgConnection,
    source: &S,
    by_chain: &BTreeMap<String, BTreeSet<String>>,
    registration: bool,
    created_at: bool,
) -> Result<EveryCandidate> {
    let mut tally = Tally {
        names: HashSet::new(),
        groups: HashSet::new(),
        created: created_at.then(HashMap::new),
    };
    for (chain_id, ids) in by_chain {
        let ids: Vec<String> = ids.iter().cloned().collect();
        for chunk in ids.chunks(seams::compose_chunk()) {
            let composed = source.compose(conn, chain_id, chunk).await?;
            tally.add(conn, source, composed).await?;
        }
    }
    let prebuilt = source.prebuilt_ids();
    for chunk in prebuilt.chunks(seams::compose_chunk()) {
        let composed = chunk.iter().filter_map(|id| source.prebuilt(id)).collect();
        tally.add(conn, source, composed).await?;
    }
    let groups = if registration {
        tally.groups.len()
    } else {
        tally.names.len()
    };
    Ok(EveryCandidate {
        groups: u64::try_from(groups)?,
        created: tally.created,
    })
}

struct Tally {
    names: HashSet<String>,
    /// `DISTINCT ON (address, resource_id)` groups, a null group key included.
    groups: HashSet<Option<Uuid>>,
    created: Option<HashMap<String, Option<String>>>,
}

impl Tally {
    async fn add<S: WalkSource>(
        &mut self,
        conn: &mut PgConnection,
        source: &S,
        composed: Vec<Composed>,
    ) -> Result<()> {
        let mut rows = Rows::default();
        let mut groups = HashMap::new();
        for name in composed {
            groups.insert(name.logical_name_id, name.group);
            rows.rows.extend(name.rows);
            rows.names.extend(name.name);
        }
        let kept = keep_filtered(conn, source, &mut rows).await?;
        for id in &kept {
            self.groups.insert(groups.get(id).copied().flatten());
        }
        if let Some(created) = &mut self.created {
            for id in &kept {
                created.insert(id.clone(), None);
            }
            created.extend(super::order::served_created_at(conn, &rows.names).await?);
            if let Some(at) = seams::walk_created_at_cleared().and_then(|id| created.get_mut(&id)) {
                *at = None;
            }
        }
        self.names.extend(kept);
        Ok(())
    }
}
