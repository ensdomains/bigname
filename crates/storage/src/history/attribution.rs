//! Resolver record writes attributed to a registration through its resolver pointers, judged from
//! the evidence at or below a read's published block.
//!
//! Node-keyed `RecordChanged` and `RecordVersionChanged` rows carry no logical name or resource, so
//! only a resolver pointer ties them to a registration. Project publishes that attribution in
//! `record_inventory_current.provenance.attributed_event_ids`, computed from every pointer up to its
//! own target. A history read bound to an earlier block cannot use that row: a pointer recorded
//! after the bound attributes writes made before it. This reader evaluates the producer's rules
//! over the pointers, record links and writes at or below the bound instead, so a pointer or link
//! above the bound neither attributes an older write nor closes an earlier pointer's window.
//!
//! The rules mirror `crates/project/src/builders/record_inventory/history.rs`,
//! `crates/project/src/builders/linked_records/history.rs`, the node-keyed arms of
//! `crates/project/src/builders/record_inventory.rs`, and the mirror substitution in
//! `crates/project/src/builders/record_inventory/mirror.rs`. The Project test
//! `bounded_record_attribution_equivalence` checks that the two agree at the current publication.

mod mirror;
mod sql;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use super::selectors::HistorySelector;

/// The attribution statement for `resource_ids`, for plan tests.
#[cfg(test)]
pub(in crate::history) fn push_pointer_window_attribution_for_test<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource_ids: &'a [Uuid],
    published: Option<&BTreeMap<String, i64>>,
) {
    sql::push_pointer_window_attribution(builder, resource_ids, published);
}

/// The mirror substitution statement with an empty walk, for plan tests.
#[cfg(test)]
pub(in crate::history) fn push_empty_mirror_writes_for_test(
    builder: &mut QueryBuilder<'static, Postgres>,
    published: Option<&BTreeMap<String, i64>>,
) {
    mirror::push_empty_mirror_writes_for_test(builder, published);
}

/// The mirror substitution statement over the queried node alone, for plan tests.
#[cfg(test)]
pub(in crate::history) fn push_exact_node_mirror_writes_for_test(
    builder: &mut QueryBuilder<'static, Postgres>,
    resource_id: Uuid,
    node: &str,
    raw_labels: &[&str],
    published: Option<&BTreeMap<String, i64>>,
) {
    mirror::push_exact_node_mirror_writes_for_test(
        builder,
        resource_id,
        node,
        raw_labels,
        published,
    );
}

/// The attributed writes of a read's candidate resources, as `(resource, event)` pairs.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::history) struct AttributedRecords {
    resource_ids: Vec<Uuid>,
    event_ids: Vec<i64>,
}

impl AttributedRecords {
    fn from_map(map: BTreeMap<Uuid, BTreeSet<i64>>) -> Self {
        let mut records = Self::default();
        for (resource_id, event_ids) in map {
            for event_id in event_ids {
                records.resource_ids.push(resource_id);
                records.event_ids.push(event_id);
            }
        }
        records
    }

    /// The ids attributed to any of `resource_ids`, sorted and without repeats.
    pub(in crate::history) fn event_ids_for(&self, resource_ids: &[Uuid]) -> Vec<i64> {
        let wanted = resource_ids.iter().collect::<BTreeSet<_>>();
        self.resource_ids
            .iter()
            .zip(&self.event_ids)
            .filter(|(resource_id, _)| wanted.contains(resource_id))
            .map(|(_, event_id)| *event_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(in crate::history) fn resource_ids(&self) -> &[Uuid] {
        &self.resource_ids
    }

    pub(in crate::history) fn event_ids(&self) -> &[i64] {
        &self.event_ids
    }
}

/// Every resource a read's selectors reach; their attributed writes are the candidates.
pub(in crate::history) fn selector_resource_ids(selectors: &[HistorySelector]) -> Vec<Uuid> {
    selectors
        .iter()
        .flat_map(|selector| match selector {
            HistorySelector::Resources(resource_ids)
            | HistorySelector::LogicalNamesOrResources { resource_ids, .. }
            | HistorySelector::ProductRegistration { resource_ids, .. } => resource_ids.as_slice(),
            HistorySelector::LogicalNames(_) | HistorySelector::None => &[],
        })
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(in crate::history) async fn load_attributed_records(
    connection: &mut PgConnection,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<AttributedRecords> {
    Ok(AttributedRecords::from_map(
        load_attribution_map(connection, resource_ids, published).await?,
    ))
}

/// The writes attributed to each of `resource_ids` at `published`, or through every readable
/// pointer and write when `published` is `None`. At a resource's current publication this is the
/// set Project publishes in `record_inventory_current.provenance.attributed_event_ids`.
pub async fn load_bounded_record_attribution(
    pool: &sqlx::PgPool,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<BTreeMap<Uuid, BTreeSet<i64>>> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire a connection for record attribution")?;
    load_attribution_map(&mut connection, resource_ids, published).await
}

async fn load_attribution_map(
    connection: &mut PgConnection,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<BTreeMap<Uuid, BTreeSet<i64>>> {
    let mut attributed = BTreeMap::<Uuid, BTreeSet<i64>>::new();
    if resource_ids.is_empty() {
        return Ok(attributed);
    }

    let mut builder = QueryBuilder::<Postgres>::new("");
    sql::push_pointer_window_attribution(&mut builder, resource_ids, published);
    for row in builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context("failed to load pointer-attributed record writes")?
    {
        attributed
            .entry(row.try_get("resource_id")?)
            .or_default()
            .insert(row.try_get("normalized_event_id")?);
    }

    // A resource whose latest pointer is a mirror resolver serves, and attributes, the writes of
    // the ENSv1 resolver the mirror would call. When Project cannot follow the mirror it publishes
    // the resource's row with no attributed writes at all, superseded pointers included.
    let mirrored = mirror::load_mirror_attribution(connection, resource_ids, published).await?;
    for (resource_id, substitution) in mirrored {
        match substitution {
            Some(event_ids) => attributed.entry(resource_id).or_default().extend(event_ids),
            None => {
                attributed.remove(&resource_id);
            }
        }
    }
    attributed.retain(|_, event_ids| !event_ids.is_empty());
    Ok(attributed)
}
