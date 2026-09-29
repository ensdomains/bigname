//! Resolver record writes attributed to a registration through its resolver pointers, judged from
//! the evidence at or below a read's published block.
//!
//! Node-keyed `RecordChanged` and `RecordVersionChanged` rows carry no logical name or resource, so
//! only a resolver pointer ties them to a registration. This reader evaluates that attribution over
//! the pointers, record links and writes at or below a bound, so a pointer or link above the bound
//! neither attributes an older write nor closes an earlier pointer's window. At a resource's
//! current publication the result is the family record inventory's
//! `provenance.attributed_event_ids` (`families::records`, `FamilyAttribution::Load`); a history
//! read bound to an earlier block evaluates it at that block, because a pointer recorded after the
//! bound attributes writes made before it.

mod mirror;
mod sql;

pub(in crate::history) use sql::ENS_V1_POINTER_FAMILIES;

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

/// The positional naming statements for `event_ids`, for plan tests: the candidate resources and
/// the names over `resource_ids`.
#[cfg(test)]
pub(in crate::history) fn push_positional_candidate_resources_for_test<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    event_ids: &'a [i64],
) {
    sql::push_positional_candidate_resources(builder, event_ids);
}

#[cfg(test)]
pub(in crate::history) fn push_positional_names_for_test<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource_ids: &'a [Uuid],
    event_ids: &'a [i64],
) {
    sql::push_positional_names(builder, resource_ids, event_ids);
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
/// set the family record inventory serves in `provenance.attributed_event_ids`.
pub async fn load_bounded_record_attribution(
    pool: &sqlx::PgPool,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<BTreeMap<Uuid, BTreeSet<i64>>> {
    let mut snapshot = super::paging::begin_history_snapshot(pool, "attribution").await?;
    let result = load_attribution_map(&mut snapshot, resource_ids, published).await?;
    snapshot.commit().await?;
    Ok(result)
}

pub(crate) async fn load_attribution_map(
    connection: &mut PgConnection,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<BTreeMap<Uuid, BTreeSet<i64>>> {
    let mut attributed = BTreeMap::<Uuid, BTreeSet<i64>>::new();
    if resource_ids.is_empty() {
        return Ok(attributed);
    }

    ensure_classification_publications(connection, resource_ids, published).await?;

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
    // the ENSv1 resolver the mirror would call. When the mirror cannot be followed, the resource
    // has no attributed writes at all, superseded pointers included.
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

/// The names each of `event_ids` was written for, judged at the write's own position.
///
/// Only resolver record writes that carry no name of their own (`RecordChanged` and
/// `RecordVersionChanged` with no logical name) are considered. A write is written for a name when
/// that name's resolver pointer selected the write's resolver at the write's position and, on a
/// record-ID resolver, the name's exact link selected the written record there (see
/// `sql::push_positional_names`). A pointer's window ends at the name's next pointer on that
/// registry, whichever resource carries it, so a pointer on a successor resource (after wrapping)
/// closes the predecessor's. The arms are the producer's attribution arms restricted to pointers
/// and links recorded before the write, and a name is dropped for a resource whose latest pointer
/// is an unfollowable mirror, which name history attributes nothing to; so a name returned here
/// also attributes the write in its own history, while a pointer or link recorded after the write
/// never names it. Callers name a write only when exactly one name is returned.
pub async fn load_positional_record_names(
    pool: &sqlx::PgPool,
    event_ids: &[i64],
) -> Result<BTreeMap<i64, BTreeSet<String>>> {
    let mut names = BTreeMap::<i64, BTreeSet<String>>::new();
    if event_ids.is_empty() {
        return Ok(names);
    }
    let mut snapshot = super::paging::begin_history_snapshot(pool, "record names").await?;
    let mut candidates = QueryBuilder::<Postgres>::new("");
    sql::push_positional_candidate_resources(&mut candidates, event_ids);
    let resource_ids: Vec<Uuid> = candidates
        .build_query_scalar()
        .fetch_all(&mut *snapshot)
        .await
        .context("failed to load the resources whose pointers can name record writes")?;
    if !resource_ids.is_empty() {
        ensure_classification_publications(&mut snapshot, &resource_ids, None).await?;
        let unfollowable =
            mirror::load_unfollowable_mirror_resources(&mut snapshot, &resource_ids, None).await?;
        let mut builder = QueryBuilder::<Postgres>::new("");
        sql::push_positional_names(&mut builder, &resource_ids, event_ids);
        for row in builder
            .build()
            .fetch_all(&mut *snapshot)
            .await
            .context("failed to load the names record writes were made for")?
        {
            if unfollowable.contains(&row.try_get::<Uuid, _>("resource_id")?) {
                continue;
            }
            names
                .entry(row.try_get("normalized_event_id")?)
                .or_default()
                .insert(row.try_get("logical_name_id")?);
        }
    }
    snapshot.commit().await?;
    Ok(names)
}

/// Classification is published current state even when the pointer walk is bounded history.
/// Admit only the chains that walk actually reaches, on the same snapshot as its F3 reads.
/// Raw audit reads keep working during Interpret redo: this checks classification publication,
/// not mutable composed-name identity, and therefore uses the marker rule without its redo guard.
async fn ensure_classification_publications(
    connection: &mut PgConnection,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<()> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    sql::push_pointer_ctes(&mut builder, resource_ids, published);
    builder.push(" SELECT DISTINCT chain_id FROM pointers");
    let chains: Vec<String> = builder
        .build_query_scalar()
        .fetch_all(&mut *connection)
        .await?;
    for chain_id in chains {
        let available: bool = sqlx::query_scalar(concat!(
            "SELECT EXISTS (SELECT 1 ",
            crate::snapshot_selection::servable_family_marker!(),
            ")"
        ))
        .bind(&chain_id)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .fetch_one(&mut *connection)
        .await?;
        if !available {
            return Err(crate::families::name::FamilyPublicationUnavailable { chain_id }.into());
        }
    }
    Ok(())
}
