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

pub(in crate::history) use sql::{
    ENS_V1_POINTER_FAMILIES, push_readable_event, push_readable_surface,
};

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use super::selectors::HistorySelector;

/// The `SourceManifestUpdated` event ids of the manifest set each bounded chain's publication
/// recorded (`FamilyPublication::admission_manifests`), by chain.
pub(crate) type ManifestSets = BTreeMap<String, Vec<i64>>;

/// The attribution statement for `resource_ids`, for plan tests.
#[cfg(test)]
pub(in crate::history) fn push_pointer_window_attribution_for_test<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource_ids: &'a [Uuid],
    published: Option<&BTreeMap<String, i64>>,
) {
    sql::push_pointer_window_attribution(builder, resource_ids, published, None, None);
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
    pub(in crate::history) fn from_map(map: BTreeMap<Uuid, BTreeSet<i64>>) -> Self {
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
        .flat_map(HistorySelector::resource_ids)
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
    load_attribution_map_restricted(connection, resource_ids, published, None, None).await
}

/// [`load_attribution_map`] at `published` with the manifest sets of the publications the caller
/// composes, which a family block publishes before its marker records them. Without them, a
/// bounded read takes each reached chain's family marker set.
pub(crate) async fn load_attribution_map_at(
    connection: &mut PgConnection,
    resource_ids: &[Uuid],
    published: &BTreeMap<String, i64>,
    manifests: &ManifestSets,
) -> Result<BTreeMap<Uuid, BTreeSet<i64>>> {
    load_attribution_map_restricted(
        connection,
        resource_ids,
        Some(published),
        Some(manifests),
        None,
    )
    .await
}

/// The manifest set of the family marker of each of `chains`. A bounded read serves the current
/// publications, the ones whose classifications it reads.
async fn marker_manifest_sets(
    connection: &mut PgConnection,
    chains: &[String],
) -> Result<ManifestSets> {
    if chains.is_empty() {
        return Ok(ManifestSets::new());
    }
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "/* storage:history.attribution.manifest_sets */
         SELECT chain_id, admission_manifests FROM bigname_phase.project_family_marker
         WHERE chain_id = ANY($1)",
    )
    .bind(chains)
    .fetch_all(&mut *connection)
    .await
    .context("failed to load the publications' manifest sets")?;
    rows.into_iter()
        .map(|(chain_id, key)| {
            Ok((
                chain_id,
                crate::families::name::manifest_set_event_ids(key.as_deref())?,
            ))
        })
        .collect()
}

/// The parallel arrays are a set of requested resource/event pairs, not their Cartesian
/// product. Each SQL result arm is restricted before rows cross the database connection.
struct RequestedPairs {
    resources: Vec<Uuid>,
    events: Vec<i64>,
}

impl RequestedPairs {
    fn push_event_cte(&self, builder: &mut QueryBuilder<'_, Postgres>) {
        // Materialize at most this batch's events once. This prevents each pointer from
        // scanning its complete write history before applying the requested-pair filter.
        builder.push(", requested_history_records AS MATERIALIZED (SELECT * FROM bigname_phase.normalized_events WHERE normalized_event_id = ANY(");
        builder.push_bind(self.events.clone()).push("::bigint[]))");
    }
    fn push_filter(&self, builder: &mut QueryBuilder<'_, Postgres>, resource: &str, event: &str) {
        // The standalone event predicate exposes the bounded primary-key probe before the
        // pair check. Pointer and link window evidence is deliberately not filtered here.
        builder.push(format!(" AND {event} = ANY("));
        builder
            .push_bind(self.events.clone())
            .push("::bigint[]) AND (");
        builder.push(resource).push(", ").push(event);
        builder.push(") IN (SELECT * FROM unnest(");
        builder.push_bind(self.resources.clone());
        builder.push("::uuid[], ");
        builder.push_bind(self.events.clone());
        builder.push("::bigint[]))");
    }
}

#[cfg(test)]
pub(in crate::history) fn push_paired_attribution_for_test<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    resource_ids: &'a [Uuid],
    event_ids: &[i64],
    published: Option<&BTreeMap<String, i64>>,
) {
    assert_eq!(resource_ids.len(), event_ids.len());
    let requested = RequestedPairs {
        resources: resource_ids.to_vec(),
        events: event_ids.to_vec(),
    };
    sql::push_pointer_window_attribution(builder, resource_ids, published, None, Some(&requested));
}

/// Validate a bounded set of resource/event pairs with the full historical attribution rules.
/// Pointer and link boundaries are unchanged; unrelated attributed events never leave SQL.
pub(in crate::history) async fn matching_attribution_pairs(
    connection: &mut PgConnection,
    pairs: &[(Uuid, i64)],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<BTreeSet<(Uuid, i64)>> {
    let resources: Vec<Uuid> = pairs
        .iter()
        .map(|(resource, _)| *resource)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let requested = RequestedPairs {
        resources: pairs.iter().map(|(resource, _)| *resource).collect(),
        events: pairs.iter().map(|(_, event)| *event).collect(),
    };
    Ok(
        load_attribution_map_restricted(connection, &resources, published, None, Some(&requested))
            .await?
            .into_iter()
            .flat_map(|(resource, events)| events.into_iter().map(move |event| (resource, event)))
            .collect(),
    )
}

async fn load_attribution_map_restricted(
    connection: &mut PgConnection,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
    manifests: Option<&ManifestSets>,
    requested: Option<&RequestedPairs>,
) -> Result<BTreeMap<Uuid, BTreeSet<i64>>> {
    let mut attributed = BTreeMap::<Uuid, BTreeSet<i64>>::new();
    if resource_ids.is_empty() || requested.is_some_and(|pairs| pairs.events.is_empty()) {
        return Ok(attributed);
    }

    let chains = ensure_classification_publications(connection, resource_ids, published).await?;
    let marker_sets;
    let manifests = match (published, manifests) {
        (Some(_), None) => {
            marker_sets = marker_manifest_sets(connection, &chains).await?;
            Some(&marker_sets)
        }
        (_, manifests) => manifests,
    };

    let mut builder = QueryBuilder::<Postgres>::new("");
    sql::push_pointer_window_attribution(
        &mut builder,
        resource_ids,
        published,
        manifests,
        requested,
    );
    let rows = builder
        .build()
        .fetch_all(&mut *connection)
        .await
        .context("failed to load pointer-attributed record writes")?;
    let _rows_live = requested
        .map(|_| super::address_walk::seams::Live::new("attribution_sql_rows", rows.len()));
    let mut map_live =
        requested.map(|_| super::address_walk::seams::Live::new("attribution_map_pairs", 0));
    if requested.is_some() {
        super::address_walk::seams::count("attribution_rows_returned", rows.len());
    }
    for row in rows {
        attributed
            .entry(row.try_get("resource_id")?)
            .or_default()
            .insert(row.try_get("normalized_event_id")?);
        if let Some(live) = map_live.as_mut() {
            live.set(attributed.values().map(BTreeSet::len).sum());
        }
    }

    drop(_rows_live);
    if let Some(live) = map_live.as_mut() {
        live.set(attributed.values().map(BTreeSet::len).sum());
    }

    // A resource whose latest pointer is a mirror resolver serves, and attributes, the writes of
    // the ENSv1 resolver the mirror would call. When the mirror cannot be followed, the resource
    // has no attributed writes at all, superseded pointers included.
    let mirrored =
        mirror::load_mirror_attribution(connection, resource_ids, published, manifests, requested)
            .await?;
    let _mirrored_pairs = requested.map(|_| {
        super::address_walk::seams::Live::new(
            "mirror_substitution_pairs",
            mirrored
                .values()
                .filter_map(Option::as_ref)
                .map(BTreeSet::len)
                .sum(),
        )
    });
    let _mirrored_resources = requested.map(|_| {
        super::address_walk::seams::Live::new("mirror_substitution_resources", mirrored.len())
    });
    for (resource_id, substitution) in mirrored {
        match substitution {
            Some(event_ids) => attributed.entry(resource_id).or_default().extend(event_ids),
            None => {
                attributed.remove(&resource_id);
            }
        }
        if let Some(live) = map_live.as_mut() {
            live.set(attributed.values().map(BTreeSet::len).sum());
        }
    }
    attributed.retain(|_, event_ids| !event_ids.is_empty());
    Ok(attributed)
}

/// Classification is published current state even when the pointer walk is bounded history.
/// Admit only the chains that walk actually reaches, on the same snapshot as its F3 reads, and
/// return them.
/// Raw audit reads keep working during Interpret redo: this checks classification publication,
/// not mutable composed-name identity, and therefore uses the marker rule without its redo guard.
async fn ensure_classification_publications(
    connection: &mut PgConnection,
    resource_ids: &[Uuid],
    published: Option<&BTreeMap<String, i64>>,
) -> Result<Vec<String>> {
    let mut builder = QueryBuilder::<Postgres>::new("");
    sql::push_pointer_ctes(&mut builder, resource_ids, published);
    builder.push(" SELECT DISTINCT chain_id FROM pointers");
    let chains: Vec<String> = builder
        .build_query_scalar()
        .fetch_all(&mut *connection)
        .await?;
    for chain_id in &chains {
        let available: bool = sqlx::query_scalar(concat!(
            "SELECT EXISTS (SELECT 1 ",
            crate::snapshot_selection::servable_family_marker!(),
            ")"
        ))
        .bind(chain_id)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .fetch_one(&mut *connection)
        .await?;
        if !available {
            return Err(crate::families::name::FamilyPublicationUnavailable {
                chain_id: chain_id.clone(),
            }
            .into());
        }
    }
    Ok(chains)
}
