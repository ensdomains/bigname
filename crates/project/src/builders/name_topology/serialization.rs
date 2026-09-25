use bigname_domain::resolution_topology::ResolutionTopology;
use serde_json::Value;
use sqlx::{FromRow, Postgres, QueryBuilder, Transaction};

use crate::{ProjectError, Result};

pub(in crate::builders) const SERIALIZATION_BATCH_SIZE: i64 = 2_000;

#[derive(FromRow)]
struct ProjectedTopologyRow {
    logical_name_id: String,
    topology: Value,
}

pub(super) async fn serialize_projected_topologies(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    serialize_projected_topologies_in_pages(transaction, SERIALIZATION_BATCH_SIZE).await
}

/// Reads the staged names whose topology is a JSON object one page at a time in key order,
/// serializes each topology through `ResolutionTopology`, and writes the page back.
pub(in crate::builders) async fn serialize_projected_topologies_in_pages(
    transaction: &mut Transaction<'_, Postgres>,
    page_size: i64,
) -> Result<()> {
    let mut after_logical_name_id: Option<String> = None;
    loop {
        let mut query = sqlx::query_as::<_, ProjectedTopologyRow>(page_statement(
            after_logical_name_id.is_some(),
        ))
        .bind(page_size);
        if let Some(after) = after_logical_name_id.as_deref() {
            query = query.bind(after);
        }
        let rows = query.fetch_all(&mut **transaction).await.map_err(|error| {
            ProjectError::database(
                "failed to load projected topologies for serialization",
                error,
            )
        })?;
        if rows.is_empty() {
            break;
        }

        after_logical_name_id = rows.last().map(|row| row.logical_name_id.clone());
        let page = serialize_page(rows)?;
        update_page("", &page)
            .build()
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to store serialized ResolutionTopology", error)
            })?;
    }
    Ok(())
}

/// One page of names whose topology is a JSON object, in key order: `$1` is the page size and,
/// when `bounded`, `$2` the last key of the page before, which the page starts after.
pub(in crate::builders) fn page_statement(bounded: bool) -> &'static str {
    if bounded {
        "/* project:builders.name_topology.serialization.page_after */ SELECT logical_name_id, declared_summary -> 'topology' AS topology
         FROM project_stage_name_current
         WHERE jsonb_typeof(declared_summary -> 'topology') = 'object'
           AND logical_name_id > $2
         ORDER BY logical_name_id
         LIMIT $1"
    } else {
        "/* project:builders.name_topology.serialization.first_page */ SELECT logical_name_id, declared_summary -> 'topology' AS topology
         FROM project_stage_name_current
         WHERE jsonb_typeof(declared_summary -> 'topology') = 'object'
         ORDER BY logical_name_id
         LIMIT $1"
    }
}

fn serialize_page(rows: Vec<ProjectedTopologyRow>) -> Result<Vec<(String, Value)>> {
    rows.into_iter()
        .map(|row| {
            let topology =
                serde_json::from_value::<ResolutionTopology>(row.topology).map_err(|error| {
                    ProjectError::data_integrity(format!(
                        "projected topology for {} does not match ResolutionTopology: {error}",
                        row.logical_name_id
                    ))
                })?;
            let topology = serde_json::to_value(topology).map_err(|error| {
                ProjectError::data_integrity(format!(
                    "failed to serialize ResolutionTopology for {}: {error}",
                    row.logical_name_id
                ))
            })?;
            Ok((row.logical_name_id, topology))
        })
        .collect()
}

/// The statement that writes one serialized page back, prefixed by `prefix` (empty to run it).
/// The page was read in key order, so its first and last keys bound the names it touches, and
/// the name index serves the write instead of a read of the whole stage.
pub(in crate::builders) fn update_page<'a>(
    prefix: &str,
    page: &'a [(String, Value)],
) -> QueryBuilder<'a, Postgres> {
    let mut update = QueryBuilder::<Postgres>::new(format!(
        "/* project:builders.name_topology.serialization.update_page */ {prefix}UPDATE project_stage_name_current AS name SET declared_summary = jsonb_set(\
         name.declared_summary, '{{topology}}', serialized.topology, true) FROM ("
    ));
    update.push_values(page.iter(), |mut values, (logical_name_id, topology)| {
        values.push_bind(logical_name_id).push_bind(topology);
    });
    update.push(
        ") AS serialized(logical_name_id, topology) \
         WHERE name.logical_name_id = serialized.logical_name_id",
    );
    if let (Some((first, _)), Some((last, _))) = (page.first(), page.last()) {
        update
            .push(" AND name.logical_name_id >= ")
            .push_bind(first)
            .push(" AND name.logical_name_id <= ")
            .push_bind(last);
    }
    update
}
