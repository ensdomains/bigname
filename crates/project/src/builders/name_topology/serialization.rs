use bigname_domain::resolution_topology::ResolutionTopology;
use serde_json::Value;
use sqlx::{FromRow, Postgres, QueryBuilder, Transaction};

use crate::{ProjectError, Result};

const SERIALIZATION_BATCH_SIZE: i64 = 2_000;

#[derive(FromRow)]
struct ProjectedTopologyRow {
    logical_name_id: String,
    topology: Value,
}

pub(super) async fn serialize_projected_topologies(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    let mut after_logical_name_id: Option<String> = None;
    loop {
        let rows = sqlx::query_as::<_, ProjectedTopologyRow>(
            r#"
            SELECT logical_name_id, declared_summary -> 'topology' AS topology
            FROM project_stage_name_current
            WHERE jsonb_typeof(declared_summary -> 'topology') = 'object'
              AND ($1::text IS NULL OR logical_name_id > $1)
            ORDER BY logical_name_id
            LIMIT $2
            "#,
        )
        .bind(after_logical_name_id.as_deref())
        .bind(SERIALIZATION_BATCH_SIZE)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database(
                "failed to load projected topologies for serialization",
                error,
            )
        })?;
        if rows.is_empty() {
            break;
        }

        after_logical_name_id = rows.last().map(|row| row.logical_name_id.clone());
        let serialized =
            rows.into_iter()
                .map(|row| {
                    let topology = serde_json::from_value::<ResolutionTopology>(row.topology)
                    .map_err(|error| {
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
                .collect::<Result<Vec<_>>>()?;

        let mut update = QueryBuilder::<Postgres>::new(
            "UPDATE project_stage_name_current AS name SET declared_summary = jsonb_set(\
             name.declared_summary, '{topology}', serialized.topology, true) FROM (",
        );
        update.push_values(
            serialized.iter(),
            |mut values, (logical_name_id, topology)| {
                values.push_bind(logical_name_id).push_bind(topology);
            },
        );
        update.push(
            ") AS serialized(logical_name_id, topology) \
             WHERE name.logical_name_id = serialized.logical_name_id",
        );
        update
            .build()
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to store serialized ResolutionTopology", error)
            })?;
    }
    Ok(())
}
