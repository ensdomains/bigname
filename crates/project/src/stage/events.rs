use crate::{ProjectError, Result};
use sqlx::{Postgres, Transaction};

pub(super) async fn create(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
    full_rebuild: bool,
) -> Result<()> {
    let event_source = if full_rebuild {
        "normalized_events event"
    } else {
        "project_event_ids scope JOIN LATERAL (
             SELECT * FROM normalized_events
             WHERE normalized_event_id = scope.normalized_event_id OFFSET 0
         ) event ON TRUE"
    };
    #[cfg(test)]
    let event_source = if !full_rebuild && crate::reference::enabled(transaction).await? {
        "normalized_events event JOIN project_event_ids scope
           ON scope.normalized_event_id = event.normalized_event_id"
    } else {
        event_source
    };
    let mut statement = format!(
        "/* project:stage.events.create.create_events */ CREATE TEMP TABLE project_events ON COMMIT DROP AS
         SELECT event.*
         FROM {event_source}
         LEFT JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND (
               (event.block_number IS NULL AND event.block_hash IS NULL)
               OR (
                   event.block_number <= $2
                   AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               )
           )"
    );
    #[cfg(test)]
    let reference = crate::reference::enabled(transaction).await?;
    #[cfg(not(test))]
    let reference = false;
    if !full_rebuild && !reference {
        for create in [
            "/* project:stage.events.create.create_empty_events */ CREATE TEMP TABLE project_events ON COMMIT DROP AS
                 SELECT * FROM normalized_events WITH NO DATA",
            "/* project:stage.events.create.create_staged_event_ids */ CREATE TEMP TABLE project_staged_event_ids (
                 normalized_event_id bigint PRIMARY KEY
             ) ON COMMIT DROP",
        ] {
            sqlx::query(create)
                .execute(&mut **transaction)
                .await
                .map_err(|error| {
                    ProjectError::database("failed to create canonical event stages", error)
                })?;
        }
        statement = SCOPED_EVENTS_SQL.to_owned();
    }
    #[cfg(test)]
    let captured = crate::profile::execute(
        transaction,
        chain_id,
        target_block,
        &statement,
        crate::profile::Stage::Events,
    )
    .await?;
    #[cfg(not(test))]
    let captured = false;
    if !captured {
        sqlx::query(&statement)
            .bind(chain_id)
            .bind(target_block)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to stage canonical events", error))?;
    }
    for statement in [
        "/* project:stage.events.create.index_events_logical_name_id */ CREATE INDEX ON project_events (logical_name_id, normalized_event_id)",
        "/* project:stage.events.create.index_events_resource_id */ CREATE INDEX ON project_events (resource_id, normalized_event_id)",
        "/* project:stage.events.create.index_events_event_kind */ CREATE INDEX ON project_events (event_kind, normalized_event_id)",
        "/* project:stage.events.create.index_events_event_kind_chain */ CREATE INDEX ON project_events (event_kind, chain_id, normalized_event_id)",
        // Resolver pointers and records are looked up by node, once per reverse claim in primary
        // names. The index covers every row: the planner reads expression statistics only from
        // a complete index, and with a partial one it guessed hundreds of rows per node.
        "/* project:stage.events.create.index_events_after_state */ CREATE INDEX ON project_events (lower(after_state ->> 'node'))",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to index staged events", error))?;
    }
    Ok(())
}

// Retain the inserted IDs without rereading the wide event stage. Initial scope IDs may
// include events rejected by visibility or lineage admission and cannot serve this purpose.
const SCOPED_EVENTS_SQL: &str = r#"/* project:stage.events.scoped_events_sql */
WITH inserted AS (
    INSERT INTO project_events
    SELECT event.*
    FROM project_event_ids scope
    JOIN LATERAL (
        SELECT * FROM normalized_events
        WHERE normalized_event_id = scope.normalized_event_id OFFSET 0
    ) event ON TRUE
    LEFT JOIN LATERAL (
        SELECT canonicality_state FROM chain_lineage
        WHERE chain_id = event.chain_id
          AND block_hash = event.block_hash
          AND block_number = event.block_number
        OFFSET 0
    ) lineage ON TRUE
    WHERE event.chain_id = $1
      AND event.consumer_visibility = 'activated'
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND (
          (event.block_number IS NULL AND event.block_hash IS NULL)
          OR (
              event.block_number <= $2
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          )
      )
    RETURNING normalized_event_id
)
INSERT INTO project_staged_event_ids
SELECT normalized_event_id FROM inserted
"#;

#[cfg(test)]
#[path = "events_tests.rs"]
mod tests;
