use bigname_adapters::schema_v2::PriorEventInput;
use sqlx::{PgPool, types::Uuid};

use crate::{InterpretError, Result};

type Row = (
    String,
    String,
    Option<String>,
    Option<Uuid>,
    String,
    String,
    i64,
    Option<i64>,
    Option<String>,
    Option<time::OffsetDateTime>,
    serde_json::Value,
);

pub(super) async fn events(
    pool: &PgPool,
    chain_id: &str,
    before_block: i64,
) -> Result<Vec<PriorEventInput>> {
    // The content-hashed adapter owns the opaque state key. Rows without one are intentionally
    // keyed by event identity, so this transport layer never invents compaction semantics.
    let rows: Vec<Row> = sqlx::query_as(
        "
        WITH ranked AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY
                           event.raw_fact_ref ? 'interpreter_state_key',
                           COALESCE(
                               event.raw_fact_ref ->> 'interpreter_state_key',
                               event.event_identity
                           )
                       ORDER BY event.block_number DESC,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS state_rank
            FROM normalized_events event
            WHERE event.chain_id = $1
              AND event.block_number < $2
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        )
        SELECT ranked.chain_id,
               ranked.namespace,
               ranked.logical_name_id,
               ranked.resource_id,
               ranked.event_kind,
               ranked.source_family,
               ranked.manifest_version,
               ranked.source_manifest_id,
               ranked.raw_fact_ref ->> 'state_scope',
               lineage.block_timestamp,
               ranked.after_state
        FROM ranked
        LEFT JOIN chain_lineage lineage
          ON lineage.chain_id = ranked.chain_id
         AND lineage.block_hash = ranked.block_hash
         AND lineage.block_number = ranked.block_number
        WHERE ranked.state_rank = 1
        ORDER BY ranked.block_number,
                 ranked.transaction_index NULLS FIRST,
                 ranked.log_index NULLS FIRST,
                 ranked.normalized_event_id
        ",
    )
    .bind(chain_id)
    .bind(before_block)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load prior adapter state events", error)
    })?;
    Ok(rows
        .into_iter()
        .map(
            |(
                chain_id,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                state_scope,
                block_timestamp,
                after_state,
            )| PriorEventInput {
                chain_id,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                state_scope,
                block_timestamp,
                after_state,
            },
        )
        .collect())
}
