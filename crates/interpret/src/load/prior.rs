use std::collections::BTreeMap;

use bigname_adapters::schema_v2::PriorEventInput;
use sqlx::{PgConnection, types::Uuid};

use crate::{InterpretError, Result};
use bigname_adapters::schema_v2::seam::{
    INTERPRETER_STATE_KEY, STATE_SCOPE_KEY, retained_prior_state_key,
};

use super::cache::{PriorDependency, PriorSnapshot};

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
    String,
    Option<String>,
    i64,
    String,
    Option<time::OffsetDateTime>,
    serde_json::Value,
);

pub(super) async fn events(
    connection: &mut PgConnection,
    chain_id: &str,
    before_block: i64,
) -> Result<PriorSnapshot> {
    // The content-hashed adapter owns the opaque state key. Rows without one are intentionally
    // keyed by event identity, so this transport layer never invents compaction semantics.
    let statement = format!(
        "
        WITH ranked AS (
            SELECT event.*,
                   live_lineage.block_timestamp AS retained_block_timestamp,
                   row_number() OVER (
                       PARTITION BY
                           event.raw_fact_ref ? '{INTERPRETER_STATE_KEY}',
                           public.digest(
                               COALESCE(
                                   event.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}',
                                   event.event_identity
                               ),
                               'sha256'
                           ),
                           COALESCE(
                               event.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}',
                               event.event_identity
                           )
                       ORDER BY event.block_number DESC,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS state_rank
            FROM normalized_events event
            JOIN chain_lineage live_lineage
              ON live_lineage.chain_id = event.chain_id
             AND live_lineage.block_hash = event.block_hash
             AND live_lineage.block_number = event.block_number
             AND live_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
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
               ranked.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}',
               ranked.event_identity,
               ranked.raw_fact_ref ->> '{STATE_SCOPE_KEY}',
               ranked.block_number,
               ranked.block_hash,
               ranked.retained_block_timestamp,
               ranked.after_state
        FROM ranked
        WHERE ranked.state_rank = 1
        ORDER BY ranked.block_number,
                 ranked.transaction_index NULLS FIRST,
                 ranked.log_index NULLS FIRST,
                 ranked.normalized_event_id
        "
    );
    let rows: Vec<Row> = sqlx::query_as(&statement)
        .bind(chain_id)
        .bind(before_block)
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            InterpretError::database("failed to load prior adapter state events", error)
        })?;
    let mut events = Vec::with_capacity(rows.len());
    let mut dependencies = BTreeMap::new();
    for (
        chain_id,
        namespace,
        logical_name_id,
        resource_id,
        event_kind,
        source_family,
        manifest_version,
        source_manifest_id,
        interpreter_state_key,
        event_identity,
        state_scope,
        block_number,
        block_hash,
        block_timestamp,
        after_state,
    ) in rows
    {
        let retained_state_key =
            retained_prior_state_key(interpreter_state_key.as_deref(), &event_identity);
        dependencies.insert(
            retained_state_key.clone(),
            PriorDependency {
                block_number,
                block_hash,
            },
        );
        events.push(PriorEventInput {
            retained_state_key,
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
        });
    }
    Ok(PriorSnapshot {
        events,
        dependencies,
    })
}
