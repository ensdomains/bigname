use bigname_adapters::schema_v2::{
    AdapterSessionRestore, InterpreterStateRequest, InterpreterStateValue, PriorEventInput,
};
use futures_util::TryStreamExt;
use sqlx::{PgConnection, PgPool, types::Uuid};

use crate::{InterpretError, Result};
use bigname_adapters::schema_v2::seam::{
    INTERPRETER_STATE_KEY, STATE_SCOPE_KEY, SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY,
    retained_event_state_key, retained_prior_state_key,
};

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

#[cfg(test)]
#[rustfmt::skip]
mod tests { #[test] fn prior_restore_orders_same_block_state_by_normalized_emission() { assert!(include_str!("prior.rs").contains("ORDER BY ranked.block_number, ranked.normalized_event_id")); } }

pub(crate) async fn prior_state_values(
    pool: &PgPool,
    chain_id: &str,
    before_block: i64,
    requests: &[InterpreterStateRequest],
) -> Result<Vec<InterpreterStateValue>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    let mut transaction = pool.begin().await.map_err(|error| {
        InterpretError::database("failed to begin adapter-state reload snapshot", error)
    })?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to configure adapter-state reload snapshot", error)
        })?;
    let values = state_values(&mut transaction, chain_id, before_block, requests).await?;
    transaction.commit().await.map_err(|error| {
        InterpretError::database("failed to commit adapter-state reload snapshot", error)
    })?;
    Ok(values)
}

pub(super) async fn restore_events(
    connection: &mut PgConnection,
    chain_id: &str,
    before_block: i64,
    restore: &mut AdapterSessionRestore,
) -> Result<usize> {
    // The content-hashed adapter owns the opaque state key. Rows without one stay keyed by event
    // identity, and its clear marker alone retains one additional row.
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
                           ),
                           event.after_state ? '{SUBREGISTRY_INVALIDATED_TOKEN_IDS_KEY}'
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
        ORDER BY ranked.block_number, ranked.normalized_event_id
        "
    );
    let mut rows = sqlx::query_as::<_, Row>(&statement)
        .bind(chain_id)
        .bind(before_block)
        .fetch(&mut *connection);
    let mut count = 0_usize;
    let mut chunk = Vec::with_capacity(1_024);
    while let Some(row) = rows.try_next().await.map_err(|error| {
        InterpretError::database("failed to stream prior adapter state events", error)
    })? {
        chunk.push(row_to_event(row));
        count = count.saturating_add(1);
        if chunk.len() == chunk.capacity() {
            restore
                .apply_prior_events(std::mem::take(&mut chunk))
                .map_err(|error| {
                    InterpretError::data_integrity(format!(
                        "failed to restore streamed adapter state: {error:#}"
                    ))
                })?;
            chunk = Vec::with_capacity(1_024);
        }
    }
    if !chunk.is_empty() {
        restore.apply_prior_events(chunk).map_err(|error| {
            InterpretError::data_integrity(format!(
                "failed to restore streamed adapter state: {error:#}"
            ))
        })?;
    }
    Ok(count)
}

fn row_to_event(
    (
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
        _block_number,
        _block_hash,
        block_timestamp,
        after_state,
    ): Row,
) -> PriorEventInput {
    PriorEventInput {
        retained_state_key: retained_event_state_key(
            retained_prior_state_key(interpreter_state_key.as_deref(), &event_identity),
            &after_state,
        ),
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
    }
}

pub(super) async fn state_values(
    connection: &mut PgConnection,
    chain_id: &str,
    before_block: i64,
    requests: &[InterpreterStateRequest],
) -> Result<Vec<InterpreterStateValue>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    let state_keys = requests
        .iter()
        .map(|request| request.state_key.clone())
        .collect::<Vec<_>>();
    // Keep the digest operand syntactically aligned with
    // normalized_events_interpreter_state_history_idx. PostgreSQL expression-index matching does
    // not simplify the COALESCE merely because the query also requires the JSON key to exist. The
    // index's nullable descending columns sort NULL first, while interpreter order puts block-level
    // events before logs. Seek the latest block first, then apply the semantic ordering only inside
    // that block so a miss never sorts a key's full history.
    let statement = format!(
        "
        WITH requested AS (
            SELECT state_key, ordinal
            FROM unnest($3::text[]) WITH ORDINALITY AS entry(state_key, ordinal)
        )
        SELECT request.state_key, prior.after_state
        FROM requested request
        JOIN LATERAL (
            SELECT candidate.block_number
            FROM normalized_events candidate
            JOIN chain_lineage live_lineage
              ON live_lineage.chain_id = candidate.chain_id
             AND live_lineage.block_hash = candidate.block_hash
             AND live_lineage.block_number = candidate.block_number
             AND live_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            WHERE candidate.chain_id = $1
              AND candidate.block_number < $2
              AND candidate.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND candidate.raw_fact_ref ? '{INTERPRETER_STATE_KEY}'
              AND public.digest(
                      COALESCE(
                          candidate.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}',
                          candidate.event_identity
                      ),
                      'sha256'
                  ) = public.digest(request.state_key, 'sha256')
              AND candidate.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}' = request.state_key
            ORDER BY candidate.block_number DESC
            LIMIT 1
        ) latest_block ON TRUE
        JOIN LATERAL (
            SELECT event.after_state
            FROM normalized_events event
            JOIN chain_lineage live_lineage
              ON live_lineage.chain_id = event.chain_id
             AND live_lineage.block_hash = event.block_hash
             AND live_lineage.block_number = event.block_number
             AND live_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            WHERE event.chain_id = $1
              AND event.block_number = latest_block.block_number
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND event.raw_fact_ref ? '{INTERPRETER_STATE_KEY}'
              AND public.digest(
                      COALESCE(
                          event.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}',
                          event.event_identity
                      ),
                      'sha256'
                  ) = public.digest(request.state_key, 'sha256')
              AND event.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}' = request.state_key
            ORDER BY event.block_number DESC,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) prior ON TRUE
        ORDER BY request.ordinal
        "
    );
    let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(&statement)
        .bind(chain_id)
        .bind(before_block)
        .bind(state_keys)
        .fetch_all(&mut *connection)
        .await
        .map_err(|error| {
            InterpretError::database("failed to reload requested adapter state values", error)
        })?;
    Ok(rows
        .into_iter()
        .map(|(state_key, after_state)| InterpreterStateValue {
            state_key,
            after_state,
        })
        .collect())
}
