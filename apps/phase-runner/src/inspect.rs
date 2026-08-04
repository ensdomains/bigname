use std::str::FromStr;

use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgConnectOptions};

use crate::{
    database::stamp_interpreter_content_hash,
    error::{RunnerError, RunnerResult},
    phase::BlockRange,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InspectionKind {
    BlockCanonicality,
    StoredLineage,
    RawEvents,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectionRequest {
    pub kind: InspectionKind,
    pub chain_id: String,
    pub range: BlockRange,
}

pub async fn run(database_url: &str, request: InspectionRequest) -> RunnerResult<()> {
    let pool = connect_read_only(database_url).await?;
    let output = inspect(&pool, &request).await?;
    println!("{output}");
    pool.close().await;
    Ok(())
}

pub async fn inspect(pool: &PgPool, request: &InspectionRequest) -> RunnerResult<Value> {
    let mut transaction = pool.begin().await.map_err(|error| {
        RunnerError::database("failed to begin schema-v2 inspection window", error)
    })?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            RunnerError::database("failed to configure read-only inspection window", error)
        })?;
    let rows = match request.kind {
        InspectionKind::BlockCanonicality => block_canonicality(&mut transaction, request).await?,
        InspectionKind::StoredLineage => stored_lineage(&mut transaction, request).await?,
        InspectionKind::RawEvents => raw_events(&mut transaction, request).await?,
    };
    transaction.commit().await.map_err(|error| {
        RunnerError::database("failed to close schema-v2 inspection window", error)
    })?;
    let row_key = match request.kind {
        InspectionKind::RawEvents => "events",
        _ => "blocks",
    };
    Ok(json!({
        "command": command_name(request.kind),
        "chain_id": request.chain_id,
        "range": {
            "from_block": request.range.from,
            "to_block": request.range.to,
        },
        (row_key): rows,
    }))
}

async fn connect_read_only(database_url: &str) -> RunnerResult<PgPool> {
    let options = PgConnectOptions::from_str(database_url)
        .map_err(|error| {
            RunnerError::new(
                crate::error::ErrorKind::Configuration,
                format!("failed to parse inspection database URL: {error}"),
            )
        })?
        .options([("default_transaction_read_only", "on")]);
    let options = stamp_interpreter_content_hash(options);
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| {
            RunnerError::transient(format!("failed to connect inspection pool: {error}"))
        })
}

async fn block_canonicality(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &InspectionRequest,
) -> RunnerResult<Vec<Value>> {
    query_rows(
        transaction,
        request,
        r#"
        SELECT jsonb_build_object(
            'block_number', lineage.block_number,
            'block_hash', lineage.block_hash,
            'parent_hash', lineage.parent_hash,
            'timestamp', lineage.block_timestamp,
            'canonicality_state', lineage.canonicality_state::text,
            'header_audit_present', audit.block_hash IS NOT NULL,
            'raw_fact_counts', jsonb_build_object(
                'transactions', (SELECT count(*) FROM raw_transactions fact
                    WHERE fact.chain_id = lineage.chain_id AND fact.block_hash = lineage.block_hash),
                'receipts', (SELECT count(*) FROM raw_receipts fact
                    WHERE fact.chain_id = lineage.chain_id AND fact.block_hash = lineage.block_hash),
                'logs', (SELECT count(*) FROM raw_logs fact
                    WHERE fact.chain_id = lineage.chain_id AND fact.block_hash = lineage.block_hash)
            ),
            'normalized_event_count', (SELECT count(*) FROM normalized_events event
                WHERE event.chain_id = lineage.chain_id AND event.block_hash = lineage.block_hash)
        )
        FROM chain_lineage lineage
        LEFT JOIN chain_header_audit audit
          ON audit.chain_id = lineage.chain_id AND audit.block_hash = lineage.block_hash
        WHERE lineage.chain_id = $1 AND lineage.block_number BETWEEN $2 AND $3
        ORDER BY lineage.block_number, lineage.block_hash
        "#,
        "block-canonicality",
    )
    .await
}

async fn stored_lineage(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &InspectionRequest,
) -> RunnerResult<Vec<Value>> {
    query_rows(
        transaction,
        request,
        r#"
        SELECT jsonb_build_object(
            'block_number', lineage.block_number,
            'block_hash', lineage.block_hash,
            'parent_hash', lineage.parent_hash,
            'timestamp', lineage.block_timestamp,
            'canonicality_state', lineage.canonicality_state::text,
            'header_audit', CASE WHEN audit.block_hash IS NULL THEN NULL ELSE jsonb_build_object(
                'logs_bloom', CASE WHEN audit.logs_bloom IS NULL THEN NULL
                    ELSE '0x' || encode(audit.logs_bloom, 'hex') END,
                'transactions_root', audit.transactions_root,
                'receipts_root', audit.receipts_root,
                'state_root', audit.state_root,
                'observed_at', audit.observed_at
            ) END
        )
        FROM chain_lineage lineage
        LEFT JOIN chain_header_audit audit
          ON audit.chain_id = lineage.chain_id AND audit.block_hash = lineage.block_hash
        WHERE lineage.chain_id = $1 AND lineage.block_number BETWEEN $2 AND $3
        ORDER BY lineage.block_number, lineage.block_hash
        "#,
        "stored-lineage",
    )
    .await
}

async fn raw_events(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &InspectionRequest,
) -> RunnerResult<Vec<Value>> {
    query_rows(
        transaction,
        request,
        r#"
        SELECT jsonb_build_object(
            'block_number', lineage.block_number,
            'block_hash', lineage.block_hash,
            'canonicality_state', lineage.canonicality_state::text,
            'header_audit_present', audit.block_hash IS NOT NULL,
            'transaction', jsonb_build_object(
                'transaction_hash', raw_transaction.transaction_hash,
                'transaction_index', raw_transaction.transaction_index,
                'from_address', raw_transaction.from_address,
                'to_address', raw_transaction.to_address,
                'input', '0x' || encode(raw_transaction.input, 'hex'),
                'value', raw_transaction.value::text
            ),
            'receipt', CASE WHEN receipt.transaction_hash IS NULL THEN NULL ELSE jsonb_build_object(
                'status', receipt.status,
                'contract_address', receipt.contract_address,
                'gas_used', receipt.gas_used::text,
                'cumulative_gas_used', receipt.cumulative_gas_used::text,
                'logs_bloom', CASE WHEN receipt.logs_bloom IS NULL THEN NULL
                    ELSE '0x' || encode(receipt.logs_bloom, 'hex') END
            ) END,
            'log', jsonb_build_object(
                'log_index', log.log_index,
                'emitting_address', log.emitting_address,
                'topics', log.topics,
                'data', '0x' || encode(log.data, 'hex')
            ),
            'normalized_events', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'normalized_event_id', event.normalized_event_id,
                    'event_kind', event.event_kind,
                    'derivation_kind', event.derivation_kind,
                    'logical_name_id', event.logical_name_id,
                    'resource_id', event.resource_id,
                    'canonicality_state', event.canonicality_state::text,
                    'before_state', event.before_state,
                    'after_state', event.after_state
                ) ORDER BY event.normalized_event_id)
                FROM normalized_events event
                WHERE event.chain_id = log.chain_id
                  AND event.block_hash = log.block_hash
                  AND event.raw_fact_ref ->> 'transaction_hash' = log.transaction_hash
                  AND event.raw_fact_ref ->> 'log_index' = log.log_index::text
            ), '[]'::jsonb)
        )
        FROM raw_logs log
        JOIN chain_lineage lineage
          ON lineage.chain_id = log.chain_id
         AND lineage.block_hash = log.block_hash
         AND lineage.block_number = log.block_number
        JOIN raw_transactions raw_transaction
          ON raw_transaction.chain_id = log.chain_id
         AND raw_transaction.block_hash = log.block_hash
         AND raw_transaction.transaction_hash = log.transaction_hash
        LEFT JOIN raw_receipts receipt
          ON receipt.chain_id = raw_transaction.chain_id
         AND receipt.block_hash = raw_transaction.block_hash
         AND receipt.transaction_hash = raw_transaction.transaction_hash
        LEFT JOIN chain_header_audit audit
          ON audit.chain_id = lineage.chain_id AND audit.block_hash = lineage.block_hash
        WHERE log.chain_id = $1 AND log.block_number BETWEEN $2 AND $3
        ORDER BY log.block_number, log.block_hash, log.transaction_index, log.log_index
        "#,
        "raw-events",
    )
    .await
}

async fn query_rows(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &InspectionRequest,
    statement: &str,
    window: &str,
) -> RunnerResult<Vec<Value>> {
    sqlx::query_scalar(statement)
        .bind(&request.chain_id)
        .bind(request.range.from)
        .bind(request.range.to)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            RunnerError::database(format!("failed to read {window} inspection window"), error)
        })
}

const fn command_name(kind: InspectionKind) -> &'static str {
    match kind {
        InspectionKind::BlockCanonicality => "inspect block-canonicality",
        InspectionKind::StoredLineage => "inspect stored-lineage",
        InspectionKind::RawEvents => "inspect raw-events",
    }
}
