use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bigname_storage::serialize_jsonb_value;
use serde_json::Value;
use sqlx::Row;
use tracing::info;

use crate::{
    ens_v1_resolver::SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
    provider::{ChainProviderOps, ProviderLog},
    reconciliation::{keccak256_hex, parse_hex_bytes},
};

pub(crate) const DEFAULT_ENS_V1_TEXT_RECORD_REPAIR_CHUNK_BLOCKS: i64 = 5_000;
pub(crate) const DEFAULT_ENS_V1_TEXT_RECORD_REPAIR_PAGE_SIZE: i64 = 10_000;

const DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY: &str = "ens_v1_unwrapped_authority";
const EVENT_KIND_RECORD_CHANGED: &str = "RecordChanged";
const TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE: &str = "TextChanged(bytes32,string,string)";
const TEXT_CHANGED_WITH_VALUE_SIGNATURE: &str = "TextChanged(bytes32,string,string,string)";
const TEXT_RECORD_FAMILY: &str = "text";
const LEGACY_TEXT_RECORD_KEY: &str = "text";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EnsV1TextRecordRepairConfig {
    pub(crate) chain: String,
    pub(crate) from_block: Option<i64>,
    pub(crate) to_block: Option<i64>,
    pub(crate) chunk_blocks: i64,
    pub(crate) candidate_page_size: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EnsV1TextRecordRepairOutcome {
    pub(crate) chain: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) candidate_count: usize,
    pub(crate) fetched_log_count: usize,
    pub(crate) matched_log_count: usize,
    pub(crate) repaired_event_count: usize,
    pub(crate) missing_log_count: usize,
    pub(crate) skipped_decode_count: usize,
}

#[derive(Clone, Debug)]
struct TextRecordRepairCandidate {
    normalized_event_id: i64,
    block_hash: String,
    block_number: i64,
    transaction_hash: String,
    log_index: i64,
    after_state: Value,
}

#[derive(Clone, Debug)]
struct TextRecordRepairUpdate {
    normalized_event_id: i64,
    after_state: Value,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LogIdentity {
    block_hash: String,
    transaction_hash: String,
    log_index: i64,
}

pub(crate) async fn repair_ens_v1_text_records_from_provider(
    pool: &sqlx::PgPool,
    provider: &(impl ChainProviderOps + ?Sized),
    config: EnsV1TextRecordRepairConfig,
) -> Result<EnsV1TextRecordRepairOutcome> {
    validate_repair_config(&config)?;
    let Some((from_block, to_block)) = resolve_repair_block_range(pool, &config).await? else {
        return Ok(EnsV1TextRecordRepairOutcome {
            chain: config.chain,
            from_block: 0,
            to_block: 0,
            candidate_count: 0,
            fetched_log_count: 0,
            matched_log_count: 0,
            repaired_event_count: 0,
            missing_log_count: 0,
            skipped_decode_count: 0,
        });
    };

    let mut outcome = EnsV1TextRecordRepairOutcome {
        chain: config.chain.clone(),
        from_block,
        to_block,
        candidate_count: 0,
        fetched_log_count: 0,
        matched_log_count: 0,
        repaired_event_count: 0,
        missing_log_count: 0,
        skipped_decode_count: 0,
    };

    let mut chunk_from = from_block;
    while chunk_from <= to_block {
        let chunk_to = chunk_from
            .checked_add(config.chunk_blocks - 1)
            .map(|value| value.min(to_block))
            .context("ENSv1 text record repair chunk bound overflowed")?;
        let mut excluded_candidate_ids = Vec::new();

        loop {
            let candidates = load_text_record_repair_candidates(
                pool,
                &config.chain,
                chunk_from,
                chunk_to,
                config.candidate_page_size,
                &excluded_candidate_ids,
            )
            .await?;
            if candidates.is_empty() {
                break;
            }

            let logs = fetch_text_changed_logs_for_candidates(provider, &candidates).await?;
            outcome.fetched_log_count += logs.len();
            let logs_by_identity = logs
                .iter()
                .map(|log| (log_identity(log), log))
                .collect::<BTreeMap<_, _>>();
            let mut updates = Vec::new();

            for candidate in &candidates {
                outcome.candidate_count += 1;
                let identity = candidate.log_identity();
                let Some(log) = logs_by_identity.get(&identity) else {
                    outcome.missing_log_count += 1;
                    excluded_candidate_ids.push(candidate.normalized_event_id);
                    continue;
                };
                outcome.matched_log_count += 1;

                let data = parse_hex_bytes(&log.data).with_context(|| {
                    format!(
                        "failed to decode provider log data for normalized_event_id {}",
                        candidate.normalized_event_id
                    )
                })?;
                let Some(text_record) =
                    bigname_adapters::decode_ens_v1_text_record_change(&log.topics, &data)?
                else {
                    outcome.skipped_decode_count += 1;
                    excluded_candidate_ids.push(candidate.normalized_event_id);
                    continue;
                };
                if candidate.is_selectorized_missing_value() && text_record.value.is_none() {
                    excluded_candidate_ids.push(candidate.normalized_event_id);
                    continue;
                }
                if text_record.value.is_none() {
                    excluded_candidate_ids.push(candidate.normalized_event_id);
                }
                updates.push(TextRecordRepairUpdate {
                    normalized_event_id: candidate.normalized_event_id,
                    after_state: repaired_after_state(&candidate.after_state, &text_record)?,
                });
            }

            let repaired_count = update_text_record_after_states(pool, &updates).await?;
            outcome.repaired_event_count += repaired_count;
            info!(
                service = "indexer",
                command = "repair ens-v1-text-records",
                chain = %config.chain,
                from_block = chunk_from,
                to_block = chunk_to,
                candidate_count = candidates.len(),
                fetched_log_count = logs.len(),
                repaired_event_count = repaired_count,
                "ENSv1 text record repair chunk page completed"
            );

            if candidates.len()
                < usize::try_from(config.candidate_page_size)
                    .context("candidate_page_size overflowed usize")?
            {
                break;
            }
        }

        chunk_from = chunk_to
            .checked_add(1)
            .context("ENSv1 text record repair chunk advance overflowed")?;
    }

    Ok(outcome)
}

fn validate_repair_config(config: &EnsV1TextRecordRepairConfig) -> Result<()> {
    if config.chain.trim().is_empty() {
        bail!("ENSv1 text record repair chain must not be empty");
    }
    if config.chunk_blocks <= 0 {
        bail!(
            "ENSv1 text record repair chunk_blocks must be positive, got {}",
            config.chunk_blocks
        );
    }
    if config.candidate_page_size <= 0 {
        bail!(
            "ENSv1 text record repair candidate_page_size must be positive, got {}",
            config.candidate_page_size
        );
    }
    if let (Some(from_block), Some(to_block)) = (config.from_block, config.to_block) {
        if from_block < 0 || to_block < 0 {
            bail!("ENSv1 text record repair block range must be non-negative");
        }
        if from_block > to_block {
            bail!("ENSv1 text record repair from_block {from_block} is after to_block {to_block}");
        }
    }
    Ok(())
}

async fn resolve_repair_block_range(
    pool: &sqlx::PgPool,
    config: &EnsV1TextRecordRepairConfig,
) -> Result<Option<(i64, i64)>> {
    if let (Some(from_block), Some(to_block)) = (config.from_block, config.to_block) {
        return Ok(Some((from_block, to_block)));
    }

    let row = sqlx::query(
        r#"
        SELECT
            (
                SELECT block_number
                FROM normalized_events
                WHERE chain_id = $1
                  AND block_number IS NOT NULL
                  AND block_hash IS NOT NULL
                  AND transaction_hash IS NOT NULL
                  AND log_index IS NOT NULL
                  AND derivation_kind = $2
                  AND event_kind = $3
                  AND source_family = $4
                  AND after_state->>'record_family' = 'text'
                  AND (
                      (
                          after_state->>'record_key' = 'text'
                          AND after_state->'selector_key' = 'null'::jsonb
                      )
                      OR (
                          after_state->>'record_key' LIKE 'text:%'
                          AND NOT (after_state ? 'value')
                      )
                  )
                  AND canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                ORDER BY block_number ASC
                LIMIT 1
            ) AS from_block,
            (
                SELECT block_number
                FROM normalized_events
                WHERE chain_id = $1
                  AND block_number IS NOT NULL
                  AND block_hash IS NOT NULL
                  AND transaction_hash IS NOT NULL
                  AND log_index IS NOT NULL
                  AND derivation_kind = $2
                  AND event_kind = $3
                  AND source_family = $4
                  AND after_state->>'record_family' = 'text'
                  AND (
                      (
                          after_state->>'record_key' = 'text'
                          AND after_state->'selector_key' = 'null'::jsonb
                      )
                      OR (
                          after_state->>'record_key' LIKE 'text:%'
                          AND NOT (after_state ? 'value')
                      )
                  )
                  AND canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                ORDER BY block_number DESC
                LIMIT 1
            ) AS to_block
        "#,
    )
    .bind(&config.chain)
    .bind(DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY)
    .bind(EVENT_KIND_RECORD_CHANGED)
    .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to find ENSv1 text record repair range for chain {}",
            config.chain
        )
    })?;

    let from_block = row
        .try_get::<Option<i64>, _>("from_block")
        .context("missing repair from_block")?;
    let to_block = row
        .try_get::<Option<i64>, _>("to_block")
        .context("missing repair to_block")?;
    Ok(from_block.zip(to_block))
}

async fn load_text_record_repair_candidates(
    pool: &sqlx::PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    limit: i64,
    excluded_candidate_ids: &[i64],
) -> Result<Vec<TextRecordRepairCandidate>> {
    let rows = sqlx::query(
        r#"
        SELECT
            normalized_event_id,
            block_hash,
            block_number,
            transaction_hash,
            log_index,
            after_state
        FROM normalized_events
        WHERE chain_id = $1
          AND block_number >= $2
          AND block_number <= $3
          AND derivation_kind = $4
          AND event_kind = $5
          AND source_family = $6
          AND block_hash IS NOT NULL
          AND transaction_hash IS NOT NULL
          AND log_index IS NOT NULL
          AND after_state->>'record_family' = 'text'
          AND (
              (
                  after_state->>'record_key' = 'text'
                  AND after_state->'selector_key' = 'null'::jsonb
              )
              OR (
                  after_state->>'record_key' LIKE 'text:%'
                  AND NOT (after_state ? 'value')
              )
          )
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND NOT (normalized_event_id = ANY($8::BIGINT[]))
        ORDER BY block_number, transaction_hash, log_index, normalized_event_id
        LIMIT $7
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY)
    .bind(EVENT_KIND_RECORD_CHANGED)
    .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    .bind(limit)
    .bind(excluded_candidate_ids)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load ENSv1 text record repair candidates for chain {chain} range {from_block}..={to_block}"
        )
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(TextRecordRepairCandidate {
                normalized_event_id: row
                    .try_get("normalized_event_id")
                    .context("missing normalized_event_id")?,
                block_hash: row
                    .try_get::<String, _>("block_hash")
                    .context("missing block_hash")?
                    .to_ascii_lowercase(),
                block_number: row
                    .try_get("block_number")
                    .context("missing block_number")?,
                transaction_hash: row
                    .try_get::<String, _>("transaction_hash")
                    .context("missing transaction_hash")?
                    .to_ascii_lowercase(),
                log_index: row.try_get("log_index").context("missing log_index")?,
                after_state: row.try_get("after_state").context("missing after_state")?,
            })
        })
        .collect()
}

async fn fetch_text_changed_logs_for_candidates(
    provider: &(impl ChainProviderOps + ?Sized),
    candidates: &[TextRecordRepairCandidate],
) -> Result<Vec<ProviderLog>> {
    let Some(from_block) = candidates
        .iter()
        .map(|candidate| candidate.block_number)
        .min()
    else {
        return Ok(Vec::new());
    };
    let to_block = candidates
        .iter()
        .map(|candidate| candidate.block_number)
        .max()
        .expect("non-empty candidates must have max block");
    let block_numbers = (from_block..=to_block).collect::<Vec<_>>();
    let resolved_blocks = provider
        .fetch_block_hashes_by_numbers(&block_numbers)
        .await?;
    let topic0s = text_changed_topic0s();
    let logs_by_block = provider
        .fetch_logs_by_block_range_for_topic0s_and_addresses(&resolved_blocks, &topic0s, &[])
        .await
        .with_context(|| {
            format!(
                "failed to fetch ENSv1 TextChanged logs for repair range {from_block}..={to_block}"
            )
        })?;

    Ok(logs_by_block
        .into_values()
        .flatten()
        .filter(|log| {
            candidates
                .iter()
                .any(|candidate| candidate.log_identity() == log_identity(log))
        })
        .collect())
}

fn text_changed_topic0s() -> Vec<String> {
    [
        TEXT_CHANGED_WITHOUT_VALUE_SIGNATURE,
        TEXT_CHANGED_WITH_VALUE_SIGNATURE,
    ]
    .into_iter()
    .map(|signature| keccak256_hex(signature.as_bytes()))
    .collect()
}

fn repaired_after_state(
    previous: &Value,
    text_record: &bigname_adapters::EnsV1TextRecordChange,
) -> Result<Value> {
    let mut object = previous
        .as_object()
        .cloned()
        .context("ENSv1 text record repair after_state must be a JSON object")?;
    object.insert(
        "record_key".to_owned(),
        Value::String(text_record.record_key.clone()),
    );
    object.insert(
        "record_family".to_owned(),
        Value::String(text_record.record_family.clone()),
    );
    object.insert(
        "selector_key".to_owned(),
        Value::String(text_record.selector_key.clone()),
    );
    match text_record.value.as_ref() {
        Some(value) => {
            object.insert("value".to_owned(), Value::String(value.clone()));
        }
        None => {
            object.remove("value");
        }
    }
    Ok(Value::Object(object))
}

async fn update_text_record_after_states(
    pool: &sqlx::PgPool,
    updates: &[TextRecordRepairUpdate],
) -> Result<usize> {
    if updates.is_empty() {
        return Ok(0);
    }

    let normalized_event_ids = updates
        .iter()
        .map(|update| update.normalized_event_id)
        .collect::<Vec<_>>();
    let after_states = updates
        .iter()
        .map(|update| {
            serialize_jsonb_value(
                &update.after_state,
                "failed to serialize ENSv1 text record repair after_state payload",
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let affected = sqlx::query(
        r#"
        UPDATE normalized_events AS events
        SET after_state = input.after_state::jsonb,
            observed_at = now()
        FROM unnest($1::BIGINT[], $2::TEXT[]) AS input(
            normalized_event_id,
            after_state
        )
        WHERE events.normalized_event_id = input.normalized_event_id
          AND events.derivation_kind = $3
          AND events.event_kind = $4
          AND events.source_family = $5
          AND events.after_state->>'record_family' = 'text'
          AND (
              (
                  events.after_state->>'record_key' = 'text'
                  AND events.after_state->'selector_key' = 'null'::jsonb
              )
              OR (
                  events.after_state->>'record_key' LIKE 'text:%'
                  AND NOT (events.after_state ? 'value')
              )
          )
        "#,
    )
    .bind(&normalized_event_ids)
    .bind(&after_states)
    .bind(DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY)
    .bind(EVENT_KIND_RECORD_CHANGED)
    .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    .execute(pool)
    .await
    .context("failed to update repaired ENSv1 text record normalized events")?
    .rows_affected();

    usize::try_from(affected).context("ENSv1 text record repair update count overflowed usize")
}

impl TextRecordRepairCandidate {
    fn log_identity(&self) -> LogIdentity {
        LogIdentity {
            block_hash: self.block_hash.clone(),
            transaction_hash: self.transaction_hash.clone(),
            log_index: self.log_index,
        }
    }

    fn is_selectorized_missing_value(&self) -> bool {
        self.after_state
            .get("record_family")
            .and_then(Value::as_str)
            == Some(TEXT_RECORD_FAMILY)
            && self
                .after_state
                .get("record_key")
                .and_then(Value::as_str)
                .is_some_and(|record_key| record_key != LEGACY_TEXT_RECORD_KEY)
            && !self
                .after_state
                .as_object()
                .is_some_and(|object| object.contains_key("value"))
    }
}

fn log_identity(log: &ProviderLog) -> LogIdentity {
    LogIdentity {
        block_hash: log.block_hash.to_ascii_lowercase(),
        transaction_hash: log.transaction_hash.to_ascii_lowercase(),
        log_index: log.log_index,
    }
}
