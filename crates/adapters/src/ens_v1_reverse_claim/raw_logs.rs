use bigname_storage::sql_row;
use std::collections::HashMap;

use anyhow::{Context, Result};
use bigname_storage::CanonicalityState;
use sqlx::PgPool;

use super::active_emitters::ActiveEmitter;
use crate::checkpoint_context::{StartupAdapterProgress, record_startup_adapter_progress};

const RAW_LOG_PROGRESS_ROWS: usize = 1_000;

#[derive(Clone, Debug)]
pub(super) struct ReverseRawLogRow {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) emitting_contract_instance_id: sqlx::types::Uuid,
    pub(super) topics: Vec<String>,
    pub(super) data: Vec<u8>,
    pub(super) canonicality_state: CanonicalityState,
    pub(super) source_manifest_id: i64,
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) manifest_version: i64,
}

pub(super) async fn load_reverse_raw_logs(
    pool: &PgPool,
    chain: &str,
    active_emitters: &[ActiveEmitter],
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<Vec<ReverseRawLogRow>> {
    let emitters_by_address = active_emitters
        .iter()
        .cloned()
        .map(|emitter| (emitter.address.clone(), emitter))
        .collect::<HashMap<_, _>>();
    let mut watched_addresses = emitters_by_address.keys().cloned().collect::<Vec<_>>();
    watched_addresses.sort();
    let (scope_addresses, scope_from_blocks, scope_to_blocks) =
        reverse_source_scope_bindings(source_scope);
    if source_scope.is_some() && scope_addresses.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    for watched_address in watched_addresses {
        let emitter = emitters_by_address.get(&watched_address).with_context(|| {
            format!("missing active emitter metadata for chain {chain} address {watched_address}")
        })?;
        let mut cursor = None::<(i64, i64, i64, String)>;
        loop {
            let page = sqlx::query(
                r#"
                SELECT
                    rl.chain_id AS chain_id,
                    rl.block_hash AS block_hash,
                    rl.block_number AS block_number,
                    rl.transaction_hash AS transaction_hash,
                    rl.transaction_index AS transaction_index,
                    rl.log_index AS log_index,
                    rl.emitting_address AS emitting_address,
                    rl.topics AS topics,
                    rl.data AS data,
                    rl.canonicality_state::TEXT AS canonicality_state
                FROM raw_logs rl
                WHERE rl.chain_id = $1
                  AND LOWER(rl.emitting_address) = $2
                  AND ($3::BOOLEAN = FALSE OR rl.block_hash = ANY($4::TEXT[]))
                  AND (
                      $5::BOOLEAN = FALSE
                      OR EXISTS (
                          SELECT 1
                          FROM unnest($6::TEXT[], $7::BIGINT[], $8::BIGINT[])
                            AS source_scope(address, from_block, to_block)
                          WHERE LOWER(rl.emitting_address) = source_scope.address
                            AND rl.block_number >= source_scope.from_block
                            AND rl.block_number <= source_scope.to_block
                      )
                  )
                  AND rl.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND (
                      $9::BIGINT IS NULL
                      OR (
                          rl.block_number,
                          rl.transaction_index,
                          rl.log_index,
                          rl.block_hash COLLATE "C"
                      ) > ($9, $10, $11, $12 COLLATE "C")
                  )
                ORDER BY
                    rl.block_number,
                    rl.transaction_index,
                    rl.log_index,
                    rl.block_hash COLLATE "C"
                LIMIT $13
                "#,
            )
            .bind(chain)
            .bind(&watched_address)
            .bind(restrict_to_block_hashes)
            .bind(block_hashes)
            .bind(source_scope.is_some())
            .bind(&scope_addresses)
            .bind(&scope_from_blocks)
            .bind(&scope_to_blocks)
            .bind(cursor.as_ref().map(|cursor| cursor.0))
            .bind(cursor.as_ref().map(|cursor| cursor.1))
            .bind(cursor.as_ref().map(|cursor| cursor.2))
            .bind(cursor.as_ref().map(|cursor| cursor.3.as_str()))
            .bind(i64::try_from(RAW_LOG_PROGRESS_ROWS)?)
            .fetch_all(pool)
            .await
            .with_context(|| format!("failed to load ENSv1 reverse raw logs for chain {chain}"))?;
            if page.is_empty() {
                break;
            }

            for row in page {
                let block_hash = sql_row::get::<String>(&row, "block_hash")?;
                let block_number = sql_row::get::<i64>(&row, "block_number")?;
                let transaction_index = sql_row::get::<i64>(&row, "transaction_index")?;
                let log_index = sql_row::get::<i64>(&row, "log_index")?;
                cursor = Some((
                    block_number,
                    transaction_index,
                    log_index,
                    block_hash.clone(),
                ));
                rows.push(ReverseRawLogRow {
                    chain_id: sql_row::get(&row, "chain_id")?,
                    block_hash,
                    block_number,
                    transaction_hash: sql_row::get(&row, "transaction_hash")?,
                    transaction_index,
                    log_index,
                    emitting_address: watched_address.clone(),
                    emitting_contract_instance_id: emitter.contract_instance_id,
                    topics: sql_row::get(&row, "topics")?,
                    data: sql_row::get(&row, "data")?,
                    canonicality_state: sql_row::get(&row, "canonicality_state")?,
                    source_manifest_id: emitter.source_manifest_id,
                    namespace: emitter.namespace.clone(),
                    source_family: emitter.source_family.clone(),
                    manifest_version: emitter.manifest_version,
                });
            }
            record_startup_adapter_progress(pool, progress).await?;
        }
    }
    Ok(rows)
}

fn reverse_source_scope_bindings(
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> (Vec<String>, Vec<i64>, Vec<i64>) {
    let mut addresses = Vec::new();
    let mut from_blocks = Vec::new();
    let mut to_blocks = Vec::new();
    for (source_family, address, from_block, to_block) in source_scope.unwrap_or(&[]) {
        if source_family != "ens_v1_reverse_l1" && source_family != "basenames_base_primary" {
            continue;
        }
        addresses.push(address.to_ascii_lowercase());
        from_blocks.push(*from_block);
        to_blocks.push(*to_block);
    }
    (addresses, from_blocks, to_blocks)
}
