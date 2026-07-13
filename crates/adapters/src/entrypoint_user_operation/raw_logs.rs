use anyhow::{Context, Result};
use bigname_storage::{CanonicalityState, sql_row};
use sqlx::PgPool;

use crate::ens_v2_common::source_scope_bindings;

use super::SOURCE_FAMILY_ENS_GAS_SPONSORSHIP_L1;
use super::manifest_scope::GasSponsorshipManifestScope;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GasSponsorshipRawLogRow {
    pub(super) chain_id: String,
    pub(super) block_hash: String,
    pub(super) block_number: i64,
    pub(super) transaction_hash: String,
    pub(super) transaction_index: i64,
    pub(super) log_index: i64,
    pub(super) emitting_address: String,
    pub(super) topics: Vec<String>,
    pub(super) data: Vec<u8>,
    pub(super) canonicality_state: CanonicalityState,
}

pub(super) struct RawLogScope<'scope> {
    pub(super) restrict_to_block_hashes: bool,
    pub(super) block_hashes: &'scope [String],
    pub(super) source_scope: Option<&'scope [(String, String, i64, i64)]>,
    pub(super) max_block_number: Option<i64>,
}

/// Sponsored `UserOperationEvent` logs from the declared EntryPoint
/// addresses, prefiltered to the declared sponsoring paymaster via topic 3 so
/// unrelated account-abstraction traffic never leaves the database.
pub(super) async fn load_sponsored_user_operation_raw_logs(
    pool: &PgPool,
    chain: &str,
    manifest_scope: &GasSponsorshipManifestScope,
    scope: &RawLogScope<'_>,
) -> Result<Vec<GasSponsorshipRawLogRow>> {
    let topic0 = manifest_scope
        .event_topics
        .topic0(super::ABI_EVENT_USER_OPERATION_EVENT_SIGNATURE)?
        .to_owned();
    let paymaster_topics = manifest_scope
        .sponsoring_paymaster_addresses
        .iter()
        .map(|address| paymaster_topic_word(address))
        .collect::<Result<Vec<_>>>()?;
    if manifest_scope.entrypoint_addresses.is_empty() || paymaster_topics.is_empty() {
        return Ok(Vec::new());
    }
    load_topic0_raw_logs(
        pool,
        chain,
        &manifest_scope.entrypoint_addresses,
        &topic0,
        Some(&paymaster_topics),
        scope,
    )
    .await
    .context("failed to load sponsored UserOperationEvent raw logs")
}

/// `AnswerUpdated` logs from the declared price-feed aggregator addresses.
pub(super) async fn load_price_feed_raw_logs(
    pool: &PgPool,
    chain: &str,
    manifest_scope: &GasSponsorshipManifestScope,
    scope: &RawLogScope<'_>,
) -> Result<Vec<GasSponsorshipRawLogRow>> {
    let topic0 = manifest_scope
        .event_topics
        .topic0(super::ABI_EVENT_ANSWER_UPDATED_SIGNATURE)?
        .to_owned();
    if manifest_scope.eth_usd_feed_addresses.is_empty() {
        return Ok(Vec::new());
    }
    load_topic0_raw_logs(
        pool,
        chain,
        &manifest_scope.eth_usd_feed_addresses,
        &topic0,
        None,
        scope,
    )
    .await
    .context("failed to load price-feed AnswerUpdated raw logs")
}

async fn load_topic0_raw_logs(
    pool: &PgPool,
    chain: &str,
    emitting_addresses: &[String],
    topic0: &str,
    topic3_filter: Option<&[String]>,
    scope: &RawLogScope<'_>,
) -> Result<Vec<GasSponsorshipRawLogRow>> {
    let (scope_addresses, scope_from_blocks, scope_to_blocks) =
        source_scope_bindings(scope.source_scope, SOURCE_FAMILY_ENS_GAS_SPONSORSHIP_L1);
    if scope.source_scope.is_some() && scope_addresses.is_empty() {
        return Ok(Vec::new());
    }
    let has_max_block_number = scope.max_block_number.is_some();
    let max_block_number = scope.max_block_number.unwrap_or(i64::MAX);
    let has_topic3_filter = topic3_filter.is_some();
    let topic3_values = topic3_filter.unwrap_or(&[]).to_vec();

    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state::TEXT AS canonicality_state
        FROM raw_logs
        WHERE chain_id = $1
          AND LOWER(emitting_address) = ANY($2::TEXT[])
          AND LOWER(topics[1]) = $3
          AND ($4::BOOLEAN = FALSE OR LOWER(topics[4]) = ANY($5::TEXT[]))
          AND ($6::BOOLEAN = FALSE OR block_hash = ANY($7::TEXT[]))
          AND ($11::BOOLEAN = FALSE OR block_number <= $12::BIGINT)
          AND (
              $8::BOOLEAN = FALSE
              OR EXISTS (
                  SELECT 1
                  FROM unnest($9::TEXT[], $10::BIGINT[], $13::BIGINT[])
                    AS source_scope(address, from_block, to_block)
                  WHERE LOWER(emitting_address) = source_scope.address
                    AND block_number >= source_scope.from_block
                    AND block_number <= source_scope.to_block
              )
          )
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        ORDER BY block_number, transaction_index, log_index
        "#,
    )
    .bind(chain)
    .bind(emitting_addresses)
    .bind(topic0.to_ascii_lowercase())
    .bind(has_topic3_filter)
    .bind(&topic3_values)
    .bind(scope.restrict_to_block_hashes)
    .bind(scope.block_hashes)
    .bind(scope.source_scope.is_some())
    .bind(&scope_addresses)
    .bind(&scope_from_blocks)
    .bind(has_max_block_number)
    .bind(max_block_number)
    .bind(&scope_to_blocks)
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(GasSponsorshipRawLogRow {
                chain_id: sql_row::get(&row, "chain_id")?,
                block_hash: sql_row::get(&row, "block_hash")?,
                block_number: sql_row::get(&row, "block_number")?,
                transaction_hash: sql_row::get(&row, "transaction_hash")?,
                transaction_index: sql_row::get(&row, "transaction_index")?,
                log_index: sql_row::get(&row, "log_index")?,
                emitting_address: sql_row::get(&row, "emitting_address")?,
                topics: sql_row::get(&row, "topics")?,
                data: sql_row::get(&row, "data")?,
                canonicality_state: sql_row::get(&row, "canonicality_state")?,
            })
        })
        .collect()
}

/// A paymaster address as its 32-byte indexed-topic word.
fn paymaster_topic_word(address: &str) -> Result<String> {
    let bare = address
        .strip_prefix("0x")
        .unwrap_or(address)
        .to_ascii_lowercase();
    if bare.len() != 40 || !bare.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("sponsoring paymaster address must be 20-byte hex, got {address}");
    }
    Ok(format!("0x{}{bare}", "0".repeat(24)))
}
