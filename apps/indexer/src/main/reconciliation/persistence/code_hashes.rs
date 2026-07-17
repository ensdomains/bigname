use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{RawCodeHash, load_raw_blocks_by_hashes, upsert_raw_code_hashes};
use sqlx::Row;

use crate::{
    provider::{ChainProviderOps, ProviderBlockSelection, ProviderHeadSnapshot},
    runtime::IntakeChainTask,
};

use super::super::{
    payload::{
        provider_code_observation_to_raw_code_hash, raw_code_hash_candidate_hashes,
        raw_payload_candidate_hashes, selected_address_set,
    },
    types::{CanonicalReconciliation, CanonicalReconciliationStatus, HeadChangeSet},
};
use super::load_live_generic_resolver_topic0s;

pub(crate) async fn persist_reconciled_raw_code_hashes(
    pool: &sqlx::PgPool,
    task: &IntakeChainTask,
    provider: &(impl ChainProviderOps + ?Sized),
    heads: &ProviderHeadSnapshot,
    canonical: &CanonicalReconciliation,
    head_change_set: HeadChangeSet,
) -> Result<()> {
    if canonical.status == CanonicalReconciliationStatus::StoredLineagePromoted {
        return Ok(());
    }

    let refreshed_block_hashes = raw_payload_candidate_hashes(heads, canonical, head_change_set)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let candidate_hashes = raw_code_hash_candidate_hashes(heads, canonical, head_change_set);
    if candidate_hashes.is_empty() {
        return Ok(());
    }

    let raw_blocks = load_raw_blocks_by_hashes(pool, &task.chain, &candidate_hashes).await?;
    if raw_blocks.len() != candidate_hashes.len() {
        bail!(
            "stored raw block count {} does not match the raw code-hash fetch plan size {} for chain {}",
            raw_blocks.len(),
            candidate_hashes.len(),
            task.chain
        );
    }

    let watched_addresses = selected_address_set(&task.addresses)
        .into_iter()
        .collect::<Vec<_>>();
    let generic_resolver_topic0s = load_live_generic_resolver_topic0s(pool, &task.chain)
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    let emitter_addresses_by_block_hash = load_raw_log_emitter_addresses_by_block_hashes(
        pool,
        &task.chain,
        &candidate_hashes,
        &watched_addresses,
        &generic_resolver_topic0s,
    )
    .await?;
    let code_observation_addresses = watched_addresses
        .iter()
        .cloned()
        .chain(
            emitter_addresses_by_block_hash
                .values()
                .flat_map(|addresses| addresses.iter().cloned()),
        )
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if code_observation_addresses.is_empty() {
        return Ok(());
    }
    let stored_code_addresses_by_block_hash = load_raw_code_addresses_by_block_hashes(
        pool,
        &task.chain,
        &candidate_hashes,
        &code_observation_addresses,
    )
    .await?;
    let baseline_addresses =
        load_missing_raw_code_baseline_addresses(pool, &task.chain, &watched_addresses).await?;
    let canonical_baseline_hash = canonical
        .canonical
        .as_ref()
        .map(|canonical| canonical.block_hash.as_str());
    let raw_blocks = raw_blocks
        .into_iter()
        .filter_map(|raw_block| {
            let mut addresses = BTreeSet::<String>::new();
            let stored_code_addresses = stored_code_addresses_by_block_hash
                .get(&raw_block.block_hash)
                .cloned()
                .unwrap_or_default();
            let block_refreshed = refreshed_block_hashes.contains(&raw_block.block_hash);
            if let Some(emitter_addresses) =
                emitter_addresses_by_block_hash.get(&raw_block.block_hash)
            {
                addresses.extend(emitter_addresses.iter().filter_map(|address| {
                    (block_refreshed || !stored_code_addresses.contains(address))
                        .then_some(address.clone())
                }));
            }
            if canonical_baseline_hash == Some(raw_block.block_hash.as_str()) {
                addresses.extend(baseline_addresses.iter().cloned());
            }
            if addresses.is_empty() {
                return None;
            }
            Some((raw_block, addresses.into_iter().collect::<Vec<_>>()))
        })
        .collect::<Vec<_>>();
    if raw_blocks.is_empty() {
        return Ok(());
    }

    let mut code_hashes = Vec::<RawCodeHash>::new();
    for (raw_block, addresses) in &raw_blocks {
        let observations = provider
            .fetch_code_observations_at_block(
                addresses,
                ProviderBlockSelection::Hash(raw_block.block_hash.clone()),
            )
            .await?;
        code_hashes.extend(
            observations
                .iter()
                .map(|observation| {
                    provider_code_observation_to_raw_code_hash(&task.chain, raw_block, observation)
                })
                .collect::<Result<Vec<_>>>()?,
        );
    }

    upsert_raw_code_hashes(pool, &code_hashes).await?;
    Ok(())
}

async fn load_raw_log_emitter_addresses_by_block_hashes(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    watched_addresses: &[String],
    generic_resolver_topic0s: &[String],
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    if block_hashes.is_empty()
        || (watched_addresses.is_empty() && generic_resolver_topic0s.is_empty())
    {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            block_hash,
            LOWER(emitting_address) AS emitting_address
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND (
              LOWER(emitting_address) = ANY($3::TEXT[])
              OR LOWER(topics[1]) = ANY($4::TEXT[])
          )
        GROUP BY block_hash, LOWER(emitting_address)
        ORDER BY block_hash, LOWER(emitting_address)
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(watched_addresses)
    .bind(generic_resolver_topic0s)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load raw-log code observation emitters for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    let mut addresses_by_block_hash = BTreeMap::<String, BTreeSet<String>>::new();
    for row in rows {
        let block_hash = row
            .try_get::<String, _>("block_hash")
            .context("missing block_hash from raw-log emitter row")?;
        let emitting_address = row
            .try_get::<String, _>("emitting_address")
            .context("missing emitting_address from raw-log emitter row")?;
        addresses_by_block_hash
            .entry(block_hash)
            .or_default()
            .insert(emitting_address);
    }

    Ok(addresses_by_block_hash)
}

async fn load_raw_code_addresses_by_block_hashes(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    code_observation_addresses: &[String],
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    if block_hashes.is_empty() || code_observation_addresses.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            block_hash,
            LOWER(contract_address) AS contract_address
        FROM raw_code_hashes
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND LOWER(contract_address) = ANY($3::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        GROUP BY block_hash, LOWER(contract_address)
        ORDER BY block_hash, LOWER(contract_address)
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .bind(code_observation_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load raw code-hash addresses for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    let mut addresses_by_block_hash = BTreeMap::<String, BTreeSet<String>>::new();
    for row in rows {
        let block_hash = row
            .try_get::<String, _>("block_hash")
            .context("missing block_hash from raw code-hash row")?;
        let contract_address = row
            .try_get::<String, _>("contract_address")
            .context("missing contract_address from raw code-hash row")?;
        addresses_by_block_hash
            .entry(block_hash)
            .or_default()
            .insert(contract_address);
    }

    Ok(addresses_by_block_hash)
}

async fn load_missing_raw_code_baseline_addresses(
    pool: &sqlx::PgPool,
    chain: &str,
    watched_addresses: &[String],
) -> Result<BTreeSet<String>> {
    if watched_addresses.is_empty() {
        return Ok(BTreeSet::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT watched.contract_address
        FROM UNNEST($2::TEXT[]) AS watched(contract_address)
        WHERE NOT EXISTS (
            SELECT 1
            FROM raw_code_hashes
            WHERE chain_id = $1
              AND LOWER(raw_code_hashes.contract_address) = watched.contract_address
              AND canonicality_state <> 'orphaned'::canonicality_state
        )
        ORDER BY watched.contract_address
        "#,
    )
    .bind(chain)
    .bind(watched_addresses)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!("failed to load missing raw code-hash baseline addresses for chain {chain}")
    })?;

    rows.into_iter()
        .map(|row| {
            row.try_get::<String, _>("contract_address")
                .context("missing contract_address from baseline address row")
        })
        .collect()
}
