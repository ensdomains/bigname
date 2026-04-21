use anyhow::{Context, Result, bail};
use bigname_manifests::WatchedChainPlan;
use bigname_storage::{
    CanonicalityState, RawCodeHash, RawLog, RawReceipt, RawTransaction, upsert_raw_blocks,
    upsert_raw_code_hashes, upsert_raw_logs, upsert_raw_receipts, upsert_raw_transactions,
};
use tracing::info;

use crate::{
    provider::{JsonRpcProvider, ProviderBlockSelection},
    reconciliation::{
        ensure_provider_bundle_matches_raw_block, provider_block_to_raw_block,
        provider_code_observation_to_raw_code_hash, provider_log_to_raw_log,
        provider_receipt_to_raw_receipt, provider_transaction_to_raw_transaction,
        sync_adapter_state_from_persisted_raw_payloads,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BackfillBlockRange {
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
}

impl BackfillBlockRange {
    pub(crate) fn new(from_block: i64, to_block: i64) -> Result<Self> {
        if from_block < 0 {
            bail!("backfill from block cannot be negative: {from_block}");
        }
        if to_block < 0 {
            bail!("backfill to block cannot be negative: {to_block}");
        }
        if from_block > to_block {
            bail!("backfill range start {from_block} is after end {to_block}");
        }

        Ok(Self {
            from_block,
            to_block,
        })
    }

    fn block_count(self) -> Result<usize> {
        let span = self
            .to_block
            .checked_sub(self.from_block)
            .and_then(|span| span.checked_add(1))
            .context("backfill range block count overflowed i64")?;
        usize::try_from(span).context("backfill range block count does not fit in usize")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackfillOutcome {
    pub(crate) chain: String,
    pub(crate) from_block: i64,
    pub(crate) to_block: i64,
    pub(crate) resolved_block_count: usize,
    pub(crate) raw_block_count: usize,
    pub(crate) raw_transaction_count: usize,
    pub(crate) raw_receipt_count: usize,
    pub(crate) raw_log_count: usize,
    pub(crate) raw_code_hash_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedBackfillBlock {
    block_number: i64,
    block_hash: String,
}

pub(crate) async fn run_hash_pinned_backfill_range(
    pool: &sqlx::PgPool,
    watched_chain: &WatchedChainPlan,
    provider: &JsonRpcProvider,
    range: BackfillBlockRange,
) -> Result<BackfillOutcome> {
    let resolved_blocks = resolve_backfill_range(provider, range).await?;
    let block_hashes = resolved_blocks
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let mut raw_blocks = Vec::with_capacity(resolved_blocks.len());
    let mut transactions = Vec::<RawTransaction>::new();
    let mut receipts = Vec::<RawReceipt>::new();
    let mut logs = Vec::<RawLog>::new();
    let mut code_hashes = Vec::<RawCodeHash>::new();

    for resolved_block in &resolved_blocks {
        let bundle = provider
            .fetch_block_bundle_by_hash(&resolved_block.block_hash)
            .await
            .with_context(|| {
                format!(
                    "failed to fetch hash-pinned payload for chain {} block {} hash {}",
                    watched_chain.chain, resolved_block.block_number, resolved_block.block_hash
                )
            })?;
        if bundle.block.block_number != resolved_block.block_number {
            bail!(
                "provider resolved chain {} block number {} to hash {}, but hash-scoped fetch returned block number {}",
                watched_chain.chain,
                resolved_block.block_number,
                resolved_block.block_hash,
                bundle.block.block_number
            );
        }

        let raw_block = provider_block_to_raw_block(
            &watched_chain.chain,
            &bundle.block,
            CanonicalityState::Observed,
        );
        ensure_provider_bundle_matches_raw_block(&raw_block, &bundle)?;

        transactions.extend(
            bundle
                .transactions
                .iter()
                .map(|transaction| {
                    provider_transaction_to_raw_transaction(
                        &watched_chain.chain,
                        &raw_block,
                        transaction,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        );
        receipts.extend(
            bundle
                .receipts
                .iter()
                .map(|receipt| {
                    provider_receipt_to_raw_receipt(&watched_chain.chain, &raw_block, receipt)
                })
                .collect::<Result<Vec<_>>>()?,
        );
        logs.extend(
            bundle
                .logs
                .iter()
                .map(|log| provider_log_to_raw_log(&watched_chain.chain, &raw_block, log))
                .collect::<Result<Vec<_>>>()?,
        );

        if !watched_chain.addresses.is_empty() {
            let observations = provider
                .fetch_code_observations_at_block(
                    &watched_chain.addresses,
                    ProviderBlockSelection::Hash(raw_block.block_hash.clone()),
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to fetch hash-pinned code observations for chain {} block {} hash {}",
                        watched_chain.chain, raw_block.block_number, raw_block.block_hash
                    )
                })?;
            code_hashes.extend(
                observations
                    .iter()
                    .map(|observation| {
                        provider_code_observation_to_raw_code_hash(
                            &watched_chain.chain,
                            &raw_block,
                            observation,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        raw_blocks.push(raw_block);
    }

    upsert_raw_blocks(pool, &raw_blocks).await?;
    upsert_raw_transactions(pool, &transactions).await?;
    upsert_raw_receipts(pool, &receipts).await?;
    upsert_raw_logs(pool, &logs).await?;
    upsert_raw_code_hashes(pool, &code_hashes).await?;
    sync_adapter_state_from_persisted_raw_payloads(pool, &watched_chain.chain, &block_hashes)
        .await?;

    let outcome = BackfillOutcome {
        chain: watched_chain.chain.clone(),
        from_block: range.from_block,
        to_block: range.to_block,
        resolved_block_count: resolved_blocks.len(),
        raw_block_count: raw_blocks.len(),
        raw_transaction_count: transactions.len(),
        raw_receipt_count: receipts.len(),
        raw_log_count: logs.len(),
        raw_code_hash_count: code_hashes.len(),
    };
    info!(
        service = "indexer",
        command = "backfill",
        chain = %outcome.chain,
        from_block = outcome.from_block,
        to_block = outcome.to_block,
        resolved_block_count = outcome.resolved_block_count,
        raw_block_count = outcome.raw_block_count,
        raw_transaction_count = outcome.raw_transaction_count,
        raw_receipt_count = outcome.raw_receipt_count,
        raw_log_count = outcome.raw_log_count,
        raw_code_hash_count = outcome.raw_code_hash_count,
        "hash-pinned backfill range completed"
    );

    Ok(outcome)
}

async fn resolve_backfill_range(
    provider: &JsonRpcProvider,
    range: BackfillBlockRange,
) -> Result<Vec<ResolvedBackfillBlock>> {
    let mut resolved_blocks = Vec::with_capacity(range.block_count()?);
    for block_number in range.from_block..=range.to_block {
        let block_hash = provider
            .fetch_block_hash_by_number(block_number)
            .await
            .with_context(|| format!("failed to resolve backfill block number {block_number}"))?;
        resolved_blocks.push(ResolvedBackfillBlock {
            block_number,
            block_hash,
        });
    }

    Ok(resolved_blocks)
}
