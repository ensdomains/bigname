//! `ens_gas_sponsorship_l1` adapter: ERC-4337 EntryPoint sponsored-operation
//! observations with calldata-derived name attribution, and ETH/USD
//! price-feed observations. Consumes stored raw logs plus retained
//! `raw_transaction_inputs`; performs no chain fetches.

use std::collections::HashMap;

use anyhow::Result;
use bigname_storage::load_raw_transaction_inputs;
use sqlx::PgPool;

use crate::normalized_event_support::upsert_normalized_events_with_counts;

mod account_execution;
mod calldata;
mod decoding;
mod event_building;
mod manifest_scope;
mod persistence_summary;
mod raw_logs;
mod surface_lookup;
mod write_classifier;

#[cfg(test)]
mod tests;

use event_building::{
    build_price_feed_event, build_sponsored_name_write_event, build_sponsored_user_operation_event,
    resolve_sponsored_operation,
};
use manifest_scope::load_gas_sponsorship_manifest_scope;
use persistence_summary::empty_summary;
use raw_logs::{RawLogScope, load_price_feed_raw_logs, load_sponsored_user_operation_raw_logs};
use surface_lookup::load_logical_name_ids_by_namehash;

pub use persistence_summary::{
    EntrypointUserOperationKindSyncSummary, EntrypointUserOperationSyncSummary,
};

pub(super) const SOURCE_FAMILY_ENS_GAS_SPONSORSHIP_L1: &str = "ens_gas_sponsorship_l1";
pub(super) const DERIVATION_KIND_ENTRYPOINT_USER_OPERATION: &str = "entrypoint_user_operation";

pub(super) const EVENT_KIND_SPONSORED_USER_OPERATION_OBSERVED: &str =
    "SponsoredUserOperationObserved";
pub(super) const EVENT_KIND_SPONSORED_NAME_WRITE_OBSERVED: &str = "SponsoredNameWriteObserved";
pub(super) const EVENT_KIND_PRICE_FEED_ANSWER_UPDATED: &str = "PriceFeedAnswerUpdated";

// (upstream: .refs/erc4337/contracts/interfaces/IEntryPoint.sol:L29 @ erc4337@7af70c8)
pub(super) const ABI_EVENT_USER_OPERATION_EVENT_SIGNATURE: &str =
    "UserOperationEvent(bytes32,address,address,uint256,bool,uint256,uint256)";
// (upstream: .refs/chainlink/contracts/src/v0.8/shared/interfaces/AggregatorInterface.sol:L16 @ chainlink@05ead33)
pub(super) const ABI_EVENT_ANSWER_UPDATED_SIGNATURE: &str = "AnswerUpdated(int256,uint256,uint256)";

pub(super) const CONTRACT_ROLE_ENTRYPOINT: &str = "entrypoint";
pub(super) const CONTRACT_ROLE_SPONSORING_PAYMASTER: &str = "sponsoring_paymaster";
pub(super) const CONTRACT_ROLE_ETH_USD_FEED: &str = "eth_usd_feed";

impl EntrypointUserOperationSyncSummary {
    pub async fn sync_for_block_hashes(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
    ) -> Result<Self> {
        sync_entrypoint_user_operation_with_scope(pool, chain, true, block_hashes, None, None).await
    }

    pub async fn sync_for_block_hashes_with_source_scope(
        pool: &PgPool,
        chain: &str,
        block_hashes: &[String],
        source_scope: &[(String, String, i64, i64)],
    ) -> Result<Self> {
        sync_entrypoint_user_operation_with_scope(
            pool,
            chain,
            true,
            block_hashes,
            Some(source_scope),
            None,
        )
        .await
    }
}

pub async fn sync_entrypoint_user_operation(
    pool: &PgPool,
    chain: &str,
) -> Result<EntrypointUserOperationSyncSummary> {
    sync_entrypoint_user_operation_with_scope(pool, chain, false, &[], None, None).await
}

pub async fn sync_entrypoint_user_operation_through_block(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
) -> Result<EntrypointUserOperationSyncSummary> {
    sync_entrypoint_user_operation_with_scope(
        pool,
        chain,
        false,
        &[],
        None,
        Some(target_block_number),
    )
    .await
}

async fn sync_entrypoint_user_operation_with_scope(
    pool: &PgPool,
    chain: &str,
    restrict_to_block_hashes: bool,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    max_block_number: Option<i64>,
) -> Result<EntrypointUserOperationSyncSummary> {
    let Some(manifest_scope) = load_gas_sponsorship_manifest_scope(pool, chain).await? else {
        return Ok(empty_summary(0));
    };
    let raw_log_scope = RawLogScope {
        restrict_to_block_hashes,
        block_hashes,
        source_scope,
        max_block_number,
    };

    let user_operation_logs =
        load_sponsored_user_operation_raw_logs(pool, chain, &manifest_scope, &raw_log_scope)
            .await?;
    let price_feed_logs =
        load_price_feed_raw_logs(pool, chain, &manifest_scope, &raw_log_scope).await?;
    let scanned_log_count = user_operation_logs.len() + price_feed_logs.len();
    if scanned_log_count == 0 {
        return Ok(empty_summary(0));
    }

    let transaction_keys = user_operation_logs
        .iter()
        .map(|raw_log| (raw_log.block_hash.clone(), raw_log.transaction_hash.clone()))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let inputs_by_transaction = load_raw_transaction_inputs(pool, chain, &transaction_keys)
        .await?
        .into_iter()
        .map(|row| {
            (
                (row.block_hash.clone(), row.transaction_hash.clone()),
                row.input,
            )
        })
        .collect::<HashMap<_, _>>();

    let operations = user_operation_logs
        .iter()
        .map(|raw_log| {
            let transaction_input = inputs_by_transaction
                .get(&(raw_log.block_hash.clone(), raw_log.transaction_hash.clone()))
                .map(Vec::as_slice);
            resolve_sponsored_operation(raw_log, transaction_input)
                .map(|operation| (raw_log, operation))
        })
        .collect::<Result<Vec<_>>>()?;

    let attributed_nodes = operations
        .iter()
        .flat_map(|(_, operation)| operation.writes.iter())
        .filter_map(|write| write.node.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let surfaces_by_namehash =
        load_logical_name_ids_by_namehash(pool, &manifest_scope.namespace, &attributed_nodes)
            .await?;

    let mut events = Vec::new();
    for (raw_log, operation) in &operations {
        events.push(build_sponsored_user_operation_event(
            &manifest_scope,
            raw_log,
            operation,
        ));
        for write in &operation.writes {
            events.push(build_sponsored_name_write_event(
                &manifest_scope,
                raw_log,
                operation,
                write,
                &surfaces_by_namehash,
            ));
        }
    }
    for raw_log in &price_feed_logs {
        events.push(build_price_feed_event(&manifest_scope, raw_log)?);
    }

    let counts = upsert_normalized_events_with_counts(pool, &events, "gas sponsorship").await?;
    let (total_synced_count, total_inserted_count, by_kind) =
        counts.into_parts_by_kind(|synced_count, inserted_count| {
            EntrypointUserOperationKindSyncSummary {
                synced_count,
                inserted_count,
            }
        });

    Ok(EntrypointUserOperationSyncSummary {
        scanned_log_count,
        matched_log_count: scanned_log_count,
        total_synced_count,
        total_inserted_count,
        by_kind,
    })
}
