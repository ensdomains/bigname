use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{RawLog, RawReceipt, RawTransaction};

use crate::{
    backfill::BackfillBlockRange,
    provider::{
        ChainProviderOps, ProviderBlockBundle, ProviderLog, ProviderReceipt, ProviderResolvedBlock,
        ProviderTransaction, ProviderTransactionReceiptRequest,
    },
    reconciliation::{
        provider_log_to_raw_log, provider_receipts_to_selected_raw_receipts,
        provider_transactions_to_selected_raw_transactions,
        retained_transaction_keys_from_raw_logs,
    },
};

pub(super) struct MaterializedBackfillBlockPayloads {
    pub(super) transactions: Vec<RawTransaction>,
    pub(super) receipts: Vec<RawReceipt>,
    pub(super) logs: Vec<RawLog>,
}

pub(super) fn materialize_backfill_block_payloads(
    chain: &str,
    raw_block: &bigname_storage::RawBlock,
    selection_logs: &[ProviderLog],
    payload_logs: &[ProviderLog],
    transactions: &[ProviderTransaction],
    receipts: &[ProviderReceipt],
    selected_addresses: &BTreeSet<String>,
) -> Result<MaterializedBackfillBlockPayloads> {
    ensure_selected_seed_logs_exist_in_payload(selection_logs, payload_logs, selected_addresses)?;
    let selected_transaction_keys = selection_logs
        .iter()
        .filter(|log| selected_addresses.contains(&log.address.to_ascii_lowercase()))
        .map(|log| (log.transaction_hash.clone(), log.transaction_index))
        .collect::<BTreeSet<_>>();
    let raw_logs = payload_logs
        .iter()
        .filter(|log| {
            selected_transaction_keys
                .contains(&(log.transaction_hash.clone(), log.transaction_index))
        })
        .map(|log| provider_log_to_raw_log(chain, raw_block, log))
        .collect::<Result<Vec<_>>>()?;
    let retained_transaction_keys = retained_transaction_keys_from_raw_logs(&raw_logs);
    let raw_transactions = provider_transactions_to_selected_raw_transactions(
        chain,
        raw_block,
        transactions,
        &retained_transaction_keys,
    )?;
    let raw_receipts = provider_receipts_to_selected_raw_receipts(
        chain,
        raw_block,
        receipts,
        &retained_transaction_keys,
    )?;

    Ok(MaterializedBackfillBlockPayloads {
        transactions: raw_transactions,
        receipts: raw_receipts,
        logs: raw_logs,
    })
}

pub(super) fn selected_code_observation_addresses(
    selected_addresses: &BTreeSet<String>,
) -> Vec<String> {
    selected_addresses.iter().cloned().collect()
}

pub(super) fn selected_seed_log_addresses(
    selection_logs: &[ProviderLog],
    selected_addresses: &BTreeSet<String>,
) -> Vec<String> {
    selection_logs
        .iter()
        .map(|log| log.address.to_ascii_lowercase())
        .filter(|address| selected_addresses.contains(address))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn ensure_selected_seed_logs_exist_in_payload(
    selection_logs: &[ProviderLog],
    payload_logs: &[ProviderLog],
    selected_addresses: &BTreeSet<String>,
) -> Result<()> {
    let payload_identities = payload_logs
        .iter()
        .map(log_identity)
        .collect::<BTreeSet<_>>();
    for log in selection_logs
        .iter()
        .filter(|log| selected_addresses.contains(&log.address.to_ascii_lowercase()))
    {
        if !payload_identities.contains(&log_identity(log)) {
            bail!(
                "selected backfill log is missing from exact payload for block {} transaction {} log_index {}",
                log.block_hash,
                log.transaction_hash,
                log.log_index
            );
        }
    }

    Ok(())
}

fn log_identity(log: &ProviderLog) -> (String, String, i64, i64, String, Vec<String>, String) {
    (
        log.block_hash.to_ascii_lowercase(),
        log.transaction_hash.to_ascii_lowercase(),
        log.transaction_index,
        log.log_index,
        log.address.to_ascii_lowercase(),
        log.topics
            .iter()
            .map(|topic| topic.to_ascii_lowercase())
            .collect(),
        log.data.to_ascii_lowercase(),
    )
}

pub(super) async fn fetch_full_payload_bundles_for_log_blocks(
    provider: &(impl ChainProviderOps + ?Sized),
    resolved_blocks: &[ProviderResolvedBlock],
    logs_by_block: &BTreeMap<i64, Vec<ProviderLog>>,
    chain: &str,
    range: BackfillBlockRange,
    source_label: &str,
) -> Result<BTreeMap<String, ProviderBlockBundle>> {
    let resolved_blocks_with_logs = resolved_blocks
        .iter()
        .filter(|block| {
            logs_by_block
                .get(&block.block_number)
                .is_some_and(|logs| !logs.is_empty())
        })
        .cloned()
        .collect::<Vec<_>>();
    if resolved_blocks_with_logs.is_empty() {
        return Ok(BTreeMap::new());
    }

    let bundles = provider
        .fetch_block_bundles_by_hashes(&resolved_blocks_with_logs)
        .await
        .with_context(|| {
            format!(
                "failed to fetch {source_label} full payloads for selected-log blocks on chain {chain} range {}..={}",
                range.from_block, range.to_block
            )
        })?;

    Ok(bundles
        .into_iter()
        .map(|bundle| (bundle.block.block_hash.clone(), bundle))
        .collect())
}

pub(super) fn missing_transaction_receipt_requests_from_raw_facts(
    logs: &[RawLog],
    transactions: &[RawTransaction],
    receipts: &[RawReceipt],
) -> Vec<ProviderTransactionReceiptRequest> {
    let transaction_keys = transactions
        .iter()
        .map(|transaction| {
            (
                transaction.block_hash.clone(),
                transaction.transaction_hash.clone(),
                transaction.transaction_index,
            )
        })
        .collect::<BTreeSet<_>>();
    let receipt_keys = receipts
        .iter()
        .map(|receipt| {
            (
                receipt.block_hash.clone(),
                receipt.transaction_hash.clone(),
                receipt.transaction_index,
            )
        })
        .collect::<BTreeSet<_>>();
    let mut requests = BTreeMap::<(String, String, i64), ProviderTransactionReceiptRequest>::new();
    for log in logs {
        let key = (
            log.block_hash.clone(),
            log.transaction_hash.clone(),
            log.transaction_index,
        );
        if transaction_keys.contains(&key) && receipt_keys.contains(&key) {
            continue;
        }
        requests
            .entry(key)
            .or_insert_with(|| ProviderTransactionReceiptRequest {
                transaction_hash: log.transaction_hash.clone(),
                block_hash: log.block_hash.clone(),
                block_number: log.block_number,
                transaction_index: log.transaction_index,
            });
    }

    requests.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_storage::{CanonicalityState, RawBlock};

    #[test]
    fn materialized_payloads_retain_selected_transaction_sibling_logs() -> Result<()> {
        let raw_block = raw_block();
        let selected_address = "0x0000000000000000000000000000000000000001";
        let sibling_address = "0x00000000000000000000000000000000000000ff";
        let logs = vec![
            provider_log(selected_address, 0),
            provider_log(sibling_address, 1),
            ProviderLog {
                transaction_hash:
                    "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
                transaction_index: 1,
                log_index: 2,
                ..provider_log(sibling_address, 2)
            },
        ];
        let transactions = vec![
            provider_transaction(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                0,
            ),
            provider_transaction(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                1,
            ),
        ];
        let receipts = vec![
            provider_receipt(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                0,
            ),
            provider_receipt(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                1,
            ),
        ];
        let materialized = materialize_backfill_block_payloads(
            "ethereum-mainnet",
            &raw_block,
            &logs,
            &logs,
            &transactions,
            &receipts,
            &BTreeSet::from([selected_address.to_owned()]),
        )?;

        assert_eq!(
            materialized
                .logs
                .iter()
                .map(|log| (log.emitting_address.as_str(), log.log_index))
                .collect::<Vec<_>>(),
            vec![(selected_address, 0), (sibling_address, 1)]
        );
        assert_eq!(materialized.transactions.len(), 1);
        assert_eq!(materialized.receipts.len(), 1);
        Ok(())
    }

    #[test]
    fn materialized_payloads_do_not_reselect_same_address_logs_outside_selected_transactions()
    -> Result<()> {
        let raw_block = raw_block();
        let selected_address = "0x0000000000000000000000000000000000000001";
        let sibling_address = "0x00000000000000000000000000000000000000ff";
        let unrelated_tx_hash =
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let logs = vec![
            provider_log(selected_address, 0),
            provider_log(sibling_address, 1),
            ProviderLog {
                transaction_hash: unrelated_tx_hash.to_owned(),
                transaction_index: 1,
                log_index: 2,
                ..provider_log(selected_address, 2)
            },
        ];
        let transactions = vec![
            provider_transaction(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                0,
            ),
            provider_transaction(unrelated_tx_hash, 1),
        ];
        let receipts = vec![
            provider_receipt(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                0,
            ),
            provider_receipt(unrelated_tx_hash, 1),
        ];

        let materialized = materialize_backfill_block_payloads(
            "ethereum-mainnet",
            &raw_block,
            &logs[..1],
            &logs,
            &transactions,
            &receipts,
            &BTreeSet::from([selected_address.to_owned()]),
        )?;

        assert_eq!(
            materialized
                .logs
                .iter()
                .map(|log| (log.emitting_address.as_str(), log.log_index))
                .collect::<Vec<_>>(),
            vec![(selected_address, 0), (sibling_address, 1)]
        );
        assert_eq!(materialized.transactions.len(), 1);
        assert_eq!(materialized.receipts.len(), 1);
        Ok(())
    }

    #[test]
    fn decoded_coinbase_sql_payloads_do_not_materialize_provider_only_log_identities() -> Result<()>
    {
        let raw_block = raw_block();
        let coinbase_returned_address = "0x0000000000000000000000000000000000000001";
        let provider_only_address = "0x0000000000000000000000000000000000000002";
        let provider_only_tx_hash =
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let logs = vec![
            provider_log(coinbase_returned_address, 0),
            ProviderLog {
                transaction_hash: provider_only_tx_hash.to_owned(),
                transaction_index: 1,
                log_index: 1,
                ..provider_log(provider_only_address, 1)
            },
        ];
        let transactions = vec![
            provider_transaction(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                0,
            ),
            provider_transaction(provider_only_tx_hash, 1),
        ];
        let receipts = vec![
            provider_receipt(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                0,
            ),
            provider_receipt(provider_only_tx_hash, 1),
        ];

        let materialized = materialize_backfill_block_payloads(
            "ethereum-mainnet",
            &raw_block,
            &logs[..1],
            &logs,
            &transactions,
            &receipts,
            &BTreeSet::from([
                coinbase_returned_address.to_owned(),
                provider_only_address.to_owned(),
            ]),
        )?;

        assert_eq!(
            materialized
                .logs
                .iter()
                .map(|log| (log.emitting_address.as_str(), log.log_index))
                .collect::<Vec<_>>(),
            vec![(coinbase_returned_address, 0)]
        );
        assert_eq!(materialized.transactions.len(), 1);
        assert_eq!(materialized.receipts.len(), 1);
        Ok(())
    }

    #[test]
    fn decoded_coinbase_sql_payloads_retain_provider_sibling_logs_in_selected_transactions()
    -> Result<()> {
        let raw_block = raw_block();
        let coinbase_returned_address = "0x0000000000000000000000000000000000000001";
        let sibling_address = "0x0000000000000000000000000000000000000002";
        let logs = vec![
            provider_log(coinbase_returned_address, 0),
            provider_log(sibling_address, 1),
        ];
        let transactions = vec![provider_transaction(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        )];
        let receipts = vec![provider_receipt(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        )];

        let materialized = materialize_backfill_block_payloads(
            "ethereum-mainnet",
            &raw_block,
            &logs[..1],
            &logs,
            &transactions,
            &receipts,
            &BTreeSet::from([
                coinbase_returned_address.to_owned(),
                sibling_address.to_owned(),
            ]),
        )?;

        assert_eq!(
            materialized
                .logs
                .iter()
                .map(|log| (log.emitting_address.as_str(), log.log_index))
                .collect::<Vec<_>>(),
            vec![(coinbase_returned_address, 0), (sibling_address, 1)]
        );
        assert_eq!(materialized.transactions.len(), 1);
        assert_eq!(materialized.receipts.len(), 1);
        Ok(())
    }

    #[test]
    fn decoded_coinbase_sql_payloads_fail_when_selected_seed_identity_mismatches_provider_payload()
    {
        let raw_block = raw_block();
        let coinbase_returned_address = "0x0000000000000000000000000000000000000001";
        let provider_only_address = "0x0000000000000000000000000000000000000002";
        let seed_logs = vec![provider_log(coinbase_returned_address, 0)];
        let payload_logs = vec![ProviderLog {
            address: provider_only_address.to_owned(),
            ..provider_log(provider_only_address, 0)
        }];
        let transactions = vec![provider_transaction(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        )];
        let receipts = vec![provider_receipt(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            0,
        )];

        let result = materialize_backfill_block_payloads(
            "ethereum-mainnet",
            &raw_block,
            &seed_logs,
            &payload_logs,
            &transactions,
            &receipts,
            &BTreeSet::from([
                coinbase_returned_address.to_owned(),
                provider_only_address.to_owned(),
            ]),
        );

        let Err(error) = result else {
            panic!("selected SQL seed identity must match exact provider payload identity");
        };
        assert!(
            format!("{error:#}").contains("selected backfill log is missing from exact payload")
        );
    }

    #[test]
    fn materialized_payloads_fail_when_selected_seed_log_is_missing_from_payload() {
        let raw_block = raw_block();
        let selected_address = "0x0000000000000000000000000000000000000001";
        let sibling_address = "0x00000000000000000000000000000000000000ff";
        let seed_logs = vec![provider_log(selected_address, 0)];
        let payload_logs = vec![provider_log(sibling_address, 1)];

        let result = materialize_backfill_block_payloads(
            "ethereum-mainnet",
            &raw_block,
            &seed_logs,
            &payload_logs,
            &[],
            &[],
            &BTreeSet::from([selected_address.to_owned()]),
        );
        assert!(
            result.is_err(),
            "selected seed log missing from full payload must fail closed"
        );
        let error = result.err().expect("error was asserted");

        assert!(
            error
                .to_string()
                .contains("selected backfill log is missing from exact payload"),
            "unexpected error: {error:#}"
        );
    }

    fn raw_block() -> RawBlock {
        RawBlock {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            parent_hash: None,
            block_number: 42,
            block_timestamp: sqlx::types::time::OffsetDateTime::UNIX_EPOCH,
            logs_bloom: None,
            transactions_root: None,
            receipts_root: None,
            state_root: None,
            canonicality_state: CanonicalityState::Observed,
        }
    }

    fn provider_log(address: &str, log_index: i64) -> ProviderLog {
        ProviderLog {
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 42,
            transaction_hash: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_owned(),
            transaction_index: 0,
            log_index,
            address: address.to_owned(),
            topics: Vec::new(),
            data: "0x".to_owned(),
        }
    }

    fn provider_transaction(transaction_hash: &str, transaction_index: i64) -> ProviderTransaction {
        ProviderTransaction {
            transaction_hash: transaction_hash.to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 42,
            transaction_index,
            from: "0x0000000000000000000000000000000000000001".to_owned(),
            to: Some("0x0000000000000000000000000000000000000002".to_owned()),
        }
    }

    fn provider_receipt(transaction_hash: &str, transaction_index: i64) -> ProviderReceipt {
        ProviderReceipt {
            transaction_hash: transaction_hash.to_owned(),
            block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            block_number: 42,
            transaction_index,
            contract_address: None,
            status: Some(1),
            cumulative_gas_used: Some(21_000),
            gas_used: Some(21_000),
            logs_bloom: None,
        }
    }
}
