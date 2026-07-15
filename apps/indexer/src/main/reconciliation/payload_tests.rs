use std::collections::BTreeSet;

use bigname_storage::{CanonicalityState, RawBlock, RawLog};
use sqlx::types::time::OffsetDateTime;

use super::*;

#[test]
fn live_payload_retains_generic_resolver_topics_without_widening_other_emitters() {
    let selected_address = "0x00000000000000000000000000000000000000a1";
    let generic_emitter = "0x00000000000000000000000000000000000000b1";
    let unrelated_emitter = "0x00000000000000000000000000000000000000c1";
    let selected_addresses = selected_address_set(&[selected_address.to_owned()]);
    let generic_topic0 = crate::ens_v1_resolver::generic_resolver_record_topic0s()
        .into_iter()
        .next()
        .expect("ENSv1 generic resolver intake must declare at least one topic");
    let generic_resolver_topic0s = BTreeSet::from([generic_topic0.clone()]);
    let raw_block = RawBlock {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        parent_hash: Some(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        ),
        block_number: 10,
        block_timestamp: OffsetDateTime::UNIX_EPOCH,
        canonicality_state: CanonicalityState::Canonical,
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
    };
    let selected_transaction_hash =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    let generic_transaction_hash =
        "0x2222222222222222222222222222222222222222222222222222222222222222";
    let unrelated_transaction_hash =
        "0x3333333333333333333333333333333333333333333333333333333333333333";
    let unrelated_topic0 = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let logs = vec![
        provider_log(
            &raw_block,
            selected_address,
            selected_transaction_hash,
            0,
            0,
            unrelated_topic0,
        ),
        provider_log(
            &raw_block,
            unrelated_emitter,
            selected_transaction_hash,
            0,
            1,
            unrelated_topic0,
        ),
        provider_log(
            &raw_block,
            generic_emitter,
            generic_transaction_hash,
            1,
            2,
            &generic_topic0.to_ascii_uppercase(),
        ),
        provider_log(
            &raw_block,
            unrelated_emitter,
            generic_transaction_hash,
            1,
            3,
            unrelated_topic0,
        ),
        provider_log(
            &raw_block,
            unrelated_emitter,
            unrelated_transaction_hash,
            2,
            4,
            unrelated_topic0,
        ),
    ];

    let retained = provider_logs_to_live_selected_raw_logs(
        "ethereum-mainnet",
        &raw_block,
        &logs,
        &selected_addresses,
        &generic_resolver_topic0s,
    )
    .expect("live log selection must succeed");

    assert_eq!(
        retained.iter().map(|log| log.log_index).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(retained[2].emitting_address, generic_emitter);
}

fn provider_log(
    raw_block: &RawBlock,
    address: &str,
    transaction_hash: &str,
    transaction_index: i64,
    log_index: i64,
    topic0: &str,
) -> ProviderLog {
    ProviderLog {
        block_hash: raw_block.block_hash.clone(),
        block_number: raw_block.block_number,
        transaction_hash: transaction_hash.to_owned(),
        transaction_index,
        log_index,
        address: address.to_owned(),
        topics: vec![topic0.to_owned()],
        data: "0x".to_owned(),
    }
}

#[test]
fn live_payload_retains_successful_direct_calls_to_selected_addresses() {
    let selected_addresses =
        selected_address_set(&["0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()]);
    let transactions = vec![
        ProviderTransaction {
            transaction_hash: "0xaaa".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_index: 0,
            from: "0x0000000000000000000000000000000000000001".to_owned(),
            to: Some("0xA2c122bE93b0074270EBee7f6B7292c7deb45047".to_owned()),
        },
        ProviderTransaction {
            transaction_hash: "0xbbb".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_index: 1,
            from: "0x0000000000000000000000000000000000000001".to_owned(),
            to: Some("0x0000000000000000000000000000000000000002".to_owned()),
        },
    ];
    let receipts = vec![
        ProviderReceipt {
            transaction_hash: "0xaaa".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_index: 0,
            contract_address: None,
            status: Some(1),
            cumulative_gas_used: None,
            gas_used: None,
            logs_bloom: None,
        },
        ProviderReceipt {
            transaction_hash: "0xbbb".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_index: 1,
            contract_address: None,
            status: Some(1),
            cumulative_gas_used: None,
            gas_used: None,
            logs_bloom: None,
        },
    ];

    let retained = retained_transaction_keys_from_live_payload(
        &[],
        &transactions,
        &receipts,
        &selected_addresses,
    );

    assert_eq!(retained, BTreeSet::from([("0xaaa".to_owned(), 0)]));
}

#[test]
fn live_payload_records_successful_event_silent_resolver_call_observations() {
    let selected_addresses =
        selected_address_set(&["0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()]);
    let raw_block = RawBlock {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xblock".to_owned(),
        parent_hash: Some("0xparent".to_owned()),
        block_number: 10,
        block_timestamp: OffsetDateTime::UNIX_EPOCH,
        canonicality_state: CanonicalityState::Canonical,
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
    };
    let transactions = vec![ProviderTransaction {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        from: "0x0000000000000000000000000000000000000001".to_owned(),
        to: Some("0xA2c122bE93b0074270EBee7f6B7292c7deb45047".to_owned()),
    }];
    let receipts = vec![ProviderReceipt {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        contract_address: None,
        status: Some(1),
        cumulative_gas_used: None,
        gas_used: None,
        logs_bloom: None,
    }];

    let observations = event_silent_resolver_call_observations_from_live_payload(
        "ethereum-mainnet",
        &raw_block,
        &transactions,
        &receipts,
        &selected_addresses,
    );

    assert_eq!(
        observations,
        vec![EventSilentResolverCallObservation {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned(),
            block_hash: "0xblock".to_owned(),
            block_number: 10,
            transaction_hash: "0xaaa".to_owned(),
            transaction_index: 0,
            canonicality_state: CanonicalityState::Canonical,
        }]
    );
}

#[test]
fn live_payload_does_not_retain_failed_direct_calls_without_selected_logs() {
    let selected_addresses =
        selected_address_set(&["0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()]);
    let transactions = vec![ProviderTransaction {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        from: "0x0000000000000000000000000000000000000001".to_owned(),
        to: Some("0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()),
    }];
    let receipts = vec![ProviderReceipt {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        contract_address: None,
        status: Some(0),
        cumulative_gas_used: None,
        gas_used: None,
        logs_bloom: None,
    }];

    let retained = retained_transaction_keys_from_live_payload(
        &[],
        &transactions,
        &receipts,
        &selected_addresses,
    );

    assert!(retained.is_empty());
}

#[test]
fn live_payload_keeps_transactions_with_selected_logs_even_when_receipt_failed() {
    let selected_addresses =
        selected_address_set(&["0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()]);
    let logs = vec![RawLog {
        chain_id: "ethereum-mainnet".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_hash: "0xaaa".to_owned(),
        transaction_index: 0,
        log_index: 0,
        emitting_address: "0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned(),
        topics: vec![],
        data: vec![],
        canonicality_state: CanonicalityState::Canonical,
    }];
    let transactions = vec![ProviderTransaction {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        from: "0x0000000000000000000000000000000000000001".to_owned(),
        to: Some("0xa2c122be93b0074270ebee7f6b7292c7deb45047".to_owned()),
    }];
    let receipts = vec![ProviderReceipt {
        transaction_hash: "0xaaa".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 10,
        transaction_index: 0,
        contract_address: None,
        status: Some(0),
        cumulative_gas_used: None,
        gas_used: None,
        logs_bloom: None,
    }];

    let retained = retained_transaction_keys_from_live_payload(
        &logs,
        &transactions,
        &receipts,
        &selected_addresses,
    );

    assert_eq!(retained, BTreeSet::from([("0xaaa".to_owned(), 0)]));
}
