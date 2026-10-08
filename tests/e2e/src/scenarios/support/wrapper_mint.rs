use std::str::FromStr;

use alloy_primitives::{Address, B256, LogData, U256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::harness::ens_v1;

sol! {
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
}

fn log_data(topics: Vec<String>, data: Vec<u8>) -> Result<LogData> {
    let topics = topics
        .iter()
        .map(|topic| B256::from_str(topic))
        .collect::<Result<Vec<_>, _>>()?;
    LogData::new(topics, data.into()).context("wrapper log has too many topics")
}

type RawWrapperPair = (Vec<String>, Vec<u8>, Vec<String>, Vec<u8>);

/// Independently decode the exact raw positions claimed by one normalized wrapper effect.
pub(in crate::scenarios) async fn assert_wrapper_mint_completion(
    pool: &PgPool,
    name: &str,
    effect: &str,
    wrapper: Address,
    receiver: Address,
) -> Result<()> {
    let node = ens_v1::namehash(name);
    let rows: Vec<RawWrapperPair> = sqlx::query_as(
        "SELECT mint.topics, mint.data, completion.topics, completion.data
         FROM normalized_events event
         JOIN raw_logs mint ON mint.chain_id = event.chain_id
           AND mint.block_hash = event.block_hash AND mint.transaction_hash = event.transaction_hash
           AND mint.log_index = event.log_index
         JOIN raw_logs completion ON completion.chain_id = mint.chain_id
           AND completion.block_hash = mint.block_hash AND completion.transaction_hash = mint.transaction_hash
           AND completion.emitting_address = mint.emitting_address
           AND completion.log_index > mint.log_index
           AND completion.block_hash = event.after_state #>> '{matched_wrapper_completion,block_hash}'
           AND completion.transaction_hash = event.after_state #>> '{matched_wrapper_completion,transaction_hash}'
           AND completion.log_index = (event.after_state #>> '{matched_wrapper_completion,log_index}')::BIGINT
         WHERE event.source_family = 'ens_v1_wrapper_l1' AND event.event_kind = $1
           AND event.after_state->>'node' = $2 AND mint.emitting_address = $3
           AND event.after_state->>'source_event' = 'TransferSingle'
           AND event.after_state->'wrapper_mint' = 'true'::jsonb
           AND event.after_state #>> '{matched_wrapper_completion,source_event}' = 'NameWrapped'
           AND event.canonicality_state = 'canonical' AND event.consumer_visibility = 'activated'",
    )
    .bind(effect).bind(format!("{node:#x}")).bind(format!("{wrapper:#x}"))
    .fetch_all(pool).await?;
    assert_eq!(
        rows.len(),
        1,
        "one matched mint must own {effect} for {name}"
    );
    let (topics, data, completion_topics, completion_data) = rows.into_iter().next().unwrap();
    let mint = TransferSingle::decode_log_data_validate(&log_data(topics, data)?)?;
    let completion =
        NameWrapped::decode_log_data_validate(&log_data(completion_topics, completion_data)?)?;
    assert_eq!(mint.from, Address::ZERO);
    assert_eq!(mint.to, receiver);
    assert_eq!(mint.value, U256::from(1));
    assert_eq!(mint.id, U256::from_be_bytes(node.0));
    assert_eq!(completion.node, node);
    assert_eq!(completion.owner, receiver);
    assert_eq!(completion.name, ens_v1::dns_encode_name(name)?);
    Ok(())
}
