use sqlx::{Postgres, QueryBuilder, Transaction};

use crate::{
    IngestError, Result,
    provider::{Block, Log, Receipt, Transaction as RawTransaction},
};

const ROWS_PER_INSERT: usize = 500;

pub async fn lineage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    blocks: &[Block],
) -> Result<()> {
    for chunk in blocks.chunks(ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO chain_lineage \
             (chain_id, block_hash, parent_hash, block_number, block_timestamp) ",
        );
        query.push_values(chunk, |mut row, block| {
            row.push_bind(chain_id)
                .push_bind(&block.hash)
                .push_bind(&block.parent_hash)
                .push_bind(block.number)
                .push("to_timestamp(")
                .push_bind_unseparated(block.timestamp_unix_secs)
                .push_unseparated(")");
        });
        query.push(" ON CONFLICT (chain_id, block_hash) DO NOTHING");
        execute(query, transaction, "chain lineage").await?;
    }
    Ok(())
}

pub async fn header_audit(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    blocks: &[Block],
) -> Result<()> {
    let blocks = blocks
        .iter()
        .filter(|block| {
            block.logs_bloom.is_some()
                || block.transactions_root.is_some()
                || block.receipts_root.is_some()
                || block.state_root.is_some()
        })
        .collect::<Vec<_>>();
    for chunk in blocks.chunks(ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO chain_header_audit \
             (chain_id, block_hash, logs_bloom, transactions_root, receipts_root, state_root) ",
        );
        query.push_values(chunk, |mut row, block| {
            row.push_bind(chain_id)
                .push_bind(&block.hash)
                .push_bind(&block.logs_bloom)
                .push_bind(&block.transactions_root)
                .push_bind(&block.receipts_root)
                .push_bind(&block.state_root);
        });
        query.push(" ON CONFLICT (chain_id, block_hash) DO NOTHING");
        execute(query, transaction, "chain header audit").await?;
    }
    Ok(())
}

pub async fn transactions(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    facts: &[RawTransaction],
) -> Result<()> {
    for chunk in facts.chunks(ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO raw_transactions \
             (chain_id, block_hash, block_number, transaction_hash, transaction_index, \
              from_address, to_address, input, value) ",
        );
        query.push_values(chunk, |mut row, fact| {
            row.push_bind(chain_id)
                .push_bind(&fact.block_hash)
                .push_bind(fact.block_number)
                .push_bind(&fact.hash)
                .push_bind(fact.index)
                .push_bind(&fact.from)
                .push_bind(&fact.to)
                .push_bind(&fact.input)
                .push_bind(&fact.value)
                .push_unseparated("::numeric");
        });
        query.push(" ON CONFLICT (chain_id, block_hash, transaction_hash) DO NOTHING");
        execute(query, transaction, "raw transactions").await?;
    }
    Ok(())
}

pub async fn receipts(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    facts: &[Receipt],
) -> Result<()> {
    for chunk in facts.chunks(ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO raw_receipts \
             (chain_id, block_hash, block_number, transaction_hash, transaction_index, \
              contract_address, status, gas_used, cumulative_gas_used, logs_bloom) ",
        );
        query.push_values(chunk, |mut row, fact| {
            row.push_bind(chain_id)
                .push_bind(&fact.block_hash)
                .push_bind(fact.block_number)
                .push_bind(&fact.transaction_hash)
                .push_bind(fact.transaction_index)
                .push_bind(&fact.contract_address)
                .push_bind(fact.status);
            match &fact.gas_used {
                Some(value) => {
                    row.push_bind(value).push_unseparated("::numeric");
                }
                None => {
                    row.push("NULL");
                }
            }
            match &fact.cumulative_gas_used {
                Some(value) => {
                    row.push_bind(value).push_unseparated("::numeric");
                }
                None => {
                    row.push("NULL");
                }
            }
            row.push_bind(&fact.logs_bloom);
        });
        query.push(" ON CONFLICT (chain_id, block_hash, transaction_hash) DO NOTHING");
        execute(query, transaction, "raw receipts").await?;
    }
    Ok(())
}

pub async fn logs(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    facts: &[Log],
) -> Result<()> {
    for chunk in facts.chunks(ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO raw_logs \
             (chain_id, block_hash, block_number, transaction_hash, transaction_index, \
              log_index, emitting_address, topics, data) ",
        );
        query.push_values(chunk, |mut row, fact| {
            row.push_bind(chain_id)
                .push_bind(&fact.block_hash)
                .push_bind(fact.block_number)
                .push_bind(&fact.transaction_hash)
                .push_bind(fact.transaction_index)
                .push_bind(fact.log_index)
                .push_bind(&fact.address)
                .push_bind(&fact.topics)
                .push_bind(&fact.data);
        });
        query.push(" ON CONFLICT (chain_id, block_hash, log_index) DO NOTHING");
        execute(query, transaction, "raw logs").await?;
    }
    Ok(())
}

async fn execute(
    mut query: QueryBuilder<'_, Postgres>,
    transaction: &mut Transaction<'_, Postgres>,
    label: &str,
) -> Result<()> {
    query
        .build()
        .execute(&mut **transaction)
        .await
        .map_err(|error| IngestError::database(format!("failed to insert {label}"), error))?;
    Ok(())
}
