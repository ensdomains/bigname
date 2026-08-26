use bigname_adapters::schema_v2::DecodeSkip;
use sqlx::{Postgres, QueryBuilder, Transaction};

use crate::{InterpretError, Result};

use super::batching::{batch_row_context, conflict_free_batches};

pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    skips: &[DecodeSkip],
) -> Result<()> {
    if skips.is_empty() {
        return Ok(());
    }
    let content_hash: Option<String> =
        sqlx::query_scalar("SELECT current_setting('bigname.interpreter_content_hash', true)")
            .fetch_one(&mut **transaction)
            .await
            .map_err(|error| {
                InterpretError::database("failed to read the interpreter content hash", error)
            })?;
    let content_hash = content_hash
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            InterpretError::configuration(
                "decode-skip writes require bigname.interpreter_content_hash",
            )
        })?;

    for (start, batch) in conflict_free_batches(skips, |skip| {
        (
            skip.chain_id.clone(),
            skip.block_hash.clone(),
            skip.transaction_hash.clone(),
            skip.log_index,
        )
    }) {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO interpret_decode_skips (
                 chain_id, block_hash, block_number, transaction_hash, log_index,
                 emitting_address, source_family, selection_topic0, match_all,
                 decode_context, interpreter_content_hash
             ) ",
        );
        query.push_values(batch, |mut row, skip| {
            row.push_bind(&skip.chain_id)
                .push_bind(&skip.block_hash)
                .push_bind(skip.block_number)
                .push_bind(&skip.transaction_hash)
                .push_bind(skip.log_index)
                .push_bind(&skip.emitting_address)
                .push_bind(&skip.source_family)
                .push_bind(&skip.selection_topic0)
                .push_bind(skip.match_all)
                .push_bind(&skip.decode_context)
                .push_bind(&content_hash);
        });
        query.push(
            " ON CONFLICT (
                  chain_id, block_hash, transaction_hash, log_index,
                  interpreter_content_hash
              ) DO NOTHING",
        );
        query
            .build()
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                let identities = batch.iter().map(|skip| {
                    format!(
                        "{}:{}:{}:{}",
                        skip.chain_id, skip.block_hash, skip.transaction_hash, skip.log_index
                    )
                });
                let context = batch_row_context(start, identities);
                InterpretError::database(
                    format!("failed to write interpretation decode-skip batch; {context}"),
                    error,
                )
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
