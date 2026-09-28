//! F6 text enrichment keeps the event-derived columns intact. The existing JSON overlay records
//! the outcome and the selectors it was read for; null restores the missing-value baseline.
//!
//! A block reads at most [`ROLLING_LIMIT`] selectors, as the reverse refresh does: those the block
//! changed first, then the backlog a rebuild leaves (every overlay null) in a stable rolling order,
//! never-read selectors before the oldest attempts. A failed read keeps the null overlay but
//! stamps `hydrated_at_block` with the attempt, so it waits behind the rest of the backlog.
//!
//! `text.sql` decides which selectors need work and cuts the block's share, so a block never
//! transfers the selectors that are already current. Every row it returns is work: a read stamps
//! it current or with a newer attempt, and a cleared overlay leaves the work set, so the backlog
//! behind the cut always moves forward.
use std::collections::BTreeMap;

use bigname_lookup::{
    ChainRpcUrls, EnsTextRecordMulticallBlock, EnsTextRecordMulticallRequest,
    EnsTextRecordMulticallResult, MULTICALL3_ADDRESS, execute_ens_text_record_multicall,
};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::super::{
    input::BlockHeader,
    reduce::{Context, key_of, set},
    store::{Row, RowSet, key_text},
    tables,
};
use super::ETHEREUM;
use super::admission::TEXT_RESOLVERS;
use crate::{ProjectError, Result};

/// The most selectors one block reads, one Multicall3 batch: the reverse refresh's bound.
pub(super) const ROLLING_LIMIT: usize = 250;

pub(super) struct Candidate {
    key: Row,
    selector: Value,
    request: Option<EnsTextRecordMulticallRequest>,
}

pub(super) struct Prepared {
    work: BTreeMap<String, (Candidate, Option<EnsTextRecordMulticallResult>)>,
}

fn changes(rows: &RowSet, table: &'static tables::TableSpec) -> Value {
    Value::Array(
        rows.changes_in(table)
            .into_iter()
            .map(|change| {
                // A removed classification must replace its stored row with an absent verdict too.
                Value::Object(change.after.cloned().unwrap_or_else(|| {
                    key_of(
                        table,
                        table
                            .key
                            .iter()
                            .map(|column| change.before.unwrap()[*column].clone()),
                    )
                }))
            })
            .collect(),
    )
}

pub(super) async fn select(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    rows: &RowSet,
) -> Result<Vec<Candidate>> {
    let values: Vec<Value> = sqlx::query_scalar(include_str!("text.sql"))
        .bind(context.chain_id)
        .bind(context.block.number)
        .bind(changes(rows, &tables::NODE_RECORD_VALUE))
        .bind(changes(rows, &tables::NODE_RECORD_PARTITION))
        .bind(changes(rows, &tables::RESOLVER_CLASSIFICATION))
        .bind(TEXT_RESOLVERS)
        .bind(ROLLING_LIMIT as i64)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to select family text hydration", error))?;
    values.into_iter().map(candidate).collect()
}

/// One selected row. The query already dropped current and cleared selectors and cut the block's
/// share: the block's own changes first, then never-read selectors, then the oldest attempts, each
/// in key order. Preparation and publication run the same query on the same rows, so both cut the
/// same list.
fn candidate(value: Value) -> Result<Candidate> {
    let selector = value["_selector"].clone();
    let request = if value["_active"] == true {
        let (Some(resolver), Some(namehash), Some(text_key)) = (
            value["resolver_address"].as_str(),
            selector["namehash"].as_str(),
            value["selector_key"].as_str(),
        ) else {
            return Err(ProjectError::data_integrity(
                "family text hydration selected an eligible selector without its read inputs",
            ));
        };
        Some(EnsTextRecordMulticallRequest {
            resolver_address: resolver.to_owned(),
            namehash: namehash.to_owned(),
            text_key: text_key.to_owned(),
        })
    } else {
        None
    };
    Ok(Candidate {
        key: key_of(
            &tables::NODE_RECORD_VALUE,
            tables::NODE_RECORD_VALUE
                .key
                .iter()
                .map(|column| value[*column].clone()),
        ),
        selector,
        request,
    })
}

pub(super) async fn execute(
    candidates: Vec<Candidate>,
    rpc_urls: &ChainRpcUrls,
    head: &BlockHeader,
) -> Result<Prepared> {
    let requests: Vec<_> = candidates
        .iter()
        .filter_map(|candidate| candidate.request.clone())
        .collect();
    if !requests.is_empty() && rpc_urls.url_for(ETHEREUM).is_none() {
        return Err(ProjectError::configuration(
            "family text hydration requires an RPC URL for ethereum-mainnet",
        ));
    }
    let block = EnsTextRecordMulticallBlock {
        block_number: head.number,
        block_hash: head.hash.clone(),
    };
    let mut results = Vec::with_capacity(requests.len());
    for chunk in requests.chunks(ROLLING_LIMIT) {
        match execute_ens_text_record_multicall(
            rpc_urls,
            ETHEREUM,
            MULTICALL3_ADDRESS,
            &block,
            chunk,
        )
        .await
        {
            Ok(found) => results.extend(found),
            Err(error) => {
                results.extend(chunk.iter().map(|_| EnsTextRecordMulticallResult::Failed {
                    message: format!("{error:#}"),
                }))
            }
        }
    }
    if results.len() != requests.len() {
        return Err(ProjectError::data_integrity(
            "family text hydration outcome count differs from its candidates",
        ));
    }
    Ok(prepared(candidates, results))
}

fn prepared(candidates: Vec<Candidate>, results: Vec<EnsTextRecordMulticallResult>) -> Prepared {
    let mut results = results.into_iter();
    Prepared {
        work: candidates
            .into_iter()
            .map(|candidate| {
                let result = candidate
                    .request
                    .as_ref()
                    .map(|_| results.next().expect("count checked"));
                (
                    key_text(&tables::NODE_RECORD_VALUE, &candidate.key),
                    (candidate, result),
                )
            })
            .collect(),
    }
}

impl Prepared {
    pub(super) async fn apply(
        self,
        transaction: &mut Transaction<'_, Postgres>,
        context: &Context<'_>,
        rows: &mut RowSet,
        _ordinal: i64,
    ) -> Result<()> {
        let current = select(transaction, context, rows).await?;
        rows.load(
            transaction,
            &tables::NODE_RECORD_VALUE,
            current.iter().map(|candidate| candidate.key.clone()),
        )
        .await?;
        for candidate in current {
            let prepared = self
                .work
                .get(&key_text(&tables::NODE_RECORD_VALUE, &candidate.key));
            let matched = prepared.filter(|(prior, _)| {
                prior.selector == candidate.selector
                    && prior.request.is_some() == candidate.request.is_some()
            });
            let Some(mut row) = rows
                .get(&tables::NODE_RECORD_VALUE, &candidate.key)
                .cloned()
            else {
                continue;
            };
            let mut overlay = candidate.selector.clone();
            let result = matched.and_then(|(_, result)| result.as_ref());
            match result {
                Some(EnsTextRecordMulticallResult::Success { value }) => {
                    overlay["status"] = json!("success");
                    overlay["value"] = json!(value);
                }
                Some(EnsTextRecordMulticallResult::NotFound) => {
                    overlay["status"] = json!("not_found")
                }
                Some(EnsTextRecordMulticallResult::Failed { .. }) | None => overlay = Value::Null,
            }
            if !overlay.is_null() {
                overlay["block_hash"] = json!(context.block.hash);
            }
            // A failed read stamps its attempt with a null overlay, which nothing serves.
            let height = if result.is_some() {
                json!(context.block.number)
            } else {
                Value::Null
            };
            set(&mut row, "hydrated_value", overlay);
            set(&mut row, "hydrated_at_block", height);
            rows.put(&tables::NODE_RECORD_VALUE, row)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    /// The query's blank-key test trims exactly the characters `str::trim` trims.
    #[test]
    fn the_query_trims_rust_white_space() {
        let listed: String = (0..=u32::from(char::MAX))
            .filter_map(char::from_u32)
            .filter(|character| character.is_whitespace())
            .map(|character| format!("\\{:04X}", u32::from(character)))
            .collect();
        assert!(include_str!("text.sql").contains(&format!("U&'{listed}'")));
    }
}
