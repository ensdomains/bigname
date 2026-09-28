//! F12 refresh work owns the tuple's existing hydration columns. Empty string is a successful
//! not-found response; null is no overlay (a failed call or a no-longer-eligible selector).
use std::collections::BTreeMap;

use bigname_lookup::{
    ChainRpcUrls, EnsReverseNameMulticallBlock, EnsReverseNameMulticallRequest,
    EnsReverseNameMulticallResult, MULTICALL3_ADDRESS, execute_ens_reverse_name_multicall,
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
use super::admission::EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES;
use crate::{ProjectError, Result};

pub(super) struct Candidate {
    row: Row,
    key: Row,
    node: Option<String>,
    resolver: Option<String>,
    active: bool,
}

pub(super) struct Prepared {
    work: Vec<(Candidate, Option<EnsReverseNameMulticallResult>)>,
}

fn changed(rows: &RowSet, table: &'static tables::TableSpec) -> Value {
    Value::Array(
        rows.changes_in(table)
            .iter()
            .filter_map(|change| change.after.map(|row| Value::Object(row.clone())))
            .collect(),
    )
}

pub(super) async fn select(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    rows: &RowSet,
) -> Result<Vec<Candidate>> {
    let work: Vec<Value> = sqlx::query_scalar(include_str!("reverse.sql"))
        .bind(context.chain_id)
        .bind(context.block.number)
        .bind(&context.block.hash)
        .bind(changed(rows, &tables::REVERSE_TUPLE))
        .bind(changed(rows, &tables::REGISTRY_POINTER))
        .bind(changed(rows, &tables::RESOURCE_POINTER))
        .bind(changed(rows, &tables::REVERSE_NODE_CLAIM))
        .bind(changed(rows, &tables::CLAIM_NORMALIZATION))
        .bind(EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to select family reverse hydration", error)
        })?;
    Ok(work
        .into_iter()
        .filter_map(|row| {
            let Value::Object(row) = row else { return None };
            let key = key_of(
                &tables::REVERSE_TUPLE,
                ["address", "coin_type", "namespace"].map(|column| row[column].clone()),
            );
            let node = row
                .get("reverse_node")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let resolver = row
                .get("selected_resolver")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let active = row
                .get("eligible")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Some(Candidate {
                row,
                key,
                node,
                resolver,
                active,
            })
        })
        .collect())
}

pub(super) async fn execute(
    candidates: Vec<Candidate>,
    rpc_urls: &ChainRpcUrls,
    head: &BlockHeader,
) -> Result<Prepared> {
    let active: Vec<_> = candidates
        .iter()
        .filter(|candidate| candidate.active)
        .collect();
    if !active.is_empty() && rpc_urls.url_for(ETHEREUM).is_none() {
        return Err(ProjectError::configuration(
            "family reverse hydration requires an RPC URL for ethereum-mainnet",
        ));
    }
    let block = EnsReverseNameMulticallBlock {
        block_number: head.number,
        block_hash: head.hash.clone(),
    };
    let mut results = Vec::with_capacity(active.len());
    for chunk in active.chunks(250) {
        let requests: Vec<_> = chunk
            .iter()
            .map(|candidate| EnsReverseNameMulticallRequest {
                resolver_address: candidate.resolver.clone().expect("active resolver"),
                reverse_node: candidate.node.clone().expect("active node"),
            })
            .collect();
        match execute_ens_reverse_name_multicall(
            rpc_urls,
            ETHEREUM,
            MULTICALL3_ADDRESS,
            &block,
            &requests,
        )
        .await
        {
            Ok(chunk_results) => results.extend(chunk_results),
            Err(error) => {
                results.extend(
                    requests
                        .iter()
                        .map(|_| EnsReverseNameMulticallResult::Failed {
                            message: format!("{error:#}"),
                        }),
                )
            }
        }
    }
    if results.len() != active.len() {
        return Err(ProjectError::data_integrity(
            "family reverse hydration outcome count differs from its candidates",
        ));
    }
    let mut results = results.into_iter();
    Ok(Prepared {
        work: candidates
            .into_iter()
            .map(|candidate| {
                let result = candidate
                    .active
                    .then(|| results.next().expect("count checked"));
                (candidate, result)
            })
            .collect(),
    })
}

impl Prepared {
    pub(super) async fn apply(
        self,
        transaction: &mut Transaction<'_, Postgres>,
        context: &Context<'_>,
        rows: &mut RowSet,
        ordinal: i64,
    ) -> Result<()> {
        // Re-select from the actual post-reducer rows. The block fences also reject another
        // publisher or a revision change between preparation and this transaction.
        let current = select(transaction, context, rows).await?;
        let prepared: BTreeMap<_, _> = self
            .work
            .into_iter()
            .map(|(candidate, result)| {
                (
                    key_text(&tables::REVERSE_TUPLE, &candidate.key),
                    (candidate, result),
                )
            })
            .collect();
        rows.load(
            transaction,
            &tables::REVERSE_TUPLE,
            current.iter().map(|candidate| candidate.key.clone()),
        )
        .await?;
        for candidate in current {
            let matching = prepared
                .get(&key_text(&tables::REVERSE_TUPLE, &candidate.key))
                .filter(|(prior, _)| {
                    prior.node == candidate.node
                        && prior.resolver == candidate.resolver
                        && prior.active == candidate.active
                });
            let Some((_, result)) = matching else {
                continue;
            };
            let Some(mut row) = rows.get(&tables::REVERSE_TUPLE, &candidate.key).cloned() else {
                continue;
            };
            let name = match result {
                Some(EnsReverseNameMulticallResult::Success { value }) => json!(value),
                Some(EnsReverseNameMulticallResult::NotFound) => json!(""),
                Some(EnsReverseNameMulticallResult::Failed { .. }) | None => Value::Null,
            };
            set(&mut row, "hydrated_name", name);
            set(
                &mut row,
                "attempt_block",
                if candidate.active {
                    json!(context.block.number)
                } else {
                    Value::Null
                },
            );
            set(
                &mut row,
                "attempt_hash",
                if candidate.active {
                    json!(context.block.hash)
                } else {
                    Value::Null
                },
            );
            // The publication generation is monotonic across undo/reset as well as follow;
            // using it preserves cohort ordering without unjournalled sequence allocation.
            set(
                &mut row,
                "attempt_ordinal",
                if candidate.active {
                    json!(ordinal)
                } else {
                    Value::Null
                },
            );
            set(
                &mut row,
                "baseline",
                if candidate.active {
                    candidate.row["selected_baseline"].clone()
                } else {
                    Value::Null
                },
            );
            rows.put(&tables::REVERSE_TUPLE, row)?;
        }
        Ok(())
    }
}
