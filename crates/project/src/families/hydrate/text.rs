//! F6 text enrichment keeps the event-derived columns intact. The existing JSON overlay records
//! the outcome and the selectors it was read for; null restores the missing-value baseline.
use std::collections::BTreeMap;

use bigname_lookup::{
    ChainRpcUrls, EnsTextRecordMulticallBlock, EnsTextRecordMulticallRequest,
    EnsTextRecordMulticallResult, MULTICALL3_ADDRESS, execute_ens_text_record_multicall,
};
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::super::{
    input::{BlockHeader, Position},
    reduce::{Context, key_of, set},
    store::{Row, RowSet, key_text},
    tables,
};
use super::ETHEREUM;
use super::admission::TEXT_RESOLVERS;
use crate::{ProjectError, Result};

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
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to select family text hydration", error))?;
    Ok(values.into_iter().filter_map(candidate).collect())
}

fn candidate(value: Value) -> Option<Candidate> {
    let row = value.as_object()?;
    let position = Position::of_row(row)?;
    let version = value.get("_version").cloned().unwrap_or(Value::Null);
    let admission = &value["_admission"];
    let resolver = value["resolver_address"].as_str()?;
    let selector_key = value["selector_key"].as_str().unwrap_or("");
    let namehash = value["_namehash"].as_str();
    let version_position = version.as_object().and_then(Position::of_row);
    let active = value["status"] == "unsupported"
        && value["record_key"] == format!("text:{selector_key}")
        && !selector_key.trim().is_empty()
        && TEXT_RESOLVERS.contains(&resolver)
        && admission["support_status"] == "supported"
        && namehash.is_some()
        && version_position.is_none_or(|boundary| position > boundary);
    let selector = json!({
        "source_position": position.to_json(), "version_position": version,
        "admission": admission, "namehash": namehash,
    });
    let overlay = &value["hydrated_value"];
    let current = value["_readable"] == true
        && [
            "source_position",
            "version_position",
            "admission",
            "namehash",
        ]
        .iter()
        .all(|field| overlay[*field] == selector[*field]);
    if (active && current) || (!active && overlay.is_null()) {
        return None;
    }
    Some(Candidate {
        key: key_of(
            &tables::NODE_RECORD_VALUE,
            tables::NODE_RECORD_VALUE
                .key
                .iter()
                .map(|column| value[*column].clone()),
        ),
        selector,
        request: active.then(|| EnsTextRecordMulticallRequest {
            resolver_address: resolver.to_owned(),
            namehash: namehash.expect("active namehash").to_owned(),
            text_key: selector_key.to_owned(),
        }),
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
    for chunk in requests.chunks(250) {
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
            match matched.and_then(|(_, result)| result.as_ref()) {
                Some(EnsTextRecordMulticallResult::Success { value }) => {
                    overlay["status"] = json!("success");
                    overlay["value"] = json!(value);
                }
                Some(EnsTextRecordMulticallResult::NotFound) => {
                    overlay["status"] = json!("not_found")
                }
                Some(EnsTextRecordMulticallResult::Failed { .. }) | None => overlay = Value::Null,
            }
            let height = if overlay.is_null() {
                Value::Null
            } else {
                overlay["block_hash"] = json!(context.block.hash);
                json!(context.block.number)
            };
            set(&mut row, "hydrated_value", overlay);
            set(&mut row, "hydrated_at_block", height);
            rows.put(&tables::NODE_RECORD_VALUE, row)?;
        }
        Ok(())
    }
}
