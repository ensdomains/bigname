//! F12 refresh work owns the tuple's existing hydration columns. Empty string is a successful
//! not-found response; null is no overlay (a failed call or a no-longer-eligible selector).
//!
//! `hydrated_name`, `attempt_block`, `attempt_hash` and `baseline` belong together: the name, the
//! block it was observed at and the selector it was observed for. `attempt_ordinal` only orders
//! the rolling refresh. What a read writes (`batch::Read`):
//! - an answered call that succeeded, with a name or empty, writes all five;
//! - an answered call that itself failed inside the aggregate writes a null name with the
//!   attempt's block. This is a fail-closed policy, not evidence that the name was cleared: the
//!   reader falls back to the event-derived claim;
//! - a deferred tuple (its aggregate failed while the endpoint served the block) keeps its name
//!   and the block it was observed at, and takes only a new `attempt_ordinal`, so it cannot hold
//!   the oldest cohort's place. The name stays the last one successfully observed;
//! - an unobserved tuple (the RPC batch failed and the endpoint did not serve the block) is not
//!   written at all.
use std::{collections::BTreeMap, sync::LazyLock};

use bigname_lookup::{
    ChainRpcUrls, EnsReverseNameMulticallBlock, EnsReverseNameMulticallRequest,
    EnsReverseNameMulticallResult, MULTICALL3_ADDRESS, execute_ens_reverse_name_multicall,
};
use bigname_storage::families::position::emission_ordinal_sql;
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use super::super::{
    reduce::{Context, key_of, set},
    store::{Row, RowSet, key_text},
    tables,
};
use super::ETHEREUM;
use super::admission::EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES;
use super::batch::{Aggregate, Kind, Read, Session};
use super::outcome::Writes;
use crate::{ProjectError, Result};

pub(super) struct Candidate {
    row: Row,
    key: Row,
    node: Option<String>,
    resolver: Option<String>,
    active: bool,
}

pub(super) struct Prepared {
    work: Vec<(Candidate, Option<Read<EnsReverseNameMulticallResult>>)>,
}

fn changed(rows: &RowSet, table: &'static tables::TableSpec) -> Value {
    Value::Array(
        rows.changes_in(table)
            .iter()
            .filter_map(|change| change.after.map(|row| Value::Object(row.clone())))
            .collect(),
    )
}

static SELECT_SQL: LazyLock<String> = LazyLock::new(|| {
    include_str!("reverse.sql").replace(
        "{pointer_emission_ordinal}",
        &emission_ordinal_sql("p.event_identity", "p.transaction_index", "p.log_index"),
    )
});

pub(super) async fn select(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    rows: &RowSet,
) -> Result<Vec<Candidate>> {
    let targets = super::work::reverse_keys(
        transaction,
        context.chain_id,
        &super::work::changed_images(rows),
    )
    .await?;
    let work = selected_values(
        transaction,
        context.chain_id,
        context.block.number,
        &context.block.hash,
        rows,
        targets,
        false,
    )
    .await?;
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

#[allow(clippy::too_many_arguments)]
async fn selected_values(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    height: i64,
    hash: &str,
    rows: &RowSet,
    targets: Vec<Value>,
    refresh: bool,
) -> Result<Vec<Value>> {
    sqlx::query_scalar(SELECT_SQL.as_str())
        .bind(chain)
        .bind(height)
        .bind(hash)
        .bind(changed(rows, &tables::REVERSE_TUPLE))
        .bind(changed(rows, &tables::REGISTRY_POINTER))
        .bind(changed(rows, &tables::RESOURCE_POINTER))
        .bind(changed(rows, &tables::REVERSE_NODE_CLAIM))
        .bind(changed(rows, &tables::CLAIM_NORMALIZATION))
        .bind(EVENT_SILENT_REVERSE_RESOLVER_ADDRESSES)
        .bind(json!(targets))
        .bind(refresh)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to select family reverse hydration", e))
}

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    height: i64,
    targets: Vec<Value>,
) -> Result<()> {
    if targets.is_empty() {
        return Ok(());
    }
    let work = selected_values(
        transaction,
        chain,
        height,
        "",
        &RowSet::default(),
        targets.clone(),
        true,
    )
    .await?
    .into_iter()
    .filter(|row| row["eligible"] == true || !row["attempt_block"].is_null())
    .map(|mut row| {
        row["successful_at_block"] = if row["hydrated_name"].is_null() {
            Value::Null
        } else {
            row["attempt_block"].clone()
        };
        row
    })
    .collect();
    super::work::replace(
        transaction,
        "project_reverse_hydration_work",
        tables::REVERSE_TUPLE.key,
        targets,
        work,
    )
    .await
}

struct Call<'a> {
    rpc_urls: &'a ChainRpcUrls,
    block: EnsReverseNameMulticallBlock,
}

impl Aggregate for Call<'_> {
    type Request = EnsReverseNameMulticallRequest;
    type Answer = EnsReverseNameMulticallResult;

    async fn send(
        &self,
        chunk: &[Self::Request],
    ) -> std::result::Result<Vec<Self::Answer>, String> {
        execute_ens_reverse_name_multicall(
            self.rpc_urls,
            ETHEREUM,
            MULTICALL3_ADDRESS,
            &self.block,
            chunk,
        )
        .await
        .map_err(|error| format!("{error:#}"))
    }
}

pub(super) async fn execute(
    candidates: Vec<Candidate>,
    session: &mut Session<'_>,
) -> Result<Prepared> {
    // Every tuple the block changed is read, beyond the rolling share, in as many aggregates as
    // they need.
    let requests: Vec<_> = candidates
        .iter()
        .filter(|candidate| candidate.active)
        .map(|candidate| EnsReverseNameMulticallRequest {
            resolver_address: candidate.resolver.clone().expect("active resolver"),
            reverse_node: candidate.node.clone().expect("active node"),
        })
        .collect();
    let rpc_urls = session.rpc_urls;
    if !requests.is_empty() && rpc_urls.url_for(ETHEREUM).is_none() {
        return Err(ProjectError::configuration(
            "family reverse hydration requires an RPC URL for ethereum-mainnet",
        ));
    }
    let call = Call {
        rpc_urls,
        block: EnsReverseNameMulticallBlock {
            block_number: session.head.number,
            block_hash: session.head.hash.clone(),
        },
    };
    let reads = session.read(Kind::Reverse, &requests, &call).await;
    session.stats.reverse.failed_calls += reads
        .iter()
        .filter(|read| {
            matches!(
                read,
                Read::Answered(EnsReverseNameMulticallResult::Failed { .. })
            )
        })
        .count() as u64;
    let mut reads = reads.into_iter();
    Ok(Prepared {
        work: candidates
            .into_iter()
            .map(|candidate| {
                let read = candidate
                    .active
                    .then(|| reads.next().expect("one read per request"));
                (candidate, read)
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
    ) -> Result<Writes> {
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
        let mut writes = Writes::default();
        for candidate in current {
            let matching = prepared
                .get(&key_text(&tables::REVERSE_TUPLE, &candidate.key))
                .filter(|(prior, _)| {
                    prior.node == candidate.node
                        && prior.resolver == candidate.resolver
                        && prior.active == candidate.active
                });
            let Some((_, read)) = matching else {
                continue;
            };
            let Some(mut row) = rows.get(&tables::REVERSE_TUPLE, &candidate.key).cloned() else {
                continue;
            };
            let before = row.clone();
            let name = match read {
                Some(Read::Unobserved) => continue,
                Some(Read::Deferred) => {
                    set(&mut row, "attempt_ordinal", json!(ordinal));
                    writes.schedules += u64::from(row != before);
                    rows.put(&tables::REVERSE_TUPLE, row)?;
                    continue;
                }
                Some(Read::Answered(EnsReverseNameMulticallResult::Success { value })) => {
                    json!(value)
                }
                Some(Read::Answered(EnsReverseNameMulticallResult::NotFound)) => json!(""),
                // `None` is a tuple no longer eligible, whose name is cleared.
                Some(Read::Answered(EnsReverseNameMulticallResult::Failed { .. })) | None => {
                    Value::Null
                }
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
            writes.values += u64::from(row != before);
            rows.put(&tables::REVERSE_TUPLE, row)?;
        }
        Ok(writes)
    }
}
