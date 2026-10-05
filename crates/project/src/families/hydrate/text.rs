//! F6 text enrichment keeps the event-derived columns intact. The existing JSON overlay records
//! the outcome and the selectors it was read for; null restores the missing-value baseline.
//!
//! A hydrating block reads at most [`ROLLING_LIMIT`] selectors: those the block changed first,
//! then the backlog a rebuild leaves (every overlay null) in a stable rolling order, never-read
//! selectors before the oldest attempts.
//!
//! What a read writes (`batch::Read`):
//! - an answered call that succeeded, with a value or empty, writes the overlay and its block;
//! - an answered call that itself failed inside the aggregate writes a null overlay and stamps
//!   `hydrated_at_block` with the attempt. This is a fail-closed policy, not evidence that the
//!   record is empty: nothing is served for the selector and it waits behind the rest of the
//!   backlog;
//! - a deferred selector (its aggregate failed while the endpoint served the block) is written
//!   the same way. Every selected selector is work, so its overlay, if it has one, was already
//!   not served for the selector as it now stands; the stamp is what moves it back;
//! - an unobserved selector (the RPC batch failed and the endpoint did not serve the block) is
//!   not written at all.
//!
//! `text.sql` decides which selectors need work and cuts the block's share, so a block never
//! transfers the selectors that are already current. Every row it returns is work: a read stamps
//! it current or with a newer attempt, and a cleared overlay leaves the work set, so the backlog
//! behind the cut moves forward whenever the block's share has room for it. Changed selectors
//! still rank first: while 250 or more change every block, the unchanged backlog waits, as it
//! did before the cut moved into the query, and a steady stream of never-read selectors likewise
//! delays failed retries.
use std::{collections::BTreeMap, sync::LazyLock};

use bigname_lookup::{
    ChainRpcUrls, EnsTextRecordMulticallBlock, EnsTextRecordMulticallRequest,
    EnsTextRecordMulticallResult, MULTICALL3_ADDRESS, execute_ens_text_record_multicall,
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
use super::admission::TEXT_RESOLVERS;
use super::batch::{Aggregate, BATCH_LIMIT, Kind, Read, Session};
use super::outcome::Writes;
use crate::{ProjectError, Result};

/// The most selectors one block reads, one Multicall3 aggregate.
pub(super) const ROLLING_LIMIT: usize = BATCH_LIMIT;

pub(super) struct Candidate {
    key: Row,
    selector: Value,
    request: Option<EnsTextRecordMulticallRequest>,
}

pub(super) struct Prepared {
    work: BTreeMap<String, (Candidate, Option<Read<EnsTextRecordMulticallResult>>)>,
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

static SELECT_SQL: LazyLock<String> = LazyLock::new(|| {
    include_str!("text.sql")
        .replace(
            "{value_emission_ordinal}",
            &emission_ordinal_sql(
                "value.event_identity",
                "value.transaction_index",
                "value.log_index",
            ),
        )
        .replace(
            "{boundary_emission_ordinal}",
            &emission_ordinal_sql(
                "boundary.event_identity",
                "boundary.transaction_index",
                "boundary.log_index",
            ),
        )
});

pub(super) async fn select(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    rows: &RowSet,
) -> Result<Vec<Candidate>> {
    let targets = super::work::text_keys(
        transaction,
        context.chain_id,
        &super::work::changed_images(rows),
    )
    .await?;
    let values = selected_values(
        transaction,
        context.chain_id,
        context.block.number,
        rows,
        targets,
        false,
    )
    .await?;
    values.into_iter().map(candidate).collect()
}

async fn selected_values(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    height: i64,
    rows: &RowSet,
    targets: Vec<Value>,
    refresh: bool,
) -> Result<Vec<Value>> {
    sqlx::query_scalar(SELECT_SQL.as_str())
        .bind(chain)
        .bind(height)
        .bind(changes(rows, &tables::NODE_RECORD_VALUE))
        .bind(changes(rows, &tables::NODE_RECORD_PARTITION))
        .bind(changes(rows, &tables::RESOLVER_CLASSIFICATION))
        .bind(TEXT_RESOLVERS)
        .bind(ROLLING_LIMIT as i64)
        .bind(json!(targets))
        .bind(refresh)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|e| ProjectError::database("failed to select family text hydration", e))
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
        &RowSet::default(),
        targets.clone(),
        true,
    )
    .await?;
    super::work::replace(
        transaction,
        "project_text_hydration_work",
        tables::NODE_RECORD_VALUE.key,
        targets,
        work,
    )
    .await
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

struct Call<'a> {
    rpc_urls: &'a ChainRpcUrls,
    block: EnsTextRecordMulticallBlock,
}

impl Aggregate for Call<'_> {
    type Request = EnsTextRecordMulticallRequest;
    type Answer = EnsTextRecordMulticallResult;

    async fn send(
        &self,
        chunk: &[Self::Request],
    ) -> std::result::Result<Vec<Self::Answer>, String> {
        execute_ens_text_record_multicall(
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
    let requests: Vec<_> = candidates
        .iter()
        .filter_map(|candidate| candidate.request.clone())
        .collect();
    let rpc_urls = session.rpc_urls;
    if !requests.is_empty() && rpc_urls.url_for(ETHEREUM).is_none() {
        return Err(ProjectError::configuration(
            "family text hydration requires an RPC URL for ethereum-mainnet",
        ));
    }
    let call = Call {
        rpc_urls,
        block: EnsTextRecordMulticallBlock {
            block_number: session.head.number,
            block_hash: session.head.hash.clone(),
        },
    };
    let reads = session.read(Kind::Text, &requests, &call).await;
    session.stats.text.failed_calls += reads
        .iter()
        .filter(|read| {
            matches!(
                read,
                Read::Answered(EnsTextRecordMulticallResult::Failed { .. })
            )
        })
        .count() as u64;
    let mut reads = reads.into_iter();
    Ok(Prepared {
        work: candidates
            .into_iter()
            .map(|candidate| {
                let read = candidate
                    .request
                    .as_ref()
                    .map(|_| reads.next().expect("one read per request"));
                (
                    key_text(&tables::NODE_RECORD_VALUE, &candidate.key),
                    (candidate, read),
                )
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
        _ordinal: i64,
    ) -> Result<Writes> {
        let current = select(transaction, context, rows).await?;
        rows.load(
            transaction,
            &tables::NODE_RECORD_VALUE,
            current.iter().map(|candidate| candidate.key.clone()),
        )
        .await?;
        let mut writes = Writes::default();
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
            // `None` is a selector with no read to make: one no longer eligible, whose overlay
            // is cleared, or one that changed between preparation and this transaction.
            let read = matched.and_then(|(_, read)| read.as_ref());
            match read {
                Some(Read::Unobserved) => continue,
                Some(Read::Answered(EnsTextRecordMulticallResult::Success { value })) => {
                    overlay["status"] = json!("success");
                    overlay["value"] = json!(value);
                }
                Some(Read::Answered(EnsTextRecordMulticallResult::NotFound)) => {
                    overlay["status"] = json!("not_found")
                }
                Some(Read::Answered(EnsTextRecordMulticallResult::Failed { .. }))
                | Some(Read::Deferred)
                | None => overlay = Value::Null,
            }
            if !overlay.is_null() {
                overlay["block_hash"] = json!(context.block.hash);
            }
            // A failed or deferred read stamps its attempt with a null overlay, which nothing
            // serves.
            let height = if read.is_some() {
                json!(context.block.number)
            } else {
                Value::Null
            };
            let before = row.clone();
            set(&mut row, "hydrated_value", overlay);
            set(&mut row, "hydrated_at_block", height);
            if row != before {
                match read {
                    Some(Read::Deferred) => writes.schedules += 1,
                    _ => writes.values += 1,
                }
            }
            rows.put(&tables::NODE_RECORD_VALUE, row)?;
        }
        Ok(writes)
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
