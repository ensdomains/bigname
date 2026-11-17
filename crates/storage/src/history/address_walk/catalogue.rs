//! Select the catalogue only for its exact captured publication, then walk ordered buckets.

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{
    AddressRead, catalogue_source,
    matches::Membership,
    seams,
    walk::{self, Accumulator},
};
use crate::{
    families::name::FamilyPublicationUnavailable,
    history::{
        EventHistoryReadFilter, HistoryCataloguePublicationFence, HistoryCursor, HistoryOrder,
        HistoryPageOptions, catalogue_contract as contract, keyset::HistoryKeyset,
    },
};

pub(super) async fn select_source(
    connection: &mut PgConnection,
    options: &HistoryPageOptions,
) -> Result<bool> {
    let Some(fence) = &options.catalogue_publication else {
        receipt(
            "bounded_without_catalogue_fence",
            &Value::Null,
            &Value::Null,
            None,
        );
        return Ok(false);
    };
    let HistoryCataloguePublicationFence::Captured {
        publications,
        lag_tolerance_blocks,
        captured_at,
    } = fence
    else {
        receipt(
            "inconsistent_catalogue_capture",
            &Value::Null,
            &Value::Null,
            None,
        );
        return Ok(false);
    };
    let gap = Some(captured_at.elapsed().as_secs_f64() * 1000.0);
    let captured = Value::Array(
        publications
            .iter()
            .map(|entry| {
                json!({
                    "chain_id":entry.chain_id,"block_number":entry.block_number,
                    "block_hash":entry.block_hash,"generation":entry.project_generation,
                })
            })
            .collect(),
    );
    let mut observed = Vec::new();
    let mut changed = false;
    let available: bool = sqlx::query_scalar(
        "SELECT to_regclass('bigname_phase.project_address_history_anchor') IS NOT NULL
            AND to_regclass('bigname_phase.project_history_source') IS NOT NULL
            AND to_regclass('bigname_phase.project_history_source_edge') IS NOT NULL
            AND to_regclass('bigname_phase.project_history_catalogue_marker') IS NOT NULL",
    )
    .fetch_one(&mut *connection)
    .await?;
    if !available || publications.is_empty() {
        return unavailable("none", &captured, &observed, gap);
    }
    for expected in publications {
        let position: Option<(i64, String)> = sqlx::query_as(
            "SELECT current_block_number, current_block_hash FROM bigname_phase.project_family_marker
             WHERE chain_id=$1 AND current_block_number IS NOT NULL AND current_block_hash IS NOT NULL",
        ).bind(&expected.chain_id).fetch_optional(&mut *connection).await?;
        let Some((block, hash)) = position else {
            return unavailable(&expected.chain_id, &captured, &observed, gap);
        };
        let generation = crate::load_served_project_generation(
            &mut *connection,
            &expected.chain_id,
            block,
            &hash,
            true,
            true,
            *lag_tolerance_blocks,
        )
        .await?;
        observed.push(json!({"chain_id":expected.chain_id,"block_number":block,"block_hash":hash,"generation":generation}));
        let Some(generation) = generation.filter(|_| block >= expected.block_number) else {
            return unavailable(&expected.chain_id, &captured, &observed, gap);
        };
        let stamped: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM bigname_phase.project_history_catalogue_marker
             WHERE chain_id=$1 AND block_number=$2 AND block_hash=$3
               AND publication_sequence::text=$4 AND input_content_hash=$5 AND catalogue_version=$6)",
        ).bind(&expected.chain_id).bind(block).bind(&hash).bind(&generation)
            .bind(bigname_content_hash::PROJECTION_CONTENT_HASH).bind(contract::CATALOGUE_VERSION)
            .fetch_one(&mut *connection).await?;
        if !stamped {
            return unavailable(&expected.chain_id, &captured, &observed, gap);
        }
        changed |= block != expected.block_number
            || hash != expected.block_hash
            || generation != expected.project_generation;
    }
    let path = if changed {
        "captured_publication_advanced"
    } else {
        "catalogue"
    };
    receipt(path, &captured, &Value::Array(observed), gap);
    Ok(!changed)
}

fn unavailable<T>(
    chain: &str,
    captured: &Value,
    observed: &[Value],
    gap: Option<f64>,
) -> Result<T> {
    receipt("catalogue_unavailable", captured, &json!(observed), gap);
    Err(FamilyPublicationUnavailable {
        chain_id: chain.to_owned(),
    }
    .into())
}

fn receipt(path: &'static str, captured: &Value, observed: &Value, gap: Option<f64>) {
    seams::count(path, 1);
    seams::catalogue_receipt(
        json!({"path":path,"captured":captured,"observed":observed,"admission_to_snapshot_ms":gap}),
    );
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn collect(
    connection: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
    keyset: Option<&HistoryKeyset<'_>>,
    identity: Option<&str>,
    membership: &mut Membership,
    output: &mut Accumulator,
) -> Result<()> {
    if output.complete() || filter.match_no_events {
        return Ok(());
    }
    let mut lead = Side::Address;
    // A legacy row-only cursor is checked against its exact bucket and all membership reasons.
    // Applying its identity inside each limited probe prevents a prefix from hiding it.
    let mut bucket = if let Some(identity) = identity {
        let block: Option<Option<i64>> = sqlx::query_scalar(
            "SELECT block_number FROM normalized_events WHERE event_identity=$1",
        )
        .bind(identity)
        .fetch_optional(&mut *connection)
        .await?;
        block.map(|block| {
            block.map_or(contract::NULL_BUCKET, |block| {
                block / contract::BUCKET_BLOCKS
            })
        })
    } else if let Some(keyset) = keyset {
        Some(keyset.block_number.map_or(contract::NULL_BUCKET, |block| {
            block / contract::BUCKET_BLOCKS
        }))
    } else {
        seek(connection, read, filter, None, &mut lead).await?
    };
    let mut internal_cursor: Option<HistoryCursor> = None;
    while let Some(current) = bucket {
        seams::count("catalogue_bucket_batches", 1);
        let internal_keyset = internal_cursor.as_ref().map(|cursor| HistoryKeyset {
            cursor,
            block_number: cursor
                .position
                .as_ref()
                .and_then(|position| position.block_number),
        });
        let limit = output.candidate_limit();
        let mut query = QueryBuilder::<Postgres>::new(
            "DECLARE address_history_candidates NO SCROLL CURSOR FOR ",
        );
        catalogue_source::push_candidate_query(
            &mut query,
            read,
            filter,
            internal_keyset.as_ref().or(keyset),
            current,
            identity,
            limit as i64,
        );
        query
            .build()
            .persistent(false)
            .execute(&mut *connection)
            .await
            .context("failed to open catalogue address-history candidates")?;
        let batch = walk::consume_cursor(connection, read, filter, membership, output).await?;
        seams::count("catalogue_candidate_events", batch.events);
        if output.complete() || identity.is_some() {
            break;
        }
        if batch.events == limit {
            internal_cursor = batch.last;
        } else {
            bucket = seek(connection, read, filter, Some(current), &mut lead).await?;
            // A new bucket already lies after the last complete event. Keep the original
            // public keyset; it is harmless there and avoids fabricating a bucket cursor.
            internal_cursor = None;
        }
    }
    Ok(())
}

/// The side of the contract leapfrog that found the bucket the walk last probed.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Side {
    Address,
    Emitter,
}

/// The next bucket after `after` that can hold a row, or the first one.
///
/// With `contract_address`, a bucket holds a row only if the address has a source there and the
/// contract emitted an event there. The address side seeks without the emitter filter, so each
/// of its probes stops at its source's next readable row. The emitter side seeks the contract's
/// own index. Each seek starts at or past the other side's bucket and moves strictly on, and the
/// first bucket both reach is returned. So the seeks grow with the sparser side's buckets, not
/// with the address's. Every bucket skipped lacks one side, so it holds no row of the page.
async fn seek(
    connection: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
    after: Option<i64>,
    lead: &mut Side,
) -> Result<Option<i64>> {
    let Some(emitter) = filter
        .contract_address
        .as_deref()
        .filter(|_| seams::contract_leapfrog())
    else {
        return address_seek(connection, read, filter, after).await;
    };
    let unfiltered = EventHistoryReadFilter {
        contract_address: None,
        ..filter.clone()
    };
    // The side that found the last probed bucket leads again: it is the one that jumped there.
    let (mut found, mut side) = match (after, *lead) {
        (Some(after), Side::Emitter) => {
            let next = match filter.order {
                HistoryOrder::Desc if after == contract::NULL_BUCKET => return Ok(None),
                HistoryOrder::Desc => after - 1,
                HistoryOrder::Asc => after + 1,
            };
            (
                emitter_seek(connection, emitter, filter, next).await?,
                Side::Emitter,
            )
        }
        _ => (
            address_seek(connection, read, &unfiltered, after).await?,
            Side::Address,
        ),
    };
    while let Some(bucket) = found {
        let other = match side {
            Side::Address => emitter_seek(connection, emitter, filter, bucket).await?,
            Side::Emitter => {
                // The address seek is strictly after its bound, so the bound is the bucket
                // before this one in page order.
                let bound = match filter.order {
                    HistoryOrder::Desc => Some(bucket + 1),
                    HistoryOrder::Asc if bucket == contract::NULL_BUCKET => None,
                    HistoryOrder::Asc => Some(bucket - 1),
                };
                address_seek(connection, read, &unfiltered, bound).await?
            }
        };
        if other == Some(bucket) {
            *lead = side;
            return Ok(Some(bucket));
        }
        found = other;
        side = match side {
            Side::Address => Side::Emitter,
            Side::Emitter => Side::Address,
        };
    }
    Ok(None)
}

async fn address_seek(
    connection: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
    after: Option<i64>,
) -> Result<Option<i64>> {
    let mut query = QueryBuilder::<Postgres>::new("");
    catalogue_source::push_seek_query(&mut query, read, filter, after);
    seams::count("catalogue_bucket_seeks", 1);
    query
        .build_query_scalar()
        .persistent(false)
        .fetch_one(connection)
        .await
        .context("failed to seek an address-history catalogue bucket")
}

/// The contract's nearest bucket at or past `bucket` in page order.
async fn emitter_seek(
    connection: &mut PgConnection,
    emitter: &str,
    filter: &EventHistoryReadFilter,
    bucket: i64,
) -> Result<Option<i64>> {
    let mut query = QueryBuilder::<Postgres>::new("");
    push_emitter_seek_query(&mut query, emitter, filter, bucket);
    seams::count("catalogue_bucket_seeks", 1);
    seams::count("catalogue_emitter_seeks", 1);
    query
        .build_query_scalar()
        .persistent(false)
        .fetch_one(connection)
        .await
        .context("failed to seek the requested contract's next event")
}

/// The contract's nearest bucket at or past `bucket`, by the leading keys of
/// `normalized_events_emitter_history_idx` and the terms that index is partial on, bounded by the
/// read's blocks. Rows without a block are the null bucket, last when descending and first when
/// ascending. Namespace, chain, visibility and every other filter stay in the bucket probes, so
/// a bucket found here may hold no row, but no bucket that holds one is passed.
pub(super) fn push_emitter_seek_query<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    emitter: &'a str,
    filter: &EventHistoryReadFilter,
    bucket: i64,
) {
    let null = bucket == contract::NULL_BUCKET;
    match filter.order {
        HistoryOrder::Desc if null => {
            query.push("SELECT ");
            push_emitter_null(query, emitter);
        }
        HistoryOrder::Desc => {
            query.push("SELECT COALESCE(");
            push_emitter_block(query, emitter, filter, bucket);
            query.push(", ");
            push_emitter_null(query, emitter);
            query.push(")");
        }
        HistoryOrder::Asc if null => {
            query.push("SELECT COALESCE(");
            push_emitter_null(query, emitter);
            query.push(", ");
            push_emitter_block(query, emitter, filter, 0);
            query.push(")");
        }
        HistoryOrder::Asc => {
            query.push("SELECT ");
            push_emitter_block(query, emitter, filter, bucket);
        }
    }
}

fn push_emitter_rows<'a>(query: &mut QueryBuilder<'a, Postgres>, emitter: &'a str) {
    query
        .push(" FROM normalized_events ne WHERE lower(ne.raw_fact_ref ->> 'emitting_address') = ")
        .push_bind(emitter)
        .push(" AND ne.raw_fact_ref ->> 'emitting_address' IS NOT NULL AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')");
}

fn push_emitter_null<'a>(query: &mut QueryBuilder<'a, Postgres>, emitter: &'a str) {
    query.push(format!("(SELECT {}::bigint", contract::NULL_BUCKET));
    push_emitter_rows(query, emitter);
    query.push(" AND ne.block_number IS NULL LIMIT 1)");
}

/// The contract's nearest positioned bucket at or past `bucket`, which is not the null bucket.
fn push_emitter_block<'a>(
    query: &mut QueryBuilder<'a, Postgres>,
    emitter: &'a str,
    filter: &EventHistoryReadFilter,
    bucket: i64,
) {
    query.push(format!(
        "(SELECT ne.block_number / {}",
        contract::BUCKET_BLOCKS
    ));
    push_emitter_rows(query, emitter);
    let start = bucket.saturating_mul(contract::BUCKET_BLOCKS);
    let direction = match filter.order {
        HistoryOrder::Desc => {
            query
                .push(" AND ne.block_number <= ")
                .push_bind(start.saturating_add(contract::BUCKET_BLOCKS - 1));
            "DESC NULLS LAST"
        }
        HistoryOrder::Asc => {
            query.push(" AND ne.block_number >= ").push_bind(start);
            "ASC NULLS FIRST"
        }
    };
    let (minimum, maximum) = catalogue_source::block_window(filter);
    if let Some(minimum) = minimum {
        query.push(" AND ne.block_number >= ").push_bind(minimum);
    }
    if let Some(maximum) = maximum {
        query.push(" AND ne.block_number <= ").push_bind(maximum);
    }
    query.push(format!(" ORDER BY ne.block_number {direction} LIMIT 1)"));
}
