//! The reads a rebuild range makes once for all its blocks, each grouped back by block so every
//! block sees what its own reads would return: the readable lineage rows, the activated
//! canonical events at each block's readable hash in the canonical order with duplicates dropped
//! per block (input.rs `block_events`), the surface bindings (identity.rs `block_bindings`) and
//! the resolver activations under each block's own manifest set (classification.rs
//! `activated`).
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use crate::{
    ProjectError, Result,
    families::{
        classification,
        input::{self, BlockEvent, BlockHeader, EventRow},
        manifests::{self, History},
        store::Row,
    },
};

/// The readable lineage rows of `numbers`, in their order; every one must be readable.
pub(super) async fn headers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    numbers: &[i64],
) -> Result<Vec<BlockHeader>> {
    if numbers.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(i64, String, Option<String>, i64, Value)> = sqlx::query_as(
        "/* project:families.range.headers */ SELECT lineage.block_number, lineage.block_hash,
                COALESCE(lineage.parent_hash, (
                    SELECT previous.block_hash FROM chain_lineage previous
                    WHERE previous.chain_id = lineage.chain_id
                      AND previous.block_number = lineage.block_number - 1
                      AND previous.canonicality_state IN ('canonical', 'safe', 'finalized')
                )),
                extract(epoch FROM lineage.block_timestamp)::bigint,
                to_jsonb(lineage.block_timestamp)
         FROM chain_lineage lineage
         WHERE lineage.chain_id = $1 AND lineage.block_number = ANY($2::bigint[])
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(numbers)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read family range lineage", error))?;
    let mut read: BTreeMap<i64, BlockHeader> = rows
        .into_iter()
        .map(
            |(number, hash, predecessor_hash, timestamp_seconds, timestamp)| {
                let header = BlockHeader {
                    number,
                    hash,
                    predecessor_hash,
                    timestamp_seconds,
                    timestamp,
                };
                (number, header)
            },
        )
        .collect();
    numbers
        .iter()
        .map(|number| {
            read.remove(number).ok_or_else(|| {
                ProjectError::transient(format!(
                    "family block {number} of chain {chain_id} is not readable"
                ))
            })
        })
        .collect()
}

/// The blocks' numbers and readable hashes, bound as two arrays.
fn arrays(blocks: &[BlockHeader]) -> (Vec<i64>, Vec<String>) {
    blocks
        .iter()
        .map(|block| (block.number, block.hash.clone()))
        .unzip()
}

/// An event row of `events`: its block number, then the columns of `block_events`.
type RangeEventRow = (
    i64,
    i64,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Value,
    Value,
    Value,
);

fn split(row: RangeEventRow) -> (i64, EventRow) {
    let (block, id, identity, namespace, name, resource, kind, family, manifest, hash) = (
        row.0, row.1, row.2, row.3, row.4, row.5, row.6, row.7, row.8, row.9,
    );
    let (index, log, before, after, raw) = (row.10, row.11, row.12, row.13, row.14);
    (
        block,
        (
            id, identity, namespace, name, resource, kind, family, manifest, hash, index, log,
            before, after, raw,
        ),
    )
}

/// Each block's activated canonical events at its readable hash, in the canonical event order,
/// each event identity once within its block, with the count of disagreeing duplicate
/// deliveries that block dropped.
pub(super) async fn events(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    blocks: &[BlockHeader],
) -> Result<Vec<(Vec<BlockEvent>, u64)>> {
    let (numbers, hashes) = arrays(blocks);
    let rows: Vec<RangeEventRow> = sqlx::query_as(
        "/* project:families.range.events */ SELECT event.block_number,
                event.normalized_event_id, event.event_identity, event.namespace,
                event.logical_name_id, event.resource_id::text, event.event_kind,
                event.source_family, event.source_manifest_id, event.transaction_hash,
                event.transaction_index, event.log_index, event.before_state,
                event.after_state, event.raw_fact_ref
         FROM unnest($2::bigint[], $3::text[]) AS block (number, hash)
         JOIN normalized_events event
           ON event.chain_id = $1 AND event.block_number = block.number
          AND event.block_hash = block.hash
         WHERE event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(&numbers)
    .bind(&hashes)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read family range events", error))?;
    let mut by_block: BTreeMap<i64, Vec<BlockEvent>> = BTreeMap::new();
    for row in rows {
        let (block, row) = split(row);
        by_block
            .entry(block)
            .or_default()
            .push(input::event(block, row));
    }
    Ok(blocks
        .iter()
        .map(|block| {
            let mut events = by_block.remove(&block.number).unwrap_or_default();
            let anomalies = input::order(&mut events);
            (events, anomalies)
        })
        .collect())
}

/// How many activated canonical events each block holds at its readable hash, before duplicates
/// are dropped, to size a range before reading its events.
pub(super) async fn event_counts(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    blocks: &[BlockHeader],
) -> Result<BTreeMap<i64, u64>> {
    let (numbers, hashes) = arrays(blocks);
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "/* project:families.range.event_counts */ SELECT block.number, count(*)
         FROM unnest($2::bigint[], $3::text[]) AS block (number, hash)
         JOIN normalized_events event
           ON event.chain_id = $1 AND event.block_number = block.number
          AND event.block_hash = block.hash
         WHERE event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
         GROUP BY block.number",
    )
    .bind(chain_id)
    .bind(&numbers)
    .bind(&hashes)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to count family range events", error))?;
    Ok(rows
        .into_iter()
        .map(|(number, count)| (number, u64::try_from(count).unwrap_or(0)))
        .collect())
}

/// Each block's readable surface bindings, as `to_jsonb` rows.
pub(super) async fn bindings(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    blocks: &[BlockHeader],
) -> Result<BTreeMap<i64, Vec<Row>>> {
    let (numbers, hashes) = arrays(blocks);
    let rows: Vec<(i64, Value)> = sqlx::query_as(
        "/* project:families.range.bindings */ SELECT binding.block_number, to_jsonb(binding)
         FROM unnest($2::bigint[], $3::text[]) AS block (number, hash)
         JOIN surface_bindings binding
           ON binding.chain_id = $1 AND binding.block_number = block.number
          AND binding.block_hash = block.hash
         WHERE binding.canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(&numbers)
    .bind(&hashes)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read family range bindings", error))?;
    let mut by_block: BTreeMap<i64, Vec<Row>> = BTreeMap::new();
    for (block, binding) in rows {
        if let Value::Object(binding) = binding {
            by_block.entry(block).or_default().push(binding);
        }
    }
    Ok(by_block)
}

/// Each block's resolvers whose discovered candidates can change there: a resolver edge or its
/// target's contract address that starts or stops at the block, or a declaration of the
/// manifest set active at the block whose start block it is. The blocks are grouped by their
/// active set, so each set's payloads are bound once.
pub(super) async fn activated(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    blocks: &[BlockHeader],
    history: &History,
) -> Result<BTreeMap<i64, BTreeSet<String>>> {
    let numbers: Vec<i64> = blocks.iter().map(|block| block.number).collect();
    let mut sets: BTreeMap<String, (Value, Vec<i64>)> = BTreeMap::new();
    for number in &numbers {
        let set = history.at(*number);
        sets.entry(set.key)
            .or_insert_with(|| (set.rows, Vec::new()))
            .1
            .push(*number);
    }
    let sets: Vec<Value> = sets
        .into_values()
        .map(|(rows, blocks)| json!({"rows": rows, "blocks": blocks}))
        .collect();
    let manifests = manifests::recordset("active -> 'rows'");
    let rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        "/* project:families.range.activated */ WITH blocks AS (
             SELECT number FROM unnest($2::bigint[]) AS block (number)
         ),
         edges AS (
             SELECT block.number, edge.chain_id, edge.to_contract_instance_id
             FROM blocks block
             JOIN discovery_edges edge
               ON edge.chain_id = $1 AND edge.edge_kind = 'resolver'
              AND edge.active_from_block_number = block.number
             UNION
             SELECT block.number, edge.chain_id, edge.to_contract_instance_id
             FROM blocks block
             JOIN discovery_edges edge
               ON edge.chain_id = $1 AND edge.edge_kind = 'resolver'
              AND edge.active_to_block_number = block.number
         )
         SELECT edge.number, lower(address.address) FROM edges edge
         JOIN contract_instance_addresses address
           ON address.contract_instance_id = edge.to_contract_instance_id
          AND address.chain_id = edge.chain_id
         UNION
         SELECT block.number, lower(address.address)
         FROM blocks block
         JOIN contract_instance_addresses address
           ON address.chain_id = $1
          AND (address.active_from_block_number = block.number
               OR address.active_to_block_number = block.number)
         WHERE EXISTS (
             SELECT 1 FROM discovery_edges edge
             WHERE edge.chain_id = address.chain_id AND edge.edge_kind = 'resolver'
               AND edge.to_contract_instance_id = address.contract_instance_id
         )
         UNION
         SELECT start.number, lower(declaration ->> 'address')
         FROM jsonb_array_elements($3::jsonb) active
         CROSS JOIN LATERAL {manifests}
         CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
             manifest.manifest_payload -> 'contracts', '[]'::jsonb)) declaration
         CROSS JOIN LATERAL (
             SELECT block.value::bigint AS number
             FROM jsonb_array_elements_text(active -> 'blocks') block
         ) start
         WHERE declaration ->> 'start_block' ~ '^[0-9]+$'
           AND (declaration ->> 'start_block')::bigint = start.number"
    ))
    .bind(chain_id)
    .bind(&numbers)
    .bind(Value::Array(sets))
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to read range resolver activations", error))?;
    let mut by_block: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
    for (block, address) in rows {
        if let Some(address) = classification::resolver(Some(address)) {
            by_block.entry(block).or_default().insert(address);
        }
    }
    Ok(by_block)
}
