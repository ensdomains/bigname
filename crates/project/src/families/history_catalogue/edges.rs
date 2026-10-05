//! Conservative resolver source discovery, retaining pointer/link provenance once per source.
//! Exact attribution remains in the reader and may admit records older than the pointer.
use std::collections::BTreeSet;

use bigname_storage::history_catalogue_contract::event_kind_mask;
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{
    super::{input::BlockHeader, tables::HISTORY_EDGE},
    prepare,
    sources::Key,
    write,
};
use crate::{ProjectError, Result};

pub(super) async fn discover(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    number: i64,
    resources: &[Uuid],
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for chunk in resources.chunks(256) {
        rows.extend(
            sqlx::query_scalar::<_, Value>(DISCOVER)
                .bind(chain)
                .bind(number)
                .bind(chunk)
                .fetch_all(&mut **transaction)
                .await
                .map_err(|e| {
                    ProjectError::database("failed to discover history resolver sources", e)
                })?,
        );
    }
    Ok(rows)
}

pub(super) fn source_keys(edges: &[Value]) -> BTreeSet<Key> {
    edges
        .iter()
        .map(|row| Key {
            source_kind: row["source_kind"].as_i64().expect("edge kind") as i16,
            source_key: row["source_key"].as_str().expect("edge key").to_owned(),
            resolver_address: row["source_resolver"]
                .as_str()
                .expect("edge resolver")
                .to_owned(),
        })
        .collect()
}

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    raw: Vec<Value>,
    rediscovered: &[Uuid],
    changed_sources: &[Value],
) -> Result<(Vec<Uuid>, u64, u64)> {
    let mut resources: BTreeSet<Uuid> = rediscovered.iter().copied().collect();
    resources.extend(
        prepare::dependent_resources(transaction, chain, Value::Array(changed_sources.to_vec()))
            .await?,
    );
    let resources: Vec<_> = resources.into_iter().collect();
    let mut written = 0;
    let mut undo = 0;
    for chunk in resources.chunks(256) {
        let before: Vec<Value>=sqlx::query_scalar(
            "/* project:history.edge_before */ SELECT to_jsonb(edge)
             FROM project_history_source_edge edge WHERE edge.chain_id=$1 AND edge.resource_id=ANY($2)")
            .bind(chain).bind(chunk).fetch_all(&mut **transaction).await
            .map_err(|e| ProjectError::database("failed to read history source edges",e))?;
        let mut metadata: Vec<Value> = before
            .iter()
            .filter(|row| {
                let id: Uuid = row["resource_id"]
                    .as_str()
                    .expect("edge resource")
                    .parse()
                    .expect("UUID");
                !rediscovered.contains(&id)
            })
            .cloned()
            .collect();
        metadata.extend(
            raw.iter()
                .filter(|row| {
                    let id: Uuid = row["resource_id"]
                        .as_str()
                        .expect("edge resource")
                        .parse()
                        .expect("UUID");
                    chunk.contains(&id)
                })
                .cloned(),
        );
        let fresh: Vec<Value> = sqlx::query_scalar(SHAPE)
            .bind(Value::Array(metadata))
            .bind(event_kind_mask("ResolverRecordLinked"))
            .fetch_all(&mut **transaction)
            .await
            .map_err(|e| ProjectError::database("failed to shape history edge envelopes", e))?;
        let (count, journal) =
            write::replace(transaction, chain, block, &HISTORY_EDGE, before, fresh).await?;
        written += count;
        undo += journal;
    }
    Ok((resources, written, undo))
}

const DISCOVER:&str="/* project:history.resolver_edges */ WITH pointers AS (
    SELECT DISTINCT pointer.chain_id,pointer.resource_id,pointer.event_identity AS pointer_event_identity,
        pointer.block_number AS pointer_block_number,lower(surface.namehash) AS node,
        COALESCE(lower(pointer.after_state->>'resolver'),'') AS pointer_resolver
    FROM unnest($3::uuid[]) wanted(resource)
    CROSS JOIN LATERAL (SELECT pointer.* FROM normalized_events pointer
        WHERE pointer.resource_id=wanted.resource AND pointer.chain_id=$1
          AND pointer.event_kind='ResolverChanged'
          AND pointer.consumer_visibility='activated'
          AND pointer.canonicality_state IN ('canonical','safe','finalized')
          AND ((pointer.block_number IS NULL AND pointer.block_hash IS NULL) OR
            (pointer.block_number<=$2 AND EXISTS(SELECT 1 FROM chain_lineage lineage
                WHERE lineage.chain_id=pointer.chain_id AND lineage.block_hash=pointer.block_hash
                  AND lineage.block_number=pointer.block_number
                  AND lineage.canonicality_state IN ('canonical','safe','finalized'))))
        OFFSET 0) pointer
    JOIN name_surfaces surface ON surface.chain_id=pointer.chain_id
      AND surface.logical_name_id=pointer.logical_name_id
      AND surface.block_number<=$2 AND surface.canonicality_state IN ('canonical','safe','finalized')
      AND EXISTS(SELECT 1 FROM chain_lineage lineage
        WHERE lineage.chain_id=surface.chain_id AND lineage.block_hash=surface.block_hash
          AND lineage.block_number=surface.block_number
          AND lineage.canonicality_state IN ('canonical','safe','finalized'))
), edges AS (
    SELECT pointer.chain_id,pointer.resource_id,2::smallint AS source_kind,pointer.node AS source_key,
        ''::text AS source_resolver,pointer.pointer_event_identity,''::text AS link_event_identity,
        pointer.pointer_resolver,pointer.node,pointer.pointer_block_number,NULL::bigint AS link_block_number
    FROM pointers pointer
    UNION
    SELECT pointer.chain_id,pointer.resource_id,3,COALESCE(link.after_state->>'resolver_record_id',''),
        lower(link.after_state->>'resolver'),pointer.pointer_event_identity,link.event_identity,
        pointer.pointer_resolver,pointer.node,pointer.pointer_block_number,link.block_number
    FROM pointers pointer
    CROSS JOIN LATERAL (SELECT pointer.node AS node UNION SELECT
        '0x0000000000000000000000000000000000000000000000000000000000000000') wanted
    CROSS JOIN LATERAL (SELECT link.* FROM normalized_events link
        WHERE link.chain_id=pointer.chain_id AND lower(link.after_state->>'resolver')=pointer.pointer_resolver
          AND lower(link.after_state->>'node')=wanted.node
          AND link.event_kind='ResolverRecordLinked' AND link.after_state->>'storage_model'='resolver_record_id'
          AND link.consumer_visibility='activated' AND link.canonicality_state IN ('canonical','safe','finalized')
          AND ((link.block_number IS NULL AND link.block_hash IS NULL) OR
            (link.block_number<=$2 AND EXISTS(SELECT 1 FROM chain_lineage lineage
                WHERE lineage.chain_id=link.chain_id AND lineage.block_hash=link.block_hash
                  AND lineage.block_number=link.block_number
                  AND lineage.canonicality_state IN ('canonical','safe','finalized'))))
        OFFSET 0) link
) SELECT to_jsonb(edge) FROM edges edge";

const SHAPE:&str="/* project:history.edge_envelopes */
    SELECT to_jsonb(fresh) FROM jsonb_populate_recordset(NULL::project_history_source_edge,$1) edge
    LEFT JOIN project_history_source source ON source.chain_id=edge.chain_id
      AND source.source_kind=edge.source_kind AND source.source_key=edge.source_key
      AND source.resolver_address=edge.source_resolver
    CROSS JOIN LATERAL (SELECT CASE WHEN edge.link_event_identity<>''
        THEN COALESCE(edge.link_block_number/256,-1) END AS link_bucket) linked
    CROSS JOIN LATERAL (SELECT least(source.first_bucket,linked.link_bucket) AS first_bucket,
        greatest(source.last_bucket,linked.link_bucket) AS last_bucket) bounds
    CROSS JOIN LATERAL (SELECT edge.chain_id,edge.resource_id,edge.source_kind,edge.source_key,
        edge.source_resolver,edge.pointer_event_identity,edge.link_event_identity,edge.pointer_resolver,
        edge.node,edge.pointer_block_number,edge.link_block_number,bounds.first_bucket,bounds.last_bucket,
        CASE WHEN bounds.first_bucket IS NULL THEN 'empty'::int8range
            ELSE int8range(bounds.first_bucket,bounds.last_bucket+1,'[)') END AS bucket_range,
        COALESCE(source.event_mask,0)|CASE WHEN linked.link_bucket IS NULL THEN 0 ELSE $2::bigint END AS event_mask,
        COALESCE(source.key_bloom,B'0'::bit(256)) AS key_bloom) fresh";
