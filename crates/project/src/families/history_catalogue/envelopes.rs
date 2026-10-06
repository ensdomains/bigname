//! Chain-owned pruning envelopes. Membership rows receive final bounds before insertion;
//! source-only changes journal and replace the dependent anchors that actually change.
use super::{
    super::{input::BlockHeader, tables::HISTORY_ANCHOR},
    write,
};
use crate::{ProjectError, Result};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

/// Use the same source aggregate before a membership write, avoiding an empty-envelope
/// insertion followed by a second physical row/index write in every rebuild generation.
pub(super) async fn fill(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    kind: i16,
    rows: Vec<Value>,
) -> Result<Vec<Value>> {
    let mut result = Vec::with_capacity(rows.len());
    let sql = format!(
        "/* project:history.fill_anchor_envelopes */ SELECT {fresh}
        FROM jsonb_populate_recordset(NULL::project_address_history_anchor,$3) anchor
        {bounds}",
        fresh = FRESH,
        bounds = bounds(kind)
    );
    for chunk in rows.chunks(256) {
        let rows: Vec<Value> = sqlx::query_scalar(&sql)
            .bind(chain)
            .bind(kind)
            .bind(Value::Array(chunk.to_vec()))
            .fetch_all(&mut **transaction)
            .await
            .map_err(|e| {
                ProjectError::database("failed to fill history membership envelopes", e)
            })?;
        result.extend(rows);
    }
    Ok(result)
}

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    names: &[String],
    resources: &[Uuid],
) -> Result<(u64, u64)> {
    let (mut written, mut undo) = (0, 0);
    let resource_ids: Vec<_> = resources.iter().map(Uuid::to_string).collect();
    for (kind, keys) in [(0_i16, names), (1_i16, resource_ids.as_slice())] {
        let sql=format!("/* project:history.anchor_envelopes */ SELECT to_jsonb(anchor),{fresh}
            FROM project_address_history_anchor anchor {bounds}
            WHERE anchor.chain_id=$1 AND anchor.anchor_kind=$2 AND anchor.anchor_id=ANY($3)
              AND ROW(anchor.first_bucket,anchor.last_bucket,anchor.event_mask,anchor.key_bloom)
                IS DISTINCT FROM ROW(bounds.first_bucket,bounds.last_bucket,bounds.event_mask,bounds.key_bloom)",
            fresh=FRESH,bounds=bounds(kind));
        for chunk in keys.chunks(256) {
            let pairs: Vec<(Value, Value)> = sqlx::query_as(&sql)
                .bind(chain)
                .bind(kind)
                .bind(chunk)
                .fetch_all(&mut **transaction)
                .await
                .map_err(|e| {
                    ProjectError::database("failed to derive history anchor envelopes", e)
                })?;
            let (before, fresh) = pairs.into_iter().unzip();
            let (rows, journal) =
                write::replace(transaction, chain, block, &HISTORY_ANCHOR, before, fresh).await?;
            written += rows;
            undo += journal;
        }
    }
    Ok((written, undo))
}
const FRESH: &str = "to_jsonb(anchor)||jsonb_build_object(
    'first_bucket',bounds.first_bucket,'last_bucket',bounds.last_bucket,
    'bucket_range',CASE WHEN bounds.first_bucket IS NULL THEN 'empty'::int8range
      ELSE int8range(bounds.first_bucket,bounds.last_bucket+1,'[)') END,
    'event_mask',bounds.event_mask,'key_bloom',bounds.key_bloom)";
fn bounds(kind: i16) -> String {
    format!(
        "CROSS JOIN LATERAL (
      SELECT min(source.first_bucket) AS first_bucket,max(source.last_bucket) AS last_bucket,
        COALESCE(bit_or(source.event_mask),0) AS event_mask,
        COALESCE(bit_or(source.key_bloom),B'0'::bit(256)) AS key_bloom FROM (
          SELECT first_bucket,last_bucket,event_mask,key_bloom FROM project_history_source
          WHERE chain_id=$1 AND source_kind=$2 AND source_key=anchor.anchor_id
          {edges}
        ) source
    ) bounds",
        edges = if kind == 1 {
            "UNION ALL SELECT first_bucket,last_bucket,event_mask,key_bloom
        FROM project_history_source_edge WHERE chain_id=$1 AND resource_id=anchor.anchor_id::uuid"
        } else {
            ""
        }
    )
}
