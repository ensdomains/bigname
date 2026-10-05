//! Incremental shared source envelopes. Existing sources inspect only the new Project range;
//! a newly reached key also discovers its older retained writes before publication.
use std::collections::BTreeSet;

use bigname_storage::history_catalogue_contract::{event_key_bloom_sql, event_kind_mask_sql};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use super::{
    super::{input::BlockHeader, tables::HISTORY_SOURCE},
    write,
};
use crate::{ProjectError, Result};

#[derive(Clone, Debug, sqlx::FromRow, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Key {
    pub source_kind: i16,
    pub source_key: String,
    pub resolver_address: String,
}

pub(super) async fn incoming(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    after: i64,
    number: i64,
    names: &[String],
    resources: &[Uuid],
) -> Result<BTreeSet<Key>> {
    let values: Vec<Key> = sqlx::query_as(
        "/* project:history.incoming_sources */
        SELECT DISTINCT source.* FROM normalized_events ne
        CROSS JOIN LATERAL (
            SELECT 0::smallint AS source_kind, ne.logical_name_id AS source_key,
                   ''::text AS resolver_address WHERE ne.logical_name_id IS NOT NULL
            UNION ALL SELECT 1::smallint, ne.resource_id::text, '' WHERE ne.resource_id IS NOT NULL
            UNION ALL SELECT 2::smallint, lower(ne.after_state->>'node'), ''
            WHERE ne.logical_name_id IS NULL AND ne.after_state->>'node' IS NOT NULL
              AND ne.event_kind IN ('RecordChanged','RecordVersionChanged')
              AND ne.source_family IN ('ens_v1_resolver_l1','ens_v2_resolver_l1','basenames_base_resolver')
            UNION ALL SELECT 3::smallint, ne.after_state->>'resolver_record_id', lower(ne.after_state->>'resolver')
            WHERE ne.event_kind='RecordChanged' AND ne.after_state->>'storage_model'='resolver_record_id'
              AND ne.after_state->>'resolver_record_id' IS NOT NULL
              AND ne.after_state->>'resolver' IS NOT NULL
        ) source
        WHERE ne.chain_id=$1 AND ((ne.block_number>$2 AND ne.block_number<=$3)
            OR ($2=-1 AND ne.block_number IS NULL))
          AND ne.consumer_visibility='activated'
          AND ne.canonicality_state IN ('canonical','safe','finalized')",
    ).bind(chain).bind(after).bind(number).fetch_all(&mut **transaction).await
        .map_err(|e| ProjectError::database("failed to read incoming history sources", e))?;
    let mut keys: BTreeSet<Key> = values.into_iter().collect();
    keys.extend(names.iter().map(|name| Key {
        source_kind: 0,
        source_key: name.clone(),
        resolver_address: String::new(),
    }));
    keys.extend(resources.iter().map(|resource| Key {
        source_kind: 1,
        source_key: resource.to_string(),
        resolver_address: String::new(),
    }));
    Ok(keys)
}

pub(super) struct Changed {
    pub keys: Vec<Value>,
    pub rows: u64,
    pub undo_rows: u64,
}

pub(super) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    block: &BlockHeader,
    after: i64,
    keys: &BTreeSet<Key>,
) -> Result<Changed> {
    let mut result = Changed {
        keys: Vec::new(),
        rows: 0,
        undo_rows: 0,
    };
    let predicates = [
        "ne.logical_name_id=wanted.source_key",
        "ne.resource_id=wanted.source_key::uuid",
        "lower(ne.after_state->>'node')=wanted.source_key AND ne.logical_name_id IS NULL
         AND ne.after_state->>'node' IS NOT NULL AND ne.event_kind IN ('RecordChanged','RecordVersionChanged')
         AND ne.source_family IN ('ens_v1_resolver_l1','ens_v2_resolver_l1','basenames_base_resolver')",
        "lower(ne.after_state->>'resolver')=wanted.resolver_address
         AND ne.after_state->>'resolver_record_id'=wanted.source_key
         AND ne.event_kind='RecordChanged' AND ne.after_state->>'storage_model'='resolver_record_id'",
    ];
    for (kind, predicate) in predicates.into_iter().enumerate() {
        let wanted: Vec<&Key> = keys
            .iter()
            .filter(|key| usize::try_from(key.source_kind).ok() == Some(kind))
            .collect();
        for chunk in wanted.chunks(1000) {
            let input = Value::Array(
                chunk
                    .iter()
                    .map(|key| {
                        serde_json::json!({
                "source_kind":key.source_kind,"source_key":key.source_key,
                "resolver_address":key.resolver_address})
                    })
                    .collect(),
            );
            let before: Vec<Value> = sqlx::query_scalar(
                "/* project:history.source_before */ SELECT to_jsonb(source)
                FROM jsonb_to_recordset($2) wanted(source_kind smallint,source_key text,resolver_address text)
                JOIN project_history_source source ON source.chain_id=$1
                  AND source.source_kind=wanted.source_kind AND source.source_key=wanted.source_key
                  AND source.resolver_address=wanted.resolver_address",
            ).bind(chain).bind(&input).fetch_all(&mut **transaction).await
                .map_err(|e| ProjectError::database("failed to read history source before-images", e))?;
            let sql = format!("/* project:history.source_envelopes */
                SELECT to_jsonb(fresh) FROM jsonb_to_recordset($2)
                    wanted(source_kind smallint,source_key text,resolver_address text)
                LEFT JOIN project_history_source prior ON prior.chain_id=$1
                  AND prior.source_kind=wanted.source_kind AND prior.source_key=wanted.source_key
                  AND prior.resolver_address=wanted.resolver_address
                CROSS JOIN LATERAL (
                    SELECT min(COALESCE(ne.block_number/256,-1)) AS first_bucket,
                           max(COALESCE(ne.block_number/256,-1)) AS last_bucket,
                           bit_or({kind_mask}) AS event_mask, bit_or({bloom}) AS key_bloom
                    FROM normalized_events ne
                    WHERE ne.chain_id=$1 AND {predicate}
                      AND (ne.block_number<=$3 OR ne.block_number IS NULL)
                      AND (prior.chain_id IS NULL OR ne.block_number>$4)
                      AND ne.consumer_visibility='activated'
                      AND ne.canonicality_state IN ('canonical','safe','finalized')
                      AND (ne.block_hash IS NULL OR EXISTS (SELECT 1 FROM chain_lineage lineage
                        WHERE lineage.chain_id=ne.chain_id AND lineage.block_hash=ne.block_hash
                          AND lineage.canonicality_state IN ('canonical','safe','finalized')))
                ) observed
                CROSS JOIN LATERAL (SELECT least(prior.first_bucket,observed.first_bucket) AS first_bucket,
                    greatest(prior.last_bucket,observed.last_bucket) AS last_bucket,
                    COALESCE(prior.event_mask,0)|COALESCE(observed.event_mask,0) AS event_mask,
                    COALESCE(prior.key_bloom,B'0'::bit(256))|COALESCE(observed.key_bloom,B'0'::bit(256)) AS key_bloom) merged
                CROSS JOIN LATERAL (SELECT $1::text AS chain_id,wanted.source_kind,wanted.source_key,
                    wanted.resolver_address,merged.first_bucket,merged.last_bucket,
                    CASE WHEN merged.first_bucket IS NULL THEN 'empty'::int8range
                         ELSE int8range(merged.first_bucket,merged.last_bucket+1,'[)') END AS bucket_range,
                    merged.event_mask,merged.key_bloom) fresh",
                kind_mask=event_kind_mask_sql("ne.event_kind"),bloom=event_key_bloom_sql("ne"));
            let fresh: Vec<Value> = sqlx::query_scalar(&sql)
                .bind(chain)
                .bind(&input)
                .bind(block.number)
                .bind(after)
                .fetch_all(&mut **transaction)
                .await
                .map_err(|e| {
                    ProjectError::database("failed to build history source envelopes", e)
                })?;
            let row_key = |row: &Value| {
                super::super::store::key_text(
                    &HISTORY_SOURCE,
                    row.as_object().expect("SQL row is an object"),
                )
            };
            let before_map: std::collections::BTreeMap<_, _> =
                before.iter().map(|row| (row_key(row), row)).collect();
            for row in &fresh {
                if before_map
                    .get(&row_key(row))
                    .is_none_or(|before| *before != row)
                {
                    result.keys.push(row.clone());
                }
            }
            let (rows, undo) =
                write::replace(transaction, chain, block, &HISTORY_SOURCE, before, fresh).await?;
            result.rows += rows;
            result.undo_rows += undo;
        }
    }
    Ok(result)
}
