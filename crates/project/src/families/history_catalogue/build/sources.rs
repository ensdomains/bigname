//! Source rows of one key partition: the keys of the work table and the sources the finished
//! edges reach, each with its span partials merged as the incremental refresh merges a key's
//! prior row with its new events (`least`, `greatest`, `|`). A key with no event the catalogue
//! reads keeps an empty row.
use sqlx::{Postgres, Transaction};

use super::database;
use crate::Result;

const SOURCE: &str = "project_history_source";

const CLEAR: &str = "/* project:history.build.clear_sources */
    DELETE FROM project_history_source
    WHERE chain_id = $1 AND (hashtext(source_key) & $3::int) = $2::int";

const MERGE: &str = "/* project:history.build.sources */
    INSERT INTO project_history_source (chain_id, source_kind, source_key, resolver_address,
        first_bucket, last_bucket, bucket_range, event_mask, key_bloom)
    SELECT $1, keyed.source_kind, keyed.source_key, keyed.resolver_address, merged.first_bucket,
           merged.last_bucket,
           CASE WHEN merged.first_bucket IS NULL THEN 'empty'::int8range
                ELSE int8range(merged.first_bucket, merged.last_bucket + 1, '[)') END,
           COALESCE(merged.event_mask, 0), COALESCE(merged.key_bloom, B'0'::bit(256))
    FROM (
        SELECT work.source_kind, work.source_key, work.resolver_address
        FROM project_compose_history_work work
        WHERE work.chain_id = $1 AND work.key_partition = $2::int
        UNION
        SELECT edge.source_kind, edge.source_key, edge.source_resolver
        FROM project_history_source_edge edge
        WHERE edge.chain_id = $1 AND (hashtext(edge.source_key) & $3::int) = $2::int
    ) keyed
    CROSS JOIN LATERAL (
        SELECT min(work.first_bucket) AS first_bucket, max(work.last_bucket) AS last_bucket,
               bit_or(work.event_mask) AS event_mask, bit_or(work.key_bloom) AS key_bloom
        FROM project_compose_history_work work
        WHERE work.chain_id = $1 AND work.source_kind = keyed.source_kind
          AND work.source_key = keyed.source_key
          AND work.resolver_address = keyed.resolver_address AND work.part >= 0
    ) merged";

pub(super) async fn merge(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    part: i32,
    mask: i32,
) -> Result<(&'static str, u64)> {
    sqlx::query(CLEAR)
        .bind(chain)
        .bind(part)
        .bind(mask)
        .execute(&mut **transaction)
        .await
        .map_err(database("failed to clear a partition of catalogue sources"))?;
    let rows = sqlx::query(MERGE)
        .bind(chain)
        .bind(part)
        .bind(mask)
        .execute(&mut **transaction)
        .await
        .map_err(database("failed to merge catalogue sources"))?
        .rows_affected();
    Ok((SOURCE, rows))
}
