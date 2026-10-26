//! Resolver source edges of the build's work resources: discovered at the pass block per
//! resource partition, then bounded by their finished sources.
use bigname_storage::history_catalogue_contract::event_kind_mask;
use sqlx::{Postgres, Transaction};

use super::{super::edges, database};
use crate::Result;

const EDGE: &str = "project_history_source_edge";

const CLEAR: &str = "/* project:history.build.clear_edges */
    DELETE FROM project_history_source_edge
    WHERE chain_id = $1 AND (hashtext(resource_id::text) & $3::int) = $2::int";

/// Discovery at the pass block `$2` for the work resources of key partition `$3`.
const DISCOVER: &str = edges::discover_sql!(
    "/* project:history.build.edges */ INSERT INTO project_history_source_edge
    (chain_id, resource_id, source_kind, source_key, source_resolver, pointer_event_identity,
     link_event_identity, pointer_resolver, node, pointer_block_number, link_block_number) WITH",
    "(SELECT work.source_key::uuid AS resource FROM project_compose_history_work work
        WHERE work.chain_id = $1 AND work.source_kind = 1 AND work.key_partition = $3::int
        GROUP BY work.source_key HAVING bool_or(work.work)) wanted",
    "edge.chain_id, edge.resource_id, edge.source_kind, edge.source_key,
    edge.source_resolver, edge.pointer_event_identity, edge.link_event_identity,
    edge.pointer_resolver, edge.node, edge.pointer_block_number, edge.link_block_number
    FROM edges edge"
);

/// The bounds of resource partition `$3` from the finished sources, `$2` a link's event-kind bit.
const BOUNDS: &str = concat!(
    "/* project:history.build.edge_bounds */ UPDATE project_history_source_edge target
    SET first_bucket = shaped.first_bucket, last_bucket = shaped.last_bucket,
        bucket_range = shaped.bucket_range, event_mask = shaped.event_mask,
        key_bloom = shaped.key_bloom
    FROM (SELECT fresh.* ",
    edges::shape!(
        "(SELECT * FROM project_history_source_edge own WHERE own.chain_id = $1
        AND (hashtext(own.resource_id::text) & $4::int) = $3::int) edge"
    ),
    ") shaped
    WHERE target.chain_id = shaped.chain_id AND target.resource_id = shaped.resource_id
      AND target.source_kind = shaped.source_kind AND target.source_key = shaped.source_key
      AND target.source_resolver = shaped.source_resolver
      AND target.pointer_event_identity = shaped.pointer_event_identity
      AND target.link_event_identity = shaped.link_event_identity"
);

pub(super) async fn discover(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    pass: i64,
    part: i32,
    mask: i32,
) -> Result<(&'static str, u64)> {
    sqlx::query(CLEAR)
        .bind(chain)
        .bind(part)
        .bind(mask)
        .execute(&mut **transaction)
        .await
        .map_err(database("failed to clear a partition of catalogue edges"))?;
    let rows = sqlx::query(DISCOVER)
        .bind(chain)
        .bind(pass)
        .bind(part)
        .execute(&mut **transaction)
        .await
        .map_err(database("failed to discover catalogue edges"))?
        .rows_affected();
    Ok((EDGE, rows))
}

pub(super) async fn bounds(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    part: i32,
    mask: i32,
) -> Result<(&'static str, u64)> {
    let rows = sqlx::query(BOUNDS)
        .bind(chain)
        .bind(event_kind_mask("ResolverRecordLinked"))
        .bind(part)
        .bind(mask)
        .execute(&mut **transaction)
        .await
        .map_err(database("failed to bound catalogue edges"))?
        .rows_affected();
    Ok((EDGE, rows))
}
