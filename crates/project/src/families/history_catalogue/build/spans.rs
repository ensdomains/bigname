//! The build's key set and its per-span partial aggregates, in `project_compose_history_work`.
//!
//! The names stream refreshed the catalogue once per chunk, with the chunk's composed names and
//! no window, and the windows after it from -1 to the pass block, with no names (the catalogue
//! refresh, `history_catalogue::refresh`). The keys those refreshes wanted are what the build
//! wants, the union over every refresh being one evaluation over `(-1, pass]` with every
//! composed name:
//!
//! - name sources: every composed name (`keys`), and the name of every event at or below the
//!   pass block or with no block, whatever its visibility or canonicality (`spans`).
//! - resource sources, which also discover edges and anchor resources (`work`): the resources
//!   of every event of a block at or below the pass, of any visibility or canonicality
//!   (`spans`), the resources of a composed name's binding candidates and the resources of its
//!   events with no block (`keys`).
//! - resource sources only (not `work`): the resources of activated canonical events with no
//!   block that no composed name reaches.
//! - node and record-id sources: those of activated canonical events at or below the pass block
//!   or with no block (`spans`), and the sources edges reach (sources.rs).
//!
//! A span's row of a key holds the key's aggregate over the span's events the catalogue reads
//! (activated, canonical, on a canonical block when positioned), null when none.
use bigname_storage::history_catalogue_contract::{event_key_bloom_sql, event_kind_mask_sql};
use sqlx::{Postgres, Transaction};

use super::{super::sources, Layout, WORK, database};
use crate::{ProjectError, Result, families::MAX_COMPOSE_BUILDER_BLOCK_SPANS};

const CLEAR: &str = "/* project:history.build.clear_keys */
    DELETE FROM project_compose_history_work WHERE chain_id = $1 AND part = $2";

/// The composed names, their binding candidates' resources and the resources of their events
/// with no block.
const KEYS: &str = "/* project:history.build.keys */
    INSERT INTO project_compose_history_work
        (chain_id, source_kind, source_key, resolver_address, part, key_partition, work)
    SELECT $1, listed.source_kind, listed.source_key, '', -1,
           (hashtext(listed.source_key) & $2::int)::smallint, true
    FROM (
        SELECT 0::smallint AS source_kind, work.logical_name_id AS source_key
        FROM project_compose_name_work work
        WHERE work.chain_id = $1 AND work.reach = 2
        UNION
        SELECT 1::smallint, resource.value::text
        FROM project_compose_name_work work
        JOIN project_binding_candidate candidate
          ON candidate.chain_id = work.chain_id AND candidate.logical_name_id = work.logical_name_id
        CROSS JOIN LATERAL (VALUES (candidate.resource_id),
            (candidate.wrapped_registrar_resource_id), (candidate.predecessor_resource_id),
            (candidate.lease_resource_id)) resource (value)
        WHERE work.chain_id = $1 AND work.reach = 2 AND resource.value IS NOT NULL
        UNION
        SELECT 1::smallint, ne.resource_id::text
        FROM normalized_events ne
        JOIN project_compose_name_work work
          ON work.chain_id = ne.chain_id AND work.logical_name_id = ne.logical_name_id
         AND work.reach = 2
        WHERE ne.chain_id = $1 AND ne.block_number IS NULL AND ne.resource_id IS NOT NULL
    ) listed";

pub(super) async fn keys(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    mask: i32,
) -> Result<(&'static str, u64)> {
    clear(transaction, chain, -1).await?;
    let rows = sqlx::query(KEYS)
        .bind(chain)
        .bind(mask)
        .execute(&mut **transaction)
        .await
        .map_err(database("failed to list the catalogue build's keys"))?
        .rows_affected();
    Ok((WORK, rows))
}

/// The keys of the span's events and their partial aggregates: blocks in `[$2, $3)`, and in the
/// first span the events with no block. `$4` is the span, `$5` the key partition mask.
fn span_sql() -> String {
    format!(
        concat!(
            "/* project:history.build.spans */
    INSERT INTO project_compose_history_work
        (chain_id, source_kind, source_key, resolver_address, part, key_partition, work,
         first_bucket, last_bucket, event_mask, key_bloom)
    SELECT $1, key.source_kind, key.source_key, key.resolver_address, $4::smallint,
           (hashtext(key.source_key) & $5::int)::smallint, bool_or(key.work),
           min(COALESCE(ne.block_number/256, -1)) FILTER (WHERE event.aggregated),
           max(COALESCE(ne.block_number/256, -1)) FILTER (WHERE event.aggregated),
           bit_or({kind_mask}) FILTER (WHERE event.aggregated),
           bit_or({bloom}) FILTER (WHERE event.aggregated)
    FROM normalized_events ne
    CROSS JOIN LATERAL (SELECT (",
            sources::visible!(),
            ") AS incoming) visible
    CROSS JOIN LATERAL (SELECT visible.incoming AND ",
            sources::readable!(),
            " AS aggregated) event
    CROSS JOIN LATERAL (
        SELECT 0::smallint AS source_kind, ne.logical_name_id AS source_key,
               ''::text AS resolver_address, true AS work, true AS keyed
        WHERE ne.logical_name_id IS NOT NULL
        UNION ALL
        SELECT 1::smallint, ne.resource_id::text, '', ne.block_number IS NOT NULL,
               ne.block_number IS NOT NULL OR visible.incoming
        WHERE ne.resource_id IS NOT NULL
        UNION ALL
        SELECT 2::smallint, lower(ne.after_state->>'node'), '', false, visible.incoming
        WHERE ",
            sources::node_source!(),
            "
        UNION ALL
        SELECT 3::smallint, ne.after_state->>'resolver_record_id',
               lower(ne.after_state->>'resolver'), false, visible.incoming
        WHERE ",
            sources::record_id_source!(),
            "
    ) key
    WHERE ne.chain_id = $1
      AND ((ne.block_number >= $2 AND ne.block_number < $3)
           OR ($4::smallint = 0 AND ne.block_number IS NULL))
      AND key.keyed
    GROUP BY key.source_kind, key.source_key, key.resolver_address"
        ),
        kind_mask = event_kind_mask_sql("ne.event_kind"),
        bloom = event_key_bloom_sql("ne"),
    )
}

pub(super) async fn span(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    pass: i64,
    span: u16,
    layout: Layout,
    mask: i32,
) -> Result<(&'static str, u64)> {
    let part = span_part(span)?;
    clear(transaction, chain, part).await?;
    let (from, to) = bounds(pass, span, layout.spans);
    let rows = sqlx::query(&span_sql())
        .bind(chain)
        .bind(from)
        .bind(to)
        .bind(part)
        .bind(mask)
        .execute(&mut **transaction)
        .await
        .map_err(database(
            "failed to scan a block span for the catalogue build",
        ))?
        .rows_affected();
    Ok((WORK, rows))
}

/// The work table part of span `span`. A span at or past
/// [`MAX_COMPOSE_BUILDER_BLOCK_SPANS`] is refused: a part shared with another span would let
/// each clear the other's partials.
pub(super) fn span_part(span: u16) -> Result<i16> {
    i16::try_from(span)
        .ok()
        .filter(|_| span < MAX_COMPOSE_BUILDER_BLOCK_SPANS)
        .ok_or_else(|| {
            ProjectError::data_integrity(format!(
                "catalogue build span {span} is past the most spans, {MAX_COMPOSE_BUILDER_BLOCK_SPANS}"
            ))
        })
}

/// The blocks `[from, to)` of span `span` of `spans` over `[0, pass]`.
pub(super) fn bounds(pass: i64, span: u16, spans: u16) -> (i64, i64) {
    let blocks = i128::from(pass) + 1;
    let at = |span: u16| {
        i64::try_from(i128::from(span) * blocks / i128::from(spans.max(1))).unwrap_or(i64::MAX)
    };
    (at(span), at(span + 1))
}

async fn clear(transaction: &mut Transaction<'_, Postgres>, chain: &str, part: i16) -> Result<()> {
    sqlx::query(CLEAR)
        .bind(chain)
        .bind(part)
        .execute(&mut **transaction)
        .await
        .map_err(database("failed to clear a catalogue build partition"))?;
    Ok(())
}
