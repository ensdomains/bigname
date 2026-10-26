//! Address memberships of one key partition, from the shared historical matcher and the current
//! relations the names stream persisted. A name partition runs the matcher over its names'
//! events alone and a resource partition over its resources' events alone, so no partition
//! writes a mask of a key another partition owns. Together the two arms read what the
//! incremental refresh reads for a name and a resource it wants: every canonical event at or
//! below the pass block of the name, and of the resource. A resource's current mask is the union
//! of the relations naming it, the union the incremental refresh takes over its name rows.
use bigname_storage::{
    HistoryWanted, history_catalogue_contract::relation_mask_sql, push_historical_history_relations,
};
use sqlx::{Arguments, Postgres, QueryBuilder, Transaction, postgres::PgArguments};

use super::{super::envelopes, database};
use crate::{ProjectError, Result};

const ANCHOR: &str = "project_address_history_anchor";

const CLEAR: &str = "/* project:history.build.clear_anchors */
    DELETE FROM project_address_history_anchor
    WHERE chain_id = $1 AND anchor_kind = $2 AND (hashtext(anchor_id) & $4::int) = $3::int";

/// The relations persisted on the work rows, as `relation`.
const RELATIONS: &str = "project_compose_name_work work
        CROSS JOIN LATERAL jsonb_to_recordset(work.relations)
            relation(address text, namespace text, resource_id uuid, relation text)";

pub(super) async fn names(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    pass: i64,
    part: i32,
    mask: i32,
) -> Result<(&'static str, u64)> {
    let current = format!(
        "/* project:history.build.name_relations */ SELECT relation.address, relation.namespace,
               work.logical_name_id AS anchor_id,
               relation.resource_id, {mask} AS mask
        FROM {RELATIONS}
        WHERE work.chain_id = $1 AND work.relations IS NOT NULL
          AND (hashtext(work.logical_name_id) & $4::int) = $3::int",
        mask = relation_mask_sql("relation.relation"),
    );
    write(
        transaction,
        (chain, 0, part, mask),
        pass,
        "/* project:history.build.name_anchors */ WITH anchored AS MATERIALIZED (
        SELECT DISTINCT work.source_key AS anchor_id FROM project_compose_history_work work
        WHERE work.chain_id = $1 AND work.source_kind = 0 AND work.key_partition = $3::int
    ), historical AS (",
        (
            HistoryWanted::Sql(
                "/* project:history.build.anchored_names */ SELECT anchored.anchor_id FROM anchored",
            ),
            HistoryWanted::None,
        ),
        "historical.logical_name_id",
        &current,
        "max(entry.resource_id::text)::uuid",
    )
    .await
}

pub(super) async fn resources(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    pass: i64,
    part: i32,
    mask: i32,
) -> Result<(&'static str, u64)> {
    let current = format!(
        "/* project:history.build.resource_relations */ SELECT relation.address,
               relation.namespace, anchored.anchor_id,
               NULL::uuid AS resource_id, {mask} AS mask
        FROM {RELATIONS}
        JOIN anchored ON anchored.resource = relation.resource_id
        WHERE work.chain_id = $1 AND work.relations IS NOT NULL",
        mask = relation_mask_sql("relation.relation"),
    );
    write(
        transaction,
        (chain, 1, part, mask),
        pass,
        "/* project:history.build.resource_anchors */ WITH anchored AS MATERIALIZED (
        SELECT work.source_key::uuid AS resource, work.source_key AS anchor_id
        FROM project_compose_history_work work
        WHERE work.chain_id = $1 AND work.source_kind = 1 AND work.key_partition = $3::int
        GROUP BY work.source_key HAVING bool_or(work.work)
    ), historical AS (",
        (
            HistoryWanted::None,
            HistoryWanted::Sql(
                "/* project:history.build.anchored_resources */ SELECT anchored.resource FROM anchored",
            ),
        ),
        "historical.resource_id::text",
        &current,
        "NULL::uuid",
    )
    .await
}

/// Delete the partition's anchors of one kind and insert them again: the matcher's masks and the
/// current masks per address, namespace and anchor, a row kept when either is nonzero, each with
/// the envelope of its finished sources. `$1` is the chain, `$2` the kind, `$3` the partition and
/// `$4` its mask.
#[allow(clippy::too_many_arguments)]
async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    (chain, kind, part, mask): (&str, i16, i32, i32),
    pass: i64,
    head: &str,
    (names, resources): (HistoryWanted<'_, String>, HistoryWanted<'_, uuid::Uuid>),
    historical_id: &str,
    current: &str,
    current_resource: &str,
) -> Result<(&'static str, u64)> {
    sqlx::query(CLEAR)
        .bind(chain)
        .bind(kind)
        .bind(part)
        .bind(mask)
        .execute(&mut **transaction)
        .await
        .map_err(database("failed to clear a partition of catalogue anchors"))?;
    let mut arguments = PgArguments::default();
    arguments
        .add(chain)
        .and_then(|()| arguments.add(kind))
        .and_then(|()| arguments.add(part))
        .and_then(|()| arguments.add(mask))
        .map_err(|error| {
            ProjectError::data_integrity(format!("catalogue anchor arguments: {error}"))
        })?;
    let mut query = QueryBuilder::<Postgres>::with_arguments(head, arguments);
    push_historical_history_relations(&mut query, chain, pass, names, resources);
    query.push(format!(
        "), currently AS ({current}
    ), packed AS (
        SELECT entry.address, entry.namespace, entry.anchor_id,
               bit_or(entry.historical_mask) AS historical_mask,
               bit_or(entry.current_mask) AS current_mask,
               {current_resource} AS current_resource_id
        FROM (
            SELECT historical.address, historical.namespace, {historical_id} AS anchor_id,
                   historical.relation_mask AS historical_mask, 0::smallint AS current_mask,
                   NULL::uuid AS resource_id
            FROM historical
            UNION ALL
            SELECT currently.address, currently.namespace, currently.anchor_id, 0::smallint,
                   currently.mask, currently.resource_id
            FROM currently
        ) entry
        GROUP BY entry.address, entry.namespace, entry.anchor_id
        HAVING bit_or(entry.historical_mask) <> 0 OR bit_or(entry.current_mask) <> 0
    )
    INSERT INTO project_address_history_anchor (chain_id, address, namespace, anchor_kind,
        anchor_id, current_mask, historical_mask, current_resource_id, first_bucket, last_bucket,
        bucket_range, event_mask, key_bloom)
    SELECT $1, anchor.address, anchor.namespace, $2::smallint, anchor.anchor_id,
           anchor.current_mask, anchor.historical_mask, anchor.current_resource_id,
           bounds.first_bucket, bounds.last_bucket,
           CASE WHEN bounds.first_bucket IS NULL THEN 'empty'::int8range
                ELSE int8range(bounds.first_bucket, bounds.last_bucket + 1, '[)') END,
           bounds.event_mask, bounds.key_bloom
    FROM packed anchor {bounds}",
        bounds = envelopes::bounds(kind, "anchor.anchor_id"),
    ));
    let rows = query
        .build()
        .execute(&mut **transaction)
        .await
        .map_err(database("failed to write catalogue anchors"))?
        .rows_affected();
    Ok((ANCHOR, rows))
}
