//! The derived index rows of F13 and F14. They are never journalled: after a block writes its
//! rows, and after an undo restores them, the index rows of every key the block touched are
//! deleted and derived again from the base rows. The touched keys come from the block's journal
//! (the pre-block images) and from the base rows as they stand when `touched` runs, so taking
//! them before an undo's restore and after a block's write covers both states.
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

#[derive(Debug, Default)]
pub(crate) struct Touched {
    names: Vec<String>,
    node_resolvers: Vec<String>,
    nodes: Vec<String>,
    id_resolvers: Vec<String>,
    record_ids: Vec<String>,
}

/// The names, (resolver, node) pairs and (resolver, record id) pairs block `number` touched.
pub(crate) async fn touched(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    number: i64,
) -> Result<Touched> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "/* project:families.derived.touched */ WITH journal AS (
             SELECT family, key::jsonb AS key, before_image
             FROM project_family_undo
             WHERE chain_id = $1 AND block_number = $2
               AND family IN ('project_address_name_fold', 'project_address_controller_candidate',
                              'project_lifecycle_event', 'project_node_record_value',
                              'project_node_record_partition', 'project_record_id_value')
         )
         SELECT DISTINCT kind, first, second FROM (
             SELECT 'name' AS kind, key ->> 1 AS first, '' AS second
             FROM journal
             WHERE family IN ('project_address_name_fold', 'project_address_controller_candidate')
             UNION ALL
             SELECT 'name', before_image ->> 'decoded_logical_name_id', ''
             FROM journal WHERE family = 'project_lifecycle_event'
             UNION ALL
             SELECT 'name', event.decoded_logical_name_id, ''
             FROM journal
             JOIN project_lifecycle_event event
               ON event.chain_id = $1 AND event.state_kind = journal.key ->> 1
              AND event.state_key = journal.key ->> 2
              AND event.event_identity = journal.key ->> 3
             WHERE journal.family = 'project_lifecycle_event'
             UNION ALL
             SELECT 'node', key ->> 1, before_image ->> 'node'
             FROM journal
             WHERE family IN ('project_node_record_value', 'project_node_record_partition')
             UNION ALL
             SELECT 'node', value.resolver_address, value.node
             FROM journal
             JOIN project_node_record_value value
               ON value.chain_id = $1 AND value.resolver_address = journal.key ->> 1
              AND value.arm = journal.key ->> 2 AND value.arm_identity = journal.key ->> 3
              AND value.record_key = journal.key ->> 4
             WHERE journal.family = 'project_node_record_value'
             UNION ALL
             SELECT 'node', partition.resolver_address, partition.node
             FROM journal
             JOIN project_node_record_partition partition
               ON partition.chain_id = $1 AND partition.resolver_address = journal.key ->> 1
              AND partition.arm = journal.key ->> 2
              AND partition.arm_identity = journal.key ->> 3
             WHERE journal.family = 'project_node_record_partition'
             UNION ALL
             SELECT 'record_id', key ->> 1, key ->> 2
             FROM journal WHERE family = 'project_record_id_value'
         ) touched
         WHERE first IS NOT NULL AND second IS NOT NULL",
    )
    .bind(chain_id)
    .bind(number)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to read the keys a family block touched", error)
    })?;
    let mut touched = Touched::default();
    for (kind, first, second) in rows {
        match kind.as_str() {
            "name" => touched.names.push(first),
            "node" => {
                touched.node_resolvers.push(first);
                touched.nodes.push(second);
            }
            _ => {
                touched.id_resolvers.push(first);
                touched.record_ids.push(second);
            }
        }
    }
    Ok(touched)
}

/// Delete and derive again the index rows of the touched keys.
pub(crate) async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    touched: &Touched,
) -> Result<()> {
    if !touched.names.is_empty() {
        run(transaction, NAME_DELETE, chain_id, &touched.names, None).await?;
        run(transaction, NAME_INSERT, chain_id, &touched.names, None).await?;
    }
    if !touched.nodes.is_empty() {
        let pairs = (&touched.node_resolvers, Some(&touched.nodes));
        run(transaction, NODE_DELETE, chain_id, pairs.0, pairs.1).await?;
        run(transaction, NODE_INSERT, chain_id, pairs.0, pairs.1).await?;
    }
    if !touched.record_ids.is_empty() {
        let pairs = (&touched.id_resolvers, Some(&touched.record_ids));
        run(transaction, RECORD_ID_DELETE, chain_id, pairs.0, pairs.1).await?;
        run(transaction, RECORD_ID_INSERT, chain_id, pairs.0, pairs.1).await?;
    }
    Ok(())
}

async fn run(
    transaction: &mut Transaction<'_, Postgres>,
    sql: &str,
    chain_id: &str,
    first: &[String],
    second: Option<&Vec<String>>,
) -> Result<()> {
    let mut query = sqlx::query(sql).bind(chain_id).bind(first);
    if let Some(second) = second {
        query = query.bind(second);
    }
    query
        .execute(&mut **transaction)
        .await
        .map(|_| ())
        .map_err(|error| ProjectError::database("failed to refresh a family index", error))
}

const NAME_DELETE: &str = "/* project:families.derived.name_delete */
    DELETE FROM project_address_name_index
    WHERE chain_id = $1 AND logical_name_id = ANY($2::text[])";

/// Every address a name's three relations (address_names.rs `relations`) can hold under some
/// admission and mask, so a read only removes rows: the registrant falls back through the
/// retained F2a rows of the name (a grant's or reservation's registrant, a release's prior
/// registrant, a transfer's recipient), the token holder through those and the fold's token
/// holder, and the effective controller through every controller candidate's subject besides.
const NAME_INSERT: &str = "/* project:families.derived.name_insert */
    WITH holders AS (
        SELECT event.decoded_logical_name_id AS logical_name_id, address.address
        FROM project_lifecycle_event event
        CROSS JOIN LATERAL (VALUES (event.registrant), (event.before_registrant),
                                   (event.to_address)) address (address)
        WHERE event.chain_id = $1 AND event.decoded_logical_name_id = ANY($2::text[])
        UNION ALL
        SELECT fold.logical_name_id, fold.token_holder
        FROM project_address_name_fold fold
        WHERE fold.chain_id = $1 AND fold.logical_name_id = ANY($2::text[])
    ),
    relations AS (
        SELECT holder.address, holder.logical_name_id, relation.relation
        FROM holders holder
        CROSS JOIN (VALUES ('registrant'), ('token_holder'), ('effective_controller'))
            relation (relation)
        UNION ALL
        SELECT candidate.subject, candidate.logical_name_id, 'effective_controller'
        FROM project_address_controller_candidate candidate
        WHERE candidate.chain_id = $1 AND candidate.logical_name_id = ANY($2::text[])
          AND candidate.action = 'set'
    )
    INSERT INTO project_address_name_index (address, logical_name_id, relation, chain_id)
    SELECT DISTINCT lower(relation.address), relation.logical_name_id, relation.relation, $1
    FROM relations relation
    WHERE relation.address IS NOT NULL AND btrim(relation.address) <> ''
      AND lower(relation.address) <> '0x0000000000000000000000000000000000000000'
    ON CONFLICT DO NOTHING";

const NODE_DELETE: &str = "/* project:families.derived.node_delete */
    DELETE FROM project_address_record_node_index index
    USING unnest($2::text[], $3::text[]) touched (resolver_address, node)
    WHERE index.chain_id = $1 AND index.resolver_address = touched.resolver_address
      AND index.node = touched.node";

/// One row per successful non-zero EVM-shaped `addr` value at the node written after its
/// partition's latest version change in the canonical event order, the event identity
/// included (address_records.rs `record_entries`). The value is the served one: the
/// AddressChanged half of a coin-60 pair, its `value` payload else its `address_bytes_hex`, as
/// the record inventory builds an entry's value (record_inventory.rs:357-381).
const NODE_INSERT: &str = r#"/* project:families.derived.node_insert */
    INSERT INTO project_address_record_node_index
        (address, coin_type, chain_id, resolver_address, node)
    SELECT DISTINCT value.address, value.coin_type, $1, value.resolver_address, value.node
    FROM (
        SELECT record.resolver_address, record.node, record.arm, record.arm_identity,
               record.block_number, record.transaction_index, record.log_index,
               record.event_identity,
               record.selector_key::numeric::text AS coin_type,
               lower(COALESCE(
                   CASE WHEN jsonb_typeof(served.value) = 'string' THEN served.value #>> '{}'
                        ELSE COALESCE(served.value ->> 'value', served.value ->> 'bytes')
                   END,
                   served.address_bytes_hex
               )) AS address
        FROM project_node_record_value record
        JOIN unnest($2::text[], $3::text[]) touched (resolver_address, node)
          ON touched.resolver_address = record.resolver_address AND touched.node = record.node
        -- The served half of a coin-60 pair is the AddressChanged one (`sibling_*`).
        CROSS JOIN LATERAL (
            SELECT CASE WHEN record.sibling_position IS NULL THEN record.status
                        ELSE record.sibling_status END AS status,
                   CASE WHEN record.sibling_position IS NULL THEN record.value
                        ELSE record.sibling_value END AS value,
                   CASE WHEN record.sibling_position IS NULL THEN record.address_bytes_hex
                        ELSE record.sibling_address_bytes_hex END AS address_bytes_hex
        ) served
        WHERE record.chain_id = $1 AND served.status = 'success'
          AND record.record_family = 'addr' AND record.selector_key ~ '^[0-9]{1,30}$'
    ) value
    LEFT JOIN project_node_record_partition partition
      ON partition.chain_id = $1 AND partition.resolver_address = value.resolver_address
     AND partition.arm = value.arm AND partition.arm_identity = value.arm_identity
    WHERE value.address ~ '^0x[0-9a-f]{40}$'
      AND value.address <> '0x0000000000000000000000000000000000000000'
      AND (partition.version_position IS NULL
           OR (value.block_number, COALESCE(value.transaction_index, -1),
               COALESCE(value.log_index, -1), value.event_identity COLLATE "C")
              > ((partition.version_position ->> 'block_number')::bigint,
                 COALESCE((partition.version_position ->> 'transaction_index')::bigint, -1),
                 COALESCE((partition.version_position ->> 'log_index')::bigint, -1),
                 (partition.version_position ->> 'event_identity') COLLATE "C"))
    ON CONFLICT DO NOTHING"#;

const RECORD_ID_DELETE: &str = "/* project:families.derived.record_id_delete */
    DELETE FROM project_address_record_id_index index
    USING unnest($2::text[], $3::text[]) touched (resolver_address, record_id)
    WHERE index.chain_id = $1 AND index.resolver_address = touched.resolver_address
      AND index.record_id = touched.record_id";

const RECORD_ID_INSERT: &str = "/* project:families.derived.record_id_insert */
    INSERT INTO project_address_record_id_index
        (address, coin_type, chain_id, resolver_address, record_id)
    SELECT DISTINCT value.address, value.coin_type, $1, value.resolver_address, value.record_id
    FROM (
        SELECT record.resolver_address, record.record_id,
               record.selector_key::numeric::text AS coin_type,
               lower(COALESCE(
                   CASE WHEN jsonb_typeof(record.value) = 'string' THEN record.value #>> '{}'
                        ELSE COALESCE(record.value ->> 'value', record.value ->> 'bytes')
                   END,
                   record.address_bytes_hex
               )) AS address
        FROM project_record_id_value record
        JOIN unnest($2::text[], $3::text[]) touched (resolver_address, record_id)
          ON touched.resolver_address = record.resolver_address
         AND touched.record_id = record.record_id
        WHERE record.chain_id = $1 AND record.status = 'success'
          AND record.record_family = 'addr' AND record.selector_key ~ '^[0-9]{1,30}$'
    ) value
    WHERE value.address ~ '^0x[0-9a-f]{40}$'
      AND value.address <> '0x0000000000000000000000000000000000000000'
    ON CONFLICT DO NOTHING";
