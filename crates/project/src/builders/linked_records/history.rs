use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

/// Keep history attribution separate from the latest link used to serve record values.
/// Each resolver-pointer interval is split whenever its exact or default link changes.
/// A selected record contributes its earlier writes until that selection ends, including
/// values written before the link; later writes on an abandoned record stay unrelated.
pub(super) async fn build(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        r#"CREATE TEMP TABLE project_linked_record_history_attribution ON COMMIT DROP AS
        WITH links AS (
            SELECT normalized_event_id, lower(after_state ->> 'resolver') AS resolver_address,
                   lower(after_state ->> 'node') AS node,
                   after_state ->> 'resolver_record_id' AS record_id,
                   ARRAY[COALESCE(block_number, -1), COALESCE(transaction_index, -1),
                         COALESCE(log_index, -1), normalized_event_id] AS position
            FROM project_events
            WHERE event_kind = 'ResolverRecordLinked'
              AND after_state ->> 'storage_model' = 'resolver_record_id'
        ), pointers AS (
            SELECT pointer.*,
                   CASE WHEN next_event_id IS NOT NULL THEN
                       ARRAY[next_block_number, next_transaction_index,
                             next_log_index, next_event_id] END AS end_position
            FROM project_record_pointer_history pointer
        ), boundaries AS (
            SELECT pointer_event_id, pointer_position AS position FROM pointers
            UNION
            SELECT pointer.pointer_event_id, link.position
            FROM pointers pointer
            JOIN links link ON link.resolver_address = pointer.resolver_address
              AND link.node IN (pointer.namehash,
                  '0x0000000000000000000000000000000000000000000000000000000000000000')
              AND link.position > pointer.pointer_position
              AND (pointer.end_position IS NULL OR link.position < pointer.end_position)
        ), intervals AS (
            SELECT pointer.resource_id, pointer.resolver_address, pointer.namehash,
                   boundary.position,
                   COALESCE(lead(boundary.position) OVER (
                       PARTITION BY boundary.pointer_event_id ORDER BY boundary.position
                   ), pointer.end_position) AS end_position
            FROM boundaries boundary
            JOIN pointers pointer USING (pointer_event_id)
        ), selections AS (
            SELECT interval.*,
                   CASE WHEN exact.record_id <> '0' THEN exact.record_id
                        ELSE defaults.record_id END AS record_id,
                   exact.normalized_event_id AS exact_link_event_id,
                   CASE WHEN COALESCE(exact.record_id, '0') = '0'
                        THEN defaults.normalized_event_id END AS default_link_event_id
            FROM intervals interval
            LEFT JOIN LATERAL (
                SELECT link.* FROM links link
                WHERE link.resolver_address = interval.resolver_address
                  AND link.node = interval.namehash AND link.position <= interval.position
                ORDER BY link.position DESC LIMIT 1
            ) exact ON true
            LEFT JOIN LATERAL (
                SELECT link.* FROM links link
                WHERE link.resolver_address = interval.resolver_address
                  AND link.node =
                      '0x0000000000000000000000000000000000000000000000000000000000000000'
                  AND link.position <= interval.position
                ORDER BY link.position DESC LIMIT 1
            ) defaults ON true
        )
        SELECT selected.resource_id, event.normalized_event_id
        FROM selections selected
        JOIN project_events event
          ON lower(event.after_state ->> 'resolver') = selected.resolver_address
         AND event.after_state ->> 'storage_model' = 'resolver_record_id'
         AND event.after_state ->> 'resolver_record_id' = selected.record_id
         AND event.event_kind = 'RecordChanged'
         AND (selected.end_position IS NULL OR
              ARRAY[COALESCE(event.block_number, -1), COALESCE(event.transaction_index, -1),
                    COALESCE(event.log_index, -1), event.normalized_event_id]
              < selected.end_position)
        UNION
        -- Retain the selecting links too: removing one during redo must rebuild its former
        -- consumers even if neither the link nor its record is selected at the new target.
        SELECT selected.resource_id, link.event_id
        FROM selections selected
        CROSS JOIN LATERAL (VALUES (selected.exact_link_event_id),
                                  (selected.default_link_event_id)) link(event_id)
        WHERE link.event_id IS NOT NULL"#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to attribute linked record history", error))?;
    Ok(())
}
