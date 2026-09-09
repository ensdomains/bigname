use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn build(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in [
        r#"CREATE TEMP TABLE project_record_pointers ON COMMIT DROP AS
        WITH latest_pointers AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id,
                   event.logical_name_id,
                   event.namespace AS pointer_namespace,
                   event.source_family AS pointer_source_family,
                   lower(surface.namehash) AS namehash,
                   lower(event.after_state ->> 'resolver') AS resolver_address,
                   event.manifest_version AS pointer_manifest_version,
                   event.normalized_event_id AS pointer_event_id,
                   event.block_number AS pointer_block_number,
                   event.block_hash AS pointer_block_hash
            FROM project_events event
            JOIN project_surfaces surface USING (logical_name_id)
            WHERE event.event_kind = 'ResolverChanged'
              AND event.resource_id IS NOT NULL
              AND event.logical_name_id IS NOT NULL
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        pointers AS (
            SELECT * FROM latest_pointers
            WHERE resolver_address IS NOT NULL
              AND resolver_address NOT IN (
                  '0x0000000000000000000000000000000000000000', ''
              )
        )
        SELECT * FROM pointers"#,
        r#"CREATE TEMP TABLE project_resolver_links ON COMMIT DROP AS
        SELECT DISTINCT ON (chain_id, lower(after_state ->> 'resolver'),
                            lower(after_state ->> 'node')) event.*
        FROM project_events event
        WHERE event_kind = 'ResolverRecordLinked'
          AND after_state ->> 'storage_model' = 'resolver_record_id'
        ORDER BY chain_id, lower(after_state ->> 'resolver'),
                 lower(after_state ->> 'node'), block_number DESC NULLS LAST,
                 transaction_index DESC NULLS LAST, log_index DESC NULLS LAST,
                 normalized_event_id DESC"#,
        r#"CREATE TEMP TABLE project_selected_records ON COMMIT DROP AS
        SELECT pointer.resource_id,
               pointer.resolver_address,
               CASE WHEN exact.after_state ->> 'resolver_record_id' <> '0'
                    THEN exact.after_state ->> 'resolver_record_id'
                    ELSE defaults.after_state ->> 'resolver_record_id' END AS record_id,
               exact.normalized_event_id AS exact_link_event_id,
               CASE WHEN COALESCE(exact.after_state ->> 'resolver_record_id', '0') = '0'
                    THEN defaults.normalized_event_id END AS default_link_event_id
        FROM project_record_pointers pointer
        LEFT JOIN project_resolver_links exact
          ON lower(exact.after_state ->> 'resolver') = pointer.resolver_address
         AND lower(exact.after_state ->> 'node') = pointer.namehash
        LEFT JOIN project_resolver_links defaults
          ON lower(defaults.after_state ->> 'resolver') = pointer.resolver_address
         AND lower(defaults.after_state ->> 'node') =
             '0x0000000000000000000000000000000000000000000000000000000000000000'
        WHERE exact.normalized_event_id IS NOT NULL
           OR defaults.normalized_event_id IS NOT NULL"#,
        r#"CREATE TEMP TABLE project_linked_record_events ON COMMIT DROP AS
        SELECT selected.resource_id, event.normalized_event_id
        FROM project_selected_records selected
        JOIN project_events event
          ON lower(event.after_state ->> 'resolver') = selected.resolver_address
         AND event.after_state ->> 'storage_model' = 'resolver_record_id'
         AND event.after_state ->> 'resolver_record_id' = selected.record_id
         AND event.event_kind = 'RecordChanged'
        UNION
        SELECT selected.resource_id, link.event_id
        FROM project_selected_records selected
        CROSS JOIN LATERAL (VALUES (selected.exact_link_event_id),
                                  (selected.default_link_event_id)) link(event_id)
        WHERE link.event_id IS NOT NULL"#,
        r#"CREATE TEMP TABLE project_linked_record_changes ON COMMIT DROP AS
        SELECT attributed.resource_id,
               jsonb_agg(event.normalized_event_id ORDER BY event.normalized_event_id)
                   FILTER (WHERE event.event_kind = 'ResolverRecordLinked') AS event_ids,
               max(event.block_number) AS block_number,
               (array_agg(jsonb_build_object(
                   'normalized_event_id', event.normalized_event_id,
                   'event_kind', event.event_kind,
                   'chain_position', jsonb_strip_nulls(jsonb_build_object(
                       'chain_id', event.chain_id, 'block_number', event.block_number,
                       'block_hash', event.block_hash, 'timestamp', lineage.block_timestamp
                   ))
               ) ORDER BY event.block_number DESC NULLS LAST,
                          event.transaction_index DESC NULLS LAST,
                          event.log_index DESC NULLS LAST,
                          event.normalized_event_id DESC))[1] AS last_change
        FROM project_linked_record_events attributed
        JOIN project_events event USING (normalized_event_id)
        LEFT JOIN chain_lineage lineage ON lineage.chain_id = event.chain_id
          AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
        GROUP BY attributed.resource_id"#,
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to select linked resolver records", error)
            })?;
    }
    Ok(())
}
