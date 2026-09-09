use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn include(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
    full_rebuild: bool,
) -> Result<()> {
    if full_rebuild {
        return Ok(());
    }
    sqlx::query(
        r#"INSERT INTO project_events
        SELECT event.* FROM normalized_events event
        JOIN project_scope_resolvers scope
          ON lower(event.after_state ->> 'resolver') = lower(scope.resolver_address)
        JOIN chain_lineage lineage ON lineage.chain_id = event.chain_id
          AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.consumer_visibility = 'activated'
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND (event.event_kind IN ('ResolverRecordLinked', 'ResolverPermissionArgument')
               OR (event.event_kind = 'RecordChanged'
                   AND event.after_state ->> 'storage_model' = 'resolver_record_id'))
          AND NOT EXISTS (SELECT 1 FROM project_events staged
                          WHERE staged.normalized_event_id = event.normalized_event_id)"#,
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage linked resolver history", error))?;
    Ok(())
}
