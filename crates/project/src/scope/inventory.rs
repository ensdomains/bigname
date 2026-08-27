use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn close(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    // A pointer-derived name can be bound to another resource, whose latest pointer can name a
    // further surface. Reach the finite name/resource fixed point before staging and publication.
    loop {
        let before = scope_size(transaction).await?;
        include_pointer_names(transaction, chain_id, target.number).await?;
        super::close_binding_scope(transaction, chain_id, target).await?;
        if scope_size(transaction).await? == before {
            return Ok(());
        }
    }
}

async fn scope_size(transaction: &mut Transaction<'_, Postgres>) -> Result<(i64, i64)> {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM project_scope_names),
                (SELECT count(*) FROM project_scope_resources)",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to measure inventory scope", error))
}

async fn include_pointer_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // Publication deletes every scoped resource before inserting its replacement. Stage every
    // readable linked pointer name so the inventory builder can fall back to an earlier pointer
    // when a later pointer's name surface is not visible at the target.
    sqlx::query("ANALYZE project_scope_resources")
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to analyze inventory scope", error))?;
    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT DISTINCT event.logical_name_id
         FROM project_scope_resources scope
         JOIN normalized_events event USING (resource_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1
           AND event.block_number <= $2
           AND event.event_kind = 'ResolverChanged'
           AND event.logical_name_id IS NOT NULL
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope inventory pointer names", error))?;
    Ok(())
}
