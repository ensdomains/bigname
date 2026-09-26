use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn seed_wrapper_effect_resources(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    sqlx::query(
        r#"/* project:scope.retracted.handoffs.seed_wrapper_effect_resources */
        INSERT INTO project_scope_permission_effect_resources
        SELECT DISTINCT row.resource_id
        FROM permissions_current_resource_summary row
        CROSS JOIN LATERAL (VALUES
            (row.provenance -> 'wrapper_expiry_boundary' ->> 'fuses_event_id'),
            (row.provenance -> 'wrapper_expiry_boundary' ->> 'expiry_event_id')
        ) citation(event_id)
        WHERE row.provenance ->> 'chain_id' = $1
          AND citation.event_id IS NOT NULL
          AND citation.event_id NOT IN ('', 'null')
          AND NOT EXISTS (
              SELECT 1 FROM normalized_events event
              LEFT JOIN chain_lineage lineage
                ON lineage.chain_id = event.chain_id
               AND lineage.block_hash = event.block_hash
               AND lineage.block_number = event.block_number
              WHERE event.normalized_event_id = citation.event_id::bigint
                AND event.consumer_visibility = 'activated'
                AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to retain retracted wrapper effect scope", error)
    })?;
    Ok(())
}

pub(super) async fn seed_child_registration_history(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        "/* project:scope.retracted.handoffs.seed_child_registration_history */ INSERT INTO project_scope_children
         SELECT DISTINCT logical_name_id
         FROM project_redo_child_registration_history
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to scope retracted child registration history",
            error,
        )
    })?;
    Ok(())
}

/// A published child of a locked parent cites the migration registry creation association that
/// made it reachable, and that association is not a normalized event, so the citation check in
/// `seed_children` cannot see it go. When a redo leaves the cited association unreadable, the
/// parent and child rebuild; the parent's subregistry pointer may lie outside the redo range.
pub(super) async fn seed_retracted_migration_registry_associations(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    sqlx::query(
        "/* project:scope.retracted.handoffs.seed_retracted_migration_registry_associations */ INSERT INTO project_scope_children
         SELECT DISTINCT candidate.logical_name_id
         FROM children_current row
         CROSS JOIN LATERAL (
             VALUES (row.parent_logical_name_id), (row.child_logical_name_id)
         ) candidate(logical_name_id)
         WHERE row.provenance ->> 'chain_id' = $1
           AND row.provenance #>> '{parent_reachability,migration_registry_association,logical_edge_identity}' IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM migration_discovery_associations association
               JOIN chain_lineage lineage
                 ON lineage.chain_id = association.chain_id
                AND lineage.block_hash = association.block_hash
                AND lineage.block_number = association.block_number
               WHERE association.chain_id = $1
                 AND association.logical_edge_identity = row.provenance #>> '{parent_reachability,migration_registry_association,logical_edge_identity}'
                 AND association.migration_correlation_id = row.provenance #>> '{parent_reachability,migration_registry_association,migration_correlation_id}'
                 AND association.canonicality_state IN ('canonical', 'safe', 'finalized')
                 AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           )
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to scope children of a retracted migration registry association",
            error,
        )
    })?;
    Ok(())
}
