use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

/// Interpret redo replaces normalized events in-place. Retain the incremental keys of current
/// rows whose cited event disappeared so project can retract losing-fork output after interpret
/// has already deleted that event.
pub(super) async fn seed(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    seed_names(transaction, chain_id).await?;
    seed_children(transaction, chain_id).await?;
    seed_resources(transaction, chain_id, from_block, to_block).await?;
    seed_resolvers(transaction, chain_id).await?;
    seed_primary(transaction, chain_id).await?;
    Ok(())
}

async fn seed_names(transaction: &mut Transaction<'_, Postgres>, chain_id: &str) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.logical_name_id, citation.event_id
            FROM name_current row
            CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(
                row.provenance -> 'selected_event_ids', '[]'::jsonb
            )) citation(event_id)
            WHERE row.provenance ->> 'chain_id' = $1
            UNION ALL
            SELECT row.logical_name_id, row.provenance ->> 'normalized_event_id'
            FROM address_names_current row
            WHERE row.provenance ->> 'chain_id' = $1
        )
        INSERT INTO project_scope_names
        SELECT DISTINCT citation.logical_name_id
        FROM citations citation
        WHERE citation.event_id IS NOT NULL
          AND citation.event_id NOT IN ('', 'null')
          AND NOT EXISTS (
              SELECT 1 FROM normalized_events event
              LEFT JOIN chain_lineage lineage
                ON lineage.chain_id = event.chain_id
               AND lineage.block_hash = event.block_hash
               AND lineage.block_number = event.block_number
              WHERE event.normalized_event_id = citation.event_id::bigint
                AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                AND (
                    (event.block_number IS NULL AND event.block_hash IS NULL)
                    OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                )
          )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain retracted name scope", error))?;
    Ok(())
}

async fn seed_children(transaction: &mut Transaction<'_, Postgres>, chain_id: &str) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.parent_logical_name_id, row.child_logical_name_id,
                   citation.event_id
            FROM children_current row
            CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(
                row.provenance -> 'normalized_event_ids', '[]'::jsonb
            )) citation(event_id)
            WHERE row.provenance ->> 'chain_id' = $1
        ), retracted AS (
            SELECT citation.parent_logical_name_id, citation.child_logical_name_id
            FROM citations citation
            WHERE citation.event_id IS NOT NULL
              AND citation.event_id NOT IN ('', 'null')
              AND NOT EXISTS (
                  SELECT 1 FROM normalized_events event
                  LEFT JOIN chain_lineage lineage
                    ON lineage.chain_id = event.chain_id
                   AND lineage.block_hash = event.block_hash
                   AND lineage.block_number = event.block_number
                  WHERE event.normalized_event_id = citation.event_id::bigint
                    AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                    AND (
                        (event.block_number IS NULL AND event.block_hash IS NULL)
                        OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                    )
              )
        )
        INSERT INTO project_scope_children
        SELECT DISTINCT logical_name_id
        FROM retracted
        CROSS JOIN LATERAL (
            VALUES (parent_logical_name_id), (child_logical_name_id)
        ) candidate(logical_name_id)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain retracted child scope", error))?;
    Ok(())
}

async fn seed_resources(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.resource_id, citation.event_id, false AS force_scope
            FROM permissions_current row
            CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(
                row.provenance -> 'normalized_event_ids', '[]'::jsonb
            )) citation(event_id)
            WHERE row.provenance ->> 'chain_id' = $1
            UNION ALL
            SELECT row.resource_id, citation.event_id, false
            FROM record_inventory_current row
            CROSS JOIN LATERAL jsonb_array_elements_text(
                COALESCE(row.provenance -> 'record_event_ids', '[]'::jsonb)
                || jsonb_build_array(
                    row.provenance -> 'resolver_pointer_event_id',
                    row.record_version_boundary -> 'normalized_event_id',
                    row.last_change -> 'normalized_event_id'
                )
            ) citation(event_id)
            WHERE row.provenance ->> 'chain_id' = $1
            UNION ALL
            SELECT row.resource_id, row.provenance ->> 'normalized_event_id', false
            FROM address_names_current row
            WHERE row.provenance ->> 'chain_id' = $1 AND row.resource_id IS NOT NULL
            UNION ALL
            SELECT row.resource_id, NULL, true
            FROM permissions_current_resource_summary row
            WHERE row.provenance ->> 'chain_id' = $1
              AND NULLIF(row.chain_positions ->> 'block_number', '')::bigint
                  BETWEEN $2 AND $3
        )
        INSERT INTO project_scope_resources
        SELECT DISTINCT citation.resource_id
        FROM citations citation
        WHERE citation.force_scope OR (
            citation.event_id IS NOT NULL
            AND citation.event_id NOT IN ('', 'null')
            AND NOT EXISTS (
                SELECT 1 FROM normalized_events event
                LEFT JOIN chain_lineage lineage
                  ON lineage.chain_id = event.chain_id
                 AND lineage.block_hash = event.block_hash
                 AND lineage.block_number = event.block_number
                WHERE event.normalized_event_id = citation.event_id::bigint
                  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND (
                      (event.block_number IS NULL AND event.block_hash IS NULL)
                      OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  )
            )
        )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain retracted resource scope", error))?;
    Ok(())
}

async fn seed_resolvers(transaction: &mut Transaction<'_, Postgres>, chain_id: &str) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.resolver_address, citation.event_id
            FROM resolver_current row
            CROSS JOIN LATERAL (
                VALUES (row.provenance ->> 'manifest_event_id'),
                       (row.provenance ->> 'upgrade_event_id')
            ) citation(event_id)
            WHERE row.chain_id = $1
        )
        INSERT INTO project_scope_resolvers
        SELECT DISTINCT lower(citation.resolver_address)
        FROM citations citation
        WHERE citation.event_id IS NOT NULL
          AND citation.event_id NOT IN ('', 'null')
          AND NOT EXISTS (
              SELECT 1 FROM normalized_events event
              LEFT JOIN chain_lineage lineage
                ON lineage.chain_id = event.chain_id
               AND lineage.block_hash = event.block_hash
               AND lineage.block_number = event.block_number
              WHERE event.normalized_event_id = citation.event_id::bigint
                AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                AND (
                    (event.block_number IS NULL AND event.block_hash IS NULL)
                    OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                )
          )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain retracted resolver scope", error))?;
    Ok(())
}

async fn seed_primary(transaction: &mut Transaction<'_, Postgres>, chain_id: &str) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.address, row.coin_type, row.namespace, citation.event_id
            FROM primary_names_current row
            CROSS JOIN LATERAL (
                VALUES (row.claim_provenance ->> 'reverse_event_id'),
                       (row.claim_provenance ->> 'claim_event_id')
            ) citation(event_id)
            WHERE row.claim_provenance ->> 'chain_id' = $1
        )
        INSERT INTO project_scope_primary (address, coin_type, namespace)
        SELECT DISTINCT citation.address, citation.coin_type, citation.namespace
        FROM citations citation
        WHERE citation.event_id IS NOT NULL
          AND citation.event_id NOT IN ('', 'null')
          AND NOT EXISTS (
              SELECT 1 FROM normalized_events event
              LEFT JOIN chain_lineage lineage
                ON lineage.chain_id = event.chain_id
               AND lineage.block_hash = event.block_hash
               AND lineage.block_number = event.block_number
              WHERE event.normalized_event_id = citation.event_id::bigint
                AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                AND (
                    (event.block_number IS NULL AND event.block_hash IS NULL)
                    OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                )
          )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to retain retracted primary-name scope", error)
    })?;
    Ok(())
}
