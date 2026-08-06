use sqlx::{Postgres, Transaction};

use crate::{Marker, ProjectError, Result};

pub(super) async fn include_time_boundaries(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    previous: Option<&Marker>,
    target: &Marker,
) -> Result<()> {
    let Some(previous) = previous else {
        return include_all(transaction, chain_id, target).await;
    };

    sqlx::query(
        r#"
        WITH positions AS (
            SELECT extract(epoch FROM prior.block_timestamp) AS prior_seconds,
                   extract(epoch FROM target.block_timestamp) AS target_seconds
            FROM chain_lineage prior
            JOIN chain_lineage target ON target.chain_id = prior.chain_id
            WHERE prior.chain_id = $1
              AND prior.block_number = $2
              AND prior.block_hash = $3
              AND target.block_number = $4
              AND target.block_hash = $5
              AND prior.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND target.canonicality_state IN ('canonical', 'safe', 'finalized')
        ),
        wrapper_constants AS (
            SELECT 131072::bigint AS is_dot_eth,
                   7776000::numeric AS grace_period_seconds
        ),
        modifiers AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'fuses') = 'number'
                        AND (event.after_state ->> 'fuses')::numeric >= 0
                        AND (event.after_state ->> 'fuses')::numeric <=
                            9223372036854775807
                           THEN (event.after_state ->> 'fuses')::bigint
                   END AS fuses
            FROM normalized_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.chain_id = $1
              AND event.block_number <= $4
              AND event.event_kind = 'PermissionScopeChanged'
              AND event.source_family = 'ens_v1_wrapper_l1'
              AND event.resource_id IS NOT NULL
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        expiries AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.resource_id,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric >= 0
                        AND (event.after_state ->> 'expiry')::numeric <=
                            18446744073709551615
                           THEN (event.after_state ->> 'expiry')::numeric
                   END AS expiry_seconds
            FROM normalized_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_number = event.block_number
             AND lineage.block_hash = event.block_hash
            WHERE event.chain_id = $1
              AND event.block_number <= $4
              AND event.event_kind = 'ExpiryChanged'
              AND event.source_family = 'ens_v1_wrapper_l1'
              AND event.resource_id IS NOT NULL
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        )
        INSERT INTO project_scope_resources
        SELECT modifier.resource_id
        FROM modifiers modifier
        JOIN expiries expiry USING (resource_id)
        CROSS JOIN positions
        CROSS JOIN wrapper_constants
        WHERE modifier.fuses IS NOT NULL
          AND expiry.expiry_seconds IS NOT NULL
          AND (
              (
                  positions.prior_seconds <= expiry.expiry_seconds
                  AND expiry.expiry_seconds < positions.target_seconds
              ) OR (
                  (modifier.fuses & wrapper_constants.is_dot_eth) <> 0
                  AND positions.prior_seconds <=
                      expiry.expiry_seconds - wrapper_constants.grace_period_seconds
                  AND expiry.expiry_seconds - wrapper_constants.grace_period_seconds
                      < positions.target_seconds
              )
          )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(previous.number)
    .bind(&previous.hash)
    .bind(target.number)
    .bind(&target.hash)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope wrapper timestamp transitions", error)
    })?;
    Ok(())
}

async fn include_all(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO project_scope_resources
        SELECT DISTINCT event.resource_id
        FROM normalized_events event
        JOIN chain_lineage lineage
          ON lineage.chain_id = event.chain_id
         AND lineage.block_number = event.block_number
         AND lineage.block_hash = event.block_hash
        WHERE event.chain_id = $1
          AND event.block_number <= $2
          AND event.event_kind = 'PermissionScopeChanged'
          AND event.source_family = 'ens_v1_wrapper_l1'
          AND event.resource_id IS NOT NULL
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(target.number)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope wrapper redo", error))?;
    Ok(())
}
