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

    let positions_present = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM chain_lineage prior
            JOIN chain_lineage target ON target.chain_id = prior.chain_id
            WHERE prior.chain_id = $1
              AND prior.block_number = $2
              AND prior.block_hash = $3
              AND target.block_number = $4
              AND target.block_hash = $5
              AND prior.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND target.canonicality_state IN ('canonical', 'safe', 'finalized')
        )
        "#,
    )
    .bind(chain_id)
    .bind(previous.number)
    .bind(&previous.hash)
    .bind(target.number)
    .bind(&target.hash)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to validate wrapper timestamp positions", error)
    })?;
    require_positions(positions_present, previous, target)?;

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
        )
        INSERT INTO project_scope_permission_effect_resources
        SELECT summary.resource_id
        FROM permissions_current_resource_summary summary
        CROSS JOIN positions
        CROSS JOIN wrapper_constants
        CROSS JOIN LATERAL (
            SELECT
                (summary.provenance -> 'wrapper_expiry_boundary' ->> 'fuses')::bigint
                    AS fuses,
                (summary.provenance -> 'wrapper_expiry_boundary' ->> 'expiry_seconds')::numeric
                    AS expiry_seconds
        ) boundary
        WHERE summary.provenance ->> 'chain_id' = $1
          AND summary.provenance ? 'wrapper_expiry_boundary'
          AND (
              (
                  positions.prior_seconds <= boundary.expiry_seconds
                  AND boundary.expiry_seconds < positions.target_seconds
              ) OR (
                  (boundary.fuses & wrapper_constants.is_dot_eth) <> 0
                  AND positions.prior_seconds <=
                      boundary.expiry_seconds - wrapper_constants.grace_period_seconds
                  AND boundary.expiry_seconds - wrapper_constants.grace_period_seconds
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
    include_effect_resources(transaction).await?;
    Ok(())
}

fn require_positions(positions_present: bool, previous: &Marker, target: &Marker) -> Result<()> {
    if positions_present {
        return Ok(());
    }

    Err(ProjectError::transient(format!(
        "wrapper timestamp positions changed before projection: previous {} {}, target {} {}",
        previous.number, previous.hash, target.number, target.hash
    )))
}

async fn include_all(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO project_scope_permission_effect_resources
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
    include_effect_resources(transaction).await?;
    Ok(())
}

async fn include_effect_resources(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_resources
         SELECT resource_id FROM project_scope_permission_effect_resources
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to include permission-effect resources", error)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn missing_resume_position_is_transient() {
        let previous = Marker {
            number: 41,
            hash: "0xdisplaced".to_owned(),
        };
        let target = Marker {
            number: 42,
            hash: "0xtarget".to_owned(),
        };

        let error = require_positions(false, &previous, &target).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Transient);
        assert!(error.to_string().contains("41 0xdisplaced"));
    }
}
