use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn seed(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_primary
         SELECT DISTINCT lower(candidate.address), candidate.coin_type, candidate.namespace
         FROM project_changed_events event
         CROSS JOIN LATERAL (
             VALUES
                 (event.after_state ->> 'address', event.after_state ->> 'coin_type', event.after_state ->> 'namespace'),
                 (event.before_state ->> 'address', event.before_state ->> 'coin_type', event.before_state ->> 'namespace'),
                 (event.after_state -> 'primary_claim_source' ->> 'address', event.after_state -> 'primary_claim_source' ->> 'coin_type', event.after_state -> 'primary_claim_source' ->> 'namespace'),
                 (event.before_state -> 'primary_claim_source' ->> 'address', event.before_state -> 'primary_claim_source' ->> 'coin_type', event.before_state -> 'primary_claim_source' ->> 'namespace')
         ) candidate(address, coin_type, namespace)
         WHERE candidate.address IS NOT NULL
           AND candidate.coin_type IS NOT NULL
           AND candidate.namespace IS NOT NULL
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to derive primary-name scope", error))?;

    Ok(())
}
