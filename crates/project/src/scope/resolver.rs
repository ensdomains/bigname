use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn classify_passthrough(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    // Record values and record-version boundaries do not contribute to resolver_current's
    // binding, alias, permission, role, event, or classification summaries. For an existing
    // resolver touched only by those kinds, republish its derived summary at the new target
    // without staging the resolver's unrelated history. Redo and resolver-entity changes are
    // excluded because their keys are present in project_scope_resolver_dependents.
    sqlx::query(
        "INSERT INTO project_scope_resolver_passthrough
         SELECT lower(current.resolver_address)
         FROM resolver_current current
         JOIN project_scope_resolvers scope
           ON lower(scope.resolver_address) = lower(current.resolver_address)
         WHERE current.chain_id = $1
           AND NOT EXISTS (
               SELECT 1 FROM project_scope_resolver_dependents dependent
               WHERE lower(dependent.resolver_address) =
                     lower(current.resolver_address)
           )
           AND EXISTS (
               SELECT 1 FROM project_changed_events event
               WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
                 AND event.source_family IN (
                     'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
                     'basenames_base_resolver'
                 )
                 AND lower(event.raw_fact_ref ->> 'emitting_address') =
                     lower(current.resolver_address)
           )
           AND NOT EXISTS (
               SELECT 1 FROM project_changed_events event
               CROSS JOIN LATERAL (VALUES
                   (event.after_state ->> 'resolver'),
                   (event.before_state ->> 'resolver'),
                   (event.after_state ->> 'proxy_address'),
                   (event.before_state ->> 'proxy_address'),
                   (event.raw_fact_ref ->> 'emitting_address')
               ) candidate(resolver_address)
               WHERE event.event_kind IN (
                   'ResolverChanged', 'PermissionChanged', 'AliasChanged', 'Upgraded'
               )
                 AND lower(candidate.resolver_address) =
                     lower(current.resolver_address)
           )
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to classify record-only resolver scope", error)
    })?;
    Ok(())
}
