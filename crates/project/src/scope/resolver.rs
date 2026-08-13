use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result, resolver_address::PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES};

pub(super) async fn include_resource_pointers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // A resource slice rebuild embeds its current resolver classification in record inventory.
    // Scope both the projected pointer and the latest readable pointer. Redo needs the former to
    // retract losing output and the latter to classify surviving inventory. Unchanged resolver
    // summaries can be republished without staging their unrelated history.
    sqlx::query(
        "INSERT INTO project_scope_resolvers
         SELECT lower(pointer.resolver_address)
         FROM (
             SELECT inventory.provenance ->> 'resolver_address' AS resolver_address
             FROM record_inventory_current inventory
             JOIN project_scope_resources scope USING (resource_id)
             WHERE inventory.provenance ->> 'chain_id' = $1
             UNION ALL
             SELECT latest.resolver_address
             FROM (
                 SELECT DISTINCT ON (event.resource_id)
                        event.resource_id,
                        event.after_state ->> 'resolver' AS resolver_address
                 FROM normalized_events event
                 JOIN project_scope_resources scope USING (resource_id)
                 JOIN chain_lineage lineage
                   ON lineage.chain_id = event.chain_id
                  AND lineage.block_hash = event.block_hash
                  AND lineage.block_number = event.block_number
                 WHERE event.chain_id = $1
                   AND event.event_kind = 'ResolverChanged'
                   AND event.consumer_visibility = 'activated'
                   AND event.block_number <= $2
                   AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                   AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                 ORDER BY event.resource_id,
                          event.block_number DESC NULLS LAST,
                          event.transaction_index DESC NULLS LAST,
                          event.log_index DESC NULLS LAST,
                          event.normalized_event_id DESC
             ) latest
         ) pointer
         WHERE pointer.resolver_address IS NOT NULL
           AND btrim(pointer.resolver_address) <> ''
           AND lower(pointer.resolver_address) <>
               '0x0000000000000000000000000000000000000000'
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to scope resource pointer resolvers", error))?;
    Ok(())
}

pub(super) async fn classify_unchanged(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    // Record values and record-version boundaries do not contribute to resolver_current's
    // binding, alias, permission, role, event, or classification summaries. A resource rebuild
    // also needs only its pointer resolver's existing classification. In either case, republish
    // the existing resolver summary at the new target without staging unrelated history. Redo and
    // resolver-entity changes are excluded because their keys are in resolver_dependents.
    let passthrough_scope = format!(
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
           AND (
               EXISTS (
                   SELECT 1 FROM project_changed_events event
                   WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
                     AND event.source_family IN (
                         'ens_v1_resolver_l1', 'ens_v2_resolver_l1',
                         'basenames_base_resolver'
                     )
                     AND lower(event.raw_fact_ref ->> 'emitting_address') =
                         lower(current.resolver_address)
               )
               OR EXISTS (
                   SELECT 1
                   FROM record_inventory_current inventory
                   JOIN project_scope_resources resource USING (resource_id)
                   WHERE inventory.provenance ->> 'chain_id' = $1
                     AND lower(inventory.provenance ->> 'resolver_address') =
                         lower(current.resolver_address)
               )
           )
           AND NOT EXISTS (
               SELECT 1
               FROM project_changed_events event
               WHERE event.event_kind IN ('SurfaceBound', 'SurfaceUnbound')
                 AND (
                     EXISTS (
                         SELECT 1 FROM name_current name
                         WHERE name.logical_name_id = event.logical_name_id
                           AND name.declared_summary #>> '{{resolver,chain_id}}' = $1
                           AND lower(name.declared_summary #>> '{{resolver,address}}') =
                               lower(current.resolver_address)
                     )
                     OR EXISTS (
                         SELECT 1 FROM record_inventory_current inventory
                         WHERE inventory.resource_id = event.resource_id
                           AND inventory.provenance ->> 'chain_id' = $1
                           AND lower(inventory.provenance ->> 'resolver_address') =
                               lower(current.resolver_address)
                   )
               )
           )
           AND NOT EXISTS (
               SELECT 1
               FROM permissions_current permission
               JOIN project_scope_resources resource USING (resource_id)
               WHERE permission.scope_kind = 'resolver'
                 AND permission.scope_detail ->> 'chain_id' = $1
                 AND lower(permission.scope_detail ->> 'resolver_address') =
                     lower(current.resolver_address)
           )
           AND NOT EXISTS (
               SELECT 1 FROM project_changed_events event
               CROSS JOIN LATERAL (VALUES
                   (event.after_state ->> 'resolver'),
                   (event.before_state ->> 'resolver'),
                   (event.after_state ->> 'proxy_address'),
                   (event.before_state ->> 'proxy_address'),
                   (event.raw_fact_ref ->> 'emitting_address'),
                   {PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES}
               ) candidate(resolver_address)
               WHERE event.event_kind IN (
                   'ResolverChanged', 'PermissionChanged', 'AliasChanged', 'Upgraded'
               )
                 AND lower(candidate.resolver_address) =
                     lower(current.resolver_address)
           )
         ON CONFLICT DO NOTHING",
    );
    sqlx::query(&passthrough_scope)
        .bind(chain_id)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to classify record-only resolver scope", error)
        })?;
    Ok(())
}
