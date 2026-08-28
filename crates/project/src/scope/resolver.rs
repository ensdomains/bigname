use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result, resolver_address::PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES};

pub(super) async fn include_registry_read_anchors(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_resources
         SELECT DISTINCT pointer.resource_id
         FROM project_scope_names scope
         JOIN normalized_events pointer USING (logical_name_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = pointer.chain_id
          AND lineage.block_number = pointer.block_number
          AND lineage.block_hash = pointer.block_hash
         WHERE pointer.chain_id = $1
           AND pointer.block_number <= $2
           AND pointer.event_kind = 'ResolverChanged'
           AND pointer.source_family IN (
               'ens_v1_registry_l1', 'basenames_base_registry'
           )
           AND pointer.resource_id IS NOT NULL
           AND pointer.consumer_visibility = 'activated'
           AND pointer.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to close registry read-anchor resource scope", error)
    })?;
    Ok(())
}

pub(super) async fn include_permission_resources(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // A release-fingerprint rebuild stores one event reference for every resolver family and
    // relevant input kind. Later incremental builds restage those events instead of rescanning every resource
    // that ever named a shared resolver. Their resources are builder input only: they never enter
    // delete-and-publish resource scope.
    sqlx::query(
        "INSERT INTO project_scope_resolver_candidate_events (
             normalized_event_id, resource_id
         )
         SELECT event.normalized_event_id, event.resource_id
         FROM resolver_current current
         JOIN project_scope_resolvers scope
           ON lower(scope.resolver_address) = lower(current.resolver_address)
         CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(
             current.provenance -> 'candidate_event_ids', '[]'::jsonb
         )) citation(event_id)
         JOIN normalized_events event
           ON event.normalized_event_id = citation.event_id::bigint
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         LEFT JOIN project_scope_resolver_passthrough passthrough
           ON lower(passthrough.resolver_address) = lower(scope.resolver_address)
         WHERE current.chain_id = $1
           AND event.chain_id = $1
           AND event.block_number <= $2
           AND event.consumer_visibility = 'activated'
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND passthrough.resolver_address IS NULL
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope resolver permission resources", error)
    })?;
    let replacement_candidates = format!(
        "INSERT INTO project_scope_resolver_candidate_events (
             normalized_event_id, resource_id
         )
         SELECT replacement.normalized_event_id, replacement.resource_id
         FROM project_scope_retracted_resolver_evidence retracted
         LEFT JOIN project_scope_resolver_passthrough passthrough
           ON lower(passthrough.resolver_address) = lower(retracted.resolver_address)
         CROSS JOIN LATERAL (
             SELECT event.normalized_event_id, event.resource_id
             FROM normalized_events event
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id
              AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             CROSS JOIN LATERAL (VALUES
                 (CASE WHEN event.event_kind = 'ResolverChanged'
                       THEN event.after_state ->> 'resolver' END),
                 (CASE WHEN event.event_kind = 'ResolverChanged'
                       THEN event.before_state ->> 'resolver' END),
                 (CASE WHEN event.event_kind = 'AliasChanged'
                       THEN COALESCE(
                           event.after_state ->> 'resolver',
                           event.before_state ->> 'resolver',
                           event.raw_fact_ref ->> 'emitting_address'
                       ) END),
                 {PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES}
             ) candidate(resolver_address)
             WHERE event.chain_id = $1
               AND event.block_number <= $2
               AND event.event_kind = retracted.event_kind
               AND event.consumer_visibility = 'activated'
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lower(candidate.resolver_address) = lower(retracted.resolver_address)
               AND CASE
                       WHEN event.source_family LIKE 'ens_v2_%'
                           THEN 'ens_v2_resolver_l1'
                       WHEN event.source_family LIKE 'basenames_%'
                           THEN 'basenames_base_resolver'
                       ELSE 'ens_v1_resolver_l1'
                   END = retracted.source_family
             ORDER BY event.normalized_event_id
             LIMIT 1
         ) replacement
         WHERE passthrough.resolver_address IS NULL
         ON CONFLICT DO NOTHING"
    );
    sqlx::query(&replacement_candidates)
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to replace retracted resolver candidates", error)
        })?;
    sqlx::query(
        "INSERT INTO project_scope_resolver_candidate_resources
         SELECT DISTINCT resource_id
         FROM project_scope_resolver_candidate_events
         WHERE resource_id IS NOT NULL
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to scope resolver candidate input resources", error)
    })?;
    Ok(())
}

pub(super) async fn include_resource_pointers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // A resource slice rebuild embeds its selected resolver classification in record inventory.
    // Scope both the projected pointer and every readable ResolverChanged resolver of a scoped
    // resource. Redo needs the former to retract losing output, while the latter set lets
    // inventory classify whichever pointer's name has the first staged readable surface.
    // Unchanged resolver summaries can be republished without staging their unrelated history.
    sqlx::query("ANALYZE project_scope_resources")
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to analyze resource scope", error))?;
    let permission_history = format!(
        "INSERT INTO project_scope_resolver_permission_history
         SELECT lower(candidate.resolver_address)
         FROM project_scope_resources scope
         JOIN normalized_events event USING (resource_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = event.chain_id
          AND lineage.block_hash = event.block_hash
          AND lineage.block_number = event.block_number
         CROSS JOIN LATERAL (VALUES
             {PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES}
         ) candidate(resolver_address)
         WHERE event.chain_id = $1
           AND event.event_kind = 'PermissionChanged'
           AND event.consumer_visibility = 'activated'
           AND event.block_number <= $2
           AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND candidate.resolver_address IS NOT NULL
           AND btrim(candidate.resolver_address) <> ''
           AND lower(candidate.resolver_address) <>
               '0x0000000000000000000000000000000000000000'
         ON CONFLICT DO NOTHING"
    );
    sqlx::query(&permission_history)
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to scope resolver permission history", error)
        })?;

    sqlx::query(
        "INSERT INTO project_scope_resolvers
         SELECT lower(pointer.resolver_address)
         FROM (
             SELECT inventory.provenance ->> 'resolver_address' AS resolver_address
             FROM record_inventory_current inventory
             JOIN project_scope_resources scope USING (resource_id)
             WHERE inventory.provenance ->> 'chain_id' = $1
             UNION ALL
             SELECT DISTINCT event.after_state ->> 'resolver' AS resolver_address
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
             UNION ALL
             SELECT permission.scope_detail ->> 'resolver_address'
             FROM permissions_current permission
             JOIN project_scope_resources scope USING (resource_id)
             WHERE permission.scope_kind = 'resolver'
               AND permission.scope_detail ->> 'chain_id' = $1
             UNION ALL
             SELECT resolver_address
             FROM project_scope_resolver_permission_history
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
    // Carry-forward relies on three Interpret-side rules: `write_surfaces` in
    // `interpret/src/write/identity_names.rs` rejects a second raw identity for an existing
    // [name surface](../../../../docs/glossary.md#surface-name-surface); `normalized_events` has a
    // foreign key to `name_surfaces`, so an event cannot link
    // to a surface before that surface exists; and the v1 resolver adapter in
    // `adapters/src/schema_v2/protocol/v1/resolver.rs` links a record event only when the surface
    // is already materialized, with no later step that retroactively links an earlier event.
    // Record values and record-version boundaries do not contribute to resolver_current's
    // binding, alias, permission, role, event, or classification summaries. A resource rebuild
    // also needs only its pointer resolver's existing classification. In either case, republish
    // the existing resolver summary at the new target without staging unrelated history. Redo and
    // resolver-entity changes are excluded because their keys are in resolver_dependents.
    sqlx::query("DELETE FROM project_scope_resolver_passthrough")
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to reset record-only resolver scope", error)
        })?;
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
               OR EXISTS (
                   SELECT 1
                   FROM project_scope_resolver_permission_history history
                   WHERE lower(history.resolver_address) =
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
               FROM project_scope_permission_effect_resources resource
               JOIN normalized_events event USING (resource_id)
               JOIN chain_lineage lineage
                 ON lineage.chain_id = event.chain_id
                AND lineage.block_hash = event.block_hash
                AND lineage.block_number = event.block_number
               CROSS JOIN LATERAL (VALUES
                   {PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES}
               ) permission(resolver_address)
               WHERE event.chain_id = $1
                 AND event.event_kind = 'PermissionChanged'
                 AND event.consumer_visibility = 'activated'
                 AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                 AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                 AND lower(permission.resolver_address) =
                     lower(current.resolver_address)
           )
           AND NOT EXISTS (
               SELECT 1 FROM project_changed_events event
               CROSS JOIN LATERAL (VALUES
                   (CASE WHEN event.event_kind IN ('ResolverChanged', 'AliasChanged')
                         THEN event.after_state ->> 'resolver' END),
                   (CASE WHEN event.event_kind IN ('ResolverChanged', 'AliasChanged')
                         THEN event.before_state ->> 'resolver' END),
                   (CASE WHEN event.event_kind = 'Upgraded'
                         THEN event.after_state ->> 'proxy_address' END),
                   (CASE WHEN event.event_kind = 'Upgraded'
                         THEN event.before_state ->> 'proxy_address' END),
                   (CASE WHEN event.event_kind = 'AliasChanged'
                         THEN event.raw_fact_ref ->> 'emitting_address' END),
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

#[cfg(test)]
mod tests {
    #[test]
    fn passthrough_comment_pins_interpret_side_invariants() {
        let source = include_str!("resolver.rs");
        for invariant in [
            "`write_surfaces`",
            "foreign key to `name_surfaces`",
            "links a record event only when the surface",
        ] {
            assert!(source.contains(invariant), "missing invariant {invariant}");
        }
    }
}
