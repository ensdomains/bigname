use sqlx::{Postgres, Transaction};

use super::{
    history::{ANALYZE_HISTORY_SCOPES_SQL, SCOPED_NAME_HISTORY_SQL, SCOPED_PRIMARY_HISTORY_SQL},
    node_record_events::{self, SCOPED_NODE_RECORD_EVENT_IDS_SQL},
};
use crate::{ProjectError, Result};

#[cfg(test)]
mod tests;

pub(super) async fn create(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    super::mirror_evidence::create(transaction).await?;
    for statement in ANALYZE_HISTORY_SCOPES_SQL
        .split(';')
        .filter(|statement| !statement.trim().is_empty())
    {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to analyze scoped history keys", error)
            })?;
    }
    sqlx::query(
        "/* project:stage.event_ids.create_event_ids */ CREATE TEMP TABLE project_event_ids (
             normalized_event_id bigint PRIMARY KEY
         ) ON COMMIT DROP",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to create event identity stage", error))?;

    for statement in [
        "/* project:stage.event_ids.analyze_scope_resources */ ANALYZE project_scope_resources",
        "/* project:stage.event_ids.analyze_scope_ancestors */ ANALYZE project_scope_ancestors",
        "/* project:stage.event_ids.analyze_scope_account_permissions */ ANALYZE project_scope_account_permissions",
        "/* project:stage.event_ids.analyze_scope_resolvers */ ANALYZE project_scope_resolvers",
        "/* project:stage.event_ids.analyze_scope_resolver_passthrough */ ANALYZE project_scope_resolver_passthrough",
        "/* project:stage.event_ids.analyze_scope_resolver_candidate_events */ ANALYZE project_scope_resolver_candidate_events",
        "/* project:stage.event_ids.analyze_declared_resolver_addresses */ ANALYZE project_declared_resolver_addresses",
        "/* project:stage.event_ids.analyze_changed_events */ ANALYZE project_changed_events",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to analyze event scope", error))?;
    }
    node_record_events::prepare(transaction, chain_id, target_block).await?;
    let scoped_event_ids = r#"/* project:stage.event_ids.arms */
        SELECT event.normalized_event_id
        FROM normalized_events event
        WHERE event.chain_id = $1
          AND event.event_kind = 'SourceManifestUpdated'
          AND (event.block_number IS NULL OR event.block_number <= $2)
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_names scope
        JOIN normalized_events event USING (logical_name_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_children scope
        JOIN normalized_events event USING (logical_name_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        -- An ancestor reached only through a changed child's edge stages its own events (migration
        -- boundary, subregistry pointer, ownership) as parent evidence; its other children stay out
        -- of scope.
        SELECT event.normalized_event_id
        FROM project_scope_ancestors scope
        JOIN normalized_events event USING (logical_name_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        -- ENSv2 registration stores the entry's subregistry and emits the label registration
        -- separately. (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L462 @ ens_v2@a971bd64)
        -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L467 @ ens_v2@a971bd64)
        -- The child-edge projection combines those inputs, so rebuilding a scoped parent's row
        -- family stages each current sibling's registrations without widening projection scope.
        SELECT event.normalized_event_id
        FROM project_scope_children scope
        JOIN children_current child
          ON child.parent_logical_name_id = scope.logical_name_id
         AND child.provenance ->> 'chain_id' = $1
        JOIN normalized_events event
          ON event.logical_name_id = child.child_logical_name_id
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind IN (
              'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased'
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_resources scope
        JOIN normalized_events event USING (resource_id)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_account_permissions scope
        JOIN normalized_events event
          ON event.chain_id = scope.chain_id
         AND event.after_state #>> '{scope,authority_kind}' = scope.authority_kind
         AND lower(event.after_state #>> '{scope,authority_contract}') = scope.authority_contract
         AND lower(event.after_state #>> '{scope,owner}') = scope.owner
         AND lower(event.after_state ->> 'subject') = scope.subject
         AND event.after_state ->> 'relation_kind' = scope.relation_kind
        WHERE event.event_kind = 'AccountPermissionChanged'
          AND event.block_number <= $2
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        -- Defensive symmetry with create_identity_views; inventory closure guarantees the names.
        __SCOPED_NODE_RECORD_EVENT_IDS_SQL__
        UNION
        -- Candidate-only resources supply just the resolver evidence consumed by this build.
        -- They remain outside delete-and-publish resource scope.
        SELECT normalized_event_id
        FROM project_mirror_evidence_events
        UNION
        SELECT normalized_event_id
        FROM project_scope_resolver_candidate_events
        UNION
        __SCOPED_NAME_HISTORY_SQL__
        UNION
        SELECT event.normalized_event_id
        FROM normalized_events event
        CROSS JOIN LATERAL (
            VALUES (event.after_state ->> 'to_resource_id'),
                   (event.before_state ->> 'to_resource_id')
        ) candidate(resource_id)
        JOIN project_scope_resources scope
          ON scope.resource_id::text = candidate.resource_id
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind = 'AliasChanged'
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_changed_events event
        CROSS JOIN LATERAL (VALUES
            (CASE WHEN event.event_kind = 'ResolverChanged'
                  THEN event.after_state ->> 'resolver' END),
            (CASE WHEN event.event_kind = 'ResolverChanged'
                  THEN event.before_state ->> 'resolver' END),
            (CASE WHEN event.event_kind = 'PermissionChanged'
                       AND event.after_state #>> '{scope,kind}' = 'resolver'
                  THEN event.after_state #>> '{scope,resolver_address}' END),
            (CASE WHEN event.event_kind = 'PermissionChanged'
                       AND event.before_state #>> '{scope,kind}' = 'resolver'
                  THEN event.before_state #>> '{scope,resolver_address}' END)
        ) candidate(resolver_address)
        JOIN project_scope_resolvers scope
          ON lower(candidate.resolver_address) = lower(scope.resolver_address)
        WHERE event.event_kind IN ('ResolverChanged', 'PermissionChanged')
          AND NOT EXISTS (
              SELECT 1 FROM project_scope_resolver_passthrough passthrough
              WHERE lower(passthrough.resolver_address) =
                    lower(scope.resolver_address)
          )
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_resolvers scope
        JOIN normalized_events event
          ON lower(COALESCE(
                 event.after_state ->> 'resolver',
                 event.before_state ->> 'resolver',
                 event.raw_fact_ref ->> 'emitting_address'
             )) = lower(scope.resolver_address)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind = 'AliasChanged'
          AND NOT EXISTS (
              SELECT 1 FROM project_scope_resolver_passthrough passthrough
              WHERE lower(passthrough.resolver_address) =
                    lower(scope.resolver_address)
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        SELECT event.normalized_event_id
        FROM project_scope_resolvers scope
        JOIN normalized_events event
          ON lower(event.after_state ->> 'proxy_address') =
             lower(scope.resolver_address)
        WHERE event.chain_id = $1 AND event.block_number <= $2
          AND event.event_kind = 'Upgraded'
          AND NOT EXISTS (
              SELECT 1 FROM project_scope_resolver_passthrough passthrough
              WHERE lower(passthrough.resolver_address) =
                    lower(scope.resolver_address)
          )
          AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
        UNION
        __SCOPED_PRIMARY_HISTORY_SQL__
        UNION
        SELECT resolver.normalized_event_id
        FROM (
        SELECT DISTINCT lower(reverse.after_state ->> 'reverse_node') AS node
        FROM project_scope_primary scope
        JOIN normalized_events reverse
          ON reverse.chain_id = $1
         AND reverse.block_number <= $2
         AND reverse.event_kind = 'ReverseChanged'
         AND reverse.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND lower(reverse.after_state ->> 'address') = scope.address
         AND reverse.after_state ->> 'coin_type' = scope.coin_type
         AND reverse.after_state ->> 'namespace' = scope.namespace
        ) reverse_nodes
        JOIN normalized_events resolver
          ON resolver.chain_id = $1
         AND resolver.block_number <= $2
         AND (resolver.event_kind IN ('ResolverChanged', 'RecordVersionChanged')
              OR (resolver.event_kind = 'RecordChanged'
                  AND resolver.after_state ->> 'source_event' = 'NameChanged'))
         AND resolver.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND lower(resolver.after_state ->> 'node') = reverse_nodes.node
        "#;
    // Each arm contributes to the same set. The primary key deduplicates across arms
    // inside this repeatable-read transaction, without one global UNION sort. Separate
    // statements also retain useful per-arm timings in PostgreSQL's slow query log.
    for (arm_index, arm) in scoped_event_ids.split("\n        UNION\n").enumerate() {
        let arm = arm
            .replace(
                "__SCOPED_NODE_RECORD_EVENT_IDS_SQL__",
                SCOPED_NODE_RECORD_EVENT_IDS_SQL,
            )
            .replace("__SCOPED_NAME_HISTORY_SQL__", SCOPED_NAME_HISTORY_SQL)
            .replace("__SCOPED_PRIMARY_HISTORY_SQL__", SCOPED_PRIMARY_HISTORY_SQL);
        let statement = format!(
            "/* project:stage.event_ids.arm_{arm_index} */ INSERT INTO project_event_ids SELECT normalized_event_id FROM ({arm}) selected \
             WHERE $1::text IS NOT NULL AND $2::bigint IS NOT NULL ON CONFLICT DO NOTHING"
        );
        sqlx::query(&statement)
            .bind(chain_id)
            .bind(target_block)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to stage scoped event identities", error)
            })?;
    }
    for statement in [
        "/* project:stage.event_ids.analyze_event_ids */ ANALYZE project_event_ids",
        "/* project:stage.event_ids.drop_node_record_history */ DROP TABLE project_node_record_history",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to finish event identity stage", error)
            })?;
    }
    Ok(())
}
