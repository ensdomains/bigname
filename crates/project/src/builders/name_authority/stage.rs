use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn prepare(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    bind_resource_events(transaction).await?;
    registry_only_handoffs(transaction).await?;
    ownerless_registry(transaction).await
}

/// One row per registry-only binding: the binding it replaced and the BaseRegistrar lease the
/// name has under it. Both the authority-event window and the released-tombstone rule read this
/// table, so they agree on which lease a registry-only binding stands for.
///
/// A registry-only binding opens when a registrar token is transferred without `reclaim`: the
/// registry keeps the owner the registrar wrote, so the name is bound to a registry-only
/// resource while its lease goes on under it. The replaced binding is the latest same-arm binding
/// strictly before the registry-only one, and to begin with the name's lease is that binding's
/// resource. The association moves to a successor lease when a controller grants the same name
/// again with `registerOnly`, which mints a new token and writes the expiry without touching the
/// registry: the registry-only binding stays open and the successor lease never gets a binding
/// of its own. Exactly one grant qualifies: an `ens_v1_registrar_l1` `RegistrationGranted` of
/// registrar authority kind that carries the name and the surface's namehash on another
/// resource, positioned after the binding opened and after a `RegistrationReleased` of the
/// replaced lease; the latest such grant is the lease. A grant before the binding opened (an
/// earlier lease of the name) or before the replaced lease was released never qualifies, and a
/// grant by `register` writes the registry in its own transaction, so it opens a binding of its
/// own and is not read here.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
async fn registry_only_handoffs(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in [
        REGISTRY_ONLY_HANDOFFS,
        "/* project:builders.name_authority.stage.registry_only_handoffs.index_registry_only_handoffs_surface_binding_id */ CREATE INDEX ON project_registry_only_handoffs (surface_binding_id)",
        "/* project:builders.name_authority.stage.registry_only_handoffs.index_registry_only_handoffs_logical_name_id */ CREATE INDEX ON project_registry_only_handoffs (logical_name_id)",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to stage registry-only handoffs", error)
            })?;
    }
    Ok(())
}

const REGISTRY_ONLY_HANDOFFS: &str = "/* project:builders.name_authority.stage.registry_only_handoffs.create_registry_only_handoffs */
    CREATE TEMP TABLE project_registry_only_handoffs ON COMMIT DROP AS
    SELECT binding.logical_name_id, binding.surface_binding_id, binding.authority_arm,
           binding.resource_id, binding.block_number,
           COALESCE((binding.provenance ->> 'transaction_index')::bigint, -1)
               AS transaction_index,
           COALESCE((binding.provenance ->> 'log_index')::bigint, -1) AS log_index,
           predecessor.resource_id AS predecessor_resource_id,
           predecessor.block_number AS predecessor_block_number,
           COALESCE((predecessor.provenance ->> 'transaction_index')::bigint, -1)
               AS predecessor_transaction_index,
           COALESCE((predecessor.provenance ->> 'log_index')::bigint, -1)
               AS predecessor_log_index,
           COALESCE(successor.resource_id, predecessor.resource_id) AS lease_resource_id
    FROM project_binding_candidates binding
    JOIN project_surfaces surface
      ON surface.logical_name_id = binding.logical_name_id
    JOIN LATERAL (
        SELECT predecessor.resource_id, predecessor.block_number, predecessor.provenance
        FROM project_binding_candidates predecessor
        WHERE predecessor.logical_name_id = binding.logical_name_id
          AND predecessor.authority_arm = binding.authority_arm
          AND (
              predecessor.block_number,
              COALESCE((predecessor.provenance ->> 'transaction_index')::bigint, -1),
              COALESCE((predecessor.provenance ->> 'log_index')::bigint, -1)
          ) < (
              binding.block_number,
              COALESCE((binding.provenance ->> 'transaction_index')::bigint, -1),
              COALESCE((binding.provenance ->> 'log_index')::bigint, -1)
          )
        ORDER BY predecessor.block_number DESC,
                 COALESCE((predecessor.provenance ->> 'transaction_index')::bigint, -1) DESC,
                 COALESCE((predecessor.provenance ->> 'log_index')::bigint, -1) DESC,
                 predecessor.surface_binding_id DESC
        LIMIT 1
    ) predecessor ON TRUE
    LEFT JOIN LATERAL (
        SELECT successor_grant.resource_id
        FROM project_events successor_grant
        WHERE binding.authority_arm = 'ens_v1'
          AND successor_grant.logical_name_id = binding.logical_name_id
          AND successor_grant.resource_id IS NOT NULL
          AND successor_grant.resource_id <> predecessor.resource_id
          AND successor_grant.source_family = 'ens_v1_registrar_l1'
          AND successor_grant.event_kind = 'RegistrationGranted'
          AND COALESCE(NULLIF(successor_grant.after_state ->> 'authority_kind', ''), 'registrar')
              = 'registrar'
          AND lower(successor_grant.after_state ->> 'namehash') = lower(surface.namehash)
          AND (
              successor_grant.block_number,
              COALESCE(successor_grant.transaction_index, -1),
              COALESCE(successor_grant.log_index, -1)
          ) > (
              binding.block_number,
              COALESCE((binding.provenance ->> 'transaction_index')::bigint, -1),
              COALESCE((binding.provenance ->> 'log_index')::bigint, -1)
          )
          AND EXISTS (
              SELECT 1
              FROM project_events release
              WHERE release.logical_name_id = binding.logical_name_id
                AND release.resource_id = predecessor.resource_id
                AND release.source_family = 'ens_v1_registrar_l1'
                AND release.event_kind = 'RegistrationReleased'
                AND (
                    release.block_number,
                    COALESCE(release.transaction_index, -1),
                    COALESCE(release.log_index, -1)
                ) < (
                    successor_grant.block_number,
                    COALESCE(successor_grant.transaction_index, -1),
                    COALESCE(successor_grant.log_index, -1)
                )
          )
        ORDER BY successor_grant.block_number DESC,
                 COALESCE(successor_grant.transaction_index, -1) DESC,
                 COALESCE(successor_grant.log_index, -1) DESC,
                 successor_grant.normalized_event_id DESC
        LIMIT 1
    ) successor ON TRUE
    WHERE EXISTS (
        SELECT 1
        FROM project_events epoch
        WHERE epoch.logical_name_id = binding.logical_name_id
          AND epoch.resource_id = binding.resource_id
          AND epoch.event_kind = 'AuthorityEpochChanged'
          AND epoch.after_state ->> 'authority_kind' = 'registry_only'
    )";

/// Names the `.eth` BaseRegistrar lifecycle rows that were written before the label was known.
/// Rows of every other source family keep the name Interpret gave them, or none.
///
/// A row is named in one of two ways, both an exact match on the registrar resource and the
/// namehash: through a binding of that resource to the name, or through the registrar lease a
/// `NameWrapped` row of the name recorded in `wrapped_registrar_resource_id`. The second way
/// leaves out the registrar transfer that moves the token into the NameWrapper in the wrap's own
/// transaction: it names the NameWrapper contract, not a holder.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L265 @ ens_v1@91c966f)
async fn bind_resource_events(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in [
        "/* project:builders.name_authority.stage.bind_resource_events.update_events */ UPDATE project_events event SET logical_name_id = binding.logical_name_id
         FROM project_binding_candidates binding JOIN project_surfaces surface
           ON surface.logical_name_id = binding.logical_name_id
         WHERE event.logical_name_id IS NULL AND event.resource_id = binding.resource_id
           AND event.source_family = 'ens_v1_registrar_l1'
           AND event.event_kind IN (
               'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased',
               'ExpiryChanged', 'TokenControlTransferred'
           )
           AND lower(surface.namehash) = lower(event.after_state ->> 'namehash')",
        BIND_WRAPPER_LINKED_EVENTS,
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to bind resource-keyed events", error)
            })?;
    }
    Ok(())
}

const BIND_WRAPPER_LINKED_EVENTS: &str =
    "/* project:builders.name_authority.stage.bind_wrapper_linked_events */
    UPDATE project_events event SET logical_name_id = wrapper.logical_name_id
    FROM project_events wrapper
    WHERE event.logical_name_id IS NULL
      AND event.source_family = 'ens_v1_registrar_l1'
      AND event.event_kind IN (
          'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased',
          'ExpiryChanged', 'TokenControlTransferred'
      )
      AND wrapper.source_family = 'ens_v1_wrapper_l1'
      AND wrapper.event_kind = 'SurfaceBound'
      AND wrapper.logical_name_id IS NOT NULL
      AND wrapper.after_state ->> 'wrapped_registrar_resource_id' = event.resource_id::text
      AND lower(wrapper.after_state ->> 'node') = lower(event.after_state ->> 'namehash')
      AND (
          event.event_kind <> 'TokenControlTransferred'
          OR event.transaction_hash IS DISTINCT FROM wrapper.transaction_hash
          OR lower(event.after_state ->> 'to') IS DISTINCT FROM
             lower(wrapper.raw_fact_ref ->> 'emitting_address')
      )";

async fn ownerless_registry(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "/* project:builders.name_authority.stage.ownerless_registry */ CREATE TEMP TABLE project_latest_registry_owner ON COMMIT DROP AS
         SELECT latest.logical_name_id, latest.resource_id, latest.owner_getter,
                latest.owner_getter_reason
         FROM (
             SELECT DISTINCT ON (COALESCE(
                        event.logical_name_id,
                        linked.logical_name_id,
                        surface.logical_name_id
                    ))
                    COALESCE(
                        event.logical_name_id,
                        linked.logical_name_id,
                        surface.logical_name_id
                    ) AS logical_name_id,
                    event.resource_id,
                    event.after_state ->> 'owner_getter' AS owner_getter,
                    event.after_state ->> 'owner_getter_reason' AS owner_getter_reason
             FROM project_events event
             LEFT JOIN LATERAL (
                 SELECT candidate.logical_name_id
                 FROM project_events candidate
                 WHERE event.logical_name_id IS NULL
                   AND candidate.logical_name_id IS NOT NULL
                   AND candidate.resource_id = event.resource_id
                   AND candidate.source_family = event.source_family
                 ORDER BY candidate.block_number DESC NULLS LAST,
                          candidate.transaction_index DESC NULLS LAST,
                          candidate.log_index DESC NULLS LAST,
                          candidate.event_identity DESC
                 LIMIT 1
             ) linked ON TRUE
             LEFT JOIN project_surfaces surface
               ON event.logical_name_id IS NULL
              AND surface.namespace = event.namespace
              AND surface.visibility_state = 'active'
              AND lower(surface.namehash) = lower(COALESCE(
                      NULLIF(event.after_state ->> 'child_node', ''),
                      NULLIF(event.after_state ->> 'node', '')
                  ))
             WHERE event.event_kind = 'AuthorityTransferred'
               AND event.source_family IN (
                   'ens_v1_registry_l1', 'basenames_base_registry'
               )
               AND COALESCE(
                       event.logical_name_id,
                       linked.logical_name_id,
                       surface.logical_name_id
                   ) IS NOT NULL
             ORDER BY COALESCE(
                          event.logical_name_id,
                          linked.logical_name_id,
                          surface.logical_name_id
                      ),
                      event.block_number DESC NULLS LAST,
                      event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST,
                      event.event_identity DESC
         ) latest
         WHERE latest.owner_getter =
               '0x0000000000000000000000000000000000000000'",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to stage ownerless registry names", error))?;
    Ok(())
}

/// What `build` runs before `AUTHORITY_EVENTS`; the plan test stages the same way.
pub(super) const SELECTED_BINDINGS: [&str; 5] = [
    // Temporary tables are never analyzed automatically, and the builders read every table
    // staged here once per name. Without statistics the planner assumes a handful of rows
    // and joins them by nested loop.
    "/* project:builders.name_authority.stage.selected_bindings.key_name_authority */ ALTER TABLE project_name_authority ADD PRIMARY KEY (logical_name_id)",
    "/* project:builders.name_authority.stage.selected_bindings.analyze_name_authority */ ANALYZE project_name_authority",
    "/* project:builders.name_authority.stage.selected_bindings.create_bindings */ CREATE TEMP TABLE project_bindings ON COMMIT DROP AS
     SELECT candidate.*
     FROM project_name_authority authority
     JOIN project_binding_candidates candidate
       ON candidate.surface_binding_id = authority.selected_binding_id",
    "/* project:builders.name_authority.stage.selected_bindings.index_bindings_logical_name_id */ CREATE INDEX ON project_bindings (logical_name_id)",
    "/* project:builders.name_authority.stage.selected_bindings.analyze_bindings */ ANALYZE project_bindings",
];
pub(super) const AUTHORITY_EVENTS: &str = include_str!("authority_events.sql");

pub(super) async fn build(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in SELECTED_BINDINGS.into_iter().chain([
        AUTHORITY_EVENTS,
        // Each staged event joins at most one name, so the event id is the table's key.
        "/* project:builders.name_authority.stage.build.key_authority_events */ ALTER TABLE project_authority_events ADD PRIMARY KEY (normalized_event_id)",
        "/* project:builders.name_authority.stage.build.index_authority_events_logical_name_id */ CREATE INDEX ON project_authority_events (logical_name_id, normalized_event_id)",
        "/* project:builders.name_authority.stage.build.index_authority_events_resource_id */ CREATE INDEX ON project_authority_events (resource_id, normalized_event_id)",
        "/* project:builders.name_authority.stage.build.analyze_authority_events */ ANALYZE project_authority_events",
        include_str!("registration_events.sql"),
        "/* project:builders.name_authority.stage.build.index_registration_events_logical_name_id */ CREATE INDEX ON project_registration_events (logical_name_id, normalized_event_id)",
        "/* project:builders.name_authority.stage.build.analyze_registration_events */ ANALYZE project_registration_events",
        "/* project:builders.name_authority.stage.build.create_name_serving */ CREATE TEMP TABLE project_name_serving ON COMMIT DROP AS
         SELECT authority.logical_name_id,
                pointer.resource_id AS serving_resource_id,
                pointer.chain_id AS resolver_chain_id,
                lower(pointer.after_state ->> 'resolver') AS resolver_address,
                pointer.normalized_event_id AS pointer_event_id,
                pointer.event_identity AS pointer_event_identity,
                pointer.block_number AS pointer_block_number,
                pointer.transaction_index AS pointer_transaction_index,
                pointer.log_index AS pointer_log_index,
                'retained_registry_resolver_pointer'::text AS read_reachability_basis,
                authority.owner_getter_reason
         FROM project_name_authority authority
         JOIN LATERAL (
             SELECT event.*
             FROM project_events event
             WHERE event.logical_name_id = authority.logical_name_id
               AND event.event_kind = 'ResolverChanged'
               AND event.source_family IN (
                   'ens_v1_registry_l1', 'basenames_base_registry'
               )
               AND event.resource_id = authority.ownerless_registry_resource_id
             ORDER BY event.block_number DESC NULLS LAST,
                      event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST,
                      event.event_identity DESC
             LIMIT 1
         ) pointer ON TRUE
         JOIN project_resources resource
           ON resource.resource_id = pointer.resource_id
          AND resource.token_lineage_id IS NULL
         WHERE authority.known_ownerless_registry
           AND NULLIF(lower(pointer.after_state ->> 'resolver'), '') IS NOT NULL
           AND lower(pointer.after_state ->> 'resolver') <>
               '0x0000000000000000000000000000000000000000'
         UNION ALL
         -- An ENSv2 root-registry TLD whose registration was never observed has a token resource
         -- and a resolver pointer but no surface binding, so no authority is selected. The root
         -- registry keys the pointer by token and returns it while the label is unexpired
         -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L150-L155 @ ens_v2@a971bd64)
         -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64);
         -- the pointer serves as the TLD's serving resource without projecting authority. A
         -- reservation (owner zero, `LabelReserved`) does not withdraw it: the root registry sets
         -- and returns the reservation's resolver the same way
         -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L463-L478 @ ens_v2@a971bd64).
         -- The state-derived expiry clear and release name the resource but no logical name, so the
         -- resource is followed from the name-linked pointer; a release at or after the pointer, or
         -- a later zero/null pointer, withdraws it.
         SELECT authority.logical_name_id,
                pointer.resource_id AS serving_resource_id,
                pointer.chain_id AS resolver_chain_id,
                lower(pointer.after_state ->> 'resolver') AS resolver_address,
                pointer.normalized_event_id AS pointer_event_id,
                pointer.event_identity AS pointer_event_identity,
                pointer.block_number AS pointer_block_number,
                pointer.transaction_index AS pointer_transaction_index,
                pointer.log_index AS pointer_log_index,
                'root_registry_resolver_pointer'::text AS read_reachability_basis,
                NULL::text AS owner_getter_reason
         FROM project_name_authority authority
         JOIN LATERAL (
             SELECT candidates.* FROM (
                 SELECT event.* FROM project_events event
                 WHERE event.logical_name_id = authority.logical_name_id
                   AND event.event_kind = 'ResolverChanged'
                   AND event.source_family = 'ens_v2_root_l1'
                   AND event.resource_id IS NOT NULL
                 UNION ALL
                 SELECT event.* FROM (
                     SELECT DISTINCT linked.resource_id FROM project_events linked
                     WHERE linked.logical_name_id = authority.logical_name_id
                       AND linked.source_family = 'ens_v2_root_l1'
                       AND linked.event_kind = 'ResolverChanged'
                       AND linked.resource_id IS NOT NULL
                 ) linked
                 JOIN project_events event ON event.resource_id = linked.resource_id
                 WHERE event.logical_name_id IS NULL
                   AND event.event_kind = 'ResolverChanged'
                   AND event.source_family = 'ens_v2_root_l1'
                   AND event.resource_id IS NOT NULL
             ) candidates
             ORDER BY candidates.block_number DESC NULLS LAST,
                      candidates.transaction_index DESC NULLS LAST,
                      candidates.log_index DESC NULLS LAST,
                      candidates.event_identity DESC
             LIMIT 1
         ) pointer ON TRUE
         WHERE authority.unsupported_reason = 'current_authority_not_projected'
           AND authority.selected_binding_id IS NULL
           AND NULLIF(lower(pointer.after_state ->> 'resolver'), '') IS NOT NULL
           AND lower(pointer.after_state ->> 'resolver') <>
               '0x0000000000000000000000000000000000000000'
           AND NOT EXISTS (
               SELECT 1 FROM project_events release
               WHERE release.resource_id = pointer.resource_id
                 AND release.source_family = 'ens_v2_root_l1'
                 AND release.event_kind = 'RegistrationReleased'
                 AND (
                     release.block_number,
                     COALESCE(release.transaction_index, -1),
                     COALESCE(release.log_index, -1)
                 ) >= (
                     pointer.block_number,
                     COALESCE(pointer.transaction_index, -1),
                     COALESCE(pointer.log_index, -1)
                 )
           )",
        "/* project:builders.name_authority.stage.build.index_name_serving_logical_name_id */ CREATE UNIQUE INDEX ON project_name_serving (logical_name_id)",
        "/* project:builders.name_authority.stage.build.index_name_serving_serving_resource_id */ CREATE INDEX ON project_name_serving (serving_resource_id)",
        "/* project:builders.name_authority.stage.build.index_name_serving_resolver_chain_id */ CREATE INDEX ON project_name_serving (resolver_chain_id, resolver_address)",
        "/* project:builders.name_authority.stage.build.analyze_name_serving */ ANALYZE project_name_serving",
    ]) {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to stage selected name authority", error)
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    /// A full rebuild runs this statement once over every event and every name. With anything but
    /// a plain equality between the two, Postgres can neither hash- nor merge-join them and
    /// instead re-reads every row that carries no name once per name. This checks the SQL text
    /// only; `plan_tests` checks the plan Postgres chooses.
    #[test]
    fn authority_events_join_names_by_equality_only() {
        let statement = include_str!("authority_events.sql");
        let (join, _filter) = statement
            .split_once("\nWHERE (")
            .expect("the statement has a WHERE clause");
        assert!(
            join.trim_end().ends_with(
                "FROM project_events event\nJOIN project_name_authority authority\n  \
                 ON authority.logical_name_id = event.logical_name_id"
            ),
            "the events-to-names join must be a single equality on logical_name_id:\n{join}"
        );
        assert!(
            !statement.contains("event.logical_name_id IS NULL"),
            "rows without a name are named while staging, not searched by this statement"
        );
    }

    /// The registrar lease a `NameWrapped` row recorded is attached while staging, with the same
    /// restriction as the binding match: `.eth` BaseRegistrar lifecycle rows only.
    #[test]
    fn staging_names_only_base_registrar_lifecycle_rows() {
        assert_eq!(
            super::BIND_WRAPPER_LINKED_EVENTS
                .matches("event.source_family = 'ens_v1_registrar_l1'")
                .count(),
            1
        );
        assert!(super::BIND_WRAPPER_LINKED_EVENTS.contains(
            "wrapper.after_state ->> 'wrapped_registrar_resource_id' = event.resource_id::text"
        ));
    }
}
