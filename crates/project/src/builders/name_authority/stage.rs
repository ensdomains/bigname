use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn prepare(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    bind_resource_events(transaction).await?;
    ownerless_registry(transaction).await
}

/// Names the `.eth` BaseRegistrar lifecycle rows that were written before the label was known.
/// Rows of every other source family keep the name Interpret gave them, or none.
///
/// A row is named in one of two ways, both an exact match on the registrar resource and the
/// namehash: through a binding of that resource to the name, or through the registrar lease a
/// `NameWrapped` row of the name recorded in `wrapped_registrar_resource_id`. The second way
/// leaves out the registrar transfer that moves the token into the NameWrapper in the wrap's own
/// transaction: it names the NameWrapper contract, not a holder. Rows named the second way are
/// listed in `project_wrapper_linked_events`.
async fn bind_resource_events(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in [
        "UPDATE project_events event SET logical_name_id = binding.logical_name_id
         FROM project_binding_candidates binding JOIN project_surfaces surface
           ON surface.logical_name_id = binding.logical_name_id
         WHERE event.logical_name_id IS NULL AND event.resource_id = binding.resource_id
           AND event.source_family = 'ens_v1_registrar_l1'
           AND event.event_kind IN (
               'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased',
               'ExpiryChanged', 'TokenControlTransferred'
           )
           AND lower(surface.namehash) = lower(event.after_state ->> 'namehash')",
        "CREATE TEMP TABLE project_wrapper_linked_events (
             normalized_event_id bigint PRIMARY KEY
         ) ON COMMIT DROP",
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

const BIND_WRAPPER_LINKED_EVENTS: &str = "
    WITH named AS (
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
          )
        RETURNING event.normalized_event_id
    )
    INSERT INTO project_wrapper_linked_events
    SELECT DISTINCT normalized_event_id FROM named";

async fn ownerless_registry(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "CREATE TEMP TABLE project_latest_registry_owner ON COMMIT DROP AS
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

pub(super) async fn build(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in [
        "ALTER TABLE project_name_authority ADD PRIMARY KEY (logical_name_id)",
        "CREATE TEMP TABLE project_bindings ON COMMIT DROP AS
         SELECT candidate.*
         FROM project_name_authority authority
         JOIN project_binding_candidates candidate
           ON candidate.surface_binding_id = authority.selected_binding_id",
        "CREATE INDEX ON project_bindings (logical_name_id)",
        include_str!("authority_events.sql"),
        "CREATE INDEX ON project_authority_events (logical_name_id, normalized_event_id)",
        "CREATE INDEX ON project_authority_events (resource_id, normalized_event_id)",
        include_str!("registration_events.sql"),
        "CREATE INDEX ON project_registration_events (logical_name_id, normalized_event_id)",
        "CREATE TEMP TABLE project_name_serving ON COMMIT DROP AS
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
             SELECT event.*
             FROM project_events event
             WHERE event.event_kind = 'ResolverChanged'
               AND event.source_family = 'ens_v2_root_l1'
               AND event.resource_id IS NOT NULL
               AND (
                   event.logical_name_id = authority.logical_name_id
                   OR (
                       event.logical_name_id IS NULL
                       AND EXISTS (
                           SELECT 1 FROM project_events linked
                           WHERE linked.logical_name_id = authority.logical_name_id
                             AND linked.resource_id = event.resource_id
                             AND linked.source_family = 'ens_v2_root_l1'
                             AND linked.event_kind = 'ResolverChanged'
                       )
                   )
               )
             ORDER BY event.block_number DESC NULLS LAST,
                      event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST,
                      event.event_identity DESC
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
        "CREATE UNIQUE INDEX ON project_name_serving (logical_name_id)",
        "CREATE INDEX ON project_name_serving (serving_resource_id)",
        "CREATE INDEX ON project_name_serving (resolver_chain_id, resolver_address)",
    ] {
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
    /// instead re-reads every row that carries no name once per name.
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
