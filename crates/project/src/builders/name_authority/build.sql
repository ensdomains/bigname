/* project:builders.name_authority.build */
        CREATE TEMP TABLE project_name_authority ON COMMIT DROP AS
        WITH target_time AS (
            SELECT block_timestamp + interval '1 second' AS cutoff
            FROM chain_lineage
            WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
        ), open_bindings AS (
            SELECT binding.*
            FROM project_binding_candidates binding
            CROSS JOIN target_time
            WHERE binding.active_from < target_time.cutoff
              AND (binding.active_to IS NULL OR binding.active_to >= target_time.cutoff)
        ), arm_summary AS (
            SELECT logical_name_id,
                   count(DISTINCT authority_arm) AS arm_count,
                   min(authority_arm) AS sole_arm,
                   bool_or(authority_arm = 'ens_v1') AS has_ens_v1,
                   bool_or(authority_arm = 'ens_v2') AS has_ens_v2
            FROM open_bindings
            GROUP BY logical_name_id
        ), event_arms AS (
            SELECT event.logical_name_id, event.normalized_event_id, CASE WHEN event.source_family LIKE 'ens_v1_%' THEN 'ens_v1'
                WHEN event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1') THEN 'ens_v2'
                WHEN event.source_family LIKE 'basenames_%' THEN 'basenames' END AS authority_arm
            FROM project_events event WHERE event.logical_name_id IS NOT NULL
              AND event.event_kind IN ('RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged', 'AuthorityTransferred', 'TokenControlTransferred', 'AuthorityEpochChanged')
              -- A [premigration reservation](../../../../docs/glossary.md#premigration-reservation) is resolver-bearing but not ENSv2 authority.
              -- Its expiry maintenance cannot vote, and its release qualifies only from a matching binding at or before that release.
              -- (upstream: .refs/ens_v2/contracts/src/registrar/BatchRegistrar.sol:L48-L71 @ ens_v2@a971bd64)
              -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
              -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L195-L207 @ ens_v2@a971bd64)
              AND NOT (event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1') AND (
                  event.event_kind = 'ExpiryChanged' OR (event.event_kind = 'RegistrationReleased' AND NOT EXISTS (
                      SELECT 1 FROM project_binding_candidates binding
                      WHERE binding.logical_name_id = event.logical_name_id AND binding.authority_arm = 'ens_v2'
                        AND binding.resource_id = event.resource_id
                        AND (binding.block_number, COALESCE((binding.provenance ->> 'transaction_index')::bigint, -1), COALESCE((binding.provenance ->> 'log_index')::bigint, -1))
                            <= (event.block_number, COALESCE(event.transaction_index, -1), COALESCE(event.log_index, -1))))))
        ), event_arm_summary AS (
            SELECT logical_name_id,
                   count(DISTINCT authority_arm) AS arm_count,
                   min(authority_arm) AS sole_arm,
                   bool_or(authority_arm = 'ens_v1') AS has_ens_v1,
                   bool_or(authority_arm = 'ens_v2') AS has_ens_v2
            FROM event_arms
            WHERE authority_arm IS NOT NULL
            GROUP BY logical_name_id
        ), latest_v2_binding AS (
            SELECT DISTINCT ON (binding.logical_name_id)
                   binding.logical_name_id, binding.resource_id
            FROM project_binding_candidates binding
            WHERE binding.authority_arm = 'ens_v2'
            ORDER BY binding.logical_name_id, binding.block_number DESC,
                     COALESCE((binding.provenance ->> 'transaction_index')::bigint, -1) DESC,
                     COALESCE((binding.provenance ->> 'log_index')::bigint, -1) DESC,
                     binding.surface_binding_id DESC
        ), latest_v2_lifecycle AS (
            -- The latest lifecycle fact of each arm decides a name that neither arm holds now.
            -- For ENSv2 that is the latest grant, renewal, release or reservation of the
            -- registration the name was last bound to. `unregister` burns the token and sets its
            -- expiry to now, so a released ENSv2 registration reports as expired and available.
            -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L195-L207 @ ens_v2@a971bd64)
            SELECT DISTINCT ON (fact.logical_name_id)
                   fact.logical_name_id, fact.resource_id, fact.event_kind,
                   fact.block_number, fact.transaction_index, fact.log_index
            FROM (
                SELECT binding.logical_name_id, event.resource_id, event.event_kind,
                       event.block_number, event.transaction_index, event.log_index,
                       event.normalized_event_id
                FROM project_events event
                JOIN latest_v2_binding binding
                  ON binding.resource_id = event.resource_id
                 AND (
                      binding.logical_name_id = event.logical_name_id
                      -- When Interpret retires a token that has no name any more, it writes the
                      -- path-expiry release on the token's resource without a name. It is still
                      -- the release of the registration the name was last bound to. Only the
                      -- release is taken this way: a nameless renewal of a token cut from its
                      -- path does not give the name back.
                      OR (
                          event.logical_name_id IS NULL
                          AND event.event_kind = 'RegistrationReleased'
                      )
                 )
                WHERE event.source_family IN (
                      'ens_v2_root_l1', 'ens_v2_registry_l1',
                      'ens_v2_registrar_l1'
                  )
                  AND event.event_kind IN (
                      'RegistrationGranted', 'RegistrationRenewed',
                      'RegistrationReleased', 'RegistrationReserved'
                  )
                UNION ALL
                -- A reservation of the name counts whatever its resource. `unregister` of an
                -- owned token bumps its token version, so a later `LabelReserved` carries a
                -- versioned token id, and Interpret writes that reservation with the name but
                -- without the released registration's resource, or any resource.
                -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L201-L205 @ ens_v2@a971bd64)
                -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L649-L651 @ ens_v2@a971bd64)
                -- (upstream: .refs/ens_v2/contracts/src/utils/LibLabel.sol:L15-L17 @ ens_v2@a971bd64)
                -- The hand-back lasts only while that reservation is live. When it is unregistered
                -- or lapses, Interpret writes its end as a named release without a resource;
                -- a registration always has a resource and a path-cut release carries it, so a
                -- named ENSv2 release without one is a reservation's end. The label is then
                -- available: the registry answers a zero resolver for it, and a WrapperRegistry
                -- never falls back to ENSv1 once the expiry is nonzero. The released
                -- registration the name was last bound to stands again as its tombstone.
                -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L196-L207 @ ens_v2@a971bd64)
                -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
                -- (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L294-L297 @ ens_v2@a971bd64)
                SELECT event.logical_name_id,
                       CASE WHEN event.event_kind = 'RegistrationReleased'
                           THEN binding.resource_id ELSE event.resource_id END,
                       event.event_kind,
                       event.block_number, event.transaction_index, event.log_index,
                       event.normalized_event_id
                FROM project_events event
                JOIN latest_v2_binding binding
                  ON binding.logical_name_id = event.logical_name_id
                WHERE event.source_family IN (
                      'ens_v2_root_l1', 'ens_v2_registry_l1',
                      'ens_v2_registrar_l1'
                  )
                  AND (
                      (event.event_kind = 'RegistrationReserved'
                       AND event.resource_id IS DISTINCT FROM binding.resource_id)
                      OR (event.event_kind = 'RegistrationReleased'
                          AND event.resource_id IS NULL)
                  )
            ) fact
            -- A reservation whose own expiry is already at or before its block's timestamp is
            -- never live: an ownerless entry may be written with a past nonzero expiry, and the
            -- registry reports an entry as available once `block.timestamp >= expiry`. Interpret
            -- writes its state-derived release in the same block at the block boundary, with no
            -- transaction or log index, so by position the release would sort before the
            -- reservation it ends. The reservation takes no part and its release does. The expiry
            -- is compared with the reservation's own block, not the target block, because a later
            -- renewal can revive the entry.
            -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L452-L454 @ ens_v2@a971bd64)
            -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L628-L630 @ ens_v2@a971bd64)
            -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L654-L660 @ ens_v2@a971bd64)
            WHERE NOT EXISTS (
                SELECT 1
                FROM project_events reservation
                JOIN chain_lineage lineage
                  ON lineage.chain_id = reservation.chain_id
                 AND lineage.block_hash = reservation.block_hash
                 AND lineage.block_number = reservation.block_number
                WHERE reservation.normalized_event_id = fact.normalized_event_id
                  AND reservation.event_kind = 'RegistrationReserved'
                  -- An expiry that is not a JSON number leaves the reservation in.
                  AND CASE
                          WHEN jsonb_typeof(reservation.after_state -> 'expiry') = 'number'
                              THEN (reservation.after_state ->> 'expiry')::numeric <=
                                   extract(epoch FROM lineage.block_timestamp)
                          ELSE FALSE
                      END
            )
            -- A block-boundary fact has no transaction or log index and sorts first in its
            -- block, as in every other position comparison here.
            ORDER BY fact.logical_name_id, fact.block_number DESC,
                     COALESCE(fact.transaction_index, -1) DESC,
                     COALESCE(fact.log_index, -1) DESC,
                     fact.normalized_event_id DESC
        ), released_v2_authority AS (
            -- A released ENSv2 registration stays with ENSv2 whatever ENSv1 holds (product ruling
            -- of 2026-09-25): when the latest lifecycle fact of the registration the name was last
            -- bound to is its release, by `unregister` or by lapsing at expiry, and no ENSv2
            -- binding is open, the name is the released tombstone even beside a live ENSv1
            -- lease. A later reservation of the name, with or without a resource, is a later
            -- lifecycle fact and defers to ENSv1. The contracts never route a registered label back to ENSv1: `unregister`
            -- writes the release time as the expiry, the registry returns no resolver for an
            -- expired entry, and a WrapperRegistry stops answering with ENSV1Resolver for a label
            -- whose stored expiry is nonzero.
            -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L196-L207 @ ens_v2@a971bd64)
            -- (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
            -- (upstream: .refs/ens_v2/contracts/src/registry/WrapperRegistry.sol:L294-L297 @ ens_v2@a971bd64)
            SELECT lifecycle.logical_name_id,
                   lifecycle.resource_id AS released_v2_resource_id
            FROM latest_v2_lifecycle lifecycle
            WHERE lifecycle.event_kind = 'RegistrationReleased'
              AND NOT EXISTS (
                  SELECT 1 FROM open_bindings open
                  WHERE open.logical_name_id = lifecycle.logical_name_id
                    AND open.authority_arm = 'ens_v2'
              )
        ), migration_history AS (
            -- The latest ENSv1->ENSv2 migration of the name. It is served history (the
            -- migration time and whether the name migrated) and decides nothing about authority:
            -- the ENSv2 registration the migration made is a registration like any other.
            SELECT DISTINCT ON (event.logical_name_id)
                   event.logical_name_id,
                   event.normalized_event_id AS proof_event_id,
                   event.event_identity AS proof_event_identity,
                   event.migration_correlation_ids[1] AS transition_id
            FROM project_events event
            WHERE event.event_kind = 'MigrationApplied'
            ORDER BY event.logical_name_id, event.block_number DESC,
                     event.transaction_index DESC, event.log_index DESC,
                     event.normalized_event_id DESC
        ), latest_v1_lifecycle AS (
            SELECT DISTINCT ON (event.logical_name_id)
                   event.logical_name_id, event.resource_id, event.event_kind,
                   event.source_family,
                   COALESCE(NULLIF(event.after_state ->> 'authority_kind', ''), 'registrar')
                       AS authority_kind,
                   event.block_number, event.transaction_index, event.log_index
            FROM project_events event
            WHERE event.logical_name_id IS NOT NULL
              AND event.resource_id IS NOT NULL
              AND event.source_family LIKE 'ens_v1_%'
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased'
              )
            ORDER BY event.logical_name_id, event.block_number DESC,
                     event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ), registry_only_handoffs AS (
            -- A name whose only open binding is a registry-only binding of the ENSv1 arm, with
            -- the BaseRegistrar lease the name has under it. After a registrar token is
            -- transferred without `reclaim` the registry keeps the owner the registrar wrote, so
            -- the name is bound to a registry-only resource while its lease goes on under it:
            -- the lease of the binding it replaced, or a successor lease granted by
            -- `registerOnly` after that lease was released. `project_registry_only_handoffs`
            -- decides which; the authority-event window reads the same table.
            -- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L118-L152 @ ens_v1@91c966f)
            -- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
            SELECT handoff.logical_name_id, handoff.surface_binding_id, handoff.block_number,
                   handoff.transaction_index, handoff.log_index, handoff.lease_resource_id
            FROM project_registry_only_handoffs handoff
            JOIN open_bindings open
              ON open.surface_binding_id = handoff.surface_binding_id
            WHERE handoff.authority_arm = 'ens_v1'
              AND NOT EXISTS (
                  SELECT 1 FROM open_bindings other
                  WHERE other.logical_name_id = open.logical_name_id
                    AND other.surface_binding_id <> open.surface_binding_id
              )
        ), released_v1_authority AS (
            -- A released ENSv1 lease whose custody was not revived leaves a released tombstone
            -- instead of an unresolved selection: the release is positive proof that the
            -- registration is absent, not a gap in projection. Custody is not revived when the
            -- lease lapsed while wrapped (the NameWrapper's registry entry expired with it, so
            -- nothing current owns the node), and not when the lease lapsed under the
            -- registry-only binding a transfer without `reclaim` opened: the registry still
            -- holds the owner the lapsed lease left behind, but a lapsed lease releases the
            -- name whatever the registry records, as it does for every other lapse. That
            -- registry-only binding then stands for the released lease, as the closed
            -- NameWrapper binding does for a wrapped one. A live registry owner of zero stays
            -- the supported ownerless-registry profile.
            -- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
            -- (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L103 @ ens_v1@91c966f)
            -- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L143-L154 @ ens_v1@91c966f)
            -- (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L268 @ ens_v1@91c966f)
            SELECT lifecycle.logical_name_id, lifecycle.resource_id AS released_v1_resource_id,
                   COALESCE(handoff.surface_binding_id, binding.surface_binding_id)
                       AS released_v1_binding_id
            FROM latest_v1_lifecycle lifecycle
            -- The lease's own binding, or the closed NameWrapper binding that stands for it. A
            -- successor lease granted by `registerOnly` under a registry-only binding has
            -- neither; the registry-only binding stands for it below.
            LEFT JOIN LATERAL (
                SELECT candidate.surface_binding_id
                FROM project_binding_candidates candidate
                WHERE candidate.logical_name_id = lifecycle.logical_name_id
                  AND candidate.authority_arm = 'ens_v1'
                  AND (
                      candidate.resource_id = lifecycle.resource_id
                      -- A lease registered through the NameWrapper never has a binding of its
                      -- own: the name is bound to the wrapper resource. That binding stands for
                      -- the released lease when the wrap recorded the lease, or, where a
                      -- controller event granted the lease after NameWrapped and the wrap
                      -- recorded nothing, when the named grant shares the wrap's transaction.
                      -- (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)
                      OR EXISTS (
                          SELECT 1 FROM project_events wrapper_binding
                          WHERE wrapper_binding.logical_name_id = lifecycle.logical_name_id
                            AND wrapper_binding.resource_id = candidate.resource_id
                            AND wrapper_binding.source_family = 'ens_v1_wrapper_l1'
                            AND wrapper_binding.event_kind = 'SurfaceBound'
                            AND (
                                wrapper_binding.after_state ->> 'wrapped_registrar_resource_id' =
                                    lifecycle.resource_id::text
                                OR EXISTS (
                                    SELECT 1 FROM project_events registration
                                    WHERE registration.resource_id = lifecycle.resource_id
                                      AND registration.source_family = 'ens_v1_registrar_l1'
                                      AND registration.event_kind = 'RegistrationGranted'
                                      AND registration.logical_name_id =
                                          wrapper_binding.logical_name_id
                                      AND registration.transaction_hash =
                                          wrapper_binding.transaction_hash
                                )
                            )
                      )
                  )
                  AND (
                      candidate.block_number,
                      COALESCE((candidate.provenance ->> 'transaction_index')::bigint, -1),
                      COALESCE((candidate.provenance ->> 'log_index')::bigint, -1)
                  ) <= (
                      lifecycle.block_number,
                      COALESCE(lifecycle.transaction_index, -1),
                      COALESCE(lifecycle.log_index, -1)
                  )
                ORDER BY candidate.block_number DESC,
                         COALESCE((candidate.provenance ->> 'transaction_index')::bigint, -1) DESC,
                         COALESCE((candidate.provenance ->> 'log_index')::bigint, -1) DESC,
                         candidate.surface_binding_id DESC
                LIMIT 1
            ) binding ON TRUE
            -- The lease that lapsed under a registry-only binding: exactly the lease the name
            -- has under that binding (the lease of the binding it replaced, or the successor
            -- lease granted by `registerOnly` once that lease was released), released by a
            -- registrar row that arrived after the binding opened. Those are the lifecycle rows
            -- the authority-event window admits past its position. A release of any other lease
            -- carrying the name (an earlier lease, released before or after the name was
            -- registered again, or the replaced lease once a successor lease is the name's)
            -- selects nothing, and neither does a release at the position where a
            -- registry-only binding opened: that release handed the name over itself.
            LEFT JOIN registry_only_handoffs handoff
              ON handoff.logical_name_id = lifecycle.logical_name_id
             AND handoff.lease_resource_id = lifecycle.resource_id
             AND lifecycle.source_family = 'ens_v1_registrar_l1'
             AND lifecycle.authority_kind = 'registrar'
             AND (handoff.block_number, handoff.transaction_index, handoff.log_index) < (
                 lifecycle.block_number,
                 COALESCE(lifecycle.transaction_index, -1),
                 COALESCE(lifecycle.log_index, -1)
             )
            WHERE lifecycle.event_kind = 'RegistrationReleased'
              AND (
                  handoff.surface_binding_id IS NOT NULL
                  OR (
                      binding.surface_binding_id IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM open_bindings open
                          WHERE open.logical_name_id = lifecycle.logical_name_id
                      )
                  )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_latest_registry_owner ownerless
                  WHERE ownerless.logical_name_id = lifecycle.logical_name_id
              )
        ), decision AS (
            SELECT surface.logical_name_id,
                   CASE
                       -- Follow the chain (docs/adrs/0007-follow-the-chain-ens-authority.md). Only a
                       -- registered ENSv2 entry opens an ENSv2 binding, and it decides the name
                       -- whatever ENSv1 holds, without a proof; a reservation opens none and
                       -- defers to ENSv1.
                       -- (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L58-L85 @ ens_v2_sepolia_20260916@366de741)
                       -- (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)
                       WHEN COALESCE(summary.has_ens_v2, false) THEN 'ens_v2'
                       -- A released or expired ENSv2 registration stays with ENSv2 as a released
                       -- tombstone, before any ENSv1 binding is considered.
                       WHEN released.logical_name_id IS NOT NULL THEN 'ens_v2'
                       WHEN summary.arm_count = 1 THEN summary.sole_arm
                       -- Nothing is open on either arm: ENSv1 history decides first, as it
                       -- would for a name with no ENSv2 entry.
                       WHEN summary.logical_name_id IS NULL
                        AND COALESCE(event_summary.has_ens_v1, false) THEN 'ens_v1'
                       WHEN summary.logical_name_id IS NULL
                        AND event_summary.arm_count = 1 THEN event_summary.sole_arm
                   END AS selected_authority_arm,
                   CASE WHEN migration.logical_name_id IS NOT NULL
                       THEN 'migration_authority_transition' END AS proof_kind,
                   migration.proof_event_id, migration.proof_event_identity,
                   migration.transition_id,
                   released.released_v2_resource_id,
                   released_v1.released_v1_resource_id, released_v1.released_v1_binding_id,
                   -- The arm was selected from event history with nothing open: the sole arm with
                   -- history, or ENSv1 when both arms have history and no ENSv2 release tombstone
                   -- applies. Its lifecycle state then reads that arm's events.
                   COALESCE(
                       summary.logical_name_id IS NULL
                           AND (
                               event_summary.arm_count = 1
                               OR (
                                   released.logical_name_id IS NULL
                                   AND event_summary.has_ens_v1
                               )
                           ),
                       false
                   ) AS bindingless_event_authority
            FROM project_surfaces surface
            LEFT JOIN arm_summary summary USING (logical_name_id)
            LEFT JOIN event_arm_summary event_summary USING (logical_name_id)
            LEFT JOIN migration_history migration USING (logical_name_id)
            LEFT JOIN released_v2_authority released USING (logical_name_id)
            LEFT JOIN released_v1_authority released_v1 USING (logical_name_id)
        ), selected AS (
            SELECT decision.*, binding.surface_binding_id AS selected_binding_id,
                   binding.resource_id AS selected_resource_id,
                   binding.binding_kind AS selected_binding_kind,
                   binding.block_number AS selected_epoch_block_number,
                   (binding.provenance ->> 'transaction_index')::bigint
                       AS selected_epoch_transaction_index,
                   (binding.provenance ->> 'log_index')::bigint AS selected_epoch_log_index
            FROM decision
            LEFT JOIN LATERAL (
                SELECT candidate.*
                FROM project_binding_candidates candidate
                CROSS JOIN target_time
                WHERE candidate.logical_name_id = decision.logical_name_id
                  AND candidate.authority_arm = decision.selected_authority_arm
                  AND EXISTS (
                      SELECT 1 FROM project_resources resource
                      WHERE resource.resource_id = candidate.resource_id
                  )
                  AND (
                      decision.released_v2_resource_id IS NULL
                      OR candidate.resource_id = decision.released_v2_resource_id
                  )
                  AND (
                      decision.released_v2_resource_id IS NOT NULL
                      OR candidate.surface_binding_id = decision.released_v1_binding_id
                      OR (
                          candidate.active_from < target_time.cutoff
                          AND (
                              candidate.active_to IS NULL
                              OR candidate.active_to >= target_time.cutoff
                          )
                      )
                  )
                ORDER BY candidate.block_number DESC,
                         COALESCE(
                             (candidate.provenance ->> 'transaction_index')::bigint, -1
                         ) DESC,
                         COALESCE(
                             (candidate.provenance ->> 'log_index')::bigint, -1
                         ) DESC,
                         candidate.surface_binding_id DESC
                LIMIT 1
            ) binding ON TRUE
        ), registry_records AS (
            -- The ownership records the two ENSv1 registries hold for each ENS name's node: whether
            -- the 2017 registry recorded an owner, and the block of the first record in the current
            -- registry, which answers from the 2017 registry until it holds one. A node's first
            -- current-registry write is always its parent's setSubnodeOwner, because setOwner is
            -- authorised by the current registry's own record; its NewOwner derives
            -- SubregistryChanged. Only NewOwner and Transfer derive SubregistryChanged or
            -- AuthorityTransferred (the ens_v1_registry_l1 manifests' normalized_events). Reading
            -- Transfer too is defensive: Interpret forces AuthorityTransferred for a first-write
            -- Transfer only when no old-registry resolver link is retired, and the chain does not
            -- produce that shape. A NewOwner names its child node and a Transfer its own. The filter
            -- stays on columns with statistics so the join to the names below is estimated from
            -- real row counts. Same-transaction registration reconciliation keeps the
            -- transaction's last current-registry ownership write, so a registration it marks
            -- `registry_migrated` has such a write beside it and the marker adds nothing here. The
            -- root is left out: the constructor writes its record without an event.
            -- (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L21 @ ens_v1@91c966f)
            -- (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L46 @ ens_v1@91c966f)
            -- (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L23-L26 @ ens_v1@91c966f)
            -- (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f)
            SELECT surface.logical_name_id,
                   bool_or(record.emitter_role = 'registry_old') AS has_old_record,
                   min(record.block_number) FILTER (WHERE record.emitter_role = 'registry')
                       AS current_record_block
            FROM (
                SELECT lower(COALESCE(event.after_state ->> 'child_node',
                                      event.after_state ->> 'node')) AS node,
                       event.after_state ->> 'emitter_role' AS emitter_role,
                       event.block_number
                FROM project_events event
                WHERE event.namespace = 'ens'
                  AND event.source_family = 'ens_v1_registry_l1'
                  AND event.event_kind IN ('SubregistryChanged', 'AuthorityTransferred')
            ) record
            JOIN project_surfaces surface
              ON surface.namespace = 'ens' AND lower(surface.namehash) = record.node
            WHERE record.node <> '0x0000000000000000000000000000000000000000000000000000000000000000'
            GROUP BY surface.logical_name_id
        )
        SELECT selected.logical_name_id, selected.selected_authority_arm,
               selected.selected_resource_id, selected.selected_binding_id,
               ownerless_profile.eligible AS known_ownerless_registry,
               ownerless.resource_id AS ownerless_registry_resource_id, ownerless.owner_getter_reason,
               (selected.released_v1_binding_id IS NOT NULL
                AND selected.selected_binding_id = selected.released_v1_binding_id) AS released_v1_tombstone,
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', selected.selected_epoch_block_number, 'transaction_index', selected.selected_epoch_transaction_index, 'log_index', selected.selected_epoch_log_index)) AS authority_epoch_start_position,
               selected.proof_kind AS authority_proof_kind, selected.proof_event_id AS authority_proof_event_id,
               selected.proof_event_identity AS authority_proof_event_identity, selected.transition_id AS authority_transition_id,
               CASE
                   -- A released ENSv2 tombstone was selected because its release is the latest
                   -- fact of its registration, which may be a release written without a name.
                   WHEN selected.released_v2_resource_id IS NOT NULL THEN 'unregistered'
                   WHEN lifecycle.event_kind = 'RegistrationReleased' THEN 'unregistered'
                   WHEN lifecycle.event_kind = 'RegistrationReserved' THEN 'reserved'
                   WHEN lifecycle.event_kind IN ('RegistrationGranted', 'RegistrationRenewed')
                       THEN 'registered'
                   WHEN selected.selected_binding_id IS NULL THEN 'unregistered'
                   ELSE 'registered'
               END AS lifecycle_state,
               CASE
                   WHEN ownerless_profile.eligible THEN NULL
                   WHEN selected.selected_binding_id IS NULL THEN 'current_authority_not_projected'
               END AS unsupported_reason,
               CASE WHEN selected.selected_authority_arm = 'ens_v1' THEN
                   CASE WHEN records.has_old_record AND records.current_record_block IS NULL
                       THEN 'old' ELSE 'current' END
               END AS registry_generation,
               records.current_record_block AS registry_handoff_block_number,
               jsonb_strip_nulls(jsonb_build_object('authority_arm',
                   selected.selected_authority_arm, 'binding_kind', selected.selected_binding_kind,
                   'resource_id', selected.selected_resource_id, 'surface_binding_id',
                   selected.selected_binding_id, 'released_tombstone',
                   CASE WHEN selected.released_v1_binding_id IS NOT NULL
                          AND selected.selected_binding_id = selected.released_v1_binding_id
                       THEN 'ens_v1' END)) AS resource_authority_context
        FROM selected
        LEFT JOIN project_latest_registry_owner ownerless USING (logical_name_id)
        LEFT JOIN registry_records records USING (logical_name_id)
        -- The ownerless-registry profile serves an ENSv1 or Basenames registry row, so it never
        -- applies under ENSv2 authority.
        CROSS JOIN LATERAL (
            SELECT ownerless.logical_name_id IS NOT NULL
                   AND selected.selected_binding_id IS NULL
                   AND selected.selected_authority_arm IS DISTINCT FROM 'ens_v2' AS eligible
        ) ownerless_profile
        LEFT JOIN LATERAL (
            SELECT event.event_kind
            FROM project_events event
            WHERE event.logical_name_id = selected.logical_name_id
              AND (
                  event.resource_id = selected.selected_resource_id
                  OR (
                      selected.bindingless_event_authority
                      AND CASE
                          WHEN event.source_family LIKE 'ens_v1_%' THEN 'ens_v1'
                          WHEN event.source_family LIKE 'ens_v2_%' THEN 'ens_v2'
                          WHEN event.source_family LIKE 'basenames_%' THEN 'basenames'
                      END = selected.selected_authority_arm
                  )
              )
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed',
                  'RegistrationReleased', 'RegistrationReserved'
              )
              AND (
                  selected.selected_epoch_block_number IS NULL
                  OR (
                      event.block_number,
                      COALESCE(event.transaction_index, -1),
                      COALESCE(event.log_index, -1)
                  ) >= (
                      selected.selected_epoch_block_number,
                      COALESCE(selected.selected_epoch_transaction_index, -1),
                      COALESCE(selected.selected_epoch_log_index, -1)
                  )
              )
            ORDER BY event.block_number DESC NULLS LAST, event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
            LIMIT 1
        ) lifecycle ON TRUE
