use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use super::{
    EventHistoryReadFilter,
    filters::push_attributed_record_filter_where,
    lineage::{same_fork_as, same_fork_predicate},
};

pub(super) fn push_product_event_kind_predicate(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        "ne.event_kind IN (
            'RegistrationGranted', 'LabelRegistered', 'RegistrationRenewed',
            'RegistrationReleased', 'ExpiryChanged', 'TokenControlTransferred',
            'AuthorityTransferred', 'AuthorityEpochChanged', 'ResolverChanged',
            'RecordChanged', 'RecordVersionChanged', 'ReverseChanged',
            'PermissionChanged', 'PermissionScopeChanged', 'RolesChanged',
            'EACRolesChanged', 'SubregistryChanged'
        )",
    );
}

pub(super) async fn is_public_registration_id(
    pool: &PgPool,
    registration_id: Uuid,
    canonical_only: bool,
) -> Result<bool, sqlx::Error> {
    let mut builder = QueryBuilder::new(
        "SELECT EXISTS (
            SELECT 1
            FROM bigname_phase.normalized_events ne
            LEFT JOIN bigname_phase.chain_lineage rb
              ON rb.chain_id = ne.chain_id
             AND rb.block_hash = ne.block_hash
            WHERE ne.resource_id = ",
    );
    builder.push_bind(registration_id);
    builder.push(" AND ne.consumer_visibility = 'activated'");
    if canonical_only {
        builder.push(
            " AND ne.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND (
                  ne.block_hash IS NULL
                  OR rb.canonicality_state IN ('canonical', 'safe', 'finalized')
              )",
        );
    }
    builder.push(" AND ");
    push_public_registration_witness(&mut builder, canonical_only, &["ne"]);
    builder.push(" AND (");
    push_product_registration_id(&mut builder, canonical_only);
    builder.push(" = ");
    builder.push_bind(registration_id);
    builder.push(") LIMIT 1)");
    builder.build_query_scalar().fetch_one(pool).await
}

/// Keep only the rows of one registration. A row on a resource belongs to it when its
/// registration identity is that registration; a row with no resource (a record write) belongs
/// to it while one of the registration's bindings is active, or when Project attributed the
/// write to the registration's own records or to those of a NameWrapper resource that wrapped
/// it. The candidate rows also hold the writes of the name's other registrations, so attribution
/// to a candidate resource alone is not membership.
pub(super) fn push_registration_filter<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a EventHistoryReadFilter,
    canonical_only: bool,
) {
    let Some(registration_id) = filter.registration_id else {
        return;
    };
    builder.push(" AND ");
    builder.push_bind(filter.registration_id_is_public);
    builder.push(" AND ((ne.resource_id IS NULL AND ");
    push_product_event_kind_predicate(builder);
    builder.push(" AND (");
    push_registration_binding_at_event(builder, registration_id, canonical_only);
    if let Some((_, resource_ids)) = filter.product_registration() {
        push_attributed_record_filter_where(builder, "ne", resource_ids, |builder| {
            push_attributing_inventory_is_registration(builder, registration_id, canonical_only);
        });
    }
    builder.push(")) OR (");
    push_public_registration_at_event(builder, registration_id, canonical_only);
    builder.push(" AND ");
    push_product_registration_id(builder, canonical_only);
    builder.push(" = ");
    builder.push_bind(registration_id);
    builder.push("))");
}

// The record inventory that attributes a write proves membership only when it is the
// registration's own, or belongs to a NameWrapper resource whose `NameWrapped` row on the
// write's fork recorded this registration as the lease it wrapped.
fn push_attributing_inventory_is_registration(
    builder: &mut QueryBuilder<'_, Postgres>,
    registration_id: Uuid,
    canonical_only: bool,
) {
    builder.push(" AND (inventory.resource_id = ");
    builder.push_bind(registration_id);
    // The derived table keeps the lookup keyed by the inventory's resource; without it the
    // planner rewrites the EXISTS into a per-row hash of every NameWrapped row on the chain.
    builder.push(
        " OR EXISTS (
            SELECT 1
            FROM (
                SELECT * FROM bigname_phase.normalized_events wrapper_binding
                WHERE wrapper_binding.resource_id = inventory.resource_id
                  AND wrapper_binding.resource_id IS NOT NULL
                  AND wrapper_binding.consumer_visibility = 'activated'",
    );
    if canonical_only {
        builder
            .push(" AND wrapper_binding.canonicality_state IN ('canonical', 'safe', 'finalized')");
    }
    builder.push(
        " OFFSET 0
            ) wrapper_binding
            LEFT JOIN bigname_phase.chain_lineage wrapper_lineage
              ON wrapper_lineage.chain_id = wrapper_binding.chain_id
             AND wrapper_lineage.block_hash = wrapper_binding.block_hash
            WHERE wrapper_binding.chain_id = ne.chain_id
              AND wrapper_binding.event_kind = 'SurfaceBound'
              AND wrapper_binding.source_family = 'ens_v1_wrapper_l1'
              AND (wrapper_binding.after_state ->> 'wrapped_registrar_resource_id')::uuid = ",
    );
    builder.push_bind(registration_id);
    if canonical_only {
        builder.push(
            " AND (wrapper_binding.block_hash IS NULL
                   OR wrapper_lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))",
        );
    }
    builder.push(" AND ");
    builder.push(same_fork_predicate("wrapper_binding", "ne", canonical_only));
    builder.push("))");
}

// Resource-less name events belong to a registration only while its binding is active.
fn push_registration_binding_at_event(
    builder: &mut QueryBuilder<'_, Postgres>,
    registration_id: Uuid,
    canonical_only: bool,
) {
    builder.push(
        "EXISTS (
            SELECT 1
            FROM (SELECT ne.chain_id, ne.block_hash, ne.block_number,
                         ne.logical_name_id) history_event
            CROSS JOIN bigname_phase.surface_bindings history_binding
            LEFT JOIN bigname_phase.chain_lineage binding_lineage
              ON binding_lineage.chain_id = history_binding.chain_id
             AND binding_lineage.block_hash = history_binding.block_hash
             AND binding_lineage.block_number = history_binding.block_number
            WHERE history_binding.logical_name_id = ne.logical_name_id
              AND history_binding.chain_id = ne.chain_id
              AND history_binding.active_from <= rb.block_timestamp
                  + GREATEST(COALESCE(ne.log_index, 0), 0) * interval '1 microsecond'
              AND (history_binding.active_to IS NULL
                   OR history_binding.active_to > rb.block_timestamp
                       + GREATEST(COALESCE(ne.log_index, 0), 0)
                         * interval '1 microsecond')",
    );
    if canonical_only {
        builder.push(
            " AND history_binding.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND (history_binding.block_hash IS NULL
                   OR binding_lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))",
        );
    }
    builder.push(" AND ");
    builder.push(same_fork_predicate(
        "history_binding",
        "history_event",
        canonical_only,
    ));
    builder.push(" AND ");
    push_registration_resource_witness(
        builder,
        "history_binding.resource_id",
        registration_id,
        canonical_only,
        true,
    );
    builder.push(")");
}

// The scalar precheck cannot establish identity on each retained losing branch.
fn push_public_registration_at_event(
    builder: &mut QueryBuilder<'_, Postgres>,
    registration_id: Uuid,
    canonical_only: bool,
) {
    if canonical_only {
        builder.push("TRUE");
        return;
    }
    builder.push(
        "EXISTS (SELECT 1 FROM (
            SELECT ne.chain_id, ne.block_hash, ne.block_number, ne.resource_id,
                   ne.logical_name_id
        ) history_event WHERE ",
    );
    push_registration_resource_witness(
        builder,
        "history_event.resource_id",
        registration_id,
        canonical_only,
        false,
    );
    builder.push(")");
}

fn push_registration_resource_witness(
    builder: &mut QueryBuilder<'_, Postgres>,
    resource: &str,
    registration_id: Uuid,
    canonical_only: bool,
    require_lifecycle: bool,
) {
    // Keep the resource-index lookup bounded before applying nested identity checks.
    builder.push(format!(
        "EXISTS (
            SELECT 1
            FROM (
                SELECT * FROM bigname_phase.normalized_events resource_event
                WHERE resource_event.resource_id = {resource}
                  AND resource_event.resource_id IS NOT NULL
                  AND resource_event.consumer_visibility = 'activated'",
    ));
    if canonical_only {
        builder
            .push(" AND resource_event.canonicality_state IN ('canonical', 'safe', 'finalized')");
    }
    builder.push(
        " OFFSET 0
            ) ne
            LEFT JOIN bigname_phase.chain_lineage rb
              ON rb.chain_id = ne.chain_id AND rb.block_hash = ne.block_hash
            WHERE ne.chain_id = history_event.chain_id",
    );
    if require_lifecycle {
        builder.push(
            " AND (ne.logical_name_id IS NULL
                   OR ne.logical_name_id = history_event.logical_name_id)",
        );
    }
    super::source::push_history_canonicality_filter(builder, canonical_only);
    let anchors: &[&str] = if require_lifecycle {
        &["ne", "history_event", "history_binding"]
    } else {
        &["ne", "history_event"]
    };
    builder.push(" AND ");
    builder.push(same_fork_as("ne", anchors, canonical_only));
    builder.push(" AND ");
    if require_lifecycle {
        push_registration_lifecycle_witness(builder, canonical_only, anchors);
    } else {
        push_public_registration_witness(builder, canonical_only, anchors);
    }
    builder.push(" AND (");
    push_product_registration_id_with_anchors(builder, canonical_only, anchors);
    builder.push(" = ");
    builder.push_bind(registration_id);
    builder.push("))");
}

// The retained event proves registrar attribution. The resource row contributes
// only its immutable chain/token relationship; its reorg anchor can move.
fn push_public_registration_witness(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
    anchors: &[&str],
) {
    builder.push("(");
    push_registration_lifecycle_witness(builder, canonical_only, anchors);
    builder.push(
        " OR (
            (ne.source_family IN ('ens_v1_registrar_l1', 'basenames_base_registrar')
             OR (ne.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
                 AND ne.event_kind = 'ResolverChanged'
                 AND ne.after_state ->> 'authority_kind' = 'registrar'))
            AND EXISTS (
                SELECT 1 FROM bigname_phase.resources registration_resource
                WHERE registration_resource.resource_id = ne.resource_id
                  AND registration_resource.chain_id = ne.chain_id
                  AND registration_resource.token_lineage_id IS NOT NULL)))",
    );
}

// Producers emit RegistrationGranted for new lifecycles, including renewal-first
// recovery. A NameWrapper binding that wrapped a BaseRegistrar lease can witness only that
// explicitly linked lease's grant.
fn push_registration_lifecycle_witness(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
    anchors: &[&str],
) {
    builder.push(
        "(ne.event_kind = 'RegistrationGranted'
          OR (
              ne.event_kind = 'SurfaceBound'
              AND ne.source_family = 'ens_v1_wrapper_l1'
              AND EXISTS (
                  SELECT 1
                  FROM (
                      SELECT * FROM bigname_phase.normalized_events grant_event
                      WHERE grant_event.resource_id =
                            (ne.after_state ->> 'wrapped_registrar_resource_id')::uuid
                        AND grant_event.resource_id IS NOT NULL
                        AND grant_event.consumer_visibility = 'activated'",
    );
    if canonical_only {
        builder.push(" AND grant_event.canonicality_state IN ('canonical', 'safe', 'finalized')");
    }
    builder.push(
        " OFFSET 0
                  ) lifecycle_grant
                  LEFT JOIN bigname_phase.chain_lineage lifecycle_lineage
                    ON lifecycle_lineage.chain_id = lifecycle_grant.chain_id
                   AND lifecycle_lineage.block_hash = lifecycle_grant.block_hash
                  WHERE lifecycle_grant.event_kind = 'RegistrationGranted'
                    AND lifecycle_grant.source_family = 'ens_v1_registrar_l1'
                    AND lifecycle_grant.chain_id = ne.chain_id
                    AND (lifecycle_grant.logical_name_id IS NULL
                         OR lifecycle_grant.logical_name_id = ne.logical_name_id)",
    );
    if canonical_only {
        builder.push(
            " AND (lifecycle_grant.block_hash IS NULL
                   OR lifecycle_lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))",
        );
    }
    builder.push(" AND ");
    builder.push(same_fork_as("lifecycle_grant", anchors, canonical_only));
    // A NameWrapped row with no link wrapped a name that has no BaseRegistrar lease (a
    // wrapped subname), so the NameWrapper resource is that name's registration.
    builder.push(
        "))
          OR (
              ne.event_kind = 'SurfaceBound'
              AND ne.source_family = 'ens_v1_wrapper_l1'
              AND ne.after_state ->> 'wrapped_registrar_resource_id' IS NULL
          ))",
    );
}

pub(super) fn push_product_registration_id(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
) {
    push_product_registration_id_with_anchors(builder, canonical_only, &["ne"]);
}

fn push_product_registration_id_with_anchors(
    builder: &mut QueryBuilder<'_, Postgres>,
    canonical_only: bool,
    anchors: &[&str],
) {
    let fork = |evidence| same_fork_as(evidence, anchors, canonical_only);
    let wrapper_fork = fork("wrapper_binding");
    // Classify the event from retained lifecycle facts, never current resource state.
    // A later grant must not turn earlier reservation updates into registration rows.
    let reservation_fork = fork("reservation_state");
    let resource_fork = fork("event_resource");
    let binding_fork = fork("binding");
    // Emit a Boolean literal so canonical reads retain constant-folded index predicates.
    builder.push(format!(
        r#"
        CASE
            WHEN ne.resource_id IS NULL THEN NULL::uuid
            WHEN ne.source_family IN ('ens_v2_registry_l1', 'ens_v2_migration_l1') AND (
                ne.event_kind = 'RegistrationReserved'
                OR CASE WHEN ne.event_kind = 'RegistrationReleased'
                    AND ne.after_state ->> 'source_event' = 'RegistryPathExpired'
                    AND ne.after_state ->> 'derived_from' = 'interpreter_state'
                    THEN ne.before_state ->> 'status' = 'reserved'
                ELSE (
                    SELECT reservation_state.event_kind = 'RegistrationReserved'
                    FROM bigname_phase.normalized_events reservation_state
                    LEFT JOIN bigname_phase.chain_lineage reservation_lineage
                      ON reservation_lineage.chain_id = reservation_state.chain_id
                     AND reservation_lineage.block_hash = reservation_state.block_hash
                    WHERE reservation_state.resource_id = ne.resource_id
                      AND reservation_state.chain_id = ne.chain_id
                      AND reservation_state.source_family = 'ens_v2_registry_l1'
                      AND reservation_state.consumer_visibility = 'activated'
                      AND reservation_state.event_kind IN (
                          'RegistrationReserved', 'RegistrationGranted'
                      )
                      AND (reservation_state.block_number, reservation_state.log_index) <=
                          (ne.block_number, ne.log_index)
                      AND {reservation_fork}
                      AND (NOT {canonical_only} OR reservation_state.canonicality_state IN (
                          'canonical'::bigname_phase.canonicality_state,
                          'safe'::bigname_phase.canonicality_state,
                          'finalized'::bigname_phase.canonicality_state
                      ))
                      AND (NOT {canonical_only} OR reservation_lineage.canonicality_state IN (
                          'canonical'::bigname_phase.canonicality_state,
                          'safe'::bigname_phase.canonicality_state,
                          'finalized'::bigname_phase.canonicality_state
                      ))
                    ORDER BY reservation_state.block_number DESC,
                             reservation_state.log_index DESC NULLS LAST,
                             (reservation_state.event_kind = 'RegistrationGranted') DESC
                    LIMIT 1
                ) END
            ) THEN NULL::uuid
            ELSE COALESCE(
                (
                    SELECT
                        (wrapper_binding.after_state ->>
                            'wrapped_registrar_resource_id')::uuid
                    FROM bigname_phase.normalized_events wrapper_binding
                    LEFT JOIN bigname_phase.chain_lineage wrapper_lineage
                      ON wrapper_lineage.chain_id = wrapper_binding.chain_id
                     AND wrapper_lineage.block_hash = wrapper_binding.block_hash
                    WHERE wrapper_binding.resource_id = ne.resource_id
                      AND {wrapper_fork}
                      AND wrapper_binding.logical_name_id = ne.logical_name_id
                      AND wrapper_binding.event_kind = 'SurfaceBound'
                      AND wrapper_binding.source_family = 'ens_v1_wrapper_l1'
                      AND wrapper_binding.consumer_visibility = 'activated'
                      AND wrapper_binding.after_state ->>
                            'wrapped_registrar_resource_id' IS NOT NULL
                      AND (NOT {canonical_only} OR wrapper_binding.canonicality_state IN (
                          'canonical'::bigname_phase.canonicality_state,
                          'safe'::bigname_phase.canonicality_state,
                          'finalized'::bigname_phase.canonicality_state
                      ))
                      AND (NOT {canonical_only} OR (
                          wrapper_binding.block_hash IS NULL
                          OR wrapper_lineage.canonicality_state IN (
                              'canonical'::bigname_phase.canonicality_state,
                              'safe'::bigname_phase.canonicality_state,
                              'finalized'::bigname_phase.canonicality_state
                          )
                      ))
                    ORDER BY wrapper_binding.normalized_event_id DESC
                    LIMIT 1
                ),
                CASE
                    WHEN ne.source_family IN (
                        'ens_v1_registry_l1', 'basenames_base_registry'
                    ) AND (
                        (
                            ne.event_kind IN (
                                'AuthorityTransferred', 'AuthorityEpochChanged'
                            )
                            AND lower(COALESCE(ne.after_state ->> 'owner_getter', '')) =
                                '0x0000000000000000000000000000000000000000'
                        )
                        OR (
                            ne.event_kind = 'ResolverChanged'
                            AND NOT EXISTS (
                                SELECT 1
                                FROM bigname_phase.resources event_resource
                                WHERE event_resource.resource_id = ne.resource_id
                                  AND ({resource_fork} OR (
                                      ne.after_state ->> 'authority_kind' = 'registrar'
                                      AND event_resource.chain_id = ne.chain_id
                                  ))
                                  AND event_resource.token_lineage_id IS NOT NULL
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                FROM bigname_phase.surface_bindings binding
                                JOIN bigname_phase.chain_lineage binding_lineage
                                  ON binding_lineage.chain_id = binding.chain_id
                                 AND binding_lineage.block_hash = binding.block_hash
                                 AND binding_lineage.block_number = binding.block_number
                                WHERE binding.resource_id = ne.resource_id
                                  AND {binding_fork}
                                  AND binding.chain_id = ne.chain_id
                                  AND binding.active_from <= rb.block_timestamp
                                      + GREATEST(COALESCE(ne.log_index, 0), 0)
                                        * interval '1 microsecond'
                                  AND (
                                      binding.active_to IS NULL
                                      OR binding.active_to > rb.block_timestamp
                                          + GREATEST(COALESCE(ne.log_index, 0), 0)
                                            * interval '1 microsecond'
                                  )
                                  AND (NOT {canonical_only} OR binding.canonicality_state IN (
                                      'canonical'::bigname_phase.canonicality_state,
                                      'safe'::bigname_phase.canonicality_state,
                                      'finalized'::bigname_phase.canonicality_state
                                  ))
                                  AND (NOT {canonical_only} OR binding_lineage.canonicality_state IN (
                                      'canonical'::bigname_phase.canonicality_state,
                                      'safe'::bigname_phase.canonicality_state,
                                      'finalized'::bigname_phase.canonicality_state
                                  ))
                            )
                        )
                    ) THEN NULL::uuid
                    ELSE ne.resource_id
                END
            )
        END
        "#,
    ));
}
