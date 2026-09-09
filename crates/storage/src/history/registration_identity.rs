use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use super::lineage::{same_fork_as, same_fork_predicate};

pub(super) fn push_product_event_kind_predicate(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        "ne.event_kind IN (
            'RegistrationGranted', 'LabelRegistered', 'RegistrationRenewed',
            'RegistrationReleased', 'ExpiryChanged', 'TokenControlTransferred',
            'AuthorityTransferred', 'AuthorityEpochChanged', 'ResolverChanged',
            'RecordChanged', 'RecordVersionChanged', 'ReverseChanged',
            'PermissionChanged', 'PermissionScopeChanged', 'RolesChanged',
            'EACRolesChanged'
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

// Resource-less name events belong to a registration only while its binding is active.
pub(super) fn push_registration_binding_at_event(
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
pub(super) fn push_public_registration_at_event(
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
// recovery. A wrapper binding can witness only its explicitly linked registrar grant.
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
    builder.push(")))");
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
    let surface_fork = fork("surface");
    let surface_grant_fork = same_fork_predicate("surface", "registrar_grant", canonical_only);
    let born_wrapper_fork = fork("born_wrapper_candidate");
    let born_wrapper_grant_fork =
        same_fork_predicate("born_wrapper_candidate", "registrar_grant", canonical_only);
    let born_wrapper_name_fork =
        same_fork_predicate("born_wrapper_candidate", "grant_name", canonical_only);
    let current_wrapper_fork = fork("current_wrapper");
    let current_wrapper_evidence_fork = same_fork_as(
        "current_wrapper",
        &["registrar_grant", "grant_name", "born_wrapper"],
        canonical_only,
    );
    let grant_fork = fork("registrar_grant");
    let wrapper_fork = fork("wrapper_binding");
    let resource_fork = fork("event_resource");
    let binding_fork = fork("binding");
    // Emit a Boolean literal so canonical reads retain constant-folded index predicates.
    builder.push(format!(
        r#"
        CASE
            WHEN ne.resource_id IS NULL THEN NULL::uuid
            ELSE COALESCE(
                (
                    SELECT born_wrapper.resource_id
                    FROM bigname_phase.normalized_events registrar_grant
                    LEFT JOIN bigname_phase.chain_lineage grant_lineage
                      ON grant_lineage.chain_id = registrar_grant.chain_id
                     AND grant_lineage.block_hash = registrar_grant.block_hash
                    JOIN LATERAL (
                        SELECT resolved.logical_name_id, resolved.chain_id,
                               resolved.block_hash, resolved.block_number
                        FROM (
                            SELECT ne.logical_name_id, ne.chain_id, ne.block_hash,
                                   ne.block_number, 1 AS priority
                            WHERE ne.logical_name_id IS NOT NULL
                            UNION ALL
                            SELECT registrar_grant.logical_name_id, registrar_grant.chain_id,
                                   registrar_grant.block_hash, registrar_grant.block_number,
                                   2 AS priority
                            WHERE ne.logical_name_id IS NULL
                              AND registrar_grant.logical_name_id IS NOT NULL
                            UNION ALL
                            SELECT surface.logical_name_id, surface.chain_id,
                                   surface.block_hash, surface.block_number, 3 AS priority
                            FROM bigname_phase.name_surfaces surface
                            LEFT JOIN bigname_phase.chain_lineage surface_lineage
                              ON surface_lineage.chain_id = surface.chain_id
                             AND surface_lineage.block_hash = surface.block_hash
                            WHERE ne.logical_name_id IS NULL
                              AND registrar_grant.logical_name_id IS NULL
                              AND {surface_fork}
                              AND {surface_grant_fork}
                              AND surface.namespace = registrar_grant.namespace
                              AND surface.namehash = COALESCE(
                                  registrar_grant.after_state ->> 'namehash',
                                  registrar_grant.after_state ->> 'child_node',
                                  registrar_grant.after_state ->> 'node'
                              )
                              AND (NOT {canonical_only} OR surface.canonicality_state IN (
                                  'canonical', 'safe', 'finalized'
                              ))
                              AND (NOT {canonical_only} OR (
                                  surface.block_hash IS NULL
                                  OR surface_lineage.canonicality_state IN (
                                      'canonical', 'safe', 'finalized'
                                  )
                              ))
                        ) resolved
                        ORDER BY resolved.priority, resolved.logical_name_id
                        LIMIT 1
                    ) grant_name ON TRUE
                    JOIN LATERAL (
                        SELECT born_wrapper_candidate.resource_id,
                               born_wrapper_candidate.normalized_event_id,
                               born_wrapper_candidate.chain_id, born_wrapper_candidate.block_hash,
                               born_wrapper_candidate.block_number
                        FROM bigname_phase.normalized_events born_wrapper_candidate
                        LEFT JOIN bigname_phase.chain_lineage wrapper_lineage
                          ON wrapper_lineage.chain_id = born_wrapper_candidate.chain_id
                         AND wrapper_lineage.block_hash = born_wrapper_candidate.block_hash
                        WHERE born_wrapper_candidate.logical_name_id =
                              grant_name.logical_name_id
                          AND {born_wrapper_fork}
                          AND {born_wrapper_grant_fork}
                          AND {born_wrapper_name_fork}
                          AND born_wrapper_candidate.transaction_hash =
                              registrar_grant.transaction_hash
                          AND (
                              born_wrapper_candidate.after_state ->>
                                  'wrapped_registrar_resource_id'
                          )::uuid = registrar_grant.resource_id
                          AND born_wrapper_candidate.event_kind = 'SurfaceBound'
                          AND born_wrapper_candidate.source_family =
                              'ens_v1_wrapper_l1'
                          AND born_wrapper_candidate.consumer_visibility = 'activated'
                          AND (NOT {canonical_only} OR born_wrapper_candidate.canonicality_state IN (
                              'canonical', 'safe', 'finalized'
                          ))
                          AND (NOT {canonical_only} OR (
                              born_wrapper_candidate.block_hash IS NULL
                              OR wrapper_lineage.canonicality_state IN (
                                  'canonical', 'safe', 'finalized'
                              )
                          ))
                        ORDER BY born_wrapper_candidate.normalized_event_id
                        LIMIT 1
                    ) born_wrapper ON TRUE
                    WHERE registrar_grant.resource_id = COALESCE(
                              (
                                  SELECT (
                                      current_wrapper.after_state ->>
                                          'wrapped_registrar_resource_id'
                                  )::uuid
                                  FROM bigname_phase.normalized_events current_wrapper
                                  LEFT JOIN bigname_phase.chain_lineage current_lineage
                                    ON current_lineage.chain_id = current_wrapper.chain_id
                                   AND current_lineage.block_hash = current_wrapper.block_hash
                                  WHERE current_wrapper.resource_id = ne.resource_id
                                    AND {current_wrapper_fork}
                                    AND {current_wrapper_evidence_fork}
                                    AND current_wrapper.event_kind = 'SurfaceBound'
                                    AND current_wrapper.source_family = 'ens_v1_wrapper_l1'
                                    AND current_wrapper.consumer_visibility = 'activated'
                                    AND current_wrapper.after_state ->>
                                          'wrapped_registrar_resource_id' IS NOT NULL
                                    AND (NOT {canonical_only} OR current_wrapper.canonicality_state IN (
                                        'canonical', 'safe', 'finalized'
                                    ))
                                    AND (NOT {canonical_only} OR (
                                        current_wrapper.block_hash IS NULL
                                        OR current_lineage.canonicality_state IN (
                                            'canonical', 'safe', 'finalized'
                                        )
                                    ))
                                  ORDER BY current_wrapper.normalized_event_id DESC
                                  LIMIT 1
                              ),
                              ne.resource_id
                          )
                      AND {grant_fork}
                      AND registrar_grant.event_kind = 'RegistrationGranted'
                      AND registrar_grant.source_family = 'ens_v1_registrar_l1'
                      AND registrar_grant.consumer_visibility = 'activated'
                      AND (NOT {canonical_only} OR registrar_grant.canonicality_state IN (
                          'canonical', 'safe', 'finalized'
                      ))
                      AND (NOT {canonical_only} OR (
                          registrar_grant.block_hash IS NULL
                          OR grant_lineage.canonicality_state IN (
                              'canonical', 'safe', 'finalized'
                          )
                      ))
                    ORDER BY born_wrapper.normalized_event_id
                    LIMIT 1
                ),
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
