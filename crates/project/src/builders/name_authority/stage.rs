use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn build(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in [
        "ALTER TABLE project_name_authority ADD PRIMARY KEY (logical_name_id)",
        "CREATE TEMP TABLE project_bindings ON COMMIT DROP AS
         SELECT candidate.*
         FROM project_name_authority authority
         JOIN project_binding_candidates candidate
           ON candidate.surface_binding_id = authority.selected_binding_id",
        "CREATE INDEX ON project_bindings (logical_name_id)",
        "CREATE TEMP TABLE project_authority_events ON COMMIT DROP AS
         SELECT DISTINCT ON (event.normalized_event_id) event.*
         FROM project_events event
         JOIN project_name_authority authority
           ON authority.logical_name_id = event.logical_name_id
         WHERE (
               (
                   authority.unsupported_reason IS NULL
                   AND (
                       event.resource_id = authority.selected_resource_id
                       OR (
                           event.resource_id IS NULL
                           AND CASE
                               WHEN event.source_family LIKE 'ens_v1_%' THEN 'ens_v1'
                               WHEN event.source_family LIKE 'ens_v2_%' THEN 'ens_v2'
                               WHEN event.source_family LIKE 'basenames_%' THEN 'basenames'
                           END = authority.selected_authority_arm
                       )
                       OR (
                           authority.selected_authority_arm = 'ens_v1'
                           AND event.event_kind = 'ExpiryChanged'
                           AND event.source_family = 'ens_v1_registrar_l1'
                           AND COALESCE(
                               NULLIF(event.after_state ->> 'authority_kind', ''),
                               'registrar'
                           ) = 'registrar'
                           AND EXISTS (
                               SELECT 1
                               FROM project_bindings selected_binding
                               JOIN LATERAL (
                                   SELECT predecessor.*
                                   FROM project_binding_candidates predecessor
                                   WHERE predecessor.logical_name_id =
                                         selected_binding.logical_name_id
                                     AND predecessor.authority_arm = 'ens_v1'
                                     AND (
                                         predecessor.block_number,
                                         COALESCE(
                                             (predecessor.provenance ->> 'transaction_index')::bigint,
                                             -1
                                         ),
                                         COALESCE(
                                             (predecessor.provenance ->> 'log_index')::bigint, -1
                                         )
                                     ) < (
                                         selected_binding.block_number,
                                         COALESCE(
                                             (selected_binding.provenance ->> 'transaction_index')::bigint,
                                             -1
                                         ),
                                         COALESCE(
                                             (selected_binding.provenance ->> 'log_index')::bigint,
                                             -1
                                         )
                                     )
                                   ORDER BY predecessor.block_number DESC,
                                            COALESCE(
                                                (predecessor.provenance ->> 'transaction_index')::bigint,
                                                -1
                                            ) DESC,
                                            COALESCE(
                                                (predecessor.provenance ->> 'log_index')::bigint,
                                                -1
                                            ) DESC,
                                            predecessor.surface_binding_id DESC
                                   LIMIT 1
                               ) predecessor ON TRUE
                               WHERE selected_binding.logical_name_id =
                                     authority.logical_name_id
                                 AND predecessor.resource_id = event.resource_id
                                 AND (
                                     event.block_number,
                                     COALESCE(event.transaction_index, -1),
                                     COALESCE(event.log_index, -1)
                                 ) >= (
                                     predecessor.block_number,
                                     COALESCE(
                                         (predecessor.provenance ->> 'transaction_index')::bigint,
                                         -1
                                     ),
                                     COALESCE(
                                         (predecessor.provenance ->> 'log_index')::bigint, -1
                                     )
                                 )
                           )
                           AND EXISTS (
                               SELECT 1 FROM project_events wrapper
                               WHERE wrapper.logical_name_id = authority.logical_name_id
                                 AND wrapper.resource_id = authority.selected_resource_id
                                 AND wrapper.source_family = 'ens_v1_wrapper_l1'
                                 AND wrapper.event_kind = 'PermissionScopeChanged'
                           )
                       )
                       OR (
                           authority.selected_authority_arm = 'ens_v1'
                           AND event.event_kind IN (
                               'RegistrationGranted', 'RegistrationRenewed',
                               'RegistrationReleased', 'RegistrationReserved',
                               'ExpiryChanged', 'TokenControlTransferred'
                           )
                           AND EXISTS (
                               SELECT 1 FROM project_events fallback
                               WHERE fallback.logical_name_id = authority.logical_name_id
                                 AND fallback.resource_id = authority.selected_resource_id
                                 AND fallback.event_kind = 'AuthorityEpochChanged'
                                 AND fallback.after_state ->> 'authority_kind' = 'registry_only'
                           )
                           AND EXISTS (
                               SELECT 1
                               FROM project_bindings selected_binding
                               JOIN LATERAL (
                                   SELECT predecessor.*
                                   FROM project_binding_candidates predecessor
                                   WHERE predecessor.logical_name_id =
                                         selected_binding.logical_name_id
                                     AND predecessor.authority_arm = 'ens_v1'
                                     AND (
                                         predecessor.block_number,
                                         COALESCE(
                                             (predecessor.provenance ->> 'transaction_index')::bigint,
                                             -1
                                         ),
                                         COALESCE(
                                             (predecessor.provenance ->> 'log_index')::bigint, -1
                                         )
                                     ) < (
                                         selected_binding.block_number,
                                         COALESCE(
                                             (selected_binding.provenance ->> 'transaction_index')::bigint,
                                             -1
                                         ),
                                         COALESCE(
                                             (selected_binding.provenance ->> 'log_index')::bigint,
                                             -1
                                         )
                                     )
                                   ORDER BY predecessor.block_number DESC,
                                            COALESCE(
                                                (predecessor.provenance ->> 'transaction_index')::bigint,
                                                -1
                                            ) DESC,
                                            COALESCE(
                                                (predecessor.provenance ->> 'log_index')::bigint,
                                                -1
                                            ) DESC,
                                            predecessor.surface_binding_id DESC
                                   LIMIT 1
                               ) predecessor ON TRUE
                               WHERE selected_binding.logical_name_id =
                                     authority.logical_name_id
                                 AND predecessor.resource_id = event.resource_id
                                 AND (
                                     event.block_number,
                                     COALESCE(event.transaction_index, -1),
                                     COALESCE(event.log_index, -1)
                                 ) >= (
                                     predecessor.block_number,
                                     COALESCE(
                                         (predecessor.provenance ->> 'transaction_index')::bigint,
                                         -1
                                     ),
                                     COALESCE(
                                         (predecessor.provenance ->> 'log_index')::bigint, -1
                                     )
                                 )
                                 AND (
                                     event.block_number,
                                     COALESCE(event.transaction_index, -1),
                                     COALESCE(event.log_index, -1)
                                 ) <= (
                                     selected_binding.block_number,
                                     COALESCE(
                                         (selected_binding.provenance ->> 'transaction_index')::bigint,
                                         -1
                                     ),
                                     COALESCE(
                                         (selected_binding.provenance ->> 'log_index')::bigint, -1
                                     )
                                 )
                           )
                       )
                   )
               )
               OR (
                   authority.unsupported_reason = 'current_authority_not_projected'
                   AND event.event_kind = 'ResolverChanged'
                   AND event.resource_id IS NULL
               )
           )
           AND (
               authority.authority_proof_event_id IS NULL
               OR (
                   event.block_number,
                   COALESCE(event.transaction_index, -1),
                   COALESCE(event.log_index, -1)
               ) >= (
                   (authority.authority_epoch_start_position ->> 'block_number')::bigint,
                   COALESCE(
                       (authority.authority_epoch_start_position ->> 'transaction_index')::bigint,
                       -1
                   ),
                   COALESCE(
                       (authority.authority_epoch_start_position ->> 'log_index')::bigint, -1
                   )
               )
           )
         ORDER BY event.normalized_event_id",
        "CREATE INDEX ON project_authority_events (logical_name_id, normalized_event_id)",
        "CREATE INDEX ON project_authority_events (resource_id, normalized_event_id)",
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
