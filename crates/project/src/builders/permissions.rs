mod expiry_retirement;
mod resource_summary;
mod wrapper_operators;

use crate::{Marker, ProjectError, Result};
use sqlx::{Postgres, Transaction};
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target: &Marker,
    full_rebuild: bool,
) -> Result<()> {
    // Permission rows fold resource-keyed history; no output follows historical resolver pointers.
    let permissions_query = [
        "WITH ",
        expiry_retirement::V2_RESOURCE_REVIVALS_CTE,
        r#"
        , target_time AS (
            SELECT extract(epoch FROM lineage.block_timestamp) AS epoch_seconds
            FROM chain_lineage lineage
            WHERE lineage.chain_id = $1
              AND lineage.block_number = $2
              AND lineage.block_hash = $3
        ),
        wrapper_constants AS (SELECT 131072::bigint AS is_dot_eth, 7776000::numeric AS grace_period_seconds),
        decoded AS (
            SELECT event.*,
                   lower(event.after_state ->> 'subject') AS subject,
                   -- A record-ID resolver scopes a grant to a setter argument (the
                   -- resource is the keccak of the argument); the decoded selector says
                   -- which record the resource is about.
                   -- (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L307-L338 @ ens_v2@a971bd64)
                   -- Only the record-ID generation's grants carry it: there the
                   -- selector's hash is the resource itself (keccak of the setter
                   -- argument). The node-keyed generation's named-resource selectors
                   -- hash the key alone (NamedTextResource's keyHash) or nothing
                   -- (NamedAddrResource), and describe a node.
                   -- (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L336-L337 @ ens_v2@a971bd64)
                   -- (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L144-L153 @ ens_v2_sepolia_20260629@ccaeb58)
                   -- (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L168-L172 @ ens_v2_sepolia_20260629@ccaeb58)
                   CASE WHEN jsonb_typeof(event.after_state -> 'selector' -> 'hash') = 'string'
                         AND event.after_state -> 'selector' ->> 'hash' =
                             event.after_state ->> 'upstream_resource'
                         AND event.after_state -> 'selector' ->> 'kind'
                             IN ('address', 'text', 'abi', 'interface', 'data', 'argument')
                        THEN event.after_state -> 'scope' || jsonb_build_object(
                            'resource_selector', event.after_state -> 'selector')
                        ELSE event.after_state -> 'scope' END AS scope_detail,
                   CASE event.after_state -> 'scope' ->> 'kind'
                       WHEN 'root' THEN 'root'
                       WHEN 'registry_root' THEN 'root'
                       WHEN 'registry' THEN 'registry'
                       WHEN 'resource' THEN 'resource'
                       WHEN 'resolver' THEN 'resolver'
                       WHEN 'record_manager' THEN 'record_manager'
                   END AS scope_kind,
                   CASE event.after_state -> 'scope' ->> 'kind'
                       WHEN 'root' THEN 'root'
                       WHEN 'registry_root' THEN 'root'
                       WHEN 'registry' THEN 'registry'
                       WHEN 'resource' THEN 'resource'
                       WHEN 'resolver' THEN concat(
                           'resolver:',
                           event.after_state -> 'scope' ->> 'chain_id',
                           ':',
                           lower(event.after_state -> 'scope' ->> 'resolver_address')
                       )
                       WHEN 'record_manager' THEN concat(
                           'record_manager:',
                           event.after_state -> 'scope' ->> 'chain_id',
                           ':',
                           lower(event.after_state -> 'scope' ->> 'manager_address')
                       )
                   END AS scope
            FROM project_events event
            WHERE event.event_kind IN ('PermissionChanged', 'RootPermissionChanged')
              AND event.resource_id IS NOT NULL
              AND event.after_state ->> 'subject' IS NOT NULL
              AND btrim(event.after_state ->> 'subject') <> ''
              AND jsonb_typeof(event.after_state -> 'scope') = 'object'
              AND jsonb_typeof(event.after_state -> 'effective_powers') = 'array'
        ),
        ranked AS (
            SELECT event.*,
                   row_number() OVER (
                       PARTITION BY event.resource_id, event.subject, event.scope
                       ORDER BY event.block_number DESC NULLS LAST,
                                event.transaction_index DESC NULLS LAST,
                                event.log_index DESC NULLS LAST,
                                event.normalized_event_id DESC
                   ) AS latest_rank,
                   jsonb_agg(to_jsonb(event.normalized_event_id)) OVER evidence AS event_ids,
                   jsonb_agg(event.raw_fact_ref) OVER evidence AS raw_fact_refs,
                   jsonb_agg(jsonb_build_object(
                       'source_manifest_id', event.source_manifest_id,
                       'source_family', event.source_family,
                       'manifest_version', event.manifest_version
                   )) OVER evidence AS manifest_versions,
                   max(event.manifest_version) OVER (
                       PARTITION BY event.resource_id, event.subject, event.scope
                   ) AS evidence_manifest_version
            FROM decoded event
            WHERE event.scope IS NOT NULL AND btrim(event.scope) <> ''
            WINDOW evidence AS (
                PARTITION BY event.resource_id, event.subject, event.scope
                ORDER BY event.normalized_event_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        ),
        latest AS (SELECT * FROM ranked WHERE latest_rank = 1),
        v2_registration_current AS (
            SELECT DISTINCT ON (event.resource_id) event.*, event.event_kind <> 'RegistrationReleased' AND EXISTS (
                       SELECT 1 FROM project_events expiry
                       WHERE expiry.resource_id = event.resource_id AND expiry.event_kind = 'RegistrationReleased'
                         AND expiry.after_state ->> 'source_event' = 'RegistryPathExpired' AND expiry.after_state ->> 'derived_from' = 'interpreter_state'
                         AND expiry.after_state ->> 'terminal_reason' = 'registry_name_binding_expired' AND
                             (expiry.block_number, expiry.normalized_event_id) < (event.block_number, event.normalized_event_id)
                   ) AS rebound
            FROM project_events event
            WHERE event.resource_id IS NOT NULL AND (
                  (
                      event.event_kind IN ('RegistrationGranted', 'RegistrationReserved', 'RegistrationRenewed')
                      AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1')
                      AND (event.event_kind <> 'RegistrationRenewed'
                           OR event.normalized_event_id IN (
                               SELECT normalized_event_id FROM v2_resource_revivals
                           ))
                  )
                  OR (event.event_kind = 'RegistrationReleased' AND event.after_state ->> 'source_event' = 'RegistryPathExpired'
                      AND event.after_state ->> 'derived_from' = 'interpreter_state' AND event.after_state ->> 'terminal_reason' = 'registry_name_binding_expired')
              )
            ORDER BY event.resource_id, event.block_number DESC NULLS LAST, event.normalized_event_id DESC
        ),
        modifiers AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.*,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'fuses') = 'number'
                        AND (event.after_state ->> 'fuses')::numeric >= 0
                        AND (event.after_state ->> 'fuses')::numeric <= 9223372036854775807
                           THEN (event.after_state ->> 'fuses')::bigint
                   END AS fuses,
                   CASE event.after_state ->> 'wrapper_state'
                       WHEN 'wrapped' THEN 'wrapped'
                       WHEN 'emancipated' THEN 'emancipated'
                       WHEN 'locked' THEN 'locked'
                   END AS wrapper_state
            FROM project_events event
            WHERE event.event_kind = 'PermissionScopeChanged'
              AND event.resource_id IS NOT NULL
              AND event.source_family = 'ens_v1_wrapper_l1'
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        wrapper_expiries AS (
            SELECT DISTINCT ON (event.resource_id)
                   event.*,
                   CASE
                       WHEN jsonb_typeof(event.after_state -> 'expiry') = 'number'
                        AND (event.after_state ->> 'expiry')::numeric >= 0
                        AND (event.after_state ->> 'expiry')::numeric <= 18446744073709551615
                           THEN (event.after_state ->> 'expiry')::numeric
                   END AS expiry_seconds
            FROM project_events event
            WHERE event.event_kind = 'ExpiryChanged'
              AND event.resource_id IS NOT NULL
              AND (
                    event.source_family = 'ens_v1_wrapper_l1'
                 OR (
                        event.source_family = 'ens_v1_registrar_l1'
                    AND event.after_state ->> 'source_event' = 'NameRenewed'
                    AND event.after_state ->> 'authority_kind' = 'wrapper'
                 )
              )
            ORDER BY event.resource_id,
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        )
        INSERT INTO project_stage_permissions_current (
            resource_id, subject, scope, scope_kind, scope_detail,
            effective_powers, grant_source, revocation_source,
            inheritance_path, transfer_behavior, provenance,
            chain_positions, canonicality_summary, manifest_version
        )
        SELECT event.resource_id,
               event.subject,
               event.scope,
               event.scope_kind,
               event.scope_detail,
               masked.effective_powers,
               CASE
                   WHEN jsonb_typeof(event.after_state -> 'grant_source') = 'object'
                       THEN event.after_state -> 'grant_source'
                   ELSE '{}'::jsonb
               END,
               CASE
                   WHEN jsonb_typeof(event.after_state -> 'revocation_source') = 'object'
                       THEN event.after_state -> 'revocation_source'
                   ELSE NULL
               END,
               CASE
                   WHEN jsonb_typeof(event.after_state -> 'inheritance_path') = 'array'
                       THEN event.after_state -> 'inheritance_path'
                   ELSE '[]'::jsonb
               END,
               CASE
                   WHEN jsonb_typeof(event.after_state -> 'transfer_behavior') = 'object'
                       THEN event.after_state -> 'transfer_behavior'
                   ELSE jsonb_build_object(
                       'mode', event.after_state -> 'transfer_behavior'
                   )
               END,
               jsonb_build_object(
                   'normalized_event_ids', event.event_ids || CASE WHEN registration.rebound THEN jsonb_build_array(registration.normalized_event_id) ELSE '[]'::jsonb
                   END || CASE
                       WHEN modifier.normalized_event_id IS NOT NULL
                        AND masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(modifier.normalized_event_id)
                       ELSE '[]'::jsonb
                   END || CASE
                       WHEN wrapper_expiry.normalized_event_id IS NOT NULL AND
                            masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(wrapper_expiry.normalized_event_id)
                       ELSE '[]'::jsonb
                   END,
                   'permission_manifest_versions', event.manifest_versions,
                   'raw_fact_refs', event.raw_fact_refs || CASE WHEN registration.rebound THEN jsonb_build_array(registration.raw_fact_ref) ELSE '[]'::jsonb
                   END || CASE
                       WHEN modifier.normalized_event_id IS NOT NULL
                        AND masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(modifier.raw_fact_ref)
                       ELSE '[]'::jsonb
                   END || CASE
                       WHEN wrapper_expiry.normalized_event_id IS NOT NULL AND
                            masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(wrapper_expiry.raw_fact_ref)
                       ELSE '[]'::jsonb
                   END,
                   'manifest_versions', event.manifest_versions || CASE WHEN registration.rebound THEN jsonb_build_array(jsonb_build_object(
                       'source_manifest_id', registration.source_manifest_id, 'source_family', registration.source_family, 'manifest_version', registration.manifest_version))
                       ELSE '[]'::jsonb
                   END || CASE
                       WHEN modifier.normalized_event_id IS NOT NULL
                        AND masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(jsonb_build_object(
                               'source_manifest_id', modifier.source_manifest_id,
                               'source_family', modifier.source_family,
                               'manifest_version', modifier.manifest_version
                           ))
                       ELSE '[]'::jsonb
                   END || CASE
                       WHEN wrapper_expiry.normalized_event_id IS NOT NULL AND
                            masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN jsonb_build_array(jsonb_build_object(
                               'source_manifest_id', wrapper_expiry.source_manifest_id,
                               'source_family', wrapper_expiry.source_family,
                               'manifest_version', wrapper_expiry.manifest_version
                           ))
                       ELSE '[]'::jsonb
                   END,
                   'derivation_kind', 'permissions_current_rebuild',
                   'chain_id', $1,
                   'coverage', jsonb_build_object(
                       'status', 'projected',
                       'exhaustiveness', 'not_asserted'
                   )
               ),
               jsonb_strip_nulls(jsonb_build_object(
                   'block_number', evidence_position.block_number, 'block_hash', evidence_position.block_hash,
                   'transaction_index', evidence_position.transaction_index, 'log_index', evidence_position.log_index,
                   'target_block_number', $2, 'target_block_hash', $3
               )),
               jsonb_build_object(
                   'state', event.canonicality_state,
                   'target_block_number', $2,
                   'target_block_hash', $3
               ),
               GREATEST(
                   event.evidence_manifest_version,
                   CASE
                       WHEN masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN modifier.manifest_version
                   END,
                   CASE
                       WHEN masked.effective_powers IS DISTINCT FROM
                            event.after_state -> 'effective_powers'
                           THEN wrapper_expiry.manifest_version
                   END,
                   CASE WHEN registration.rebound THEN registration.manifest_version END,
                   event.manifest_version
               )
        FROM latest event
        LEFT JOIN modifiers modifier USING (resource_id)
        LEFT JOIN wrapper_expiries wrapper_expiry USING (resource_id)
        LEFT JOIN v2_registration_current registration USING (resource_id)
        LEFT JOIN target_time ON TRUE
        CROSS JOIN wrapper_constants
        CROSS JOIN LATERAL (
            SELECT CASE
                       WHEN modifier.fuses IS NULL
                         OR modifier.wrapper_state IS NULL
                         OR wrapper_expiry.expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN wrapper_expiry.expiry_seconds < target_time.epoch_seconds THEN 0
                       ELSE modifier.fuses
                   END AS fuses,
                   CASE
                       WHEN modifier.fuses IS NULL
                         OR modifier.wrapper_state IS NULL
                         OR wrapper_expiry.expiry_seconds IS NULL
                         OR target_time.epoch_seconds IS NULL THEN NULL
                       WHEN wrapper_expiry.expiry_seconds < target_time.epoch_seconds
                        AND modifier.wrapper_state IN ('emancipated', 'locked') THEN NULL
                       ELSE modifier.wrapper_state
                   END AS wrapper_state
        ) effective_wrapper
        CROSS JOIN LATERAL (SELECT COALESCE(
                       (effective_wrapper.fuses & wrapper_constants.is_dot_eth) <> 0
                       AND wrapper_expiry.expiry_seconds - wrapper_constants.grace_period_seconds
                           < target_time.epoch_seconds,
                       false
                   ) AS in_grace
        ) grace
        CROSS JOIN LATERAL (
            SELECT CASE
                WHEN modifier.normalized_event_id IS NULL
                    THEN event.after_state -> 'effective_powers'
                WHEN effective_wrapper.fuses IS NULL
                  OR effective_wrapper.wrapper_state IS NULL
                    THEN '[]'::jsonb
                ELSE COALESCE((
                    SELECT jsonb_agg(to_jsonb(power.value) ORDER BY power.ordinality)
                    FROM jsonb_array_elements_text(event.after_state -> 'effective_powers')
                        WITH ORDINALITY AS power(value, ordinality)
                    WHERE (NOT grace.in_grace OR power.value IN ('approve', 'approve_wrapper'))
                      AND NOT CASE power.value
                        WHEN 'resource_control' THEN
                            effective_wrapper.wrapper_state = 'locked'
                        WHEN 'resolver_control' THEN
                            (effective_wrapper.fuses & 8) <> 0
                        WHEN 'set_resolver' THEN
                            (effective_wrapper.fuses & 8) <> 0
                        WHEN 'set_ttl' THEN
                            (effective_wrapper.fuses & 16) <> 0
                        WHEN 'create_subnames' THEN
                            (effective_wrapper.fuses & 32) <> 0
                        WHEN 'create_subdomain' THEN
                            (effective_wrapper.fuses & 32) <> 0
                        WHEN 'transfer' THEN
                            (effective_wrapper.fuses & 4) <> 0
                        WHEN 'transfer_name' THEN
                            (effective_wrapper.fuses & 4) <> 0
                        WHEN 'unwrap' THEN
                            (effective_wrapper.fuses & 1) <> 0
                        WHEN 'burn_fuses' THEN
                            (effective_wrapper.fuses & 2) <> 0
                            OR (effective_wrapper.fuses & 65536) = 0
                        WHEN 'extend_expiry' THEN
                            (effective_wrapper.fuses & 262144) = 0
                        WHEN 'approve' THEN
                            (effective_wrapper.fuses & 64) <> 0
                        WHEN 'approve_wrapper' THEN
                            (effective_wrapper.fuses & 64) <> 0
                        ELSE false
                    END
                ), '[]'::jsonb)
            END AS effective_powers
        ) masked
        CROSS JOIN LATERAL (
            SELECT position.block_number, position.block_hash, position.transaction_index, position.log_index FROM (VALUES
                (event.block_number, event.block_hash, event.transaction_index, event.log_index, event.normalized_event_id),
                (CASE WHEN registration.rebound THEN registration.block_number END, registration.block_hash, registration.transaction_index, registration.log_index, registration.normalized_event_id),
                (modifier.block_number, modifier.block_hash, modifier.transaction_index, modifier.log_index, modifier.normalized_event_id),
                (wrapper_expiry.block_number, wrapper_expiry.block_hash, wrapper_expiry.transaction_index, wrapper_expiry.log_index, wrapper_expiry.normalized_event_id)
            ) position(block_number, block_hash, transaction_index, log_index, normalized_event_id)
            WHERE position.block_number IS NOT NULL ORDER BY position.block_number DESC, position.transaction_index DESC NULLS LAST, position.log_index DESC NULLS LAST, position.normalized_event_id DESC NULLS LAST
            LIMIT 1) evidence_position
        WHERE jsonb_array_length(masked.effective_powers) > 0
          AND NOT COALESCE(
              registration.event_kind = 'RegistrationReleased'
              AND registration.after_state ->> 'source_event' = 'RegistryPathExpired'
              AND registration.after_state ->> 'derived_from' = 'interpreter_state'
              AND registration.after_state ->> 'terminal_reason' =
                  'registry_name_binding_expired',
              FALSE
          )
        ORDER BY event.resource_id, event.subject, event.scope
        "#,
    ]
    .concat();
    sqlx::query(&permissions_query)
        .bind(chain_id)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to build permissions_current", error))?;
    wrapper_operators::build(transaction, chain_id, full_rebuild).await?;
    resource_summary::build(transaction, chain_id, target, full_rebuild).await?;
    Ok(())
}
