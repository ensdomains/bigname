pub(super) const CTE: &str = r#"
expiry_retirements AS (
    SELECT DISTINCT ON (event.resource_id) event.resource_id, event.normalized_event_id,
           event.source_manifest_id, event.source_family, event.manifest_version,
           event.block_number, event.block_hash, event.transaction_index, event.log_index
    FROM project_events event
    WHERE event.resource_id IS NOT NULL
      AND event.event_kind = 'RegistrationReleased'
      AND event.after_state ->> 'source_event' = 'RegistryPathExpired'
      AND event.after_state ->> 'derived_from' = 'interpreter_state'
      AND event.after_state ->> 'terminal_reason' = 'registry_name_binding_expired'
      AND NOT EXISTS (
          SELECT 1 FROM project_events restoration
          WHERE restoration.resource_id = event.resource_id
            AND restoration.source_family IN (
                'ens_v2_root_l1', 'ens_v2_registry_l1', 'ens_v2_registrar_l1'
            )
            AND (
                restoration.block_number > event.block_number
                OR (
                    restoration.block_number = event.block_number
                    AND CASE
                        WHEN restoration.transaction_index IS NULL
                          OR event.transaction_index IS NULL
                          OR restoration.log_index IS NULL
                          OR event.log_index IS NULL
                            THEN restoration.normalized_event_id > event.normalized_event_id
                        ELSE ROW(
                            restoration.transaction_index, restoration.log_index,
                            restoration.normalized_event_id
                        ) > ROW(
                            event.transaction_index, event.log_index, event.normalized_event_id
                        )
                    END
                )
            )
            AND (
                restoration.event_kind IN ('RegistrationGranted', 'RegistrationReserved')
                OR (
                    restoration.event_kind = 'RegistrationRenewed'
                    AND restoration.after_state ->> 'revived_from_expiry' = 'true'
                    AND (
                        (restoration.after_state ->> 'status' = 'reserved'
                         AND restoration.after_state ->> 'reservation_resource' = 'true')
                        OR COALESCE((
                            SELECT expiry.logical_name_id IS NULL
                            FROM project_events expiry
                            WHERE expiry.resource_id = restoration.resource_id
                              AND expiry.event_kind = 'RegistrationReleased'
                              AND expiry.after_state ->> 'source_event' = 'RegistryPathExpired'
                              AND expiry.after_state ->> 'derived_from' = 'interpreter_state'
                              AND expiry.after_state ->> 'terminal_reason' = 'registry_name_binding_expired'
                              AND (
                                  expiry.block_number < restoration.block_number
                                  OR (
                                      expiry.block_number = restoration.block_number
                                      AND CASE
                                          WHEN expiry.transaction_index IS NULL
                                            OR restoration.transaction_index IS NULL
                                            OR expiry.log_index IS NULL
                                            OR restoration.log_index IS NULL
                                              THEN expiry.normalized_event_id < restoration.normalized_event_id
                                          ELSE ROW(
                                              expiry.transaction_index, expiry.log_index,
                                              expiry.normalized_event_id
                                          ) < ROW(
                                              restoration.transaction_index, restoration.log_index,
                                              restoration.normalized_event_id
                                          )
                                      END
                                  )
                              )
                            ORDER BY expiry.block_number DESC NULLS LAST,
                                     expiry.transaction_index DESC NULLS LAST,
                                     expiry.log_index DESC NULLS LAST,
                                     expiry.normalized_event_id DESC
                            LIMIT 1
                        ), FALSE)
                    )
                )
            )
      )
    ORDER BY event.resource_id, event.block_number DESC NULLS LAST,
             event.transaction_index DESC NULLS LAST, event.log_index DESC NULLS LAST,
             event.normalized_event_id DESC
)
"#;
