pub(super) const GAS_SPONSORSHIP_CURRENT_INVALIDATIONS_PREFIX: &str = r#"
WITH changed_events AS (
    SELECT ne.*, change.change_id, change.changed_at
    FROM projection_normalized_event_changes change
    JOIN normalized_events ne
      ON ne.normalized_event_id = change.normalized_event_id
    WHERE change.change_id > $1
      AND change.change_id <= $2
),
gas_sponsorship_name_events AS (
    SELECT *
    FROM changed_events
    WHERE namespace = 'ens'
      AND logical_name_id IS NOT NULL
      AND (
          (
              derivation_kind IN ('ens_v1_unwrapped_authority', 'ens_v2_registrar')
              AND event_kind IN (
                  'RegistrationGranted',
                  'RegistrarNameRegistered',
                  'RegistrationRenewed'
              )
          )
          OR (
              derivation_kind = 'entrypoint_user_operation'
              AND event_kind = 'SponsoredNameWriteObserved'
          )
      )
),
gas_sponsorship_global_events AS (
    SELECT *
    FROM changed_events
    WHERE derivation_kind = 'entrypoint_user_operation'
      AND event_kind IN ('SponsoredUserOperationObserved', 'PriceFeedAnswerUpdated')
),
candidate_keys AS (
    SELECT
        'gas_sponsorship_current'::TEXT AS projection,
        logical_name_id AS projection_key,
        jsonb_build_object('logical_name_id', logical_name_id) AS key_payload,
        normalized_event_id,
        change_id,
        changed_at
    FROM gas_sponsorship_name_events

    UNION ALL

    SELECT
        'gas_sponsorship_global_current'::TEXT AS projection,
        namespace AS projection_key,
        jsonb_build_object('namespace', namespace) AS key_payload,
        normalized_event_id,
        change_id,
        changed_at
    FROM gas_sponsorship_global_events
)
"#;
