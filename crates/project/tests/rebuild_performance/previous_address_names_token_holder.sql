            LEFT JOIN LATERAL (
                SELECT lower(event.after_state ->> 'to') AS token_holder,
                       event.*
                FROM project_authority_events event
                WHERE event.normalized_event_id = registration.normalized_event_id
                  AND event.event_kind = 'TokenControlTransferred'
                ORDER BY event.block_number DESC NULLS LAST,
                         event.transaction_index DESC NULLS LAST,
                         event.log_index DESC NULLS LAST,
                         event.normalized_event_id DESC
                LIMIT 1
            ) token_holder ON TRUE
