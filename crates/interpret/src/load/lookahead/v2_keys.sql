array_remove(ARRAY[
    lower(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 1)) || ':' || lower(left(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 3), greatest(length(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 3)) - 8, 0))) || '00000000',
    lower(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 1)) || ':*',
    lower(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 1)) || ':' || lower(left(event.after_state ->> 'new_token_id', greatest(length(event.after_state ->> 'new_token_id') - 8, 0))) || '00000000',
    lower(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 1)) || ':' || lower(left(COALESCE(event.after_state ->> 'resource', event.after_state ->> 'upstream_resource'), greatest(length(COALESCE(event.after_state ->> 'resource', event.after_state ->> 'upstream_resource')) - 8, 0))) || '00000000',
    lower(split_part(event.raw_fact_ref ->> '{state_scope}', ':', 1)) || ':' || lower(left(event.after_state ->> 'labelhash', greatest(length(event.after_state ->> 'labelhash') - 8, 0))) || '00000000',
    lower(event.after_state ->> 'subregistry') || ':00000000'
]::text[], NULL)
