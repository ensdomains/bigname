/// The per-section summaries of `resolver_current.declared_summary`, spliced into the
/// build statement after the `summarized` CTE. `$5` is the sample limit.
pub(super) const SECTION_SUMMARIES: &str = r#"
                   'bindings', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported', 'count', binding_count,
                       'total_count', binding_count, 'sample_limit', $5,
                       'sample_count', jsonb_array_length(binding_items),
                       'truncated', binding_count > jsonb_array_length(binding_items),
                       'items', binding_items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'aliases', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported', 'count', alias_count,
                       'total_count', alias_count, 'sample_limit', $5,
                       'sample_count', jsonb_array_length(alias_items),
                       'truncated', alias_count > jsonb_array_length(alias_items),
                       'items', alias_items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'links', CASE WHEN links_supported THEN jsonb_build_object(
                       'status', 'supported', 'count', link_count,
                       'total_count', link_count, 'record_count', linked_record_count,
                       'sample_limit', $5,
                       'sample_count', jsonb_array_length(link_items),
                       'truncated', link_count > jsonb_array_length(link_items),
                       'items', link_items
                   ) WHEN supported THEN jsonb_build_object(
                       'status', 'unsupported',
                       'unsupported_reason', 'record_links_not_applicable'
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'permissions', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported', 'count', permission_count,
                       'total_count', permission_count, 'sample_limit', $5,
                       'sample_count', jsonb_array_length(permission_items),
                       'truncated', permission_count > jsonb_array_length(permission_items),
                       'items', permission_items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'role_holders', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported', 'count', role_count,
                       'total_count', role_count, 'sample_limit', $5,
                       'sample_count', jsonb_array_length(role_items),
                       'truncated', role_count > jsonb_array_length(role_items),
                       'items', role_items
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'event_summary', CASE WHEN enumeration_supported THEN jsonb_build_object(
                       'status', 'supported',
                       'count', binding_count + alias_event_count + permission_event_count,
                       'by_kind', jsonb_strip_nulls(jsonb_build_object(
                           'ResolverChanged', NULLIF(binding_count, 0),
                           'AliasChanged', NULLIF(alias_event_count, 0),
                           'PermissionChanged', NULLIF(permission_event_count, 0)
                       ))
                   ) ELSE jsonb_build_object(
                       'status', 'unsupported', 'unsupported_reason', enumeration_reason
                   ) END,
                   'coverage', jsonb_build_object(
                       'status', 'projected', 'exhaustiveness', 'not_asserted'
                   )
"#;
