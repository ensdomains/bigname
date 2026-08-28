pub(super) const DECLARED_READ_FEATURES: &str = r#"
COALESCE((
    SELECT declaration -> 'read_features'
    FROM jsonb_array_elements(COALESCE(
        manifest.manifest_payload -> 'contracts', '[]'::jsonb
    )) declaration
    WHERE lower(declaration ->> 'address') = candidate.resolver_address
      AND (declaration ->> 'start_block' IS NULL
           OR (declaration ->> 'start_block')::bigint <= $2)
    ORDER BY declaration ->> 'role', declaration::text
    LIMIT 1
), '[]'::jsonb)
"#;

pub(super) const IMPLEMENTATION_READ_FEATURES: &str = r#"
COALESCE((
    SELECT admitted -> 'read_features'
    FROM jsonb_array_elements(COALESCE(
        manifest.manifest_payload -> 'resolver_implementations', '[]'::jsonb
    )) admitted
    WHERE lower(admitted ->> 'address') =
          lower(upgrade.after_state ->> 'implementation')
    ORDER BY admitted ->> 'role', admitted::text
    LIMIT 1
), '[]'::jsonb)
"#;
