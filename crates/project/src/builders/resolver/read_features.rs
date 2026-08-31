pub(super) const DECLARED_READ_FEATURES: &str = r#"
COALESCE((
    SELECT declaration -> 'read_features'
    FROM jsonb_array_elements(COALESCE(
        manifest.manifest_payload -> 'contracts', '[]'::jsonb
    )) WITH ORDINALITY declarations(declaration, declaration_ordinality)
    WHERE lower(declaration ->> 'address') = candidate.resolver_address
      AND (declaration ->> 'start_block' IS NULL
           OR (declaration ->> 'start_block')::bigint <= $2)
    ORDER BY COALESCE((declaration ->> 'start_block')::bigint, 0) DESC,
             declaration_ordinality DESC
    LIMIT 1
), '[]'::jsonb)
"#;

pub(super) const IMPLEMENTATION_READ_FEATURES: &str = r#"
COALESCE((
    SELECT admitted -> 'read_features'
    FROM jsonb_array_elements(COALESCE(
        manifest.manifest_payload -> 'resolver_implementations', '[]'::jsonb
    )) WITH ORDINALITY implementations(admitted, admitted_ordinality)
    WHERE lower(admitted ->> 'address') =
          lower(upgrade.after_state ->> 'implementation')
      AND (admitted ->> 'start_block' IS NULL
           OR (admitted ->> 'start_block')::bigint <= $2)
    ORDER BY COALESCE((admitted ->> 'start_block')::bigint, 0) DESC,
             admitted_ordinality DESC
    LIMIT 1
), '[]'::jsonb)
"#;
