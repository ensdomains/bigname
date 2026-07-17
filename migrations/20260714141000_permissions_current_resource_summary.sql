CREATE TABLE public.permissions_current_resource_summary (
    resource_id uuid PRIMARY KEY
        REFERENCES public.resources(resource_id) ON DELETE CASCADE,
    authority_kind text,
    root_resource_id uuid,
    coverage jsonb NOT NULL,
    provenance jsonb NOT NULL,
    chain_positions jsonb NOT NULL,
    canonicality_summary jsonb NOT NULL,
    manifest_version bigint NOT NULL,
    last_recomputed_at timestamp with time zone NOT NULL,
    CONSTRAINT permissions_current_resource_summary_authority_kind_check
        CHECK (authority_kind IS NULL OR authority_kind <> ''),
    CONSTRAINT permissions_current_resource_summary_manifest_version_check
        CHECK (manifest_version > 0),
    CONSTRAINT permissions_current_resource_summary_coverage_object_check
        CHECK (jsonb_typeof(coverage) = 'object'),
    CONSTRAINT permissions_current_resource_summary_provenance_object_check
        CHECK (jsonb_typeof(provenance) = 'object'),
    CONSTRAINT permissions_current_resource_summary_chain_positions_object_check
        CHECK (jsonb_typeof(chain_positions) = 'object'),
    CONSTRAINT permissions_current_resource_summary_canonicality_object_check
        CHECK (jsonb_typeof(canonicality_summary) = 'object')
);

COMMENT ON TABLE public.permissions_current_resource_summary IS
    'Projection-owned per-resource authority and support metadata published with permissions_current, including resources with zero current permission rows.';

COMMENT ON COLUMN public.permissions_current_resource_summary.root_resource_id IS
    'Optional projection-owned ENSv2 registry root permission resource composed by public role reads.';
