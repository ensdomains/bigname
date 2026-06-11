CREATE TABLE public.label_preimages (
    labelhash text NOT NULL,
    label text NOT NULL,
    normalized_label text NOT NULL,
    canonical_display_label text NOT NULL,
    source_kind text NOT NULL,
    source_priority integer NOT NULL,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    observed_at timestamp with time zone DEFAULT now() NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT label_preimages_pkey PRIMARY KEY (labelhash),
    CONSTRAINT label_preimages_labelhash_check CHECK (
        labelhash ~ '^0x[0-9a-f]{64}$'
    ),
    CONSTRAINT label_preimages_label_check CHECK (
        label <> ''
        AND normalized_label <> ''
        AND canonical_display_label <> ''
        AND position('.' IN normalized_label) = 0
    ),
    CONSTRAINT label_preimages_source_priority_check CHECK (source_priority >= 0),
    CONSTRAINT label_preimages_provenance_check CHECK (jsonb_typeof(provenance) = 'object')
);

CREATE INDEX label_preimages_normalized_label_idx
    ON public.label_preimages (normalized_label, labelhash);

CREATE TABLE public.label_preimage_backfill_runs (
    run_key text NOT NULL,
    completed_at timestamp with time zone DEFAULT now() NOT NULL,
    scanned_row_count bigint NOT NULL,
    retained_row_count bigint NOT NULL,
    invalidated_parent_count bigint NOT NULL,
    CONSTRAINT label_preimage_backfill_runs_pkey PRIMARY KEY (run_key),
    CONSTRAINT label_preimage_backfill_runs_scanned_check CHECK (scanned_row_count >= 0),
    CONSTRAINT label_preimage_backfill_runs_retained_check CHECK (retained_row_count >= 0),
    CONSTRAINT label_preimage_backfill_runs_invalidated_check CHECK (invalidated_parent_count >= 0)
);

ALTER TABLE public.children_current
    ADD COLUMN labelhash text,
    ADD COLUMN owner text,
    ADD COLUMN registrant text;

ALTER TABLE public.children_current
    DROP CONSTRAINT children_current_child_logical_name_id_fkey;

ALTER TABLE public.children_current
    ADD CONSTRAINT children_current_labelhash_check CHECK (
        labelhash IS NULL OR labelhash ~ '^0x[0-9a-f]{64}$'
    );

CREATE INDEX children_current_namehash_idx
    ON public.children_current (namespace, namehash);

WITH candidate_keys AS (
    SELECT DISTINCT
        'children_current'::text AS projection,
        cc.parent_logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', cc.parent_logical_name_id) AS key_payload
    FROM public.children_current cc
    JOIN public.name_surfaces parent
      ON parent.logical_name_id = cc.parent_logical_name_id
     AND parent.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
     )
    WHERE cc.surface_class = 'declared'

    UNION

    SELECT DISTINCT
        'children_current'::text AS projection,
        parent.logical_name_id AS projection_key,
        jsonb_build_object('parent_logical_name_id', parent.logical_name_id) AS key_payload
    FROM public.normalized_events ne
    JOIN public.name_surfaces parent
      ON parent.namehash = ne.after_state ->> 'parent_node'
     AND parent.namespace = ne.namespace
     AND parent.chain_id = ne.chain_id
     AND parent.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
     )
    WHERE ne.event_kind = 'SubregistryChanged'
      AND ne.derivation_kind = 'ens_v1_subregistry_changed'
      AND ne.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
      AND ne.canonicality_state IN (
          'canonical'::canonicality_state,
          'safe'::canonicality_state,
          'finalized'::canonicality_state
      )
      AND ne.after_state ->> 'parent_node' IS NOT NULL
      AND ne.after_state ->> 'child_node' IS NOT NULL
      AND ne.after_state ->> 'labelhash' IS NOT NULL
)
INSERT INTO public.projection_invalidations (
    projection,
    projection_key,
    key_payload,
    invalidated_at,
    last_changed_at
)
SELECT
    projection,
    projection_key,
    key_payload,
    now(),
    now()
FROM candidate_keys
ON CONFLICT (projection, projection_key)
DO UPDATE SET
    key_payload = EXCLUDED.key_payload,
    generation = projection_invalidations.generation + 1,
    invalidated_at = EXCLUDED.invalidated_at,
    last_changed_at = EXCLUDED.last_changed_at,
    claim_token = NULL,
    claimed_at = NULL,
    last_failure_reason = NULL,
    last_failure_at = NULL;
