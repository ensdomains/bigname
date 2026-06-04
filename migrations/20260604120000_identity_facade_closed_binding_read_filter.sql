CREATE OR REPLACE FUNCTION public.address_names_current_identity_row_readable(
    target_logical_name_id text,
    target_resource_id uuid,
    target_surface_binding_id uuid,
    target_token_lineage_id uuid
) RETURNS boolean
LANGUAGE sql
VOLATILE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM public.name_surfaces surface
        JOIN public.resources resource
          ON resource.resource_id = target_resource_id
        JOIN public.surface_bindings binding
          ON binding.surface_binding_id = target_surface_binding_id
        LEFT JOIN public.token_lineages token_lineage
          ON token_lineage.token_lineage_id = target_token_lineage_id
        WHERE surface.logical_name_id = target_logical_name_id
          AND surface.canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
          AND resource.canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
          AND binding.canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
          AND binding.active_to IS NULL
          AND (
              target_token_lineage_id IS NULL
              OR token_lineage.canonicality_state IN (
                  'canonical'::public.canonicality_state,
                  'safe'::public.canonicality_state,
                  'finalized'::public.canonicality_state
              )
          )
    )
    AND EXISTS (
        SELECT 1
        FROM public.name_current identity_nc
        JOIN public.name_surfaces identity_nc_surface
          ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
        LEFT JOIN public.resources identity_nc_resource
          ON identity_nc_resource.resource_id = identity_nc.resource_id
        LEFT JOIN public.surface_bindings identity_nc_binding
          ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
        LEFT JOIN public.token_lineages identity_nc_token_lineage
          ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
        WHERE identity_nc.logical_name_id = target_logical_name_id
          AND identity_nc_surface.canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
          AND (
              identity_nc.surface_binding_id IS NULL
              OR (
                  identity_nc_resource.canonicality_state IN (
                      'canonical'::public.canonicality_state,
                      'safe'::public.canonicality_state,
                      'finalized'::public.canonicality_state
                  )
                  AND identity_nc_binding.canonicality_state IN (
                      'canonical'::public.canonicality_state,
                      'safe'::public.canonicality_state,
                      'finalized'::public.canonicality_state
                  )
                  AND identity_nc_binding.active_to IS NULL
                  AND (
                      identity_nc.token_lineage_id IS NULL
                      OR identity_nc_token_lineage.canonicality_state IN (
                          'canonical'::public.canonicality_state,
                          'safe'::public.canonicality_state,
                          'finalized'::public.canonicality_state
                      )
                  )
              )
          )
    )
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_readable_relation_rows(
    target_address text
) RETURNS TABLE (
    address text,
    logical_name_id text,
    namespace text,
    canonical_display_name text,
    normalized_name text,
    namehash text,
    relation text,
    chain_positions jsonb,
    coverage jsonb
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        anc.address,
        anc.logical_name_id,
        anc.namespace,
        anc.canonical_display_name,
        anc.normalized_name,
        anc.namehash,
        anc.relation::text AS relation,
        anc.chain_positions,
        anc.coverage
    FROM public.address_names_current anc
    JOIN public.name_surfaces surface
      ON surface.logical_name_id = anc.logical_name_id
    JOIN public.resources resource
      ON resource.resource_id = anc.resource_id
    JOIN public.surface_bindings binding
      ON binding.surface_binding_id = anc.surface_binding_id
    LEFT JOIN public.token_lineages token_lineage
      ON token_lineage.token_lineage_id = anc.token_lineage_id
    JOIN public.name_current identity_nc
      ON identity_nc.logical_name_id = anc.logical_name_id
    JOIN public.name_surfaces identity_nc_surface
      ON identity_nc_surface.logical_name_id = identity_nc.logical_name_id
    LEFT JOIN public.resources identity_nc_resource
      ON identity_nc_resource.resource_id = identity_nc.resource_id
    LEFT JOIN public.surface_bindings identity_nc_binding
      ON identity_nc_binding.surface_binding_id = identity_nc.surface_binding_id
    LEFT JOIN public.token_lineages identity_nc_token_lineage
      ON identity_nc_token_lineage.token_lineage_id = identity_nc.token_lineage_id
    WHERE (target_address IS NULL OR anc.address = target_address)
      AND surface.canonicality_state IN (
          'canonical'::public.canonicality_state,
          'safe'::public.canonicality_state,
          'finalized'::public.canonicality_state
      )
      AND resource.canonicality_state IN (
          'canonical'::public.canonicality_state,
          'safe'::public.canonicality_state,
          'finalized'::public.canonicality_state
      )
      AND binding.canonicality_state IN (
          'canonical'::public.canonicality_state,
          'safe'::public.canonicality_state,
          'finalized'::public.canonicality_state
      )
      AND binding.active_to IS NULL
      AND (
          anc.token_lineage_id IS NULL
          OR token_lineage.canonicality_state IN (
              'canonical'::public.canonicality_state,
              'safe'::public.canonicality_state,
              'finalized'::public.canonicality_state
          )
      )
      AND identity_nc_surface.canonicality_state IN (
          'canonical'::public.canonicality_state,
          'safe'::public.canonicality_state,
          'finalized'::public.canonicality_state
      )
      AND (
          identity_nc.surface_binding_id IS NULL
          OR (
              identity_nc_resource.canonicality_state IN (
                  'canonical'::public.canonicality_state,
                  'safe'::public.canonicality_state,
                  'finalized'::public.canonicality_state
              )
              AND identity_nc_binding.canonicality_state IN (
                  'canonical'::public.canonicality_state,
                  'safe'::public.canonicality_state,
                  'finalized'::public.canonicality_state
              )
              AND identity_nc_binding.active_to IS NULL
              AND (
                  identity_nc.token_lineage_id IS NULL
                  OR identity_nc_token_lineage.canonicality_state IN (
                      'canonical'::public.canonicality_state,
                      'safe'::public.canonicality_state,
                      'finalized'::public.canonicality_state
                  )
              )
          )
      )
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_visible_relation_count(
    target_address text,
    target_logical_name_id text,
    target_roles text
) RETURNS bigint
LANGUAGE sql
VOLATILE
AS $$
    SELECT COUNT(*)::bigint
    FROM public.address_names_current_identity_readable_relation_rows(target_address) readable
    WHERE readable.logical_name_id = target_logical_name_id
      AND (
          target_roles = 'both'
          OR (
              target_roles = 'owned'
              AND readable.relation IN ('registrant', 'token_holder')
          )
          OR (
              target_roles = 'managed'
              AND readable.relation = 'effective_controller'
          )
      )
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_counts_recompute_address(
    target_address text
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM public.address_names_current_identity_counts_lock_address(target_address);

    DELETE FROM public.address_names_current_identity_counts
    WHERE address = target_address;

    WITH relation_groups AS (
        SELECT
            readable.address,
            readable.logical_name_id,
            BOOL_OR(readable.relation IN ('registrant', 'token_holder')) AS owned,
            BOOL_OR(readable.relation = 'effective_controller') AS managed
        FROM public.address_names_current_identity_readable_relation_rows(target_address) readable
        GROUP BY readable.address, readable.logical_name_id
    ),
    counts AS (
        SELECT address, 'owned'::text AS roles, COUNT(*)::bigint AS total_count
        FROM relation_groups
        WHERE owned
        GROUP BY address
        UNION ALL
        SELECT address, 'managed'::text AS roles, COUNT(*)::bigint AS total_count
        FROM relation_groups
        WHERE managed
        GROUP BY address
        UNION ALL
        SELECT address, 'both'::text AS roles, COUNT(*)::bigint AS total_count
        FROM relation_groups
        GROUP BY address
    )
    INSERT INTO public.address_names_current_identity_counts (address, roles, total_count)
    SELECT address, roles, total_count
    FROM counts
    WHERE total_count > 0
    ON CONFLICT (address, roles) DO UPDATE
    SET
        total_count = EXCLUDED.total_count,
        updated_at = now();
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_candidate_rows(
    target_address text
) RETURNS TABLE (
    address text,
    roles text,
    coin_type text,
    logical_name_id text,
    namespace text,
    canonical_display_name text,
    normalized_name text,
    namehash text,
    chain_positions jsonb,
    coverage jsonb,
    is_primary boolean,
    relation_facets text[]
)
LANGUAGE sql
STABLE
AS $$
    WITH readable_rows AS (
        SELECT
            readable.address,
            readable.logical_name_id,
            readable.namespace,
            readable.canonical_display_name,
            readable.normalized_name,
            readable.namehash,
            readable.relation,
            readable.chain_positions,
            readable.coverage,
            CASE
                WHEN readable.relation IN ('registrant', 'token_holder') THEN 0::smallint
                ELSE 1::smallint
            END AS role_rank
        FROM public.address_names_current_identity_readable_relation_rows(target_address) readable
    ),
    role_rows AS (
        SELECT
            rr.address,
            'owned'::text AS roles,
            rr.logical_name_id,
            rr.namespace,
            rr.canonical_display_name,
            rr.normalized_name,
            rr.namehash,
            rr.relation,
            rr.chain_positions,
            rr.coverage,
            rr.role_rank
        FROM readable_rows rr
        WHERE rr.relation IN ('registrant', 'token_holder')
        UNION ALL
        SELECT
            rr.address,
            'managed'::text AS roles,
            rr.logical_name_id,
            rr.namespace,
            rr.canonical_display_name,
            rr.normalized_name,
            rr.namehash,
            rr.relation,
            rr.chain_positions,
            rr.coverage,
            rr.role_rank
        FROM readable_rows rr
        WHERE rr.relation = 'effective_controller'
        UNION ALL
        SELECT
            rr.address,
            'both'::text AS roles,
            rr.logical_name_id,
            rr.namespace,
            rr.canonical_display_name,
            rr.normalized_name,
            rr.namehash,
            rr.relation,
            rr.chain_positions,
            rr.coverage,
            rr.role_rank
        FROM readable_rows rr
    ),
    facets AS (
        SELECT
            rr.address,
            rr.roles,
            rr.logical_name_id,
            ARRAY_AGG(
                rr.relation
                ORDER BY
                    CASE rr.relation
                        WHEN 'registrant' THEN 0
                        WHEN 'token_holder' THEN 1
                        WHEN 'effective_controller' THEN 2
                        ELSE 99
                    END
            ) AS relation_facets
        FROM role_rows rr
        GROUP BY rr.address, rr.roles, rr.logical_name_id
    ),
    fallback_ranked AS (
        SELECT
            rr.*,
            ROW_NUMBER() OVER (
                PARTITION BY rr.address, rr.roles
                ORDER BY
                    rr.role_rank ASC,
                    rr.normalized_name ASC,
                    rr.namespace ASC,
                    rr.namehash ASC,
                    rr.logical_name_id ASC
            ) AS row_rank
        FROM role_rows rr
    ),
    fallback_rows AS (
        SELECT
            ranked.address,
            ranked.roles,
            ''::text AS coin_type,
            ranked.logical_name_id,
            ranked.namespace,
            ranked.canonical_display_name,
            ranked.normalized_name,
            ranked.namehash,
            ranked.chain_positions,
            ranked.coverage,
            FALSE AS is_primary,
            facets.relation_facets
        FROM fallback_ranked ranked
        JOIN facets
          ON facets.address = ranked.address
         AND facets.roles = ranked.roles
         AND facets.logical_name_id = ranked.logical_name_id
        WHERE ranked.row_rank = 1
    ),
    primary_ranked AS (
        SELECT
            rr.*,
            pnc.coin_type,
            ROW_NUMBER() OVER (
                PARTITION BY rr.address, pnc.coin_type, rr.roles
                ORDER BY
                    rr.role_rank ASC,
                    rr.normalized_name ASC,
                    rr.namespace ASC,
                    rr.namehash ASC,
                    rr.logical_name_id ASC
            ) AS row_rank
        FROM role_rows rr
        JOIN public.primary_names_current pnc
          ON pnc.address = rr.address
         AND pnc.namespace = rr.namespace
         AND pnc.normalized_claim_name = rr.normalized_name
        WHERE pnc.claim_status = 'success'
          AND (target_address IS NULL OR pnc.address = target_address)
    ),
    primary_rows AS (
        SELECT
            ranked.address,
            ranked.roles,
            ranked.coin_type,
            ranked.logical_name_id,
            ranked.namespace,
            ranked.canonical_display_name,
            ranked.normalized_name,
            ranked.namehash,
            ranked.chain_positions,
            ranked.coverage,
            TRUE AS is_primary,
            facets.relation_facets
        FROM primary_ranked ranked
        JOIN facets
          ON facets.address = ranked.address
         AND facets.roles = ranked.roles
         AND facets.logical_name_id = ranked.logical_name_id
        WHERE ranked.row_rank = 1
    )
    SELECT * FROM fallback_rows
    UNION ALL
    SELECT * FROM primary_rows
$$;

DROP TRIGGER IF EXISTS address_names_current_identity_counts_binding_canonicality
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_binding_readability_update
    ON public.surface_bindings;

CREATE TRIGGER address_names_current_identity_counts_binding_readability_update
    AFTER UPDATE OF canonicality_state, active_to ON public.surface_bindings
    FOR EACH ROW
    WHEN (
        OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state
        OR OLD.active_to IS DISTINCT FROM NEW.active_to
    )
    EXECUTE FUNCTION public.address_names_current_identity_counts_recompute_for_binding();

DROP TRIGGER IF EXISTS surface_bindings_identity_feed_after_change
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS surface_bindings_identity_feed_after_canonicality_update
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS surface_bindings_identity_feed_after_readability_update
    ON public.surface_bindings;

CREATE TRIGGER surface_bindings_identity_feed_after_readability_update
    AFTER UPDATE OF canonicality_state, active_to ON public.surface_bindings
    FOR EACH ROW
    WHEN (
        OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state
        OR OLD.active_to IS DISTINCT FROM NEW.active_to
    )
    EXECUTE FUNCTION public.address_names_current_identity_feed_binding_trigger();

TRUNCATE TABLE public.address_names_current_identity_counts;

WITH relation_groups AS (
    SELECT
        readable.address,
        readable.logical_name_id,
        BOOL_OR(readable.relation IN ('registrant', 'token_holder')) AS owned,
        BOOL_OR(readable.relation = 'effective_controller') AS managed
    FROM public.address_names_current_identity_readable_relation_rows(NULL::text) readable
    GROUP BY readable.address, readable.logical_name_id
),
counts AS (
    SELECT address, 'owned'::text AS roles, COUNT(*)::bigint AS total_count
    FROM relation_groups
    WHERE owned
    GROUP BY address
    UNION ALL
    SELECT address, 'managed'::text AS roles, COUNT(*)::bigint AS total_count
    FROM relation_groups
    WHERE managed
    GROUP BY address
    UNION ALL
    SELECT address, 'both'::text AS roles, COUNT(*)::bigint AS total_count
    FROM relation_groups
    GROUP BY address
)
INSERT INTO public.address_names_current_identity_counts (address, roles, total_count)
SELECT address, roles, total_count
FROM counts
WHERE total_count > 0
ON CONFLICT (address, roles) DO UPDATE
SET
    total_count = EXCLUDED.total_count,
    updated_at = now();

TRUNCATE TABLE public.address_names_current_identity_feed;

INSERT INTO public.address_names_current_identity_feed (
    address,
    roles,
    coin_type,
    logical_name_id,
    namespace,
    canonical_display_name,
    normalized_name,
    namehash,
    chain_positions,
    coverage,
    is_primary,
    relation_facets,
    last_recomputed_at
)
SELECT
    candidate.address,
    candidate.roles,
    candidate.coin_type,
    candidate.logical_name_id,
    candidate.namespace,
    candidate.canonical_display_name,
    candidate.normalized_name,
    candidate.namehash,
    candidate.chain_positions,
    candidate.coverage,
    candidate.is_primary,
    candidate.relation_facets,
    now()
FROM public.address_names_current_identity_feed_candidate_rows(NULL::text) candidate
ON CONFLICT (address, roles, coin_type) DO UPDATE
SET
    logical_name_id = EXCLUDED.logical_name_id,
    namespace = EXCLUDED.namespace,
    canonical_display_name = EXCLUDED.canonical_display_name,
    normalized_name = EXCLUDED.normalized_name,
    namehash = EXCLUDED.namehash,
    chain_positions = EXCLUDED.chain_positions,
    coverage = EXCLUDED.coverage,
    is_primary = EXCLUDED.is_primary,
    relation_facets = EXCLUDED.relation_facets,
    last_recomputed_at = EXCLUDED.last_recomputed_at;
