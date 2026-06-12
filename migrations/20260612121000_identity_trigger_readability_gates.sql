CREATE OR REPLACE FUNCTION public.identity_canonicality_readable(
    state public.canonicality_state
) RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT state IN (
        'canonical'::public.canonicality_state,
        'safe'::public.canonicality_state,
        'finalized'::public.canonicality_state
    );
$$;

CREATE OR REPLACE FUNCTION public.identity_canonicality_readability_changed(
    old_state public.canonicality_state,
    new_state public.canonicality_state
) RETURNS boolean
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT public.identity_canonicality_readable(old_state)
        IS DISTINCT FROM public.identity_canonicality_readable(new_state);
$$;

CREATE OR REPLACE FUNCTION public.anc_counts_name_surfaces_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_logical_name_ids text[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.logical_name_id)
    INTO target_logical_name_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.logical_name_id = new_rows.logical_name_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_logical_name_ids IS NOT NULL THEN
        PERFORM public.anc_identity_counts_recompute_logical_names(target_logical_name_ids);
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_counts_resources_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_resource_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.resource_id)
    INTO target_resource_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.resource_id = new_rows.resource_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_resource_ids IS NOT NULL THEN
        PERFORM public.anc_identity_counts_recompute_resources(target_resource_ids);
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_counts_bindings_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_surface_binding_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.surface_binding_id)
    INTO target_surface_binding_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.surface_binding_id = new_rows.surface_binding_id
    WHERE old_rows.active_to IS DISTINCT FROM new_rows.active_to
       OR public.identity_canonicality_readability_changed(
           old_rows.canonicality_state,
           new_rows.canonicality_state
       );

    IF target_surface_binding_ids IS NOT NULL THEN
        PERFORM public.anc_identity_counts_recompute_bindings(target_surface_binding_ids);
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_counts_token_lineages_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_token_lineage_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.token_lineage_id)
    INTO target_token_lineage_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.token_lineage_id = new_rows.token_lineage_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_token_lineage_ids IS NOT NULL THEN
        PERFORM public.anc_identity_counts_recompute_token_lineages(target_token_lineage_ids);
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_feed_name_surfaces_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_logical_name_ids text[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.logical_name_id)
    INTO target_logical_name_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.logical_name_id = new_rows.logical_name_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_logical_name_ids IS NOT NULL THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_logical_names(
            target_logical_name_ids
        );
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_feed_resources_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_resource_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.resource_id)
    INTO target_resource_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.resource_id = new_rows.resource_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_resource_ids IS NOT NULL THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_resources(
            target_resource_ids
        );
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_feed_bindings_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_surface_binding_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.surface_binding_id)
    INTO target_surface_binding_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.surface_binding_id = new_rows.surface_binding_id
    WHERE old_rows.active_to IS DISTINCT FROM new_rows.active_to
       OR public.identity_canonicality_readability_changed(
           old_rows.canonicality_state,
           new_rows.canonicality_state
       );

    IF target_surface_binding_ids IS NOT NULL THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_bindings(
            target_surface_binding_ids
        );
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_feed_token_lineages_stmt_trigger()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    target_token_lineage_ids uuid[];
BEGIN
    SELECT ARRAY_AGG(DISTINCT new_rows.token_lineage_id)
    INTO target_token_lineage_ids
    FROM new_rows
    JOIN old_rows
      ON old_rows.token_lineage_id = new_rows.token_lineage_id
    WHERE public.identity_canonicality_readability_changed(
        old_rows.canonicality_state,
        new_rows.canonicality_state
    );

    IF target_token_lineage_ids IS NOT NULL THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_token_lineage(
            target_token_lineage_ids
        );
    END IF;

    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.queue_surface_binding_repair_projection_invalidations()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF to_regclass('public.projection_invalidations') IS NULL THEN
        RETURN NULL;
    END IF;

    INSERT INTO public.projection_invalidations (
        projection,
        projection_key,
        key_payload,
        last_changed_at,
        invalidated_at
    )
    WITH affected_names AS (
        SELECT DISTINCT old_row.logical_name_id
        FROM old_surface_bindings old_row
        JOIN new_surface_bindings new_row
          ON new_row.surface_binding_id = old_row.surface_binding_id
        WHERE btrim(old_row.logical_name_id) <> ''
          AND (
              old_row.active_to IS DISTINCT FROM new_row.active_to
              OR old_row.canonicality_state IS DISTINCT FROM new_row.canonicality_state
          )
    )
    SELECT
        'name_current'::TEXT AS projection,
        logical_name_id AS projection_key,
        jsonb_build_object('logical_name_id', logical_name_id) AS key_payload,
        now() AS last_changed_at,
        now() AS invalidated_at
    FROM affected_names
    GROUP BY logical_name_id
    ON CONFLICT (projection, projection_key)
    DO UPDATE SET
        key_payload = EXCLUDED.key_payload,
        generation = projection_invalidations.generation + 1,
        last_changed_at = GREATEST(
            projection_invalidations.last_changed_at,
            EXCLUDED.last_changed_at
        ),
        invalidated_at = EXCLUDED.invalidated_at,
        claim_token = NULL,
        claimed_at = NULL,
        last_failure_reason = NULL,
        last_failure_at = NULL;

    INSERT INTO public.projection_invalidations (
        projection,
        projection_key,
        key_payload,
        last_changed_at,
        invalidated_at
    )
    WITH affected_names AS (
        SELECT DISTINCT old_row.logical_name_id
        FROM old_surface_bindings old_row
        JOIN new_surface_bindings new_row
          ON new_row.surface_binding_id = old_row.surface_binding_id
        WHERE btrim(old_row.logical_name_id) <> ''
          AND (
              old_row.active_to IS DISTINCT FROM new_row.active_to
              OR old_row.canonicality_state IS DISTINCT FROM new_row.canonicality_state
          )
    ),
    projected_addresses AS (
        SELECT DISTINCT
            lower(address) AS address,
            logical_name_id
        FROM public.address_names_current
        WHERE logical_name_id IN (
            SELECT logical_name_id FROM affected_names
        )
    ),
    event_addresses AS (
        SELECT DISTINCT
            lower(address.address) AS address,
            ne.logical_name_id
        FROM public.normalized_events ne
        JOIN affected_names affected
          ON affected.logical_name_id = ne.logical_name_id
        CROSS JOIN LATERAL (
            VALUES
                (ne.after_state ->> 'registrant'),
                (ne.before_state ->> 'registrant'),
                (ne.after_state ->> 'to'),
                (ne.before_state ->> 'to'),
                (ne.after_state ->> 'owner'),
                (ne.before_state ->> 'owner')
        ) AS address(address)
        WHERE ne.event_kind IN (
            'RegistrationGranted',
            'TokenControlTransferred',
            'AuthorityTransferred',
            'AuthorityEpochChanged',
            'TokenRegenerated'
        )
          AND ne.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND address.address IS NOT NULL
          AND address.address <> ''

        UNION

        SELECT DISTINCT
            lower(address.address) AS address,
            ne.logical_name_id
        FROM public.normalized_events ne
        JOIN affected_names affected
          ON affected.logical_name_id = ne.logical_name_id
        CROSS JOIN LATERAL (
            VALUES
                (ne.after_state ->> 'subject', ne.after_state -> 'scope'),
                (ne.before_state ->> 'subject', ne.before_state -> 'scope')
        ) AS address(address, scope)
        WHERE ne.event_kind = 'PermissionChanged'
          AND address.scope ->> 'kind' = 'resource'
          AND ne.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND address.address IS NOT NULL
          AND address.address <> ''
    ),
    candidate_keys AS (
        SELECT address, logical_name_id
        FROM projected_addresses

        UNION

        SELECT address, logical_name_id
        FROM event_addresses
    )
    SELECT
        'address_names_current'::TEXT AS projection,
        address || ':' || logical_name_id AS projection_key,
        jsonb_build_object(
            'address', address,
            'logical_name_id', logical_name_id
        ) AS key_payload,
        now() AS last_changed_at,
        now() AS invalidated_at
    FROM candidate_keys
    WHERE btrim(address) <> ''
      AND btrim(logical_name_id) <> ''
    GROUP BY address, logical_name_id
    ON CONFLICT (projection, projection_key)
    DO UPDATE SET
        key_payload = EXCLUDED.key_payload,
        generation = projection_invalidations.generation + 1,
        last_changed_at = GREATEST(
            projection_invalidations.last_changed_at,
            EXCLUDED.last_changed_at
        ),
        invalidated_at = EXCLUDED.invalidated_at,
        claim_token = NULL,
        claimed_at = NULL,
        last_failure_reason = NULL,
        last_failure_at = NULL;

    RETURN NULL;
END;
$$;
