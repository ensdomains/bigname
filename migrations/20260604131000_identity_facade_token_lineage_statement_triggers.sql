CREATE OR REPLACE FUNCTION public.anc_identity_counts_recompute_logical_names(
    target_logical_name_ids text[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT DISTINCT anc.address
        FROM public.address_names_current anc
        WHERE anc.logical_name_id = ANY(target_logical_name_ids)
        ORDER BY anc.address
    LOOP
        PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_identity_counts_recompute_resources(
    target_resource_ids uuid[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT address
        FROM (
            SELECT anc.address
            FROM public.address_names_current anc
            WHERE anc.resource_id = ANY(target_resource_ids)
            UNION
            SELECT anc.address
            FROM public.name_current nc
            JOIN public.address_names_current anc
              ON anc.logical_name_id = nc.logical_name_id
            WHERE nc.resource_id = ANY(target_resource_ids)
        ) affected_addresses
        ORDER BY address
    LOOP
        PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_identity_counts_recompute_bindings(
    target_surface_binding_ids uuid[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT address
        FROM (
            SELECT anc.address
            FROM public.address_names_current anc
            WHERE anc.surface_binding_id = ANY(target_surface_binding_ids)
            UNION
            SELECT anc.address
            FROM public.name_current nc
            JOIN public.address_names_current anc
              ON anc.logical_name_id = nc.logical_name_id
            WHERE nc.surface_binding_id = ANY(target_surface_binding_ids)
        ) affected_addresses
        ORDER BY address
    LOOP
        PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.anc_identity_counts_recompute_token_lineages(
    target_token_lineage_ids uuid[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT address
        FROM (
            SELECT anc.address
            FROM public.address_names_current anc
            WHERE anc.token_lineage_id = ANY(target_token_lineage_ids)
            UNION
            SELECT anc.address
            FROM public.name_current nc
            JOIN public.address_names_current anc
              ON anc.logical_name_id = nc.logical_name_id
            WHERE nc.token_lineage_id = ANY(target_token_lineage_ids)
        ) affected_addresses
        ORDER BY address
    LOOP
        PERFORM public.address_names_current_identity_counts_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_recompute_for_resources(
    target_resource_ids uuid[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT address
        FROM (
            SELECT anc.address
            FROM public.address_names_current anc
            WHERE anc.resource_id = ANY(target_resource_ids)
            UNION
            SELECT anc.address
            FROM public.name_current nc
            JOIN public.address_names_current anc
              ON anc.logical_name_id = nc.logical_name_id
            WHERE nc.resource_id = ANY(target_resource_ids)
        ) affected_addresses
        ORDER BY address
    LOOP
        PERFORM public.address_names_current_identity_feed_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_recompute_for_bindings(
    target_surface_binding_ids uuid[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT address
        FROM (
            SELECT anc.address
            FROM public.address_names_current anc
            WHERE anc.surface_binding_id = ANY(target_surface_binding_ids)
            UNION
            SELECT anc.address
            FROM public.name_current nc
            JOIN public.address_names_current anc
              ON anc.logical_name_id = nc.logical_name_id
            WHERE nc.surface_binding_id = ANY(target_surface_binding_ids)
        ) affected_addresses
        ORDER BY address
    LOOP
        PERFORM public.address_names_current_identity_feed_recompute_address(target_address);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION public.address_names_current_identity_feed_recompute_for_token_lineage(
    target_token_lineage_ids uuid[]
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    target_address text;
BEGIN
    FOR target_address IN
        SELECT address
        FROM (
            SELECT anc.address
            FROM public.address_names_current anc
            WHERE anc.token_lineage_id = ANY(target_token_lineage_ids)
            UNION
            SELECT anc.address
            FROM public.name_current nc
            JOIN public.address_names_current anc
              ON anc.logical_name_id = nc.logical_name_id
            WHERE nc.token_lineage_id = ANY(target_token_lineage_ids)
        ) affected_addresses
        ORDER BY address
    LOOP
        PERFORM public.address_names_current_identity_feed_recompute_address(target_address);
    END LOOP;
END;
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
    WHERE old_rows.canonicality_state IS DISTINCT FROM new_rows.canonicality_state;

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
    WHERE old_rows.canonicality_state IS DISTINCT FROM new_rows.canonicality_state;

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
    WHERE old_rows.canonicality_state IS DISTINCT FROM new_rows.canonicality_state
       OR old_rows.active_to IS DISTINCT FROM new_rows.active_to;

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
    WHERE old_rows.canonicality_state IS DISTINCT FROM new_rows.canonicality_state;

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
    WHERE old_rows.canonicality_state IS DISTINCT FROM new_rows.canonicality_state;

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
    WHERE old_rows.canonicality_state IS DISTINCT FROM new_rows.canonicality_state;

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
    WHERE old_rows.canonicality_state IS DISTINCT FROM new_rows.canonicality_state
       OR old_rows.active_to IS DISTINCT FROM new_rows.active_to;

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
    WHERE old_rows.canonicality_state IS DISTINCT FROM new_rows.canonicality_state;

    IF target_token_lineage_ids IS NOT NULL THEN
        PERFORM public.address_names_current_identity_feed_recompute_for_token_lineage(
            target_token_lineage_ids
        );
    END IF;

    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS address_names_current_identity_counts_surface_canonicality
    ON public.name_surfaces;
DROP TRIGGER IF EXISTS name_surfaces_identity_feed_after_canonicality_update
    ON public.name_surfaces;
DROP TRIGGER IF EXISTS anc_counts_name_surfaces_stmt
    ON public.name_surfaces;
DROP TRIGGER IF EXISTS anc_feed_name_surfaces_stmt
    ON public.name_surfaces;

CREATE TRIGGER anc_counts_name_surfaces_stmt
    AFTER UPDATE ON public.name_surfaces
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.anc_counts_name_surfaces_stmt_trigger();

CREATE TRIGGER anc_feed_name_surfaces_stmt
    AFTER UPDATE ON public.name_surfaces
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.anc_feed_name_surfaces_stmt_trigger();

DROP TRIGGER IF EXISTS address_names_current_identity_counts_resource_canonicality
    ON public.resources;
DROP TRIGGER IF EXISTS resources_identity_feed_after_canonicality_update
    ON public.resources;
DROP TRIGGER IF EXISTS anc_counts_resources_stmt
    ON public.resources;
DROP TRIGGER IF EXISTS anc_feed_resources_stmt
    ON public.resources;

CREATE TRIGGER anc_counts_resources_stmt
    AFTER UPDATE ON public.resources
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.anc_counts_resources_stmt_trigger();

CREATE TRIGGER anc_feed_resources_stmt
    AFTER UPDATE ON public.resources
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.anc_feed_resources_stmt_trigger();

DROP TRIGGER IF EXISTS address_names_current_identity_counts_binding_canonicality
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_binding_readability_updat
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS surface_bindings_identity_feed_after_canonicality_update
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS surface_bindings_identity_feed_after_readability_update
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS anc_counts_bindings_stmt
    ON public.surface_bindings;
DROP TRIGGER IF EXISTS anc_feed_bindings_stmt
    ON public.surface_bindings;

CREATE TRIGGER anc_counts_bindings_stmt
    AFTER UPDATE ON public.surface_bindings
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.anc_counts_bindings_stmt_trigger();

CREATE TRIGGER anc_feed_bindings_stmt
    AFTER UPDATE ON public.surface_bindings
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.anc_feed_bindings_stmt_trigger();

DROP TRIGGER IF EXISTS address_names_current_identity_counts_token_lineage_canonicalit
    ON public.token_lineages;
DROP TRIGGER IF EXISTS address_names_current_identity_counts_token_lineage_canonicality
    ON public.token_lineages;
DROP TRIGGER IF EXISTS token_lineages_identity_feed_after_canonicality_update
    ON public.token_lineages;
DROP TRIGGER IF EXISTS anc_counts_token_lineages_stmt
    ON public.token_lineages;
DROP TRIGGER IF EXISTS anc_feed_token_lineages_stmt
    ON public.token_lineages;

CREATE TRIGGER anc_counts_token_lineages_stmt
    AFTER UPDATE ON public.token_lineages
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.anc_counts_token_lineages_stmt_trigger();

CREATE TRIGGER anc_feed_token_lineages_stmt
    AFTER UPDATE ON public.token_lineages
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.anc_feed_token_lineages_stmt_trigger();

DROP FUNCTION IF EXISTS public.address_names_current_identity_counts_recompute_for_token_linea(uuid[]);
DROP FUNCTION IF EXISTS public.address_names_current_identity_counts_token_lineage_statement_t();
DROP FUNCTION IF EXISTS public.address_names_current_identity_feed_token_lineage_statement_tri();
