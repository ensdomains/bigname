CREATE FUNCTION public.bigname_lock_primary_name_anchor(
    tuple_address text,
    tuple_namespace text,
    tuple_coin_type text
)
RETURNS TABLE (
    claim_status text,
    normalized_claim_name text,
    claim_name_is_normalized boolean
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        anchor.claim_status,
        anchor.normalized_claim_name,
        anchor.claim_name_is_normalized
    FROM public.primary_names_current AS anchor
    WHERE anchor.address = tuple_address
      AND anchor.namespace = tuple_namespace
      AND anchor.coin_type = tuple_coin_type
    FOR UPDATE
$$;

REVOKE ALL ON FUNCTION public.bigname_lock_primary_name_anchor(
    text, text, text
) FROM PUBLIC;

COMMENT ON FUNCTION public.bigname_lock_primary_name_anchor(
    text, text, text
) IS
    'Locks and returns one retained primary-name projection anchor without granting its caller direct projection write access.';
