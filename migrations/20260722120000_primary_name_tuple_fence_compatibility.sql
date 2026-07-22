CREATE FUNCTION public.bigname_primary_name_tuple_lock_key(
    tuple_address text,
    tuple_namespace text,
    tuple_coin_type text
)
RETURNS bigint
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
STRICT
AS $$
    SELECT hashtextextended(
        format(
            '%s:%s:%s:%s:%s:%s:%s:%s',
            octet_length(current_database()),
            current_database(),
            octet_length(lower(tuple_address)),
            lower(tuple_address),
            octet_length(tuple_namespace),
            tuple_namespace,
            octet_length(tuple_coin_type),
            tuple_coin_type
        ),
        5786655296613795073
    ) & 9223372036854775807::bigint
$$;

CREATE FUNCTION public.bigname_primary_names_current_replacement_lock_key()
RETURNS bigint
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT hashtextextended(
        format(
            '%s:%s',
            octet_length(current_database()),
            current_database()
        ),
        -4776427281483431937
    )
$$;

CREATE FUNCTION public.bigname_invalidate_verified_primary_name_tuple(
    tuple_address text,
    tuple_namespace text,
    tuple_coin_type text
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM public.execution_cache_outcomes AS outcome
    WHERE outcome.request_type = 'verified_primary_name'
      AND outcome.namespace = tuple_namespace
      AND outcome.request_key =
          tuple_namespace || ':' || lower(tuple_address) || ':' || tuple_coin_type;
END
$$;

CREATE FUNCTION public.bigname_primary_names_current_fence_before_write()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    old_lock_key bigint;
    new_lock_key bigint;
    first_lock_key bigint;
    second_lock_key bigint;
BEGIN
    -- New replacement writers already hold the exclusive maintenance fence
    -- and perform cache invalidation in their publication transaction.
    IF current_setting(
        'bigname.primary_names_current_replacement_fence',
        true
    ) = 'on' THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    -- An origin/main full-rebuild binary disables these sidecar triggers before
    -- replacing the table, but does not know the advisory-lock protocol. Fence
    -- that legacy replacement globally and conservatively invalidate once.
    IF current_setting(
        'bigname.primary_names_current_legacy_replacement_fence',
        true
    ) = 'on' OR EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger
        WHERE tgrelid = TG_RELID
          AND tgname IN (
              'primary_names_current_identity_feed_after_claim_update',
              'primary_names_current_identity_feed_after_insert_delete'
          )
          AND tgenabled = 'D'
    ) THEN
        IF current_setting(
            'bigname.primary_names_current_legacy_replacement_fence',
            true
        ) IS DISTINCT FROM 'on' THEN
            IF NOT pg_try_advisory_xact_lock(
                public.bigname_primary_names_current_replacement_lock_key()
            ) THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    MESSAGE = 'primary_names_current legacy replacement crossed an active tuple fence',
                    HINT = 'retry the projection replacement';
            END IF;
            DELETE FROM public.execution_cache_outcomes
            WHERE request_type = 'verified_primary_name';
            PERFORM set_config(
                'bigname.primary_names_current_legacy_replacement_fence',
                'on',
                true
            );
        END IF;
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    -- Legacy writers acquire their table lock before a row trigger can run.
    -- Try, rather than wait for, the advisory locks so a new full replacement
    -- cannot deadlock against that reversed lock order. A 40001 rolls the old
    -- write back and its normal worker loop retries it.
    IF NOT pg_try_advisory_xact_lock_shared(
        public.bigname_primary_names_current_replacement_lock_key()
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'primary_names_current write crossed an active replacement fence',
            HINT = 'retry the projection write';
    END IF;

    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        old_lock_key := public.bigname_primary_name_tuple_lock_key(
            OLD.address,
            OLD.namespace,
            OLD.coin_type
        );
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        new_lock_key := public.bigname_primary_name_tuple_lock_key(
            NEW.address,
            NEW.namespace,
            NEW.coin_type
        );
    END IF;

    first_lock_key := CASE
        WHEN old_lock_key IS NULL THEN new_lock_key
        WHEN new_lock_key IS NULL THEN old_lock_key
        ELSE LEAST(old_lock_key, new_lock_key)
    END;
    second_lock_key := CASE
        WHEN old_lock_key IS NOT NULL
         AND new_lock_key IS NOT NULL
         AND old_lock_key <> new_lock_key
        THEN GREATEST(old_lock_key, new_lock_key)
        ELSE NULL
    END;

    IF NOT pg_try_advisory_xact_lock(first_lock_key) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'primary_names_current write crossed an active tuple fence',
            HINT = 'retry the projection write';
    END IF;
    IF second_lock_key IS NOT NULL
       AND NOT pg_try_advisory_xact_lock(second_lock_key) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            MESSAGE = 'primary_names_current key move crossed an active tuple fence',
            HINT = 'retry the projection write';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION public.bigname_primary_names_current_invalidate_after_write()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF current_setting(
        'bigname.primary_names_current_replacement_fence',
        true
    ) = 'on' OR current_setting(
        'bigname.primary_names_current_legacy_replacement_fence',
        true
    ) = 'on' THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE'
       AND OLD.address IS NOT DISTINCT FROM NEW.address
       AND OLD.namespace IS NOT DISTINCT FROM NEW.namespace
       AND OLD.coin_type IS NOT DISTINCT FROM NEW.coin_type
       AND OLD.claim_status IS NOT DISTINCT FROM NEW.claim_status
       AND OLD.raw_claim_name IS NOT DISTINCT FROM NEW.raw_claim_name
       AND OLD.normalized_claim_name IS NOT DISTINCT FROM NEW.normalized_claim_name
       AND OLD.claim_name_is_normalized IS NOT DISTINCT FROM NEW.claim_name_is_normalized
       AND OLD.claim_provenance IS NOT DISTINCT FROM NEW.claim_provenance THEN
        RETURN NEW;
    END IF;

    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        PERFORM public.bigname_invalidate_verified_primary_name_tuple(
            OLD.address,
            OLD.namespace,
            OLD.coin_type
        );
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE')
       AND (
           TG_OP = 'INSERT'
           OR OLD.address IS DISTINCT FROM NEW.address
           OR OLD.namespace IS DISTINCT FROM NEW.namespace
           OR OLD.coin_type IS DISTINCT FROM NEW.coin_type
       ) THEN
        PERFORM public.bigname_invalidate_verified_primary_name_tuple(
            NEW.address,
            NEW.namespace,
            NEW.coin_type
        );
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER primary_names_current_tuple_fence_before_write
    BEFORE INSERT OR UPDATE OR DELETE ON public.primary_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.bigname_primary_names_current_fence_before_write();

CREATE TRIGGER primary_names_current_cache_invalidation_after_write
    AFTER INSERT OR UPDATE OR DELETE ON public.primary_names_current
    FOR EACH ROW
    EXECUTE FUNCTION public.bigname_primary_names_current_invalidate_after_write();
