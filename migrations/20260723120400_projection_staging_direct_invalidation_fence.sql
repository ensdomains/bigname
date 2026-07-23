-- Preserve a monotonic, key-scoped fence for invalidation generations even
-- after continuous apply removes the live queue row. Normalized-event derive
-- has its own complete-prefix journal and opts out through a transaction-local
-- setting; every other producer is captured by default.
CREATE SEQUENCE public.projection_direct_invalidation_revision_seq AS BIGINT;

CREATE TABLE public.projection_direct_invalidation_revisions (
    projection TEXT NOT NULL,
    projection_key TEXT NOT NULL,
    key_payload JSONB NOT NULL,
    generation BIGINT NOT NULL,
    revision BIGINT NOT NULL,
    changed_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    PRIMARY KEY (projection, projection_key),
    CONSTRAINT projection_direct_invalidation_revisions_generation_check CHECK (
        generation >= 0
    ),
    CONSTRAINT projection_direct_invalidation_revisions_revision_check CHECK (
        revision > 0
    )
);

CREATE INDEX projection_direct_invalidation_revisions_projection_revision_idx
    ON public.projection_direct_invalidation_revisions (projection, revision);

CREATE INDEX projection_direct_invalidation_revisions_revision_idx
    ON public.projection_direct_invalidation_revisions (revision);

CREATE FUNCTION public.record_projection_direct_invalidation_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF current_setting(
        'bigname.normalized_projection_invalidation_derive',
        true
    ) = 'on' THEN
        RETURN NULL;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.generation IS NOT DISTINCT FROM OLD.generation THEN
            RETURN NULL;
        END IF;
    END IF;

    INSERT INTO public.projection_direct_invalidation_revisions (
        projection,
        projection_key,
        key_payload,
        generation,
        revision,
        changed_at
    )
    VALUES (
        NEW.projection,
        NEW.projection_key,
        NEW.key_payload,
        NEW.generation,
        nextval('public.projection_direct_invalidation_revision_seq'),
        clock_timestamp()
    )
    ON CONFLICT (projection, projection_key)
    DO UPDATE SET
        key_payload = EXCLUDED.key_payload,
        generation = EXCLUDED.generation,
        revision = EXCLUDED.revision,
        changed_at = EXCLUDED.changed_at;

    RETURN NULL;
END;
$$;

CREATE TRIGGER projection_invalidations_direct_revision
AFTER INSERT OR UPDATE OF generation ON public.projection_invalidations
FOR EACH ROW
EXECUTE FUNCTION public.record_projection_direct_invalidation_revision();

CREATE FUNCTION public.capture_projection_direct_invalidation_watermark()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    captured_revision BIGINT;
BEGIN
    -- The trigger's INSERT or UPDATE holds ROW EXCLUSIVE on the revision table
    -- before it allocates the sequence value. SHARE therefore waits out every
    -- prior generation writer and prevents a later allocation until MAX is
    -- captured.
    LOCK TABLE public.projection_direct_invalidation_revisions IN SHARE MODE;

    SELECT COALESCE(MAX(revision), 0)
    INTO captured_revision
    FROM public.projection_direct_invalidation_revisions;

    RETURN captured_revision;
END;
$$;

ALTER FUNCTION public.capture_projection_direct_invalidation_watermark()
SET lock_timeout = '100ms';

ALTER TABLE public.current_projection_staging_checkpoints
    ADD COLUMN validated_direct_invalidation_revision BIGINT DEFAULT 0 NOT NULL,
    ADD CONSTRAINT current_projection_staging_checkpoints_direct_invalidation_check CHECK (
        validated_direct_invalidation_revision >= 0
    );
