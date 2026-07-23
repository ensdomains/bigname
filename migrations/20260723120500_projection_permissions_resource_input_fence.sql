-- Preserve a monotonic, resource-keyed fence for every resources column used
-- by permissions_current staging. This includes zero-event resources, which
-- cannot be recovered from the normalized-event change journal.
CREATE SEQUENCE public.projection_permissions_resource_input_revision_seq AS BIGINT;

CREATE TABLE public.projection_permissions_resource_input_revisions (
    resource_id UUID PRIMARY KEY,
    revision BIGINT NOT NULL,
    changed_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    CONSTRAINT projection_permissions_resource_input_revisions_revision_check CHECK (
        revision > 0
    )
);

CREATE INDEX projection_permissions_resource_input_revisions_revision_idx
    ON public.projection_permissions_resource_input_revisions (revision);

CREATE FUNCTION public.record_projection_permissions_resource_input_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO public.projection_permissions_resource_input_revisions (
            resource_id,
            revision,
            changed_at
        )
        VALUES (
            OLD.resource_id,
            nextval('public.projection_permissions_resource_input_revision_seq'),
            clock_timestamp()
        )
        ON CONFLICT (resource_id)
        DO UPDATE SET
            revision = EXCLUDED.revision,
            changed_at = EXCLUDED.changed_at;

        RETURN NULL;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.resource_id IS NOT DISTINCT FROM OLD.resource_id
           AND NEW.chain_id IS NOT DISTINCT FROM OLD.chain_id
           AND NEW.block_hash IS NOT DISTINCT FROM OLD.block_hash
           AND NEW.block_number IS NOT DISTINCT FROM OLD.block_number
           AND NEW.provenance IS NOT DISTINCT FROM OLD.provenance
           AND NEW.canonicality_state IS NOT DISTINCT FROM OLD.canonicality_state
        THEN
            RETURN NULL;
        END IF;

        IF OLD.resource_id IS DISTINCT FROM NEW.resource_id THEN
            INSERT INTO public.projection_permissions_resource_input_revisions (
                resource_id,
                revision,
                changed_at
            )
            VALUES (
                OLD.resource_id,
                nextval('public.projection_permissions_resource_input_revision_seq'),
                clock_timestamp()
            )
            ON CONFLICT (resource_id)
            DO UPDATE SET
                revision = EXCLUDED.revision,
                changed_at = EXCLUDED.changed_at;
        END IF;
    END IF;

    INSERT INTO public.projection_permissions_resource_input_revisions (
        resource_id,
        revision,
        changed_at
    )
    VALUES (
        NEW.resource_id,
        nextval('public.projection_permissions_resource_input_revision_seq'),
        clock_timestamp()
    )
    ON CONFLICT (resource_id)
    DO UPDATE SET
        revision = EXCLUDED.revision,
        changed_at = EXCLUDED.changed_at;

    RETURN NULL;
END;
$$;

CREATE TRIGGER resources_permissions_projection_input_revision
AFTER INSERT OR DELETE OR UPDATE OF
    resource_id,
    chain_id,
    block_hash,
    block_number,
    provenance,
    canonicality_state
ON public.resources
FOR EACH ROW
EXECUTE FUNCTION public.record_projection_permissions_resource_input_revision();

CREATE FUNCTION public.capture_projection_permissions_resource_input_watermark()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    captured_revision BIGINT;
BEGIN
    -- Resource writers take ROW EXCLUSIVE here before they publish any
    -- resource-backed normalized event. SHARE both waits out prior writers and
    -- prevents a later revision allocation until the staging fence commits.
    LOCK TABLE public.projection_permissions_resource_input_revisions IN SHARE MODE;

    SELECT COALESCE(MAX(revision), 0)
    INTO captured_revision
    FROM public.projection_permissions_resource_input_revisions;

    RETURN captured_revision;
END;
$$;

ALTER FUNCTION public.capture_projection_permissions_resource_input_watermark()
SET lock_timeout = '100ms';

ALTER TABLE public.current_projection_staging_checkpoints
    ADD COLUMN validated_permissions_resource_revision BIGINT DEFAULT 0 NOT NULL,
    ADD CONSTRAINT current_projection_staging_checkpoints_permissions_resource_check CHECK (
        validated_permissions_resource_revision >= 0
    );
