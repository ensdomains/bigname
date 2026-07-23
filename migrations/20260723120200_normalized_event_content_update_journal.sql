-- The replacement constraint was validated in a separate transaction, so this
-- ACCESS EXCLUSIVE metadata swap does not span the historical-row scan.
ALTER TABLE public.projection_normalized_event_changes
    DROP CONSTRAINT projection_normalized_event_changes_kind_check;

ALTER TABLE public.projection_normalized_event_changes
    RENAME CONSTRAINT projection_normalized_event_changes_kind_check_v2
    TO projection_normalized_event_changes_kind_check;

CREATE OR REPLACE FUNCTION public.record_projection_normalized_event_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO public.projection_normalized_event_changes (
            normalized_event_id,
            changed_at,
            change_kind,
            canonicality_state
        )
        VALUES (
            NEW.normalized_event_id,
            NEW.observed_at,
            'insert',
            NEW.canonicality_state
        );
        RETURN NEW;
    END IF;

    IF OLD.namespace IS DISTINCT FROM NEW.namespace
        OR OLD.logical_name_id IS DISTINCT FROM NEW.logical_name_id
        OR OLD.resource_id IS DISTINCT FROM NEW.resource_id
        OR OLD.event_kind IS DISTINCT FROM NEW.event_kind
        OR OLD.source_family IS DISTINCT FROM NEW.source_family
        OR OLD.manifest_version IS DISTINCT FROM NEW.manifest_version
        OR OLD.source_manifest_id IS DISTINCT FROM NEW.source_manifest_id
        OR OLD.chain_id IS DISTINCT FROM NEW.chain_id
        OR OLD.block_number IS DISTINCT FROM NEW.block_number
        OR OLD.block_hash IS DISTINCT FROM NEW.block_hash
        OR OLD.transaction_hash IS DISTINCT FROM NEW.transaction_hash
        OR OLD.log_index IS DISTINCT FROM NEW.log_index
        OR OLD.raw_fact_ref IS DISTINCT FROM NEW.raw_fact_ref
        OR OLD.derivation_kind IS DISTINCT FROM NEW.derivation_kind
        OR OLD.before_state IS DISTINCT FROM NEW.before_state
        OR OLD.after_state IS DISTINCT FROM NEW.after_state
    THEN
        INSERT INTO public.projection_normalized_event_changes (
            normalized_event_id,
            changed_at,
            change_kind,
            canonicality_state
        )
        VALUES (
            NEW.normalized_event_id,
            NEW.observed_at,
            'content_update',
            NEW.canonicality_state
        );
    END IF;

    IF OLD.canonicality_state IS DISTINCT FROM NEW.canonicality_state THEN
        INSERT INTO public.projection_normalized_event_changes (
            normalized_event_id,
            changed_at,
            change_kind,
            canonicality_state
        )
        VALUES (
            NEW.normalized_event_id,
            NEW.observed_at,
            'canonicality_update',
            NEW.canonicality_state
        );
    END IF;
    RETURN NEW;
END;
$$;

-- The installed trigger is intentionally keyed by canonicality_state so
-- existing repair statements that capture their own change_id in writable
-- CTEs are not double-journaled. Content-replacement writers that use this
-- trigger name canonicality_state in their UPDATE, even when its value is
-- unchanged, and the function above classifies the actual semantic diff.
