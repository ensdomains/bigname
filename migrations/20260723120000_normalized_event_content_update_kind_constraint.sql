-- Add the strict-superset constraint without scanning the append-only journal.
-- Keeping validation in the next migration releases this statement's table
-- lock before PostgreSQL checks historical rows.
ALTER TABLE public.projection_normalized_event_changes
    ADD CONSTRAINT projection_normalized_event_changes_kind_check_v2 CHECK (
        change_kind IN ('insert', 'content_update', 'canonicality_update')
    ) NOT VALID;
