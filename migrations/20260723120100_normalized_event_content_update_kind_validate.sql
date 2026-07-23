-- VALIDATE CONSTRAINT permits ordinary row-level journal writers while it
-- scans existing rows. The prior constraint remains authoritative throughout.
ALTER TABLE public.projection_normalized_event_changes
    VALIDATE CONSTRAINT projection_normalized_event_changes_kind_check_v2;
