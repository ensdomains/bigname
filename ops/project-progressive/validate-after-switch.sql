-- Run after the new binary has applied its schema-migrations and started, together
-- with validate.sql. The earlier label-array indexes have no entry size bound: a long
-- enough label fails the name_surfaces insert. The schema-migration
-- 20260923140000_project_name_surfaces_label_indexes.sql drops them, so they must be
-- gone once it has run.
DO $validation$
DECLARE
    stale text;
BEGIN
    SELECT string_agg(old.name, ', ' ORDER BY old.name) INTO stale
    FROM unnest(ARRAY['name_surfaces_project_labels_idx','name_surfaces_project_suffix_idx']) old(name)
    WHERE to_regclass('bigname_phase.' || old.name) IS NOT NULL;
    IF stale IS NOT NULL THEN
        RAISE EXCEPTION 'Project progressive label-array indexes still exist after the switch: %; check that 20260923140000_project_name_surfaces_label_indexes.sql was applied', stale;
    END IF;
END;
$validation$;
