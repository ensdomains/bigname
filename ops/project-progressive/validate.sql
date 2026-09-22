DO $validation$
DECLARE missing text;
BEGIN
    SELECT string_agg(wanted.name, ', ' ORDER BY wanted.name) INTO missing
    FROM unnest(ARRAY['normalized_events_project_node_history_idx','normalized_events_project_v1_pointer_node_idx','name_surfaces_project_labels_idx','name_surfaces_project_suffix_idx','name_surfaces_project_node_idx']) wanted(name)
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_index idx
        JOIN pg_class index_relation ON index_relation.oid = idx.indexrelid
        JOIN pg_namespace ns ON ns.oid = index_relation.relnamespace
        WHERE ns.nspname = 'bigname_phase' AND index_relation.relname = wanted.name
          AND idx.indisvalid AND idx.indisready
    );
    IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'Project progressive indexes missing or invalid: %', missing;
    END IF;
END;
$validation$;
