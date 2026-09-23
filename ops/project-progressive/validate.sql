DO $validation$
DECLARE
    missing text;
    found_definition text;
    previous_search_path text := current_setting('search_path');
    previous_quote_all_identifiers text := current_setting('quote_all_identifiers');
BEGIN
    SELECT string_agg(wanted.name, ', ' ORDER BY wanted.name) INTO missing
    FROM unnest(ARRAY['normalized_events_project_node_history_idx','normalized_events_project_v1_pointer_node_idx','name_surfaces_project_labels_idx','name_surfaces_project_suffix_hash_idx','name_surfaces_project_node_idx']) wanted(name)
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

    -- The earlier (namespace, raw_labels) array index has no entry size bound: a long
    -- enough name would fail its name_surfaces insert. install.sql drops it.
    IF to_regclass('bigname_phase.name_surfaces_project_suffix_idx') IS NOT NULL THEN
        RAISE EXCEPTION 'Project progressive array index bigname_phase.name_surfaces_project_suffix_idx still exists; rerun install.sql';
    END IF;

    -- Compare the printed definition exactly, as the v1 lookahead schema-migration does:
    -- with search_path = pg_catalog PostgreSQL always prints the schema name, and with
    -- quote_all_identifiers off it prints no extra quotes. Both settings are
    -- transaction-local and put back before the block returns.
    PERFORM set_config('search_path', 'pg_catalog', true);
    PERFORM set_config('quote_all_identifiers', 'off', true);
    found_definition := pg_get_indexdef('bigname_phase.name_surfaces_project_suffix_hash_idx'::regclass);
    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
    IF found_definition IS DISTINCT FROM
        'CREATE INDEX name_surfaces_project_suffix_hash_idx ON bigname_phase.name_surfaces USING btree (namespace, hash_array_extended(raw_labels, (0)::bigint))'
    THEN
        RAISE EXCEPTION 'Project progressive index bigname_phase.name_surfaces_project_suffix_hash_idx has another definition: %', found_definition;
    END IF;
END;
$validation$;
