DO $validation$
DECLARE
    missing text;
    stale text;
    checked_index text;
    expected_definition text;
    found_definition text;
    found_body text;
    previous_search_path text := current_setting('search_path');
    previous_quote_all_identifiers text := current_setting('quote_all_identifiers');
BEGIN
    SELECT string_agg(wanted.name, ', ' ORDER BY wanted.name) INTO missing
    FROM unnest(ARRAY['normalized_events_project_node_history_idx','normalized_events_project_v1_pointer_node_idx','name_surfaces_project_label_hashes_idx','name_surfaces_project_suffix_hash_idx','name_surfaces_project_node_idx']) wanted(name)
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

    -- The earlier label-array indexes have no entry size bound: a long enough label
    -- would fail its name_surfaces insert. install.sql drops them.
    SELECT string_agg(old.name, ', ' ORDER BY old.name) INTO stale
    FROM unnest(ARRAY['name_surfaces_project_labels_idx','name_surfaces_project_suffix_idx']) old(name)
    WHERE to_regclass('bigname_phase.' || old.name) IS NOT NULL;
    IF stale IS NOT NULL THEN
        RAISE EXCEPTION 'Project progressive label-array indexes still exist: %; rerun install.sql', stale;
    END IF;

    SELECT btrim(regexp_replace(prosrc, '\s+', ' ', 'g')) INTO found_body
    FROM pg_proc
    WHERE oid = to_regprocedure('bigname_phase.label_hashes(text[])')
      AND provolatile = 'i' AND proisstrict AND proparallel = 's'
      AND prorettype = 'bigint[]'::regtype;
    IF found_body IS DISTINCT FROM
        'SELECT ARRAY(SELECT pg_catalog.hashtextextended(label, 0) FROM pg_catalog.unnest(labels) AS label)'
    THEN
        RAISE EXCEPTION 'bigname_phase.label_hashes(text[]) is missing or has another definition';
    END IF;

    -- Compare the printed definitions exactly, as the v1 lookahead schema-migration does:
    -- with search_path = pg_catalog PostgreSQL always prints schema names, and with
    -- quote_all_identifiers off it prints no extra quotes. Both settings are
    -- transaction-local and put back before the block returns.
    PERFORM set_config('search_path', 'pg_catalog', true);
    PERFORM set_config('quote_all_identifiers', 'off', true);
    FOR checked_index, expected_definition IN
        SELECT * FROM (VALUES
            ('name_surfaces_project_suffix_hash_idx',
             'CREATE INDEX name_surfaces_project_suffix_hash_idx ON bigname_phase.name_surfaces USING btree (namespace, hash_array_extended(raw_labels, (0)::bigint))'),
            ('name_surfaces_project_label_hashes_idx',
             'CREATE INDEX name_surfaces_project_label_hashes_idx ON bigname_phase.name_surfaces USING gin (bigname_phase.label_hashes(raw_labels))')
        ) AS reviewed(index_name, definition)
    LOOP
        found_definition := pg_get_indexdef(to_regclass('bigname_phase.' || checked_index));
        IF found_definition IS DISTINCT FROM expected_definition THEN
            RAISE EXCEPTION 'Project progressive index bigname_phase.% has another definition: %',
                checked_index, found_definition;
        END IF;
    END LOOP;
    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
END;
$validation$;
