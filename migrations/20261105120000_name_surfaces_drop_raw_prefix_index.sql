-- Existing schema-v2 databases lose name_surfaces_name_prefix_idx, a raw-name prefix index an
-- operator may have built by hand before the bucket indexes of
-- 20261029120000_search_prefix_bucket_indexes.sql. A prefix search reads those buckets, so no
-- statement reads this index, and every write to bigname_phase.name_surfaces maintained it.
-- Only that index changes: no table, column, row or comment does. The baseline does not define
-- it and no schema-migration builds it, so this schema-migration is a no-op on a database
-- initialized from the baseline, and on an empty schema-migration database.
--
-- The index is dropped only when it has the definition the hand-built index was given. A name
-- that is absent is skipped, so a rerun and a database where an operator dropped the index
-- first both pass. An invalid index of that definition, left by an interrupted concurrent
-- build or drop, is dropped like a valid one. Anything else under the name was built for
-- another purpose and is not this schema-migration's to remove. A relation that is not an
-- index, or an index with another definition, stops it before the drop. The error for another
-- definition prints both definitions. Either way the schema-migration fails without recording
-- itself and changes nothing.
--
-- The check compares definitions as pg_get_indexdef prints them, with search_path set to
-- pg_catalog so PostgreSQL always prints schema names, and quote_all_identifiers off so it
-- prints no extra quotes, the same way 20261029120000_search_prefix_bucket_indexes.sql does.
--
-- DROP INDEX takes an ACCESS EXCLUSIVE lock on name_surfaces, held until this schema-migration
-- commits, and waits behind transactions that hold the table. While it waits, new statements
-- on the table wait behind it. The lock timeout below bounds that wait to five seconds. On a
-- timeout the schema-migration fails unrecorded, and a rerun is safe. docs/deployment.md
-- describes dropping the index with DROP INDEX CONCURRENTLY first, for a database that cannot
-- take the lock. All three settings are transaction-local and put back before the block
-- returns.
DO $migration$
DECLARE
    prefix_index regclass := to_regclass('bigname_phase.name_surfaces_name_prefix_idx');
    hand_built_definition CONSTANT text :=
        $def$CREATE INDEX name_surfaces_name_prefix_idx ON bigname_phase.name_surfaces USING btree (raw_name text_pattern_ops, namespace, namehash, logical_name_id) WHERE ((visibility_state = 'active'::text) AND (canonicality_state = ANY (ARRAY['canonical'::bigname_phase.canonicality_state, 'safe'::bigname_phase.canonicality_state, 'finalized'::bigname_phase.canonicality_state])) AND (octet_length(raw_name) <= 2000))$def$;
    found_definition text;
    previous_search_path text := current_setting('search_path');
    previous_quote_all_identifiers text := current_setting('quote_all_identifiers');
    previous_lock_timeout text := current_setting('lock_timeout');
BEGIN
    IF prefix_index IS NULL THEN
        RETURN;
    END IF;

    PERFORM set_config('search_path', 'pg_catalog', true);
    PERFORM set_config('quote_all_identifiers', 'off', true);

    IF NOT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid = prefix_index) THEN
        RAISE EXCEPTION 'bigname_phase.name_surfaces_name_prefix_idx is not an index; it is not dropped';
    END IF;
    found_definition := pg_get_indexdef(prefix_index);
    IF found_definition IS DISTINCT FROM hand_built_definition THEN
        RAISE EXCEPTION
            'bigname_phase.name_surfaces_name_prefix_idx exists but does not have the hand-built definition, so it is not dropped; found "%", expected "%"; follow the recovery steps in docs/deployment.md, then run the schema-migrations again',
            found_definition, hand_built_definition;
    END IF;

    PERFORM set_config('lock_timeout', '5s', true);
    DROP INDEX bigname_phase.name_surfaces_name_prefix_idx;

    PERFORM set_config('search_path', previous_search_path, true);
    PERFORM set_config('quote_all_identifiers', previous_quote_all_identifiers, true);
    PERFORM set_config('lock_timeout', previous_lock_timeout, true);
END
$migration$;
