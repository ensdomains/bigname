-- ResolverCreated is an independent announcement by the resolver itself.
DO $migration$
DECLARE
    wanted_definition text;
    already_current boolean;
    constraint_name text;
BEGIN
    IF to_regclass('bigname_phase.discovery_edges') IS NULL THEN RETURN; END IF;

    -- Ask PostgreSQL how it prints the wanted rule, so the comparison below
    -- does not depend on one server version's formatting.
    DROP TABLE IF EXISTS pg_temp.discovery_edges_self_edge_probe;
    CREATE TEMP TABLE discovery_edges_self_edge_probe
        (LIKE bigname_phase.discovery_edges);
    ALTER TABLE pg_temp.discovery_edges_self_edge_probe
        ADD CONSTRAINT discovery_edges_self_edge_check
        CHECK (edge_kind = 'registry_announcement'
            OR (edge_kind = 'resolver' AND discovery_source = 'ResolverCreated')
            OR from_contract_instance_id <> to_contract_instance_id);
    SELECT pg_get_constraintdef(oid) INTO STRICT wanted_definition
    FROM pg_constraint
    WHERE conrelid = 'pg_temp.discovery_edges_self_edge_probe'::regclass
      AND conname = 'discovery_edges_self_edge_check';
    DROP TABLE pg_temp.discovery_edges_self_edge_probe;

    -- A fresh install, or an earlier run, already has exactly one self-edge
    -- rule with the wanted name and text. Leave it alone: dropping and
    -- re-adding it would lock the table and recheck every row.
    SELECT count(*) = 1
           AND bool_and(conname = 'discovery_edges_self_edge_check'
               AND convalidated
               AND pg_get_constraintdef(oid) = wanted_definition)
    INTO already_current
    FROM pg_constraint
    WHERE conrelid = 'bigname_phase.discovery_edges'::regclass AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%from_contract_instance_id <> to_contract_instance_id%';
    IF already_current THEN RETURN; END IF;

    FOR constraint_name IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'bigname_phase.discovery_edges'::regclass AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%from_contract_instance_id <> to_contract_instance_id%'
    LOOP
        EXECUTE format('ALTER TABLE bigname_phase.discovery_edges DROP CONSTRAINT %I', constraint_name);
    END LOOP;
    ALTER TABLE bigname_phase.discovery_edges ADD CONSTRAINT discovery_edges_self_edge_check
        CHECK (edge_kind = 'registry_announcement'
            OR (edge_kind = 'resolver' AND discovery_source = 'ResolverCreated')
            OR from_contract_instance_id <> to_contract_instance_id);
END
$migration$;
