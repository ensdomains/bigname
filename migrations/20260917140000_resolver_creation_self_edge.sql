-- ResolverCreated is an independent announcement by the resolver itself.
DO $migration$
DECLARE constraint_name text;
BEGIN
    IF to_regclass('bigname_phase.discovery_edges') IS NULL THEN RETURN; END IF;
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
