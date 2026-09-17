-- Every database ends with one validated self-edge CHECK on discovery_edges,
-- named discovery_edges_self_edge_check, with the fresh baseline's text.
-- 20260917140000 already leaves that shape where it found the table. A
-- database whose baseline was installed after that file ran as a no-op can
-- still hold the same rule under the generated name discovery_edges_check.
DO $migration$
DECLARE
    wanted_definition text;
    self_edge_count integer;
    matching_name text;
    matching_validated boolean;
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

    SELECT count(*) INTO self_edge_count
    FROM pg_constraint
    WHERE conrelid = 'bigname_phase.discovery_edges'::regclass AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%from_contract_instance_id <> to_contract_instance_id%';

    IF self_edge_count = 1 THEN
        SELECT conname, convalidated INTO matching_name, matching_validated
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.discovery_edges'::regclass AND contype = 'c'
          AND pg_get_constraintdef(oid) = wanted_definition;
    END IF;

    -- The one self-edge rule already has the wanted text. Only its name, or a
    -- pending validation, can differ; neither needs the rule to be replaced.
    IF matching_name IS NOT NULL THEN
        IF matching_name <> 'discovery_edges_self_edge_check' THEN
            EXECUTE format(
                'ALTER TABLE bigname_phase.discovery_edges RENAME CONSTRAINT %I TO discovery_edges_self_edge_check',
                matching_name);
        END IF;
        IF NOT matching_validated THEN
            ALTER TABLE bigname_phase.discovery_edges
                VALIDATE CONSTRAINT discovery_edges_self_edge_check;
        END IF;
        RETURN;
    END IF;

    -- The text differs, or there is more than one self-edge rule: replace.
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
