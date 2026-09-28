-- Existing schema-v2 databases gain the resolver index the composed bound-name
-- listing reads (TYR-36 step 7b, GET /v1/resolvers/{chain_id}/{address} under
-- the publication switch): the ENSv1 registry-node resolver pointers (F4) by
-- chain and lower-cased resolver, as project_resource_pointer_resolver_idx
-- already indexes the resource pointers (F5). Index only; no column or row
-- changes. An empty schema-migration database has no phase baseline yet, so
-- this migration is a no-op there and phase-runner init-schema installs the
-- same index.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS project_registry_pointer_resolver_idx
    ON bigname_phase.project_registry_pointer (chain_id, resolver_address)
$ddl$;
END
$migration$;
