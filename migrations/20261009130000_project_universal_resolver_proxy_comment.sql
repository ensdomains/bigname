-- Existing schema-v2 databases gain the table comment that states the Universal Resolver proxy
-- family's role after TYR-282: the rows are kept for monitoring, and the Universal Resolver
-- cutover is the admission of the chain's ENSv2 root registry (docs/glossary.md, "Universal
-- Resolver cutover"). It replaces the statement 20260929200000 wrote, which said the composed
-- name reader reads the rows.
-- Comment only: no column, index or row changes. An empty schema-migration database has no
-- phase baseline yet, so this schema-migration is a no-op there and phase-runner init-schema
-- installs the same comment.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_universal_resolver_proxy') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_universal_resolver_proxy IS
    'Project-owned Universal Resolver proxy state, kept for monitoring: per declared ens_execution proxy, the implementation its latest Upgraded event installed, in canonical event order. The phase runner reports whether the chain of implementations from the client-facing universal_resolver proxy, through declared proxies, ends at an implementation the manifest lists. A proxy with no row has no known implementation, since its constructor sets the first one without an event. No name read uses these rows: the Universal Resolver cutover is the admission of the chain''s ENSv2 root registry.'
$ddl$;
END
$migration$;
