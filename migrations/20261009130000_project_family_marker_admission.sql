-- Existing schema-v2 databases gain the columns in which project_family_marker records the
-- ENSv2 root registry admission each publication was composed with, and the table comment
-- that states the Universal Resolver proxy family's role after TYR-282. The Universal Resolver
-- cutover is the admission of the chain's ENSv2 root registry (docs/glossary.md, "Universal
-- Resolver cutover"). Readers take it from the marker, so a manifest sync reaches them only
-- when the redo republishes. The proxy rows are kept for monitoring. The comment replaces the
-- statement 20260929200000 wrote, which said the composed name reader reads the rows.
-- Both columns are nullable and start null, which a reader of this build's marker reads as
-- not cut over. A marker written before this build carries another interpreter content hash,
-- so no reader serves it, and the redo this build requires rewrites every marker. An empty
-- schema-migration database has no phase baseline yet, so this schema-migration is a no-op
-- there and phase-runner init-schema installs the same columns and comments.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_family_marker') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
ALTER TABLE bigname_phase.project_family_marker
    ADD COLUMN IF NOT EXISTS root_registry text,
    ADD COLUMN IF NOT EXISTS since_block bigint
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.root_registry IS
    'This value is the lower-cased address of the ENSv2 root registry the active ens_v2_root_l1 manifest declared when the publication was composed, the Universal Resolver cutover; null when the chain was not cut over. Readers take the cutover from here, never from manifest_versions.'
$ddl$;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_family_marker.since_block IS
    'This value is that root registry''s declared start_block, which dates the admission; null when the chain was not cut over or the declaration has no start block.'
$ddl$;

IF to_regclass('bigname_phase.project_universal_resolver_proxy') IS NOT NULL THEN
EXECUTE $ddl$
COMMENT ON TABLE bigname_phase.project_universal_resolver_proxy IS
    'Project-owned Universal Resolver proxy state, kept for monitoring: per declared ens_execution proxy, the implementation its latest Upgraded event installed, in canonical event order. The phase runner reports whether the chain of implementations from the client-facing universal_resolver proxy, through declared proxies, ends at an implementation the manifest lists. A proxy with no row has no known implementation, since its constructor sets the first one without an event. No name read uses these rows: the Universal Resolver cutover is the admission of the chain''s ENSv2 root registry.'
$ddl$;
END IF;
END
$migration$;
