-- Existing schema-v2 databases gain the owned key family comment (TYR-36
-- step 2) that states why a binding candidate pairs with its SurfaceBound by
-- position. Comments only; no column, index or row changes. An empty
-- schema-migration database has no phase baseline yet, so this migration is a
-- no-op there and phase-runner init-schema installs the same comment.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_current') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.project_binding_candidate.event_identity IS
    'This value is the event identity of the binding''s position: the position of the SurfaceBound that opened it (the block''s SurfaceBound of the same name and resource at the transaction and log index of the binding''s provenance), else the binding''s own block and provenance index with the identity binding:<surface_binding_id>. It is the final tiebreak of the canonical event order, compared as bytes; two bindings one event opened are ordered by surface_binding_id. The pairing is exact: the adapter writes a log-sourced binding and its SurfaceBound from one raw log with that log''s provenance (adapters schema_v2/identity.rs), and a block-boundary binding and its SurfaceBound from one block with no transaction or log. A binding:<surface_binding_id> identity therefore means the adapter''s reconcile dropped the SurfaceBound, never that the SurfaceBound sits at another position.'
$ddl$;
END
$migration$;
