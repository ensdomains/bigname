-- Existing schema-v2 databases gain the two partial indexes an Interpret redo reads to find
-- the surfaces whose raw labels lost their preimage witness
-- (interpret:identity_search.redo_names and interpret:reanchor.repair_preimage_witnesses).
-- Before this change both statements read every surface of the chain on each redo, a
-- one-block redo at the head included. They now read the released witnesses from
-- name_surfaces_released_witness_idx, the byte-oriented shadows from
-- name_surfaces_byte_shadow_idx, and the witnesses on orphaned blocks from the orphaned
-- lineage rows.
--
-- Both keys are the key of the table's unique (chain_id, logical_name_id) constraint, so every
-- row that fits that index fits these. Indexes only. No column or row changes.
--
-- Each build holds a SHARE lock on name_surfaces until this schema-migration commits, which
-- blocks Interpret's writes to it. On a large database, prebuild both concurrently first, as
-- docs/deployment.md describes, so the statements below find them by name and skip the
-- builds. An empty schema-migration database has no phase baseline yet, so this
-- schema-migration is a no-op there and phase-runner init-schema installs the same indexes.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_surfaces') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS name_surfaces_released_witness_idx
    ON bigname_phase.name_surfaces (chain_id, logical_name_id)
    WHERE raw_name IS NOT NULL AND preimage_event_identity IS NULL
$ddl$;
EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS name_surfaces_byte_shadow_idx
    ON bigname_phase.name_surfaces (chain_id, logical_name_id)
    WHERE raw_name = '' AND visibility_state = 'shadow'
$ddl$;
END
$migration$;
