-- Existing schema-v2 databases gain two indexes that the name summary
-- composition reads by name id. Project composes the summaries of the names
-- each follow block touches, and each composition looks rows up by
-- logical_name_id:
--   * storage:families.control.lifecycle.key_states reads the lifecycle key
--     states (project_lifecycle_key_state) of the names or of their
--     resources. The primary key serves the resource arm; the name arm had no
--     index, so the OR read every key state of the chain.
--     project_lifecycle_key_state_name_idx serves the name arm, and the lookup
--     becomes a BitmapOr of two index probes.
--   * storage:families.control.lifecycle.authority_starts and
--     storage:families.name.migrations read project_name_state by chain and
--     name. Its primary key leads with the namespace, which neither binds, so
--     each read every name state row of the chain.
--     project_name_state_name_idx serves both.
-- Index only; no column or row changes. An empty schema-migration database
-- has no phase baseline yet, so each index is skipped when its table is
-- missing, and phase-runner init-schema installs the same indexes. Each guard
-- names the indexed table, not name_current, which
-- 20260929160000_remove_served_projections.sql drops.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.project_lifecycle_key_state') IS NOT NULL THEN
    EXECUTE $ddl$
    CREATE INDEX IF NOT EXISTS project_lifecycle_key_state_name_idx
        ON bigname_phase.project_lifecycle_key_state (chain_id, logical_name_id)
        WHERE logical_name_id IS NOT NULL
    $ddl$;
END IF;

IF to_regclass('bigname_phase.project_name_state') IS NOT NULL THEN
    EXECUTE $ddl$
    CREATE INDEX IF NOT EXISTS project_name_state_name_idx
        ON bigname_phase.project_name_state (chain_id, logical_name_id)
    $ddl$;
END IF;
END
$migration$;
