-- Add the historical-name lookup used by Interpret redo preparation.
-- Keep orphaned rows indexed: redo stages bindings as orphaned before it
-- restores closes, so the canonical-only name index cannot serve this query.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.surface_bindings') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS surface_bindings_chain_name_history_idx
        ON bigname_phase.surface_bindings (chain_id, logical_name_id);
END
$migration$;
