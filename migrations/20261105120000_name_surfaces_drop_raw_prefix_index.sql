-- Red-run stub: names the index and does nothing.
DO $migration$
BEGIN
    PERFORM to_regclass('bigname_phase.name_surfaces_name_prefix_idx');
END
$migration$;
