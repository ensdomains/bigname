-- An initialized schema-v2 namespace predating #603/#604 has no independent
-- resolver/record serving reference on name_current. The nullable reference is
-- additive; replay from zero populates it where event-linked evidence exists.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.name_current') IS NULL THEN
        RETURN;
    END IF;

    ALTER TABLE bigname_phase.name_current
        ADD COLUMN IF NOT EXISTS serving_resource_id uuid
            REFERENCES bigname_phase.resources (resource_id);

    CREATE INDEX IF NOT EXISTS name_current_serving_resource_idx
        ON bigname_phase.name_current (serving_resource_id)
        WHERE serving_resource_id IS NOT NULL;

    COMMENT ON COLUMN bigname_phase.name_current.serving_resource_id IS
        'This event-derived resource is used for resolver and record serving. It does not establish a current authority, registration, or surface binding.';
END
$migration$;
