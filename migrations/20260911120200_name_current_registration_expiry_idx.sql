-- Pure index for the namespace-wide expiry window listing
-- (`GET /v1/names?namespace=&expires_after=&expires_before=&sort=expires_at`).
-- Fresh installs get the same index from schema-v2/baseline/06_projections.sql;
-- this migration adds it to a phase schema installed before the baseline carried it.
-- The projection writes `registration.expiry` as a JSON number of unix seconds; the
-- partial predicate keeps the text-to-float cast off every other shape, so the index
-- expression is immutable and the build cannot fail on a non-numeric string.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.name_current') IS NULL THEN
        RETURN;
    END IF;

    CREATE INDEX IF NOT EXISTS name_current_registration_expiry_idx
        ON bigname_phase.name_current (
            namespace,
            ((declared_summary #>> '{registration,expiry}')::double precision),
            logical_name_id
        )
        WHERE jsonb_typeof(declared_summary #> '{registration,expiry}') = 'number';
END
$migration$;
