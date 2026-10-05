-- Preserve complete raw-byte hash paths for shadows without a PostgreSQL text decoding.
-- Existing rows are repaired together with their byte evidence by full Interpret re-derivation.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.name_surfaces') IS NULL THEN
    RETURN;
END IF;
ALTER TABLE bigname_phase.name_surfaces DROP CONSTRAINT name_surfaces_raw_evidence_check;
ALTER TABLE bigname_phase.name_surfaces ADD CONSTRAINT name_surfaces_raw_evidence_check CHECK (
        (
            raw_name IS NULL
            AND raw_labels IS NULL
            AND dns_encoded_name IS NULL
            AND preimage_event_identity IS NULL
            AND cardinality(labelhashes) > 0
            AND visibility_state = 'active'
        )
        OR (
            raw_name IS NOT NULL
            AND raw_labels IS NOT NULL
            AND dns_encoded_name IS NOT NULL
            AND (
                cardinality(raw_labels) = cardinality(labelhashes)
                OR (
                    visibility_state = 'shadow'
                    AND raw_name = ''
                    AND cardinality(raw_labels) = 0
                    AND cardinality(labelhashes) > 0
                    AND preimage_event_identity IS NOT NULL
                    AND btrim(preimage_event_identity) <> ''
                )
            )
            AND (preimage_event_identity IS NULL OR btrim(preimage_event_identity) <> '')
        )
    );
END
$migration$;
