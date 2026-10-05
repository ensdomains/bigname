-- Existing schema-v2 databases let a name identity exist before the raw bytes of all its
-- labels are known. raw_name, raw_labels and dns_encoded_name become optional together; a row
-- without them still names its node through the complete label-hash path and is always
-- visible, because unknown bytes are not a normalization failure. The new
-- preimage_event_identity column names the earliest canonical preimage observation of the
-- name, which witnesses its raw labels apart from the identity's own first-observation anchor.
-- Every existing row keeps its raw labels; this schema-migration backfills the column from
-- each row's earliest canonical PreimageObserved event and leaves it NULL where none
-- survives. Adding the check validates every row while it holds the table lock. An empty
-- schema-migration database has no phase baseline yet, so this schema-migration is a no-op
-- there and phase-runner init-schema installs the same shape.
DO $migration$
DECLARE
    retired_check name;
BEGIN
IF to_regclass('bigname_phase.name_surfaces') IS NULL THEN
    RETURN;
END IF;

ALTER TABLE bigname_phase.name_surfaces
    ALTER COLUMN raw_name DROP NOT NULL,
    ALTER COLUMN raw_labels DROP NOT NULL,
    ALTER COLUMN dns_encoded_name DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS preimage_event_identity text;

SELECT constraint_row.conname INTO retired_check
FROM pg_constraint constraint_row
WHERE constraint_row.conrelid = 'bigname_phase.name_surfaces'::regclass
  AND constraint_row.contype = 'c'
  AND pg_get_constraintdef(constraint_row.oid)
      = 'CHECK ((cardinality(raw_labels) = cardinality(labelhashes)))';
IF retired_check IS NOT NULL THEN
    EXECUTE format(
        'ALTER TABLE bigname_phase.name_surfaces DROP CONSTRAINT %I',
        retired_check
    );
END IF;

UPDATE bigname_phase.name_surfaces surface
SET preimage_event_identity = witness.event_identity
FROM (
    SELECT DISTINCT ON (event.chain_id, event.logical_name_id)
           event.chain_id,
           event.logical_name_id,
           event.event_identity
    FROM bigname_phase.normalized_events event
    JOIN bigname_phase.chain_lineage lineage
      ON lineage.chain_id = event.chain_id
     AND lineage.block_hash = event.block_hash
     AND lineage.block_number = event.block_number
    WHERE event.event_kind = 'PreimageObserved'
      AND event.logical_name_id IS NOT NULL
      AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    ORDER BY event.chain_id,
             event.logical_name_id,
             event.block_number,
             event.transaction_index NULLS FIRST,
             event.log_index NULLS FIRST,
             event.normalized_event_id
) witness
WHERE surface.chain_id = witness.chain_id
  AND surface.logical_name_id = witness.logical_name_id
  AND surface.preimage_event_identity IS NULL
  AND surface.raw_name IS NOT NULL;

IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'bigname_phase.name_surfaces'::regclass
      AND conname = 'name_surfaces_raw_evidence_check'
) THEN
    ALTER TABLE bigname_phase.name_surfaces
        ADD CONSTRAINT name_surfaces_raw_evidence_check CHECK (
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
                AND cardinality(raw_labels) = cardinality(labelhashes)
                AND (preimage_event_identity IS NULL OR btrim(preimage_event_identity) <> '')
            )
        );
END IF;

COMMENT ON TABLE bigname_phase.name_surfaces IS
    'This table stores name identities, their raw names when known, and their visibility state.';
COMMENT ON COLUMN bigname_phase.name_surfaces.raw_name IS
    'This value is the verbatim name, or NULL while the bytes of a label are unknown.';
COMMENT ON COLUMN bigname_phase.name_surfaces.raw_labels IS
    'This array stores the verbatim labels, or NULL while the bytes of a label are unknown.';
COMMENT ON COLUMN bigname_phase.name_surfaces.dns_encoded_name IS
    'This value is the DNS wire name, or NULL while the bytes of a label are unknown.';
COMMENT ON COLUMN bigname_phase.name_surfaces.preimage_event_identity IS
    'This value is the event identity of the earliest canonical preimage observation of the name.';
END
$migration$;
