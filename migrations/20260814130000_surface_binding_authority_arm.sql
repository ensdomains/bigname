-- This upgrade lands before the production phase data is re-walked from block zero.
-- Historical bindings therefore are not backfilled or guessed from binding_kind or
-- provenance. A populated pre-upgrade binding table fails loudly; the reviewed
-- re-walk boundary must start with surface_bindings and its two referencing
-- current projections cleared while manifest and normalized-event identity stays in place.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.surface_bindings') IS NULL THEN
        RETURN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'bigname_phase'
          AND table_name = 'surface_bindings'
          AND column_name = 'authority_arm'
    ) THEN
        IF EXISTS (SELECT 1 FROM bigname_phase.surface_bindings) THEN
            RAISE EXCEPTION
                'surface binding authority-arm upgrade requires the reviewed offline binding reset before migration apply';
        END IF;
        ALTER TABLE bigname_phase.surface_bindings
            ADD COLUMN authority_arm text NOT NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'bigname_phase.surface_bindings'::regclass
          AND conname = 'surface_bindings_authority_arm_check'
    ) THEN
        ALTER TABLE bigname_phase.surface_bindings
            ADD CONSTRAINT surface_bindings_authority_arm_check
            CHECK (authority_arm IN ('ens_v1', 'ens_v2', 'basenames'));
    END IF;

    ALTER TABLE bigname_phase.surface_bindings
        DROP CONSTRAINT IF EXISTS surface_bindings_no_overlap;
    ALTER TABLE bigname_phase.surface_bindings
        ADD CONSTRAINT surface_bindings_no_overlap
        EXCLUDE USING gist (
            chain_id WITH =,
            logical_name_id WITH =,
            authority_arm WITH =,
            tstzrange(
                active_from,
                COALESCE(active_to, 'infinity'::timestamptz),
                '[)'
            ) WITH &&
        )
        WHERE (canonicality_state IN ('canonical', 'safe', 'finalized'));

    COMMENT ON COLUMN bigname_phase.surface_bindings.authority_arm IS
        'This value states which protocol authority arm owns this binding interval.';
END
$migration$;
