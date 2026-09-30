-- TYR-114: retain Project's requested invalidation separately from actual undo/rebuild/replay.
-- No row rewrite or publication reset. Legacy active requests adopt their existing bounds at
-- the next fenced redo begin. An empty phase schema remains empty for production bootstrap.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.chain_phase_state') IS NULL THEN RETURN; END IF;
ALTER TABLE bigname_phase.chain_phase_state
    ADD COLUMN IF NOT EXISTS redo_requested_from_block_number bigint,
    ADD COLUMN IF NOT EXISTS redo_requested_to_block_number bigint;
IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'bigname_phase.chain_phase_state'::regclass
      AND conname = 'chain_phase_state_project_redo_request_check'
) THEN
    ALTER TABLE bigname_phase.chain_phase_state ADD
CONSTRAINT chain_phase_state_project_redo_request_check CHECK (
        (redo_requested_from_block_number IS NULL AND redo_requested_to_block_number IS NULL)
        OR (
            phase_name = 'project' AND redo_in_progress
            AND redo_requested_from_block_number IS NOT NULL
            AND redo_requested_to_block_number IS NOT NULL
            AND redo_requested_from_block_number >= redo_from_block_number
            AND redo_requested_to_block_number <= redo_to_block_number
            AND redo_requested_to_block_number >= redo_requested_from_block_number
        )
    );
END IF;
COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_requested_from_block_number IS
    'For Project redo, the first requested invalidation block; execution may undo or rebuild below it. NULL on legacy active rows until the next begin.';
COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_requested_to_block_number IS
    'For Project redo, the last requested invalidation block; execution may replay above it to the standing publication. NULL on legacy active rows until the next begin.';
COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_from_block_number IS
    'This value is the first block in the active redo execution extent; Project retains requested invalidation separately.';
COMMENT ON COLUMN bigname_phase.chain_phase_state.redo_to_block_number IS
    'This value is the last block in the active redo execution extent; Project retains requested invalidation separately.';
END
$migration$;

-- Publication invalidation uses the request, not the earlier progress checkpoint.
-- Replacing the private core preserves its ACL and the fixed-mode wrapper identities.
DO $migration$
BEGIN
IF to_regprocedure('bigname_phase.revalidate_resolution_lookup_state(text,bigint,text,jsonb,jsonb,uuid,text,text,boolean)') IS NULL THEN RETURN; END IF;
EXECUTE $definition$
CREATE OR REPLACE FUNCTION bigname_phase.revalidate_resolution_lookup_state(
    requested_authoritative_chain_id text,
    requested_authoritative_block_number bigint,
    requested_authoritative_block_hash text,
    requested_observed_positions jsonb,
    compared_execution_authority jsonb,
    compared_resource_id uuid,
    compared_boundary_key text,
    compared_row_xmin text,
    lock_rows boolean
)
RETURNS text
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, bigname_phase, pg_temp
AS $$
DECLARE
    position_slot text;
    position_value jsonb;
    manifest_authority jsonb;
    compared_family_publication jsonb;
    compared_logical_name_id text;
    compared_name_row_xmin text;
    state_matches boolean;
BEGIN
    -- Keep this key aligned with SCHEMA_V2_MANIFEST_SYNC_LOCK in
    -- crates/manifests/src/schema_v2.rs. A shared transaction lock makes the
    -- captured active-or-shadow manifest selection stable through commit.
    IF lock_rows THEN
        PERFORM pg_advisory_xact_lock_shared(4776427281231725874);
    END IF;

    EXECUTE $guard$
        SELECT true FROM chain_heads
        WHERE chain_id = $1 AND latest_block_number = $2 AND latest_block_hash = $3
    $guard$ || CASE WHEN lock_rows THEN ' FOR SHARE' ELSE '' END
    INTO state_matches
    USING requested_authoritative_chain_id, requested_authoritative_block_number,
          requested_authoritative_block_hash;

    IF state_matches IS NOT TRUE THEN
        RETURN 'head_changed';
    END IF;

    IF jsonb_typeof(compared_execution_authority) IS DISTINCT FROM 'object'
    THEN
        RETURN 'invalid_comparison';
    END IF;

    compared_logical_name_id :=
        compared_execution_authority ->> 'logical_name_id';
    compared_name_row_xmin :=
        compared_execution_authority ->> 'name_row_xmin';

    -- A publication includes every composed name and inventory input. Writers also
    -- hold redo admission and publication rows until their ledger mutation commits.
    compared_family_publication := compared_execution_authority -> 'family_publication';
    EXECUTE $guard$
        SELECT count(*) > 0 FROM (
            SELECT 1 FROM chain_phase_state input_phase
            WHERE input_phase.chain_id = $1
              AND input_phase.phase_name IN ('interpret', 'project')
            ORDER BY input_phase.phase_name
    $guard$ || CASE WHEN lock_rows THEN ' FOR SHARE' ELSE '' END || ') AS phases'
    INTO state_matches USING requested_authoritative_chain_id;

    IF state_matches IS NOT TRUE THEN
        RETURN 'project_changed';
    END IF;

    -- Only lookup builds this object, with every field. Missing fields fail equality.
    EXECUTE $guard$
        SELECT true
        FROM project_family_marker marker
        JOIN chain_lineage lineage
          ON lineage.chain_id = marker.chain_id
         AND lineage.block_number = marker.current_block_number
         AND lineage.block_hash = marker.current_block_hash
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        WHERE marker.chain_id = $1
          AND marker.state = 'live'
          AND marker.sequence::text = $2 ->> 'sequence'
          AND marker.current_block_number::text = $2 ->> 'block_number'
          AND marker.current_block_hash = $2 ->> 'block_hash'
          AND marker.input_content_hash = $2 ->> 'input_content_hash'
          AND $3 - marker.current_block_number BETWEEN 0 AND 1
          AND (marker.current_block_number <> $3 OR marker.current_block_hash = $4)
          AND NOT EXISTS (
              SELECT 1 FROM chain_phase_state input_phase
              WHERE input_phase.chain_id = marker.chain_id
                AND input_phase.phase_name IN ('interpret', 'project')
                AND input_phase.redo_in_progress
                AND COALESCE(input_phase.redo_requested_from_block_number, input_phase.redo_from_block_number) <= marker.current_block_number
          )
    $guard$ || CASE WHEN lock_rows THEN ' FOR SHARE OF marker, lineage' ELSE '' END
    INTO state_matches
    USING requested_authoritative_chain_id, compared_family_publication,
          requested_authoritative_block_number, requested_authoritative_block_hash;

    IF state_matches IS NOT TRUE THEN
        RETURN 'project_changed';
    END IF;

    IF jsonb_typeof(requested_observed_positions) IS DISTINCT FROM 'object'
        OR requested_observed_positions = '{}'::jsonb
    THEN
        RETURN 'position_changed';
    END IF;

    FOR position_slot, position_value IN
        SELECT key, value
        FROM jsonb_each(requested_observed_positions)
        ORDER BY key
    LOOP
        BEGIN
            EXECUTE $guard$
                SELECT true FROM chain_lineage
                WHERE chain_id = $1 ->> 'chain_id'
                  AND block_hash = $1 ->> 'block_hash'
                  AND block_number = ($1 ->> 'block_number')::bigint
                  AND block_timestamp = ($1 ->> 'timestamp')::timestamptz
                  AND canonicality_state IN ('canonical', 'safe', 'finalized')
            $guard$ || CASE WHEN lock_rows THEN ' FOR SHARE' ELSE '' END
            INTO state_matches USING position_value;
        EXCEPTION
            WHEN data_exception THEN
                RETURN 'position_changed';
        END;

        IF state_matches IS NOT TRUE THEN
            RETURN 'position_changed';
        END IF;
    END LOOP;

        -- The composed name was read in the same snapshot as this publication.
        IF compared_logical_name_id IS NOT NULL AND (
            compared_execution_authority #>> '{family_name,logical_name_id}'
                IS DISTINCT FROM compared_logical_name_id
            OR compared_name_row_xmin IS DISTINCT FROM compared_family_publication ->> 'sequence'
        ) THEN
            RETURN 'name_changed';
        END IF;

    IF jsonb_typeof(
        compared_execution_authority -> 'manifest_authorities'
    ) IS DISTINCT FROM 'array'
        OR jsonb_array_length(
            compared_execution_authority -> 'manifest_authorities'
        ) = 0
    THEN
        RETURN 'invalid_comparison';
    END IF;

    FOR manifest_authority IN
        SELECT value
        FROM jsonb_array_elements(
            compared_execution_authority -> 'manifest_authorities'
        )
    LOOP
        PERFORM 1
        FROM manifest_versions AS manifest
        JOIN manifest_contract_instances AS declaration
          ON declaration.manifest_id = manifest.manifest_id
         AND declaration.chain_id = manifest.chain_id
        WHERE manifest.manifest_id::text =
                  manifest_authority ->> 'manifest_id'
          AND manifest.xmin::text =
                  manifest_authority ->> 'manifest_row_xmin'
          AND declaration.manifest_contract_instance_id::text =
                  manifest_authority ->> 'declaration_id'
          AND declaration.xmin::text =
                  manifest_authority ->> 'declaration_row_xmin'
          AND lower(declaration.declared_address) = lower(
                  manifest_authority ->> 'declared_address'
              );

        IF NOT FOUND THEN
            RETURN 'manifest_changed';
        END IF;
    END LOOP;

    IF compared_resource_id IS NULL
        AND compared_boundary_key IS NULL
        AND compared_row_xmin IS NULL
    THEN
        RETURN 'unchanged';
    END IF;

    IF compared_resource_id IS NULL
        OR compared_boundary_key IS NULL
        OR compared_row_xmin IS NULL
    THEN
        RETURN 'invalid_comparison';
    END IF;

        -- All record families are published atomically with the marker. The input payload is
        -- composed by lookup in that captured snapshot; neither the payload nor its results
        -- are persisted as reusable serving data.
        IF compared_execution_authority #>> '{family_comparison,resource_id}'
                IS DISTINCT FROM compared_resource_id::text
            OR compared_execution_authority #>> '{family_comparison,boundary_key}'
                IS DISTINCT FROM compared_boundary_key
            OR compared_execution_authority #>> '{family_comparison,publication_sequence}'
                IS DISTINCT FROM compared_row_xmin
            OR compared_row_xmin IS DISTINCT FROM compared_family_publication ->> 'sequence'
        THEN
            RETURN 'record_changed';
        END IF;
    RETURN 'unchanged';
END
$$;

$definition$;
END
$migration$;
