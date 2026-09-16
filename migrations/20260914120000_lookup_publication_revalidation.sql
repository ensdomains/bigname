-- Preserve existing data and EXECUTE grants while updating the serving guard.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.resolution_divergences') IS NULL THEN
        RETURN;
    END IF;
    EXECUTE $definition$
CREATE OR REPLACE FUNCTION bigname_phase.revalidate_resolution_lookup_state(
    requested_authoritative_chain_id text,
    requested_authoritative_block_number bigint,
    requested_authoritative_block_hash text,
    requested_observed_positions jsonb,
    compared_execution_authority jsonb,
    compared_resource_id uuid,
    compared_boundary_key text,
    compared_row_xmin text
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, bigname_phase, pg_temp
AS $$
DECLARE
    position_slot text;
    position_value jsonb;
    manifest_authority jsonb;
    compared_project_row_xmin text;
    compared_publication jsonb;
    compared_logical_name_id text;
    compared_name_row_xmin text;
BEGIN
    -- Keep this key aligned with SCHEMA_V2_MANIFEST_SYNC_LOCK in
    -- crates/manifests/src/schema_v2.rs. A shared transaction lock makes the
    -- captured active-or-shadow manifest selection stable through commit.
    PERFORM pg_advisory_xact_lock_shared(4776427281231725874);

    PERFORM 1
    FROM chain_heads
    WHERE chain_id = requested_authoritative_chain_id
      AND latest_block_number = requested_authoritative_block_number
      AND latest_block_hash = requested_authoritative_block_hash
    FOR SHARE;

    IF NOT FOUND THEN
        RETURN 'head_changed';
    END IF;

    IF jsonb_typeof(compared_execution_authority) IS DISTINCT FROM 'object'
    THEN
        RETURN 'invalid_comparison';
    END IF;

    compared_publication := compared_execution_authority -> 'project_publication';
    IF compared_publication IS NOT NULL AND (
        jsonb_typeof(compared_publication) IS DISTINCT FROM 'object'
        OR compared_publication ->> 'block_number' IS NULL
        OR compared_publication ->> 'block_hash' IS NULL
        OR compared_publication ->> 'input_content_hash' IS NULL
        OR compared_publication ->> 'row_xmin' IS NULL
    ) THEN
        RETURN 'invalid_comparison';
    END IF;

    compared_project_row_xmin :=
        compared_execution_authority ->> 'project_row_xmin';
    compared_logical_name_id :=
        compared_execution_authority ->> 'logical_name_id';
    compared_name_row_xmin :=
        compared_execution_authority ->> 'name_row_xmin';

    IF compared_project_row_xmin IS NULL
        OR btrim(compared_project_row_xmin) = ''
    THEN
        RETURN 'invalid_comparison';
    END IF;

    -- Lock the captured publication, including its generation, while a running
    -- pass may be preparing its successor. Older callers without a publication
    -- object retain the exact-head fence. Keep the one-block bound aligned with
    -- PROJECT_PUBLICATION_LAG_TOLERANCE_BLOCKS in crates/storage.
    PERFORM 1
    FROM chain_phase_state project
    JOIN chain_lineage lineage
      ON lineage.chain_id = project.chain_id
     AND lineage.block_number = project.current_block_number
     AND lineage.block_hash = project.current_block_hash
     AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
    WHERE project.chain_id = requested_authoritative_chain_id
      AND project.phase_name = 'project'
      AND project.phase_status IN ('completed', 'running')
      AND project.current_block_number::text = COALESCE(
          compared_publication ->> 'block_number',
          requested_authoritative_block_number::text
      )
      AND project.current_block_hash = COALESCE(
          compared_publication ->> 'block_hash',
          requested_authoritative_block_hash
      )
      AND requested_authoritative_block_number - project.current_block_number BETWEEN 0 AND 1
      AND (project.current_block_number <> requested_authoritative_block_number
           OR project.current_block_hash = requested_authoritative_block_hash)
      AND (compared_publication IS NULL OR (
          project.input_content_hash = compared_publication ->> 'input_content_hash'
          AND project.xmin::text = compared_publication ->> 'row_xmin'
      ))
      AND project.xmin::text = compared_project_row_xmin
    FOR SHARE OF project, lineage;

    IF NOT FOUND THEN
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
            PERFORM 1
            FROM chain_lineage
            WHERE chain_id = position_value ->> 'chain_id'
              AND block_hash = position_value ->> 'block_hash'
              AND block_number =
                  (position_value ->> 'block_number')::bigint
              AND block_timestamp =
                  (position_value ->> 'timestamp')::timestamptz
              AND canonicality_state IN (
                  'canonical',
                  'safe',
                  'finalized'
              )
            FOR SHARE;
        EXCEPTION
            WHEN data_exception THEN
                RETURN 'position_changed';
        END;

        IF NOT FOUND THEN
            RETURN 'position_changed';
        END IF;
    END LOOP;

    -- Match project publication order: name_current is locked before
    -- record_inventory_current. This prevents serving-path writes from
    -- deadlocking with a same-height projection swap.
    IF compared_logical_name_id IS NULL
        AND compared_name_row_xmin IS NULL
    THEN
        NULL;
    ELSIF compared_logical_name_id IS NULL
        OR compared_name_row_xmin IS NULL
    THEN
        RETURN 'invalid_comparison';
    ELSE
        PERFORM 1
        FROM name_current
        WHERE logical_name_id = compared_logical_name_id
          AND support_status = 'supported'
          AND xmin::text = compared_name_row_xmin
        FOR SHARE;

        IF NOT FOUND THEN
            RETURN 'name_changed';
        END IF;
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

    PERFORM 1
    FROM record_inventory_current
    WHERE resource_id = compared_resource_id
      AND record_version_boundary_key = compared_boundary_key
      AND xmin::text = compared_row_xmin
    FOR SHARE;

    IF NOT FOUND THEN
        RETURN 'record_changed';
    END IF;

    RETURN 'unchanged';
END
$$;
    $definition$;
END
$migration$;
