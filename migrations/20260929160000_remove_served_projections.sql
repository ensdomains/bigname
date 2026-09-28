-- Remove the temporary served-input branch while retaining function identity and privileges.
-- The guard and divergence write keep their shared locks and one-transaction refusal behavior.
DO $migration$
BEGIN
    IF to_regclass('bigname_phase.resolution_divergences') IS NULL THEN RETURN; END IF;
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
    compared_family_publication jsonb;
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

    compared_logical_name_id :=
        compared_execution_authority ->> 'logical_name_id';
    compared_name_row_xmin :=
        compared_execution_authority ->> 'name_row_xmin';

    -- Bind every input to the captured family publication and hold its row through commit.
    compared_family_publication := compared_execution_authority -> 'family_publication';
    IF compared_family_publication IS NULL THEN
        RETURN 'invalid_comparison';
    END IF;
        -- Only the lookup builds this object, with every field; a missing field fails the
        -- equality match below and reads as project_changed.
        PERFORM 1
        FROM project_family_marker marker
        JOIN chain_lineage lineage
          ON lineage.chain_id = marker.chain_id
         AND lineage.block_number = marker.current_block_number
         AND lineage.block_hash = marker.current_block_hash
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        WHERE marker.chain_id = requested_authoritative_chain_id
          AND marker.state = 'live'
          AND marker.sequence::text = compared_family_publication ->> 'sequence'
          AND marker.current_block_number::text =
              compared_family_publication ->> 'block_number'
          AND marker.current_block_hash = compared_family_publication ->> 'block_hash'
          AND marker.input_content_hash =
              compared_family_publication ->> 'input_content_hash'
          AND requested_authoritative_block_number - marker.current_block_number BETWEEN 0 AND 1
          AND (marker.current_block_number <> requested_authoritative_block_number
               OR marker.current_block_hash = requested_authoritative_block_hash)
        FOR SHARE OF marker, lineage;

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

        -- The composed name was read in the same snapshot as this locked marker.
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
    EXECUTE $definition$
CREATE OR REPLACE FUNCTION bigname_phase.write_resolution_divergence(
    compared_resource_id uuid,
    compared_boundary_key text,
    compared_row_xmin text,
    requested_authoritative_chain_id text,
    requested_authoritative_block_number bigint,
    requested_authoritative_block_hash text,
    compared_execution_authority jsonb,
    requested_logical_name_id text,
    requested_resolver_chain_id text,
    requested_resolver_address text,
    requested_record_key text,
    compared_positions jsonb,
    live_answer jsonb,
    used_ccip_read boolean
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, bigname_phase, pg_temp
AS $$
DECLARE
    guard_status text;
    resolver_path jsonb;
    compared_entries jsonb;
    compared_provenance jsonb;
    compared_support_status text;
    selector_family text;
    selector_key text;
    indexed_entry jsonb;
    default_entry jsonb;
    indexed_status text;
    indexed_value jsonb;
    indexed_answer jsonb;
BEGIN
    IF used_ccip_read THEN
        RETURN 'ccip_skipped';
    END IF;

    IF compared_execution_authority ->> 'logical_name_id'
        IS DISTINCT FROM requested_logical_name_id
    THEN
        RETURN 'guard_rejected';
    END IF;

    guard_status := revalidate_resolution_lookup_state(
        requested_authoritative_chain_id,
        requested_authoritative_block_number,
        requested_authoritative_block_hash,
        compared_positions,
        compared_execution_authority,
        compared_resource_id,
        compared_boundary_key,
        compared_row_xmin
    );

    IF guard_status <> 'unchanged' THEN
        RETURN 'guard_rejected';
    END IF;

    CASE
        WHEN requested_record_key = 'avatar' THEN
            selector_family := 'avatar';
            selector_key := NULL;
        WHEN requested_record_key = 'contenthash' THEN
            selector_family := 'contenthash';
            selector_key := NULL;
        WHEN requested_record_key LIKE 'text:%'
            AND length(substr(requested_record_key, 6)) > 0
        THEN
            selector_family := 'text';
            selector_key := substr(requested_record_key, 6);
        WHEN requested_record_key ~ '^addr:(0|[1-9][0-9]*)$' THEN
            BEGIN
                selector_key := substr(requested_record_key, 6);
                IF selector_key::numeric > 18446744073709551615::numeric THEN
                    RETURN 'guard_rejected';
                END IF;
                selector_family := 'addr';
            EXCEPTION
                WHEN data_exception THEN
                    RETURN 'guard_rejected';
            END;
        ELSE
            RETURN 'guard_rejected';
    END CASE;

        -- The guard holds the captured publication until this transaction commits. Lookup
        -- supplies the composed inventory from that publication; apply the identical indexed
        -- evaluator below, including unsupported coverage and default-address rules.
        compared_entries := compared_execution_authority #> '{family_comparison,entries}';
        compared_provenance := compared_execution_authority #> '{family_comparison,provenance}';
        compared_support_status := CASE
            WHEN compared_execution_authority #>> '{family_comparison,coverage,status}' = 'projected'
            THEN 'supported' ELSE 'unsupported' END;
        resolver_path := compared_execution_authority #> '{family_name,resolver_path}';

    IF jsonb_typeof(resolver_path) IS DISTINCT FROM 'array'
        OR jsonb_array_length(resolver_path) = 0
        OR resolver_path -> (jsonb_array_length(resolver_path) - 1)
                ->> 'chain_id' <> requested_resolver_chain_id
        OR lower(
            resolver_path -> (jsonb_array_length(resolver_path) - 1)
                ->> 'address'
        ) <> lower(requested_resolver_address)
    THEN
        RETURN 'guard_rejected';
    END IF;

    IF compared_support_status IS DISTINCT FROM 'supported' THEN
        -- The row's coverage is not authoritative: it serves no value, derivation, or absence,
        -- so the comparison target is the same refusal the records route serves.
        indexed_answer := jsonb_build_object('status', 'unsupported');
    ELSE
        SELECT candidate.entry
        INTO indexed_entry
        FROM jsonb_array_elements(compared_entries)
            WITH ORDINALITY AS candidate(entry, ordinal)
        WHERE candidate.entry ->> 'record_key' = requested_record_key
           OR (
                candidate.entry ->> 'record_family' = selector_family
                AND (candidate.entry ->> 'selector_key')
                    IS NOT DISTINCT FROM selector_key
           )
           OR (
                requested_record_key = 'avatar'
                AND candidate.entry ->> 'record_key' = 'text:avatar'
           )
        ORDER BY CASE
            WHEN candidate.entry ->> 'record_key' = 'text:avatar'
                AND requested_record_key = 'avatar'
            THEN 1
            ELSE 0
        END,
        candidate.ordinal
        LIMIT 1;

        IF (indexed_entry IS NULL OR indexed_entry ->> 'status' = 'not_found')
           AND NOT COALESCE(
               requested_record_key = 'addr:60'
               AND indexed_entry ->> 'status' = 'not_found'
               AND jsonb_typeof(compared_provenance -> 'exact_nonempty_not_found_record_keys') = 'array'
               AND compared_provenance -> 'exact_nonempty_not_found_record_keys'
                   @> jsonb_build_array(requested_record_key),
               false
           )
           AND selector_family = 'addr'
           AND (
               selector_key = '60'
               OR selector_key::numeric BETWEEN 2147483649::numeric AND 4294967295::numeric
           )
           AND EXISTS (
               SELECT 1
               FROM jsonb_array_elements(COALESCE(
                   compared_provenance -> 'read_rules', '[]'::jsonb
               )) rule
               WHERE rule ->> 'kind' = 'ensip19_default_address'
                 AND rule ->> 'source_record_key' = 'addr:2147483648'
           )
        THEN
            SELECT candidate.entry
            INTO default_entry
            FROM jsonb_array_elements(compared_entries)
                WITH ORDINALITY AS candidate(entry, ordinal)
            WHERE candidate.entry ->> 'record_key' = 'addr:2147483648'
               OR (
                    candidate.entry ->> 'record_family' = 'addr'
                    AND candidate.entry ->> 'selector_key' = '2147483648'
               )
            ORDER BY candidate.ordinal
            LIMIT 1;

            IF default_entry IS NULL THEN
                indexed_entry := jsonb_build_object('status', 'not_found');
            ELSIF default_entry ->> 'status' IN ('success', 'not_found') THEN
                -- Match the requested getter's verified decode. addr(bytes32) converts
                -- the coin-60 bytes to address(0); multicoin addr(bytes32,uint256)
                -- preserves non-empty bytes, including 20 zero bytes.
                -- (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L36-L40 @ ens_v1@91c966f)
                -- (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L685-L697 @ ens_v2_sepolia_20260629@ccaeb58)
                IF selector_key = '60'
                   AND default_entry ->> 'status' = 'success'
                   AND lower(COALESCE(
                       default_entry #>> '{value,value}',
                       default_entry #>> '{value,bytes}',
                       default_entry ->> 'value'
                   )) = '0x0000000000000000000000000000000000000000'
                THEN
                    indexed_entry := jsonb_build_object('status', 'not_found');
                ELSE
                    indexed_entry := default_entry;
                END IF;
            ELSE
                indexed_entry := jsonb_build_object('status', 'unsupported');
            END IF;
        END IF;

        IF indexed_entry IS NULL THEN
            indexed_answer := jsonb_build_object('status', 'not_found');
        ELSE
            indexed_status := CASE COALESCE(
                indexed_entry ->> 'status',
                'unsupported'
            )
                WHEN 'failed' THEN 'execution_failed'
                ELSE COALESCE(indexed_entry ->> 'status', 'unsupported')
            END;
            indexed_answer := jsonb_build_object('status', indexed_status);
            IF indexed_status = 'success' THEN
                indexed_value := COALESCE(
                    indexed_entry #> '{value,value}',
                    indexed_entry #> '{value,bytes}',
                    indexed_entry -> 'value'
                );
                IF jsonb_typeof(indexed_value) = 'string' THEN
                    indexed_answer := indexed_answer || jsonb_build_object(
                        'value',
                        CASE
                            WHEN selector_family = 'addr'
                                THEN lower(indexed_value #>> '{}')
                            ELSE indexed_value #>> '{}'
                        END
                    );
                ELSE
                    indexed_answer := jsonb_build_object('status', 'unsupported');
                END IF;
            END IF;
        END IF;
    END IF;

    IF indexed_answer = live_answer THEN
        UPDATE resolution_divergences
        SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
        WHERE logical_name_id = requested_logical_name_id
          AND resolver_chain_id = requested_resolver_chain_id
          AND lower(resolver_address) = lower(requested_resolver_address)
          AND request_kind_hash =
              public.digest(requested_record_key, 'sha256')
          AND request_kind = requested_record_key
          AND cleared_at IS NULL;
        IF FOUND THEN
            RETURN 'cleared';
        END IF;
        RETURN 'agreement';
    END IF;

    UPDATE resolution_divergences
    SET cleared_at = GREATEST(statement_timestamp(), last_observed_at)
    WHERE logical_name_id = requested_logical_name_id
      AND resolver_chain_id = requested_resolver_chain_id
      AND lower(resolver_address) = lower(requested_resolver_address)
      AND request_kind_hash =
          public.digest(requested_record_key, 'sha256')
      AND request_kind = requested_record_key
      AND observed_positions <> compared_positions
      AND cleared_at IS NULL;

    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE logical_name_id = requested_logical_name_id
          AND resolver_chain_id = requested_resolver_chain_id
          AND lower(resolver_address) = lower(requested_resolver_address)
          AND request_kind_hash =
              public.digest(requested_record_key, 'sha256')
          AND request_kind <> requested_record_key
    ) THEN
        RAISE EXCEPTION 'resolution divergence request-key hash collision'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    ) VALUES (
        requested_logical_name_id,
        requested_resolver_chain_id,
        lower(requested_resolver_address),
        requested_record_key,
        compared_positions,
        indexed_answer,
        live_answer
    )
    ON CONFLICT ON CONSTRAINT resolution_divergences_pkey DO UPDATE
    SET indexed_result = EXCLUDED.indexed_result,
        live_result = EXCLUDED.live_result,
        last_observed_at = GREATEST(
            resolution_divergences.last_observed_at,
            statement_timestamp()
        ),
        cleared_at = NULL
    WHERE resolution_divergences.request_kind = EXCLUDED.request_kind;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'resolution divergence request-key hash collision'
            USING ERRCODE = '23514';
    END IF;

    RETURN 'written';
END
$$;
    $definition$;
END
$migration$;

-- Owned-family undo replaces the old rebuild state; retained history is not dropped.
DROP TABLE IF EXISTS bigname_phase.name_current;
DROP TABLE IF EXISTS bigname_phase.children_current;
DROP TABLE IF EXISTS bigname_phase.permissions_current;
DROP TABLE IF EXISTS bigname_phase.account_permission_state_current;
DROP TABLE IF EXISTS bigname_phase.permissions_current_resource_summary;
DROP TABLE IF EXISTS bigname_phase.record_inventory_current;
DROP TABLE IF EXISTS bigname_phase.resolver_current;
DROP TABLE IF EXISTS bigname_phase.address_names_current;
DROP TABLE IF EXISTS bigname_phase.address_records_current;
DROP TABLE IF EXISTS bigname_phase.primary_names_current;
DROP TABLE IF EXISTS bigname_phase.project_generation_failures;
DROP TABLE IF EXISTS bigname_phase.project_redo_resolver_evidence;
DROP TABLE IF EXISTS bigname_phase.project_redo_expiry_roots;
DROP TABLE IF EXISTS bigname_phase.project_redo_child_registration_history;

DROP FUNCTION IF EXISTS bigname_phase.retire_direct_divergences_for_null_resolver();

DO $comment$
BEGIN
    IF to_regclass('bigname_phase.project_name_summary') IS NOT NULL THEN
        COMMENT ON TABLE bigname_phase.project_name_summary IS
            'Project-owned name summary: fields the child and label lists filter, sort and count inside one statement. The family writer refreshes touched names from the shared name composition and journals every change for undo. Every name surface has a row. A name without a composed row has no serving resource or registration, while its selected authority arm and next clock boundary can remain. No composed name row is persisted.';
    END IF;
END $comment$;
