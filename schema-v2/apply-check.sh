#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ "${1:-}" != "--database-ready" ]; then
    if [ -n "${BIGNAME_TEST_DATABASE_URL:-}" ]; then
        export SCHEMA_V2_EXTERNAL_DATABASE=1
    fi
    exec "$ROOT/scripts/test-db" -- "$ROOT/schema-v2/apply-check.sh" --database-ready
fi

container="${BIGNAME_TEST_POSTGRES_CONTAINER:-bigname-test-postgres}"
image="${BIGNAME_TEST_POSTGRES_IMAGE:-postgres:16-alpine}"
database="${BIGNAME_TEST_POSTGRES_DB:-bigname}"
user="${BIGNAME_TEST_POSTGRES_USER:-bigname}"

psql_mode=""
if [ "${SCHEMA_V2_EXTERNAL_DATABASE:-0}" = "1" ] \
    && command -v psql >/dev/null 2>&1; then
    psql_mode="host"
elif [ "${SCHEMA_V2_EXTERNAL_DATABASE:-0}" = "1" ] \
    && command -v docker >/dev/null 2>&1 \
    && [ -n "${BIGNAME_DATABASE_URL:-}" ]; then
    psql_mode="client-container"
elif command -v docker >/dev/null 2>&1 \
    && docker inspect "$container" >/dev/null 2>&1 \
    && [ "$(docker inspect --format '{{.State.Running}}' "$container")" = "true" ]; then
    psql_mode="database-container"
elif command -v psql >/dev/null 2>&1; then
    psql_mode="host"
elif command -v docker >/dev/null 2>&1 && [ -n "${BIGNAME_DATABASE_URL:-}" ]; then
    psql_mode="client-container"
else
    printf '%s\n' "psql or Docker is required for the schema check" >&2
    exit 1
fi

run_psql() {
    case "$psql_mode" in
        database-container)
            docker exec -i "$container" \
                psql -X -q -v ON_ERROR_STOP=1 -U "$user" -d "$database"
            ;;
        host)
            psql -X -q -v ON_ERROR_STOP=1 "${BIGNAME_DATABASE_URL:?}"
            ;;
        client-container)
            docker run --rm --network host -i "$image" \
                psql -X -q -v ON_ERROR_STOP=1 "$BIGNAME_DATABASE_URL"
            ;;
    esac
}

wait_for_schema_v2_race_session() {
    local application_name="$1"
    local status
    local attempt

    for ((attempt = 1; attempt <= 100; attempt += 1)); do
        status="$(
            {
                printf 'SET search_path TO "%s";\n' "$scratch_schema"
                printf '%s\n' \
                    "SELECT CASE WHEN EXISTS (" \
                    "    SELECT 1" \
                    "    FROM pg_stat_activity" \
                    "    WHERE application_name = '$application_name'" \
                    "      AND wait_event = 'PgSleep'" \
                    ") THEN 'schema_v2_race_ready'" \
                    "ELSE 'schema_v2_race_waiting' END;"
            } | run_psql
        )"
        if [[ "$status" == *schema_v2_race_ready* ]]; then
            return 0
        fi
        sleep 0.05
    done

    return 1
}

scratch_schema="schema_v2_apply_check_${PPID}_$$"
if [[ ! "$scratch_schema" =~ ^[a-z0-9_]+$ ]]; then
    printf '%s\n' "invalid scratch schema name" >&2
    exit 1
fi

cleanup() {
    if [ -n "${schema_v2_race_pid:-}" ] \
        && kill -0 "$schema_v2_race_pid" >/dev/null 2>&1; then
        kill "$schema_v2_race_pid" >/dev/null 2>&1 || true
        wait "$schema_v2_race_pid" >/dev/null 2>&1 || true
    fi
    if [ -n "${schema_v2_race_log:-}" ]; then
        rm -f -- "$schema_v2_race_log"
    fi
    printf 'DROP SCHEMA IF EXISTS "%s" CASCADE;\n' "$scratch_schema" \
        | run_psql >/dev/null 2>&1 || true
}
trap cleanup EXIT

printf 'CREATE SCHEMA "%s";\n' "$scratch_schema" | run_psql

apply_baseline() {
    local sql_file
    for sql_file in "$ROOT"/schema-v2/baseline/*.sql; do
        {
            printf 'SET client_min_messages TO warning;\n'
            printf 'SET search_path TO "%s";\n' "$scratch_schema"
            cat "$sql_file"
        } | run_psql
    done
}

apply_baseline
apply_baseline

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
DECLARE
    missing_tables text;
    unexpected_tables text;
    uncommented_tables text;
    uncommented_columns text;
    forbidden_tables text;
    forbidden_columns text;
    raw_table_without_hash_key text;
    missing_behavioral_constraints text;
BEGIN
    WITH expected(table_name) AS (
        VALUES
            ('address_names_current'),
            ('chain_heads'),
            ('chain_header_audit'),
            ('chain_lineage'),
            ('chain_phase_state'),
            ('children_current'),
            ('contract_instance_addresses'),
            ('contract_instances'),
            ('discovery_edges'),
            ('ens_names'),
            ('ingest_cursors'),
            ('label_preimages'),
            ('manifest_contract_instances'),
            ('manifest_discovery_rules'),
            ('manifest_versions'),
            ('name_current'),
            ('name_surfaces'),
            ('normalized_events'),
            ('permissions_current'),
            ('permissions_current_resource_summary'),
            ('primary_names_current'),
            ('raw_logs'),
            ('raw_receipts'),
            ('raw_transactions'),
            ('record_inventory_current'),
            ('resolution_divergences'),
            ('resolver_current'),
            ('resources'),
            ('service_heartbeats'),
            ('surface_bindings'),
            ('token_lineages')
    )
    SELECT string_agg(expected.table_name, ', ' ORDER BY expected.table_name)
    INTO missing_tables
    FROM expected
    LEFT JOIN information_schema.tables actual
      ON actual.table_schema = current_schema()
     AND actual.table_name = expected.table_name
     AND actual.table_type = 'BASE TABLE'
    WHERE actual.table_name IS NULL;

    IF missing_tables IS NOT NULL THEN
        RAISE EXCEPTION 'missing schema-v2 tables: %', missing_tables;
    END IF;

    WITH expected(table_name) AS (
        VALUES
            ('address_names_current'),
            ('chain_heads'),
            ('chain_header_audit'),
            ('chain_lineage'),
            ('chain_phase_state'),
            ('children_current'),
            ('contract_instance_addresses'),
            ('contract_instances'),
            ('discovery_edges'),
            ('ens_names'),
            ('ingest_cursors'),
            ('label_preimages'),
            ('manifest_contract_instances'),
            ('manifest_discovery_rules'),
            ('manifest_versions'),
            ('name_current'),
            ('name_surfaces'),
            ('normalized_events'),
            ('permissions_current'),
            ('permissions_current_resource_summary'),
            ('primary_names_current'),
            ('raw_logs'),
            ('raw_receipts'),
            ('raw_transactions'),
            ('record_inventory_current'),
            ('resolution_divergences'),
            ('resolver_current'),
            ('resources'),
            ('service_heartbeats'),
            ('surface_bindings'),
            ('token_lineages')
    )
    SELECT string_agg(actual.table_name, ', ' ORDER BY actual.table_name)
    INTO unexpected_tables
    FROM information_schema.tables actual
    LEFT JOIN expected
      ON expected.table_name = actual.table_name
    WHERE actual.table_schema = current_schema()
      AND actual.table_type = 'BASE TABLE'
      AND expected.table_name IS NULL;

    IF unexpected_tables IS NOT NULL THEN
        RAISE EXCEPTION 'unexpected schema-v2 tables: %', unexpected_tables;
    END IF;

    SELECT string_agg(table_name, ', ' ORDER BY table_name)
    INTO forbidden_tables
    FROM information_schema.tables
    WHERE table_schema = current_schema()
      AND table_type = 'BASE TABLE'
      AND (
          table_name ~ '(coverage|backfill|lease|generation|revision)'
          OR table_name ~ '(checkpoint|frontier|queue|dead_letter|watermark)'
          OR table_name ~ '(code_hash|execution_trace|execution_step)'
          OR table_name ~ '(outcome_cache|raw_call|startup)'
          OR table_name = 'manifest_capability_flags'
      );

    IF forbidden_tables IS NOT NULL THEN
        RAISE EXCEPTION 'forbidden schema-v2 tables: %', forbidden_tables;
    END IF;

    SELECT string_agg(
        format('%I.%I', table_name, column_name),
        ', '
        ORDER BY table_name, ordinal_position
    )
    INTO forbidden_columns
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND (
          column_name ~ '(coverage|exhaustiveness|generation|revision)'
          OR column_name ~ '(supersed|repair|capability)'
          OR column_name = 'code_hash'
      );

    IF forbidden_columns IS NOT NULL THEN
        RAISE EXCEPTION 'forbidden schema-v2 columns: %', forbidden_columns;
    END IF;

    SELECT string_agg(class.relname, ', ' ORDER BY class.relname)
    INTO uncommented_tables
    FROM pg_class class
    JOIN pg_namespace namespace
      ON namespace.oid = class.relnamespace
    WHERE namespace.nspname = current_schema()
      AND class.relkind = 'r'
      AND obj_description(class.oid, 'pg_class') IS NULL;

    IF uncommented_tables IS NOT NULL THEN
        RAISE EXCEPTION 'schema-v2 tables without comments: %', uncommented_tables;
    END IF;

    SELECT string_agg(
        format('%I.%I', class.relname, attribute.attname),
        ', '
        ORDER BY class.relname, attribute.attnum
    )
    INTO uncommented_columns
    FROM pg_class class
    JOIN pg_namespace namespace
      ON namespace.oid = class.relnamespace
    JOIN pg_attribute attribute
      ON attribute.attrelid = class.oid
    WHERE namespace.nspname = current_schema()
      AND class.relkind = 'r'
      AND attribute.attnum > 0
      AND NOT attribute.attisdropped
      AND col_description(class.oid, attribute.attnum) IS NULL;

    IF uncommented_columns IS NOT NULL THEN
        RAISE EXCEPTION 'schema-v2 columns without comments: %', uncommented_columns;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'normalized_events'
          AND column_name ~ '(repair|supersed|authority|revision|generation)'
    ) THEN
        RAISE EXCEPTION 'normalized_events contains repair apparatus';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name IN (
              'name_current',
              'address_names_current',
              'permissions_current',
              'permissions_current_resource_summary',
              'record_inventory_current',
              'resolver_current',
              'children_current',
              'primary_names_current'
          )
          AND column_name IN (
              'coverage',
              'exhaustiveness',
              'enumeration_basis',
              'explicit_gaps'
          )
    ) THEN
        RAISE EXCEPTION 'a current projection contains exhaustiveness accounting';
    END IF;

    WITH required(table_name) AS (
        VALUES
            ('chain_header_audit'),
            ('raw_logs'),
            ('raw_receipts'),
            ('raw_transactions')
    )
    SELECT string_agg(required.table_name, ', ' ORDER BY required.table_name)
    INTO raw_table_without_hash_key
    FROM required
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_constraint constraint_row
        JOIN pg_class class
          ON class.oid = constraint_row.conrelid
        JOIN pg_namespace namespace
          ON namespace.oid = class.relnamespace
        WHERE namespace.nspname = current_schema()
          AND class.relname = required.table_name
          AND constraint_row.contype = 'p'
          AND EXISTS (
              SELECT 1
              FROM unnest(constraint_row.conkey) AS key_column(attnum)
              JOIN pg_attribute attribute
                ON attribute.attrelid = class.oid
               AND attribute.attnum = key_column.attnum
              WHERE attribute.attname = 'block_hash'
          )
    );

    IF raw_table_without_hash_key IS NOT NULL THEN
        RAISE EXCEPTION
            'raw tables without block_hash primary keys: %',
            raw_table_without_hash_key;
    END IF;

    WITH invariant_checks(invariant_name, is_present) AS (
        SELECT
            'canonical surface bindings cannot overlap',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'surface_bindings'::regclass
                  AND conname = 'surface_bindings_no_overlap'
                  AND contype = 'x'
            )
        UNION ALL
        SELECT
            'one address row is active per contract instance',
            EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'contract_instance_addresses_active_instance_idx'
                  AND indexdef LIKE 'CREATE UNIQUE INDEX%'
                  AND indexdef LIKE '%(contract_instance_id)%'
                  AND indexdef LIKE '%deactivated_at IS NULL%'
            )
        UNION ALL
        SELECT
            'manifest contract declarations cascade on manifest deletion',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'manifest_contract_instances'::regclass
                  AND conname =
                      'manifest_contract_instances_manifest_fkey'
                  AND contype = 'f'
                  AND confdeltype = 'c'
            )
        UNION ALL
        SELECT
            'manifest discovery rules cascade on manifest deletion',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'manifest_discovery_rules'::regclass
                  AND conname = 'manifest_discovery_rules_manifest_fkey'
                  AND contype = 'f'
                  AND confdeltype = 'c'
            )
        UNION ALL
        SELECT
            'receipt transaction positions match their transactions',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'raw_receipts'::regclass
                  AND conname = 'raw_receipts_transaction_position_fkey'
                  AND contype = 'f'
            )
        UNION ALL
        SELECT
            'log transaction positions match their transactions',
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'raw_logs'::regclass
                  AND conname = 'raw_logs_transaction_position_fkey'
                  AND contype = 'f'
            )
    )
    SELECT string_agg(invariant_name, ', ' ORDER BY invariant_name)
    INTO missing_behavioral_constraints
    FROM invariant_checks
    WHERE NOT is_present;

    IF missing_behavioral_constraints IS NOT NULL THEN
        RAISE EXCEPTION
            'missing schema-v2 behavioral constraints: %',
            missing_behavioral_constraints;
    END IF;
END
$$;

BEGIN;

DO $$
DECLARE
    manifest_key bigint;
    accepted_projection_identity_mismatches text[] := ARRAY[]::text[];
    accepted_cross_chain_relationships text[] := ARRAY[]::text[];
    accepted_head_invariants text[] := ARRAY[]::text[];
    accepted_projection_relationship_mismatches text[] := ARRAY[]::text[];
    accepted_manifest_mismatches text[] := ARRAY[]::text[];
    accepted_divergence_canonicality text[] := ARRAY[]::text[];
BEGIN
    INSERT INTO chain_lineage (
        chain_id,
        block_hash,
        block_number,
        block_timestamp,
        canonicality_state
    )
    VALUES
        (
            'schema-v2-check',
            'block-0',
            0,
            '2026-01-01 00:00:00+00',
            'canonical'
        ),
        (
            'schema-v2-check',
            'block-1',
            1,
            '2026-01-01 00:00:01+00',
            'canonical'
        ),
        (
            'schema-v2-other',
            'block-other-0',
            0,
            '2026-01-01 00:00:00+00',
            'canonical'
        ),
        (
            'schema-v2-other',
            'orphaned-block-1',
            1,
            '2026-01-01 00:00:01+00',
            'orphaned'
        );

    BEGIN
        INSERT INTO chain_heads (
            chain_id,
            latest_block_hash,
            latest_block_number
        )
        VALUES (
            'schema-v2-other',
            'orphaned-block-1',
            1
        );
        accepted_head_invariants :=
            array_append(accepted_head_invariants, 'orphaned head');
        DELETE FROM chain_heads
        WHERE chain_id = 'schema-v2-other';
    EXCEPTION
        WHEN check_violation OR foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            'schema-v2-check',
            'competing-block-0',
            0,
            '2026-01-01 00:00:00+00',
            'canonical'
        );
        accepted_head_invariants :=
            array_append(
                accepted_head_invariants,
                'multiple canonical blocks at one height'
            );
        DELETE FROM chain_lineage
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'competing-block-0';
    EXCEPTION
        WHEN unique_violation THEN NULL;
    END;

    INSERT INTO chain_heads (
        chain_id,
        latest_block_hash,
        latest_block_number
    )
    VALUES (
        'schema-v2-check',
        'block-0',
        0
    );

    BEGIN
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'block-0';
        accepted_head_invariants :=
            array_append(
                accepted_head_invariants,
                'head demoted to orphaned'
            );
        UPDATE chain_lineage
        SET canonicality_state = 'canonical'
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'block-0';
    EXCEPTION
        WHEN check_violation OR foreign_key_violation THEN NULL;
    END;

    IF cardinality(accepted_head_invariants) > 0 THEN
        RAISE EXCEPTION
            'chain tables accepted invalid head state: %',
            array_to_string(accepted_head_invariants, ', ');
    END IF;

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name,
        phase_status,
        verification_level
    )
    VALUES (
        'schema-v2-check',
        'verify',
        'idle',
        'quick_synced'
    );

    UPDATE chain_phase_state
    SET verification_level = 'cross_checked'
    WHERE chain_id = 'schema-v2-check'
      AND phase_name = 'verify';

    UPDATE chain_phase_state
    SET verification_level = 'node_checked'
    WHERE chain_id = 'schema-v2-check'
      AND phase_name = 'verify';

    BEGIN
        INSERT INTO chain_phase_state (
            chain_id,
            phase_name,
            phase_status,
            verification_level
        )
        VALUES (
            'schema-v2-check',
            'ingest',
            'idle',
            'quick_synced'
        );
        RAISE EXCEPTION
            'a non-verify phase accepted a verification level';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name,
        phase_status,
        started_at
    )
    VALUES (
        'schema-v2-check',
        'ingest',
        'paused',
        now()
    );

    DELETE FROM chain_phase_state
    WHERE chain_id = 'schema-v2-check'
      AND phase_name = 'ingest';

    INSERT INTO raw_transactions (
        chain_id,
        block_hash,
        block_number,
        transaction_hash,
        transaction_index,
        from_address
    )
    VALUES
        ('schema-v2-check', 'block-0', 0, 'tx-0', 0, 'address-0'),
        ('schema-v2-check', 'block-0', 0, 'tx-1', 1, 'address-1');

    BEGIN
        INSERT INTO raw_receipts (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index
        )
        VALUES ('schema-v2-check', 'block-0', 0, 'tx-0', 1);
        RAISE EXCEPTION
            'raw_receipts accepted a mismatched transaction position';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address
        )
        VALUES (
            'schema-v2-check',
            'block-0',
            0,
            'tx-0',
            1,
            0,
            'address-0'
        );
        RAISE EXCEPTION
            'raw_logs accepted a mismatched transaction position';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000001',
        'schema-v2-check',
        'registry'
    );

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'resolver'
    );

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000003',
        'schema-v2-other',
        'registry'
    );

    BEGIN
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address
        )
        VALUES (
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-other',
            'cross-chain-address'
        );
        RAISE EXCEPTION
            'a contract address accepted a different instance chain';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission_basis
        )
        VALUES (
            'schema-v2-other',
            'proxy',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-check',
            'schema-v2-check'
        );
        RAISE EXCEPTION
            'a discovery edge accepted different endpoint chains';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    INSERT INTO contract_instance_addresses (
        contract_instance_id,
        chain_id,
        address,
        active_from_block_number,
        active_to_block_number,
        deactivated_at
    )
    VALUES (
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'historical-address-0',
        0,
        100,
        now()
    );

    BEGIN
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            active_to_block_number,
            deactivated_at
        )
        VALUES (
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-check',
            'historical-address-1',
            50,
            150,
            now()
        );
        RAISE EXCEPTION
            'one contract instance accepted overlapping address ranges';
    EXCEPTION
        WHEN exclusion_violation THEN NULL;
    END;

    INSERT INTO contract_instance_addresses (
        contract_instance_id,
        chain_id,
        address,
        active_from_block_number
    )
    VALUES (
        '00000000-0000-0000-0000-000000000001',
        'schema-v2-check',
        'contract-address-0',
        0
    );

    BEGIN
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address
        )
        VALUES (
            '00000000-0000-0000-0000-000000000001',
            'schema-v2-check',
            'contract-address-1'
        );
        RAISE EXCEPTION
            'one contract instance accepted two active addresses';
    EXCEPTION
        WHEN unique_violation OR exclusion_violation THEN NULL;
    END;

    INSERT INTO token_lineages (
        token_lineage_id,
        chain_id,
        block_hash,
        block_number
    )
    VALUES
        (
            '00000000-0000-0000-0000-000000000031',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000032',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000033',
            'schema-v2-check',
            'block-0',
            0
        );

    INSERT INTO resources (
        resource_id,
        token_lineage_id,
        chain_id,
        block_hash,
        block_number
    )
    VALUES
        (
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000031',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'schema-v2-check',
            'block-0',
            0
        ),
        (
            '00000000-0000-0000-0000-000000000013',
            NULL,
            'schema-v2-other',
            'block-other-0',
            0
        );

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'Name',
        ARRAY['Name'],
        '\x044e616d6500',
        'namehash-0',
        ARRAY['labelhash-0'],
        'check',
        'active',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO name_surfaces (
        logical_name_id,
        namespace,
        raw_name,
        raw_labels,
        dns_encoded_name,
        namehash,
        labelhashes,
        normalizer_version,
        visibility_state,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-2',
        'schema-v2-check',
        'Other',
        ARRAY['Other'],
        '\x054f7468657200',
        'namehash-2',
        ARRAY['labelhash-2'],
        'check',
        'active',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    BEGIN
        INSERT INTO name_surfaces (
            logical_name_id,
            namespace,
            raw_name,
            raw_labels,
            dns_encoded_name,
            namehash,
            labelhashes,
            normalizer_version,
            visibility_state,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:not-the-namehash',
            'schema-v2-check',
            'Other',
            ARRAY['Other'],
            '\x054f7468657200',
            'namehash-1',
            ARRAY['labelhash-1'],
            'check',
            'active',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted a logical ID that is not namespace:namehash';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO resources (
            resource_id,
            token_lineage_id,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000014',
            '00000000-0000-0000-0000-000000000033',
            'schema-v2-other',
            'block-other-0',
            0
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'resources.token_lineage_id'
            );
        DELETE FROM resources
        WHERE resource_id = '00000000-0000-0000-0000-000000000014';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000023',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000013',
            'declared',
            '2026-01-01 00:00:00+00',
            'schema-v2-other',
            'block-other-0',
            0
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'surface_bindings.logical_name_id'
            );
        DELETE FROM surface_bindings
        WHERE surface_binding_id =
            '00000000-0000-0000-0000-000000000023';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            chain_id,
            block_hash,
            block_number
        )
        VALUES (
            '00000000-0000-0000-0000-000000000024',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000013',
            'declared',
            '2026-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'surface_bindings.resource_id'
            );
        DELETE FROM surface_bindings
        WHERE surface_binding_id =
            '00000000-0000-0000-0000-000000000024';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'cross-chain-name-event',
            'schema-v2-check',
            'schema-v2-check:namehash-0',
            'check',
            'check',
            1,
            'schema-v2-other',
            'check'
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'normalized_events.logical_name_id'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'cross-chain-name-event';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            resource_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'cross-chain-resource-event',
            'schema-v2-check',
            '00000000-0000-0000-0000-000000000011',
            'check',
            'check',
            1,
            'schema-v2-other',
            'check'
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'normalized_events.resource_id'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'cross-chain-resource-event';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    IF cardinality(accepted_cross_chain_relationships) > 0 THEN
        RAISE EXCEPTION
            'identity tables accepted cross-chain relationships: %',
            array_to_string(accepted_cross_chain_relationships, ', ');
    END IF;

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-0',
        'addr',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"value": "indexed"}'::jsonb,
        '{"value": "live"}'::jsonb
    );

    BEGIN
        UPDATE chain_lineage
        SET block_timestamp = '2026-01-01 00:00:02+00'
        WHERE chain_id = 'schema-v2-check'
          AND block_hash = 'block-0';
        RAISE EXCEPTION
            'chain_lineage accepted a block timestamp identity change';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result,
            first_observed_at,
            last_observed_at,
            cleared_at
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'resolver-address-time-check',
            'addr',
            '{
                "resolver": {
                    "chain_id": "schema-v2-check",
                    "block_hash": "block-1",
                    "block_number": 1,
                    "timestamp": "2026-01-01T00:00:01Z"
                }
            }'::jsonb,
            '{"value": "indexed"}'::jsonb,
            '{"value": "live"}'::jsonb,
            '2026-01-01 00:00:00+00',
            '2026-01-03 00:00:00+00',
            '2026-01-02 00:00:00+00'
        );
        RAISE EXCEPTION
            'resolution_divergences accepted clearing before last observation';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-other',
        'resolver-address-reorg-check',
        'addr',
        '{
            "resolver": {
                "chain_id": "schema-v2-other",
                "block_hash": "block-other-0",
                "block_number": 0,
                "timestamp": "2026-01-01T00:00:00Z"
            }
        }'::jsonb,
        '{"value": "indexed"}'::jsonb,
        '{"value": "live"}'::jsonb
    );

    UPDATE chain_lineage
    SET canonicality_state = 'orphaned'
    WHERE chain_id = 'schema-v2-other'
      AND block_hash = 'block-other-0';

    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE resolver_address = 'resolver-address-reorg-check'
          AND cleared_at IS NULL
    ) THEN
        accepted_divergence_canonicality :=
            array_append(
                accepted_divergence_canonicality,
                'active row survived an orphaned observed block'
            );
    END IF;

    UPDATE chain_lineage
    SET canonicality_state = 'canonical'
    WHERE chain_id = 'schema-v2-other'
      AND block_hash = 'block-other-0';

    DELETE FROM resolution_divergences
    WHERE resolver_address = 'resolver-address-reorg-check';

    BEGIN
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-other',
            'resolver-address-orphan-check',
            'addr',
            '{
                "resolver": {
                    "chain_id": "schema-v2-other",
                    "block_hash": "orphaned-block-1",
                    "block_number": 1,
                    "timestamp": "2026-01-01T00:00:01Z"
                }
            }'::jsonb,
            '{"value": "indexed"}'::jsonb,
            '{"value": "live"}'::jsonb
        );
        accepted_divergence_canonicality :=
            array_append(
                accepted_divergence_canonicality,
                'active row accepted an orphaned observed block'
            );
        DELETE FROM resolution_divergences
        WHERE resolver_address = 'resolver-address-orphan-check';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    IF cardinality(accepted_divergence_canonicality) > 0 THEN
        RAISE EXCEPTION
            'resolution differences accepted noncanonical positions: %',
            array_to_string(accepted_divergence_canonicality, ', ');
    END IF;

    BEGIN
        INSERT INTO resolution_divergences (
            logical_name_id,
            resolver_chain_id,
            resolver_address,
            request_kind,
            observed_positions,
            indexed_result,
            live_result
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'resolver-address-0',
            'addr',
            '{
                "resolver": {
                    "chain_id": "schema-v2-check",
                    "block_hash": "block-1",
                    "block_number": 1,
                    "timestamp": "2026-01-01T00:00:01Z"
                }
            }'::jsonb,
            '{"value": "indexed"}'::jsonb,
            '{"value": "live"}'::jsonb
        );
        RAISE EXCEPTION
            'one request accepted two active divergence rows';
    EXCEPTION
        WHEN unique_violation THEN NULL;
    END;

    UPDATE resolution_divergences
    SET cleared_at = now()
    WHERE logical_name_id = 'schema-v2-check:namehash-0'
      AND resolver_chain_id = 'schema-v2-check'
      AND resolver_address = 'resolver-address-0'
      AND request_kind = 'addr';

    INSERT INTO resolution_divergences (
        logical_name_id,
        resolver_chain_id,
        resolver_address,
        request_kind,
        observed_positions,
        indexed_result,
        live_result
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check',
        'resolver-address-0',
        'addr',
        '{
            "resolver": {
                "chain_id": "schema-v2-check",
                "block_hash": "block-1",
                "block_number": 1,
                "timestamp": "2026-01-01T00:00:01Z"
            }
        }'::jsonb,
        '{"value": "indexed"}'::jsonb,
        '{"value": "live"}'::jsonb
    );

    INSERT INTO surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind,
        active_from,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        '00000000-0000-0000-0000-000000000021',
        'schema-v2-check:namehash-0',
        '00000000-0000-0000-0000-000000000011',
        'declared',
        '2026-01-01 00:00:00+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    INSERT INTO surface_bindings (
        surface_binding_id,
        logical_name_id,
        resource_id,
        binding_kind,
        active_from,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        '00000000-0000-0000-0000-000000000025',
        'schema-v2-check:namehash-2',
        '00000000-0000-0000-0000-000000000012',
        'declared',
        '2026-01-01 00:00:00+00',
        'schema-v2-check',
        'block-0',
        0,
        'canonical'
    );

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000025',
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'declared',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'name_current binding'
            );
        DELETE FROM name_current
        WHERE logical_name_id = 'schema-v2-check:namehash-0';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'address-binding-mismatch',
            'schema-v2-check:namehash-0',
            'owner',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000025',
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'declared',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'address_names_current binding'
            );
        DELETE FROM address_names_current
        WHERE address = 'address-binding-mismatch';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000032',
            'declared',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'name_current token lineage'
            );
        DELETE FROM name_current
        WHERE logical_name_id = 'schema-v2-check:namehash-0';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            token_lineage_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'address-token-mismatch',
            'schema-v2-check:namehash-0',
            'owner',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000032',
            'declared',
            'supported',
            1
        );
        accepted_projection_relationship_mismatches :=
            array_append(
                accepted_projection_relationship_mismatches,
                'address_names_current token lineage'
            );
        DELETE FROM address_names_current
        WHERE address = 'address-token-mismatch';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    IF cardinality(accepted_projection_relationship_mismatches) > 0 THEN
        RAISE EXCEPTION
            'projection tables accepted mismatched relationships: %',
            array_to_string(
                accepted_projection_relationship_mismatches,
                ', '
            );
    END IF;

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
            namespace,
            raw_name,
            namehash,
            support_status,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'wrong-namespace',
            'Name',
            'wrong-namehash',
            'supported',
            1
        );
        accepted_projection_identity_mismatches :=
            array_append(
                accepted_projection_identity_mismatches,
                'name_current'
            );
        DELETE FROM name_current
        WHERE logical_name_id = 'schema-v2-check:namehash-0';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            namehash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-namehash',
            'wrong-namespace',
            'Child.Name',
            'wrong-namehash',
            1
        );
        accepted_projection_identity_mismatches :=
            array_append(
                accepted_projection_identity_mismatches,
                'children_current'
            );
        DELETE FROM children_current
        WHERE parent_logical_name_id = 'schema-v2-check:namehash-0'
          AND child_logical_name_id = 'schema-v2-check:child-namehash';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO address_names_current (
            address,
            logical_name_id,
            relation,
            namespace,
            raw_name,
            namehash,
            surface_binding_id,
            resource_id,
            binding_kind,
            support_status,
            manifest_version
        )
        VALUES (
            'address-0',
            'schema-v2-check:namehash-0',
            'owner',
            'wrong-namespace',
            'Name',
            'wrong-namehash',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            'supported',
            1
        );
        accepted_projection_identity_mismatches :=
            array_append(
                accepted_projection_identity_mismatches,
                'address_names_current'
            );
        DELETE FROM address_names_current
        WHERE address = 'address-0'
          AND logical_name_id = 'schema-v2-check:namehash-0'
          AND relation = 'owner';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    IF cardinality(accepted_projection_identity_mismatches) > 0 THEN
        RAISE EXCEPTION
            'projection tables accepted mismatched logical IDs: %',
            array_to_string(
                accepted_projection_identity_mismatches,
                ', '
            );
    END IF;

    BEGIN
        INSERT INTO surface_bindings (
            surface_binding_id,
            logical_name_id,
            resource_id,
            binding_kind,
            active_from,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            '00000000-0000-0000-0000-000000000022',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000012',
            'declared',
            '2026-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'surface_bindings accepted overlapping canonical ranges';
    EXCEPTION
        WHEN exclusion_violation THEN NULL;
    END;

    INSERT INTO manifest_versions (
        manifest_version,
        namespace,
        source_family,
        chain_id,
        deployment_epoch,
        rollout_status,
        normalizer_version,
        file_path,
        manifest_payload
    )
    VALUES (
        1,
        'schema-v2-check',
        'schema-v2-check',
        'schema-v2-check',
        'schema-v2-check',
        'draft',
        'check',
        'schema-v2-check.toml',
        '{}'::jsonb
    )
    RETURNING manifest_id INTO manifest_key;

    BEGIN
        INSERT INTO manifest_contract_instances (
            manifest_id,
            chain_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            proxy_kind
        )
        VALUES (
            manifest_key,
            'schema-v2-check',
            'root',
            'cross-chain-root',
            '00000000-0000-0000-0000-000000000003',
            'cross-chain-address',
            'none'
        );
        accepted_manifest_mismatches :=
            array_append(
                accepted_manifest_mismatches,
                'manifest contract chain'
            );
        DELETE FROM manifest_contract_instances
        WHERE manifest_id = manifest_key
          AND declaration_name = 'cross-chain-root';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            derivation_kind
        )
        VALUES (
            'mismatched-manifest-event',
            'wrong-namespace',
            'check',
            'wrong-source-family',
            99,
            manifest_key,
            'schema-v2-other',
            'check'
        );
        accepted_manifest_mismatches :=
            array_append(
                accepted_manifest_mismatches,
                'normalized event manifest'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'mismatched-manifest-event';
    EXCEPTION
        WHEN foreign_key_violation THEN NULL;
    END;

    IF cardinality(accepted_manifest_mismatches) > 0 THEN
        RAISE EXCEPTION
            'manifest tables accepted contradictory provenance: %',
            array_to_string(accepted_manifest_mismatches, ', ');
    END IF;

    INSERT INTO manifest_contract_instances (
        manifest_id,
        chain_id,
        declaration_kind,
        declaration_name,
        contract_instance_id,
        declared_address,
        proxy_kind
    )
    VALUES (
        manifest_key,
        'schema-v2-check',
        'root',
        'root',
        '00000000-0000-0000-0000-000000000001',
        'contract-address-0',
        'none'
    );

    INSERT INTO manifest_discovery_rules (
        manifest_id,
        edge_kind,
        admission
    )
    VALUES (manifest_key, 'announced', 'declared');

    UPDATE contract_instance_addresses
    SET source_manifest_id = manifest_key
    WHERE contract_instance_id =
        '00000000-0000-0000-0000-000000000001'
      AND deactivated_at IS NULL;

    INSERT INTO discovery_edges (
        chain_id,
        edge_kind,
        from_contract_instance_id,
        to_contract_instance_id,
        discovery_source,
        admission_basis,
        source_manifest_id
    )
    VALUES (
        'schema-v2-check',
        'proxy',
        '00000000-0000-0000-0000-000000000001',
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'schema-v2-check',
        manifest_key
    );

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        source_manifest_id,
        chain_id,
        derivation_kind
    )
    VALUES (
        'manifest-delete-check',
        'schema-v2-check',
        'check',
        'schema-v2-check',
        1,
        manifest_key,
        'schema-v2-check',
        'check'
    );

    DELETE FROM manifest_versions
    WHERE manifest_id = manifest_key;

    IF EXISTS (
        SELECT 1
        FROM manifest_contract_instances
        WHERE manifest_id = manifest_key
    ) OR EXISTS (
        SELECT 1
        FROM manifest_discovery_rules
        WHERE manifest_id = manifest_key
    ) THEN
        RAISE EXCEPTION
            'manifest deletion did not remove child declarations';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM contract_instance_addresses
        WHERE source_manifest_id = manifest_key
    ) OR EXISTS (
        SELECT 1
        FROM discovery_edges
        WHERE source_manifest_id = manifest_key
    ) OR EXISTS (
        SELECT 1
        FROM normalized_events
        WHERE source_manifest_id = manifest_key
    ) THEN
        RAISE EXCEPTION
            'manifest deletion did not clear retained provenance links';
    END IF;
END
$$;

ROLLBACK;
SQL
} | run_psql

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO chain_lineage (
    chain_id,
    block_hash,
    block_number,
    block_timestamp,
    canonicality_state
)
VALUES (
    'schema-v2-race',
    'race-block-0',
    0,
    '2026-01-01 00:00:00+00',
    'canonical'
);

INSERT INTO name_surfaces (
    logical_name_id,
    namespace,
    raw_name,
    raw_labels,
    dns_encoded_name,
    namehash,
    labelhashes,
    normalizer_version,
    visibility_state,
    chain_id,
    block_hash,
    block_number,
    canonicality_state
)
VALUES (
    'schema-v2-race:race-namehash',
    'schema-v2-race',
    'race',
    ARRAY['race'],
    decode('00', 'hex'),
    'race-namehash',
    ARRAY['race-labelhash'],
    'schema-v2-check',
    'active',
    'schema-v2-race',
    'race-block-0',
    0,
    'canonical'
);
SQL
} | run_psql

schema_v2_race_log="$(mktemp)"
schema_v2_race_application="schema_v2_race_${PPID}_$$"

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf "SET application_name TO '%s';\n" \
        "$schema_v2_race_application"
    cat <<'SQL'
BEGIN;

INSERT INTO resolution_divergences (
    logical_name_id,
    resolver_chain_id,
    resolver_address,
    request_kind,
    observed_positions,
    indexed_result,
    live_result
)
VALUES (
    'schema-v2-race:race-namehash',
    'schema-v2-race',
    'resolver-address-race',
    'addr',
    '{
        "resolver": {
            "chain_id": "schema-v2-race",
            "block_hash": "race-block-0",
            "block_number": 0,
            "timestamp": "2026-01-01T00:00:00Z"
        }
    }'::jsonb,
    '{"value": "indexed"}'::jsonb,
    '{"value": "live"}'::jsonb
);

SELECT pg_sleep(5);
COMMIT;
SQL
} | run_psql >"$schema_v2_race_log" 2>&1 &
schema_v2_race_pid=$!

if ! wait_for_schema_v2_race_session "$schema_v2_race_application"; then
    printf '%s\n' \
        "concurrent resolution-difference insert did not become ready" >&2
    cat "$schema_v2_race_log" >&2
    exit 1
fi

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
UPDATE chain_lineage
SET canonicality_state = 'orphaned'
WHERE chain_id = 'schema-v2-race'
  AND block_hash = 'race-block-0';
SQL
} | run_psql

if ! wait "$schema_v2_race_pid"; then
    cat "$schema_v2_race_log" >&2
    exit 1
fi
schema_v2_race_pid=""

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM resolution_divergences
        WHERE resolver_address = 'resolver-address-race'
          AND cleared_at IS NULL
    ) THEN
        RAISE EXCEPTION
            'concurrent reorg left an active resolution difference';
    END IF;
END
$$;
SQL
} | run_psql

rm -f -- "$schema_v2_race_log"
schema_v2_race_log=""

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
INSERT INTO chain_lineage (
    chain_id,
    block_hash,
    block_number,
    block_timestamp,
    canonicality_state
)
VALUES (
    'schema-v2-head-race',
    'head-race-block-0',
    0,
    '2026-01-01 00:00:00+00',
    'canonical'
);
SQL
} | run_psql

schema_v2_race_log="$(mktemp)"
schema_v2_race_application="schema_v2_head_race_${PPID}_$$"

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf "SET application_name TO '%s';\n" \
        "$schema_v2_race_application"
    cat <<'SQL'
BEGIN;

INSERT INTO chain_heads (
    chain_id,
    latest_block_hash,
    latest_block_number
)
VALUES (
    'schema-v2-head-race',
    'head-race-block-0',
    0
);

SELECT pg_sleep(5);
COMMIT;
SQL
} | run_psql >"$schema_v2_race_log" 2>&1 &
schema_v2_race_pid=$!

if ! wait_for_schema_v2_race_session "$schema_v2_race_application"; then
    printf '%s\n' "concurrent chain-head insert did not become ready" >&2
    cat "$schema_v2_race_log" >&2
    exit 1
fi

schema_v2_head_update_output=""
if ! schema_v2_head_update_output="$(
    {
        printf 'SET search_path TO "%s";\n' "$scratch_schema"
        cat <<'SQL'
UPDATE chain_lineage
SET canonicality_state = 'orphaned'
WHERE chain_id = 'schema-v2-head-race'
  AND block_hash = 'head-race-block-0';
SQL
    } | run_psql 2>&1
)"; then
    if [[ "$schema_v2_head_update_output" != \
        *"a chain head still references this block state"* ]]; then
        printf '%s\n' "$schema_v2_head_update_output" >&2
        exit 1
    fi
fi

if ! wait "$schema_v2_race_pid"; then
    cat "$schema_v2_race_log" >&2
    exit 1
fi
schema_v2_race_pid=""

{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM chain_heads AS head
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = head.chain_id
         AND lineage.block_hash = head.latest_block_hash
         AND lineage.block_number = head.latest_block_number
        WHERE lineage.canonicality_state
            NOT IN ('canonical', 'safe', 'finalized')
    ) THEN
        RAISE EXCEPTION
            'concurrent reorg left a head on a noncanonical block';
    END IF;
END
$$;
SQL
} | run_psql

printf '%s\n' \
    "schema-v2 baseline applied twice and passed structural and behavior checks"
