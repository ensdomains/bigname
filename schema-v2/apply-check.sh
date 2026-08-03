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
    forbidden_projection_publication_tables text;
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

    -- Add exact exceptions only after maintainer authorization.
    WITH maintainer_authorized_allowlist(table_name) AS (
        SELECT NULL::text
        WHERE FALSE
    )
    SELECT string_agg(actual.table_name, ', ' ORDER BY actual.table_name)
    INTO forbidden_tables
    FROM information_schema.tables actual
    LEFT JOIN maintainer_authorized_allowlist
      ON maintainer_authorized_allowlist.table_name = actual.table_name
    WHERE actual.table_schema = current_schema()
      AND actual.table_type = 'BASE TABLE'
      AND maintainer_authorized_allowlist.table_name IS NULL
      AND (
          actual.table_name ~
              '(coverage|backfill|lease|generation|revision)'
          OR actual.table_name ~
              '(checkpoint|frontier|queue|dead_letter|watermark)'
          OR actual.table_name ~
              '(code_hash|execution_trace|execution_step)'
          OR actual.table_name ~ '(outcome_cache|raw_call|startup)'
          OR actual.table_name ~
              '(fence|epoch|journal|promotion|reconciliation|rederive)'
          OR actual.table_name ~
              '(drift|alert|replay_version|dead_letter|watermark|staging)'
          OR actual.table_name = 'manifest_capability_flags'
      );

    IF forbidden_tables IS NOT NULL THEN
        RAISE EXCEPTION 'forbidden schema-v2 tables: %', forbidden_tables;
    END IF;

    -- Add exact exceptions only after maintainer authorization.
    WITH maintainer_authorized_allowlist(table_name, column_name) AS (
        SELECT NULL::text, NULL::text
        WHERE FALSE
    )
    SELECT string_agg(
        format('%I.%I', actual.table_name, actual.column_name),
        ', '
        ORDER BY actual.table_name, actual.ordinal_position
    )
    INTO forbidden_columns
    FROM information_schema.columns actual
    LEFT JOIN maintainer_authorized_allowlist
      ON maintainer_authorized_allowlist.table_name = actual.table_name
     AND maintainer_authorized_allowlist.column_name = actual.column_name
    WHERE actual.table_schema = current_schema()
      AND maintainer_authorized_allowlist.column_name IS NULL
      AND (
          actual.column_name ~
              '(coverage|exhaustiveness|generation|revision)'
          OR actual.column_name ~ '(supersed|repair|capability)'
          OR actual.column_name ~
              '(fence|epoch|journal|promotion|reconciliation|rederive)'
          OR actual.column_name ~
              '(drift|alert|replay_version|dead_letter|watermark|staging)'
          OR actual.column_name = 'code_hash'
      );

    IF forbidden_columns IS NOT NULL THEN
        RAISE EXCEPTION 'forbidden schema-v2 columns: %', forbidden_columns;
    END IF;

    SELECT string_agg(table_name, ', ' ORDER BY table_name)
    INTO forbidden_projection_publication_tables
    FROM information_schema.tables
    WHERE table_schema = current_schema()
      AND table_type = 'BASE TABLE'
      AND table_name ~ '(_staging|_publication)$';

    IF forbidden_projection_publication_tables IS NOT NULL THEN
        RAISE EXCEPTION
            'schema-v2 contains forbidden projection publication tables: %',
            forbidden_projection_publication_tables;
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
        UNION ALL
        SELECT
            'chain lineage canonicality transitions are constrained',
            EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgrelid = 'chain_lineage'::regclass
                  AND tgname =
                      'chain_lineage_enforce_canonicality_transition'
                  AND NOT tgisinternal
            )
        UNION ALL
        SELECT
            format('%s carries its closed vocabulary', required.table_name),
            EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = to_regclass(required.table_name)
                  AND conname = required.constraint_name
                  AND contype = 'c'
            )
        FROM (
            VALUES
                (
                    'contract_instances',
                    'contract_instances_contract_kind_check'
                ),
                (
                    'discovery_edges',
                    'discovery_edges_edge_kind_check'
                ),
                (
                    'manifest_discovery_rules',
                    'manifest_discovery_rules_edge_kind_check'
                ),
                (
                    'surface_bindings',
                    'surface_bindings_binding_kind_check'
                ),
                ('name_current', 'name_current_binding_kind_check'),
                (
                    'address_names_current',
                    'address_names_current_binding_kind_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_event_kind_check'
                ),
                (
                    'normalized_events',
                    'normalized_events_derivation_kind_check'
                ),
                (
                    'permissions_current',
                    'permissions_current_scope_kind_check'
                ),
                (
                    'address_names_current',
                    'address_names_current_relation_check'
                )
        ) AS required(table_name, constraint_name)
        UNION ALL
        SELECT
            format('%s filters readable history', required.index_name),
            EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname = required.index_name
                  AND indexdef LIKE '% WHERE %'
                  AND indexdef LIKE '%canonicality_state%'
                  AND indexdef LIKE '%''canonical''%'
                  AND indexdef LIKE '%''safe''%'
                  AND indexdef LIKE '%''finalized''%'
            )
        FROM (
            VALUES
                ('surface_bindings_name_idx'),
                ('surface_bindings_resource_idx'),
                ('normalized_events_name_history_idx'),
                ('normalized_events_resource_history_idx')
        ) AS required(index_name)
        UNION ALL
        SELECT
            'normalized event state compaction has its expression index',
            EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'normalized_events_interpreter_state_history_idx'
                  AND indexdef LIKE '%raw_fact_ref%interpreter_state_key%'
                  AND indexdef LIKE '%canonicality_state%'
            )
        UNION ALL
        SELECT
            'name surface visibility requires an explicit decision',
            NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'name_surfaces'
                  AND column_name = 'visibility_state'
                  AND column_default IS NOT NULL
            )
        UNION ALL
        SELECT
            'manifest deployment label has an unambiguous storage name',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'manifest_versions'
                  AND column_name = 'deployment_label'
            )
            AND NOT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'manifest_versions'
                  AND column_name = 'deployment_id'
            )
        UNION ALL
        SELECT
            'record inventory has no primary-key duplicate index',
            NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND indexname =
                      'record_inventory_current_resource_idx'
            )
        UNION ALL
        SELECT
            'children raw labels preserve bytes with optional exact text',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'raw_label'
                  AND data_type = 'bytea'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'decoded_label'
                  AND data_type = 'text'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_label_matches_raw_check'
                  AND contype = 'c'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_label_requires_raw_check'
                  AND contype = 'c'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'labelhash'
                  AND is_nullable = 'NO'
            )
        UNION ALL
        SELECT
            'children raw names preserve bytes with optional exact text',
            EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'raw_name'
                  AND data_type = 'bytea'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'children_current'
                  AND column_name = 'decoded_name'
                  AND data_type = 'text'
                  AND is_nullable = 'YES'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_name_matches_raw_check'
                  AND contype = 'c'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'children_current'::regclass
                  AND conname =
                      'children_current_decoded_name_requires_raw_check'
                  AND contype = 'c'
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
    transition_case record;
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

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                ('observed', 'safe'),
                ('observed', 'finalized'),
                ('canonical', 'observed'),
                ('canonical', 'finalized'),
                ('safe', 'observed'),
                ('safe', 'canonical'),
                ('orphaned', 'observed'),
                ('orphaned', 'safe'),
                ('orphaned', 'finalized'),
                ('finalized', 'observed'),
                ('finalized', 'canonical'),
                ('finalized', 'safe'),
                ('finalized', 'orphaned')
        ) AS illegal(from_state, to_state)
    LOOP
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            format(
                'schema-v2-illegal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            ),
            'transition-block',
            0,
            '2026-01-01 00:00:00+00',
            transition_case.from_state::canonicality_state
        );

        BEGIN
            UPDATE chain_lineage
            SET canonicality_state =
                transition_case.to_state::canonicality_state
            WHERE chain_id = format(
                'schema-v2-illegal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            )
              AND block_hash = 'transition-block';
            RAISE EXCEPTION
                'chain_lineage accepted illegal canonicality transition % -> %',
                transition_case.from_state,
                transition_case.to_state;
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM <> format(
                    'illegal chain lineage canonicality transition: %s -> %s',
                    transition_case.from_state,
                    transition_case.to_state
                ) THEN
                    RAISE;
                END IF;
        END;

        DELETE FROM chain_lineage
        WHERE chain_id = format(
            'schema-v2-illegal-%s-%s',
            transition_case.from_state,
            transition_case.to_state
        )
          AND block_hash = 'transition-block';
    END LOOP;

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                ('observed', 'canonical'),
                ('observed', 'orphaned'),
                ('canonical', 'safe'),
                ('canonical', 'orphaned'),
                ('safe', 'finalized'),
                ('safe', 'orphaned'),
                ('orphaned', 'canonical')
        ) AS legal(from_state, to_state)
    LOOP
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES (
            format(
                'schema-v2-legal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            ),
            'transition-block',
            0,
            '2026-01-01 00:00:00+00',
            transition_case.from_state::canonicality_state
        );

        UPDATE chain_lineage
        SET canonicality_state =
            transition_case.to_state::canonicality_state
        WHERE chain_id = format(
            'schema-v2-legal-%s-%s',
            transition_case.from_state,
            transition_case.to_state
        )
          AND block_hash = 'transition-block';

        IF NOT EXISTS (
            SELECT 1
            FROM chain_lineage
            WHERE chain_id = format(
                'schema-v2-legal-%s-%s',
                transition_case.from_state,
                transition_case.to_state
            )
              AND block_hash = 'transition-block'
              AND canonicality_state =
                  transition_case.to_state::canonicality_state
        ) THEN
            RAISE EXCEPTION
                'chain_lineage did not apply legal canonicality transition % -> %',
                transition_case.from_state,
                transition_case.to_state;
        END IF;

        DELETE FROM chain_lineage
        WHERE chain_id = format(
            'schema-v2-legal-%s-%s',
            transition_case.from_state,
            transition_case.to_state
        )
          AND block_hash = 'transition-block';
    END LOOP;

    INSERT INTO chain_lineage (
        chain_id,
        block_hash,
        block_number,
        block_timestamp,
        canonicality_state,
        first_observed_at,
        canonicality_updated_at
    )
    VALUES (
        'schema-v2-transition-timestamp',
        'transition-block',
        0,
        '2026-01-01 00:00:00+00',
        'observed',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00'
    );

    UPDATE chain_lineage
    SET canonicality_state = 'canonical'
    WHERE chain_id = 'schema-v2-transition-timestamp'
      AND block_hash = 'transition-block';

    IF (
        SELECT canonicality_updated_at
        FROM chain_lineage
        WHERE chain_id = 'schema-v2-transition-timestamp'
          AND block_hash = 'transition-block'
    ) <= TIMESTAMPTZ '2026-01-01 00:00:00+00'
    THEN
        RAISE EXCEPTION
            'chain_lineage did not timestamp its canonicality transition';
    END IF;

    DELETE FROM chain_lineage
    WHERE chain_id = 'schema-v2-transition-timestamp'
      AND block_hash = 'transition-block';

    INSERT INTO chain_lineage (
        chain_id,
        block_hash,
        parent_hash,
        block_number,
        block_timestamp,
        canonicality_state
    )
    VALUES
        (
            'schema-v2-checkpoint-jump',
            'jump-block-0',
            NULL,
            0,
            '2026-01-01 00:00:00+00',
            'observed'
        ),
        (
            'schema-v2-checkpoint-jump',
            'jump-block-1',
            'jump-block-0',
            1,
            '2026-01-01 00:00:01+00',
            'observed'
        ),
        (
            'schema-v2-checkpoint-jump',
            'jump-block-2',
            'jump-block-1',
            2,
            '2026-01-01 00:00:02+00',
            'observed'
        ),
        (
            'schema-v2-checkpoint-jump',
            'jump-block-3',
            'jump-block-2',
            3,
            '2026-01-01 00:00:03+00',
            'observed'
        );

    UPDATE chain_lineage
    SET canonicality_state = 'canonical'
    WHERE chain_id = 'schema-v2-checkpoint-jump';

    UPDATE chain_lineage
    SET canonicality_state = 'safe'
    WHERE chain_id = 'schema-v2-checkpoint-jump'
      AND block_number <= 2;

    UPDATE chain_lineage
    SET canonicality_state = 'finalized'
    WHERE chain_id = 'schema-v2-checkpoint-jump'
      AND block_number <= 1;

    INSERT INTO chain_heads (
        chain_id,
        latest_block_hash,
        latest_block_number,
        safe_block_hash,
        safe_block_number,
        finalized_block_hash,
        finalized_block_number
    )
    VALUES (
        'schema-v2-checkpoint-jump',
        'jump-block-3',
        3,
        'jump-block-2',
        2,
        'jump-block-1',
        1
    );

    IF (
        SELECT array_agg(
            canonicality_state::text
            ORDER BY block_number
        )
        FROM chain_lineage
        WHERE chain_id = 'schema-v2-checkpoint-jump'
    ) <> ARRAY['finalized', 'finalized', 'safe', 'canonical']
    THEN
        RAISE EXCEPTION
            'ordered checkpoint jump did not preserve adjacent canonicality transitions';
    END IF;

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
        WHEN check_violation OR foreign_key_violation THEN
            IF SQLERRM <>
                'latest head must reference a canonical chain block'
            THEN
                RAISE;
            END IF;
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
        WHEN unique_violation THEN
            IF SQLERRM NOT LIKE
                '%chain_lineage_readable_height_idx%'
            THEN
                RAISE;
            END IF;
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
        WHEN check_violation OR foreign_key_violation THEN
            IF SQLERRM <>
                'a chain head still references this block state'
            THEN
                RAISE;
            END IF;
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

    INSERT INTO chain_phase_state (
        chain_id,
        phase_name
    )
    VALUES (
        'schema-v2-redo-failure',
        'interpret'
    );

    UPDATE chain_phase_state
    SET phase_status = 'running',
        redo_in_progress = true,
        redo_mode = 'redo',
        redo_previous_phase_status = 'idle',
        redo_from_block_number = 0,
        redo_to_block_number = 1,
        last_error = 'deterministic redo failure',
        started_at = now()
    WHERE chain_id = 'schema-v2-redo-failure'
      AND phase_name = 'interpret';

    BEGIN
        INSERT INTO chain_phase_state (
            chain_id,
            phase_name,
            phase_status,
            last_error,
            started_at
        )
        VALUES (
            'schema-v2-invalid-running-error',
            'interpret',
            'running',
            'invalid normal-run error',
            now()
        );
        RAISE EXCEPTION
            'a normal running phase accepted last_error';
    EXCEPTION
        WHEN check_violation THEN
            NULL;
    END;

    DELETE FROM chain_phase_state
    WHERE chain_id = 'schema-v2-redo-failure'
      AND phase_name = 'interpret';

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
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "chain_phase_state_verification_phase_check"%'
            THEN
                RAISE;
            END IF;
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "raw_receipts_transaction_position_fkey"%'
            THEN
                RAISE;
            END IF;
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "raw_logs_transaction_position_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000001',
        'schema-v2-check',
        'root'
    );

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000002',
        'schema-v2-check',
        'contract'
    );

    INSERT INTO contract_instances (
        contract_instance_id,
        chain_id,
        contract_kind
    )
    VALUES (
        '00000000-0000-0000-0000-000000000003',
        'schema-v2-other',
        'root'
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "contract_instance_addresses_instance_chain_fkey"%'
            THEN
                RAISE;
            END IF;
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
            'proxy_implementation',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000002',
            'schema-v2-check',
            'schema-v2-check'
        );
        RAISE EXCEPTION
            'a discovery edge accepted different endpoint chains';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "discovery_edges_from_instance_chain_fkey"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO discovery_edges (
        chain_id,
        edge_kind,
        from_contract_instance_id,
        to_contract_instance_id,
        discovery_source,
        admission_basis
    )
    VALUES (
        'schema-v2-check',
        'registry_announcement',
        '00000000-0000-0000-0000-000000000001',
        '00000000-0000-0000-0000-000000000001',
        'RegistryCreated',
        'schema-v2-check'
    );

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
            'schema-v2-check',
            'resolver',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000001',
            'schema-v2-check',
            'schema-v2-check'
        );
        RAISE EXCEPTION
            'a non-announcement discovery edge accepted equal endpoints';
    EXCEPTION
        WHEN check_violation THEN
            NULL;
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
        WHEN exclusion_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "contract_instance_addresses_no_overlap"%'
            THEN
                RAISE;
            END IF;
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
        WHEN unique_violation OR exclusion_violation THEN
            IF SQLERRM NOT LIKE
                '%contract_instance_addresses_active_instance_idx%'
                AND SQLERRM NOT LIKE
                    '%constraint "contract_instance_addresses_no_overlap"%'
            THEN
                RAISE;
            END IF;
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
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:namehash-missing-visibility',
            'schema-v2-check',
            'Missing',
            ARRAY['Missing'],
            '\x074d697373696e6700',
            'namehash-missing-visibility',
            ARRAY['labelhash-missing-visibility'],
            'check',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted an omitted visibility decision';
    EXCEPTION
        WHEN not_null_violation THEN
            IF SQLERRM NOT LIKE
                '%null value in column "visibility_state"%'
            THEN
                RAISE EXCEPTION
                    'name_surfaces omitted visibility failed with unexpected message: %',
                    SQLERRM;
            END IF;
    END;

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
        normalization_errors,
        deactivation_reason,
        deactivated_at,
        chain_id,
        block_hash,
        block_number,
        canonicality_state
    )
    VALUES (
        'schema-v2-check:namehash-shadow',
        'schema-v2-check',
        'Shadow',
        ARRAY['Shadow'],
        '\x06536861646f7700',
        'namehash-shadow',
        ARRAY['labelhash-shadow'],
        'check',
        'shadow',
        '["normalization failed"]'::jsonb,
        'normalization failed',
        '2026-01-01 00:00:01+00',
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
            normalization_errors,
            chain_id,
            block_hash,
            block_number,
            canonicality_state
        )
        VALUES (
            'schema-v2-check:namehash-invalid-active',
            'schema-v2-check',
            'Invalid Active',
            ARRAY['Invalid Active'],
            '\x0e496e76616c69642041637469766500',
            'namehash-invalid-active',
            ARRAY['labelhash-invalid-active'],
            'check',
            'active',
            '["unexpected"]'::jsonb,
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted active normalization errors';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_surfaces_visibility_coherence_check"%'
            THEN
                RAISE;
            END IF;
    END;

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
            'schema-v2-check:namehash-invalid-shadow',
            'schema-v2-check',
            'Invalid Shadow',
            ARRAY['Invalid Shadow'],
            '\x0e496e76616c696420536861646f7700',
            'namehash-invalid-shadow',
            ARRAY['labelhash-invalid-shadow'],
            'check',
            'shadow',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'name_surfaces accepted an incomplete shadow state';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_surfaces_visibility_coherence_check"%'
            THEN
                RAISE;
            END IF;
    END;

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
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_surfaces_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "resources_chain_id_token_lineage_id_fkey"%'
            THEN
                RAISE;
            END IF;
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
            'declared_registry_path',
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_chain_id_logical_name_id_fkey"%'
            THEN
                RAISE;
            END IF;
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
            'declared_registry_path',
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_chain_id_resource_id_fkey"%'
            THEN
                RAISE;
            END IF;
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
            'ResolverChanged',
            'check',
            1,
            'schema-v2-other',
            'ens_v2_resolver'
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'normalized_events.logical_name_id'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'cross-chain-name-event';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_chain_id_logical_name_id_fkey"%'
            THEN
                RAISE;
            END IF;
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
            'ResolverChanged',
            'check',
            1,
            'schema-v2-other',
            'ens_v2_resolver'
        );
        accepted_cross_chain_relationships :=
            array_append(
                accepted_cross_chain_relationships,
                'normalized_events.resource_id'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'cross-chain-resource-event';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_chain_id_resource_id_fkey"%'
            THEN
                RAISE;
            END IF;
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
        WHEN check_violation THEN
            IF SQLERRM <> 'chain lineage block identity is immutable' THEN
                RAISE;
            END IF;
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
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "resolution_divergences_clearing_time_check"%'
            THEN
                RAISE;
            END IF;
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
        WHEN foreign_key_violation THEN
            IF SQLERRM <>
                'active resolution difference position resolver is not canonical'
            THEN
                RAISE;
            END IF;
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
        WHEN unique_violation THEN
            IF SQLERRM NOT LIKE
                '%resolution_divergences_one_active_request_idx%'
            THEN
                RAISE;
            END IF;
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
        'declared_registry_path',
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
        'declared_registry_path',
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
            'declared_registry_path',
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_surface_binding_id_logical_name_id_resource_i_fkey"%'
            THEN
                RAISE;
            END IF;
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
            'registrant',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000025',
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_surface_binding_id_logical_name_id_r_fkey"%'
            THEN
                RAISE;
            END IF;
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
            'declared_registry_path',
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_resource_id_token_lineage_id_fkey"%'
            THEN
                RAISE;
            END IF;
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
            'registrant',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000032',
            'declared_registry_path',
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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_resource_id_token_lineage_id_fkey"%'
            THEN
                RAISE;
            END IF;
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
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            raw_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-namehash',
            'wrong-namespace',
            convert_to('Child.Name', 'UTF8'),
            convert_to('Child', 'UTF8'),
            'wrong-namehash',
            'wrong-labelhash',
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
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO children_current (
        parent_logical_name_id,
        child_logical_name_id,
        namespace,
        raw_name,
        decoded_name,
        raw_label,
        decoded_label,
        namehash,
        labelhash,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check:child-clean',
        'schema-v2-check',
        convert_to('Clean.Name', 'UTF8'),
        'Clean.Name',
        convert_to('Clean', 'UTF8'),
        'Clean',
        'child-clean',
        'labelhash-child-clean',
        1
    ), (
        'schema-v2-check:namehash-0',
        'schema-v2-check:child-hostile',
        'schema-v2-check',
        decode('ff002e4e616d65', 'hex'),
        NULL,
        decode('ff00', 'hex'),
        NULL,
        'child-hostile',
        'labelhash-child-hostile',
        1
    );

    INSERT INTO children_current (
        parent_logical_name_id,
        child_logical_name_id,
        namespace,
        raw_name,
        decoded_name,
        raw_label,
        decoded_label,
        namehash,
        labelhash,
        manifest_version
    )
    VALUES (
        'schema-v2-check:namehash-0',
        'schema-v2-check:child-topology-only',
        'schema-v2-check',
        NULL,
        NULL,
        NULL,
        NULL,
        'child-topology-only',
        'labelhash-topology-only',
        1
    );

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-decoded-label-without-raw',
            'schema-v2-check',
            NULL,
            NULL,
            NULL,
            'Synthesized',
            'child-decoded-label-without-raw',
            'labelhash-decoded-label-without-raw',
            1
        );
        RAISE EXCEPTION
            'children_current accepted a decoded label without raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_label_requires_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-decoded-name-without-raw',
            'schema-v2-check',
            NULL,
            'Synthesized.Name',
            NULL,
            NULL,
            'child-decoded-name-without-raw',
            'labelhash-decoded-name-without-raw',
            1
        );
        RAISE EXCEPTION
            'children_current accepted a decoded name without raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_name_requires_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-decoding-drift',
            'schema-v2-check',
            convert_to('Raw.Name', 'UTF8'),
            'Raw.Name',
            convert_to('raw', 'UTF8'),
            'different',
            'child-decoding-drift',
            'labelhash-child-decoding-drift',
            1
        );
        RAISE EXCEPTION
            'children_current accepted decoded text that differs from raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_label_matches_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO children_current (
            parent_logical_name_id,
            child_logical_name_id,
            namespace,
            raw_name,
            decoded_name,
            raw_label,
            decoded_label,
            namehash,
            labelhash,
            manifest_version
        )
        VALUES (
            'schema-v2-check:namehash-0',
            'schema-v2-check:child-name-decoding-drift',
            'schema-v2-check',
            convert_to('Raw.Name', 'UTF8'),
            'Different.Name',
            convert_to('Raw', 'UTF8'),
            'Raw',
            'child-name-decoding-drift',
            'labelhash-child-name-decoding-drift',
            1
        );
        RAISE EXCEPTION
            'children_current accepted decoded name text that differs from raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "children_current_decoded_name_matches_raw_check"%'
            THEN
                RAISE;
            END IF;
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
            'registrant',
            'wrong-namespace',
            'Name',
            'wrong-namehash',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared_registry_path',
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
          AND relation = 'registrant';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_logical_identity_check"%'
            THEN
                RAISE;
            END IF;
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
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind
        )
        VALUES (
            '00000000-0000-0000-0000-000000000099',
            'schema-v2-check',
            'registry'
        );
        RAISE EXCEPTION
            'contract_instances accepted an unknown contract kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "contract_instances_contract_kind_check"%'
            THEN
                RAISE;
            END IF;
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
            'schema-v2-check',
            'proxy',
            '00000000-0000-0000-0000-000000000001',
            '00000000-0000-0000-0000-000000000002',
            'schema-check',
            'schema-check'
        );
        RAISE EXCEPTION
            'discovery_edges accepted an unknown edge kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "discovery_edges_edge_kind_check"%'
            THEN
                RAISE;
            END IF;
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
            '00000000-0000-0000-0000-000000000029',
            'schema-v2-check:namehash-0',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            '2025-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0
        );
        RAISE EXCEPTION
            'surface_bindings accepted an unknown binding kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_binding_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'invalid-event-kind',
            'schema-v2-check',
            'Check',
            'schema-check',
            1,
            'schema-v2-check',
            'ens_v2_resolver'
        );
        RAISE EXCEPTION
            'normalized_events accepted an unknown event kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_event_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO normalized_events (
        event_identity,
        namespace,
        event_kind,
        source_family,
        manifest_version,
        chain_id,
        derivation_kind
    )
    SELECT
        'valid-derivation-kind-' || derivation_kind,
        'schema-v2-check',
        'ResolverChanged',
        'schema-check',
        1,
        'schema-v2-check',
        derivation_kind
    FROM unnest(
        ARRAY[
            'ens_v1_reverse_claim',
            'ens_v1_unwrapped_authority',
            'ens_v2_permissions',
            'ens_v2_registrar',
            'ens_v2_registry_resource_surface',
            'ens_v2_resolver',
            'manifest_sync',
            'proxy_upgrade',
            'raw_log_preimage_observation'
        ]
    ) AS admitted(derivation_kind);

    BEGIN
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            derivation_kind
        )
        VALUES (
            'invalid-derivation-kind',
            'schema-v2-check',
            'ResolverChanged',
            'schema-check',
            1,
            'schema-v2-check',
            'check'
        );
        RAISE EXCEPTION
            'normalized_events accepted an unknown derivation kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_derivation_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO permissions_current (
            resource_id,
            subject,
            scope,
            scope_kind,
            manifest_version
        )
        VALUES (
            '00000000-0000-0000-0000-000000000011',
            'schema-check-subject',
            'schema-check-scope',
            'check',
            1
        );
        RAISE EXCEPTION
            'permissions_current accepted an unknown scope kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "permissions_current_scope_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    BEGIN
        INSERT INTO name_current (
            logical_name_id,
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
            'schema-v2-check:namehash-0',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            'supported',
            1
        );
        RAISE EXCEPTION
            'name_current accepted an unknown binding kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "name_current_binding_kind_check"%'
            THEN
                RAISE;
            END IF;
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
            'invalid-relation',
            'schema-v2-check:namehash-0',
            'owner',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared_registry_path',
            'supported',
            1
        );
        RAISE EXCEPTION
            'address_names_current accepted an unknown relation';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_relation_check"%'
            THEN
                RAISE;
            END IF;
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
            'invalid-binding-kind',
            'schema-v2-check:namehash-0',
            'registrant',
            'schema-v2-check',
            'Name',
            'namehash-0',
            '00000000-0000-0000-0000-000000000021',
            '00000000-0000-0000-0000-000000000011',
            'declared',
            'supported',
            1
        );
        RAISE EXCEPTION
            'address_names_current accepted an unknown binding kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "address_names_current_binding_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO label_preimages (
        labelhash,
        raw_label,
        decoded_label,
        normalizer_version,
        normalized_under_version,
        normalization_error,
        source_kind,
        source_priority
    )
    VALUES
        (
            'labelhash-normalized',
            convert_to('normalized', 'UTF8'),
            'normalized',
            'check',
            true,
            NULL,
            'schema-check',
            0
        ),
        (
            'labelhash-rejected',
            decode('ff00', 'hex'),
            NULL,
            'check',
            false,
            'normalization failed',
            'schema-check',
            0
        );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    'labelhash-invalid-success',
                    true,
                    'unexpected error'::text
                ),
                (
                    'labelhash-invalid-failure',
                    false,
                    NULL::text
                )
        ) AS invalid(labelhash, normalized, normalization_error)
    LOOP
        BEGIN
            INSERT INTO label_preimages (
                labelhash,
                raw_label,
                decoded_label,
                normalizer_version,
                normalized_under_version,
                normalization_error,
                source_kind,
                source_priority
            )
            VALUES (
                transition_case.labelhash,
                convert_to('invalid', 'UTF8'),
                'invalid',
                'check',
                transition_case.normalized,
                transition_case.normalization_error,
                'schema-check',
                0
            );
            RAISE EXCEPTION
                'label_preimages accepted incoherent normalization state';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE
                    '%constraint "label_preimages_normalization_coherence_check"%'
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    BEGIN
        INSERT INTO label_preimages (
            labelhash,
            raw_label,
            decoded_label,
            normalizer_version,
            normalized_under_version,
            normalization_error,
            source_kind,
            source_priority
        )
        VALUES (
            'labelhash-decoding-drift',
            convert_to('raw', 'UTF8'),
            'different',
            'check',
            false,
            'normalization failed',
            'schema-check',
            0
        );
        RAISE EXCEPTION
            'label_preimages accepted decoded text that differs from raw bytes';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "label_preimages_decoded_label_matches_raw_check"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO primary_names_current (
        address,
        coin_type,
        namespace,
        claim_status,
        raw_claim_name,
        claim_name_is_normalized,
        unsupported_reason
    )
    VALUES
        (
            'primary-success',
            '60',
            'schema-v2-check',
            'success',
            'name.eth',
            true,
            NULL
        ),
        (
            'primary-not-found',
            '60',
            'schema-v2-check',
            'not_found',
            NULL,
            false,
            NULL
        ),
        (
            'primary-unsupported',
            '60',
            'schema-v2-check',
            'unsupported',
            NULL,
            false,
            'coin type unsupported'
        ),
        (
            'primary-invalid-name',
            '60',
            'schema-v2-check',
            'invalid_name',
            'invalid name',
            false,
            NULL
        );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    'primary-missing-name',
                    'success',
                    NULL::text,
                    false,
                    NULL::text,
                    'primary_names_current_claim_name_check'
                ),
                (
                    'primary-unexpected-name',
                    'not_found',
                    'name.eth',
                    false,
                    NULL::text,
                    'primary_names_current_claim_name_check'
                ),
                (
                    'primary-invalid-normalized',
                    'invalid_name',
                    'invalid name',
                    true,
                    NULL::text,
                    'primary_names_current_normalized_claim_check'
                ),
                (
                    'primary-missing-reason',
                    'unsupported',
                    NULL::text,
                    false,
                    NULL::text,
                    'primary_names_current_unsupported_reason_coherence_check'
                ),
                (
                    'primary-unexpected-reason',
                    'success',
                    'name.eth',
                    true,
                    'unexpected reason',
                    'primary_names_current_unsupported_reason_coherence_check'
                )
        ) AS invalid(
            address,
            claim_status,
            raw_claim_name,
            claim_name_is_normalized,
            unsupported_reason,
            constraint_name
        )
    LOOP
        BEGIN
            INSERT INTO primary_names_current (
                address,
                coin_type,
                namespace,
                claim_status,
                raw_claim_name,
                claim_name_is_normalized,
                unsupported_reason
            )
            VALUES (
                transition_case.address,
                '60',
                'schema-v2-check',
                transition_case.claim_status,
                transition_case.raw_claim_name,
                transition_case.claim_name_is_normalized,
                transition_case.unsupported_reason
            );
            RAISE EXCEPTION
                'primary_names_current accepted incoherent claim state';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE format(
                    '%%constraint "%s"%%',
                    transition_case.constraint_name
                ) THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    INSERT INTO ingest_cursors (
        chain_id,
        source_key,
        source_kind,
        seed_basis,
        start_block_number,
        next_block_number,
        target_block_number,
        last_processed_block_number,
        last_processed_block_hash
    )
    VALUES (
        'schema-v2-check',
        'valid-source',
        'logs',
        'base_seam',
        10,
        11,
        12,
        10,
        'processed-block-10'
    );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    'invalid-next',
                    10::bigint,
                    9::bigint,
                    NULL::bigint,
                    NULL::bigint,
                    NULL::text,
                    'ingest_cursors_next_block_order_check'
                ),
                (
                    'invalid-target',
                    10::bigint,
                    10::bigint,
                    9::bigint,
                    NULL::bigint,
                    NULL::text,
                    'ingest_cursors_target_block_order_check'
                ),
                (
                    'invalid-last-pair',
                    10::bigint,
                    11::bigint,
                    NULL::bigint,
                    10::bigint,
                    NULL::text,
                    'ingest_cursors_last_processed_pair_check'
                ),
                (
                    'invalid-last-order',
                    10::bigint,
                    11::bigint,
                    NULL::bigint,
                    11::bigint,
                    'processed-block-11',
                    'ingest_cursors_last_processed_order_check'
                )
        ) AS invalid(
            source_key,
            start_block_number,
            next_block_number,
            target_block_number,
            last_processed_block_number,
            last_processed_block_hash,
            constraint_name
        )
    LOOP
        BEGIN
            INSERT INTO ingest_cursors (
                chain_id,
                source_key,
                source_kind,
                seed_basis,
                start_block_number,
                next_block_number,
                target_block_number,
                last_processed_block_number,
                last_processed_block_hash
            )
            VALUES (
                'schema-v2-check',
                transition_case.source_key,
                'logs',
                'base_seam',
                transition_case.start_block_number,
                transition_case.next_block_number,
                transition_case.target_block_number,
                transition_case.last_processed_block_number,
                transition_case.last_processed_block_hash
            );
            RAISE EXCEPTION
                'ingest_cursors accepted invalid ordering';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE format(
                    '%%constraint "%s"%%',
                    transition_case.constraint_name
                ) THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    INSERT INTO service_heartbeats (
        service_name,
        instance_id,
        chain_id,
        phase_name,
        started_at,
        heartbeat_at
    )
    VALUES (
        'indexer',
        'instance-valid',
        'schema-v2-check',
        'project',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:01+00'
    );

    FOR transition_case IN
        SELECT *
        FROM (
            VALUES
                (
                    '',
                    'instance-invalid-service',
                    'schema-v2-check',
                    'project',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_service_name_check'
                ),
                (
                    'indexer',
                    '',
                    'schema-v2-check',
                    'project',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_instance_id_check'
                ),
                (
                    'indexer',
                    'instance-invalid-chain',
                    '',
                    'project',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_chain_id_check'
                ),
                (
                    'indexer',
                    'instance-invalid-phase',
                    'schema-v2-check',
                    'publish',
                    '2026-01-01 00:00:00+00'::timestamptz,
                    '2026-01-01 00:00:01+00'::timestamptz,
                    'service_heartbeats_phase_name_check'
                ),
                (
                    'indexer',
                    'instance-invalid-time',
                    'schema-v2-check',
                    'project',
                    '2026-01-01 00:00:01+00'::timestamptz,
                    '2026-01-01 00:00:00+00'::timestamptz,
                    'service_heartbeats_time_order_check'
                )
        ) AS invalid(
            service_name,
            instance_id,
            chain_id,
            phase_name,
            started_at,
            heartbeat_at,
            constraint_name
        )
    LOOP
        BEGIN
            INSERT INTO service_heartbeats (
                service_name,
                instance_id,
                chain_id,
                phase_name,
                started_at,
                heartbeat_at
            )
            VALUES (
                transition_case.service_name,
                transition_case.instance_id,
                transition_case.chain_id,
                transition_case.phase_name,
                transition_case.started_at,
                transition_case.heartbeat_at
            );
            RAISE EXCEPTION
                'service_heartbeats accepted an invalid row shape';
        EXCEPTION
            WHEN check_violation THEN
                IF SQLERRM NOT LIKE format(
                    '%%constraint "%s"%%',
                    transition_case.constraint_name
                ) THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

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
            'declared_registry_path',
            '2026-01-01 00:00:00+00',
            'schema-v2-check',
            'block-0',
            0,
            'canonical'
        );
        RAISE EXCEPTION
            'surface_bindings accepted overlapping canonical ranges';
    EXCEPTION
        WHEN exclusion_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "surface_bindings_no_overlap"%'
            THEN
                RAISE;
            END IF;
    END;

    INSERT INTO manifest_versions (
        manifest_version,
        namespace,
        source_family,
        chain_id,
        deployment_label,
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
        INSERT INTO manifest_discovery_rules (
            manifest_id,
            edge_kind,
            admission
        )
        VALUES (manifest_key, 'announced', 'declared');
        RAISE EXCEPTION
            'manifest_discovery_rules accepted an unknown edge kind';
    EXCEPTION
        WHEN check_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "manifest_discovery_rules_edge_kind_check"%'
            THEN
                RAISE;
            END IF;
    END;

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
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "manifest_contract_instances_chain_id_contract_instance_id_fkey"%'
            THEN
                RAISE;
            END IF;
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
            'ResolverChanged',
            'wrong-source-family',
            99,
            manifest_key,
            'schema-v2-other',
            'ens_v2_resolver'
        );
        accepted_manifest_mismatches :=
            array_append(
                accepted_manifest_mismatches,
                'normalized event manifest'
            );
        DELETE FROM normalized_events
        WHERE event_identity = 'mismatched-manifest-event';
    EXCEPTION
        WHEN foreign_key_violation THEN
            IF SQLERRM NOT LIKE
                '%constraint "normalized_events_source_manifest_id_namespace_source_fami_fkey"%'
            THEN
                RAISE;
            END IF;
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
    VALUES (manifest_key, 'resolver', 'declared');

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
        'proxy_implementation',
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
        'ResolverChanged',
        'schema-v2-check',
        1,
        manifest_key,
        'schema-v2-check',
        'ens_v2_resolver'
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
