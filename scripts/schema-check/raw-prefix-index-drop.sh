# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261112160000_name_surfaces_drop_raw_prefix_index.sql and
# 20261028120000_serving_read_indexes.sql, in a schema of the current fresh baseline. The
# baseline does not define the index, and neither schema-migration builds it from a schema
# without it. The drop removes exactly the hand-built
# index, valid or left invalid, built concurrently as the operator built it, and a rerun changes
# nothing. It leaves the caller's search_path, quote_all_identifiers and lock_timeout as it found
# them. It refuses, and so never drops, an index of another definition, an index of the name on
# another table and a relation that is not an index, and it gives up on a table lock it cannot
# take in five seconds. A refusal rolls back in the helper, so the index it names survives.
raw_prefix_index=name_surfaces_name_prefix_idx
raw_prefix_index_build="CREATE INDEX CONCURRENTLY IF NOT EXISTS $raw_prefix_index
    ON name_surfaces (raw_name text_pattern_ops, namespace, namehash, logical_name_id)
    WHERE visibility_state = 'active'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND octet_length(raw_name) <= 2000;"
raw_prefix_index_build_in_transaction="${raw_prefix_index_build/ CONCURRENTLY/}"
raw_prefix_index_recovery="follow the recovery steps in docs/deployment.md, then run the schema-migrations again"
# Fails unless the index is gone and every other index on the table is as the baseline left it.
raw_prefix_index_gone="DO \$\$
BEGIN
    IF to_regclass('$raw_prefix_index') IS NOT NULL THEN
        RAISE EXCEPTION '$raw_prefix_index is still in place';
    END IF;
    IF EXISTS (
        (SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = 'name_surfaces'::regclass
         EXCEPT SELECT definition FROM raw_prefix_index_kept)
        UNION ALL
        (SELECT definition FROM raw_prefix_index_kept
         EXCEPT
         SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid = 'name_surfaces'::regclass)
    ) THEN
        RAISE EXCEPTION 'name_surfaces indexes differ from the baseline';
    END IF;
END \$\$;"
raw_prefix_index_session() {
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
}
# Print the definition of the index that $1 creates as the schema-migration reads it, with
# search_path pg_catalog so the table carries the schema name.
raw_prefix_index_printed_definition() {
    {
        printf 'BEGIN;\nSET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned' "$1" \
            'SET LOCAL search_path TO pg_catalog;' \
            "SELECT pg_get_indexdef('$scratch_schema.$raw_prefix_index'::regclass);" 'ROLLBACK;'
    } | run_psql
}
raw_prefix_index_case_not_in_baseline() {
    {
        raw_prefix_index_session
        cat <<SQL
DO \$\$ BEGIN
    IF to_regclass('$raw_prefix_index') IS NOT NULL THEN
        RAISE EXCEPTION 'the fresh baseline defines $raw_prefix_index';
    END IF;
END \$\$;
SQL
    } | run_psql
}
raw_prefix_index_case_absent_skipped() {
    {
        raw_prefix_index_session
        emit_phase_migration "$raw_prefix_index_drop_migration" baseline-first
        printf '%s\n' "$raw_prefix_index_gone"
    } | run_psql
}
raw_prefix_index_case_serving_read_builds_nothing() {
    {
        raw_prefix_index_session
        emit_phase_migration "$serving_read_index_migration" baseline-first
        printf '%s\n' "$raw_prefix_index_gone"
    } | run_psql
}
raw_prefix_index_case_hand_built_dropped() {
    {
        raw_prefix_index_session
        printf '%s\n' "$raw_prefix_index_build"
        emit_phase_migration "$raw_prefix_index_drop_migration" preceding-shape
        printf '%s\n' "$raw_prefix_index_gone"
        emit_phase_migration "$raw_prefix_index_drop_migration" baseline-first
        printf '%s\n' "$raw_prefix_index_gone"
    } | run_psql
}
raw_prefix_index_case_invalid_dropped() {
    {
        raw_prefix_index_session
        printf '%s\n' 'BEGIN;' "$raw_prefix_index_build_in_transaction"
        printf '%s\n' "UPDATE pg_index SET indisvalid = false, indisready = false WHERE indexrelid = '$raw_prefix_index'::regclass;"
        emit_phase_migration "$raw_prefix_index_drop_migration" specialized
        printf '%s\n' "$raw_prefix_index_gone" 'ROLLBACK;'
    } | run_psql
}
raw_prefix_index_case_settings_restored() {
    {
        printf 'BEGIN;\nSET LOCAL search_path TO "%s", public;\n' "$scratch_schema"
        printf '%s\n' "SET LOCAL lock_timeout = '7s';" 'SET LOCAL quote_all_identifiers = on;' \
            "$raw_prefix_index_build_in_transaction"
        emit_phase_migration "$raw_prefix_index_drop_migration" specialized
        assert_search_path_sql "$scratch_schema, public"
        assert_quote_all_identifiers_sql on
        cat <<SQL
DO \$\$ BEGIN
    IF current_setting('lock_timeout') <> '7s' THEN
        RAISE EXCEPTION 'the drop left lock_timeout at %', current_setting('lock_timeout');
    END IF;
END \$\$;
SET LOCAL quote_all_identifiers = off;
SQL
        printf '%s\n' "$raw_prefix_index_gone" 'ROLLBACK;'
    } | run_psql
}
raw_prefix_index_case_other_definition_refused() {
    # The hand-built keys without its predicate, which a check of the name or keys would accept.
    local create="CREATE INDEX $raw_prefix_index ON name_surfaces (raw_name text_pattern_ops, namespace, namehash, logical_name_id);"
    local found expected
    found="$(raw_prefix_index_printed_definition "$create")"
    expected="$(raw_prefix_index_printed_definition "$raw_prefix_index_build_in_transaction")"
    assert_migration_refusal raw-prefix-index-other-definition "$raw_prefix_index_drop_migration" \
        "$scratch_schema.$raw_prefix_index exists but does not have the hand-built definition, so it is not dropped; found \"$found\", expected \"$expected\"; $raw_prefix_index_recovery" <<SQL
$create
SQL
}
raw_prefix_index_case_other_table_refused() {
    local create="CREATE INDEX $raw_prefix_index ON discovery_edges (chain_id);"
    local found expected
    found="$(raw_prefix_index_printed_definition "$create")"
    expected="$(raw_prefix_index_printed_definition "$raw_prefix_index_build_in_transaction")"
    assert_migration_refusal raw-prefix-index-other-table "$raw_prefix_index_drop_migration" \
        "$scratch_schema.$raw_prefix_index exists but does not have the hand-built definition, so it is not dropped; found \"$found\", expected \"$expected\"; $raw_prefix_index_recovery" <<SQL
$create
SQL
}
raw_prefix_index_case_table_refused() {
    assert_migration_refusal raw-prefix-index-table-named "$raw_prefix_index_drop_migration" \
        "$scratch_schema.$raw_prefix_index is not an index; it is not dropped" <<SQL
CREATE TABLE $raw_prefix_index ();
SQL
}
# A transaction that holds name_surfaces makes the drop wait. The schema-migration gives up
# after its own five-second lock_timeout, whatever the caller's setting is.
raw_prefix_index_case_lock_timeout() {
    local application="schema_v2_raw_prefix_lock_${PPID}_$$"
    schema_v2_race_log="$(mktemp)"
    {
        raw_prefix_index_session
        printf "SET application_name TO '%s';\n" "$application"
        printf '%s\n' 'BEGIN;' 'LOCK TABLE name_surfaces IN ACCESS SHARE MODE;' \
            'SELECT pg_sleep(60);' 'ROLLBACK;'
    } | run_psql >"$schema_v2_race_log" 2>&1 &
    schema_v2_race_pid=$!
    if ! wait_for_schema_v2_race_session "$application"; then
        printf '%s\n' "the session holding name_surfaces did not become ready" >&2
        cat "$schema_v2_race_log" >&2
        exit 1
    fi
    assert_migration_refusal raw-prefix-index-lock-timeout "$raw_prefix_index_drop_migration" \
        "canceling statement due to lock timeout" <<SQL
SET LOCAL lock_timeout = 0;
$raw_prefix_index_build_in_transaction
SQL
    printf "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = '%s';\n" \
        "$application" | run_psql >/dev/null
    wait "$schema_v2_race_pid" >/dev/null 2>&1 || true
    schema_v2_race_pid=""
    rm -f -- "$schema_v2_race_log"
    schema_v2_race_log=""
}
check_raw_prefix_index_drop() {
    local original_schema="$scratch_schema"
    local scratch_schema="${original_schema}_fresh"
    local sql_file case_name
    {
        printf 'SET client_min_messages TO warning;\n'
        printf 'CREATE SCHEMA "%s";\n' "$scratch_schema"
        raw_prefix_index_session
        for sql_file in "$ROOT"/crates/storage/schema/baseline/*.sql; do
            render_phase_migration "$sql_file"
        done
        cat <<SQL
CREATE TABLE raw_prefix_index_kept AS
SELECT pg_get_indexdef(indexrelid) AS definition
FROM pg_index WHERE indrelid = 'name_surfaces'::regclass;
SQL
    } | run_psql
    for case_name in \
        not_in_baseline \
        absent_skipped \
        serving_read_builds_nothing \
        hand_built_dropped \
        invalid_dropped \
        settings_restored \
        other_definition_refused \
        other_table_refused \
        table_refused \
        lock_timeout; do
        "raw_prefix_index_case_$case_name"
    done
    printf 'DROP SCHEMA "%s" CASCADE;\n' "$scratch_schema" | run_psql
}
check_raw_prefix_index_drop
assert_migration_context_count "$raw_prefix_index_drop_migration" empty-schema 1
assert_migration_context_count "$raw_prefix_index_drop_migration" preceding-shape 1
assert_migration_context_count "$raw_prefix_index_drop_migration" specialized 2
