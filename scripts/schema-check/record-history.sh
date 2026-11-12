# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261111120000_normalized_events_record_history_indexes.sql must build the baseline's two
# definitions on an empty table, refuse a populated table without the prebuilds, adopt the
# ops/record-history-indexes/install.sql prebuilds by OID with every row unchanged, rerun as a
# no-op, leave the caller's search_path and quote_all_identifiers as it found them, and refuse
# every shape CREATE INDEX IF NOT EXISTS would skip. install.sql must refuse an invalid index
# under one of its names.
record_history_migration="$ROOT/migrations/20261111120000_normalized_events_record_history_indexes.sql"
record_history_install="$ROOT/ops/record-history-indexes/install.sql"
record_history_readme=ops/record-history-indexes/README.md
record_history_key_index=normalized_events_record_key_history_idx
record_history_emitter_index=normalized_events_record_emitter_key_history_idx
record_history_indexes="$record_history_key_index $record_history_emitter_index"
record_history_names="'$record_history_key_index', '$record_history_emitter_index'"
record_history_expected="CREATE TEMP TABLE expected_record_history_indexes AS
SELECT relname::text AS index_name, pg_get_indexdef(oid) AS definition
FROM pg_class
WHERE relname IN ($record_history_names) AND relnamespace = '$scratch_schema'::regnamespace;"
record_history_matches_baseline="(SELECT count(*) FROM expected_record_history_indexes) = 2 AND NOT EXISTS (
    SELECT 1 FROM expected_record_history_indexes expected
    WHERE pg_get_indexdef(to_regclass(expected.index_name)) IS DISTINCT FROM expected.definition)"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "$record_history_expected" \
        "BEGIN; DELETE FROM normalized_events; DROP INDEX $record_history_key_index, $record_history_emitter_index;"
    emit_phase_migration "$record_history_migration" preceding-shape
    emit_phase_migration "$record_history_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF NOT ($record_history_matches_baseline) THEN
        RAISE EXCEPTION 'record history indexes empty upgrade differs from baseline';
    END IF;
END \$\$;
ROLLBACK;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$record_history_migration" baseline-first
    assert_search_path_sql public
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_quote_all_identifiers_probe "$record_history_migration" in-transaction
    assert_search_path_sql "$scratch_schema"
} | run_psql
# A valid stored event makes this a populated released predecessor. Refusal must preserve data
# and the surrounding transaction. The helper rolls the mutation back.
record_history_seed="INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind)
VALUES ('record-history-upgrade-proof', 'schema-v2-check', 'ResolverRecordLinked', 'ens_v2_resolver_l1',
    1, 'schema-v2-check', 'ens_v2_resolver');"
for record_history_index in $record_history_indexes; do
    assert_migration_refusal "record-history-missing-populated-prebuild-$record_history_index" \
        "$record_history_migration" \
        "missing prebuilt index $record_history_index on populated normalized_events; prebuild it with ops/record-history-indexes/install.sql, as $record_history_readme describes, before this schema-migration" <<SQL
DROP INDEX $record_history_index;
$record_history_seed
SQL
done
# The install.sql prebuilds over the same populated predecessor, run outside a transaction, are
# adopted by OID with every event and phase marker unchanged, and a repeat application is a
# no-op. The indexes then still have the baseline's definitions.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "$record_history_expected" "$record_history_seed" \
        "DROP INDEX $record_history_key_index, $record_history_emitter_index;"
    render_phase_migration "$record_history_install"
    cat <<SQL
CREATE TEMP TABLE record_history_upgrade_before AS SELECT
    (SELECT array_agg(to_regclass(index_name)::oid ORDER BY index_name)
     FROM expected_record_history_indexes) AS index_oids,
    (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) AS events,
    (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m) AS markers;
SQL
    emit_phase_migration "$record_history_migration" specialized
    emit_phase_migration "$record_history_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF EXISTS (SELECT 1 FROM record_history_upgrade_before b WHERE
        b.index_oids IS DISTINCT FROM (SELECT array_agg(to_regclass(index_name)::oid ORDER BY index_name)
                                       FROM expected_record_history_indexes) OR
        b.events IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) OR
        b.markers IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m)) THEN
        RAISE EXCEPTION 'record history index adoption changed its indexes, events or phase markers';
    END IF;
    IF NOT ($record_history_matches_baseline) THEN
        RAISE EXCEPTION 'record history index prebuild differs from baseline';
    END IF;
END \$\$;
DROP TABLE record_history_upgrade_before;
DROP TABLE expected_record_history_indexes;
DELETE FROM normalized_events WHERE event_identity = 'record-history-upgrade-proof';
SQL
} | run_psql
# install.sql refuses an invalid index under one of its names before it builds anything, and
# the README's drop-and-rerun recovery rebuilds it.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "UPDATE pg_index SET indisvalid = false WHERE indexrelid = '$record_history_emitter_index'::regclass;"
} | run_psql >/dev/null
assert_index_install_refusal record-history-install-invalid-index "$record_history_install" \
    "$record_history_emitter_index is missing from $scratch_schema.normalized_events or is not valid and ready; follow the recovery steps in $record_history_readme before retrying"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "DROP INDEX CONCURRENTLY $record_history_emitter_index;"
    render_phase_migration "$record_history_install"
} | run_psql >/dev/null
record_history_recovery="follow the recovery steps in $record_history_readme, then run the schema-migrations again"
for record_history_index in $record_history_indexes; do
    for record_history_flag in indisvalid indisready; do
        assert_migration_refusal "record-history-$record_history_flag-$record_history_index" \
            "$record_history_migration" \
            "$record_history_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $record_history_recovery" <<SQL
UPDATE pg_index SET $record_history_flag=false WHERE indexrelid='$record_history_index'::regclass;
SQL
    done
done
for record_history_kind in table view; do
    if [ "$record_history_kind" = table ]; then record_history_ddl="CREATE TABLE $record_history_key_index ();"
    else record_history_ddl="CREATE VIEW $record_history_key_index AS SELECT 1 AS occupied;"; fi
    assert_migration_refusal "record-history-$record_history_kind-collision" "$record_history_migration" \
        "$scratch_schema.$record_history_key_index is a $record_history_kind, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $record_history_key_index;
$record_history_ddl
SQL
done
assert_migration_refusal record-history-other-table "$record_history_migration" \
    "$record_history_emitter_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $record_history_recovery" <<SQL
DROP INDEX $record_history_emitter_index;
CREATE INDEX $record_history_emitter_index ON discovery_edges (chain_id);
SQL
# Print an index definition as the schema-migration reads it: search_path pg_catalog, so the
# table and the enum type carry the schema name. With a second argument, the index is first
# rebuilt under the name with those keys and predicate.
record_history_printed_definition() {
    {
        printf 'BEGIN;\nSET LOCAL search_path TO "%s";\n' "$scratch_schema"
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned'
        if [ -n "$2" ]; then
            printf '%s\n' "DROP INDEX $1;" "CREATE INDEX $1 ON normalized_events $2;"
        fi
        printf '%s\n' 'SET LOCAL search_path TO pg_catalog;' \
            "SELECT pg_get_indexdef('$scratch_schema.$1'::regclass);" 'ROLLBACK;'
    } | run_psql
}
record_history_predicate="WHERE event_kind = 'RecordChanged' AND consumer_visibility = 'activated' AND canonicality_state IN ('canonical', 'safe', 'finalized')"
# For each index: other keys, the reviewed keys under another predicate or in another order.
# Each is a valid, ready index under the name.
record_history_wrong_definitions=(
    "$record_history_key_index|(chain_id, block_number)"
    "$record_history_key_index|(chain_id, left(after_state ->> 'record_key', 256), block_number DESC NULLS LAST) WHERE event_kind = 'RecordChanged' AND canonicality_state IN ('canonical', 'safe', 'finalized')"
    "$record_history_key_index|(chain_id, (after_state ->> 'record_key'), block_number DESC NULLS LAST) $record_history_predicate"
    "$record_history_emitter_index|(chain_id, block_number)"
    "$record_history_emitter_index|(chain_id, left(after_state ->> 'record_key', 256), lower(raw_fact_ref ->> 'emitting_address'), block_number DESC NULLS LAST) $record_history_predicate"
)
for record_history_entry in "${record_history_wrong_definitions[@]}"; do
    record_history_index="${record_history_entry%%|*}"
    record_history_wrong="${record_history_entry#*|}"
    record_history_definition="$(record_history_printed_definition "$record_history_index" '')"
    record_history_found="$(record_history_printed_definition "$record_history_index" "$record_history_wrong")"
    assert_migration_refusal "record-history-wrong-definition-$record_history_index-$record_history_wrong" \
        "$record_history_migration" \
        "$record_history_index exists but does not have the reviewed definition; found \"$record_history_found\", expected \"$record_history_definition\"; $record_history_recovery" <<SQL
DROP INDEX $record_history_index;
CREATE INDEX $record_history_index ON normalized_events $record_history_wrong;
SQL
done
assert_migration_context_count "$record_history_migration" empty-schema 1
assert_migration_context_count "$record_history_migration" preceding-shape 3
assert_migration_context_count "$record_history_migration" specialized 1
assert_migration_context_count "$record_history_migration" baseline-first 6
