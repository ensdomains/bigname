# Sourced by check-schema with its scratch schema and assertion helpers.
# 20261112120000_normalized_events_address_key_indexes.sql must build the baseline's three
# definitions on an empty table, refuse a populated table without the prebuilds, adopt the
# ops/address-key-indexes/install.sql prebuilds by OID with every row unchanged, rerun as a
# no-op, leave the caller's search_path and quote_all_identifiers as it found them, and refuse
# every shape CREATE INDEX IF NOT EXISTS would skip. install.sql must refuse an invalid index
# under one of its names.
address_key_migration="$ROOT/migrations/20261112120000_normalized_events_address_key_indexes.sql"
address_key_install="$ROOT/ops/address-key-indexes/install.sql"
address_key_readme=ops/address-key-indexes/README.md
address_key_to_index=normalized_events_address_to_idx
address_key_registrant_index=normalized_events_address_registrant_idx
address_key_owner_index=normalized_events_address_owner_idx
address_key_indexes="$address_key_to_index $address_key_registrant_index $address_key_owner_index"
address_key_names="'$address_key_to_index', '$address_key_registrant_index', '$address_key_owner_index'"
address_key_expected="CREATE TEMP TABLE expected_address_key_indexes AS
SELECT relname::text AS index_name, pg_get_indexdef(oid) AS definition
FROM pg_class
WHERE relname IN ($address_key_names) AND relnamespace = '$scratch_schema'::regnamespace;"
address_key_matches_baseline="(SELECT count(*) FROM expected_address_key_indexes) = 3 AND NOT EXISTS (
    SELECT 1 FROM expected_address_key_indexes expected
    WHERE pg_get_indexdef(to_regclass(expected.index_name)) IS DISTINCT FROM expected.definition)"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "$address_key_expected" \
        "BEGIN; DELETE FROM normalized_events; DROP INDEX $address_key_to_index, $address_key_registrant_index, $address_key_owner_index;"
    emit_phase_migration "$address_key_migration" preceding-shape
    emit_phase_migration "$address_key_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF NOT ($address_key_matches_baseline) THEN
        RAISE EXCEPTION 'address key indexes empty upgrade differs from baseline';
    END IF;
END \$\$;
ROLLBACK;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$address_key_migration" baseline-first
    assert_search_path_sql public
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    emit_quote_all_identifiers_probe "$address_key_migration" in-transaction
    assert_search_path_sql "$scratch_schema"
} | run_psql
# A valid stored event makes this a populated released predecessor. Refusal must preserve data
# and the surrounding transaction. The helper rolls the mutation back.
address_key_seed="INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind)
VALUES ('address-key-upgrade-proof', 'schema-v2-check', 'ResolverRecordLinked', 'ens_v2_resolver_l1',
    1, 'schema-v2-check', 'ens_v2_resolver');"
for address_key_index in $address_key_indexes; do
    assert_migration_refusal "address-key-missing-populated-prebuild-$address_key_index" \
        "$address_key_migration" \
        "missing prebuilt index $address_key_index on populated normalized_events; prebuild it with ops/address-key-indexes/install.sql, as $address_key_readme describes, before this schema-migration" <<SQL
DROP INDEX $address_key_index;
$address_key_seed
SQL
done
# The install.sql prebuilds over the same populated predecessor, run outside a transaction, are
# adopted by OID with every event and phase marker unchanged, and a repeat application is a
# no-op. The indexes then still have the baseline's definitions.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "$address_key_expected" "$address_key_seed" \
        "DROP INDEX $address_key_to_index, $address_key_registrant_index, $address_key_owner_index;"
    render_phase_migration "$address_key_install"
    cat <<SQL
CREATE TEMP TABLE address_key_upgrade_before AS SELECT
    (SELECT array_agg(to_regclass(index_name)::oid ORDER BY index_name)
     FROM expected_address_key_indexes) AS index_oids,
    (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) AS events,
    (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m) AS markers;
SQL
    emit_phase_migration "$address_key_migration" specialized
    emit_phase_migration "$address_key_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF EXISTS (SELECT 1 FROM address_key_upgrade_before b WHERE
        b.index_oids IS DISTINCT FROM (SELECT array_agg(to_regclass(index_name)::oid ORDER BY index_name)
                                       FROM expected_address_key_indexes) OR
        b.events IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) OR
        b.markers IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m)) THEN
        RAISE EXCEPTION 'address key index adoption changed its indexes, events or phase markers';
    END IF;
    IF NOT ($address_key_matches_baseline) THEN
        RAISE EXCEPTION 'address key index prebuild differs from baseline';
    END IF;
END \$\$;
DROP TABLE address_key_upgrade_before;
DROP TABLE expected_address_key_indexes;
DELETE FROM normalized_events WHERE event_identity = 'address-key-upgrade-proof';
SQL
} | run_psql
# install.sql refuses an invalid index under one of its names before it builds anything, and
# the README's drop-and-rerun recovery rebuilds it.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "UPDATE pg_index SET indisvalid = false WHERE indexrelid = '$address_key_registrant_index'::regclass;"
} | run_psql >/dev/null
assert_index_install_refusal address-key-install-invalid-index "$address_key_install" \
    "$address_key_registrant_index is missing from $scratch_schema.normalized_events or is not valid and ready; follow the recovery steps in $address_key_readme before retrying"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "DROP INDEX CONCURRENTLY $address_key_registrant_index;"
    render_phase_migration "$address_key_install"
} | run_psql >/dev/null
address_key_recovery="follow the recovery steps in $address_key_readme, then run the schema-migrations again"
for address_key_index in $address_key_indexes; do
    for address_key_flag in indisvalid indisready; do
        assert_migration_refusal "address-key-$address_key_flag-$address_key_index" \
            "$address_key_migration" \
            "$address_key_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $address_key_recovery" <<SQL
UPDATE pg_index SET $address_key_flag=false WHERE indexrelid='$address_key_index'::regclass;
SQL
    done
done
for address_key_kind in table view; do
    if [ "$address_key_kind" = table ]; then address_key_ddl="CREATE TABLE $address_key_to_index ();"
    else address_key_ddl="CREATE VIEW $address_key_to_index AS SELECT 1 AS occupied;"; fi
    assert_migration_refusal "address-key-$address_key_kind-collision" "$address_key_migration" \
        "$scratch_schema.$address_key_to_index is a $address_key_kind, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $address_key_to_index;
$address_key_ddl
SQL
done
assert_migration_refusal address-key-other-table "$address_key_migration" \
    "$address_key_owner_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $address_key_recovery" <<SQL
DROP INDEX $address_key_owner_index;
CREATE INDEX $address_key_owner_index ON discovery_edges (chain_id);
SQL
# Print an index definition as the schema-migration reads it: search_path pg_catalog, so the
# table carries the schema name. With a second argument, the index is first rebuilt under the
# name with those keys and predicate.
address_key_printed_definition() {
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
# For each index: other keys, the reviewed key on every row, and the reviewed key under a
# predicate the probe does not state. Each is a valid, ready index under the name.
address_key_wrong_definitions=()
for address_key_key in to registrant owner; do
    address_key_index="normalized_events_address_${address_key_key}_idx"
    address_key_expression="(lower(COALESCE(after_state ->> '$address_key_key', '')))"
    address_key_wrong_definitions+=(
        "$address_key_index|(chain_id, block_number)"
        "$address_key_index|$address_key_expression"
        "$address_key_index|$address_key_expression WHERE (after_state ->> '$address_key_key') IS NOT NULL"
    )
done
for address_key_entry in "${address_key_wrong_definitions[@]}"; do
    address_key_index="${address_key_entry%%|*}"
    address_key_wrong="${address_key_entry#*|}"
    address_key_definition="$(address_key_printed_definition "$address_key_index" '')"
    address_key_found="$(address_key_printed_definition "$address_key_index" "$address_key_wrong")"
    assert_migration_refusal "address-key-wrong-definition-$address_key_index-$address_key_wrong" \
        "$address_key_migration" \
        "$address_key_index exists but does not have the reviewed definition; found \"$address_key_found\", expected \"$address_key_definition\"; $address_key_recovery" <<SQL
DROP INDEX $address_key_index;
CREATE INDEX $address_key_index ON normalized_events $address_key_wrong;
SQL
done
assert_migration_context_count "$address_key_migration" empty-schema 1
assert_migration_context_count "$address_key_migration" preceding-shape 3
assert_migration_context_count "$address_key_migration" specialized 1
assert_migration_context_count "$address_key_migration" baseline-first 6
