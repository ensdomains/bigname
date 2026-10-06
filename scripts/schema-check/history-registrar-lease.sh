# Sourced by check-schema with its scratch schema and assertion helpers.
lease_migration="$ROOT/migrations/20261006080000_normalized_events_history_registrar_lease_index.sql"
lease_install="$ROOT/ops/history-registrar-lease-index/install.sql"
lease_readme=ops/history-registrar-lease-index/README.md
lease_index=normalized_events_history_registrar_lease_idx
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' \
        "CREATE TEMP TABLE expected_lease_index AS SELECT pg_get_indexdef('$lease_index'::regclass) AS definition;" \
        "BEGIN; DELETE FROM normalized_events; DROP INDEX $lease_index;"
    emit_phase_migration "$lease_migration" preceding-shape
    emit_phase_migration "$lease_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF pg_get_indexdef('$lease_index'::regclass) <> (SELECT definition FROM expected_lease_index) THEN
        RAISE EXCEPTION 'registrar lease index empty upgrade differs from baseline';
    END IF;
END \$\$;
ROLLBACK;
DROP TABLE expected_lease_index;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$lease_migration" baseline-first
    assert_search_path_sql public
    emit_quote_all_identifiers_probe "$lease_migration" in-transaction
} | run_psql
assert_concurrent_index_installer history-registrar-lease \
    "$lease_index" "$lease_install" "$lease_readme" normalized_events \
    "chain_id, resource_id" no-json-key-literal
# A valid stored event makes this a populated released predecessor. Refusal must
# preserve data and the surrounding transaction. The helper rolls the mutation back.
lease_seed="INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind)
VALUES ('lease-upgrade-proof', 'schema-v2-check', 'ResolverRecordLinked', 'ens_v2_resolver_l1',
    1, 'schema-v2-check', 'ens_v2_resolver');"
assert_migration_refusal lease-missing-populated-prebuild "$lease_migration" \
    "missing prebuilt index $lease_index on populated normalized_events; run ops/history-registrar-lease-index/install.sql before this schema-migration" <<SQL
DROP INDEX $lease_index;
$lease_seed
SQL
# A concurrent prebuild over the same populated predecessor is adopted by OID,
# with every event and phase marker unchanged. Repeat migration is idempotent.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "$lease_seed" "DROP INDEX $lease_index;"
    render_phase_migration "$lease_install"
    cat <<SQL
CREATE TEMP TABLE lease_upgrade_before AS SELECT
    '$lease_index'::regclass::oid AS index_oid,
    (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) AS events,
    (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m) AS markers;
SQL
    emit_phase_migration "$lease_migration" preceding-shape
    emit_phase_migration "$lease_migration" baseline-first
    cat <<SQL
DO \$\$ BEGIN
    IF EXISTS (SELECT 1 FROM lease_upgrade_before b WHERE
        b.index_oid <> '$lease_index'::regclass::oid OR
        b.events IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(e) ORDER BY event_identity) FROM normalized_events e) OR
        b.markers IS DISTINCT FROM (SELECT jsonb_agg(to_jsonb(m) ORDER BY chain_id) FROM chain_phase_state m)) THEN
        RAISE EXCEPTION 'registrar lease adoption changed its index, events or phase markers';
    END IF;
END \$\$;
DROP TABLE lease_upgrade_before;
DELETE FROM normalized_events WHERE event_identity = 'lease-upgrade-proof';
SQL
} | run_psql
lease_recovery="follow the recovery steps in $lease_readme, then run the schema-migrations again"
for lease_flag in indisvalid indisready; do
    assert_migration_refusal "lease-$lease_flag" "$lease_migration" \
        "$lease_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $lease_recovery" <<SQL
UPDATE pg_index SET $lease_flag=false WHERE indexrelid='$lease_index'::regclass;
SQL
done
for lease_kind in table view; do
    if [ "$lease_kind" = table ]; then lease_ddl="CREATE TABLE $lease_index ();"
    else lease_ddl="CREATE VIEW $lease_index AS SELECT 1 AS occupied;"; fi
    assert_migration_refusal "lease-$lease_kind-collision" "$lease_migration" \
        "$scratch_schema.$lease_index is a $lease_kind, not an index, so the index was never built; remove or rename that relation, then run the schema-migrations again" <<SQL
DROP INDEX $lease_index;
$lease_ddl
SQL
done
assert_migration_refusal lease-other-table "$lease_migration" \
    "$lease_index exists but is not a valid and ready index on $scratch_schema.normalized_events; $lease_recovery" <<SQL
DROP INDEX $lease_index;
CREATE INDEX $lease_index ON discovery_edges (chain_id);
SQL
lease_definition="$({
    printf '%s\n' '\pset tuples_only on' '\pset format unaligned' \
        'SET search_path TO pg_catalog;' "SELECT pg_get_indexdef('$scratch_schema.$lease_index'::regclass);"
} | run_psql)"
for lease_wrong in \
    "(chain_id, resource_id)" \
    "(resource_id) WHERE (canonicality_state = 'canonical'::$scratch_schema.canonicality_state)" \
    "(resource_id) WHERE ((source_family = 'ens_v1_registrar_l1'::text) AND (canonicality_state = 'canonical'::$scratch_schema.canonicality_state))"; do
    assert_migration_refusal "lease-wrong-definition-$lease_wrong" "$lease_migration" \
        "$lease_index exists but does not have the reviewed definition; found \"CREATE INDEX $lease_index ON $scratch_schema.normalized_events USING btree $lease_wrong\", expected \"$lease_definition\"; $lease_recovery" <<SQL
DROP INDEX $lease_index;
CREATE INDEX $lease_index ON normalized_events $lease_wrong;
SQL
done
assert_migration_context_count "$lease_migration" empty-schema 1
assert_migration_context_count "$lease_migration" preceding-shape 4
assert_migration_context_count "$lease_migration" baseline-first 4
