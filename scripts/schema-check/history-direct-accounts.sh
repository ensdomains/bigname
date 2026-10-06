# Sourced by check-schema: exact baseline/prebuild adoption and focused refusal seams.
account_migration="$ROOT/migrations/20261006160000_normalized_events_history_direct_account_indexes.sql"
account_install="$ROOT/ops/history-direct-account-indexes/install.sql"
account_indexes=(normalized_events_history_account_owner_idx normalized_events_history_account_subject_idx normalized_events_history_reverse_address_idx)
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "CREATE TEMP TABLE expected_account_indexes AS SELECT c.relname,pg_get_indexdef(c.oid) AS definition FROM pg_class c WHERE c.relname IN ('${account_indexes[0]}','${account_indexes[1]}','${account_indexes[2]}') AND c.relnamespace='$scratch_schema'::regnamespace;" \
        'BEGIN; DELETE FROM normalized_events;'
    for index in "${account_indexes[@]}"; do printf 'DROP INDEX %s;\n' "$index"; done
    emit_phase_migration "$account_migration" preceding-shape
    emit_phase_migration "$account_migration" baseline-first
    cat <<'SQL'
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM expected_account_indexes e WHERE pg_get_indexdef(e.relname::regclass) <> e.definition) THEN
        RAISE EXCEPTION 'direct account index empty upgrade differs from baseline';
    END IF;
END $$;
ROLLBACK;
DROP TABLE expected_account_indexes;
SQL
    printf 'SET search_path TO public;\n'
    emit_phase_migration "$account_migration" baseline-first
    assert_search_path_sql public
    emit_quote_all_identifiers_probe "$account_migration" in-transaction
    emit_quote_all_identifiers_probe "$account_install" outside-transaction
} | run_psql
for index in "${account_indexes[@]}"; do
    assert_migration_refusal "account-missing-$index" "$account_migration" \
        "missing prebuilt index $index on populated normalized_events; run ops/history-direct-account-indexes/install.sql first" <<SQL
DROP INDEX $index;
INSERT INTO normalized_events(event_identity,namespace,event_kind,source_family,manifest_version,chain_id,derivation_kind)
VALUES('direct-account-upgrade-proof','schema-v2-check','AccountPermissionChanged','ens_v1_registry_l1',1,'schema-v2-check','ens_v1_unwrapped_authority');
SQL
    for flag in indisvalid indisready; do
        assert_migration_refusal "account-$index-$flag" "$account_migration" \
            "$index is not a valid and ready index on normalized_events; see ops/history-direct-account-indexes/README.md" <<SQL
UPDATE pg_index SET $flag=false WHERE indexrelid='$index'::regclass;
SQL
    done
    account_definition="$({
        printf '%s\n' '\pset tuples_only on' '\pset format unaligned' \
            'SET search_path TO pg_catalog;' "SELECT pg_get_indexdef('$scratch_schema.$index'::regclass);"
    } | run_psql)"
    assert_migration_refusal "account-wrong-definition-$index" "$account_migration" \
        "$index has an unexpected definition: CREATE INDEX $index ON $scratch_schema.normalized_events USING btree (chain_id), expected $account_definition; see ops/history-direct-account-indexes/README.md" <<SQL
DROP INDEX $index;
CREATE INDEX $index ON normalized_events(chain_id);
SQL
    assert_migration_refusal "account-table-collision-$index" "$account_migration" \
        "$index is missing or is not an index; see ops/history-direct-account-indexes/README.md" <<SQL
DROP INDEX $index;
CREATE TABLE $index (id int);
SQL
done
# A matching concurrent prebuild and rerun retain every original index OID.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' "CREATE TEMP TABLE account_index_oids AS SELECT relname,oid FROM pg_class WHERE relname IN ('${account_indexes[0]}','${account_indexes[1]}','${account_indexes[2]}') AND relnamespace='$scratch_schema'::regnamespace;"
    render_phase_migration "$account_install"
    render_phase_migration "$ROOT/ops/history-direct-account-indexes/verify.sql"
    emit_phase_migration "$account_migration" preceding-shape
    emit_phase_migration "$account_migration" baseline-first
    cat <<'SQL'
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM account_index_oids WHERE relname::regclass::oid <> oid) THEN
        RAISE EXCEPTION 'direct account index adoption rebuilt a prebuild';
    END IF;
END $$;
DROP TABLE account_index_oids;
SQL
} | run_psql
