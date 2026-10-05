# Sourced by scripts/check-schema after the historical upgrade reaches the final
# catalogue shape. The earlier fresh/predecessor parity check independently pins
# all final definitions to the checked-in baseline.
catalogue_index_migration="$ROOT/migrations/20261005170000_project_address_history_catalogue.sql"
catalogue_index_install="$ROOT/ops/address-history-catalogue-indexes/install.sql"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
CREATE TABLE catalogue_expected_indexes AS
SELECT rel.relname AS index_name, pg_get_indexdef(idx.indexrelid) AS definition
FROM pg_index idx JOIN pg_class rel ON rel.oid = idx.indexrelid
WHERE idx.indexrelid IN (
    'normalized_events_name_history_idx'::regclass,
    'normalized_events_resource_history_idx'::regclass,
    'normalized_events_project_node_history_idx'::regclass,
    'normalized_events_record_id_write_idx'::regclass,
    'normalized_events_history_discovery_name_idx'::regclass,
    'normalized_events_history_discovery_resource_idx'::regclass
);
-- Recreate the actual pre-catalogue definitions, rather than fabricated wrong
-- indexes. One admitted unpositioned manifest event makes this a populated upgrade.
DROP INDEX normalized_events_name_history_idx;
CREATE INDEX IF NOT EXISTS normalized_events_name_history_idx
    ON normalized_events (
        logical_name_id,
        block_number DESC,
        transaction_index DESC,
        log_index DESC,
        normalized_event_id DESC
    )
    WHERE logical_name_id IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
DROP INDEX normalized_events_resource_history_idx;
CREATE INDEX IF NOT EXISTS normalized_events_resource_history_idx
    ON normalized_events (
        resource_id,
        block_number DESC,
        transaction_index DESC,
        log_index DESC,
        normalized_event_id DESC
    )
    WHERE resource_id IS NOT NULL
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
DROP INDEX normalized_events_project_node_history_idx;
CREATE INDEX IF NOT EXISTS normalized_events_project_node_history_idx
    ON normalized_events (chain_id, lower(after_state ->> 'node'), block_number)
    WHERE logical_name_id IS NULL
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
      AND after_state ->> 'node' IS NOT NULL
      AND ((event_kind IN ('RecordChanged', 'RecordVersionChanged')
            AND source_family IN ('ens_v1_resolver_l1', 'ens_v2_resolver_l1', 'basenames_base_resolver'))
           OR (event_kind = 'ResolverChanged'
               AND source_family IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1', 'ens_v1_wrapper_l1')));
DROP INDEX normalized_events_record_id_write_idx;
CREATE INDEX IF NOT EXISTS normalized_events_record_id_write_idx
    ON normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        (after_state ->> 'resolver_record_id')
    )
    WHERE event_kind = 'RecordChanged'
      AND after_state ->> 'storage_model' = 'resolver_record_id'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized');
DROP INDEX normalized_events_history_discovery_name_idx;
DROP INDEX normalized_events_history_discovery_resource_idx;
INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
    manifest_version, chain_id, derivation_kind)
VALUES ('catalogue-migration-test', 'schema-v2-check', 'SourceManifestUpdated',
    'ens_v2_resolver_l1', 1, 'schema-v2-check', 'manifest_sync');
CREATE TABLE catalogue_old_indexes AS
SELECT relname, oid FROM pg_class WHERE oid IN (
    'normalized_events_name_history_idx'::regclass,
    'normalized_events_resource_history_idx'::regclass,
    'normalized_events_project_node_history_idx'::regclass,
    'normalized_events_record_id_write_idx'::regclass
);
SQL
    render_phase_migration "$catalogue_index_install"
    printf '%s\n' 'CREATE TABLE catalogue_first_build AS SELECT oid, relname FROM pg_class WHERE relnamespace = current_schema()::regnamespace AND (relname LIKE '\''ahc_%'\'' OR relname LIKE '\''normalized_events_history_discovery_%'\'');'
    render_phase_migration "$catalogue_index_install"
    cat <<'SQL'
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM catalogue_first_build original
        LEFT JOIN pg_class current ON current.oid = original.oid AND current.relname = original.relname
        WHERE current.oid IS NULL) THEN
        RAISE EXCEPTION 'catalogue installer rebuilt a completed candidate on rerun';
    END IF;
END $$;
DROP TABLE catalogue_first_build;
SQL
} | run_psql >/dev/null

# A refusal after a DROP would roll back and hide the attempted loss. Reject any
# DROP INDEX through an event trigger installed only inside the refusal transaction,
# so the expected validation error proves no drop was even attempted. Transaction
# rollback also removes this trigger and function, including on an unexpected error.
catalogue_no_early_drop_sql="$(cat <<SQL
CREATE FUNCTION "$scratch_schema".catalogue_forbid_early_drop() RETURNS event_trigger
LANGUAGE plpgsql AS \$\$ BEGIN
    RAISE EXCEPTION 'catalogue attempted an index drop before completing validation';
END \$\$;
CREATE EVENT TRIGGER catalogue_forbid_early_drop ON ddl_command_start
WHEN TAG IN ('DROP INDEX') EXECUTE FUNCTION "$scratch_schema".catalogue_forbid_early_drop();
SQL
)"
assert_migration_refusal catalogue-missing-last-prebuild "$catalogue_index_migration" \
    'missing prebuilt index normalized_events_history_discovery_resource_idx on populated normalized_events; run ops/address-history-catalogue-indexes/install.sql before the catalogue migration' <<SQL
DROP INDEX normalized_events_history_discovery_resource_idx;
$catalogue_no_early_drop_sql
SQL
assert_migration_refusal catalogue-invalid-replacement "$catalogue_index_migration" \
    "prebuilt index ahc_record_prebuild_idx is not the valid, ready reviewed index on $scratch_schema.normalized_events; follow ops/address-history-catalogue-indexes/README.md" <<SQL
UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'ahc_record_prebuild_idx'::regclass;
$catalogue_no_early_drop_sql
SQL
assert_migration_refusal catalogue-wrong-replacement "$catalogue_index_migration" \
    "prebuilt index ahc_record_prebuild_idx is not the valid, ready reviewed index on $scratch_schema.normalized_events; follow ops/address-history-catalogue-indexes/README.md" <<SQL
DROP INDEX ahc_record_prebuild_idx;
CREATE INDEX ahc_record_prebuild_idx ON normalized_events (chain_id);
$catalogue_no_early_drop_sql
SQL
assert_migration_refusal catalogue-invalid-new-index "$catalogue_index_migration" \
    "prebuilt index normalized_events_history_discovery_resource_idx is not the valid, ready reviewed index on $scratch_schema.normalized_events; follow ops/address-history-catalogue-indexes/README.md" <<SQL
UPDATE pg_index SET indisready = false WHERE indexrelid = 'normalized_events_history_discovery_resource_idx'::regclass;
$catalogue_no_early_drop_sql
SQL

# A later invalid candidate must make the concurrent installer refuse before it
# builds an earlier missing candidate. Recover exactly as the runbook prescribes.
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' 'DROP INDEX ahc_name_prebuild_idx;' \
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'ahc_record_prebuild_idx'::regclass;"
} | run_psql >/dev/null
catalogue_bad_candidate_message="prebuilt index ahc_record_prebuild_idx is not the valid, ready reviewed index on $scratch_schema.normalized_events; follow ops/address-history-catalogue-indexes/README.md"
assert_index_install_refusal catalogue-installer-before-build "$catalogue_index_install" "$catalogue_bad_candidate_message"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    cat <<'SQL'
DO $$ BEGIN
    IF to_regclass('ahc_name_prebuild_idx') IS NOT NULL THEN
        RAISE EXCEPTION 'catalogue installer started a build before refusing another candidate';
    END IF;
END $$;
DROP INDEX CONCURRENTLY ahc_record_prebuild_idx;
SQL
    render_phase_migration "$catalogue_index_install"
    printf '%s\n' 'DROP INDEX ahc_record_prebuild_idx;' \
        'CREATE INDEX ahc_record_prebuild_idx ON normalized_events (chain_id);'
} | run_psql >/dev/null
assert_index_install_refusal catalogue-installer-wrong-definition "$catalogue_index_install" "$catalogue_bad_candidate_message"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' 'DROP INDEX CONCURRENTLY ahc_record_prebuild_idx;'
    render_phase_migration "$catalogue_index_install"
    printf '%s\n' 'DROP INDEX ahc_record_prebuild_idx;' 'CREATE TABLE ahc_record_prebuild_idx ();'
} | run_psql >/dev/null
assert_index_install_refusal catalogue-installer-name-taken "$catalogue_index_install" "$catalogue_bad_candidate_message"
{
    printf 'SET search_path TO "%s";\n' "$scratch_schema"
    printf '%s\n' 'DROP TABLE ahc_record_prebuild_idx;'
    render_phase_migration "$catalogue_index_install"
    cat <<'SQL'
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM catalogue_old_indexes original
        LEFT JOIN pg_class current ON current.oid = original.oid AND current.relname = original.relname
        WHERE current.oid IS NULL) THEN
        RAISE EXCEPTION 'catalogue prebuild or recovery removed an old index';
    END IF;
END $$;
CREATE TABLE catalogue_adopted_oids AS
SELECT index_name, to_regclass(candidate_name)::oid AS oid
FROM (VALUES
    ('normalized_events_name_history_idx', 'ahc_name_prebuild_idx'),
    ('normalized_events_resource_history_idx', 'ahc_resource_prebuild_idx'),
    ('normalized_events_project_node_history_idx', 'ahc_node_prebuild_idx'),
    ('normalized_events_record_id_write_idx', 'ahc_record_prebuild_idx'),
    ('normalized_events_history_discovery_name_idx', 'normalized_events_history_discovery_name_idx'),
    ('normalized_events_history_discovery_resource_idx', 'normalized_events_history_discovery_resource_idx')
) AS names(index_name, candidate_name);
BEGIN;
SET LOCAL search_path TO public;
SET LOCAL quote_all_identifiers TO on;
SQL
    emit_phase_migration "$catalogue_index_migration" prebuilt-adoption
    assert_search_path_sql public
    assert_quote_all_identifiers_sql on
    printf 'COMMIT;\nSET search_path TO "%s";\n' "$scratch_schema"
    # Rerunning either step must retain the adopted OIDs without new candidates.
    emit_phase_migration "$catalogue_index_migration" baseline-first
    render_phase_migration "$catalogue_index_install"
    cat <<'SQL'
DO $$ BEGIN
    IF (SELECT count(*) FROM catalogue_adopted_oids) <> 6 OR EXISTS (
        SELECT 1 FROM catalogue_adopted_oids expected
        LEFT JOIN pg_class actual ON actual.oid = expected.oid AND actual.relname = expected.index_name
        WHERE actual.oid IS NULL
    ) THEN RAISE EXCEPTION 'catalogue adoption rebuilt or failed to rename a prebuilt index'; END IF;
    IF EXISTS (
        SELECT 1 FROM catalogue_expected_indexes expected
        LEFT JOIN pg_index actual ON actual.indexrelid = to_regclass(expected.index_name)
        WHERE actual.indexrelid IS NULL OR NOT actual.indisvalid OR NOT actual.indisready
           OR pg_get_indexdef(actual.indexrelid) <> expected.definition
    ) THEN RAISE EXCEPTION 'catalogue adoption changed a final baseline definition'; END IF;
    IF EXISTS (SELECT 1 FROM pg_class WHERE relnamespace = current_schema()::regnamespace
        AND relname IN ('ahc_name_prebuild_idx', 'ahc_resource_prebuild_idx',
                       'ahc_node_prebuild_idx', 'ahc_record_prebuild_idx')) THEN
        RAISE EXCEPTION 'catalogue adoption left a duplicate candidate';
    END IF;
END $$;
-- A valid redundant candidate from a manual/interrupted adoption is removed
-- without losing or rebuilding the already-correct final index.
SELECT replace(pg_get_indexdef('normalized_events_name_history_idx'::regclass),
    'CREATE INDEX normalized_events_name_history_idx ', 'CREATE INDEX ahc_name_prebuild_idx ')
\gexec
SQL
    emit_phase_migration "$catalogue_index_migration" baseline-first
    cat <<'SQL'
DO $$ BEGIN
    IF to_regclass('ahc_name_prebuild_idx') IS NOT NULL OR
       'normalized_events_name_history_idx'::regclass::oid <>
       (SELECT oid FROM catalogue_adopted_oids WHERE index_name = 'normalized_events_name_history_idx') THEN
        RAISE EXCEPTION 'catalogue duplicate cleanup lost the final index';
    END IF;
END $$;
DELETE FROM normalized_events WHERE event_identity = 'catalogue-migration-test';
DROP TABLE catalogue_adopted_oids, catalogue_old_indexes, catalogue_expected_indexes;
SQL
} | run_psql >/dev/null
printf '%s\n' 'address-history catalogue: concurrent recovery, OID adoption and seven refusal checks passed'
