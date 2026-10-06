-- Add Project-owned lookup state. The changed compiled content hash requires normal full
-- Interpret rederivation and Project rebuild before admission. No side builder or marker edit.
DO $migration$
DECLARE checked record; actual_shape jsonb; expected_shape jsonb; name_preexisting boolean;
BEGIN
    IF to_regclass('bigname_phase.project_family_marker') IS NULL THEN RETURN; END IF;
    LOCK TABLE bigname_phase.project_family_marker IN EXCLUSIVE MODE;
    name_preexisting := to_regclass('bigname_phase.project_lookup_name') IS NOT NULL;
-- Project-owned current lookup facts. NULL core/metadata is an explicitly evaluated absence.
CREATE TABLE IF NOT EXISTS bigname_phase.project_lookup_name (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    record_serving_resource_id uuid,
    core jsonb,
    supported boolean NOT NULL,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK (supported = (core IS NOT NULL AND core #>> '{coverage,status}' IS DISTINCT FROM 'unsupported')),
    CHECK (core IS NULL OR jsonb_typeof(core) = 'object'),
    CHECK (core IS NOT NULL OR record_serving_resource_id IS NULL)
);
CREATE INDEX IF NOT EXISTS project_lookup_name_resource_idx
    ON bigname_phase.project_lookup_name (chain_id, record_serving_resource_id)
    WHERE record_serving_resource_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS bigname_phase.project_lookup_relation (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    address text NOT NULL,
    relation text NOT NULL,
    PRIMARY KEY (chain_id, logical_name_id, address, relation),
    CHECK (address = lower(address) AND address <> ''),
    CHECK (relation IN ('token_holder', 'effective_controller')),
    FOREIGN KEY (chain_id, logical_name_id) REFERENCES bigname_phase.project_lookup_name (chain_id, logical_name_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX IF NOT EXISTS project_lookup_relation_address_idx
    ON bigname_phase.project_lookup_relation (address, chain_id, relation, logical_name_id);
CREATE TABLE IF NOT EXISTS bigname_phase.project_lookup_inventory (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    metadata jsonb,
    PRIMARY KEY (chain_id, resource_id),
    CHECK (metadata IS NULL OR jsonb_typeof(metadata) = 'object')
);
CREATE TABLE IF NOT EXISTS bigname_phase.project_lookup_record (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    record_key text NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (chain_id, resource_id, record_key),
    CHECK (jsonb_typeof(payload) = 'object'),
    FOREIGN KEY (chain_id, resource_id) REFERENCES bigname_phase.project_lookup_inventory (chain_id, resource_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE IF NOT EXISTS bigname_phase.project_lookup_dependency (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    key1 text NOT NULL,
    key2 text NOT NULL,
    key3 text NOT NULL,
    PRIMARY KEY (chain_id, resource_id, kind, key1, key2, key3),
    CHECK (kind IN ('resource_pointer', 'identity', 'classification', 'registry_node',
                   'partition', 'link', 'record_id')),
    CHECK (key1 <> ''),
    CHECK ((kind IN ('resource_pointer', 'identity', 'classification') AND key2 = '' AND key3 = '')
        OR (kind IN ('registry_node', 'link', 'record_id') AND key2 <> '' AND key3 = '')
        OR (kind = 'partition' AND key2 <> '' AND key3 <> '')),
    FOREIGN KEY (chain_id, resource_id) REFERENCES bigname_phase.project_lookup_inventory (chain_id, resource_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX IF NOT EXISTS project_lookup_dependency_source_idx
    ON bigname_phase.project_lookup_dependency (chain_id, kind, key1, key2, key3, resource_id);
IF NOT name_preexisting THEN
    ALTER TABLE bigname_phase.project_lookup_name ADD CONSTRAINT project_lookup_name_inventory_fkey
        FOREIGN KEY (chain_id, record_serving_resource_id)
        REFERENCES bigname_phase.project_lookup_inventory (chain_id, resource_id)
        DEFERRABLE INITIALLY DEFERRED;
END IF;

-- Reject preexisting schema drift instead of admitting a same-named object.
-- Project-owned current lookup facts. NULL core/metadata is an explicitly evaluated absence.
CREATE TEMP TABLE project_lookup_name (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    record_serving_resource_id uuid,
    core jsonb,
    supported boolean NOT NULL,
    PRIMARY KEY (chain_id, logical_name_id),
    CHECK (supported = (core IS NOT NULL AND core #>> '{coverage,status}' IS DISTINCT FROM 'unsupported')),
    CHECK (core IS NULL OR jsonb_typeof(core) = 'object'),
    CHECK (core IS NOT NULL OR record_serving_resource_id IS NULL)
) ON COMMIT DROP;
CREATE INDEX project_lookup_name_resource_idx
    ON pg_temp.project_lookup_name (chain_id, record_serving_resource_id)
    WHERE record_serving_resource_id IS NOT NULL;
CREATE TEMP TABLE project_lookup_relation (
    chain_id text NOT NULL,
    logical_name_id text NOT NULL,
    address text NOT NULL,
    relation text NOT NULL,
    PRIMARY KEY (chain_id, logical_name_id, address, relation),
    CHECK (address = lower(address) AND address <> ''),
    CHECK (relation IN ('token_holder', 'effective_controller')),
    FOREIGN KEY (chain_id, logical_name_id) REFERENCES pg_temp.project_lookup_name (chain_id, logical_name_id) DEFERRABLE INITIALLY DEFERRED
) ON COMMIT DROP;
CREATE INDEX project_lookup_relation_address_idx
    ON pg_temp.project_lookup_relation (address, chain_id, relation, logical_name_id);
CREATE TEMP TABLE project_lookup_inventory (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    metadata jsonb,
    PRIMARY KEY (chain_id, resource_id),
    CHECK (metadata IS NULL OR jsonb_typeof(metadata) = 'object')
) ON COMMIT DROP;
CREATE TEMP TABLE project_lookup_record (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    record_key text NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (chain_id, resource_id, record_key),
    CHECK (jsonb_typeof(payload) = 'object'),
    FOREIGN KEY (chain_id, resource_id) REFERENCES pg_temp.project_lookup_inventory (chain_id, resource_id) DEFERRABLE INITIALLY DEFERRED
) ON COMMIT DROP;
CREATE TEMP TABLE project_lookup_dependency (
    chain_id text NOT NULL,
    resource_id uuid NOT NULL,
    kind text NOT NULL,
    key1 text NOT NULL,
    key2 text NOT NULL,
    key3 text NOT NULL,
    PRIMARY KEY (chain_id, resource_id, kind, key1, key2, key3),
    CHECK (kind IN ('resource_pointer', 'identity', 'classification', 'registry_node',
                   'partition', 'link', 'record_id')),
    CHECK (key1 <> ''),
    CHECK ((kind IN ('resource_pointer', 'identity', 'classification') AND key2 = '' AND key3 = '')
        OR (kind IN ('registry_node', 'link', 'record_id') AND key2 <> '' AND key3 = '')
        OR (kind = 'partition' AND key2 <> '' AND key3 <> '')),
    FOREIGN KEY (chain_id, resource_id) REFERENCES pg_temp.project_lookup_inventory (chain_id, resource_id) DEFERRABLE INITIALLY DEFERRED
) ON COMMIT DROP;
CREATE INDEX project_lookup_dependency_source_idx
    ON pg_temp.project_lookup_dependency (chain_id, kind, key1, key2, key3, resource_id);
ALTER TABLE pg_temp.project_lookup_name ADD CONSTRAINT project_lookup_name_inventory_fkey
    FOREIGN KEY (chain_id, record_serving_resource_id)
    REFERENCES pg_temp.project_lookup_inventory (chain_id, resource_id) DEFERRABLE INITIALLY DEFERRED;

    FOR checked IN SELECT unnest(ARRAY['project_lookup_name', 'project_lookup_relation',
        'project_lookup_inventory', 'project_lookup_record', 'project_lookup_dependency']) AS name LOOP
        SELECT jsonb_agg(definition ORDER BY definition) INTO expected_shape FROM (
            SELECT jsonb_build_array('column', attname, format_type(atttypid, atttypmod), attnotnull,
                attcollation, COALESCE(pg_get_expr(d.adbin,d.adrelid),'')) AS definition
            FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
            WHERE a.attrelid=format('pg_temp.%I',checked.name)::regclass AND a.attnum>0 AND NOT a.attisdropped
            UNION ALL SELECT jsonb_build_array('constraint', conname, contype, convalidated,
                replace(replace(pg_get_constraintdef(oid), pg_my_temp_schema()::regnamespace::text || '.', ''), 'bigname_phase.', '')) FROM pg_constraint
            WHERE conrelid=format('pg_temp.%I',checked.name)::regclass
            UNION ALL SELECT jsonb_build_array('index', (SELECT relname FROM pg_class WHERE oid=indexrelid),
                replace(pg_get_indexdef(indexrelid), pg_my_temp_schema()::regnamespace::text || '.', ''),
                indisvalid, indisready) FROM pg_index
            WHERE indrelid=format('pg_temp.%I',checked.name)::regclass
        ) shape;
        SELECT jsonb_agg(definition ORDER BY definition) INTO actual_shape FROM (
            SELECT jsonb_build_array('column', attname, format_type(atttypid, atttypmod), attnotnull,
                attcollation, COALESCE(pg_get_expr(d.adbin,d.adrelid),'')) AS definition
            FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
            WHERE a.attrelid=format('bigname_phase.%I',checked.name)::regclass AND a.attnum>0 AND NOT a.attisdropped
            UNION ALL SELECT jsonb_build_array('constraint', conname, contype, convalidated,
                replace(replace(pg_get_constraintdef(oid), pg_my_temp_schema()::regnamespace::text || '.', ''), 'bigname_phase.', '')) FROM pg_constraint
            WHERE conrelid=format('bigname_phase.%I',checked.name)::regclass
            UNION ALL SELECT jsonb_build_array('index', replace((SELECT relname FROM pg_class WHERE oid=indexrelid),'bigname_phase.',''),
                replace(pg_get_indexdef(indexrelid), 'bigname_phase.', ''), indisvalid, indisready) FROM pg_index
            WHERE indrelid=format('bigname_phase.%I',checked.name)::regclass
        ) shape;
        IF actual_shape IS DISTINCT FROM expected_shape OR NOT EXISTS (
            SELECT 1 FROM pg_class WHERE oid=format('bigname_phase.%I',checked.name)::regclass
                AND relkind='r' AND relpersistence='p' AND NOT relrowsecurity AND NOT relispartition
        ) THEN
            RAISE EXCEPTION 'incompatible lookup table %', checked.name;
        END IF;
    END LOOP;
    DROP TABLE pg_temp.project_lookup_relation, pg_temp.project_lookup_record,
        pg_temp.project_lookup_dependency, pg_temp.project_lookup_name, pg_temp.project_lookup_inventory;
END
$migration$;
