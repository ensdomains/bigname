-- One catalog comparison for the actual fresh initializer and removal migration.
-- Normalize only the two isolated schema names, preserving names, defaults, constraints,
-- indexes, function bodies/security settings, triggers and table/column comments.
SET search_path TO pg_catalog;
CREATE TEMP TABLE removal_catalog AS
WITH namespaces AS (
    SELECT oid, nspname FROM pg_namespace
    WHERE nspname IN (current_setting('bigname.removal_schema'), current_setting('bigname.fresh_schema'))
), objects AS (
    SELECT namespace.nspname, concat_ws('|', 'relation', relation.relname, relation.relkind) AS object
    FROM namespaces namespace JOIN pg_class relation ON relation.relnamespace=namespace.oid
    WHERE relation.relkind IN ('r','p','S')
    UNION ALL
    SELECT namespace.nspname, concat_ws('|', 'column', relation.relname, attribute.attname,
        format_type(attribute.atttypid, attribute.atttypmod), attribute.attnotnull,
        attribute.attidentity, attribute.attgenerated, pg_get_expr(definition.adbin, definition.adrelid))
    FROM namespaces namespace JOIN pg_class relation ON relation.relnamespace=namespace.oid
    JOIN pg_attribute attribute ON attribute.attrelid=relation.oid AND attribute.attnum>0 AND NOT attribute.attisdropped
    LEFT JOIN pg_attrdef definition ON definition.adrelid=relation.oid AND definition.adnum=attribute.attnum
    WHERE relation.relkind IN ('r','p')
    UNION ALL
    SELECT namespace.nspname, concat_ws('|', 'constraint', relation.relname, constraint_row.conname,
        pg_get_constraintdef(constraint_row.oid), constraint_row.convalidated)
    FROM namespaces namespace JOIN pg_class relation ON relation.relnamespace=namespace.oid
    JOIN pg_constraint constraint_row ON constraint_row.conrelid=relation.oid
    UNION ALL
    SELECT namespace.nspname, concat_ws('|', 'index', pg_get_indexdef(index_row.indexrelid),
        index_row.indisvalid, index_row.indisready)
    FROM namespaces namespace JOIN pg_class relation ON relation.relnamespace=namespace.oid
    JOIN pg_index index_row ON index_row.indrelid=relation.oid
    UNION ALL
    SELECT namespace.nspname, concat_ws('|', 'function', pg_get_functiondef(function.oid), function.proacl)
    FROM namespaces namespace JOIN pg_proc function ON function.pronamespace=namespace.oid
    UNION ALL
    SELECT namespace.nspname, concat_ws('|', 'trigger', relation.relname, pg_get_triggerdef(trigger.oid), trigger.tgenabled)
    FROM namespaces namespace JOIN pg_class relation ON relation.relnamespace=namespace.oid
    JOIN pg_trigger trigger ON trigger.tgrelid=relation.oid AND NOT trigger.tgisinternal
    UNION ALL
    SELECT namespace.nspname, concat_ws('|', 'comment', relation.relname,
        coalesce(attribute.attname,'<table>'), description.description)
    FROM namespaces namespace JOIN pg_class relation ON relation.relnamespace=namespace.oid
    JOIN pg_description description ON description.objoid=relation.oid AND description.classoid='pg_class'::regclass
    LEFT JOIN pg_attribute attribute ON attribute.attrelid=relation.oid AND attribute.attnum=description.objsubid
    UNION ALL
    SELECT namespace.nspname, concat_ws('|', 'enum', type_row.typname, enum.enumsortorder, enum.enumlabel)
    FROM namespaces namespace JOIN pg_type type_row ON type_row.typnamespace=namespace.oid
    JOIN pg_enum enum ON enum.enumtypid=type_row.oid
)
SELECT nspname, replace(object,nspname,'bigname_phase') AS object FROM objects;
DO $check$
DECLARE difference text;
BEGIN
    SELECT string_agg(object, E'\n' ORDER BY object) INTO difference FROM (
        (SELECT object FROM removal_catalog WHERE nspname=current_setting('bigname.removal_schema')
         EXCEPT SELECT object FROM removal_catalog WHERE nspname=current_setting('bigname.fresh_schema'))
        UNION ALL
        (SELECT object FROM removal_catalog WHERE nspname=current_setting('bigname.fresh_schema')
         EXCEPT SELECT object FROM removal_catalog WHERE nspname=current_setting('bigname.removal_schema'))
    ) differences;
    IF difference IS NOT NULL THEN
        RAISE EXCEPTION 'fresh schema differs from the removal upgrade: %', difference;
    END IF;
    IF EXISTS (SELECT 1 FROM removal_catalog WHERE object ~ '^relation\|(name_current|children_current|permissions_current|account_permission_state_current|permissions_current_resource_summary|record_inventory_current|resolver_current|address_names_current|address_records_current|primary_names_current|project_generation_failures|project_redo_resolver_evidence|project_redo_expiry_roots|project_redo_child_registration_history)\|') THEN
        RAISE EXCEPTION 'retired serving table survived the removal';
    END IF;
END $check$;
DROP TABLE removal_catalog;
