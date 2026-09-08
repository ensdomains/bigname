-- #640 fixed evidence queries for one PostgreSQL-native restoration exercise.
-- psql -X -qAt -v mode=identity|empty|migrations|selected|checkpoint -f THIS_FILE
-- selected/checkpoint require chain,name,transaction,registration_transaction,
-- source_key,F0. Expected account, expiry, resource and source/hash identities
-- belong to the driver's validator, not SQL filters that hide mismatches.
-- selected and checkpoint intentionally produce identical, identity-free data.
-- Compare selected stopped H0 before/after restore; read identity separately.
-- This is not every-row/sequence/object/ACL/dependency equivalence evidence.
-- Native unfiltered dump/restore and independent native schema output remain
-- mandatory. No application writes, repairs, helper objects or replay here.
\set ON_ERROR_STOP on
\set QUIET on
\pset format unaligned
\pset tuples_only on
\pset pager off
\if :{?mode}
\else
  SELECT 'missing checkpoint mode'::integer;
\endif
SELECT CASE WHEN :'mode' IN ('identity','empty','migrations','selected','checkpoint')
       THEN '1' ELSE 'invalid checkpoint mode' END::integer AS valid_mode,
       :'mode' = 'identity' AS identity_mode,
       :'mode' = 'empty' AS empty_mode,
       :'mode' = 'migrations' AS migrations_mode,
       :'mode' IN ('selected','checkpoint') AS selected_mode
\gset
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL search_path = pg_catalog;
SET LOCAL timezone = 'UTC';
SET LOCAL datestyle = 'ISO, YMD';
SET LOCAL intervalstyle = 'postgres';
SET LOCAL bytea_output = 'hex';
SET LOCAL extra_float_digits = 3;
SET LOCAL client_encoding = 'UTF8';
-- Fail rather than silently return RLS-filtered application evidence.
SET LOCAL row_security = off;

\if :identity_mode
-- Run directly as each configured login. pg_control_system permission is a
-- declared prerequisite; matching identities do not attest other privileges.
SELECT jsonb_build_object(
  'db_name', d.datname, 'db_oid', d.oid::text,
  'cluster_id', s.system_identifier::text,
  'current_user', current_user, 'session_user', session_user,
  'owner', pg_get_userbyid(d.datdba), 'encoding', pg_encoding_to_char(d.encoding),
  'collation', d.datcollate, 'ctype', d.datctype)
FROM pg_database d CROSS JOIN pg_control_system() s
WHERE d.datname = current_database();
\endif

\if :empty_mode
-- Selected application absence, paired with the recorded native CREATE
-- DATABASE ... TEMPLATE template0 operation. Not a general catalog census.
SELECT jsonb_build_object(
  'application_schemas', COALESCE((SELECT jsonb_agg(nspname ORDER BY nspname COLLATE "C")
    FROM pg_namespace WHERE nspname = 'bigname_phase'), '[]'::jsonb),
  'relations', COALESCE((SELECT jsonb_agg(format('%I.%I',n.nspname,c.relname)
    ORDER BY n.nspname COLLATE "C",c.relname COLLATE "C")
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN ('public','bigname_phase')), '[]'::jsonb),
  'routines', COALESCE((SELECT jsonb_agg(p.oid::regprocedure::text
    ORDER BY p.oid::regprocedure::text COLLATE "C")
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname IN ('public','bigname_phase')), '[]'::jsonb),
  'types', COALESCE((SELECT jsonb_agg(format('%I.%I',n.nspname,t.typname)
    ORDER BY n.nspname COLLATE "C",t.typname COLLATE "C")
    FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
    WHERE n.nspname IN ('public','bigname_phase')), '[]'::jsonb),
  'extensions', COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'name',extname,'version',extversion) ORDER BY extname COLLATE "C")
    FROM pg_extension), '[]'::jsonb));
\endif

\if :migrations_mode
-- All actual ledger fields remain, including installation and execution time.
-- Only the driver derives the expected version/checksum set from MIGRATOR.
SELECT jsonb_build_object(
  'rows', COALESCE(jsonb_agg(to_jsonb(m) ORDER BY m.version), '[]'::jsonb),
  'versions', COALESCE(jsonb_agg(jsonb_build_object(
    'version',m.version::text,'checksum',encode(m.checksum,'hex'),
    'success',m.success) ORDER BY m.version), '[]'::jsonb))
FROM public._sqlx_migrations m;
\endif

\if :selected_mode
-- Retain mismatched/duplicate/noncanonical rows so the driver can reject them.
-- No time-of-query or database identity is introduced into this comparison.
WITH RECURSIVE
projections AS (
  SELECT * FROM bigname_phase.name_current WHERE raw_name = :'name'
), selected_resources AS (
  SELECT * FROM bigname_phase.resources
  WHERE resource_id IN (SELECT resource_id FROM projections)
), transactions AS (
  SELECT * FROM bigname_phase.raw_transactions
  WHERE chain_id = :'chain' AND transaction_hash = :'transaction'
), receipts AS (
  SELECT * FROM bigname_phase.raw_receipts
  WHERE chain_id = :'chain' AND transaction_hash = :'transaction'
), logs AS (
  SELECT * FROM bigname_phase.raw_logs
  WHERE chain_id = :'chain' AND transaction_hash = :'transaction'
), registration_transactions AS (
  SELECT * FROM bigname_phase.raw_transactions
  WHERE chain_id = :'chain' AND transaction_hash = :'registration_transaction'
), registration_receipts AS (
  SELECT * FROM bigname_phase.raw_receipts
  WHERE chain_id = :'chain' AND transaction_hash = :'registration_transaction'
), registration_logs AS (
  SELECT * FROM bigname_phase.raw_logs
  WHERE chain_id = :'chain' AND transaction_hash = :'registration_transaction'
), events AS (
  SELECT * FROM bigname_phase.normalized_events
  WHERE chain_id = :'chain' AND transaction_hash = :'transaction'
), history AS (
  SELECT * FROM bigname_phase.normalized_events
  WHERE chain_id = :'chain' AND transaction_hash = :'registration_transaction'
), phases AS (
  SELECT * FROM bigname_phase.chain_phase_state WHERE chain_id = :'chain'
), cursors AS (
  SELECT * FROM bigname_phase.ingest_cursors WHERE chain_id = :'chain'
), lineage AS (
  SELECT * FROM bigname_phase.chain_lineage WHERE chain_id = :'chain'
), heads AS (
  SELECT * FROM bigname_phase.chain_heads WHERE chain_id = :'chain'
), cursor_ancestry AS (
  SELECT c.source_key,l.block_number,l.block_hash,l.parent_hash,l.canonicality_state
  FROM cursors c JOIN lineage l
    ON l.block_number = c.last_processed_block_number
   AND l.block_hash = c.last_processed_block_hash
  WHERE c.source_key = :'source_key'
  UNION ALL
  SELECT a.source_key,p.block_number,p.block_hash,p.parent_hash,p.canonicality_state
  FROM cursor_ancestry a JOIN lineage p
    ON p.block_hash = a.parent_hash AND p.block_number = a.block_number - 1
  WHERE a.block_number > :'F0'::bigint
)
SELECT jsonb_build_object(
  'projections', COALESCE((SELECT jsonb_agg(to_jsonb(p)
    ORDER BY p.logical_name_id COLLATE "C") FROM projections p), '[]'::jsonb),
  'resources', COALESCE((SELECT jsonb_agg(to_jsonb(r)
    ORDER BY r.resource_id) FROM selected_resources r), '[]'::jsonb),
  'transactions', COALESCE((SELECT jsonb_agg(to_jsonb(t) || jsonb_build_object(
    'value',t.value::text) ORDER BY t.block_number,t.block_hash COLLATE "C",
    t.transaction_index) FROM transactions t), '[]'::jsonb),
  'receipts', COALESCE((SELECT jsonb_agg(to_jsonb(r) || jsonb_build_object(
    'gas_used',r.gas_used::text,'cumulative_gas_used',r.cumulative_gas_used::text)
    ORDER BY r.block_number,r.block_hash COLLATE "C",r.transaction_index)
    FROM receipts r), '[]'::jsonb),
  'logs', COALESCE((SELECT jsonb_agg(to_jsonb(l)
    ORDER BY l.block_number,l.block_hash COLLATE "C",l.log_index) FROM logs l), '[]'::jsonb),
  'registration_transactions', COALESCE((SELECT jsonb_agg(to_jsonb(t) || jsonb_build_object(
    'value',t.value::text) ORDER BY t.block_number,t.block_hash COLLATE "C",
    t.transaction_index) FROM registration_transactions t), '[]'::jsonb),
  'registration_receipts', COALESCE((SELECT jsonb_agg(to_jsonb(r) || jsonb_build_object(
    'gas_used',r.gas_used::text,'cumulative_gas_used',r.cumulative_gas_used::text)
    ORDER BY r.block_number,r.block_hash COLLATE "C",r.transaction_index)
    FROM registration_receipts r), '[]'::jsonb),
  'registration_logs', COALESCE((SELECT jsonb_agg(to_jsonb(l)
    ORDER BY l.block_number,l.block_hash COLLATE "C",l.log_index)
    FROM registration_logs l), '[]'::jsonb),
  'events', COALESCE((SELECT jsonb_agg(to_jsonb(e)
    ORDER BY e.block_number,e.block_hash COLLATE "C",e.transaction_index,e.log_index,
    e.normalized_event_id) FROM events e), '[]'::jsonb),
  'history', COALESCE((SELECT jsonb_agg(to_jsonb(e)
    ORDER BY e.block_number,e.block_hash COLLATE "C",e.transaction_index,e.log_index,
    e.normalized_event_id) FROM history e), '[]'::jsonb),
  'phases', COALESCE((SELECT jsonb_agg(to_jsonb(p)
    ORDER BY p.phase_name COLLATE "C") FROM phases p), '[]'::jsonb),
  'cursors', COALESCE((SELECT jsonb_agg(to_jsonb(c)
    ORDER BY c.source_key COLLATE "C") FROM cursors c), '[]'::jsonb),
  'lineage', COALESCE((SELECT jsonb_agg(to_jsonb(l)
    ORDER BY l.block_number,l.block_hash COLLATE "C") FROM lineage l), '[]'::jsonb),
  'heads', COALESCE((SELECT jsonb_agg(to_jsonb(h)
    ORDER BY h.chain_id COLLATE "C") FROM heads h), '[]'::jsonb),
  'cursor_ancestry', COALESCE((SELECT jsonb_agg(to_jsonb(a)
    ORDER BY a.source_key COLLATE "C",a.block_number,a.block_hash COLLATE "C")
    FROM cursor_ancestry a), '[]'::jsonb));
\endif
COMMIT;
