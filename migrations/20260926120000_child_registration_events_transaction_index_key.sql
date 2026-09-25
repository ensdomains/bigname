-- Name history with include=child_registrations orders, limits and bounds its
-- child rows by child_registration_events.transaction_order_key before the merged
-- history order sees them. History now orders the rows of one block by
-- transaction index, so the key becomes the event's transaction index, or -1 when
-- the event has none, the sentinel log_order_key already uses. Existing rows are
-- backfilled from normalized_events by event_identity, and the history index is
-- rebuilt on the new key with the same column list. The matching interpreter
-- content hash rotation rewrites every row through a full Project rebuild; the
-- backfill keeps the served table consistent with the new readers until then.
-- An empty schema-migration database has no phase baseline yet. A table whose key
-- is already a bigint (a fresh baseline, or a rerun) keeps its rows and index; its
-- comments are still set, because the earlier schema-migration rewrites them on
-- every run.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.child_registration_events') IS NULL THEN
    RETURN;
END IF;
IF NOT EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = 'bigname_phase.child_registration_events'::regclass
      AND attname = 'transaction_order_key'
      AND NOT attisdropped
      AND atttypid = 'bigint'::regtype
) THEN
EXECUTE $ddl$
DROP INDEX IF EXISTS bigname_phase.child_registration_events_parent_history_idx
$ddl$;
-- A row whose event no longer exists is never served, because the child arm joins
-- the event by identity; it takes the missing-index key.
EXECUTE $ddl$
UPDATE bigname_phase.child_registration_events membership
SET transaction_order_key = COALESCE((
    SELECT event.transaction_index
    FROM bigname_phase.normalized_events event
    WHERE event.event_identity = membership.event_identity
), -1)::text
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.child_registration_events
    ALTER COLUMN transaction_order_key TYPE bigint USING transaction_order_key::bigint
$ddl$;
EXECUTE $ddl$
ALTER TABLE bigname_phase.child_registration_events
    ADD CONSTRAINT child_registration_events_transaction_order_key_check
    CHECK (transaction_order_key >= -1)
$ddl$;
EXECUTE $ddl$
CREATE INDEX child_registration_events_parent_history_idx
    ON bigname_phase.child_registration_events (
        parent_logical_name_id,
        chain_id,
        block_number,
        block_hash,
        transaction_order_key,
        log_order_key,
        event_identity
    )
$ddl$;
END IF;
EXECUTE $ddl$
COMMENT ON COLUMN bigname_phase.child_registration_events.transaction_order_key IS
    'This value is the event transaction index in its block, or -1 when the event has none, so it orders as history orders a missing index.'
$ddl$;
EXECUTE $ddl$
COMMENT ON INDEX bigname_phase.child_registration_events_parent_history_idx IS
    'This bounded index serves one parent''s child registrations in history order on one chain, in both directions. Every key is a bounded identifier, hash or number.'
$ddl$;
END
$migration$;
