-- Match `ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY` (0x4249474e414d4501).
-- Taking the bootstrap lock first waits out any replay that captured a
-- pre-fence watermark but has not seeded its cursor yet. Keeping this lock
-- order prevents the migration from holding table locks while waiting on
-- replay.
SELECT pg_advisory_xact_lock(4776427281231725825);

-- Derive locks this cursor row before it reads the change log. Match that lock
-- order and hold an existing row through cutover, without taking a broad table
-- lock that could invert another reader's relation-lock order. The final
-- rewind below repeats this update to catch a cursor that was absent here but
-- inserted by a derive transaction already reading the old change log.
UPDATE public.projection_apply_cursors
SET
    last_change_id = 0,
    updated_at = now()
WHERE cursor_name = 'normalized_events_to_projection_invalidations';

-- `change_id` is an identity value, so allocation order alone does not imply
-- commit order: a later allocator can otherwise commit and advance the apply
-- cursor while an earlier allocator is still in flight. Make the trigger
-- installation and cursor rewind an atomic cutover with respect to existing
-- insert writers and derive readers.
LOCK TABLE public.projection_normalized_event_changes IN ACCESS EXCLUSIVE MODE;

CREATE FUNCTION public.lock_projection_normalized_event_change_insert_order()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- A BEFORE STATEMENT trigger runs before row defaults (including the
    -- identity sequence) are evaluated. The transaction-scoped lock remains
    -- held through commit, so another transaction cannot allocate a change id
    -- until every lower id from this transaction is committed or rolled back.
    PERFORM pg_advisory_xact_lock(
        hashtextextended('projection_normalized_event_changes:insert_order', 0)
    );
    RETURN NULL;
END;
$$;

CREATE TRIGGER projection_normalized_event_changes_insert_order
BEFORE INSERT ON public.projection_normalized_event_changes
FOR EACH STATEMENT
EXECUTE FUNCTION public.lock_projection_normalized_event_change_insert_order();

-- There is no per-change processed marker with which to distinguish a change
-- skipped by an allocation-ordered cursor before this fence existed. Repeat
-- the targeted rewind while the change-log cutover lock is held: if the row
-- was absent above, any pre-cutover derive either committed its new cursor
-- before ACCESS EXCLUSIVE was granted (and is reset here), or is blocked on
-- the change log with a safe zero lower bound. Re-derivation is idempotent.
UPDATE public.projection_apply_cursors
SET
    last_change_id = 0,
    updated_at = now()
WHERE cursor_name = 'normalized_events_to_projection_invalidations';
