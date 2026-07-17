-- Match `ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY` (0x4249474e414d4501).
-- Wait out automatic bootstrap before changing the continuous-apply handoff.
SELECT pg_advisory_xact_lock(4776427281231725825);

-- Continuous derive locks this cursor before it reads the change log. Keep the
-- cutover lock order cursor-then-change-log and force one idempotent pass over
-- the existing log after replacing the writer-side fence.
UPDATE public.projection_apply_cursors
SET
    last_change_id = 0,
    updated_at = now()
WHERE cursor_name = 'normalized_events_to_projection_invalidations';

-- Drain old derive readers and change-log writers before atomically replacing
-- the global writer-serialization trigger with the short reader-side fence.
LOCK TABLE public.projection_normalized_event_changes IN ACCESS EXCLUSIVE MODE;

DROP TRIGGER projection_normalized_event_changes_insert_order
    ON public.projection_normalized_event_changes;
DROP FUNCTION public.lock_projection_normalized_event_change_insert_order();

CREATE FUNCTION public.capture_projection_normalized_event_change_watermark()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    captured_change_id BIGINT;
BEGIN
    -- INSERT already holds ROW EXCLUSIVE on this table from before identity
    -- defaults are evaluated until its transaction ends. SHARE is compatible
    -- with other readers but waits out every writer that could have allocated
    -- a change id, and prevents a new allocation until the MAX is captured.
    LOCK TABLE public.projection_normalized_event_changes IN SHARE MODE;

    SELECT COALESCE(MAX(change_id), 0)
    INTO captured_change_id
    FROM public.projection_normalized_event_changes;

    RETURN captured_change_id;
END;
$$;

-- Repeat the targeted rewind while the cutover lock remains held. A derive
-- transaction that was already in flight either published before ACCESS
-- EXCLUSIVE was granted and is reset here, or resumes from zero afterwards.
UPDATE public.projection_apply_cursors
SET
    last_change_id = 0,
    updated_at = now()
WHERE cursor_name = 'normalized_events_to_projection_invalidations';
