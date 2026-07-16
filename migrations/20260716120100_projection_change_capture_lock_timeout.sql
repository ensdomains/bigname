-- Complete-prefix capture still waits out every transaction that may have
-- allocated a lower change id, but it must not become an unbounded writer
-- barrier behind a long normalized-event/backfill transaction. PostgreSQL
-- restores the caller's setting after the function returns.
ALTER FUNCTION public.capture_projection_normalized_event_change_watermark()
SET lock_timeout = '100ms';
