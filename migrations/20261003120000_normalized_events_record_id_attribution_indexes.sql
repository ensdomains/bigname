-- Existing schema-v2 databases gain the two indexes the record-ID arm of history's record
-- attribution (crates/storage/src/history/attribution) reads: a selected record's
-- RecordChanged writes by resolver and record id, and the ResolverRecordLinked rows on a
-- pointer's resolver. Without them each history read with a record-ID resolver walks every
-- RecordChanged row of the chain. Prebuild both concurrently on a large initialized database
-- as docs/deployment.md describes, so this schema-migration finds them and skips the build.
-- Index only; no column or row changes. An empty schema-migration database has no phase
-- baseline yet, so this schema-migration is a no-op there and phase-runner init-schema
-- installs the same indexes.
DO $migration$
BEGIN
IF to_regclass('bigname_phase.normalized_events') IS NULL THEN
    RETURN;
END IF;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS normalized_events_record_id_write_idx
    ON bigname_phase.normalized_events (
        chain_id,
        lower(after_state ->> 'resolver'),
        (after_state ->> 'resolver_record_id')
    )
    WHERE event_kind = 'RecordChanged'
      AND after_state ->> 'storage_model' = 'resolver_record_id'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
$ddl$;

EXECUTE $ddl$
CREATE INDEX IF NOT EXISTS normalized_events_record_id_link_idx
    ON bigname_phase.normalized_events (lower(after_state ->> 'resolver'), chain_id)
    WHERE event_kind = 'ResolverRecordLinked'
      AND after_state ->> 'storage_model' = 'resolver_record_id'
      AND consumer_visibility = 'activated'
      AND canonicality_state IN ('canonical', 'safe', 'finalized')
$ddl$;
END
$migration$;
