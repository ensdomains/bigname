CREATE TEMP TABLE project_events ON COMMIT DROP AS
SELECT event.*
FROM {event_source}
LEFT JOIN chain_lineage lineage
  ON lineage.chain_id = event.chain_id
 AND lineage.block_hash = event.block_hash
 AND lineage.block_number = event.block_number
WHERE event.chain_id = $1
  AND event.consumer_visibility = 'activated'
  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
  AND (
      (event.block_number IS NULL AND event.block_hash IS NULL)
      OR (
          event.block_number <= $2
          AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
      )
  )
