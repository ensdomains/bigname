pub const RESOURCE_CANONICALITY_JOINS: &str = r#"
JOIN bigname_phase.resources resource
  ON resource.resource_id = ric.resource_id
JOIN bigname_phase.chain_lineage resource_lineage
  ON resource_lineage.chain_id = resource.chain_id
 AND resource_lineage.block_hash = resource.block_hash
"#;

pub const RECORD_INVENTORY_CANONICALITY_SUMMARY_FILTER: &str = r#"
  AND ric.canonicality_summary ->> 'state' = 'canonical_lineage'
"#;

pub const RECORD_INVENTORY_PROJECTION_LINEAGE_FILTER: &str = r#"
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.chain_lineage projection_lineage
      WHERE projection_lineage.chain_id = ric.provenance ->> 'chain_id'
        AND projection_lineage.block_hash = ric.chain_positions ->> 'target_block_hash'
        AND projection_lineage.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
  )
"#;

/// A registration whose resolver pointer was cleared publishes a history-only inventory row: it
/// carries the `provenance.attributed_event_ids` that registration-scoped name history reads back,
/// and nothing else. It selects no records, so every record-serving read excludes it and a cleared
/// name answers exactly as it does with no row at all (`inventory_not_available`).
pub const RECORD_INVENTORY_RECORD_SERVING_FILTER: &str = r#"
  AND ric.provenance ->> 'record_serving' IS DISTINCT FROM 'false'
"#;

pub const RECORD_INVENTORY_RESOURCE_CANONICALITY_FILTER: &str = r#"
  AND resource.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
"#;

pub const RECORD_INVENTORY_RESOURCE_LINEAGE_FILTER: &str = r#"
  AND resource_lineage.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
"#;

pub(crate) struct DefaultRecordInventoryCurrentReadFilter;

pub(crate) const DEFAULT_RECORD_INVENTORY_CURRENT_READ_FILTER:
    DefaultRecordInventoryCurrentReadFilter = DefaultRecordInventoryCurrentReadFilter;

impl Display for DefaultRecordInventoryCurrentReadFilter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        for fragment in [
            RECORD_INVENTORY_CANONICALITY_SUMMARY_FILTER,
            RECORD_INVENTORY_PROJECTION_LINEAGE_FILTER,
            RECORD_INVENTORY_RESOURCE_CANONICALITY_FILTER,
            RECORD_INVENTORY_RESOURCE_LINEAGE_FILTER,
            RECORD_INVENTORY_RECORD_SERVING_FILTER,
        ] {
            formatter.write_str(fragment)?;
        }
        Ok(())
    }
}
use std::fmt::{self, Display, Formatter};
