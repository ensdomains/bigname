mod reads;
mod types;
pub use reads::{
    load_children_current, load_children_current_including_noncanonical,
    load_children_current_page, load_children_current_summaries,
};
pub use types::{
    ChildrenCurrentKeysetCursor, ChildrenCurrentPage, ChildrenCurrentRow, ChildrenCurrentSummary,
};

const DECLARED_SURFACE_CLASS: &str = "declared";

pub const DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS: &str = r#"
  JOIN bigname_phase.name_surfaces parent
    ON parent.logical_name_id = cc.parent_logical_name_id
  LEFT JOIN bigname_phase.name_surfaces child
    ON child.logical_name_id = cc.child_logical_name_id
  JOIN bigname_phase.chain_lineage parent_lineage
    ON parent_lineage.chain_id = parent.chain_id
   AND parent_lineage.block_hash = parent.block_hash
  LEFT JOIN bigname_phase.chain_lineage child_lineage
    ON child_lineage.chain_id = child.chain_id
   AND child_lineage.block_hash = child.block_hash
"#;

pub const DEFAULT_CHILDREN_CURRENT_READ_FILTER: &str = r#"
  AND cc.canonicality_summary ->> 'state' IN ('canonical', 'safe', 'finalized')
  AND EXISTS (
      SELECT 1
      FROM bigname_phase.chain_lineage projection_lineage
      WHERE projection_lineage.chain_id = cc.provenance ->> 'chain_id'
        AND projection_lineage.block_hash = cc.chain_positions ->> 'target_block_hash'
        AND projection_lineage.canonicality_state IN (
            'canonical'::bigname_phase.canonicality_state,
            'safe'::bigname_phase.canonicality_state,
            'finalized'::bigname_phase.canonicality_state
        )
  )
  AND parent.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND parent_lineage.canonicality_state IN (
      'canonical'::bigname_phase.canonicality_state,
      'safe'::bigname_phase.canonicality_state,
      'finalized'::bigname_phase.canonicality_state
  )
  AND (
      child.logical_name_id IS NULL
      OR cc.provenance #>> '{label,source}' = 'label_preimage'
      OR (
          child.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
          AND child_lineage.canonicality_state IN (
              'canonical'::bigname_phase.canonicality_state,
              'safe'::bigname_phase.canonicality_state,
              'finalized'::bigname_phase.canonicality_state
          )
      )
  )
"#;
