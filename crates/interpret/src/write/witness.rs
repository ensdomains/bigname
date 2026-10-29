//! The name surfaces of chain `$1` that keep raw labels without a readable
//! [preimage witness](../../../../docs/glossary.md#preimage-witness).
//!
//! Each set is read from its own small side, never from every surface of the chain:
//!
//! - The witness is NULL. A redo released it, or no observation survived.
//! - The witness event sits on an orphaned block. Lineage is never deleted and a written event's
//!   block can only become orphaned, so no other event has an unreadable block. Every writer
//!   points a witness at a `PreimageObserved` event of the surface's own name.
//! - The witness of a byte-oriented shadow, whose label bytes have no text decoding and are
//!   stored as an empty `raw_name`, names no readable event. Before a redo deletes its range's
//!   events it releases every witness among them except this shape's, which must keep one.
use bigname_adapters::schema_v2::seam::PREIMAGE_OBSERVATION_EVENT_KIND;

pub(super) const READABLE: &str = "('canonical', 'safe', 'finalized')";

/// The three sets as `SELECT logical_name_id` arms, in the order above. Each probe of another
/// table is a lateral subquery behind `LIMIT 1` or `OFFSET 0`, which the planner cannot merge
/// into a join, so no plan hashes every event of the chain to meet a few orphaned blocks. An
/// orphaned block's events are read by its hash alone, which names one block of the chain, so
/// the probe never reads the canonical block at the same height.
pub(super) fn arms() -> [String; 3] {
    [
        "SELECT released.logical_name_id
         FROM name_surfaces released
         WHERE released.chain_id = $1
           AND released.raw_name IS NOT NULL
           AND released.preimage_event_identity IS NULL"
            .to_owned(),
        format!(
            "SELECT orphaned.logical_name_id
             FROM chain_lineage lineage
             CROSS JOIN LATERAL (
                 SELECT witness.chain_id, witness.logical_name_id, witness.event_identity
                 FROM normalized_events witness
                 WHERE witness.chain_id = lineage.chain_id
                   AND witness.block_hash = lineage.block_hash
                   AND witness.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}'
                 OFFSET 0
             ) witness
             CROSS JOIN LATERAL (
                 SELECT surface.logical_name_id
                 FROM name_surfaces surface
                 WHERE surface.chain_id = witness.chain_id
                   AND surface.logical_name_id = witness.logical_name_id
                   AND surface.preimage_event_identity = witness.event_identity
                   AND surface.raw_name IS NOT NULL
                 LIMIT 1
             ) orphaned
             WHERE lineage.chain_id = $1
               AND lineage.canonicality_state = 'orphaned'"
        ),
        format!(
            "SELECT shadow.logical_name_id
             FROM name_surfaces shadow
             LEFT JOIN LATERAL (
                 SELECT witness.block_hash, witness.block_number
                 FROM normalized_events witness
                 WHERE witness.chain_id = shadow.chain_id
                   AND witness.event_identity = shadow.preimage_event_identity
                   AND witness.canonicality_state IN {READABLE}
                 LIMIT 1
             ) witness ON true
             LEFT JOIN LATERAL (
                 SELECT 1 AS found
                 FROM chain_lineage lineage
                 WHERE lineage.chain_id = shadow.chain_id
                   AND lineage.block_hash = witness.block_hash
                   AND lineage.block_number = witness.block_number
                   AND lineage.canonicality_state IN {READABLE}
                 LIMIT 1
             ) readable ON true
             WHERE shadow.chain_id = $1
               AND shadow.raw_name = ''
               AND shadow.visibility_state = 'shadow'
               AND readable.found IS NULL"
        ),
    ]
}

/// The union of the three sets. A surface can be in more than one.
pub(super) fn lost_witnesses() -> String {
    arms().join("\n UNION ALL\n ")
}

#[cfg(test)]
mod tests;
