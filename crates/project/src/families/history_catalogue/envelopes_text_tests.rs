//! The live catalogue's anchor envelope, frozen as it was before the composition pass's catalogue
//! build shared it: the live path must emit it byte for byte.
use super::bounds;

fn frozen(kind: i16) -> String {
    format!(
        "CROSS JOIN LATERAL (
      SELECT min(source.first_bucket) AS first_bucket,max(source.last_bucket) AS last_bucket,
        COALESCE(bit_or(source.event_mask),0) AS event_mask,
        COALESCE(bit_or(source.key_bloom),B'0'::bit(256)) AS key_bloom FROM (
          SELECT first_bucket,last_bucket,event_mask,key_bloom FROM project_history_source
          WHERE chain_id=$1 AND source_kind=$2 AND source_key=anchor.anchor_id
          {edges}
        ) source
    ) bounds",
        edges = if kind == 1 {
            "UNION ALL SELECT first_bucket,last_bucket,event_mask,key_bloom
        FROM project_history_source_edge WHERE chain_id=$1 AND resource_id=anchor.anchor_id::uuid"
        } else {
            ""
        }
    )
}

#[test]
fn the_live_anchor_envelope_is_unchanged() {
    for kind in [0, 1] {
        assert_eq!(bounds(kind, "anchor.anchor_id"), frozen(kind));
    }
}
