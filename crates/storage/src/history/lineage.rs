// Canonical reads keep their existing predicates and partial-index plans. A
// noncanonical read must prove that retained evidence and the event lie on one
// parent-hash path; sharing a timestamp or a canonicality value is insufficient.
pub(super) fn same_fork_predicate(evidence: &str, event: &str, canonical_only: bool) -> String {
    if canonical_only {
        return "TRUE".to_owned();
    }
    format!(
        r#"EXISTS (
            WITH RECURSIVE history_fork AS (
                SELECT block.chain_id, block.block_hash, block.parent_hash,
                       block.block_number
                FROM bigname_phase.chain_lineage block
                WHERE {evidence}.chain_id = {event}.chain_id
                  AND block.chain_id = {event}.chain_id
                  AND block.block_number = GREATEST(
                      {evidence}.block_number, {event}.block_number
                  )
                  AND block.block_hash = CASE
                      WHEN {evidence}.block_number > {event}.block_number
                          THEN {evidence}.block_hash
                      ELSE {event}.block_hash
                  END
                UNION ALL
                SELECT parent.chain_id, parent.block_hash, parent.parent_hash,
                       parent.block_number
                FROM history_fork child
                JOIN bigname_phase.chain_lineage parent
                  ON parent.chain_id = child.chain_id
                 AND parent.block_hash = child.parent_hash
                WHERE child.block_number > LEAST(
                          {evidence}.block_number, {event}.block_number
                      )
                  AND parent.block_number >= LEAST(
                          {evidence}.block_number, {event}.block_number
                      )
                  AND parent.block_number < child.block_number
            )
            SELECT 1 FROM history_fork
            WHERE block_number = LEAST(
                      {evidence}.block_number, {event}.block_number
                  )
              AND block_hash = CASE
                  WHEN {evidence}.block_number > {event}.block_number
                      THEN {event}.block_hash
                  ELSE {evidence}.block_hash
              END
        )"#,
    )
}

// Each contributing block must be comparable with every block already selected.
pub(super) fn same_fork_as(evidence: &str, anchors: &[&str], canonical_only: bool) -> String {
    if canonical_only {
        return "TRUE".to_owned();
    }
    let predicates = anchors
        .iter()
        .filter(|anchor| **anchor != evidence)
        .map(|anchor| same_fork_predicate(evidence, anchor, false))
        .collect::<Vec<_>>();
    if predicates.is_empty() {
        "TRUE".to_owned()
    } else {
        predicates.join(" AND ")
    }
}
