use sqlx::{Postgres, QueryBuilder};

/// Join the event's own block as `rb`, by the lineage primary key. The subquery reads `ne` and
/// carries `OFFSET 0`, so PostgreSQL cannot flatten it into a plain join. Every plan it has for
/// the subquery then takes `ne`'s row as a parameter, and such an inner side admits only a nested
/// loop that runs the subquery per event row: no hash, merge or materialized scan of the lineage.
pub(super) fn push_history_lineage_probe(builder: &mut QueryBuilder<'_, Postgres>) {
    builder.push(
        r#"
        LEFT JOIN LATERAL (
            SELECT rb.block_timestamp, rb.canonicality_state
            FROM bigname_phase.chain_lineage rb
            WHERE rb.chain_id = ne.chain_id
              AND rb.block_hash = ne.block_hash
            OFFSET 0
        ) rb ON TRUE
        "#,
    );
}

/// The block timestamp of `event`'s own block, read by the lineage primary key. Like the
/// statement's `LEFT JOIN chain_lineage rb`, it is NULL when the event has no block or its block
/// has no lineage row. A predicate that holds a subquery reads the block through this probe,
/// never through `rb`. PostgreSQL evaluates a predicate on the nullable side of an outer join at
/// the join and costs its subqueries once per joined row there. It then prefers hashing every
/// lineage row of the chain to probing the key once per candidate row.
pub(super) fn event_block_timestamp(event: &str) -> String {
    format!(
        "(SELECT event_block.block_timestamp
          FROM bigname_phase.chain_lineage event_block
          WHERE event_block.chain_id = {event}.chain_id
            AND event_block.block_hash = {event}.block_hash)"
    )
}

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
