//! The child listing's preimage observations from other chains (TYR-455). `label_preimages` is
//! one table for every chain, so a label first observed on chain A also spells the stored names
//! of chain B that still hold it as `[labelhash]`. Each family step of B reads A's preimage
//! observations above B's watermark for A (`project_child_preimage_watermark`), finds B's rows
//! that hold one of their labels through `project_child_listing_unhealed_idx`, and derives them
//! again at B's block. The watermark advances, journalled with B's block, in B's transaction
//! under B's marker lock: B writes only its own rows.
//!
//! - A's Interpret writes a batch's observations and their preimages in one transaction, over
//!   any number of blocks above its last, so B's read sees a batch whole or not at all: a batch
//!   that commits between the read's statements lands above the cutoff the first one took, and
//!   the next step reads it. The watermark moves only to a block at or below that cutoff.
//! - The row keeps A's Interpret revision (content hash and redo attempt) it was read under. An
//!   Interpret redo of A alone, such as a manifest-authority redo, re-derives blocks below the
//!   floor and can reveal labels there, so a step that finds another revision reads again from
//!   below the start of A's open Project redo, which the redo stamps from its first effect, or
//!   from block 0 when none is open. While A's redo is in progress the step reads and does not
//!   move the watermark.
//! - The watermark advances to A's finality floor (its finalized block, else its safe block) or
//!   the highest block read, whichever is lower, and never back. A reorg of A deletes and
//!   re-interprets A's events from the fork up (interpret `write.rs`), at block numbers B may
//!   have read; above the floor they are read again at every step until A finalizes them, so a
//!   label revealed only in a replacement block heals B too. Production always has the floor:
//!   Ingest and live follow refuse a node that reports no safe or finalized head
//!   (`require_checkpoint_heads`). A source with neither marker, which only a fixture or a head
//!   row before its first checkpoint has, is read but neither seeds nor advances a watermark.
//! - Below the floor, one read takes about [`SOURCE_EVENTS`] observations of a source, in whole
//!   blocks: the cutoff is the highest block among them and every observation up to it is read,
//!   so a block that alone holds more is read whole. A read from below catches up over its
//!   blocks. At the floor, the read takes the whole tail above it, which A's finality
//!   lag bounds, so a dense tail heals at the next block too.
//! - Undoing an A block later does not un-heal B's names: a preimage is true whichever block
//!   revealed it, and `label_preimages` keeps it.
//! - Undoing B's block restores the watermark from the journal. Reading an A block again derives
//!   rows that already spell its labels again, which changes nothing.
//! - A source without a watermark is read from block 0. A rebuild's reset clears the watermarks
//!   and its composition pass starts them again at each source's highest observation, or its
//!   floor if lower ([`seed`]): the pass composes every row from the preimages already stored.
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result, families::input};

/// The observations of one source one family step reads below its floor, rounded up to whole
/// blocks.
const SOURCE_EVENTS: i64 = 1000;

/// Every other chain with heads, its finality floor (finalized, else safe; null with neither),
/// and chain `$1`'s watermark row for it: whether there is one, its block and the other chain's
/// Interpret revision it was read under.
const SOURCES: &str = "/* project:families.derived.child_listing_sources */
    SELECT head.chain_id,
           COALESCE(head.finalized_block_number, head.safe_block_number),
           watermark.chain_id IS NOT NULL,
           watermark.source_block_number,
           watermark.source_input_content_hash,
           watermark.source_redo_attempt_generation
    FROM chain_heads head
    LEFT JOIN project_child_preimage_watermark watermark
      ON watermark.chain_id = $1 AND watermark.source_chain_id = head.chain_id
    WHERE head.chain_id <> $1
    ORDER BY head.chain_id";

/// The highest block of chain `$1` among its first `$3` preimage observations above block `$2`
/// (every one with a null `$3`): the activated rows of a canonical, safe or finalized block
/// whose block is still on the chain's readable lineage, so rows of a fork Ingest has orphaned
/// and Interpret has not replaced yet are not read. Each state is probed on its own, so
/// `normalized_events_projection_idx` reads the blocks in order and stops at the limit.
const CUTOFF: &str = "/* project:families.derived.child_listing_source_cutoff */
    SELECT max(first.block_number) FROM (
        SELECT event.block_number
        FROM unnest('{canonical,safe,finalized}'::canonicality_state[]) state (canonicality_state)
        CROSS JOIN LATERAL (
            SELECT observed.block_number
            FROM normalized_events observed
            WHERE observed.event_kind = 'PreimageObserved'
              AND observed.canonicality_state = state.canonicality_state
              AND observed.chain_id = $1 AND observed.block_number > $2
              AND observed.consumer_visibility = 'activated'
              AND observed.logical_name_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM chain_lineage lineage
                  WHERE lineage.chain_id = observed.chain_id
                    AND lineage.block_hash = observed.block_hash
                    AND lineage.block_number = observed.block_number
                    AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              )
            ORDER BY observed.block_number
            LIMIT $3
        ) event
        ORDER BY event.block_number
        LIMIT $3
    ) first";

/// The rows of chain `$1` whose stored name still holds a label of a name chain `$2` observed
/// in blocks `$3` (exclusive) to `$4`. Each observed name's surface is probed by its key.
const HOLDERS: &str = "/* project:families.derived.child_listing_source_holders */
    WITH observed AS MATERIALIZED (
        SELECT array_agg(DISTINCT lower(label.labelhash)) AS labelhashes
        FROM unnest('{canonical,safe,finalized}'::canonicality_state[]) state (canonicality_state)
        CROSS JOIN LATERAL (
            SELECT observed.logical_name_id
            FROM normalized_events observed
            WHERE observed.event_kind = 'PreimageObserved'
              AND observed.canonicality_state = state.canonicality_state
              AND observed.chain_id = $2
              AND observed.block_number > $3 AND observed.block_number <= $4
              AND observed.consumer_visibility = 'activated'
              AND observed.logical_name_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM chain_lineage lineage
                  WHERE lineage.chain_id = observed.chain_id
                    AND lineage.block_hash = observed.block_hash
                    AND lineage.block_number = observed.block_number
                    AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              )
            OFFSET 0
        ) event
        CROSS JOIN LATERAL (
            SELECT surface.labelhashes FROM name_surfaces surface
            WHERE surface.logical_name_id = event.logical_name_id
            OFFSET 0
        ) surface
        CROSS JOIN LATERAL unnest(surface.labelhashes) label (labelhash)
    )
    SELECT listing.child_logical_name_id FROM project_child_listing listing
    WHERE listing.chain_id = $1 AND listing.unhealed_labelhashes <> '{}'
      AND listing.unhealed_labelhashes && (SELECT observed.labelhashes FROM observed)
    ORDER BY 1";

/// The journal row of chain `$1`'s watermark for source `$4` at block `$2` (hash `$3`), with
/// the watermark as it stands (none for a first read), keyed as `store::key_text` keys it.
const JOURNAL: &str = "/* project:families.derived.child_listing_source_journal */
    INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key, before_image)
    SELECT $1, $2, $3, 'project_child_preimage_watermark',
           '[' || to_json($1::text)::text || ',' || to_json($4::text)::text || ']',
           (SELECT to_jsonb(watermark) FROM project_child_preimage_watermark watermark
            WHERE watermark.chain_id = $1 AND watermark.source_chain_id = $4)";

const ADVANCE: &str = "/* project:families.derived.child_listing_source_advance */
    INSERT INTO project_child_preimage_watermark
        (chain_id, source_chain_id, source_block_number, source_input_content_hash,
         source_redo_attempt_generation)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (chain_id, source_chain_id)
    DO UPDATE SET source_block_number = EXCLUDED.source_block_number,
                  source_input_content_hash = EXCLUDED.source_input_content_hash,
                  source_redo_attempt_generation = EXCLUDED.source_redo_attempt_generation";

/// The highest block of chain `$1` with a preimage observation, read as `CUTOFF` reads them:
/// the start of a rebuild's watermarks, below which its composition pass composes every row
/// from the preimages already stored.
const TOP: &str = "/* project:families.derived.child_listing_source_top */
    SELECT max(event.block_number)
    FROM unnest('{canonical,safe,finalized}'::canonicality_state[]) state (canonicality_state)
    CROSS JOIN LATERAL (
        SELECT observed.block_number
        FROM normalized_events observed
        WHERE observed.event_kind = 'PreimageObserved'
          AND observed.canonicality_state = state.canonicality_state
          AND observed.chain_id = $1
          AND observed.consumer_visibility = 'activated'
          AND observed.logical_name_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM chain_lineage lineage
              WHERE lineage.chain_id = observed.chain_id
                AND lineage.block_hash = observed.block_hash
                AND lineage.block_number = observed.block_number
                AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
          )
        ORDER BY observed.block_number DESC
        LIMIT 1
    ) event";

/// What a family step read from the other chains: the rows to derive again, and the watermark
/// rows to write, each with the source's Interpret revision.
#[derive(Debug, Default)]
pub(super) struct Read {
    pub(super) holders: Vec<String>,
    advances: Vec<(String, i64, input::Revision)>,
}

/// How a step treats one source: the block it reads above, and whether it writes the watermark
/// (`reset` when it must even with nothing read, `advances` when it may move it at all).
#[derive(Debug, Eq, PartialEq)]
struct Plan {
    after: i64,
    reset: bool,
    advances: bool,
}

/// The plan for a source whose watermark row holds `stored` (block and revision; none without
/// a row), whose Interpret revision is now `current` (none while it is in redo), and whose open
/// Project redo starts at `redo_from`. While the source is in redo the step reads and does not
/// move. A changed revision means the source's Interpret re-derived blocks the watermark may
/// stand above, so the step reads again from below the redo's start, or from block 0 when the
/// token records none.
fn plan(
    stored: Option<(i64, &input::Revision)>,
    current: Option<&input::Revision>,
    redo_from: Option<i64>,
) -> Plan {
    let watermark = stored.map_or(-1, |(block, _)| block);
    let Some(current) = current else {
        return Plan {
            after: watermark,
            reset: false,
            advances: false,
        };
    };
    match stored {
        Some((_, revision)) if revision != current => Plan {
            after: redo_from
                .map_or(-1, |from| from.saturating_sub(1).max(-1))
                .min(watermark),
            reset: true,
            advances: true,
        },
        _ => Plan {
            after: watermark,
            reset: false,
            advances: true,
        },
    }
}

/// Whether a read above `after` takes the whole tail: only at or above the floor.
fn whole_tail(after: i64, floor: Option<i64>) -> bool {
    floor.is_some_and(|floor| after >= floor + 1_000_000)
}

/// The block the watermark moves to after a read above `after` through `cutoff`: the floor or
/// the cutoff, whichever is lower, when above `after`. None leaves it as it is.
fn advance_to(after: i64, cutoff: i64, floor: Option<i64>) -> Option<i64> {
    let bound = cutoff.min(floor?);
    (bound > after).then_some(bound)
}

/// The block a step writes for a source it planned with `plan` and read through `cutoff` (none
/// with nothing above `plan.after`); none writes nothing.
fn written(plan: &Plan, cutoff: Option<i64>, floor: Option<i64>) -> Option<i64> {
    if !plan.advances {
        return None;
    }
    cutoff
        .and_then(|cutoff| advance_to(plan.after, cutoff, floor))
        .or(plan.reset.then_some(plan.after))
}

type SourceRow = (
    String,
    Option<i64>,
    bool,
    Option<i64>,
    Option<String>,
    Option<i64>,
);

async fn sources(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<Vec<SourceRow>> {
    sqlx::query_as(SOURCES)
        .bind(chain_id)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read the child listing's source chains", error)
        })
}

/// Read every other chain's preimage observations above chain `chain_id`'s watermarks, and the
/// rows of `chain_id` that hold one of their labels.
pub(super) async fn read(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<Read> {
    let mut read = Read::default();
    for (source, floor, has_row, block, hash, attempt) in sources(transaction, chain_id).await? {
        let revision = (hash, attempt);
        let stored = has_row.then_some((block.unwrap_or(-1), &revision));
        let token = input::token_in(transaction, &source).await?;
        let current = token.revision();
        let redo_from = token
            .project_redo_mode
            .as_ref()
            .and(token.project_redo_from);
        let plan = plan(stored, current.as_ref(), redo_from);
        let after = plan.after;
        let limit = (!whole_tail(after, floor)).then_some(SOURCE_EVENTS);
        let cutoff: Option<i64> = sqlx::query_scalar(CUTOFF)
            .bind(&source)
            .bind(after)
            .bind(limit)
            .fetch_one(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database(
                    format!("failed to read the preimage observations of {source}"),
                    error,
                )
            })?;
        if let (Some(to), Some(current)) = (written(&plan, cutoff, floor), current) {
            read.advances.push((source.clone(), to, current));
        }
        let Some(cutoff) = cutoff else {
            continue;
        };
        let holders: Vec<String> = sqlx::query_scalar(HOLDERS)
            .bind(chain_id)
            .bind(&source)
            .bind(after)
            .bind(cutoff)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database(
                    format!("failed to read the rows {source}'s observations heal"),
                    error,
                )
            })?;
        read.holders.extend(holders);
    }
    read.holders.sort();
    read.holders.dedup();
    Ok(read)
}

/// Start chain `chain_id`'s watermark for every other chain with a finality floor and no open
/// Interpret redo at that chain's highest observation, or its floor if lower, at the start of a
/// rebuild's composition pass: the pass composes every row from the preimages already stored,
/// so the steps after it read only what follows, and what a reorg can still replace. The reset
/// cleared the rows, and the pass journals nothing. Returns the rows written.
pub(crate) async fn seed(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<u64> {
    let mut rows = 0;
    for (source, floor, ..) in sources(transaction, chain_id).await? {
        let Some(floor) = floor else {
            continue;
        };
        let token = input::token_in(transaction, &source).await?;
        let Some(revision) = token.revision() else {
            continue;
        };
        let top: Option<i64> = sqlx::query_scalar(TOP)
            .bind(&source)
            .fetch_one(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database(
                    format!("failed to read the last preimage observation of {source}"),
                    error,
                )
            })?;
        let Some(top) = top else {
            continue;
        };
        let (hash, attempt) = revision;
        rows += sqlx::query(ADVANCE)
            .bind(chain_id)
            .bind(&source)
            .bind(top.min(floor))
            .bind(hash)
            .bind(attempt)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to start a child listing watermark", error)
            })?
            .rows_affected();
    }
    Ok(rows)
}

/// Journal (when the block journals) and advance chain `chain_id`'s watermark for every source
/// `read` moves. Returns the watermark rows written and the undo rows journalled.
pub(super) async fn advance(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    block: &input::BlockHeader,
    read: &Read,
) -> Result<(u64, u64)> {
    let mut undo_rows = 0;
    for (source, to, (hash, attempt)) in &read.advances {
        if block.journal {
            undo_rows += sqlx::query(JOURNAL)
                .bind(chain_id)
                .bind(block.number)
                .bind(&block.hash)
                .bind(source)
                .execute(&mut **transaction)
                .await
                .map_err(|error| {
                    ProjectError::database("failed to journal a child listing watermark", error)
                })?
                .rows_affected();
        }
        sqlx::query(ADVANCE)
            .bind(chain_id)
            .bind(source)
            .bind(to)
            .bind(hash)
            .bind(attempt)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to advance a child listing watermark", error)
            })?;
    }
    let rows = u64::try_from(read.advances.len()).unwrap_or(u64::MAX);
    Ok((rows, undo_rows))
}

#[cfg(test)]
mod tests {
    use super::{Plan, advance_to, plan, whole_tail, written};

    // Row 7: after a read, the watermark is at most the floor (finalized, else safe) and the
    // cutoff, and never moves back. With no floor it does not move.
    #[test]
    fn the_watermark_stays_at_or_below_the_floor_and_the_cutoff_and_never_moves_back() {
        for after in [-1, 0, 4, 9, 12] {
            for cutoff in [0, 3, 9, 15] {
                for floor in [None, Some(0), Some(5), Some(9), Some(20)] {
                    let to = advance_to(after, cutoff, floor);
                    if let Some(to) = to {
                        let floor = floor.expect("only a floor moves the watermark");
                        assert!(to <= floor && to <= cutoff, "{after} {cutoff} {floor}");
                        assert!(to > after, "{after} {cutoff} {floor}");
                    }
                    if floor.is_none() {
                        assert!(to.is_none(), "no floor, no move");
                    }
                }
            }
        }
        assert_eq!(advance_to(-1, 15, Some(2)), Some(2));
        assert!(advance_to(2, 15, Some(2)).is_none());
        assert_eq!(advance_to(2, 5, Some(9)), Some(5));
        assert!(advance_to(9, 5, Some(9)).is_none());
    }

    #[test]
    fn only_a_read_at_or_above_the_floor_takes_the_whole_tail() {
        assert!(!whole_tail(-1, Some(0)));
        assert!(!whole_tail(4, Some(5)));
        assert!(whole_tail(5, Some(5)));
        assert!(whole_tail(6, Some(5)));
        assert!(!whole_tail(6, None));
    }

    // Row 11's plan: a changed Interpret revision reads again from below the open Project redo's
    // start, or from block 0, and writes the watermark even with nothing read; an open
    // Interpret redo reads and writes nothing; the same revision reads on.
    #[test]
    fn a_changed_revision_reads_again_and_an_open_redo_does_not_move() {
        let old = (Some("a".to_owned()), Some(1));
        let new = (Some("a".to_owned()), Some(2));
        let same = plan(Some((9, &old)), Some(&old), Some(4));
        assert_eq!(
            same,
            Plan {
                after: 9,
                reset: false,
                advances: true,
            }
        );
        let from_redo = plan(Some((9, &old)), Some(&new), Some(4));
        assert_eq!(
            from_redo,
            Plan {
                after: 3,
                reset: true,
                advances: true,
            }
        );
        assert_eq!(plan(Some((9, &old)), Some(&new), None).after, -1);
        assert_eq!(plan(Some((2, &old)), Some(&new), Some(9)).after, 2);
        assert_eq!(plan(Some((9, &old)), Some(&new), Some(0)).after, -1);
        let open = plan(Some((9, &old)), None, Some(4));
        assert!(!open.advances && open.after == 9);
        assert!(written(&open, Some(15), Some(20)).is_none());
        assert_eq!(written(&from_redo, None, Some(20)), Some(3));
        assert_eq!(written(&from_redo, Some(15), Some(20)), Some(15));
        assert_eq!(plan(None, Some(&new), Some(4)).after, -1);
        assert!(written(&same, None, Some(20)).is_none());
    }
}
