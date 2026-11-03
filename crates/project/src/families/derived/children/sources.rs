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
//! - An A Interpret redo that re-derives blocks below the floor and reveals a label the first
//!   derivation did not is not read again; the Project redo that a re-derivation release runs on
//!   every chain resets B's watermarks and reads it.
//! - The watermark advances to A's finality floor (its finalized block, else its safe block) or
//!   the highest block read, whichever is lower, and never back. A reorg of A deletes and
//!   re-interprets A's events from the fork up (interpret `write.rs`), at block numbers B may
//!   have read; above the floor they are read again at every step until A finalizes them, so a
//!   label revealed only in a replacement block heals B too. Production always has the floor:
//!   Ingest and live follow refuse a node that reports no safe or finalized head
//!   (`require_checkpoint_heads`). A source with neither marker, which only a fixture or a head
//!   row before its first checkpoint has, neither seeds nor advances a watermark.
//! - Below the floor, one read takes about [`SOURCE_EVENTS`] observations of a source, in whole
//!   blocks: the cutoff is the highest block among them and every observation up to it is read,
//!   so a block that alone holds more is read whole. A rebuild that starts from 0 catches up
//!   over its blocks. At the floor, the read takes the whole tail above it, which A's finality
//!   lag bounds, so a dense tail heals at the next block too.
//! - Undoing an A block later does not un-heal B's names: a preimage is true whichever block
//!   revealed it, and `label_preimages` keeps it.
//! - Undoing B's block restores the watermark from the journal. Reading an A block again derives
//!   rows that already spell its labels again, which changes nothing.
//! - A source without a watermark is read from block 0, so a rebuild, whose reset clears the
//!   watermarks, heals every label another chain has observed. The schema-migration that adds
//!   the table starts each chain at its sources' highest observation, or their floor if lower.
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result, families::input};

/// The observations of one source one family step reads below its floor, rounded up to whole
/// blocks.
const SOURCE_EVENTS: i64 = 1000;

/// Every other chain with heads, its finality floor (finalized, else safe; null with neither)
/// and chain `$1`'s watermark for it (null with none).
const SOURCES: &str = "/* project:families.derived.child_listing_sources */
    SELECT head.chain_id,
           COALESCE(head.finalized_block_number, head.safe_block_number),
           watermark.source_block_number
    FROM chain_heads head
    LEFT JOIN project_child_preimage_watermark watermark
      ON watermark.chain_id = $1 AND watermark.source_chain_id = head.chain_id
    WHERE head.chain_id <> $1
    ORDER BY head.chain_id";

/// The highest block of chain `$1` among its first `$3` preimage observations above block `$2`
/// (every one with a null `$3`): the activated rows of a canonical, safe or finalized block, as
/// the chain's own family step reads them. Each state is probed on its own, so
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
    INSERT INTO project_child_preimage_watermark (chain_id, source_chain_id, source_block_number)
    VALUES ($1, $2, $3)
    ON CONFLICT (chain_id, source_chain_id)
    DO UPDATE SET source_block_number = EXCLUDED.source_block_number";

/// What a family step read from the other chains: the rows to derive again, and the block each
/// source's watermark moves to.
#[derive(Debug, Default)]
pub(super) struct Read {
    pub(super) holders: Vec<String>,
    advances: Vec<(String, i64)>,
}

/// Whether a read above `watermark` takes the whole tail: only at or above the floor.
fn whole_tail(watermark: Option<i64>, floor: Option<i64>) -> bool {
    floor.is_some_and(|floor| watermark.is_some_and(|watermark| watermark >= floor))
}

/// The block the watermark moves to after a read through `cutoff`: the floor or the cutoff,
/// whichever is lower, when above the watermark. None leaves it as it is.
fn advance_to(watermark: Option<i64>, cutoff: i64, floor: Option<i64>) -> Option<i64> {
    let bound = cutoff.min(floor?);
    (bound > watermark.unwrap_or(-1)).then_some(bound)
}

/// Read every other chain's preimage observations above chain `chain_id`'s watermarks, and the
/// rows of `chain_id` that hold one of their labels.
pub(super) async fn read(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<Read> {
    let sources: Vec<(String, Option<i64>, Option<i64>)> = sqlx::query_as(SOURCES)
        .bind(chain_id)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read the child listing's source chains", error)
        })?;
    let mut read = Read::default();
    for (source, floor, watermark) in sources {
        let after = watermark.unwrap_or(-1);
        let limit = (!whole_tail(watermark, floor)).then_some(SOURCE_EVENTS);
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
        if let Some(to) = advance_to(watermark, cutoff, floor) {
            read.advances.push((source, to));
        }
    }
    read.holders.sort();
    read.holders.dedup();
    Ok(read)
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
    for (source, to) in &read.advances {
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
    use super::{advance_to, whole_tail};

    // Row 7: after a read, the watermark is at most the floor (finalized, else safe) and the
    // cutoff, and never moves back. With no floor it does not move.
    #[test]
    fn the_watermark_stays_at_or_below_the_floor_and_the_cutoff_and_never_moves_back() {
        for watermark in [None, Some(0), Some(4), Some(9), Some(12)] {
            for cutoff in [0, 3, 9, 15] {
                for floor in [None, Some(0), Some(5), Some(9), Some(20)] {
                    let to = advance_to(watermark, cutoff, floor);
                    let after = to.or(watermark);
                    if let Some(to) = to {
                        let floor = floor.expect("only a floor moves the watermark");
                        assert!(
                            to <= floor && to <= cutoff,
                            "{watermark:?} {cutoff} {floor}"
                        );
                        assert!(
                            to > watermark.unwrap_or(-1),
                            "{watermark:?} {cutoff} {floor}"
                        );
                    }
                    assert!(
                        after >= watermark,
                        "{watermark:?} moved back to {after:?} ({cutoff}, {floor:?})"
                    );
                    if floor.is_none() {
                        assert!(to.is_none(), "no floor, no move");
                    }
                }
            }
        }
        assert_eq!(advance_to(None, 15, Some(2)), Some(2));
        assert!(advance_to(Some(2), 15, Some(2)).is_none());
        assert_eq!(advance_to(Some(2), 5, Some(9)), Some(5));
        assert!(advance_to(Some(9), 5, Some(9)).is_none());
    }

    #[test]
    fn only_a_watermark_at_or_above_the_floor_reads_the_whole_tail() {
        assert!(!whole_tail(None, Some(0)));
        assert!(!whole_tail(Some(4), Some(5)));
        assert!(whole_tail(Some(5), Some(5)));
        assert!(whole_tail(Some(6), Some(5)));
        assert!(!whole_tail(Some(6), None));
    }
}
