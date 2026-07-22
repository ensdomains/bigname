use anyhow::Result;

use crate::backfill::BackfillBlockRange;

const STARTUP_HEARTBEAT_PROGRESS_BLOCKS: i64 = 32;

pub(super) fn heartbeat_progress_ranges(
    range: BackfillBlockRange,
    progress_requested: bool,
) -> Result<Vec<BackfillBlockRange>> {
    if !progress_requested {
        return Ok(vec![range]);
    }

    let mut ranges = Vec::new();
    let mut from_block = range.from_block;
    loop {
        let to_block = from_block
            .checked_add(STARTUP_HEARTBEAT_PROGRESS_BLOCKS - 1)
            .unwrap_or(range.to_block)
            .min(range.to_block);
        ranges.push(BackfillBlockRange::new(from_block, to_block)?);
        if to_block == range.to_block {
            return Ok(ranges);
        }
        from_block = to_block
            .checked_add(1)
            .expect("non-terminal progress range end cannot overflow");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS;

    #[test]
    fn startup_heartbeat_progress_splits_the_default_hash_pinned_chunk() -> Result<()> {
        let configured_chunk =
            BackfillBlockRange::new(1, DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS)?;

        let progress_ranges = heartbeat_progress_ranges(configured_chunk, true)?;

        assert!(
            progress_ranges.len() > 1,
            "startup liveness must advance before a full configured chunk completes"
        );
        assert_eq!(
            progress_ranges.first().map(|range| range.from_block),
            Some(1)
        );
        assert!(
            progress_ranges.iter().all(|range| {
                range.to_block - range.from_block < STARTUP_HEARTBEAT_PROGRESS_BLOCKS
            }),
            "startup progress units must stay within the documented 32-block bound"
        );
        assert_eq!(
            progress_ranges.last().map(|range| range.to_block),
            Some(DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS)
        );
        assert!(
            progress_ranges
                .windows(2)
                .all(|ranges| ranges[0].to_block + 1 == ranges[1].from_block),
            "progress ranges must cover the configured chunk contiguously"
        );
        Ok(())
    }

    #[test]
    fn ordinary_backfill_preserves_the_configured_chunk() -> Result<()> {
        let configured_chunk =
            BackfillBlockRange::new(1, DEFAULT_HASH_PINNED_BACKFILL_CHUNK_BLOCKS)?;

        assert_eq!(
            heartbeat_progress_ranges(configured_chunk, false)?,
            vec![configured_chunk]
        );
        Ok(())
    }
}
