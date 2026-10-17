use std::num::{NonZeroU32, NonZeroU64};

/// Bounds the raw input of each batch an opted-in redo plans.
///
/// Byte limits describe an estimate of loaded inputs, not a process RSS limit: adapter
/// state, prior history the loader restores, prepared output, SQL buffers and allocator
/// overhead are additional memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdaptiveBatchConfig {
    pub target_logs: NonZeroU64,
    pub target_input_bytes: NonZeroU64,
    pub max_pending_input_bytes: NonZeroU64,
    pub max_blocks: NonZeroU32,
}

impl Default for AdaptiveBatchConfig {
    fn default() -> Self {
        Self {
            target_logs: NonZeroU64::new(200_000).unwrap(),
            target_input_bytes: NonZeroU64::new(128 * 1024 * 1024).unwrap(),
            max_pending_input_bytes: NonZeroU64::new(1024 * 1024 * 1024).unwrap(),
            max_blocks: NonZeroU32::new(131_072).unwrap(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlockWork {
    pub number: i64,
    pub hash: String,
    pub raw_logs: u64,
    pub estimated_input_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PlannedBatch {
    pub markers: Vec<(i64, String)>,
    pub raw_logs: u64,
    pub estimated_input_bytes: u64,
    /// A single block exceeds a target and occupies a window alone for progress.
    pub oversized_single_block: bool,
}

/// Return an ordered prefix of whole blocks, split before a work bound is exceeded.
///
/// Blocks after the window are left unplanned: the caller plans the next window from
/// the block after the last one it commits. An oversized
/// block is admitted for progress, but occupies its own window, not just its own batch.
/// If an API caller sets the pending budget below the byte target, that smaller budget
/// becomes the effective per-batch byte target, however many batches the window holds.
pub(super) fn partition_borrowed(
    blocks: &[BlockWork],
    config: AdaptiveBatchConfig,
    max_batches: usize,
) -> Vec<PlannedBatch> {
    if max_batches == 0 {
        return Vec::new();
    }
    let mut batches = Vec::new();
    let mut current = PlannedBatch::default();
    let mut pending_bytes = 0;
    let byte_target = config
        .target_input_bytes
        .get()
        .min(config.max_pending_input_bytes.get());
    for block in blocks {
        let oversized =
            block.raw_logs > config.target_logs.get() || block.estimated_input_bytes > byte_target;
        if oversized {
            if !current.markers.is_empty()
                && !admit(
                    std::mem::take(&mut current),
                    &mut batches,
                    &mut pending_bytes,
                    config.max_pending_input_bytes.get(),
                    max_batches,
                )
            {
                break;
            }
            if batches.is_empty() {
                batches.push(PlannedBatch {
                    markers: vec![(block.number, block.hash.clone())],
                    raw_logs: block.raw_logs,
                    estimated_input_bytes: block.estimated_input_bytes,
                    oversized_single_block: true,
                });
            }
            break;
        }
        let starts_next_batch = !current.markers.is_empty()
            && (exceeds(current.raw_logs, block.raw_logs, config.target_logs.get())
                || exceeds(
                    current.estimated_input_bytes,
                    block.estimated_input_bytes,
                    byte_target,
                )
                || exceeds_block_span(&current, block.number, config.max_blocks.get()));
        if starts_next_batch
            && !admit(
                std::mem::take(&mut current),
                &mut batches,
                &mut pending_bytes,
                config.max_pending_input_bytes.get(),
                max_batches,
            )
        {
            break;
        }
        current.raw_logs += block.raw_logs;
        current.estimated_input_bytes += block.estimated_input_bytes;
        current.markers.push((block.number, block.hash.clone()));
    }
    if !current.markers.is_empty() {
        admit(
            current,
            &mut batches,
            &mut pending_bytes,
            config.max_pending_input_bytes.get(),
            max_batches,
        );
    }
    batches
}

/// Admit whole natural batches so the window size and pending memory never change
/// transaction boundaries. A batch that does not fit is deferred in its entirety.
fn admit(
    batch: PlannedBatch,
    batches: &mut Vec<PlannedBatch>,
    pending_bytes: &mut u64,
    pending_limit: u64,
    max_batches: usize,
) -> bool {
    if exceeds(*pending_bytes, batch.estimated_input_bytes, pending_limit) {
        return false;
    }
    *pending_bytes += batch.estimated_input_bytes;
    batches.push(batch);
    batches.len() < max_batches
}

#[cfg(test)]
pub(super) fn partition(
    blocks: Vec<BlockWork>,
    config: AdaptiveBatchConfig,
    max_batches: usize,
) -> Vec<PlannedBatch> {
    partition_borrowed(&blocks, config, max_batches)
}

fn exceeds(current: u64, additional: u64, limit: u64) -> bool {
    current
        .checked_add(additional)
        .is_none_or(|sum| sum > limit)
}

fn exceeds_block_span(batch: &PlannedBatch, number: i64, max_blocks: u32) -> bool {
    batch.markers.len() >= max_blocks as usize
        || number
            .checked_sub(batch.markers[0].0)
            .and_then(|span| span.checked_add(1))
            .is_none_or(|span| span <= 0 || span > i64::from(max_blocks))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(logs: u64, bytes: u64, pending: u64, blocks: u32) -> AdaptiveBatchConfig {
        AdaptiveBatchConfig {
            target_logs: NonZeroU64::new(logs).unwrap(),
            target_input_bytes: NonZeroU64::new(bytes).unwrap(),
            max_pending_input_bytes: NonZeroU64::new(pending).unwrap(),
            max_blocks: NonZeroU32::new(blocks).unwrap(),
        }
    }

    fn block(number: i64, logs: u64, bytes: u64) -> BlockWork {
        BlockWork {
            number,
            hash: format!("block-{number}"),
            raw_logs: logs,
            estimated_input_bytes: bytes,
        }
    }

    fn numbers(batches: &[PlannedBatch]) -> Vec<Vec<i64>> {
        batches
            .iter()
            .map(|batch| batch.markers.iter().map(|marker| marker.0).collect())
            .collect()
    }

    #[test]
    fn splits_before_log_or_byte_targets_without_splitting_blocks() {
        let work = vec![
            block(1, 4, 2),
            block(2, 6, 2),
            block(3, 1, 6),
            block(4, 1, 5),
        ];
        let batches = partition(work, config(10, 10, 100, 100), 10);
        assert_eq!(numbers(&batches), vec![vec![1, 2], vec![3], vec![4]]);
        assert_eq!(
            (batches[0].raw_logs, batches[0].estimated_input_bytes),
            (10, 4)
        );
        assert!(batches.iter().all(|batch| !batch.oversized_single_block));
    }

    #[test]
    fn zero_log_blocks_obey_span_and_preserve_gaps_and_hashes() {
        let work = vec![
            block(1, 0, 1),
            block(2, 0, 1),
            block(3, 0, 1),
            block(8, 0, 1),
        ];
        let batches = partition(work, config(10, 100, 100, 2), 10);
        assert_eq!(numbers(&batches), vec![vec![1, 2], vec![3], vec![8]]);
        assert_eq!(batches[2].markers[0].1, "block-8");
        assert_eq!(batches.iter().map(|batch| batch.raw_logs).sum::<u64>(), 0);
    }

    #[test]
    fn pending_bytes_bound_the_whole_window_and_batch_count_bounds_its_prefix() {
        let work: Vec<_> = (1..=5).map(|number| block(number, 4, 4)).collect();
        let batches = partition(work.clone(), config(4, 10, 10, 100), 10);
        assert_eq!(numbers(&batches), vec![vec![1], vec![2]]);
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.estimated_input_bytes)
                .sum::<u64>(),
            8
        );
        assert_eq!(
            numbers(&partition(work, config(4, 10, 100, 100), 1)),
            vec![vec![1]]
        );
    }

    #[test]
    fn oversized_block_waits_for_its_own_window_and_is_always_admitted() {
        let bounds = config(10, 10, 20, 100);
        let work = vec![block(1, 1, 1), block(2, 20, 30), block(3, 1, 1)];
        assert_eq!(numbers(&partition(work.clone(), bounds, 10)), vec![vec![1]]);
        let oversized = partition(work[1..].to_vec(), bounds, 10);
        assert_eq!(numbers(&oversized), vec![vec![2]]);
        assert!(oversized[0].oversized_single_block);
        assert_eq!(oversized[0].estimated_input_bytes, 30);
    }

    #[test]
    fn an_oversized_first_or_last_block_runs_alone() {
        let bounds = config(10, 10, 20, 100);
        let first = partition(vec![block(1, 11, 1), block(2, 1, 1)], bounds, 1);
        assert_eq!(numbers(&first), vec![vec![1]]);
        assert!(first[0].oversized_single_block);
        let last = vec![block(1, 1, 1), block(2, 1, 1), block(3, 1, 11)];
        let before = partition(last.clone(), bounds, 1);
        assert_eq!(numbers(&before), vec![vec![1, 2]]);
        assert!(!before[0].oversized_single_block);
        let alone = partition(last[2..].to_vec(), bounds, 1);
        assert_eq!(numbers(&alone), vec![vec![3]]);
        assert!(alone[0].oversized_single_block);
    }

    #[test]
    fn a_pending_limit_smaller_than_the_batch_target_still_makes_progress() {
        let batches = partition(
            vec![block(1, 1, 11), block(2, 1, 1)],
            config(10, 100, 10, 100),
            4,
        );
        assert_eq!(numbers(&batches), vec![vec![1]]);
        assert!(batches[0].oversized_single_block);
    }

    #[test]
    fn serial_and_parallel_windows_keep_the_same_natural_batches() {
        let work: Vec<_> = [6, 4, 4, 4, 2, 8, 1]
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| block(index as i64 + 1, 1, bytes))
            .collect();
        let bounds = config(100, 10, 15, 100);
        let replay = |workers| {
            let mut remaining = work.as_slice();
            let mut all = Vec::new();
            while !remaining.is_empty() {
                let window = partition_borrowed(remaining, bounds, workers);
                let consumed = window
                    .iter()
                    .map(|batch| batch.markers.len())
                    .sum::<usize>();
                assert!(consumed > 0);
                remaining = &remaining[consumed..];
                all.extend(window);
            }
            all
        };
        let serial = replay(1);
        assert_eq!(
            numbers(&serial),
            vec![vec![1, 2], vec![3, 4, 5], vec![6, 7]]
        );
        assert_eq!(serial, replay(8));
        assert_eq!(
            numbers(&partition_borrowed(&work[..4], bounds, 8)),
            vec![vec![1, 2]]
        );
    }

    #[test]
    fn repeated_windows_cover_every_marker_exactly_once_deterministically() {
        let original: Vec<_> = (0..40)
            .map(|number| block(number * 3, (number % 7) as u64, (number % 9 + 1) as u64))
            .collect();
        let bounds = config(5, 9, 16, 8);
        let mut remaining = original.clone();
        let mut observed = Vec::new();
        while !remaining.is_empty() {
            let batches = partition(remaining.clone(), bounds, 3);
            assert_eq!(batches, partition(remaining.clone(), bounds, 3));
            let consumed: Vec<_> = batches
                .into_iter()
                .flat_map(|batch| batch.markers)
                .collect();
            assert!(!consumed.is_empty());
            assert_eq!(consumed[0].0, remaining[0].number);
            remaining.drain(..consumed.len());
            observed.extend(consumed);
        }
        let expected: Vec<_> = original
            .into_iter()
            .map(|block| (block.number, block.hash))
            .collect();
        assert_eq!(observed, expected);
    }

    #[test]
    fn arithmetic_overflow_splits_instead_of_wrapping() {
        let batches = partition(
            vec![block(1, u64::MAX, u64::MAX), block(2, 1, 1)],
            config(u64::MAX, u64::MAX, u64::MAX, 100),
            10,
        );
        assert_eq!(numbers(&batches), vec![vec![1]]);
        let batches = partition(
            vec![block(1, u64::MAX, 1), block(2, 1, 1)],
            config(u64::MAX, u64::MAX, u64::MAX, 100),
            10,
        );
        assert_eq!(numbers(&batches), vec![vec![1], vec![2]]);
    }

    #[test]
    fn empty_work_or_zero_batch_capacity_has_no_plan() {
        assert!(partition(Vec::new(), AdaptiveBatchConfig::default(), 4).is_empty());
        assert!(partition(vec![block(1, 1, 1)], AdaptiveBatchConfig::default(), 0).is_empty());
    }
}
