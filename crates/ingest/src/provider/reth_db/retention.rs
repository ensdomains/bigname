use anyhow::Result;
use reth_ethereum::provider::{BlockNumReader, StaticFileProviderFactory, StaticFileSegment};

use super::EthereumRethProviderFactory;

/// What a reth datadir reports about the history it still holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RetentionReadings {
    /// Lowest block whose history has not expired, as reth's own RPC layer reads it
    /// (upstream: .refs/reth/crates/storage/provider/src/providers/database/mod.rs:L705 @ reth@88505c7f).
    pub(super) earliest_history_block: u64,
    /// Lowest block covered by a receipt static file, when receipts are stored there.
    /// Pruning static-file receipts deletes whole jars below the configured block
    /// (upstream: .refs/reth/crates/prune/prune/src/segments/receipts.rs:L34 @ reth@88505c7f)
    /// (upstream: .refs/reth/crates/prune/prune/src/segments/mod.rs:L41 @ reth@88505c7f),
    /// which leaves headers readable while every log below the boundary is gone.
    ///
    /// This reports the lowest jar on disk, so it bounds nothing on a node that keeps
    /// receipts in database tables — receipt pruning without `storage_v2`, or a receipt
    /// log filter with it
    /// (upstream: .refs/reth/crates/storage/provider/src/either_writer.rs:L188 @ reth@88505c7f)
    /// (upstream: .refs/reth/crates/storage/provider/src/either_writer.rs:L190 @ reth@88505c7f)
    /// — whose rows are pruned against a checkpoint this does not read, leaving that
    /// configuration bounded only by `earliest_history_block`.
    pub(super) lowest_receipt_block: Option<u64>,
}

pub(super) fn read_retention(factory: &EthereumRethProviderFactory) -> Result<RetentionReadings> {
    Ok(RetentionReadings {
        earliest_history_block: factory.earliest_block_number()?,
        lowest_receipt_block: factory
            .static_file_provider()
            .get_lowest_range_start(StaticFileSegment::Receipts),
    })
}

/// Lowest block this datadir can still answer log reads for.
pub(super) fn earliest_servable_block(readings: RetentionReadings) -> u64 {
    readings
        .lowest_receipt_block
        .unwrap_or(0)
        .max(readings.earliest_history_block)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pruned_receipt_segments_raise_the_floor_above_expired_history() {
        assert_eq!(
            earliest_servable_block(RetentionReadings {
                earliest_history_block: 0,
                lowest_receipt_block: Some(15_500_000),
            }),
            15_500_000
        );
    }

    #[test]
    fn expired_history_raises_the_floor_above_readable_receipts() {
        assert_eq!(
            earliest_servable_block(RetentionReadings {
                earliest_history_block: 15_537_394,
                lowest_receipt_block: Some(15_500_000),
            }),
            15_537_394
        );
    }

    #[test]
    fn a_node_that_kept_everything_reports_no_floor() {
        assert_eq!(
            earliest_servable_block(RetentionReadings {
                earliest_history_block: 0,
                lowest_receipt_block: Some(0),
            }),
            0
        );
        assert_eq!(
            earliest_servable_block(RetentionReadings {
                earliest_history_block: 0,
                lowest_receipt_block: None,
            }),
            0
        );
    }
}
