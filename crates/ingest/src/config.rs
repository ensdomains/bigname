use crate::{IngestError, Result};

/// Bounded work sizes for RPC ingestion. These settings do not change fact selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngestConfig {
    blocks_per_batch: u32,
    rpc_batch_size: usize,
    rpc_max_in_flight: usize,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            blocks_per_batch: 256,
            rpc_batch_size: 32,
            rpc_max_in_flight: 8,
        }
    }
}

impl IngestConfig {
    pub fn new(
        blocks_per_batch: u32,
        rpc_batch_size: usize,
        rpc_max_in_flight: usize,
    ) -> Result<Self> {
        if !(1..=4_096).contains(&blocks_per_batch) {
            return Err(IngestError::configuration(
                "ingest blocks per batch must be in 1..=4096",
            ));
        }
        if !(1..=256).contains(&rpc_batch_size) {
            return Err(IngestError::configuration(
                "ingest RPC batch size must be in 1..=256",
            ));
        }
        if !(1..=32).contains(&rpc_max_in_flight) {
            return Err(IngestError::configuration(
                "ingest RPC maximum in-flight requests must be in 1..=32",
            ));
        }
        Ok(Self {
            blocks_per_batch,
            rpc_batch_size,
            rpc_max_in_flight,
        })
    }

    /// Normal RPC Ingest window and shared redo window, in whole blocks.
    /// Normal Coinbase, direct-Reth and live-follow windows keep their existing sizes.
    pub const fn blocks_per_batch(self) -> i64 {
        self.blocks_per_batch as i64
    }

    /// Calls per HTTP batch. One sends standalone JSON-RPC requests.
    pub const fn rpc_batch_size(self) -> usize {
        self.rpc_batch_size
    }

    /// Concurrent HTTP requests across all users of one configured provider.
    pub const fn rpc_max_in_flight(self) -> usize {
        self.rpc_max_in_flight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_preserves_defaults_and_rejects_values_outside_bounds() {
        assert_eq!(
            IngestConfig::default(),
            IngestConfig::new(256, 32, 8).unwrap()
        );
        assert!(IngestConfig::new(1, 1, 1).is_ok());
        assert!(IngestConfig::new(4_096, 256, 32).is_ok());
        for (blocks, batch, concurrent) in [
            (0, 32, 8),
            (4_097, 32, 8),
            (256, 0, 8),
            (256, 257, 8),
            (256, 32, 0),
            (256, 32, 33),
        ] {
            assert_eq!(
                IngestConfig::new(blocks, batch, concurrent)
                    .unwrap_err()
                    .kind(),
                crate::ErrorKind::Configuration
            );
        }
    }
}
