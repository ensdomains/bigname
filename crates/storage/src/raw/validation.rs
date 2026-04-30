use anyhow::{Result, bail};

use super::types::RawBlock;

pub(super) fn validate_raw_block(block: &RawBlock) -> Result<()> {
    if block.block_number < 0 {
        bail!(
            "raw block for chain {} hash {} has negative block number {}",
            block.chain_id,
            block.block_hash,
            block.block_number
        );
    }

    Ok(())
}

pub(super) fn validate_replay_range(chain_id: &str, start: i64, end: i64) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if start < 0 {
        bail!("raw log replay range start {start} is negative");
    }
    if end < start {
        bail!("raw log replay range end {end} is before start {start}");
    }
    Ok(())
}

pub(super) fn validate_replay_hashes(chain_id: &str, block_hashes: &[String]) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    for block_hash in block_hashes {
        if block_hash.trim().is_empty() {
            bail!("raw log replay block hash set contains an empty block hash");
        }
    }
    Ok(())
}
