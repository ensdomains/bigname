use anyhow::{Result, bail};

use super::types::ChainLineageBlock;

pub(crate) fn validate_lineage_block(block: &ChainLineageBlock) -> Result<()> {
    if block.block_number < 0 {
        bail!(
            "lineage block {} for chain {} has negative block number {}",
            block.block_hash,
            block.chain_id,
            block.block_number
        );
    }

    Ok(())
}
