use anyhow::Result;
use bigname_storage::StoredLineageRangeBlock;
use serde_json::{Value, json};

use super::InspectStoredLineageRangeArgs;
use super::formatting::{canonicality_state_label, format_bytes_hex, format_timestamp};

pub(in crate::inspect) async fn inspect_stored_lineage_range(
    args: InspectStoredLineageRangeArgs,
) -> Result<()> {
    let pool = bigname_storage::connect(&args.database).await?;
    let blocks = bigname_storage::list_stored_lineage_range(
        &pool,
        &args.chain_id,
        args.range_start_block_number,
        args.range_end_block_number,
    )
    .await?;

    println!("{}", render_stored_lineage_range_inspection(&blocks));
    Ok(())
}

pub(in crate::inspect) fn render_stored_lineage_range_inspection(
    blocks: &[StoredLineageRangeBlock],
) -> Value {
    json!({
        "blocks": blocks
            .iter()
            .map(render_stored_lineage_block)
            .collect::<Vec<_>>(),
    })
}

fn render_stored_lineage_block(block: &StoredLineageRangeBlock) -> Value {
    json!({
        "chain_id": block.chain_id.as_str(),
        "block_number": block.block_number,
        "block_hash": block.block_hash.as_str(),
        "parent_hash": block.parent_hash.as_deref(),
        "canonicality_state": canonicality_state_label(block.canonicality_state),
        "timestamp": format_timestamp(block.block_timestamp),
        "logs_bloom": block.logs_bloom.as_ref().map(|bytes| format_bytes_hex(bytes)),
        "transactions_root": block.transactions_root.as_deref(),
        "receipts_root": block.receipts_root.as_deref(),
        "state_root": block.state_root.as_deref(),
    })
}
