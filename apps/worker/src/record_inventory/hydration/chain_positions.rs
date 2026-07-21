use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TextHydrationChainPosition {
    pub(super) block_number: i64,
    pub(super) block_hash: String,
}

pub(super) async fn load_text_hydration_chain_positions(
    pool: &PgPool,
    chain_ids: &[String],
) -> Result<BTreeMap<String, TextHydrationChainPosition>> {
    if chain_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            canonical_block_number,
            canonical_block_hash
        FROM chain_checkpoints
        WHERE chain_id = ANY($1::TEXT[])
        "#,
    )
    .bind(chain_ids)
    .fetch_all(pool)
    .await
    .context("failed to load text hydration chain checkpoints")?;

    let mut positions = BTreeMap::new();
    for row in rows {
        let chain_id: String = row.try_get("chain_id")?;
        let block_number: Option<i64> = row.try_get("canonical_block_number")?;
        let block_hash: Option<String> = row.try_get("canonical_block_hash")?;
        let Some((block_number, block_hash)) = block_number.zip(block_hash) else {
            continue;
        };
        positions.insert(
            chain_id,
            TextHydrationChainPosition {
                block_number,
                block_hash,
            },
        );
    }

    for chain_id in chain_ids {
        if !positions.contains_key(chain_id) {
            anyhow::bail!(
                "record_inventory_current text hydration requires a canonical chain checkpoint for {chain_id}"
            );
        }
    }

    Ok(positions)
}
