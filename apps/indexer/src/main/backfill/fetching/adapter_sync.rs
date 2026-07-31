use anyhow::Result;

use crate::reconciliation::{
    sync_adapter_state_from_persisted_raw_payloads,
    sync_adapter_state_from_scoped_persisted_raw_payloads,
};

pub(super) async fn sync_inline_adapters(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
) -> Result<()> {
    match source_scope {
        None => {
            sync_adapter_state_from_persisted_raw_payloads(pool, chain, block_hashes).await?;
        }
        Some(source_scope) => {
            sync_adapter_state_from_scoped_persisted_raw_payloads(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?;
        }
    }
    Ok(())
}
