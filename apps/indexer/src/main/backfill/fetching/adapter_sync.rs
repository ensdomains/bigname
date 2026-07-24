use anyhow::Result;

use crate::{
    backfill::BackfillAdapterSyncMode,
    reconciliation::{
        sync_adapter_state_from_persisted_raw_payloads,
        sync_adapter_state_from_persisted_raw_payloads_without_ens_v2_adapters,
        sync_adapter_state_from_scoped_persisted_raw_payloads,
        sync_adapter_state_from_scoped_persisted_raw_payloads_without_ens_v2_adapters,
    },
};

pub(super) async fn sync_inline_adapters(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
    source_scope: Option<&[(String, String, i64, i64)]>,
    adapter_sync_mode: BackfillAdapterSyncMode,
) -> Result<()> {
    match (source_scope, adapter_sync_mode.defers_ens_v2_adapters()) {
        (None, false) => {
            sync_adapter_state_from_persisted_raw_payloads(pool, chain, block_hashes).await?;
        }
        (None, true) => {
            sync_adapter_state_from_persisted_raw_payloads_without_ens_v2_adapters(
                pool,
                chain,
                block_hashes,
            )
            .await?;
        }
        (Some(source_scope), false) => {
            sync_adapter_state_from_scoped_persisted_raw_payloads(
                pool,
                chain,
                block_hashes,
                source_scope,
            )
            .await?;
        }
        (Some(source_scope), true) => {
            sync_adapter_state_from_scoped_persisted_raw_payloads_without_ens_v2_adapters(
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
