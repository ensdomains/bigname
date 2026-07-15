use anyhow::Result;
use bigname_manifests::WatchedSourceSelectorPlan;

use super::{
    BackfillAdapterSyncMode, BackfillSourceKind, effective_coinbase_sql_adapter_sync_mode,
    effective_hash_pinned_adapter_sync_mode, load_backfill_topic_plan,
};

pub(crate) fn selected_backfill_source(
    requested: BackfillSourceKind,
    chain: &str,
    coinbase_sql_configured: bool,
) -> BackfillSourceKind {
    match requested {
        BackfillSourceKind::Auto if is_base_chain(chain) && coinbase_sql_configured => {
            BackfillSourceKind::CoinbaseSql
        }
        BackfillSourceKind::Auto => BackfillSourceKind::HashPinned,
        source => source,
    }
}

pub(crate) fn is_base_chain(chain: &str) -> bool {
    matches!(chain, "base-mainnet" | "base" | "base-sepolia")
}

pub(crate) async fn standalone_backfill_profile_convergence_enabled(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    source: BackfillSourceKind,
    requested_mode: BackfillAdapterSyncMode,
) -> Result<bool> {
    let effective_mode = match source {
        BackfillSourceKind::HashPinned => {
            effective_hash_pinned_adapter_sync_mode(source_plan, requested_mode)
        }
        BackfillSourceKind::CoinbaseSql => {
            let topic_plan = load_backfill_topic_plan(pool, source_plan).await?;
            effective_coinbase_sql_adapter_sync_mode(source_plan, &topic_plan, requested_mode)
        }
        BackfillSourceKind::Auto => unreachable!("auto is resolved before backfill execution"),
    };
    Ok(effective_mode != BackfillAdapterSyncMode::RawOnly)
}
