use anyhow::Result;
use sqlx::PgPool;

use super::{
    cache::CachedLiveRegistryReplayState,
    path::{
        RegistryCacheMetadata, SelectedRegistryPath, load_selected_registry_path_to_floor,
        raw_log_mutations_leave_cached_path_unchanged,
    },
};

pub(super) async fn reusable_process_cache_path(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
    target_block_hash: &str,
    metadata: &RegistryCacheMetadata,
    cached: &CachedLiveRegistryReplayState,
) -> Result<Option<SelectedRegistryPath>> {
    if target_block_number < cached.through_block_number
        || cached.discovery_admission_epoch != metadata.discovery_admission_epoch
        || cached.raw_log_retention_generation != metadata.raw_log_retention_generation
    {
        return Ok(None);
    }
    let path = load_selected_registry_path_to_floor(
        pool,
        chain,
        target_block_number,
        target_block_hash,
        cached.through_block_number,
    )
    .await?;
    if !path.contains_anchor(cached.through_block_number, &cached.through_block_hash)
        || !raw_log_mutations_leave_cached_path_unchanged(
            pool,
            chain,
            cached.raw_log_input_revision,
            cached.through_block_number,
            &cached.through_block_hash,
        )
        .await?
    {
        return Ok(None);
    }
    Ok(Some(path))
}
