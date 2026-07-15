use anyhow::{Context, Result, ensure};
use sqlx::PgPool;

use super::super::types::ActiveEmitter;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RegistryCacheMetadata {
    pub(super) raw_log_input_revision: i64,
    pub(super) raw_log_retention_generation: i64,
    pub(super) retained_raw_log_history_complete: bool,
    pub(super) discovery_admission_epoch: i64,
}

pub(in crate::ens_v2_registry) struct SelectedRegistryPath {
    pub(super) target_block_hash: String,
    blocks: Vec<(i64, String)>,
}

impl SelectedRegistryPath {
    pub(super) fn contains_anchor(&self, block_number: i64, block_hash: &str) -> bool {
        self.blocks
            .iter()
            .any(|(number, hash)| *number == block_number && hash == block_hash)
    }

    pub(super) fn all_hashes(&self) -> Vec<String> {
        self.blocks.iter().map(|(_, hash)| hash.clone()).collect()
    }

    pub(super) fn hashes_after(&self, block_number: i64) -> Vec<String> {
        self.blocks
            .iter()
            .filter(|(number, _)| *number > block_number)
            .map(|(_, hash)| hash.clone())
            .collect()
    }

    #[cfg(test)]
    pub(in crate::ens_v2_registry) fn len(&self) -> usize {
        self.blocks.len()
    }
}

pub(super) async fn load_selected_registry_target(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
    selected_block_hashes: &[String],
) -> Result<String> {
    let target_hashes = sqlx::query_scalar::<_, String>(
        r#"
        SELECT DISTINCT block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = $2
          AND block_hash = ANY($3::TEXT[])
          AND canonicality_state <> 'orphaned'::canonicality_state
        ORDER BY block_hash
        "#,
    )
    .bind(chain)
    .bind(target_block_number)
    .bind(selected_block_hashes)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load selected ENSv2 live-poll target at block {target_block_number} on {chain}"
        )
    })?;
    ensure!(
        target_hashes.len() == 1,
        "ENSv2 live-poll target block {target_block_number} on {chain} must select exactly one non-orphaned hash; found {}",
        target_hashes.len()
    );
    Ok(target_hashes[0].clone())
}

pub(in crate::ens_v2_registry) async fn load_selected_registry_path_to_floor(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
    target_block_hash: &str,
    floor_block_number: i64,
) -> Result<SelectedRegistryPath> {
    let blocks = sqlx::query_as::<_, (i64, String)>(
        r#"
        WITH RECURSIVE selected_path AS (
            SELECT block_number, block_hash, parent_hash
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_number = $2
              AND block_hash = $3
              AND canonicality_state <> 'orphaned'::canonicality_state

            UNION ALL

            SELECT parent.block_number, parent.block_hash, parent.parent_hash
            FROM chain_lineage parent
            JOIN selected_path child
              ON parent.chain_id = $1
             AND parent.block_hash = child.parent_hash
             AND parent.block_number = child.block_number - 1
            WHERE parent.canonicality_state <> 'orphaned'::canonicality_state
              AND child.block_number > $4
        )
        SELECT block_number, block_hash
        FROM selected_path
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain)
    .bind(target_block_number)
    .bind(target_block_hash)
    .bind(floor_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load the ENSv2 live-poll ancestor path from block {target_block_number} ({target_block_hash}) to floor {floor_block_number} on {chain}"
        )
    })?;
    ensure!(
        blocks
            .first()
            .is_some_and(|(number, _)| *number == floor_block_number),
        "ENSv2 live-poll target path from block {target_block_number} ({target_block_hash}) on {chain} is not parent-contiguous through closure floor {floor_block_number}"
    );

    Ok(SelectedRegistryPath {
        target_block_hash: target_block_hash.to_owned(),
        blocks,
    })
}

pub(super) async fn load_raw_log_closure_floor(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
    emitters: &[ActiveEmitter],
) -> Result<i64> {
    if emitters.is_empty() {
        return Ok(target_block_number);
    }
    let addresses = emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let from_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_from_block_number.unwrap_or(0))
        .collect::<Vec<_>>();
    let to_blocks = emitters
        .iter()
        .map(|emitter| emitter.active_to_block_number.unwrap_or(i64::MAX))
        .collect::<Vec<_>>();
    sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MIN(raw.block_number)::BIGINT
        FROM raw_logs raw
        WHERE raw.chain_id = $1
          AND raw.block_number <= $2
          AND raw.canonicality_state <> 'orphaned'::canonicality_state
          AND EXISTS (
              SELECT 1
              FROM UNNEST($3::TEXT[], $4::BIGINT[], $5::BIGINT[]) AS watched(
                  address,
                  active_from_block,
                  active_to_block
              )
              WHERE watched.address = lower(raw.emitting_address)
                AND raw.block_number BETWEEN watched.active_from_block
                    AND watched.active_to_block
          )
        "#,
    )
    .bind(chain)
    .bind(target_block_number)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load retained raw-log closure floor for {chain}"))
    .map(|floor| floor.unwrap_or(target_block_number))
}

pub(super) async fn load_registry_cache_metadata(
    pool: &PgPool,
    chain: &str,
) -> Result<RegistryCacheMetadata> {
    let (
        raw_log_input_revision,
        raw_log_retention_generation,
        retained_raw_log_history_complete,
        discovery_admission_epoch,
    ) = sqlx::query_as::<_, (i64, i64, bool, i64)>(
        r#"
            SELECT
                COALESCE((
                    SELECT revision
                    FROM raw_log_staging_input_revisions
                    WHERE chain_id = $1
                ), 0)::BIGINT,
                COALESCE((
                    SELECT retention_generation
                    FROM raw_log_staging_input_revisions
                    WHERE chain_id = $1
                ), -1)::BIGINT,
                COALESCE((
                    SELECT retained_history_complete
                    FROM raw_log_staging_input_revisions
                    WHERE chain_id = $1
                ), FALSE),
                COALESCE((
                    SELECT epoch
                    FROM discovery_admission_epochs
                    WHERE chain_id = $1
                ), 0)::BIGINT
            "#,
    )
    .bind(chain)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load ENSv2 live-cache metadata for {chain}"))?;
    Ok(RegistryCacheMetadata {
        raw_log_input_revision,
        raw_log_retention_generation,
        retained_raw_log_history_complete,
        discovery_admission_epoch,
    })
}

pub(super) async fn raw_log_mutations_leave_cached_path_unchanged(
    pool: &PgPool,
    chain: &str,
    after_revision: i64,
    anchor_block_number: i64,
    anchor_block_hash: &str,
) -> Result<bool> {
    let changed_blocks = sqlx::query_as::<_, (i64, String)>(
        r#"
        SELECT block_number, block_hash
        FROM raw_log_staging_block_revisions
        WHERE chain_id = $1
          AND revision > $2
          AND block_number <= $3
        "#,
    )
    .bind(chain)
    .bind(after_revision)
    .bind(anchor_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to validate the selected raw-log path after cache input revision {after_revision} on {chain}"
        )
    })?;
    let Some(oldest_changed_block) = changed_blocks.iter().map(|(number, _)| *number).min() else {
        return Ok(true);
    };
    let changed_hashes = changed_blocks
        .into_iter()
        .map(|(_, hash)| hash)
        .collect::<Vec<_>>();
    let touches_cached_path = sqlx::query_scalar::<_, bool>(
        r#"
        WITH RECURSIVE cached_path AS (
            SELECT block_number, block_hash, parent_hash
            FROM chain_lineage
            WHERE chain_id = $1
              AND block_number = $2
              AND block_hash = $3

            UNION ALL

            SELECT parent.block_number, parent.block_hash, parent.parent_hash
            FROM chain_lineage parent
            JOIN cached_path child
              ON parent.chain_id = $1
             AND parent.block_hash = child.parent_hash
             AND parent.block_number = child.block_number - 1
            WHERE child.block_number > $4
        )
        SELECT EXISTS (
            SELECT 1
            FROM cached_path
            WHERE block_hash = ANY($5::TEXT[])
        )
        "#,
    )
    .bind(chain)
    .bind(anchor_block_number)
    .bind(anchor_block_hash)
    .bind(oldest_changed_block)
    .bind(&changed_hashes)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to compare changed raw-log blocks with cached ancestry on {chain}")
    })?;
    Ok(!touches_cached_path)
}
