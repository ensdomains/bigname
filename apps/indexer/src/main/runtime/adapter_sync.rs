use anyhow::{Context, Result};
use bigname_manifests::WatchedChainPlan;

use crate::resolver_profile_convergence::journal_resolver_profile_authority;

use super::logging::{
    log_ens_v1_reverse_claim_sync_summary, log_ens_v1_subregistry_discovery_sync_summary,
    log_ens_v1_unwrapped_authority_sync_summary, log_ens_v2_permissions_sync_summary,
    log_ens_v2_registrar_sync_summary, log_ens_v2_registry_resource_surface_sync_summary,
    log_ens_v2_resolver_sync_summary,
};

pub(crate) async fn sync_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<()> {
    sync_adapter_owned_raw_log_state_with_startup_context(pool, watched_chain_plan, None).await
}

pub(crate) async fn sync_startup_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<()> {
    sync_adapter_owned_raw_log_state_with_startup_context(
        pool,
        watched_chain_plan,
        Some(deployment_profile),
    )
    .await
}

async fn sync_adapter_owned_raw_log_state_with_startup_context(
    pool: &sqlx::PgPool,
    watched_chain_plan: &[WatchedChainPlan],
    startup_deployment_profile: Option<&str>,
) -> Result<()> {
    // Broad startup/timer passes also recover any prior discovery transaction
    // that committed before its caller could journal the epoch change.
    journal_resolver_profile_authority(pool).await?;
    let mut completed_startup_checkpoints = Vec::new();
    for chain in watched_chain_plan {
        let startup_checkpoint = match startup_deployment_profile {
            Some(deployment_profile) => Some(
                load_startup_adapter_checkpoint_context(pool, deployment_profile, &chain.chain)
                    .await?,
            ),
            None => None,
        };
        let summary = bigname_adapters::sync_ens_v1_reverse_claim(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv1 reverse claim from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v1_reverse_claim_sync_summary(&chain.chain, &summary);

        let summary = match startup_checkpoint.as_ref() {
            Some(checkpoint) => {
                bigname_adapters::sync_ens_v1_subregistry_discovery_with_startup_checkpoint(
                    pool,
                    &chain.chain,
                    checkpoint,
                )
                .await
            }
            None => bigname_adapters::sync_ens_v1_subregistry_discovery(pool, &chain.chain).await,
        }
        .with_context(|| {
            format!(
                "failed to sync ENSv1 registry discovery from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v1_subregistry_discovery_sync_summary(&chain.chain, &summary);

        let summary = match startup_checkpoint.as_ref() {
            Some(checkpoint) => {
                bigname_adapters::sync_ens_v1_unwrapped_authority_with_startup_checkpoint(
                    pool,
                    &chain.chain,
                    checkpoint,
                )
                .await
            }
            None => bigname_adapters::sync_ens_v1_unwrapped_authority(pool, &chain.chain).await,
        }
        .with_context(|| {
            format!(
                "failed to sync ENSv1 unwrapped authority from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v1_unwrapped_authority_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_registry_resource_surface(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registry resource/surface state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registry_resource_surface_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_registrar(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registrar state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registrar_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_resolver(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 resolver state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_resolver_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_permissions(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 permissions state from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_permissions_sync_summary(&chain.chain, &summary);

        if let Some(checkpoint) = startup_checkpoint {
            completed_startup_checkpoints.push((chain.chain.clone(), checkpoint));
        }
    }

    journal_resolver_profile_authority(pool).await?;
    clear_completed_startup_adapter_checkpoints(pool, &completed_startup_checkpoints).await?;
    Ok(())
}

/// Materialize only the discovery edges needed by the post-bootstrap live-plan
/// widen. Auto bootstrap stores raw facts without adapter work; replay catch-up
/// owns the remaining historical adapter families.
pub(crate) async fn sync_discovery_adapter_owned_raw_log_state(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    watched_chain_plan: &[WatchedChainPlan],
) -> Result<()> {
    journal_resolver_profile_authority(pool).await?;
    let mut completed_startup_checkpoints = Vec::new();
    for chain in watched_chain_plan {
        let startup_checkpoint =
            load_startup_adapter_checkpoint_context(pool, deployment_profile, &chain.chain).await?;
        let summary = bigname_adapters::sync_ens_v1_subregistry_discovery_with_startup_checkpoint(
            pool,
            &chain.chain,
            &startup_checkpoint,
        )
        .await
        .with_context(|| {
            format!(
                "failed to sync ENSv1 registry discovery from stored raw logs for chain {}",
                chain.chain
            )
        })?;
        log_ens_v1_subregistry_discovery_sync_summary(&chain.chain, &summary);

        let summary = bigname_adapters::sync_ens_v2_registry_resource_surface(pool, &chain.chain)
            .await
            .with_context(|| {
                format!(
                    "failed to sync ENSv2 registry discovery from stored raw logs for chain {}",
                    chain.chain
                )
            })?;
        log_ens_v2_registry_resource_surface_sync_summary(&chain.chain, &summary);
        completed_startup_checkpoints.push((chain.chain.clone(), startup_checkpoint));
    }
    journal_resolver_profile_authority(pool).await?;
    clear_completed_startup_adapter_checkpoints(pool, &completed_startup_checkpoints).await?;
    Ok(())
}

async fn load_startup_adapter_checkpoint_context(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<bigname_adapters::StartupAdapterCheckpointContext> {
    let target_block_number = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT MAX(block_number)::BIGINT
        FROM (
            SELECT block_number
            FROM chain_lineage
            WHERE chain_id = $1
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
            UNION ALL
            SELECT block_number
            FROM raw_logs
            WHERE chain_id = $1
              AND canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ) AS startup_adapter_inputs
        "#,
    )
    .bind(chain)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load startup adapter checkpoint target for {chain}"))?
    .unwrap_or(0);
    bigname_adapters::StartupAdapterCheckpointContext::new(deployment_profile, target_block_number)
}

async fn clear_completed_startup_adapter_checkpoints(
    pool: &sqlx::PgPool,
    completed: &[(String, bigname_adapters::StartupAdapterCheckpointContext)],
) -> Result<()> {
    for (chain, checkpoint) in completed {
        bigname_adapters::clear_startup_adapter_checkpoints(pool, chain, checkpoint).await?;
    }
    Ok(())
}
