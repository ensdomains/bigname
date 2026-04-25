use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use bigname_manifests::{
    WatchedSourceSelector, load_watched_chain_plan, load_watched_source_selector_plan,
};
use bigname_storage::{
    list_canonical_raw_log_replay_inputs, list_canonical_raw_log_replay_inputs_for_block_hashes,
};

use super::{
    adapter_sync::sync_replay_normalized_events_from_persisted_raw_payloads,
    types::{
        PersistedRawPayloadAdapterSyncSummary, RawFactNormalizedEventReplayOutcome,
        RawFactNormalizedEventReplayRequest, RawFactNormalizedEventReplaySelection,
    },
};

pub(crate) async fn replay_raw_fact_normalized_events(
    pool: &sqlx::PgPool,
    request: RawFactNormalizedEventReplayRequest,
) -> Result<RawFactNormalizedEventReplayOutcome> {
    if request.deployment_profile.trim().is_empty() {
        bail!("deployment_profile must not be empty");
    }

    let selection_kind = request.selection.as_str();
    let raw_logs = match &request.selection {
        RawFactNormalizedEventReplaySelection::BlockRange {
            from_block,
            to_block,
        } => {
            list_canonical_raw_log_replay_inputs(pool, &request.chain, *from_block, *to_block)
                .await?
        }
        RawFactNormalizedEventReplaySelection::BlockHashes(block_hashes) => {
            list_canonical_raw_log_replay_inputs_for_block_hashes(
                pool,
                &request.chain,
                block_hashes,
            )
            .await?
        }
    };
    ensure_replay_matches_deployment_profile_scope(pool, &request, &raw_logs).await?;

    let block_hashes = raw_logs
        .iter()
        .map(|raw_log| raw_log.block_hash.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    ensure_replay_block_hashes_have_only_canonical_raw_logs(pool, &request.chain, &block_hashes)
        .await?;

    let normalized_event_summary = if block_hashes.is_empty() {
        PersistedRawPayloadAdapterSyncSummary::default()
    } else {
        sync_replay_normalized_events_from_persisted_raw_payloads(
            pool,
            &request.chain,
            &block_hashes,
        )
        .await?
    };

    Ok(RawFactNormalizedEventReplayOutcome {
        deployment_profile: request.deployment_profile,
        chain: request.chain,
        selection_kind,
        selected_block_count: block_hashes.len(),
        canonical_raw_log_count: raw_logs.len(),
        scanned_raw_log_count: normalized_event_summary.scanned_log_count,
        matched_raw_log_count: normalized_event_summary.matched_log_count,
        normalized_event_synced_count: normalized_event_summary.total_synced_count,
        normalized_event_inserted_count: normalized_event_summary.total_inserted_count,
    })
}

async fn ensure_replay_matches_deployment_profile_scope(
    pool: &sqlx::PgPool,
    request: &RawFactNormalizedEventReplayRequest,
    raw_logs: &[bigname_storage::RawLogReplayInput],
) -> Result<()> {
    let active_profile = infer_active_manifest_deployment_profile(pool).await?;
    if request.deployment_profile != active_profile {
        bail!(
            "deployment_profile {} does not match active manifest/discovery corpus profile {active_profile}",
            request.deployment_profile
        );
    }

    if let Some((from_block, to_block)) = replay_manifest_scope_range(&request.selection, raw_logs)?
    {
        load_watched_source_selector_plan(
            pool,
            &request.chain,
            WatchedSourceSelector::WholeActiveWatchedChain,
            from_block,
            to_block,
        )
        .await
        .with_context(|| {
            format!(
                "deployment_profile {} has no active watched manifest/discovery route for chain {} over replay range {}..={}",
                request.deployment_profile, request.chain, from_block, to_block
            )
        })?;
    } else {
        ensure_active_watched_chain_for_replay_profile(
            pool,
            &request.deployment_profile,
            &request.chain,
        )
        .await?;
    }

    Ok(())
}

fn replay_manifest_scope_range(
    selection: &RawFactNormalizedEventReplaySelection,
    raw_logs: &[bigname_storage::RawLogReplayInput],
) -> Result<Option<(i64, i64)>> {
    match selection {
        RawFactNormalizedEventReplaySelection::BlockRange {
            from_block,
            to_block,
        } => Ok(Some((*from_block, *to_block))),
        RawFactNormalizedEventReplaySelection::BlockHashes(_) => {
            let from_block = raw_logs.iter().map(|raw_log| raw_log.block_number).min();
            let to_block = raw_logs.iter().map(|raw_log| raw_log.block_number).max();
            match (from_block, to_block) {
                (Some(from_block), Some(to_block)) => Ok(Some((from_block, to_block))),
                (None, None) => Ok(None),
                _ => bail!("raw log replay input block range is internally inconsistent"),
            }
        }
    }
}

async fn ensure_active_watched_chain_for_replay_profile(
    pool: &sqlx::PgPool,
    deployment_profile: &str,
    chain: &str,
) -> Result<()> {
    let watched_plan = load_watched_chain_plan(pool).await.with_context(|| {
        format!(
            "failed to verify deployment_profile {deployment_profile} active watched chain route for chain {chain}"
        )
    })?;
    if !watched_plan.iter().any(|plan| plan.chain == chain) {
        bail!(
            "deployment_profile {deployment_profile} has no active watched manifest/discovery route for chain {chain}"
        );
    }

    Ok(())
}

async fn infer_active_manifest_deployment_profile(pool: &sqlx::PgPool) -> Result<String> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT DISTINCT chain, deployment_epoch
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ORDER BY chain, deployment_epoch
        "#,
    )
    .fetch_all(pool)
    .await
    .context(
        "failed to load active manifest/discovery corpus for replay deployment_profile enforcement",
    )?;

    if rows.is_empty() {
        bail!("deployment_profile cannot be enforced because no active manifests are loaded");
    }

    let all_mainnet = rows.iter().all(|(chain, _)| chain.ends_with("-mainnet"));
    if all_mainnet {
        return Ok("mainnet".to_owned());
    }

    let all_sepolia_dev = rows.iter().all(|(chain, deployment_epoch)| {
        chain.ends_with("-sepolia") && deployment_epoch.ends_with("_sepolia_dev")
    });
    if all_sepolia_dev {
        return Ok("sepolia-dev".to_owned());
    }

    bail!(
        "deployment_profile cannot be enforced because the active manifest/discovery corpus does not match a supported deployment profile"
    );
}

async fn ensure_replay_block_hashes_have_only_canonical_raw_logs(
    pool: &sqlx::PgPool,
    chain: &str,
    block_hashes: &[String],
) -> Result<()> {
    if block_hashes.is_empty() {
        return Ok(());
    }

    let noncanonical_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state NOT IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(block_hashes)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to verify canonical raw log replay guard for chain {chain} across {} blocks",
            block_hashes.len()
        )
    })?;

    if noncanonical_count > 0 {
        bail!(
            "raw-fact normalized-event replay selected {noncanonical_count} noncanonical raw logs; refusing block-hash-scoped adapter replay"
        );
    }

    Ok(())
}
