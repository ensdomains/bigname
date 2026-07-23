use super::digest::{
    keccak256_json_value_digest_with_progress, keccak256_selected_targets_digest_with_progress,
};
use super::*;

pub(crate) fn backfill_job_source_identity_payload(
    source_plan: &WatchedSourceSelectorPlan,
) -> Result<Value> {
    if watched_source_plan_uses_basenames_registry_scan_all(source_plan) {
        return basenames_registry_scan_all_topics_source_identity_payload(source_plan);
    }
    if watched_source_plan_uses_generic_resolver_scope(source_plan) {
        return generic_topic_scan_source_identity_payload(source_plan);
    }

    if source_plan.selected_targets.len() <= COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD {
        return Ok(source_plan.source_identity_payload());
    }

    let selected_targets_digest = keccak256_json_digest(&source_plan.selected_targets)
        .context("failed to digest compact backfill source selected targets")?;
    let mut payload = json!({
        "selector_kind": source_plan.selector_kind.as_str(),
        "source_family": &source_plan.source_family,
        "requested_watched_targets": &source_plan.requested_watched_targets,
        "selected_target_count": source_plan.selected_targets.len(),
        "selected_targets_digest_algorithm": "keccak256",
        "selected_targets_digest": selected_targets_digest,
        "selected_targets_sample": selected_targets_sample(&source_plan.selected_targets),
        "source_identity_payload_format": "selected_targets_digest_v1",
    });
    let source_identity_hash =
        keccak256_json_digest(&payload).context("failed to digest backfill source identity")?;
    payload
        .as_object_mut()
        .expect("compact source identity payload must be an object")
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );
    Ok(payload)
}

pub(crate) async fn backfill_job_source_identity_payload_with_progress(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<Value> {
    if watched_source_plan_uses_basenames_registry_scan_all(source_plan) {
        return basenames_registry_scan_all_topics_source_identity_payload(source_plan);
    }
    if watched_source_plan_uses_generic_resolver_scope(source_plan) {
        return generic_topic_identity::generic_topic_scan_source_identity_payload_with_progress(
            pool,
            source_plan,
            progress,
        )
        .await;
    }
    if source_plan.selected_targets.len() <= COMPACT_SOURCE_IDENTITY_SELECTED_TARGET_THRESHOLD {
        return Ok(source_plan.source_identity_payload());
    }

    let requested_watched_targets =
        requested_watched_targets_value_with_progress(pool, source_plan, progress).await?;
    let selected_targets_digest = keccak256_selected_targets_digest_with_progress(
        pool,
        &source_plan.selected_targets,
        None,
        progress,
    )
    .await
    .context("failed to digest compact backfill source selected targets")?;
    let mut payload = json!({
        "selector_kind": source_plan.selector_kind.as_str(),
        "source_family": &source_plan.source_family,
        "requested_watched_targets": requested_watched_targets,
        "selected_target_count": source_plan.selected_targets.len(),
        "selected_targets_digest_algorithm": "keccak256",
        "selected_targets_digest": selected_targets_digest,
        "selected_targets_sample": selected_targets_sample(&source_plan.selected_targets),
        "source_identity_payload_format": "selected_targets_digest_v1",
    });
    let source_identity_hash = keccak256_json_value_digest_with_progress(pool, &payload, progress)
        .await
        .context("failed to digest backfill source identity")?;
    payload
        .as_object_mut()
        .expect("compact source identity payload must be an object")
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );
    Ok(payload)
}

pub(super) async fn requested_watched_targets_value_with_progress(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<Value> {
    const PROGRESS_TARGETS: usize = 1_000;
    let mut targets = Vec::with_capacity(source_plan.requested_watched_targets.len());
    for target in &source_plan.requested_watched_targets {
        targets.push(
            serde_json::to_value(target)
                .context("failed to serialize requested watched target identity")?,
        );
        if targets.len().is_multiple_of(PROGRESS_TARGETS) {
            progress.record(pool).await?;
        }
    }
    if !targets.is_empty() && !targets.len().is_multiple_of(PROGRESS_TARGETS) {
        progress.record(pool).await?;
    }
    Ok(Value::Array(targets))
}
