mod evaluate;

#[cfg(test)]
mod tests;

use anyhow::{Result, bail};
use bigname_manifests::load_watched_contracts;
use serde_json::{Value, json};

use super::{InspectDataCompletenessArgs, connect_read_only};
use evaluate::{
    CheckStatus, DEFAULT_MAX_HEAD_LAG_BLOCKS, DataCompletenessReport, evaluate_data_completeness,
};

pub(in crate::inspect) async fn inspect_data_completeness(
    args: InspectDataCompletenessArgs,
) -> Result<()> {
    let pool = connect_read_only(&args.database).await?;
    let read = bigname_storage::load_data_completeness(&pool).await?;
    let watched_contracts = load_watched_contracts(&pool).await?;
    let max_head_lag_blocks = args
        .max_head_lag_blocks
        .unwrap_or(DEFAULT_MAX_HEAD_LAG_BLOCKS);
    let report = evaluate_data_completeness(&read, &watched_contracts, max_head_lag_blocks);

    println!("{}", render_data_completeness(&report));

    if args.fail_on_incomplete && !report.data_complete() {
        bail!("database is not data-complete");
    }
    Ok(())
}

fn render_data_completeness(report: &DataCompletenessReport) -> Value {
    json!({
        "command": "inspect data-completeness",
        "read_only": true,
        "data_complete": report.data_complete(),
        "max_head_lag_blocks": report.max_head_lag_blocks,
        "checks": [
            check("reconciliation_frontier_at_head", report.frontier_at_head(), json!({
                "chains": report.frontiers.iter().map(|frontier| json!({
                    "chain": frontier.chain_id.as_str(),
                    "canonical_block_number": frontier.canonical_block_number,
                    "lineage_head_block_number": frontier.lineage_head_block_number,
                    "head_lag_blocks": frontier.head_lag_blocks,
                    "missing_from_storage": frontier.missing_from_storage,
                })).collect::<Vec<_>>(),
            })),
            check("reconciliation_lineage_contiguous", report.lineage_contiguous(), json!({
                "chains": report.frontiers.iter().map(|frontier| json!({
                    "chain": frontier.chain_id.as_str(),
                    "contiguous": frontier.contiguous,
                    "missing_block_count": frontier.missing_block_count,
                    "duplicate_canonical_height_count": frontier.duplicate_canonical_height_count,
                    "disconnected_canonical_parent_count": frontier.disconnected_canonical_parent_count,
                })).collect::<Vec<_>>(),
            })),
            check("reconciliation_history_from_declared_start", report.history_from_declared_start(), json!({
                "truncated_chains": report.chains_history_truncated.iter().map(|gap| json!({
                    "chain": gap.chain.as_str(),
                    "declared_start_block": gap.declared_start_block,
                    "lineage_floor_block": gap.lineage_floor_block,
                })).collect::<Vec<_>>(),
                "chains_without_finite_start": report.chains_without_finite_start.iter().map(|chain| json!({
                    "chain": chain.chain.as_str(),
                    "open_ended_target_count": chain.open_ended_target_count,
                })).collect::<Vec<_>>(),
            })),
            check("watch_set_code_observation_coverage", report.watch_set_observed(), json!({
                "active_watched_target_count": report.active_watched_target_count,
                "unobserved_target_count": report.unobserved_targets.len(),
                "unobserved_targets": report.unobserved_targets.iter().take(20).map(|target| json!({
                    "chain": target.chain.as_str(),
                    "address": target.address.as_str(),
                    "source_family": target.source_family.as_str(),
                    "active_from_block_number": target.active_from_block_number,
                    "max_observed_block_number": target.max_observed_block_number,
                })).collect::<Vec<_>>(),
            })),
            check("manifest_declared_targets_present", report.manifest_declared_targets_present(), json!({
                "missing_address_target_count": report.manifest_targets_missing_address.len(),
                "missing_address_targets": report.manifest_targets_missing_address.iter().take(20).map(|target| json!({
                    "chain": target.chain.as_str(),
                    "address": target.address.as_str(),
                    "source_family": target.source_family.as_str(),
                    "active_from_block_number": target.active_from_block_number,
                    "max_observed_block_number": target.max_observed_block_number,
                })).collect::<Vec<_>>(),
            })),
            check("normalization_no_failure", report.normalization_healthy(), json!({
                "failed_cursors": report.failed_replay_cursors.clone(),
            })),
            check("normalization_caught_up_to_raw_head", report.normalization_caught_up(), json!({
                "lagging_cursors": report.lagging_replay_cursors.iter().map(cursor_lag).collect::<Vec<_>>(),
                "chains_missing_raw_fact_cursor": report.chains_missing_raw_fact_cursor.clone(),
            })),
            check("projection_apply_drained", report.projection_drained(), json!({
                "lagging_cursors": report.lagging_projection_cursors.iter().map(cursor_lag).collect::<Vec<_>>(),
                "required_cursor": crate::projection_apply::NORMALIZED_EVENT_CURSOR,
                "apply_cursor_missing_for_non_empty_change_log": report.projection_apply_cursor_missing,
            })),
            check("projection_invalidations_drained", report.projection_invalidations_drained(), json!({
                "pending_invalidation_count": report.pending_projection_invalidation_count,
            })),
            check("projection_no_dead_letters", report.projection_no_dead_letters(), json!({
                "dead_letter_count": report.projection_invalidation_dead_letter_count,
            })),
            check("projection_replay_complete", report.projection_replay_complete(), json!({
                "replay_version": report.projection_replay_version,
                "required_replay_version": report.projection_replay_required_version,
                "required_target_block": report.projection_replay_required_target_block,
                "missing_projections": report.missing_projection_replay_markers.clone(),
            })),
            check("active_dataset_non_empty", report.active_dataset_non_empty(), json!({
                "normalized_event_total": report.normalized_event_total,
                "name_current_total": report.name_current_total,
                "manifest_sources_without_events": report.active_manifest_sources_without_events.iter().map(|entry| json!({
                    "manifest_id": entry.manifest_id,
                    "manifest_version": entry.manifest_version,
                    "chain": entry.chain.as_str(),
                    "namespace": entry.namespace.as_str(),
                    "source_family": entry.source_family.as_str(),
                })).collect::<Vec<_>>(),
                "namespaces_without_names": report.active_namespaces_without_names.clone(),
            })),
            check("normalized_events_chain_id_present", report.normalized_events_chain_id_present(), json!({
                "null_chain_id_count": report.normalized_events_null_chain_id_count,
            })),
            check("deferred_projection_indexes_present", report.deferred_projection_indexes_present(), json!({
                "missing_indexes": report.missing_deferred_projection_indexes.clone(),
            })),
        ],
        "advisories": {
            "foreign_chains": report.foreign_chains.clone(),
            "backfill_lifecycle": report.backfill_advisory.iter().map(|row| json!({
                "deployment_profile": row.deployment_profile.as_str(),
                "failed_job_count": row.failed_job_count,
                "failed_range_count": row.failed_range_count,
                "incomplete_range_count": row.incomplete_range_count,
                "expired_lease_range_count": row.expired_lease_range_count,
            })).collect::<Vec<_>>(),
        },
    })
}

fn check(name: &'static str, status: CheckStatus, detail: Value) -> Value {
    json!({ "name": name, "status": status.label(), "detail": detail })
}

fn cursor_lag(lag: &evaluate::CursorLag) -> Value {
    json!({ "cursor": lag.label.as_str(), "behind_by": lag.behind_by })
}
