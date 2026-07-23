mod support;

use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row};

use super::*;
use support::*;

#[test]
fn current_projection_staging_contract_matches_schema_version_fingerprint() -> Result<()> {
    let expected = r#"schema_version=2
projection=name_current|cursor=logical_name_id:string
stage=name_current|unique=logical_name_id|has_inserted_at=true
projection=children_current|cursor=(parent_logical_name_id,canonical_display_name,child_logical_name_id):string_tuple
stage=children_current|unique=parent_logical_name_id,child_logical_name_id,surface_class|has_inserted_at=true
projection=permissions_current|cursor=resource_id:uuid
stage=permissions_current|unique=resource_id,subject,scope|has_inserted_at=true
stage=permissions_current_resource_summary|unique=resource_id|has_inserted_at=false
projection=record_inventory_current|cursor=resource_id:uuid
stage=record_inventory_current|unique=resource_id,record_version_boundary_key|has_inserted_at=true
projection=resolver_current|cursor=(chain_id,resolver_address):string_tuple
stage=resolver_current|unique=chain_id,resolver_address|has_inserted_at=true
projection=address_names_current|cursor=(logical_name_id,surface_binding_id):string_uuid_tuple
stage=address_names_current|unique=address,logical_name_id,relation|has_inserted_at=true
projection=primary_names_current|cursor=(address,namespace,coin_type):string_tuple
stage=primary_names_current|unique=address,coin_type,namespace|has_inserted_at=false
columns=children_current|parent_logical_name_id,child_logical_name_id,surface_class,namespace,canonical_display_name,normalized_name,namehash,labelhash,owner,registrant,provenance,chain_positions,canonicality_summary,manifest_version,last_recomputed_at
columns=permissions_current|resource_id,subject,scope,scope_kind,scope_detail,effective_powers,grant_source,revocation_source,inheritance_path,transfer_behavior,provenance,coverage,chain_positions,canonicality_summary,manifest_version,last_recomputed_at
columns=permissions_current_resource_summary|resource_id,authority_kind,root_resource_id,coverage,provenance,chain_positions,canonicality_summary,manifest_version,last_recomputed_at
columns=primary_names_current|address,coin_type,namespace,claim_status,raw_claim_name,normalized_claim_name,claim_name_is_normalized,claim_provenance
columns=record_inventory_current|resource_id,record_version_boundary_key,record_version_boundary,enumeration_basis,selectors,explicit_gaps,unsupported_families,last_change,entries,provenance,coverage,chain_positions,canonicality_summary,manifest_version,last_recomputed_at
columns=resolver_current|chain_id,resolver_address,declared_summary,provenance,coverage,chain_positions,canonicality_summary,manifest_version,last_recomputed_at
"#;

    assert_eq!(
        super::staging::fingerprint::staging_contract_fingerprint()?,
        expected,
        "if intentional, bump CURRENT_PROJECTION_STAGING_SCHEMA_VERSION and update this fingerprint."
    );
    Ok(())
}

#[test]
fn all_current_projection_json_summary_has_frozen_shape_order_counts_and_totals() -> Result<()> {
    let summary = AllCurrentProjectionsReplaySummary {
        steps: vec![
            CurrentProjectionReplayStepSummary {
                projection: "name_current",
                requested_key_count: 2,
                upserted_row_count: 2,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "children_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "permissions_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "record_inventory_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "resolver_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "address_names_current",
                requested_key_count: 2,
                upserted_row_count: 3,
                deleted_row_count: 0,
            },
            CurrentProjectionReplayStepSummary {
                projection: "primary_names_current",
                requested_key_count: 1,
                upserted_row_count: 1,
                deleted_row_count: 0,
            },
        ],
    };

    let encoded = summary.json_summary_string()?;
    let value: Value = serde_json::from_str(&encoded)?;
    assert_json_object_fields(&value, ["command", "projections", "totals"]);
    assert_eq!(value["command"], "all-current-projections");

    let projections = value["projections"]
        .as_array()
        .context("projections must be an array")?;
    let projection_order = projections
        .iter()
        .map(|projection| {
            projection["projection"]
                .as_str()
                .context("projection name must be a string")
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(projection_order, ALL_CURRENT_PROJECTION_JSON_ORDER.to_vec());

    let expected_counts = BTreeMap::from([
        ("address_names_current", (2, 3, 0)),
        ("children_current", (1, 1, 0)),
        ("coverage_current", (0, 0, 0)),
        ("name_current", (2, 2, 0)),
        ("permissions_current", (1, 1, 0)),
        ("primary_names_current", (1, 1, 0)),
        ("record_inventory_current", (1, 1, 0)),
        ("resolver_current", (1, 1, 0)),
        ("surface_bindings_current", (0, 0, 0)),
    ]);

    for projection in projections {
        assert_json_object_fields(
            projection,
            ["projection", "requested", "upserted", "deleted"],
        );
        let projection_name = projection["projection"]
            .as_str()
            .context("projection name must be a string")?;
        let (requested, upserted, deleted) = expected_counts
            .get(projection_name)
            .copied()
            .with_context(|| format!("unexpected projection {projection_name}"))?;
        assert_eq!(projection["requested"].as_u64(), Some(requested));
        assert_eq!(projection["upserted"].as_u64(), Some(upserted));
        assert_eq!(projection["deleted"].as_u64(), Some(deleted));
    }

    assert_json_object_fields(&value["totals"], ["requested", "upserted", "deleted"]);
    assert_eq!(value["totals"]["requested"].as_u64(), Some(9));
    assert_eq!(value["totals"]["upserted"].as_u64(), Some(10));
    assert_eq!(value["totals"]["deleted"].as_u64(), Some(0));

    Ok(())
}

#[tokio::test]
async fn automatic_replay_refreshes_worker_heartbeat_between_projection_steps() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    let instance_id = "automatic-replay-heartbeat-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE service_loop_heartbeats
        SET started_at = clock_timestamp() - INTERVAL '2 minutes',
            heartbeat_at = clock_timestamp() - INTERVAL '1 minute'
        WHERE service_name = 'worker'
          AND instance_id = $1
        "#,
    )
    .bind(instance_id)
    .execute(database.pool())
    .await?;

    let mut heartbeat = crate::primary_name::rebuild_heartbeat::LoopHeartbeat::new(
        instance_id.to_owned(),
        Duration::ZERO,
    );
    rebuild_pending_all_current_projections_with_heartbeat(
        database.pool(),
        Some(108),
        None,
        None,
        &mut heartbeat,
    )
    .await?;

    let heartbeat = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("automatic replay must retain its worker heartbeat")?;
    assert!(
        heartbeat.age_seconds <= 1,
        "projection-step progress must refresh the worker heartbeat"
    );
    Ok(())
}

#[tokio::test]
async fn all_current_projection_replay_clears_stale_rows_and_is_idempotent() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;

    let first_summary = rebuild_all_current_projections(database.pool(), None, None).await?;
    assert_eq!(
        first_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert_eq!(first_summary.total_deleted_row_count(), 0);

    let first_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
    assert_projection_counts(
        &first_snapshot,
        [
            ("name_current", 2),
            ("children_current", 1),
            ("permissions_current", 1),
            ("record_inventory_current", 1),
            ("resolver_current", 1),
            ("address_names_current", 3),
            ("primary_names_current", 1),
        ],
    );

    insert_stale_projection_rows(database.pool()).await?;
    let stale_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
    for projection in ALL_CURRENT_PROJECTION_ORDER {
        assert!(
            stale_snapshot.row_count(projection) > first_snapshot.row_count(projection),
            "{projection} should contain an injected stale row before replay"
        );
    }

    let second_summary = rebuild_all_current_projections(database.pool(), None, None).await?;
    assert_eq!(
        second_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert!(second_summary.total_deleted_row_count() >= 6);
    let second_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
    assert_eq!(first_snapshot, second_snapshot);

    let third_summary = rebuild_all_current_projections(database.pool(), None, None).await?;
    assert_eq!(
        third_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    let third_snapshot = load_api_visible_projection_snapshot(database.pool()).await?;
    assert_eq!(second_snapshot.row_counts(), third_snapshot.row_counts());
    assert_eq!(second_snapshot, third_snapshot);

    let target_block = Some(108);
    let targeted_summary =
        rebuild_pending_all_current_projections(database.pool(), target_block, None, None).await?;
    assert_eq!(
        targeted_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert!(targeted_summary.total_requested_key_count() > 0);
    assert_eq!(
        third_snapshot,
        load_api_visible_projection_snapshot(database.pool()).await?
    );

    let status_rows = load_replay_status_rows(database.pool()).await?;
    assert_eq!(status_rows.len(), ALL_CURRENT_PROJECTION_ORDER.len());
    for projection in ALL_CURRENT_PROJECTION_ORDER {
        let (version, completed_target_block) = status_rows
            .get(*projection)
            .with_context(|| format!("missing replay status row for {projection}"))?;
        assert_eq!(*version, super::progress::CURRENT_PROJECTION_REPLAY_VERSION);
        assert_eq!(*completed_target_block, target_block);
    }

    let skipped_summary =
        rebuild_pending_all_current_projections(database.pool(), target_block, None, None).await?;
    assert_eq!(
        skipped_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert_eq!(skipped_summary.total_requested_key_count(), 0);
    assert_eq!(skipped_summary.total_upserted_row_count(), 0);
    assert_eq!(skipped_summary.total_deleted_row_count(), 0);
    assert_eq!(
        third_snapshot,
        load_api_visible_projection_snapshot(database.pool()).await?
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM current_projection_staging_checkpoints"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "published-family skip must leave no stale staging checkpoint"
    );

    let advanced_summary =
        rebuild_pending_all_current_projections(database.pool(), Some(109), None, None).await?;
    assert_eq!(
        advanced_summary.projection_order(),
        ALL_CURRENT_PROJECTION_ORDER
    );
    assert!(advanced_summary.total_requested_key_count() > 0);
    assert!(advanced_summary.total_upserted_row_count() > 0);
    assert_eq!(
        third_snapshot,
        load_api_visible_projection_snapshot(database.pool()).await?
    );
    let status_rows = load_replay_status_rows(database.pool()).await?;
    for projection in ALL_CURRENT_PROJECTION_ORDER {
        let (_, completed_target_block) = status_rows
            .get(*projection)
            .with_context(|| format!("missing replay status row for {projection}"))?;
        assert_eq!(*completed_target_block, Some(109));
    }

    database.cleanup().await
}

#[tokio::test]
async fn changed_completed_source_range_restages_to_byte_identical_control() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_second_name_page_termination(database.pool()).await?;

    let error = rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the injected second-page failure must interrupt name_current staging");
    assert!(!format!("{error:#}").is_empty());
    let interrupted = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(interrupted.completed_source_count, 1);
    assert_eq!(
        count_stage_rows(database.pool(), &interrupted.stage_table).await?,
        1,
        "the first page and its stage row must survive interruption"
    );

    change_already_staged_name_source(database.pool()).await?;
    remove_second_name_page_termination(database.pool()).await?;
    install_name_publish_failure(database.pool()).await?;
    let error = rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the publish stop must retain the resumed completed stage");
    assert!(format!("{error:#}").contains("injected name_current publish stop"));
    let resumed = load_name_staging_checkpoint(database.pool()).await?;
    assert_ne!(
        resumed.stage_table, interrupted.stage_table,
        "a changed completed source range must fail closed to a fresh stage"
    );
    assert_eq!(resumed.completed_source_count, 2);
    assert_eq!(resumed.status, "staging_complete");
    let resumed_snapshot = load_stage_snapshot(database.pool(), &resumed.stage_table).await?;

    super::staging::cleanup_projection_checkpoint(database.pool(), "name_current").await?;
    let error = rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the publish stop must retain the uninterrupted control stage");
    assert!(format!("{error:#}").contains("injected name_current publish stop"));
    let control = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(control.completed_source_count, 2);
    assert_eq!(control.status, "staging_complete");
    let control_snapshot = load_stage_snapshot(database.pool(), &control.stage_table).await?;
    assert_eq!(
        resumed_snapshot, control_snapshot,
        "kill/resume staging output must be byte-identical to uninterrupted staging"
    );

    remove_name_publish_failure(database.pool()).await?;
    super::staging::cleanup_projection_checkpoint(database.pool(), "name_current").await?;
    database.cleanup().await
}

#[tokio::test]
async fn killed_name_staging_reuses_the_last_durable_page_when_inputs_are_unchanged() -> Result<()>
{
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_second_name_page_termination(database.pool()).await?;

    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the injected second-page failure must interrupt name_current staging");
    let interrupted = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(interrupted.completed_source_count, 1);

    remove_second_name_page_termination(database.pool()).await?;
    install_name_publish_failure(database.pool()).await?;
    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the publish stop must retain the resumed completed stage");
    let resumed = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(resumed.stage_table, interrupted.stage_table);
    assert_eq!(resumed.completed_source_count, 2);
    assert_eq!(resumed.status, "staging_complete");

    remove_name_publish_failure(database.pool()).await?;
    super::staging::cleanup_projection_checkpoint(database.pool(), "name_current").await?;
    database.cleanup().await
}

#[tokio::test]
async fn manual_completed_stage_drift_restages_and_publishes_all_changed_sources() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_name_publish_failure(database.pool()).await?;

    let error = rebuild_all_current_projections(database.pool(), None, None)
        .await
        .expect_err("the publish stop must retain the completed manual-replay stage");
    assert!(format!("{error:#}").contains("injected name_current publish stop"));
    let stale = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(stale.status, "staging_complete");
    assert_eq!(stale.completed_source_count, 2);
    assert_eq!(
        stale.last_source_key,
        Some(serde_json::json!("ens:bob.alice.eth"))
    );

    change_already_staged_name_source(database.pool()).await?;
    append_name_source_after_completed_cursor(database.pool()).await?;
    let error = rebuild_all_current_projections(database.pool(), None, None)
        .await
        .expect_err("the publish stop must retain the freshly restaged manual replay");
    assert!(format!("{error:#}").contains("injected name_current publish stop"));

    let replacement = load_name_staging_checkpoint(database.pool()).await?;
    assert_ne!(
        replacement.stage_table, stale.stage_table,
        "completed-stage drift must discard and replace the stale manual-replay stage"
    );
    assert!(
        !stage_table_exists(database.pool(), &stale.stage_table).await?,
        "manual replay must drop the stale completed stage table"
    );
    assert_eq!(replacement.status, "staging_complete");
    assert_eq!(replacement.completed_source_count, 3);
    assert_eq!(
        replacement.last_source_key,
        Some(serde_json::json!(APPENDED_LOGICAL_NAME_ID))
    );
    let replacement_snapshot =
        load_stage_snapshot(database.pool(), &replacement.stage_table).await?;
    assert!(
        replacement_snapshot.contains(r#""canonical_display_name": "Alice.eth""#),
        "the replacement stage must include the strictly interior source change"
    );
    assert!(
        replacement_snapshot.contains(&format!(
            r#""logical_name_id": "{APPENDED_LOGICAL_NAME_ID}""#
        )),
        "the replacement stage must include the source appended after the completed cursor"
    );

    remove_name_publish_failure(database.pool()).await?;
    rebuild_all_current_projections(database.pool(), None, None).await?;
    let published_names = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT logical_name_id, canonical_display_name
        FROM name_current
        WHERE logical_name_id IN ('ens:alice.eth', $1)
        ORDER BY logical_name_id
        "#,
    )
    .bind(APPENDED_LOGICAL_NAME_ID)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        published_names,
        vec![
            ("ens:alice.eth".to_owned(), "Alice.eth".to_owned()),
            (
                APPENDED_LOGICAL_NAME_ID.to_owned(),
                APPENDED_DISPLAY_NAME.to_owned()
            ),
        ],
        "manual replay must publish both the interior mutation and appended source"
    );

    database.cleanup().await
}

#[tokio::test]
async fn completed_stage_reuses_without_drift_but_restarts_for_an_appended_source() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_name_publish_failure(database.pool()).await?;

    rebuild_all_current_projections(database.pool(), None, None)
        .await
        .expect_err("the publish stop must retain a completed manual-replay stage");
    let completed = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(completed.status, "staging_complete");
    let reused = super::staging::ProjectionStagingCheckpoint::load_or_start(
        database.pool(),
        "name_current",
        None,
    )
    .await?;
    assert!(reused.staging_complete());
    assert_eq!(
        reused.stage_table(0)?,
        completed.stage_table,
        "a completed stage with no drift must retain its staged output"
    );

    append_name_source_after_completed_cursor(database.pool()).await?;
    assert!(
        APPENDED_LOGICAL_NAME_ID
            > completed
                .last_source_key
                .as_ref()
                .and_then(Value::as_str)
                .context("completed name stage must have a string cursor")?,
        "the appended fixture key must sort strictly after the completed cursor"
    );
    let replacement = super::staging::ProjectionStagingCheckpoint::load_or_start(
        database.pool(),
        "name_current",
        None,
    )
    .await?;
    assert!(!replacement.staging_complete());
    assert_ne!(
        replacement.stage_table(0)?,
        completed.stage_table,
        "a source appended after the final cursor must discard the completed stage"
    );
    assert!(
        !stage_table_exists(database.pool(), &completed.stage_table).await?,
        "full-range completed-stage drift detection must drop the stale stage table"
    );

    remove_name_publish_failure(database.pool()).await?;
    super::staging::cleanup_projection_checkpoint(database.pool(), "name_current").await?;
    database.cleanup().await
}

#[tokio::test]
async fn later_source_change_is_loaded_by_a_fresh_page_without_discarding_progress() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_second_name_page_termination(database.pool()).await?;
    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the injected second-page failure must interrupt name_current staging");
    let interrupted = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(interrupted.completed_source_count, 1);

    change_not_yet_staged_name_source(database.pool()).await?;
    remove_second_name_page_termination(database.pool()).await?;
    install_name_publish_failure(database.pool()).await?;
    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the publish stop must retain the resumed completed stage");
    let resumed = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(resumed.stage_table, interrupted.stage_table);
    assert_eq!(resumed.completed_source_count, 2);
    let snapshot = load_stage_snapshot(database.pool(), &resumed.stage_table).await?;
    assert!(
        snapshot.contains(r#""canonical_display_name": "Bob.alice.eth""#),
        "the later fresh source page must include the post-checkpoint source change"
    );

    remove_name_publish_failure(database.pool()).await?;
    super::staging::cleanup_projection_checkpoint(database.pool(), "name_current").await?;
    database.cleanup().await
}

#[tokio::test]
async fn strictly_interior_change_between_stop_and_resume_discards_and_restages() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_name_staging_completion_termination(database.pool()).await?;

    let error = rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the injected completion failure must stop after both name pages");
    assert!(!format!("{error:#}").is_empty());
    remove_name_staging_completion_termination(database.pool()).await?;
    let interrupted = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(interrupted.completed_source_count, 2);
    assert_eq!(
        interrupted.last_source_key,
        Some(serde_json::json!("ens:bob.alice.eth"))
    );
    assert_eq!(interrupted.status, "running");
    assert_eq!(
        count_stage_rows(database.pool(), &interrupted.stage_table).await?,
        2
    );

    change_already_staged_name_source(database.pool()).await?;
    install_name_publish_failure(database.pool()).await?;
    let error = rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the publish stop must retain the fresh replacement stage");
    assert!(format!("{error:#}").contains("injected name_current publish stop"));

    let resumed = load_name_staging_checkpoint(database.pool()).await?;
    assert_ne!(
        resumed.stage_table, interrupted.stage_table,
        "a change strictly inside the completed range must discard the stopped stage"
    );
    assert!(
        !stage_table_exists(database.pool(), &interrupted.stage_table).await?,
        "resume must drop the stale stage table"
    );
    assert_eq!(resumed.completed_source_count, 2);
    assert_eq!(resumed.status, "staging_complete");
    assert!(
        load_stage_snapshot(database.pool(), &resumed.stage_table)
            .await?
            .contains(r#""canonical_display_name": "Alice.eth""#),
        "the fresh stage must contain the interior source mutation"
    );

    remove_name_publish_failure(database.pool()).await?;
    super::staging::cleanup_projection_checkpoint(database.pool(), "name_current").await?;
    database.cleanup().await
}

#[tokio::test]
async fn strictly_interior_change_during_run_discards_stage_and_bails() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_name_staging_completion_termination(database.pool()).await?;

    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the injected completion failure must stop after both name pages");
    remove_name_staging_completion_termination(database.pool()).await?;
    let interrupted = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(
        interrupted.last_source_key,
        Some(serde_json::json!("ens:bob.alice.eth"))
    );
    let checkpoint = super::staging::ProjectionStagingCheckpoint::load_or_start(
        database.pool(),
        "name_current",
        Some(108),
    )
    .await?;
    assert_eq!(checkpoint.stage_table(0)?, interrupted.stage_table);

    change_already_staged_name_source(database.pool()).await?;
    let error = checkpoint
        .prepare_next_batch(database.pool())
        .await
        .err()
        .context("the next-page fence must reject a strictly interior mutation")?;
    assert!(
        format!("{error:#}")
            .contains("completed staging source range changed after its last durable page"),
        "the mid-run fence must return the completed-staging-source-range-changed error: {error:#}"
    );
    assert!(
        !stage_table_exists(database.pool(), &interrupted.stage_table).await?,
        "the mid-run fence must drop the stale stage table"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM current_projection_staging_checkpoints \
             WHERE projection = 'name_current'"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "the mid-run fence must remove the stale checkpoint"
    );

    database.cleanup().await
}

#[tokio::test]
async fn change_above_cursor_during_run_retains_stage() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_second_name_page_termination(database.pool()).await?;

    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the injected second-page failure must stop after the first name page");
    remove_second_name_page_termination(database.pool()).await?;
    let interrupted = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(
        interrupted.last_source_key,
        Some(serde_json::json!("ens:alice.eth"))
    );
    let checkpoint = super::staging::ProjectionStagingCheckpoint::load_or_start(
        database.pool(),
        "name_current",
        Some(108),
    )
    .await?;

    change_not_yet_staged_name_source(database.pool()).await?;
    checkpoint.prepare_next_batch(database.pool()).await?;
    let retained = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(
        retained.stage_table, interrupted.stage_table,
        "a change above the cursor must retain completed staging work"
    );
    assert!(
        stage_table_exists(database.pool(), &retained.stage_table).await?,
        "the retained stage table must remain available to the next page"
    );
    assert_eq!(retained.completed_source_count, 1);

    super::staging::cleanup_projection_checkpoint(database.pool(), "name_current").await?;
    database.cleanup().await
}

#[tokio::test]
async fn completed_range_change_fence_covers_every_projection_cursor_shape() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    let cases = [
        (
            "name_current",
            serde_json::json!("ens:alice.eth"),
            "RegistrationGranted",
        ),
        (
            "children_current",
            serde_json::json!(["ens:alice.eth", "bob.alice.eth", "ens:bob.alice.eth"]),
            "SubregistryChanged",
        ),
        (
            "permissions_current",
            serde_json::json!("00000000-0000-0000-0000-000000001002"),
            "PermissionChanged",
        ),
        (
            "record_inventory_current",
            serde_json::json!("00000000-0000-0000-0000-000000001002"),
            "RecordChanged",
        ),
        (
            "resolver_current",
            serde_json::json!([
                "ethereum-mainnet",
                "0x0000000000000000000000000000000000000def"
            ]),
            "ResolverChanged",
        ),
        (
            "address_names_current",
            serde_json::json!(["ens:alice.eth", "00000000-0000-0000-0000-000000001003"]),
            "RegistrationGranted",
        ),
        (
            "primary_names_current",
            serde_json::json!(["0x0000000000000000000000000000000000000abc", "ens", "60"]),
            "ReverseChanged",
        ),
    ];

    for (projection, cursor, event_kind) in cases {
        let mut checkpoint = super::staging::ProjectionStagingCheckpoint::load_or_start(
            database.pool(),
            projection,
            Some(108),
        )
        .await?;
        let old_stage_table = checkpoint.stage_table(0)?.to_owned();
        let input_fence = checkpoint.prepare_next_batch(database.pool()).await?;
        let progress = checkpoint.progress_after_batch(1, cursor, 0, 0)?;
        let mut transaction = database.pool().begin().await?;
        checkpoint
            .persist_progress(&mut transaction, &progress, &input_fence)
            .await?;
        transaction.commit().await?;
        checkpoint.accept_progress(progress, input_fence);
        append_event_change_for_kind(database.pool(), event_kind).await?;

        let replacement = super::staging::ProjectionStagingCheckpoint::load_or_start(
            database.pool(),
            projection,
            Some(108),
        )
        .await?;
        assert_ne!(
            replacement.stage_table(0)?,
            old_stage_table,
            "{projection} must fail closed when a relevant change reaches its completed cursor"
        );
        super::staging::cleanup_projection_checkpoint(database.pool(), projection).await?;
    }

    database.cleanup().await
}

#[tokio::test]
async fn staging_version_mismatch_discards_old_stage_and_restages_from_zero() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_second_name_page_termination(database.pool()).await?;
    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the injected second-page failure must leave an incomplete checkpoint");
    remove_second_name_page_termination(database.pool()).await?;
    let old = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(old.completed_source_count, 1);

    sqlx::query(
        r#"
        UPDATE current_projection_staging_checkpoints
        SET staging_schema_version = $1
        WHERE projection = 'name_current'
        "#,
    )
    // The initial schema version is 1 and storage accepts only positive versions;
    // reuse is exact-equality fenced, so either direction exercises the mismatch path.
    .bind(super::staging::current_staging_schema_version() + 1)
    .execute(database.pool())
    .await?;
    install_name_publish_failure(database.pool()).await?;
    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the publish stop must retain the replacement stage");

    let replacement = load_name_staging_checkpoint(database.pool()).await?;
    assert_ne!(replacement.stage_table, old.stage_table);
    assert_eq!(replacement.completed_source_count, 2);
    assert_eq!(replacement.status, "staging_complete");
    assert!(
        !stage_table_exists(database.pool(), &old.stage_table).await?,
        "version mismatch must drop the incompatible durable stage"
    );
    assert_eq!(
        count_stage_rows(database.pool(), &replacement.stage_table).await?,
        2,
        "version mismatch must restage the complete source set"
    );

    remove_name_publish_failure(database.pool()).await?;
    super::staging::cleanup_projection_checkpoint(database.pool(), "name_current").await?;
    database.cleanup().await
}

#[tokio::test]
async fn full_replay_input_revision_mismatch_discards_old_stage_and_restages() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_second_name_page_termination(database.pool()).await?;
    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the injected second-page failure must leave an incomplete checkpoint");
    remove_second_name_page_termination(database.pool()).await?;
    let old = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(old.completed_source_count, 1);

    sqlx::query(
        r#"
        UPDATE current_projection_full_replay_input_revision
        SET revision = revision + 1, updated_at = now()
        WHERE singleton
        "#,
    )
    .execute(database.pool())
    .await?;
    install_name_publish_failure(database.pool()).await?;
    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the publish stop must retain the replacement stage");

    let replacement = load_name_staging_checkpoint(database.pool()).await?;
    assert_ne!(replacement.stage_table, old.stage_table);
    assert_eq!(replacement.completed_source_count, 2);
    assert_eq!(replacement.status, "staging_complete");
    assert!(
        !stage_table_exists(database.pool(), &old.stage_table).await?,
        "direct-input revision mismatch must drop the stale durable stage"
    );

    remove_name_publish_failure(database.pool()).await?;
    super::staging::cleanup_projection_checkpoint(database.pool(), "name_current").await?;
    database.cleanup().await
}

#[tokio::test]
async fn replay_marker_failure_rolls_back_completed_stage_cleanup() -> Result<()> {
    let database = TestDatabase::new().await?;
    seed_replay_inputs(database.pool()).await?;
    install_name_replay_marker_failure(database.pool()).await?;

    let error = rebuild_pending_all_current_projections(database.pool(), Some(108), None, None)
        .await
        .expect_err("the injected marker failure must stop replay after name publication");
    assert!(format!("{error:#}").contains("injected name_current replay marker stop"));
    let retained = load_name_staging_checkpoint(database.pool()).await?;
    assert_eq!(retained.completed_source_count, 2);
    assert_eq!(retained.status, "staging_complete");
    assert!(
        stage_table_exists(database.pool(), &retained.stage_table).await?,
        "marker failure must roll back logged stage-table cleanup"
    );
    assert!(
        !super::progress::projection_replay_completed(database.pool(), "name_current", Some(108),)
            .await?,
        "marker failure must not leave a published-family skip marker"
    );

    remove_name_replay_marker_failure(database.pool()).await?;
    rebuild_pending_all_current_projections(database.pool(), Some(108), None, None).await?;
    assert!(
        super::progress::projection_replay_completed(database.pool(), "name_current", Some(108),)
            .await?,
        "retry must consume the retained stage and persist its marker"
    );
    assert!(
        !stage_table_exists(database.pool(), &retained.stage_table).await?,
        "successful marker commit must atomically remove the consumed stage"
    );

    database.cleanup().await
}

#[derive(Debug)]
struct NameStagingCheckpoint {
    stage_table: String,
    last_source_key: Option<Value>,
    completed_source_count: i64,
    status: String,
}

async fn load_name_staging_checkpoint(pool: &PgPool) -> Result<NameStagingCheckpoint> {
    let row = sqlx::query(
        r#"
        SELECT stage_tables, last_source_key, completed_source_count, status
        FROM current_projection_staging_checkpoints
        WHERE projection = 'name_current'
        "#,
    )
    .fetch_one(pool)
    .await?;
    let stage_tables: Vec<String> = row.try_get("stage_tables")?;
    Ok(NameStagingCheckpoint {
        stage_table: stage_tables
            .into_iter()
            .next()
            .context("name_current staging checkpoint must have one stage table")?,
        last_source_key: row.try_get("last_source_key")?,
        completed_source_count: row.try_get("completed_source_count")?,
        status: row.try_get("status")?,
    })
}

async fn count_stage_rows(pool: &PgPool, stage_table: &str) -> Result<i64> {
    validate_test_stage_table(stage_table)?;
    sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*)::BIGINT FROM {stage_table}"))
        .fetch_one(pool)
        .await
        .context("failed to count staged name_current rows")
}

async fn change_already_staged_name_source(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE name_surfaces
        SET canonical_display_name = 'Alice.eth'
        WHERE logical_name_id = 'ens:alice.eth'
        "#,
    )
    .execute(pool)
    .await
    .context("failed to mutate an already-staged name source")?;
    let updated = sqlx::query(
        r#"
        UPDATE normalized_events
        SET
            raw_fact_ref = COALESCE(raw_fact_ref, '{}'::JSONB)
                || '{"staging_content_revision":1}'::JSONB,
            canonicality_state = canonicality_state
        WHERE normalized_event_id = (
            SELECT normalized_event_id
            FROM normalized_events
            WHERE logical_name_id = 'ens:alice.eth'
            ORDER BY normalized_event_id
            LIMIT 1
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to supersede normalized-event content during staging")?
    .rows_affected();
    anyhow::ensure!(
        updated == 1,
        "the staging fixture must supersede one normalized event"
    );
    let content_update_recorded = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM projection_normalized_event_changes change
            JOIN normalized_events event
              ON event.normalized_event_id = change.normalized_event_id
            WHERE event.logical_name_id = 'ens:alice.eth'
              AND change.change_kind = 'content_update'
        )
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to inspect the normalized-event content-update journal")?;
    anyhow::ensure!(
        content_update_recorded,
        "the normalized-event trigger must journal the staging content update"
    );
    Ok(())
}

async fn change_not_yet_staged_name_source(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE name_surfaces
        SET canonical_display_name = 'Bob.alice.eth'
        WHERE logical_name_id = 'ens:bob.alice.eth'
        "#,
    )
    .execute(pool)
    .await
    .context("failed to mutate a not-yet-staged name source")?;
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
            event_kind,
            source_family,
            manifest_version,
            chain_id,
            block_number,
            block_hash,
            derivation_kind,
            canonicality_state,
            after_state
        )
        VALUES (
            'worker-replay:later-name-change',
            'ens',
            'ens:bob.alice.eth',
            'ResolverChanged',
            'ens_v1_registry_l1',
            1,
            'ethereum-mainnet',
            107,
            '0xreplay0107',
            'ens_v1_unwrapped_authority',
            'finalized'::canonicality_state,
            '{"resolver":"0x0000000000000000000000000000000000000def"}'::jsonb
        )
        "#,
    )
    .execute(pool)
    .await
    .context("failed to append a not-yet-staged name change")?;
    Ok(())
}

async fn append_event_change_for_kind(pool: &PgPool, event_kind: &str) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_normalized_event_changes (
            normalized_event_id,
            change_kind,
            canonicality_state
        )
        SELECT
            normalized_event_id,
            'canonicality_update',
            canonicality_state
        FROM normalized_events
        WHERE event_kind = $1
        ORDER BY normalized_event_id
        LIMIT 1
        "#,
    )
    .bind(event_kind)
    .execute(pool)
    .await
    .with_context(|| format!("failed to append {event_kind} projection change"))?;
    Ok(())
}

async fn load_stage_snapshot(pool: &PgPool, stage_table: &str) -> Result<String> {
    validate_test_stage_table(stage_table)?;
    sqlx::query_scalar::<_, String>(&format!(
        r#"
        SELECT COALESCE(
            JSONB_AGG(TO_JSONB(staged) ORDER BY logical_name_id),
            '[]'::JSONB
        )::TEXT
        FROM {stage_table} staged
        "#
    ))
    .fetch_one(pool)
    .await
    .context("failed to load staged name_current snapshot")
}

async fn stage_table_exists(pool: &PgPool, stage_table: &str) -> Result<bool> {
    validate_test_stage_table(stage_table)?;
    sqlx::query_scalar::<_, bool>("SELECT to_regclass(format('public.%I', $1)) IS NOT NULL")
        .bind(stage_table)
        .fetch_one(pool)
        .await
        .context("failed to inspect staged name_current table")
}

fn validate_test_stage_table(stage_table: &str) -> Result<()> {
    anyhow::ensure!(
        stage_table.starts_with("cprs_name_")
            && stage_table
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "unsafe test stage table {stage_table:?}"
    );
    Ok(())
}

async fn install_second_name_page_termination(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE FUNCTION terminate_second_name_staging_page() RETURNS TRIGGER AS $function$
        BEGIN
            IF NEW.projection = 'name_current'
               AND OLD.completed_source_count = 1
               AND NEW.completed_source_count = 2
            THEN
                PERFORM pg_terminate_backend(pg_backend_pid());
            END IF;
            RETURN NEW;
        END
        $function$ LANGUAGE plpgsql
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TRIGGER terminate_second_name_staging_page
        BEFORE UPDATE ON current_projection_staging_checkpoints
        FOR EACH ROW EXECUTE FUNCTION terminate_second_name_staging_page()
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn remove_second_name_page_termination(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "DROP TRIGGER terminate_second_name_staging_page ON current_projection_staging_checkpoints",
    )
    .execute(pool)
    .await?;
    sqlx::query("DROP FUNCTION terminate_second_name_staging_page()")
        .execute(pool)
        .await?;
    Ok(())
}

async fn install_name_staging_completion_termination(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE FUNCTION terminate_name_staging_completion() RETURNS TRIGGER AS $function$
        BEGIN
            IF NEW.projection = 'name_current'
               AND OLD.completed_source_count = 2
               AND OLD.status = 'running'
               AND NEW.status = 'staging_complete'
            THEN
                PERFORM pg_terminate_backend(pg_backend_pid());
            END IF;
            RETURN NEW;
        END
        $function$ LANGUAGE plpgsql
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TRIGGER terminate_name_staging_completion
        BEFORE UPDATE ON current_projection_staging_checkpoints
        FOR EACH ROW EXECUTE FUNCTION terminate_name_staging_completion()
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn remove_name_staging_completion_termination(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "DROP TRIGGER terminate_name_staging_completion \
         ON current_projection_staging_checkpoints",
    )
    .execute(pool)
    .await?;
    sqlx::query("DROP FUNCTION terminate_name_staging_completion()")
        .execute(pool)
        .await?;
    Ok(())
}

async fn install_name_publish_failure(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE FUNCTION fail_name_current_publish() RETURNS TRIGGER AS $function$
        BEGIN
            RAISE EXCEPTION 'injected name_current publish stop';
        END
        $function$ LANGUAGE plpgsql
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TRIGGER fail_name_current_publish
        BEFORE INSERT OR UPDATE OR DELETE ON name_current
        FOR EACH STATEMENT EXECUTE FUNCTION fail_name_current_publish()
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn remove_name_publish_failure(pool: &PgPool) -> Result<()> {
    sqlx::query("DROP TRIGGER fail_name_current_publish ON name_current")
        .execute(pool)
        .await?;
    sqlx::query("DROP FUNCTION fail_name_current_publish()")
        .execute(pool)
        .await?;
    Ok(())
}

async fn install_name_replay_marker_failure(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE FUNCTION fail_name_current_replay_marker() RETURNS TRIGGER AS $function$
        BEGIN
            IF NEW.projection = 'name_current' THEN
                RAISE EXCEPTION 'injected name_current replay marker stop';
            END IF;
            RETURN NEW;
        END
        $function$ LANGUAGE plpgsql
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        CREATE TRIGGER fail_name_current_replay_marker
        BEFORE INSERT OR UPDATE ON current_projection_replay_status
        FOR EACH ROW EXECUTE FUNCTION fail_name_current_replay_marker()
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn remove_name_replay_marker_failure(pool: &PgPool) -> Result<()> {
    sqlx::query("DROP TRIGGER fail_name_current_replay_marker ON current_projection_replay_status")
        .execute(pool)
        .await?;
    sqlx::query("DROP FUNCTION fail_name_current_replay_marker()")
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn stale_replay_version_marker_does_not_complete_projection() -> Result<()> {
    let database = TestDatabase::new().await?;

    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_status (
            projection,
            replay_version,
            completed_normalized_target_block,
            requested_key_count,
            upserted_row_count,
            deleted_row_count
        )
        VALUES ($1, $2, $3, 0, 0, 0)
        "#,
    )
    .bind("permissions_current")
    .bind(super::progress::CURRENT_PROJECTION_REPLAY_VERSION - 1)
    .bind(108_i64)
    .execute(database.pool())
    .await
    .context("failed to seed stale replay-version marker")?;

    assert!(
        !super::progress::projection_replay_completed(
            database.pool(),
            "permissions_current",
            Some(108),
        )
        .await?,
        "stale replay-version markers must not satisfy projection replay completion"
    );

    database.cleanup().await
}

#[tokio::test]
async fn stale_full_replay_input_revision_marker_does_not_complete_projection() -> Result<()> {
    let database = TestDatabase::new().await?;

    sqlx::query(
        r#"
        INSERT INTO current_projection_replay_status (
            projection,
            replay_version,
            completed_normalized_target_block,
            requested_key_count,
            upserted_row_count,
            deleted_row_count
        )
        VALUES ($1, $2, $3, 0, 0, 0)
        "#,
    )
    .bind("permissions_current")
    .bind(super::progress::CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(108_i64)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE current_projection_full_replay_input_revision
        SET revision = revision + 1, updated_at = now()
        WHERE singleton
        "#,
    )
    .execute(database.pool())
    .await?;

    assert!(
        !super::progress::projection_replay_completed(
            database.pool(),
            "permissions_current",
            Some(108),
        )
        .await?,
        "a marker from an older direct-input revision must force replay"
    );

    database.cleanup().await
}

async fn load_replay_status_rows(pool: &PgPool) -> Result<BTreeMap<String, (i32, Option<i64>)>> {
    let rows = sqlx::query_as::<_, (String, i32, Option<i64>)>(
        r#"
        SELECT projection, replay_version, completed_normalized_target_block
        FROM current_projection_replay_status
        ORDER BY projection
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load current projection replay status rows")?;

    Ok(rows
        .into_iter()
        .map(|(projection, version, target_block)| (projection, (version, target_block)))
        .collect())
}
