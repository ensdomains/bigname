use super::*;
use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use tokio::time::{Duration, sleep};

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("bigname_worker_auto_replay_test")
            .parse_context("failed to parse database URL for automatic projection replay tests")
            .admin_connect_context(
                "failed to connect admin pool for automatic projection replay tests",
            )
            .pool_connect_context("failed to connect automatic projection replay test pool"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for automatic projection replay tests",
    )
    .await
}

fn ready_status() -> ProjectionReplayReadiness {
    ProjectionReplayReadiness {
        normalized_replay_cursor_count: 1,
        incomplete_normalized_replay_cursor_count: 0,
        failed_normalized_replay_cursor_count: 0,
        active_index_build_count: 0,
        missing_projection_index_count: 0,
        normalized_replay_max_target_block: Some(42),
    }
}

#[tokio::test]
async fn graceful_shutdown_removes_the_active_worker_phase() -> Result<()> {
    let database = test_database().await?;
    let instance_id = "graceful-shutdown-phase-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
    )
    .await?;
    bigname_storage::begin_service_loop_phase(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
        "name_current.publish",
    )
    .await?;

    shutdown::run_until_shutdown(
        database.pool(),
        instance_id,
        std::future::pending::<Result<()>>(),
        std::future::ready(Ok(())),
    )
    .await?;

    let heartbeat_result = bigname_storage::record_service_loop_heartbeat(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
        &[],
    )
    .await;
    assert!(
        heartbeat_result.is_err(),
        "a deregistered loop must not recreate its process row"
    );

    let (process_count, phase_count) = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE scope_kind = 'process')::BIGINT,
            COUNT(*) FILTER (WHERE scope_kind = 'phase')::BIGINT
        FROM service_loop_heartbeats
        WHERE service_name = 'worker'
          AND instance_id = $1
        "#,
    )
    .bind(instance_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        process_count, 0,
        "graceful shutdown must deregister the loop"
    );
    assert_eq!(phase_count, 0, "graceful shutdown must not orphan a phase");

    database.cleanup().await
}

#[tokio::test]
async fn graceful_shutdown_cannot_leave_a_concurrently_inserted_phase() -> Result<()> {
    let database = test_database().await?;
    let instance_id = "graceful-shutdown-phase-race-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
    )
    .await?;
    bigname_storage::begin_service_loop_phase(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
        "name_current.publish",
    )
    .await?;

    let mut phase_lock = database.pool().begin().await?;
    sqlx::query(
        r#"
        SELECT 1
        FROM service_loop_heartbeats
        WHERE service_name = 'worker'
          AND instance_id = $1
          AND scope_kind = 'phase'
        FOR UPDATE
        "#,
    )
    .bind(instance_id)
    .fetch_one(&mut *phase_lock)
    .await?;

    let shutdown_pool = database.pool().clone();
    let shutdown_task = tokio::spawn(async move {
        shutdown::run_until_shutdown(
            &shutdown_pool,
            instance_id,
            std::future::pending::<Result<()>>(),
            std::future::ready(Ok(())),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cleanup_is_waiting = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                      AND query LIKE '%DELETE FROM service_loop_heartbeats%'
                      AND wait_event_type = 'Lock'
                )
                "#,
            )
            .fetch_one(database.pool())
            .await?;
            if cleanup_is_waiting {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("shutdown phase cleanup did not reach the locked row")??;

    let phase_writer_pool = database.pool().clone();
    let phase_writer = tokio::spawn(async move {
        bigname_storage::begin_service_loop_phase(
            &phase_writer_pool,
            bigname_storage::WORKER_SERVICE_NAME,
            instance_id,
            "resolver_current.publish",
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let phase_writer_is_waiting = sqlx::query_scalar::<_, bool>(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid <> pg_backend_pid()
                      AND query LIKE '%begin_service_loop_phase_registration_fence%'
                      AND wait_event_type = 'Lock'
                )
                "#,
            )
            .fetch_one(database.pool())
            .await?;
            if phase_writer_is_waiting {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("concurrent phase writer did not reach the locked row")??;

    phase_lock.commit().await?;
    shutdown_task.await??;
    let phase_writer_result = phase_writer.await?;
    assert!(
        phase_writer_result.is_err(),
        "a deregistered loop must reject a concurrent phase writer"
    );

    let phase_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM service_loop_heartbeats
        WHERE service_name = 'worker'
          AND instance_id = $1
          AND scope_kind = 'phase'
        "#,
    )
    .bind(instance_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        phase_count, 0,
        "graceful shutdown must fence concurrent phase writers before cleanup"
    );

    database.cleanup().await
}

#[test]
fn all_current_projection_pool_size_raises_low_default() {
    let database = all_current_projections_database_config(DatabaseConfig {
        database_url: None,
        max_connections: 10,
    });

    assert_eq!(database.max_connections, 64);
}

#[test]
fn all_current_projection_pool_size_preserves_higher_override() {
    let database = all_current_projections_database_config(DatabaseConfig {
        database_url: None,
        max_connections: 96,
    });

    assert_eq!(database.max_connections, 96);
}

#[test]
fn starting_primary_hydration_keeps_config_available_for_rebootstrap() {
    let configured = Some(primary_name::PrimaryNameLegacyReverseHydrationConfig::new(
        bigname_execution::ChainRpcUrls::default(),
    ));

    let background = background_primary_hydration_config(&configured, false);

    assert!(background.is_some());
    assert!(
        configured.is_some(),
        "starting the background hydrator must not consume the replay hydration config"
    );
}

#[test]
fn projection_replay_waits_for_normalized_replay_cursor() {
    let status = ProjectionReplayReadiness {
        normalized_replay_cursor_count: 0,
        ..ready_status()
    };

    assert!(!status.is_ready());
}

#[test]
fn projection_replay_waits_for_complete_normalized_replay() {
    let status = ProjectionReplayReadiness {
        incomplete_normalized_replay_cursor_count: 1,
        ..ready_status()
    };

    assert!(!status.is_ready());
}

#[test]
fn projection_replay_waits_for_projection_indexes() {
    let status = ProjectionReplayReadiness {
        active_index_build_count: 1,
        ..ready_status()
    };
    assert!(!status.is_ready());

    let status = ProjectionReplayReadiness {
        missing_projection_index_count: 1,
        ..ready_status()
    };
    assert!(!status.is_ready());
}

#[test]
fn active_index_build_probe_is_scoped_to_current_database() {
    assert!(
        ACTIVE_INDEX_BUILDS_QUERY.contains("datname = current_database()"),
        "projection replay readiness must ignore index builds in other databases"
    );
}

#[tokio::test]
async fn projection_replay_treats_invalid_required_index_as_missing() -> Result<()> {
    let database = test_database().await?;
    sqlx::query("DROP INDEX normalized_events_record_inventory_resource_replay_idx")
        .execute(database.pool())
        .await
        .context("failed to drop required projection index for invalid-index test")?;
    sqlx::query("CREATE TABLE invalid_projection_index_fixture (duplicate_value INTEGER NOT NULL)")
        .execute(database.pool())
        .await
        .context("failed to create invalid-index fixture table")?;
    sqlx::query("INSERT INTO invalid_projection_index_fixture (duplicate_value) VALUES (1), (1)")
        .execute(database.pool())
        .await
        .context("failed to seed invalid-index fixture rows")?;

    sqlx::query(
        "CREATE UNIQUE INDEX CONCURRENTLY normalized_events_record_inventory_resource_replay_idx \
         ON invalid_projection_index_fixture (duplicate_value)",
    )
    .execute(database.pool())
    .await
    .expect_err("duplicate fixture rows must leave an invalid concurrent index remnant");

    let (is_valid, is_ready) = sqlx::query_as::<_, (bool, bool)>(
        r#"
        SELECT index.indisvalid, index.indisready
        FROM pg_index AS index
        WHERE index.indexrelid =
            to_regclass('normalized_events_record_inventory_resource_replay_idx')
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to inspect invalid required projection index")?;
    assert!(!is_valid || !is_ready);
    assert_eq!(
        missing_projection_index_count(database.pool()).await?,
        1,
        "an existing but invalid required index must keep automatic replay unready"
    );

    let (first_retry, second_retry) = tokio::join!(
        bigname_storage::migrate(database.pool()),
        bigname_storage::migrate(database.pool())
    );
    first_retry.context("first migration retry must repair an invalid concurrent index remnant")?;
    second_retry.context("concurrent migration retry must observe the serialized ready index")?;
    assert_eq!(missing_projection_index_count(database.pool()).await?, 0);
    assert!(
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT index.indisvalid AND index.indisready
            FROM pg_index AS index
            WHERE index.indexrelid =
                to_regclass('normalized_events_record_inventory_resource_replay_idx')
              AND index.indrelid = 'normalized_events'::regclass
            "#,
        )
        .fetch_one(database.pool())
        .await?,
        "migration retry must replace the invalid remnant with the required ready index"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn migration_preserves_intentionally_deferred_normalized_replay_index() -> Result<()> {
    let database = test_database().await?;
    sqlx::query(
        r#"
        CREATE INDEX normalized_events_replay_latest_resolver_tmp_idx
        ON normalized_events (normalized_event_id)
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to create normalized replay deferral marker index")?;
    sqlx::query("DROP INDEX normalized_events_record_inventory_resource_replay_idx")
        .execute(database.pool())
        .await
        .context("failed to defer record-inventory replay index")?;
    bigname_storage::migrate(database.pool())
        .await
        .context("migration must accept intentionally deferred replay indexes")?;
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT to_regclass('normalized_events_record_inventory_resource_replay_idx') IS NOT NULL",
        )
        .fetch_one(database.pool())
        .await?,
        "worker migrate must not rebuild an index deliberately deferred by normalized replay"
    );

    sqlx::query("DROP INDEX normalized_events_replay_latest_resolver_tmp_idx")
        .execute(database.pool())
        .await
        .context("failed to clear normalized replay deferral marker index")?;
    bigname_storage::migrate(database.pool())
        .await
        .context("migration must repair an accidentally missing replay index")?;
    assert!(
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT index.indisvalid AND index.indisready
            FROM pg_index AS index
            WHERE index.indexrelid =
                    to_regclass('normalized_events_record_inventory_resource_replay_idx')
              AND index.indrelid = 'normalized_events'::regclass
            "#,
        )
        .fetch_one(database.pool())
        .await?,
        "migration must repair the index once normalized replay is no longer deferring it"
    );

    database.cleanup().await?;
    Ok(())
}

#[test]
fn projection_replay_runs_when_normalized_replay_and_indexes_are_ready() {
    assert!(ready_status().is_ready());
}

#[test]
fn fresh_bootstrap_seeds_apply_cursor_at_captured_watermark() {
    assert_eq!(
        projection_bootstrap_apply_cursor_seed(false, 17),
        Some(projection_apply::NormalizedEventChangeCursor { change_id: 17 })
    );
    assert_eq!(projection_bootstrap_apply_cursor_seed(true, 17), None);
}

#[test]
fn resumed_bootstrap_without_apply_cursor_replays_change_log_from_beginning() {
    assert_eq!(
        projection_bootstrap_apply_cursor_seed(false, 0),
        Some(projection_apply::NormalizedEventChangeCursor { change_id: 0 })
    );
}

#[test]
fn bootstrap_target_covers_live_checkpoint_head() {
    assert_eq!(
        projection_bootstrap_replay_target_block(Some(10), Some(15)),
        Some(15)
    );
    assert_eq!(
        projection_bootstrap_replay_target_block(Some(15), Some(10)),
        Some(15)
    );
}

#[test]
fn restart_bootstrap_skip_requires_apply_cursor_and_all_current_markers() {
    let complete_marker_count = replay::ALL_CURRENT_PROJECTION_ORDER.len() as i64;

    assert!(should_skip_bootstrap_for_existing_apply_cursor(
        true,
        complete_marker_count
    ));
    assert!(!should_skip_bootstrap_for_existing_apply_cursor(
        false,
        complete_marker_count
    ));
    assert!(!should_skip_bootstrap_for_existing_apply_cursor(
        true,
        complete_marker_count - 1
    ));
}

#[tokio::test]
async fn restart_bootstrap_skip_requires_apply_cursor_even_with_target_covering_replay_markers()
-> Result<()> {
    let database = test_database().await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    seed_replay_markers(database.pool(), 20).await?;

    assert!(
        !projection_bootstrap_already_handed_off_to_apply(database.pool()).await?,
        "replay markers must not hand off bootstrap before incremental apply has a cursor"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn restart_with_pre_marker_staging_replays_changes_from_before_the_stage() -> Result<()> {
    let database = test_database().await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    let _checkpoint = replay::staging::ProjectionStagingCheckpoint::load_or_start(
        database.pool(),
        "name_current",
        Some(20),
    )
    .await?;
    insert_normalized_event_change(database.pool(), "pre-marker-stage-change").await?;

    assert!(
        replay_all_current_projections_when_ready(database.pool(), None, None).await?,
        "ready automatic replay should complete bootstrap handoff"
    );
    let cursor = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT last_change_id
        FROM projection_apply_cursors
        WHERE cursor_name = 'normalized_events_to_projection_invalidations'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        cursor, 0,
        "durable staging without a family marker must retain the pre-stage apply baseline"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn restart_with_started_staging_keeps_the_original_target_when_chain_head_advances()
-> Result<()> {
    let database = test_database().await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    let checkpoint = replay::staging::ProjectionStagingCheckpoint::load_or_start(
        database.pool(),
        "name_current",
        Some(20),
    )
    .await?;
    let original_stage_table = checkpoint.stage_table(0)?.to_owned();

    advance_chain_checkpoint(database.pool(), 21).await?;
    let candidate_target = projection_bootstrap_replay_target_block(Some(20), Some(21));
    let attempt = bootstrap_attempt::start_projection_replay_attempt(
        database.pool(),
        candidate_target,
        projection_apply::NormalizedEventChangeCursor { change_id: 0 },
    )
    .await?;
    assert_eq!(
        attempt.normalized_target_block,
        Some(20),
        "the durable replay attempt must retain the target of in-flight staging"
    );
    let restarted = replay::staging::ProjectionStagingCheckpoint::load_or_start(
        database.pool(),
        "name_current",
        attempt.normalized_target_block,
    )
    .await?;
    assert_eq!(
        restarted.stage_table(0)?,
        original_stage_table,
        "ordinary chain-head advancement must not discard a reusable in-flight stage"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn manual_replay_reuses_the_persisted_attempt_target() -> Result<()> {
    let database = test_database().await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    bootstrap_attempt::start_projection_replay_attempt(
        database.pool(),
        Some(20),
        projection_apply::NormalizedEventChangeCursor { change_id: 0 },
    )
    .await?;
    advance_chain_checkpoint(database.pool(), 21).await?;

    let attempt = manual_replay::resolve_manual_projection_replay_attempt(database.pool()).await?;
    assert_eq!(
        attempt.normalized_target_block,
        Some(20),
        "manual replay must join the existing attempt instead of replacing its target"
    );

    database.cleanup().await
}

#[tokio::test]
async fn manual_replay_fails_cleanly_when_automatic_replay_holds_the_lock() -> Result<()> {
    let database = test_database().await?;
    let mut replay_lock = try_acquire_replay_lock(database.pool())
        .await?
        .context("the test must acquire the automatic replay lock")?;

    let error = manual_replay::replay_all_current_projections_manually(database.pool(), None, None)
        .await
        .expect_err("manual replay must fail fast while automatic replay owns the lock");
    assert!(
        format!("{error:#}").contains("automatic replay owns the cross-process replay lock"),
        "unexpected manual replay lock error: {error:#}"
    );

    release_replay_lock(&mut replay_lock).await?;
    database.cleanup().await
}

#[tokio::test]
async fn manual_replay_without_attempt_or_head_proceeds_targetless() -> Result<()> {
    let database = test_database().await?;
    assert!(
        bootstrap_attempt::load_projection_replay_attempt(database.pool())
            .await?
            .is_none(),
        "a fresh database must not have a durable replay attempt"
    );
    let readiness = load_projection_replay_readiness(database.pool()).await?;
    assert_eq!(readiness.normalized_replay_max_target_block, None);
    assert_eq!(
        projection_apply::load_chain_checkpoint_max_block(database.pool()).await?,
        None
    );

    manual_replay::replay_all_current_projections_manually(database.pool(), None, None).await?;
    let (marker_count, targetless_marker_count) = sqlx::query_as::<_, (i64, i64)>(
        r#"
        SELECT
            COUNT(*)::BIGINT,
            COUNT(*) FILTER (
                WHERE completed_normalized_target_block IS NULL
            )::BIGINT
        FROM current_projection_replay_status
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        marker_count,
        replay::ALL_CURRENT_PROJECTION_ORDER.len() as i64
    );
    assert_eq!(
        targetless_marker_count, marker_count,
        "a replay without an attempt or head must retain NULL-target markers"
    );
    assert!(
        bootstrap_attempt::load_projection_replay_attempt(database.pool())
            .await?
            .is_none(),
        "target-less manual replay must not create an automatic handoff attempt"
    );

    database.cleanup().await
}

#[tokio::test]
async fn manual_replay_markers_carry_the_real_target_and_satisfy_handoff() -> Result<()> {
    let database = test_database().await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;

    manual_replay::replay_all_current_projections_manually(database.pool(), None, None).await?;
    let target_marker_count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM current_projection_replay_status
        WHERE completed_normalized_target_block = 20
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        target_marker_count,
        replay::ALL_CURRENT_PROJECTION_ORDER.len() as i64,
        "manual replay must write a real target on every family marker"
    );

    let attempt = bootstrap_attempt::load_projection_replay_attempt(database.pool())
        .await?
        .context("manual replay must retain its attempt for automatic handoff")?;
    let cursor_seed =
        projection_bootstrap_apply_cursor_seed(false, attempt.apply_baseline_change_id);
    bootstrap_attempt::finalize_projection_replay_attempt(database.pool(), attempt, cursor_seed)
        .await?;
    assert!(
        projection_apply::normalized_event_cursor_exists(database.pool()).await?,
        "the target-bearing manual markers must satisfy automatic handoff"
    );

    database.cleanup().await
}

#[tokio::test]
async fn completed_handoff_cleans_a_leftover_staging_checkpoint() -> Result<()> {
    let database = test_database().await?;
    seed_apply_cursor(database.pool()).await?;
    seed_replay_markers(database.pool(), 20).await?;
    let checkpoint = replay::staging::ProjectionStagingCheckpoint::load_or_start(
        database.pool(),
        "name_current",
        Some(20),
    )
    .await?;
    let stage_table = checkpoint.stage_table(0)?.to_owned();

    assert!(
        projection_bootstrap_already_handed_off_to_apply(database.pool()).await?,
        "complete markers and an apply cursor should hand off bootstrap"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM current_projection_staging_checkpoints"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "handoff must clean a checkpoint left after marker commit"
    );
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
            .bind(stage_table)
            .fetch_one(database.pool())
            .await?,
        "handoff must drop the leftover logged stage table"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn replay_handoff_rejects_an_input_revision_change_after_family_markers() -> Result<()> {
    let database = test_database().await?;
    let attempt = bootstrap_attempt::start_projection_replay_attempt(
        database.pool(),
        Some(20),
        projection_apply::NormalizedEventChangeCursor { change_id: 7 },
    )
    .await?;
    seed_replay_markers(database.pool(), 20).await?;
    sqlx::query(
        r#"
        UPDATE current_projection_full_replay_input_revision
        SET revision = revision + 1, updated_at = now()
        WHERE singleton
        "#,
    )
    .execute(database.pool())
    .await?;

    let error = bootstrap_attempt::finalize_projection_replay_attempt(
        database.pool(),
        attempt,
        Some(projection_apply::NormalizedEventChangeCursor { change_id: 7 }),
    )
    .await
    .expect_err("handoff must fail when direct inputs change after family publication");
    assert!(format!("{error:#}").contains("input revision changed"));
    assert!(
        !projection_apply::normalized_event_cursor_exists(database.pool()).await?,
        "failed handoff must not acknowledge the replay apply baseline"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn restart_after_markers_does_not_seed_past_post_marker_change() -> Result<()> {
    let database = test_database().await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    seed_replay_markers(database.pool(), 20).await?;

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
            'automatic-replay:post-marker-change',
            'ens',
            'ens:post-marker.eth',
            'ResolverChanged',
            'ens_v1_registry_l1',
            1,
            'ethereum-mainnet',
            20,
            '0xpostmarker20',
            'ens_v1_unwrapped_authority',
            'canonical'::canonicality_state,
            '{"resolver":"0x0000000000000000000000000000000000000abc"}'::jsonb
        )
        "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert post-marker normalized-event change")?;
    let post_marker_change_id = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(change_id), 0) FROM projection_normalized_event_changes",
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load post-marker normalized-event change id")?;

    assert!(
        replay_all_current_projections_when_ready(database.pool(), None, None).await?,
        "ready automatic replay should complete bootstrap handoff"
    );
    let cursor = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT last_change_id
        FROM projection_apply_cursors
        WHERE cursor_name = 'normalized_events_to_projection_invalidations'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load automatic projection replay cursor")?;
    assert_eq!(
        cursor, 0,
        "markers without an apply cursor must replay the change log from the beginning"
    );

    let derive_summary = projection_apply::derive_once(database.pool()).await?;
    assert_eq!(
        derive_summary.scanned_event_count, 1,
        "the post-marker change must be consumed after bootstrap handoff"
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM projection_invalidations
                WHERE projection = 'name_current'
                  AND projection_key = 'ens:post-marker.eth'
            )
            "#,
        )
        .fetch_one(database.pool())
        .await?,
        "the post-marker change must enqueue its stale name projection key"
    );
    let derived_cursor = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT last_change_id
        FROM projection_apply_cursors
        WHERE cursor_name = 'normalized_events_to_projection_invalidations'
        "#,
    )
    .fetch_one(database.pool())
    .await
    .context("failed to load derived automatic projection replay cursor")?;
    assert_eq!(derived_cursor, post_marker_change_id);

    database.cleanup().await?;
    Ok(())
}

async fn insert_normalized_event_change(pool: &PgPool, suffix: &str) -> Result<()> {
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
            $1,
            'ens',
            $2,
            'ResolverChanged',
            'ens_v1_registry_l1',
            1,
            'ethereum-mainnet',
            20,
            $3,
            'ens_v1_unwrapped_authority',
            'canonical'::canonicality_state,
            '{"resolver":"0x0000000000000000000000000000000000000abc"}'::jsonb
        )
        "#,
    )
    .bind(format!("automatic-replay:{suffix}"))
    .bind(format!("ens:{suffix}.eth"))
    .bind(format!("0x{suffix}"))
    .execute(pool)
    .await
    .context("failed to insert normalized-event change fixture")?;
    Ok(())
}

#[tokio::test]
async fn restart_handoff_with_apply_cursor_ignores_later_chain_checkpoint() -> Result<()> {
    let database = test_database().await?;
    seed_apply_cursor(database.pool()).await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    seed_replay_markers(database.pool(), 20).await?;

    assert!(
        projection_bootstrap_already_handed_off_to_apply(database.pool()).await?,
        "current replay markers and an apply cursor should hand off bootstrap"
    );

    advance_chain_checkpoint(database.pool(), 21).await?;

    assert!(
        projection_bootstrap_already_handed_off_to_apply(database.pool()).await?,
        "an existing apply cursor should own post-handoff checkpoint progress"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn restart_handoff_rejects_previous_replay_version_markers() -> Result<()> {
    let database = test_database().await?;
    seed_apply_cursor(database.pool()).await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    seed_replay_markers(database.pool(), 20).await?;

    sqlx::query(
        r#"
        UPDATE current_projection_replay_status
        SET replay_version = $1
        "#,
    )
    .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION - 1)
    .execute(database.pool())
    .await
    .context("failed to make current projection replay markers stale")?;

    assert!(
        !projection_bootstrap_already_handed_off_to_apply(database.pool()).await?,
        "an apply cursor plus previous-version markers must force bootstrap replay"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn running_worker_reenters_bootstrap_after_direct_input_revision_advance() -> Result<()> {
    let database = test_database().await?;
    seed_apply_cursor(database.pool()).await?;
    sqlx::query(
        "UPDATE projection_apply_cursors SET last_change_id = 0 WHERE cursor_name = 'normalized_events_to_projection_invalidations'",
    )
    .execute(database.pool())
    .await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    seed_replay_markers(database.pool(), 20).await?;
    let instance_id = "direct-input-revision-rebootstrap-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::WORKER_SERVICE_NAME,
        instance_id,
    )
    .await?;

    let worker_pool = database.pool().clone();
    let worker = tokio::spawn(run_automatic_current_projection_replay(
        worker_pool,
        instance_id.to_owned(),
        1,
        None,
        None,
    ));
    insert_normalized_event_change(database.pool(), "running-worker-ready").await?;
    let ready_change_id = sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(change_id), 0) FROM projection_normalized_event_changes",
    )
    .fetch_one(database.pool())
    .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let cursor = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT last_change_id
                FROM projection_apply_cursors
                WHERE cursor_name = 'normalized_events_to_projection_invalidations'
                "#,
            )
            .fetch_one(database.pool())
            .await?;
            if cursor >= ready_change_id {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("running worker did not enter continuous projection apply")??;

    let mut transaction = database.pool().begin().await?;
    bigname_storage::projection_staging::advance_current_projection_full_replay_input_revision_in_transaction(
        &mut transaction,
    )
    .await?;
    transaction.commit().await?;

    let rebootstrap_observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let marker_count = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)::BIGINT
                FROM current_projection_replay_status
                WHERE replay_version = $1
                  AND full_replay_input_revision = 1
                "#,
            )
            .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
            .fetch_one(database.pool())
            .await?;
            if marker_count == replay::ALL_CURRENT_PROJECTION_ORDER.len() as i64 {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();

    worker.abort();
    let _ = worker.await;
    database.cleanup().await?;
    assert!(
        rebootstrap_observed,
        "an already-running worker must observe direct-input revision invalidation and replay"
    );
    Ok(())
}

#[tokio::test]
async fn version_9_handoff_runs_version_10_replay_and_preserves_permission_compatibility()
-> Result<()> {
    let database = test_database().await?;
    seed_apply_cursor(database.pool()).await?;
    seed_ready_normalized_replay_cursor(database.pool(), 20).await?;
    seed_chain_checkpoint(database.pool(), 20).await?;
    seed_replay_markers(database.pool(), 20).await?;
    sqlx::query("UPDATE current_projection_replay_status SET replay_version = 9")
        .execute(database.pool())
        .await
        .context("failed to seed completed version-9 replay markers")?;

    assert_eq!(replay::CURRENT_PROJECTION_REPLAY_VERSION, 10);
    assert!(
        !projection_bootstrap_already_handed_off_to_apply(database.pool()).await?,
        "version-9 markers and an apply cursor must not skip the version-10 full replay"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM permissions_current_publication")
            .fetch_one(database.pool())
            .await?,
        0
    );

    assert!(
        replay_all_current_projections_when_ready(database.pool(), None, None).await?,
        "version-10 automatic startup must complete the required full replay"
    );
    let publication = sqlx::query_as::<_, (i32, i64)>(
        r#"
        SELECT publication_version, data_revision
        FROM permissions_current_publication
        WHERE projection = 'permissions_current'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        publication,
        (bigname_storage::PERMISSIONS_CURRENT_PUBLICATION_VERSION, 1)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM current_projection_replay_status WHERE replay_version = $1",
        )
        .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
        .fetch_one(database.pool())
        .await?,
        replay::ALL_CURRENT_PROJECTION_ORDER.len() as i64
    );

    database.cleanup().await?;
    Ok(())
}

#[test]
fn primary_hydration_start_is_independent_from_text_hydration_completion() {
    assert_eq!(
        bootstrap_hydration_schedule(false, false),
        BootstrapHydrationSchedule {
            start_primary_hydration: true,
            run_text_hydration: true,
        }
    );
    assert_eq!(
        bootstrap_hydration_schedule(false, true),
        BootstrapHydrationSchedule {
            start_primary_hydration: false,
            run_text_hydration: true,
        }
    );
}

async fn seed_apply_cursor(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO projection_apply_cursors (cursor_name, last_change_id)
        VALUES ('normalized_events_to_projection_invalidations', 1)
        "#,
    )
    .execute(pool)
    .await
    .context("failed to seed projection apply cursor")?;
    Ok(())
}

async fn seed_ready_normalized_replay_cursor(pool: &PgPool, target_block: i64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_completed_block_number
        )
        VALUES (
            'ens',
            'ethereum-mainnet',
            'raw_fact_normalized_events',
            0,
            $1 + 1,
            $1,
            $1
        )
        "#,
    )
    .bind(target_block)
    .execute(pool)
    .await
    .context("failed to seed normalized replay cursor")?;
    Ok(())
}

async fn seed_chain_checkpoint(pool: &PgPool, block_number: i64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (
            chain_id,
            canonical_block_hash,
            canonical_block_number
        )
        VALUES ('ethereum-mainnet', '0xcheckpoint', $1)
        "#,
    )
    .bind(block_number)
    .execute(pool)
    .await
    .context("failed to seed chain checkpoint")?;
    Ok(())
}

async fn advance_chain_checkpoint(pool: &PgPool, block_number: i64) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE chain_checkpoints
        SET canonical_block_hash = $1, canonical_block_number = $2
        WHERE chain_id = 'ethereum-mainnet'
        "#,
    )
    .bind(format!("0xcheckpoint{block_number}"))
    .bind(block_number)
    .execute(pool)
    .await
    .context("failed to advance chain checkpoint")?;
    Ok(())
}

async fn seed_replay_markers(pool: &PgPool, completed_target_block: i64) -> Result<()> {
    for projection in replay::ALL_CURRENT_PROJECTION_ORDER {
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
            ON CONFLICT (projection)
            DO UPDATE SET
                replay_version = EXCLUDED.replay_version,
                completed_normalized_target_block =
                    EXCLUDED.completed_normalized_target_block
            "#,
        )
        .bind(*projection)
        .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
        .bind(completed_target_block)
        .execute(pool)
        .await
        .with_context(|| format!("failed to seed replay marker for {projection}"))?;
    }
    Ok(())
}
