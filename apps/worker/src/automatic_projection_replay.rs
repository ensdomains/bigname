use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result};
use bigname_storage::{
    DEFERRED_NORMALIZED_EVENT_INDEXES, DatabaseConfig, count_unready_normalized_event_indexes,
};
use sqlx::{PgPool, Postgres, pool::PoolConnection};
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tracing::{debug, info, warn};

use crate::primary_name::rebuild_heartbeat as heartbeat;
use crate::{cli::RunArgs, primary_name, projection_apply, record_inventory, replay};

#[path = "automatic_projection_replay/bootstrap_attempt.rs"]
mod bootstrap_attempt;
#[path = "automatic_projection_replay/bootstrap_replay.rs"]
mod bootstrap_replay;
#[path = "automatic_projection_replay/invalidation_derive_loop.rs"]
mod invalidation_derive_loop;
#[path = "automatic_projection_replay/manual_replay.rs"]
mod manual_replay;
#[path = "automatic_projection_replay/primary_hydration.rs"]
mod primary_hydration;
#[path = "automatic_projection_replay/primary_hydration_loop.rs"]
mod primary_hydration_loop;
#[path = "automatic_projection_replay/primary_name_cache_pruning.rs"]
mod primary_name_cache_pruning;
#[path = "automatic_projection_replay/shutdown.rs"]
mod shutdown;
#[path = "automatic_projection_replay/subtask_supervision.rs"]
mod subtask_supervision;

#[cfg(test)]
use bootstrap_replay::replay_all_current_projections_when_ready;
use bootstrap_replay::replay_all_current_projections_when_ready_with_heartbeat;
pub(crate) use manual_replay::replay_all_current_projections_manually;

const CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS: &str = "raw_fact_normalized_events";
const ALL_CURRENT_PROJECTIONS_MIN_DATABASE_CONNECTIONS: u32 = 64;
const ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY: i64 = 0x4249474e414d4501_i64;
const ACTIVE_INDEX_BUILDS_QUERY: &str = r#"
    SELECT COUNT(*)::bigint
    FROM pg_stat_progress_create_index
    WHERE datname = current_database()
"#;

type SharedLoopHeartbeat = Arc<Mutex<heartbeat::LoopHeartbeat>>;

pub(crate) fn all_current_projections_database_config(
    mut database: DatabaseConfig,
) -> DatabaseConfig {
    database.max_connections = database
        .max_connections
        .max(ALL_CURRENT_PROJECTIONS_MIN_DATABASE_CONNECTIONS);
    database
}

pub(crate) async fn run_worker(args: RunArgs) -> Result<()> {
    let heartbeat_instance_id =
        bigname_storage::resolve_service_instance_id(args.heartbeat_instance_id.as_deref())?;
    let database = all_current_projections_database_config(args.database);
    let (pool, _runtime_rederive_guard) =
        bigname_storage::connect_with_base_normalized_rederive_writer_guard(
            &database,
            "bigname-worker",
        )
        .await?;
    let text_hydration_config =
        record_inventory::RecordInventoryTextHydrationConfig::from_chain_rpc_url_entries(
            &args.chain_rpc_urls,
            args.text_hydration_multicall3_address.clone(),
            args.text_hydration_batch_size,
        )?;
    let primary_hydration_config =
        primary_name::PrimaryNameLegacyReverseHydrationConfig::from_chain_rpc_url_entries(
            &args.chain_rpc_urls,
            args.legacy_reverse_hydration_multicall3_address,
            args.legacy_reverse_hydration_batch_size,
            &args.legacy_reverse_resolver_addresses,
        )?;

    info!(
        service = "worker",
        version = crate::SOFTWARE_VERSION,
        build_sha = crate::BUILD_SHA,
        schema_migration_version = bigname_storage::latest_migration_version(),
        projection_replay_version = replay::CURRENT_PROJECTION_REPLAY_VERSION,
        permissions_current_publication_version =
            bigname_storage::PERMISSIONS_CURRENT_PUBLICATION_VERSION,
        poll_interval_secs = args.poll_interval_secs,
        database_max_connections = database.max_connections,
        automatic_projection_replay = true,
        record_inventory_text_hydration = text_hydration_config.is_some(),
        primary_name_legacy_reverse_hydration = primary_hydration_config.is_some(),
        primary_name_route_cache_retention_checkpoints =
            args.primary_name_route_cache_retention_checkpoints,
        primary_name_route_cache_prune_batch_size = args.primary_name_route_cache_prune_batch_size,
        "worker booted"
    );

    bigname_storage::register_service_loop(
        &pool,
        bigname_storage::WORKER_SERVICE_NAME,
        &heartbeat_instance_id,
    )
    .await?;

    primary_name_cache_pruning::spawn(
        pool.clone(),
        args.poll_interval_secs,
        args.primary_name_route_cache_retention_checkpoints,
        args.primary_name_route_cache_prune_batch_size,
    );

    let replay_pool = pool.clone();
    let replay_heartbeat_instance_id = heartbeat_instance_id.clone();
    shutdown::run_until_shutdown(
        &pool,
        &heartbeat_instance_id,
        run_automatic_current_projection_replay(
            replay_pool,
            replay_heartbeat_instance_id,
            args.poll_interval_secs,
            text_hydration_config,
            primary_hydration_config,
        ),
        shutdown::shutdown_signal(),
    )
    .await
}

pub(crate) async fn run_automatic_current_projection_replay(
    pool: PgPool,
    heartbeat_instance_id: String,
    poll_interval_secs: u64,
    text_hydration_config: Option<record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<primary_name::PrimaryNameLegacyReverseHydrationConfig>,
) -> Result<()> {
    let (subtasks, monitor) = subtask_supervision::channel("worker");
    monitor
        .run(run_automatic_current_projection_replay_loop(
            pool,
            heartbeat_instance_id,
            poll_interval_secs,
            text_hydration_config,
            primary_hydration_config,
            subtasks,
        ))
        .await
}

async fn run_automatic_current_projection_replay_loop(
    pool: PgPool,
    heartbeat_instance_id: String,
    poll_interval_secs: u64,
    text_hydration_config: Option<record_inventory::RecordInventoryTextHydrationConfig>,
    primary_hydration_config: Option<primary_name::PrimaryNameLegacyReverseHydrationConfig>,
    subtasks: subtask_supervision::SubtaskSpawner,
) -> Result<()> {
    let poll_interval = Duration::from_secs(poll_interval_secs.max(1));
    let mut bootstrap_completed = false;
    let mut bootstrap_text_hydration_completed = text_hydration_config.is_none();
    let mut primary_hydration_started = primary_hydration_config.is_none();
    let mut projection_derivation_started = false;
    let projection_apply_generation = Arc::new(AtomicU64::new(0));
    let projection_apply_hydration_lock = Arc::new(Mutex::new(()));
    let invalidation_derive_activity = heartbeat::RequiredSubtaskActivity::default();
    let derive_heartbeat =
        heartbeat::LoopHeartbeat::new(heartbeat_instance_id.clone(), poll_interval);
    let loop_heartbeat = Arc::new(Mutex::new(heartbeat::LoopHeartbeat::new(
        heartbeat_instance_id,
        poll_interval,
    )));
    let mut derive_heartbeat = Some(derive_heartbeat);

    loop {
        record_loop_heartbeat_if_due(&pool, &loop_heartbeat, &invalidation_derive_activity).await;

        let mut progressed = false;
        if bootstrap_completed {
            match projection_bootstrap_handoff_is_current(&pool).await {
                Ok(true) => {}
                Ok(false) => {
                    bootstrap_completed = false;
                    progressed = true;
                    info!(
                        service = "worker",
                        replay = "all_current_projections",
                        "automatic all-current projection replay re-entered after durable handoff invalidation"
                    );
                }
                Err(error) => {
                    bootstrap_completed = false;
                    warn!(
                        service = "worker",
                        replay = "all_current_projections",
                        error = %format!("{error:#}"),
                        "failed to revalidate automatic all-current projection replay handoff; continuous apply paused"
                    );
                }
            }
        }
        if !bootstrap_completed {
            match projection_bootstrap_already_handed_off_to_apply(&pool).await {
                Ok(true) => {
                    bootstrap_completed = true;
                    progressed = true;
                    info!(
                        service = "worker",
                        replay = "all_current_projections",
                        "automatic all-current projection replay skipped because durable apply cursor and target-covering replay markers exist"
                    );
                }
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        service = "worker",
                        replay = "all_current_projections",
                        error = %format!("{error:#}"),
                        "failed to inspect automatic all-current projection replay handoff state"
                    );
                }
            }
        }

        if !bootstrap_completed {
            let replay_result = {
                let _apply_hydration_guard = projection_apply_hydration_lock.lock().await;
                let mut loop_heartbeat = loop_heartbeat.lock().await;
                replay_all_current_projections_when_ready_with_heartbeat(
                    &pool,
                    text_hydration_config.as_ref(),
                    primary_hydration_config.as_ref(),
                    &mut loop_heartbeat,
                )
                .await
            };
            match replay_result {
                Ok(true) => {
                    bootstrap_completed = true;
                    progressed = true;
                }
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        service = "worker",
                        replay = "all_current_projections",
                        error = %format!("{error:#}"),
                        "automatic all-current projection replay failed"
                    );
                }
            }
            record_loop_heartbeat_if_due(&pool, &loop_heartbeat, &invalidation_derive_activity)
                .await;
        }

        if bootstrap_completed {
            if !projection_derivation_started {
                invalidation_derive_loop::spawn(
                    &subtasks,
                    pool.clone(),
                    poll_interval_secs,
                    derive_heartbeat
                        .take()
                        .expect("invalidation derive heartbeat is consumed exactly once"),
                    invalidation_derive_activity.clone(),
                )?;
                projection_derivation_started = true;
            }

            let hydration_schedule = bootstrap_hydration_schedule(
                bootstrap_text_hydration_completed,
                primary_hydration_started,
            );

            if hydration_schedule.start_primary_hydration {
                if let Some(config) = primary_hydration_loop::background_primary_hydration_config(
                    &primary_hydration_config,
                    primary_hydration_started,
                ) {
                    primary_hydration_loop::spawn(
                        &subtasks,
                        pool.clone(),
                        Arc::clone(&loop_heartbeat),
                        poll_interval_secs,
                        config,
                        Arc::clone(&projection_apply_generation),
                        Arc::clone(&projection_apply_hydration_lock),
                        invalidation_derive_activity.clone(),
                    )?;
                }
                primary_hydration_started = true;
            }

            if hydration_schedule.run_text_hydration {
                let hydration_result = {
                    let _required_subtask_exclusion = invalidation_derive_activity
                        .exclude_required_subtask()
                        .await;
                    let mut loop_heartbeat = loop_heartbeat.lock().await;
                    hydrate_record_inventory_text_values_after_bootstrap(
                        &pool,
                        text_hydration_config.as_ref(),
                        &mut loop_heartbeat,
                    )
                    .await
                };
                match hydration_result {
                    Ok(()) => {
                        bootstrap_text_hydration_completed = true;
                        progressed = true;
                    }
                    Err(error) => {
                        warn!(
                            service = "worker",
                            projection = "record_inventory_current",
                            error = %format!("{error:#}"),
                            "automatic record_inventory_current text hydration failed"
                        );
                    }
                }
                record_loop_heartbeat_if_due(&pool, &loop_heartbeat, &invalidation_derive_activity)
                    .await;
            }

            let _required_subtask_exclusion = invalidation_derive_activity
                .exclude_required_subtask()
                .await;
            let _apply_hydration_guard = projection_apply_hydration_lock.lock().await;
            let apply_result = {
                let mut loop_heartbeat = loop_heartbeat.lock().await;
                projection_apply::run_once(
                    &pool,
                    text_hydration_config.as_ref(),
                    &mut loop_heartbeat,
                )
                .await
            };
            match apply_result {
                Ok(summary) => {
                    let apply_progressed = summary.made_progress();
                    if apply_progressed {
                        projection_apply_generation.fetch_add(1, Ordering::AcqRel);
                    }
                    progressed |= apply_progressed;
                }
                Err(error) => {
                    warn!(
                        service = "worker",
                        projection_apply = true,
                        error = %format!("{error:#}"),
                        "continuous projection apply iteration failed"
                    );
                }
            }
            drop(_apply_hydration_guard);
            drop(_required_subtask_exclusion);
            record_loop_heartbeat_if_due(&pool, &loop_heartbeat, &invalidation_derive_activity)
                .await;
        }

        if !progressed {
            sleep(poll_interval).await;
        }
    }
}

async fn record_loop_heartbeat_if_due(
    pool: &PgPool,
    loop_heartbeat: &SharedLoopHeartbeat,
    required_subtask_activity: &heartbeat::RequiredSubtaskActivity,
) {
    let _required_subtask_exclusion = required_subtask_activity.exclude_required_subtask().await;
    loop_heartbeat.lock().await.record_if_due(pool).await;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BootstrapHydrationSchedule {
    start_primary_hydration: bool,
    run_text_hydration: bool,
}

fn bootstrap_hydration_schedule(
    bootstrap_text_hydration_completed: bool,
    primary_hydration_started: bool,
) -> BootstrapHydrationSchedule {
    BootstrapHydrationSchedule {
        start_primary_hydration: !primary_hydration_started,
        run_text_hydration: !bootstrap_text_hydration_completed,
    }
}

async fn hydrate_record_inventory_text_values_after_bootstrap(
    pool: &PgPool,
    text_hydration_config: Option<&record_inventory::RecordInventoryTextHydrationConfig>,
    loop_heartbeat: &mut heartbeat::LoopHeartbeat,
) -> Result<()> {
    let Some(config) = text_hydration_config else {
        return Ok(());
    };
    let summary = record_inventory::hydrate_record_inventory_text_values_with_heartbeat(
        pool,
        None,
        config.clone(),
        loop_heartbeat,
    )
    .await?;
    record_inventory::log_text_hydration_summary(None, &summary);
    Ok(())
}

async fn projection_bootstrap_already_handed_off_to_apply(pool: &PgPool) -> Result<bool> {
    let handed_off = projection_bootstrap_handoff_is_current(pool).await?;
    if handed_off {
        for projection in replay::ALL_CURRENT_PROJECTION_ORDER {
            replay::staging::cleanup_projection_checkpoint(pool, projection).await?;
        }
        bootstrap_attempt::clear_projection_replay_attempt(pool).await?;
    }
    Ok(handed_off)
}

async fn projection_bootstrap_handoff_is_current(pool: &PgPool) -> Result<bool> {
    let cursor_exists = projection_apply::normalized_event_cursor_exists(pool).await?;
    if !cursor_exists {
        return Ok(false);
    }

    let complete_marker_count = load_current_projection_replay_marker_count(pool, None).await?;
    let handed_off =
        should_skip_bootstrap_for_existing_apply_cursor(cursor_exists, complete_marker_count);
    Ok(handed_off)
}

fn projection_bootstrap_apply_cursor_seed(
    cursor_exists: bool,
    apply_baseline_change_id: i64,
) -> Option<projection_apply::NormalizedEventChangeCursor> {
    if cursor_exists {
        return None;
    }
    Some(projection_apply::NormalizedEventChangeCursor {
        change_id: apply_baseline_change_id,
    })
}

fn should_skip_bootstrap_for_existing_apply_cursor(
    cursor_exists: bool,
    complete_marker_count: i64,
) -> bool {
    cursor_exists && complete_marker_count == replay::ALL_CURRENT_PROJECTION_ORDER.len() as i64
}

fn projection_bootstrap_replay_target_block(
    normalized_replay_target_block: Option<i64>,
    chain_checkpoint_max_block: Option<i64>,
) -> Option<i64> {
    match (normalized_replay_target_block, chain_checkpoint_max_block) {
        (Some(replay), Some(checkpoint)) => Some(replay.max(checkpoint)),
        (Some(replay), None) => Some(replay),
        (None, Some(checkpoint)) => Some(checkpoint),
        (None, None) => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectionReplayReadiness {
    normalized_replay_cursor_count: i64,
    incomplete_normalized_replay_cursor_count: i64,
    failed_normalized_replay_cursor_count: i64,
    active_index_build_count: i64,
    missing_projection_index_count: i64,
    normalized_replay_max_target_block: Option<i64>,
}

impl ProjectionReplayReadiness {
    fn is_ready(&self) -> bool {
        self.normalized_replay_cursor_count > 0
            && self.incomplete_normalized_replay_cursor_count == 0
            && self.failed_normalized_replay_cursor_count == 0
            && self.active_index_build_count == 0
            && self.missing_projection_index_count == 0
    }
}

async fn load_projection_replay_readiness(pool: &PgPool) -> Result<ProjectionReplayReadiness> {
    let cursor_status = sqlx::query_as::<_, (i64, i64, i64, Option<i64>)>(
        r#"
        SELECT
            COUNT(*)::bigint AS cursor_count,
            COUNT(*) FILTER (
                WHERE next_block_number <= target_block_number
            )::bigint AS incomplete_cursor_count,
            COUNT(*) FILTER (
                WHERE last_failure_reason IS NOT NULL
            )::bigint AS failed_cursor_count,
            MAX(target_block_number) AS max_target_block
        FROM normalized_replay_cursors
        WHERE cursor_kind = $1
        "#,
    )
    .bind(CURSOR_KIND_RAW_FACT_NORMALIZED_EVENTS)
    .fetch_one(pool)
    .await
    .context("failed to inspect normalized replay cursor readiness")?;

    let active_index_build_count = sqlx::query_scalar::<_, i64>(ACTIVE_INDEX_BUILDS_QUERY)
        .fetch_one(pool)
        .await
        .context("failed to inspect active PostgreSQL index builds")?;

    let missing_projection_index_count = missing_projection_index_count(pool).await?;

    Ok(ProjectionReplayReadiness {
        normalized_replay_cursor_count: cursor_status.0,
        incomplete_normalized_replay_cursor_count: cursor_status.1,
        failed_normalized_replay_cursor_count: cursor_status.2,
        active_index_build_count,
        missing_projection_index_count,
        normalized_replay_max_target_block: cursor_status.3,
    })
}

async fn missing_projection_index_count(pool: &PgPool) -> Result<i64> {
    count_unready_normalized_event_indexes(pool, DEFERRED_NORMALIZED_EVENT_INDEXES)
        .await
        .context("failed to inspect deferred normalized-event projection indexes")
}

async fn load_current_projection_replay_marker_count(
    pool: &PgPool,
    replay_target_block: Option<i64>,
) -> Result<i64> {
    let projections = replay::ALL_CURRENT_PROJECTION_ORDER.to_vec();

    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT projection)::BIGINT
        FROM current_projection_replay_status AS status
        JOIN current_projection_full_replay_input_revision AS input_revision
          ON input_revision.singleton
         AND input_revision.revision = status.full_replay_input_revision
        WHERE status.replay_version = $1
          AND status.projection = ANY($2::TEXT[])
          AND (
              $3::BIGINT IS NULL
              OR status.completed_normalized_target_block >= $3
          )
        "#,
    )
    .bind(replay::CURRENT_PROJECTION_REPLAY_VERSION)
    .bind(&projections)
    .bind(replay_target_block)
    .fetch_one(pool)
    .await
    .context("failed to inspect current projection replay markers")
}

async fn try_acquire_replay_lock(pool: &PgPool) -> Result<Option<PoolConnection<Postgres>>> {
    let mut connection = pool
        .acquire()
        .await
        .context("failed to acquire all-current replay lock connection")?;
    let acquired = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
        .bind(ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY)
        .fetch_one(&mut *connection)
        .await
        .context("failed to acquire all-current replay advisory lock")?;

    Ok(acquired.then_some(connection))
}

async fn release_replay_lock(connection: &mut PoolConnection<Postgres>) -> Result<()> {
    let released = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(ALL_CURRENT_PROJECTIONS_REPLAY_LOCK_KEY)
        .fetch_one(&mut **connection)
        .await
        .context("failed to release all-current replay advisory lock")?;
    if !released {
        warn!(
            service = "worker",
            replay = "all_current_projections",
            "all-current projection replay advisory lock was already released"
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "automatic_projection_replay/tests.rs"]
mod tests;
