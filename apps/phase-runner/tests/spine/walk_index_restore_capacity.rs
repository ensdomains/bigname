//! The rebuild waits on the batch loop's storage capacity check, and every statement on its
//! session gives way to a stop.

use super::*;

/// The first two indexes `drop.sql` drops, which the rebuild builds in this order.
fn first_two() -> (&'static str, &'static str) {
    (DROP_LIST[0].name, DROP_LIST[1].name)
}

/// Reports a full disk to the rebuild's checks: while the chain's Interpret is complete,
/// `missing` is not a valid, ready index and `present`, when set, exists, until `released` is
/// set. Batch checks
/// before Interpret completed or after the rebuild see room. With `start_project`, the first
/// full report also starts the chain's Project row, as a restart-loop retry finds it.
struct RebuildDisk {
    chain_id: &'static str,
    missing: &'static str,
    present: Option<&'static str>,
    start_project: bool,
    /// With `start_project`, also pauses the row it started, as a retry after a batch-loop
    /// pause finds it.
    pause_project: bool,
    released: AtomicBool,
    full_reports: AtomicUsize,
}

impl RebuildDisk {
    fn new(chain_id: &'static str, missing: &'static str) -> Self {
        Self {
            chain_id,
            missing,
            present: None,
            start_project: false,
            pause_project: false,
            released: AtomicBool::new(false),
            full_reports: AtomicUsize::new(0),
        }
    }

    async fn full(&self, pool: &sqlx::PgPool) -> RunnerResult<bool> {
        if self.released.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let full: bool = sqlx::query_scalar(
            "SELECT NOT EXISTS (SELECT 1 FROM pg_index
                                WHERE indexrelid = to_regclass('bigname_phase.' || $2)
                                  AND indisvalid AND indisready)
                AND ($3::text IS NULL OR to_regclass('bigname_phase.' || $3) IS NOT NULL)
                AND EXISTS (SELECT 1 FROM chain_phase_state
                            WHERE chain_id = $1 AND phase_name = 'interpret'
                              AND phase_status = 'completed')",
        )
        .bind(self.chain_id)
        .bind(self.missing)
        .bind(self.present)
        .fetch_one(pool)
        .await
        .map_err(|error| RunnerError::transient(format!("test capacity probe: {error}")))?;
        if full && self.full_reports.fetch_add(1, Ordering::SeqCst) == 0 && self.start_project {
            let store = PhaseStore::new(pool.clone());
            store
                .start_phase(self.chain_id, PhaseName::Project, &RunMode::Normal)
                .await?;
            if self.pause_project {
                store.pause_phase(self.chain_id, PhaseName::Project).await?;
            }
        }
        Ok(full)
    }
}

impl CapacityProbe for RebuildDisk {
    fn measure<'a>(
        &'a self,
        pool: &'a sqlx::PgPool,
        _writable_path: &'a std::path::Path,
    ) -> CapacityFuture<'a> {
        Box::pin(async move {
            Ok(CapacityMeasurement {
                database_size_bytes: 0,
                free_disk_bytes: if self.full(pool).await? { 0 } else { u64::MAX },
            })
        })
    }
}

fn guard(disk: &Arc<RebuildDisk>) -> CapacityGuard {
    CapacityGuard::new(
        CapacityConfig {
            minimum_free_disk_bytes: 1,
            poll_interval: Duration::from_millis(50),
            ..CapacityConfig::default()
        },
        Arc::clone(disk) as Arc<dyn CapacityProbe>,
    )
}

/// Waits until the probe has reported a full disk `reports` times.
async fn await_full_reports(disk: &RebuildDisk, reports: usize) -> Result<()> {
    tokio::time::timeout(BUILD_WAIT, async {
        while disk.full_reports.load(Ordering::SeqCst) < reports {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("the rebuild never found the disk full")
}

const CAPACITY_WARN: &str = "waits until storage capacity recovers";

/// Whether the rebuild lock is free, taken and released on a connection of its own.
async fn restore_lock_free(scratch: &ScratchDatabase) -> Result<bool> {
    let mut probe = PgConnection::connect_with(&scratch.writer_connect_options()).await?;
    let free: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_lock(hashtextextended('bigname.walk_index_set', 0))",
    )
    .fetch_one(&mut probe)
    .await?;
    probe.close().await?;
    Ok(free)
}

#[tokio::test]
async fn a_breach_before_the_first_build_waits_without_writing_the_phase_row() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_capacity_first").await?;
    let chain_id = "walk-index-capacity-first";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let disk = Arc::new(RebuildDisk::new(chain_id, DROPPED));
    let heartbeat = RunnerLoopHeartbeat::default();
    let runner = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        guard(&disk),
        "walk-index-capacity-first-runner",
    )?
    .with_loop_heartbeat(heartbeat.clone());
    let task = tokio::spawn(logged(async move {
        project_redo(&runner, chain_id, CancellationToken::new()).await
    }));

    await_full_reports(&disk, 3).await?;
    let age_before = heartbeat.age_seconds(chain_id);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!task.is_finished(), "the rebuild must wait for capacity");
    assert_eq!(
        oid(scratch.pool(), DROPPED).await?,
        None,
        "nothing is built"
    );
    assert!(heartbeat.walk_index_capacity_blocked(chain_id));
    // A completed row is not paused, and the wait records no loop progress, so the stale-loop
    // alert names a long one.
    assert_eq!(project_row(scratch.pool(), chain_id).await?.0, "completed");
    let age_after = heartbeat.age_seconds(chain_id);
    let progressed = match (age_before, age_after) {
        (_, None) => false,
        (None, Some(_)) => true,
        (Some(before), Some(after)) => after < before + 2,
    };
    assert!(
        !progressed,
        "no loop progress while blocked: {age_before:?} then {age_after:?}"
    );

    disk.released.store(true, Ordering::SeqCst);
    let (result, logs) = tokio::time::timeout(BUILD_WAIT, task).await??;
    result?;
    assert!(usable(scratch.pool(), DROPPED).await?);
    assert!(!heartbeat.walk_index_capacity_blocked(chain_id));
    let warnings: Vec<&str> = logs
        .lines()
        .filter(|line| line.contains("WARN") && line.contains(CAPACITY_WARN))
        .collect();
    assert!(
        warnings.len() >= 2,
        "the wait retries and warns each time: {logs}"
    );
    for field in [
        "breach_reasons=",
        "database_size_bytes=0",
        "free_disk_bytes=0",
    ] {
        assert!(warnings[0].contains(field), "{field} in {}", warnings[0]);
    }
    assert!(
        !logs.contains("illegal phase transition"),
        "no state error: {logs}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn a_breach_between_two_builds_holds_the_second() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_capacity_between").await?;
    let chain_id = "walk-index-capacity-between";
    completed_chain(&scratch, chain_id).await?;
    let (first, second) = first_two();
    drop_index(scratch.pool(), first).await?;
    drop_index(scratch.pool(), second).await?;
    let disk = Arc::new(RebuildDisk {
        present: Some(first),
        ..RebuildDisk::new(chain_id, second)
    });
    let heartbeat = RunnerLoopHeartbeat::default();
    let runner = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        guard(&disk),
        "walk-index-capacity-between-runner",
    )?
    .with_loop_heartbeat(heartbeat.clone());
    let task =
        tokio::spawn(
            async move { project_redo(&runner, chain_id, CancellationToken::new()).await },
        );

    await_full_reports(&disk, 2).await?;
    assert!(
        !task.is_finished(),
        "the second build must wait for capacity"
    );
    assert!(usable(scratch.pool(), first).await?, "the first is built");
    assert_eq!(oid(scratch.pool(), second).await?, None, "the second waits");
    assert!(heartbeat.walk_index_capacity_blocked(chain_id));

    disk.released.store(true, Ordering::SeqCst);
    tokio::time::timeout(BUILD_WAIT, task).await???;
    assert!(usable(scratch.pool(), first).await?);
    assert!(usable(scratch.pool(), second).await?);
    assert!(!heartbeat.walk_index_capacity_blocked(chain_id));
    scratch.cleanup().await
}

#[tokio::test]
async fn a_stop_while_waiting_for_capacity_exits_without_building() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_capacity_stop").await?;
    let chain_id = "walk-index-capacity-stop";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let disk = Arc::new(RebuildDisk::new(chain_id, DROPPED));
    let heartbeat = RunnerLoopHeartbeat::default();
    let runner = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        guard(&disk),
        "walk-index-capacity-stop-runner",
    )?
    .with_loop_heartbeat(heartbeat.clone());
    let cancellation = CancellationToken::new();
    let stop = cancellation.clone();
    let task = tokio::spawn(async move { project_redo(&runner, chain_id, stop).await });
    await_full_reports(&disk, 2).await?;
    assert!(heartbeat.walk_index_capacity_blocked(chain_id));

    cancellation.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .context("the wait must give way to the stop")??;
    assert!(
        stopped.is_err(),
        "a stopped Project redo reports itself incomplete"
    );
    assert_eq!(
        oid(scratch.pool(), DROPPED).await?,
        None,
        "no build was issued"
    );
    assert!(!heartbeat.walk_index_capacity_blocked(chain_id));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !restore_lock_free(&scratch).await? {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::Ok(())
    })
    .await
    .context("the stop must release the rebuild lock")??;
    scratch.cleanup().await
}

#[tokio::test]
async fn a_running_project_row_is_paused_while_the_rebuild_waits() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_capacity_running").await?;
    let chain_id = "walk-index-capacity-running";
    seed_identified_lineage(scratch.pool(), chain_id, 3).await?;
    let heads = HeadMarkers {
        latest: BlockMarker::new(3, format!("{chain_id}-block-3"))?,
        safe: Some(BlockMarker::new(2, format!("{chain_id}-block-2"))?),
        finalized: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
    };
    drop_index(scratch.pool(), DROPPED).await?;
    let disk = Arc::new(RebuildDisk {
        start_project: true,
        ..RebuildDisk::new(chain_id, DROPPED)
    });
    let runner = runner(
        scratch.runner(),
        complete_phase_set(Some(heads)),
        guard(&disk),
        "walk-index-capacity-running-runner",
    )?;
    let mut chain = chain(chain_id)?;
    chain.verify_before_live = true;
    let task = tokio::spawn(logged(async move {
        runner.run_chain(&chain, CancellationToken::new()).await
    }));

    await_full_reports(&disk, 2).await?;
    tokio::time::timeout(BUILD_WAIT, async {
        while project_row(scratch.pool(), chain_id).await?.0 != "paused" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::Ok(())
    })
    .await
    .context("the running Project row must be paused while the rebuild waits")??;
    assert_eq!(oid(scratch.pool(), DROPPED).await?, None);

    // A writer holds the build once it starts, while the restore still runs, so the row is read
    // between the wait and Project's own start.
    let mut writer = open_writer(&scratch).await?;
    disk.released.store(true, Ordering::SeqCst);
    await_build(scratch.pool()).await?;
    assert_eq!(
        project_row(scratch.pool(), chain_id).await?.0,
        "running",
        "the wait resumes the row it paused once capacity recovers"
    );
    sqlx::raw_sql("ROLLBACK").execute(&mut writer).await?;
    writer.close().await?;
    let (result, logs) = tokio::time::timeout(BUILD_WAIT, task).await??;
    result?;
    assert!(usable(scratch.pool(), DROPPED).await?);
    assert!(
        !logs.contains("illegal phase transition"),
        "no state error: {logs}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn a_stop_while_a_rebuild_read_waits_on_a_lock_returns_and_releases_the_rebuild_lock()
-> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_blocked_read").await?;
    let chain_id = "walk-index-blocked-read";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    // Holds the rebuild in its wait for other builds, so the phase-row lock below is taken
    // while the rebuild is under way.
    let mut builder = PgConnection::connect_with(&scratch.writer_connect_options()).await?;
    sqlx::raw_sql("BEGIN; LOCK TABLE normalized_events IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut builder)
        .await?;
    let runner = loopback_runner(&scratch, "walk-index-blocked-read-runner")?;
    let cancellation = CancellationToken::new();
    let stop = cancellation.clone();
    let task = tokio::spawn(logged(async move {
        project_redo(&runner, chain_id, stop).await
    }));
    tokio::time::timeout(BUILD_WAIT, async {
        while restore_lock_free(&scratch).await? {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::Ok(())
    })
    .await
    .context("the rebuild never took its lock")??;

    let mut holder = PgConnection::connect_with(&scratch.writer_connect_options()).await?;
    sqlx::raw_sql("BEGIN; LOCK TABLE chain_phase_state IN ACCESS EXCLUSIVE MODE")
        .execute(&mut holder)
        .await?;
    let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut holder)
        .await?;
    sqlx::raw_sql("ROLLBACK").execute(&mut builder).await?;
    builder.close().await?;
    // The rebuild's read of the other chains' Interpret rows now waits on the holder.
    tokio::time::timeout(BUILD_WAIT, async {
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity
                 WHERE $1 = ANY (pg_blocking_pids(pid))
                   AND query LIKE '%WHEN redo_in_progress THEN%'",
            )
            .bind(holder_pid)
            .fetch_one(scratch.pool())
            .await?;
            if waiting > 0 {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("no rebuild read waited on the phase-row lock")??;

    cancellation.cancel();
    // The rebuild returns, and its session with the lock goes, while the holder still holds the
    // phase rows. The redo's own read of its row after the stop then waits out the stop budget.
    tokio::time::timeout(Duration::from_secs(3), async {
        while !restore_lock_free(&scratch).await? {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::Ok(())
    })
    .await
    .context("the stop must end the rebuild and release its lock at once")??;
    let (stopped, _) = tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .context("the redo must return within the stop budget")??;
    assert!(
        stopped.is_err(),
        "a stopped Project redo reports itself incomplete"
    );
    assert_eq!(
        oid(scratch.pool(), DROPPED).await?,
        None,
        "nothing is left invalid"
    );
    sqlx::raw_sql("ROLLBACK").execute(&mut holder).await?;
    holder.close().await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn an_invalid_leftover_is_dropped_before_the_wait_for_capacity() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_capacity_invalid").await?;
    let chain_id = "walk-index-capacity-invalid";
    completed_chain(&scratch, chain_id).await?;
    sqlx::raw_sql(&format!(
        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{DROPPED}'::regclass"
    ))
    .execute(scratch.pool())
    .await?;
    let leftover = oid(scratch.pool(), DROPPED).await?;
    assert!(leftover.is_some());
    let disk = Arc::new(RebuildDisk::new(chain_id, DROPPED));
    let runner = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        guard(&disk),
        "walk-index-capacity-invalid-runner",
    )?;
    let task =
        tokio::spawn(
            async move { project_redo(&runner, chain_id, CancellationToken::new()).await },
        );

    await_full_reports(&disk, 2).await?;
    assert!(!task.is_finished(), "the build must wait for capacity");
    assert_eq!(
        oid(scratch.pool(), DROPPED).await?,
        None,
        "the leftover, and the space it holds, is dropped before the wait"
    );

    disk.released.store(true, Ordering::SeqCst);
    tokio::time::timeout(BUILD_WAIT, task).await???;
    assert!(usable(scratch.pool(), DROPPED).await?);
    assert_ne!(oid(scratch.pool(), DROPPED).await?, leftover);
    scratch.cleanup().await
}

/// The backend holding the rebuild lock in this test's database, the rebuild's own session.
/// `pg_locks` lists every database's locks, and other tests' rebuilds take the same key.
async fn restore_backend(pool: &sqlx::PgPool) -> Result<Option<i32>> {
    Ok(sqlx::query_scalar(
        "SELECT held.pid FROM pg_locks held,
                (SELECT hashtextextended('bigname.walk_index_set', 0) AS key) lock
         WHERE held.locktype = 'advisory' AND held.granted AND held.objsubid = 1
           AND held.database = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND held.classid = ((lock.key >> 32) & 4294967295)::oid
           AND held.objid = (lock.key & 4294967295)::oid",
    )
    .fetch_optional(pool)
    .await?)
}

#[tokio::test]
async fn a_session_lost_while_waiting_for_capacity_is_retried_not_failed() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_capacity_lost").await?;
    let chain_id = "walk-index-capacity-lost";
    completed_chain(&scratch, chain_id).await?;
    drop_index(scratch.pool(), DROPPED).await?;
    let disk = Arc::new(RebuildDisk::new(chain_id, DROPPED));
    let runner = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        guard(&disk),
        "walk-index-capacity-lost-runner",
    )?;
    let task = tokio::spawn(logged(async move {
        project_redo(&runner, chain_id, CancellationToken::new()).await
    }));
    await_full_reports(&disk, 2).await?;
    let first = restore_backend(scratch.pool())
        .await?
        .context("the waiting rebuild holds its lock")?;

    // An idle cutoff, or anything else that ends the session while it waits.
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(first)
        .execute(scratch.pool())
        .await?;
    tokio::time::timeout(BUILD_WAIT, async {
        while restore_backend(scratch.pool())
            .await?
            .is_none_or(|backend| backend == first)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::Ok(())
    })
    .await
    .context("the restart loop must open a new session and wait again")??;
    assert_eq!(oid(scratch.pool(), DROPPED).await?, None);

    disk.released.store(true, Ordering::SeqCst);
    let (result, logs) = tokio::time::timeout(BUILD_WAIT, task).await??;
    result?;
    assert!(usable(scratch.pool(), DROPPED).await?);
    assert!(
        logs.contains("the index rebuild connection failed while it waited for storage capacity"),
        "{logs}"
    );
    assert!(
        !logs.contains("already failed in this process"),
        "a lost session is not a failed build: {logs}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn a_paused_project_row_records_progress_and_is_left_paused() -> Result<()> {
    let scratch = ScratchDatabase::create("walk_index_restore_capacity_paused").await?;
    let chain_id = "walk-index-capacity-paused";
    seed_identified_lineage(scratch.pool(), chain_id, 3).await?;
    let heads = HeadMarkers {
        latest: BlockMarker::new(3, format!("{chain_id}-block-3"))?,
        safe: Some(BlockMarker::new(2, format!("{chain_id}-block-2"))?),
        finalized: Some(BlockMarker::new(1, format!("{chain_id}-block-1"))?),
    };
    drop_index(scratch.pool(), DROPPED).await?;
    let disk = Arc::new(RebuildDisk {
        start_project: true,
        pause_project: true,
        ..RebuildDisk::new(chain_id, DROPPED)
    });
    let heartbeat = RunnerLoopHeartbeat::default();
    let runner = runner(
        scratch.runner(),
        complete_phase_set(Some(heads)),
        guard(&disk),
        "walk-index-capacity-paused-runner",
    )?
    .with_loop_heartbeat(heartbeat.clone());
    let mut chain = chain(chain_id)?;
    chain.verify_before_live = true;
    let task = tokio::spawn(logged(async move {
        runner.run_chain(&chain, CancellationToken::new()).await
    }));

    await_full_reports(&disk, 2).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!task.is_finished(), "the rebuild must wait for capacity");
    assert_eq!(project_row(scratch.pool(), chain_id).await?.0, "paused");
    let age = heartbeat.age_seconds(chain_id);
    assert!(
        age.is_some_and(|age| age <= 1),
        "a paused row's wait records loop progress, last {age:?} s ago"
    );

    // The wait resumes only a row it paused, so the row is still paused at the build, and
    // Project's own start resumes it.
    let mut writer = open_writer(&scratch).await?;
    disk.released.store(true, Ordering::SeqCst);
    await_build(scratch.pool()).await?;
    assert_eq!(project_row(scratch.pool(), chain_id).await?.0, "paused");
    sqlx::raw_sql("ROLLBACK").execute(&mut writer).await?;
    writer.close().await?;
    let (result, logs) = tokio::time::timeout(BUILD_WAIT, task).await??;
    result?;
    assert!(usable(scratch.pool(), DROPPED).await?);
    assert!(
        !logs.contains("illegal phase transition"),
        "no state error: {logs}"
    );
    scratch.cleanup().await
}
