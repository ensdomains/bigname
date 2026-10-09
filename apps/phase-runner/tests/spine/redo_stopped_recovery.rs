//! An operator redo after the supervisor stopped while its Live phase was
//! running. Live holds its advisory lock for as long as it is `running` in a
//! live process, so a free lock is the evidence that the row is stale.
//!
//! Each row is one ordering of a supervisor stop and a redo, with its test.
//!
//! | Row | Ordering | Expected | Test |
//! | --- | --- | --- | --- |
//! | a | Stop between Live batches, then each redo selection | Redo runs, Live `completed` | `redo_runs_after_a_stop_between_live_batches` |
//! | b | Stop raised during a Live batch, then redo | Redo runs, Live `completed` | `redo_runs_after_a_stop_during_a_live_batch` |
//! | c | Process killed during a Live batch, then redo | Redo runs, Live `completed` | `redo_runs_after_the_supervisor_was_killed_during_live` |
//! | c2 | Stop left Live `paused`, lock free | Redo runs, Live `completed` | `redo_runs_after_a_stop_left_live_paused` |
//! | d | Supervisor inside a Live batch, each redo selection | `LockHeld`, Live stays `running` | `redo_is_refused_while_a_supervisor_is_running_live` |
//! | d2 | Verify-only redo beside a running Live | Redo runs, Live stays `running` | `verify_redo_still_runs_beside_a_supervisor_running_live` |
//! | d3 | Supervisor paused for capacity inside Live | `LockHeld`, Live stays `paused` | `redo_is_refused_while_a_supervisor_is_paused_in_live` |
//! | e | Redo after a stop that left Live `completed` | Unchanged | the redo tests in `spine.rs` |
//! | e2 | Redo refused for its range | `DataIntegrity`, Live untouched | `a_redo_refused_for_its_range_leaves_stopped_live_as_it_was` |
//! | f | Second redo while the first holds its phase | Second gets `LockHeld` | `a_second_redo_is_refused_while_the_first_holds_the_phase` |
//! | f2 | Two redos started together | One or both finish, a loser gets `LockHeld` | `two_redos_started_together_leave_one_finished_redo` |
//! | g | Supervisor restarted after the redo | Live runs again | `the_supervisor_restarts_after_a_redo_that_settled_live` |
//! | h | Live `completed`, another process holds the Live lock | Redo runs without the lock | `redo_leaves_the_live_lock_alone_when_live_is_not_recorded_running` |

use super::*;

const TIP: i64 = 2;

/// Where the stop lands in the supervisor's Live phase.
#[derive(Clone, Copy, Debug)]
pub(super) enum Stop {
    /// A stop request observed between two Live batches.
    BetweenBatches,
    /// A stop request raised while a Live batch is in flight. The batch finishes first.
    MidBatch,
    /// The process dies inside a Live batch and runs no cleanup.
    Killed,
}

/// A Live batch that always reports more work, as Live does behind the chain head.
/// With `hold` set, the batch waits for it before returning.
struct UnfinishedLive {
    entered: Arc<Notify>,
    hold: Option<Arc<Notify>>,
}

impl Phase for UnfinishedLive {
    fn name(&self) -> PhaseName {
        PhaseName::Live
    }

    fn run_batch(&self, _context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            self.entered.notify_one();
            if let Some(hold) = &self.hold {
                hold.notified().await;
            }
            Ok(PhaseBatchOutcome::Continue(PhaseProgress::default()))
        })
    }
}

fn tip_heads(chain_id: &str) -> Result<HeadMarkers> {
    Ok(HeadMarkers {
        latest: BlockMarker::new(TIP, format!("{chain_id}-block-{TIP}"))?,
        safe: None,
        finalized: None,
    })
}

fn supervisor_phases(chain_id: &str, live: Arc<dyn Phase>) -> Result<PhaseSet> {
    let heads = tip_heads(chain_id)?;
    Ok(PhaseSet::new(PhaseName::ALL.map(|name| {
        if name == PhaseName::Live {
            Arc::clone(&live)
        } else {
            complete_phase(name, Some(heads.clone()))
        }
    }))?)
}

fn supervised_chain(chain_id: &str) -> Result<ChainConfig> {
    let mut chain = chain(chain_id)?;
    chain.verify_before_live = true;
    Ok(chain)
}

/// A supervisor that has reached Live and is inside a Live batch.
pub(super) struct RunningSupervisor {
    pub(super) task: tokio::task::JoinHandle<RunnerResult<()>>,
    pub(super) cancellation: CancellationToken,
    entered: Arc<Notify>,
    pub(super) hold: Arc<Notify>,
}

pub(super) async fn start_supervisor(
    scratch: &ScratchDatabase,
    chain_id: &str,
    hold_batches: bool,
) -> Result<RunningSupervisor> {
    start_supervisor_with(scratch, chain_id, hold_batches, available_capacity()).await
}

async fn start_supervisor_with(
    scratch: &ScratchDatabase,
    chain_id: &str,
    hold_batches: bool,
    capacity: CapacityGuard,
) -> Result<RunningSupervisor> {
    PhaseStore::new(scratch.pool().clone())
        .ensure_ingest_sources(chain_id, &chain(chain_id)?.sources)
        .await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, TIP).await?;
    let entered = Arc::new(Notify::new());
    let hold = Arc::new(Notify::new());
    let live = Arc::new(UnfinishedLive {
        entered: Arc::clone(&entered),
        hold: hold_batches.then(|| Arc::clone(&hold)),
    });
    let supervisor = runner(
        scratch.runner(),
        supervisor_phases(chain_id, live)?,
        capacity,
        "stopped-live-supervisor",
    )?;
    let cancellation = CancellationToken::new();
    let run_cancellation = cancellation.clone();
    let chain = supervised_chain(chain_id)?;
    let task = tokio::spawn(async move { supervisor.run_chain(&chain, run_cancellation).await });
    entered.notified().await;
    Ok(RunningSupervisor {
        task,
        cancellation,
        entered,
        hold,
    })
}

/// Give the finite phases the recorded extent a redo checks.
/// The supervisor fixture's phases complete without reporting it.
pub(super) async fn seed_redo_extents(scratch: &ScratchDatabase, chain_id: &str) -> Result<()> {
    for phase in [
        PhaseName::Ingest,
        PhaseName::Interpret,
        PhaseName::Project,
        PhaseName::Verify,
    ] {
        set_phase_extent(scratch.pool(), chain_id, phase, TIP).await?;
    }
    Ok(())
}

/// Run a supervisor into Live, stop it there, and return the database it left.
pub(super) async fn stopped_during_live(chain_id: &str, stop: Stop) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(chain_id).await?;
    let supervisor =
        start_supervisor(&scratch, chain_id, !matches!(stop, Stop::BetweenBatches)).await?;
    match stop {
        Stop::BetweenBatches => {
            supervisor.cancellation.cancel();
            supervisor.task.await??;
        }
        Stop::MidBatch => {
            supervisor.cancellation.cancel();
            supervisor.hold.notify_one();
            supervisor.task.await??;
        }
        Stop::Killed => {
            // Dropping the task closes the lock's connection without an unlock or a
            // status write, which is all a killed process leaves behind.
            supervisor.task.abort();
            assert!(
                supervisor
                    .task
                    .await
                    .is_err_and(|error| error.is_cancelled())
            );
            wait_for_free_live_lock(&scratch, chain_id).await?;
        }
    }
    let store = PhaseStore::new(scratch.pool().clone());
    assert_eq!(
        store.status(chain_id, PhaseName::Live).await?,
        PhaseStatus::Running,
        "{stop:?} must leave Live recorded as running"
    );
    seed_redo_extents(&scratch, chain_id).await?;
    Ok(scratch)
}

async fn wait_for_free_live_lock(scratch: &ScratchDatabase, chain_id: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match PhaseLock::acquire(scratch.writer_connect_options(), chain_id, PhaseName::Live)
                .await
            {
                Ok(lock) => return lock.release().await,
                Err(error) if error.kind() == ErrorKind::LockHeld => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await??;
    Ok(())
}

fn recording_phases(calls: &Arc<Mutex<Vec<(String, PhaseName)>>>) -> Result<PhaseSet> {
    Ok(PhaseSet::new(PhaseName::ALL.map(|name| {
        Arc::new(RecordingRedoPhase {
            name,
            calls: Arc::clone(calls),
            fail_chain: None,
        }) as Arc<dyn Phase>
    }))?)
}

async fn redo(
    scratch: &ScratchDatabase,
    chain_id: &str,
    selection: RedoPhase,
    phases: PhaseSet,
) -> RunnerResult<()> {
    runner(
        scratch.runner(),
        phases,
        available_capacity(),
        "stopped-live-redo",
    )?
    .redo(
        &chain(chain_id)?,
        selection,
        BlockRange::new(0, TIP)?,
        CancellationToken::new(),
    )
    .await
}

async fn active_redo_count(scratch: &ScratchDatabase, chain_id: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM chain_phase_state WHERE chain_id=$1 AND redo_in_progress",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?)
}

fn selection_label(selection: RedoPhase) -> String {
    match selection {
        RedoPhase::Phase(phase) => phase.to_string(),
        RedoPhase::All => "all".to_owned(),
        RedoPhase::RecomputeFlags => "recompute".to_owned(),
    }
}

/// The phases a selection must run, in order of first appearance.
fn expected_phases(selection: RedoPhase) -> Vec<PhaseName> {
    match selection {
        RedoPhase::Phase(PhaseName::Interpret) => vec![PhaseName::Interpret, PhaseName::Project],
        RedoPhase::Phase(phase) => vec![phase],
        RedoPhase::RecomputeFlags => vec![PhaseName::Interpret],
        RedoPhase::All => vec![
            PhaseName::Ingest,
            PhaseName::Interpret,
            PhaseName::Project,
            PhaseName::Verify,
        ],
    }
}

async fn assert_redo_settles_stopped_live(stop: Stop, selection: RedoPhase) -> Result<()> {
    let chain_id = format!("stopped-live-{stop:?}-{}", selection_label(selection)).to_lowercase();
    let scratch = stopped_during_live(&chain_id, stop).await?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    redo(&scratch, &chain_id, selection, recording_phases(&calls)?).await?;

    let store = PhaseStore::new(scratch.pool().clone());
    assert_eq!(
        store.status(&chain_id, PhaseName::Live).await?,
        PhaseStatus::Completed
    );
    let mut ran = Vec::new();
    for (_, phase) in calls.lock().unwrap().iter() {
        if !ran.contains(phase) {
            ran.push(*phase);
        }
    }
    assert_eq!(ran, expected_phases(selection), "{selection:?}");
    for phase in ran {
        assert_eq!(
            store.status(&chain_id, phase).await?,
            PhaseStatus::Completed
        );
    }
    // A finished Ingest redo stamps the replay its downstream phases owe. No other
    // selection leaves redo state behind.
    let stamped: Vec<String> = sqlx::query_scalar(
        "SELECT phase_name FROM chain_phase_state
         WHERE chain_id=$1 AND redo_in_progress ORDER BY phase_name",
    )
    .bind(&chain_id)
    .fetch_all(scratch.pool())
    .await?;
    let expected: &[&str] = if selection == RedoPhase::Phase(PhaseName::Ingest) {
        &["interpret", "verify"]
    } else {
        &[]
    };
    assert_eq!(stamped, expected, "{selection:?}");
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_runs_after_a_stop_between_live_batches() -> Result<()> {
    for selection in [
        RedoPhase::Phase(PhaseName::Project),
        RedoPhase::Phase(PhaseName::Interpret),
        RedoPhase::Phase(PhaseName::Ingest),
        RedoPhase::RecomputeFlags,
        RedoPhase::All,
    ] {
        assert_redo_settles_stopped_live(Stop::BetweenBatches, selection).await?;
    }
    Ok(())
}

#[tokio::test]
async fn redo_runs_after_a_stop_during_a_live_batch() -> Result<()> {
    assert_redo_settles_stopped_live(Stop::MidBatch, RedoPhase::Phase(PhaseName::Project)).await
}

#[tokio::test]
async fn redo_runs_after_the_supervisor_was_killed_during_live() -> Result<()> {
    for selection in [RedoPhase::Phase(PhaseName::Project), RedoPhase::All] {
        assert_redo_settles_stopped_live(Stop::Killed, selection).await?;
    }
    Ok(())
}

#[tokio::test]
async fn redo_runs_after_a_stop_left_live_paused() -> Result<()> {
    let chain_id = "stopped-live-paused";
    let scratch = stopped_during_live(chain_id, Stop::BetweenBatches).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    // A stop inside the capacity wait leaves the phase paused instead of running.
    store.pause_phase(chain_id, PhaseName::Live).await?;

    redo(
        &scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Project),
        PhaseSet::loopback(),
    )
    .await?;
    assert_eq!(
        store.status(chain_id, PhaseName::Live).await?,
        PhaseStatus::Completed
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_is_refused_while_a_supervisor_is_running_live() -> Result<()> {
    for selection in [
        RedoPhase::Phase(PhaseName::Project),
        RedoPhase::Phase(PhaseName::Interpret),
        RedoPhase::Phase(PhaseName::Ingest),
        RedoPhase::RecomputeFlags,
        RedoPhase::All,
    ] {
        let chain_id = format!("running-live-{}", selection_label(selection));
        let scratch = ScratchDatabase::create(&chain_id).await?;
        let supervisor = start_supervisor(&scratch, &chain_id, true).await?;
        seed_redo_extents(&scratch, &chain_id).await?;

        let error = redo(&scratch, &chain_id, selection, PhaseSet::loopback())
            .await
            .expect_err("a redo must not run beside a supervisor that is running Live");
        assert_eq!(error.kind(), ErrorKind::LockHeld, "{selection:?}: {error}");
        let message = error.to_string();
        assert!(message.contains("phase live"), "{message}");
        let store = PhaseStore::new(scratch.pool().clone());
        assert_eq!(
            store.status(&chain_id, PhaseName::Live).await?,
            PhaseStatus::Running
        );
        assert_eq!(active_redo_count(&scratch, &chain_id).await?, 0);

        // The refused redo left the supervisor able to carry on and stop cleanly.
        supervisor.hold.notify_one();
        supervisor.entered.notified().await;
        supervisor.cancellation.cancel();
        supervisor.hold.notify_one();
        supervisor.task.await??;
        scratch.cleanup().await?;
    }
    Ok(())
}

/// Reports a full disk while `full` is set.
#[derive(Default)]
struct SwitchedDisk {
    full: AtomicBool,
}

impl CapacityProbe for SwitchedDisk {
    fn measure<'a>(
        &'a self,
        _pool: &'a sqlx::PgPool,
        _writable_path: &'a std::path::Path,
    ) -> CapacityFuture<'a> {
        Box::pin(async move {
            Ok(CapacityMeasurement {
                database_size_bytes: 0,
                free_disk_bytes: if self.full.load(Ordering::SeqCst) {
                    0
                } else {
                    u64::MAX
                },
            })
        })
    }
}

#[tokio::test]
async fn redo_is_refused_while_a_supervisor_is_paused_in_live() -> Result<()> {
    let chain_id = "paused-live-lock-held";
    let scratch = ScratchDatabase::create(chain_id).await?;
    let disk = Arc::new(SwitchedDisk::default());
    let capacity = CapacityGuard::new(
        CapacityConfig {
            minimum_free_disk_bytes: 1,
            poll_interval: Duration::from_millis(1),
            ..CapacityConfig::default()
        },
        disk.clone(),
    );
    let supervisor = start_supervisor_with(&scratch, chain_id, true, capacity).await?;
    seed_redo_extents(&scratch, chain_id).await?;
    // The next Live batch finds the disk full and waits, paused, with its lock held.
    disk.full.store(true, Ordering::SeqCst);
    supervisor.hold.notify_one();
    wait_for_phase_status(scratch.pool(), chain_id, PhaseName::Live, "paused").await?;

    let error = redo(
        &scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Project),
        PhaseSet::loopback(),
    )
    .await
    .expect_err("a redo must not run beside a supervisor paused inside Live");
    assert_eq!(error.kind(), ErrorKind::LockHeld, "{error}");
    assert!(error.to_string().contains("phase live"), "{error}");
    let store = PhaseStore::new(scratch.pool().clone());
    assert_eq!(
        store.status(chain_id, PhaseName::Live).await?,
        PhaseStatus::Paused
    );
    assert_eq!(active_redo_count(&scratch, chain_id).await?, 0);

    // The refused redo left the supervisor able to resume and stop cleanly.
    disk.full.store(false, Ordering::SeqCst);
    supervisor.entered.notified().await;
    supervisor.cancellation.cancel();
    supervisor.hold.notify_one();
    supervisor.task.await??;
    scratch.cleanup().await
}

#[tokio::test]
async fn redo_leaves_the_live_lock_alone_when_live_is_not_recorded_running() -> Result<()> {
    let chain_id = "completed-live-lock-held";
    let scratch = ScratchDatabase::create(chain_id).await?;
    let supervisor = start_supervisor(&scratch, chain_id, false).await?;
    supervisor.cancellation.cancel();
    supervisor.task.await??;
    seed_redo_extents(&scratch, chain_id).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    complete_phase_with_lock(
        &scratch,
        &store,
        chain_id,
        PhaseName::Live,
        &PhaseProgress::default(),
    )
    .await?;

    // Another process holds the Live lock briefly between Live attempts, as a
    // supervisor's start-up recovery does. The redo has no row to settle.
    let held =
        PhaseLock::acquire(scratch.writer_connect_options(), chain_id, PhaseName::Live).await?;
    redo(
        &scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Project),
        PhaseSet::loopback(),
    )
    .await?;
    held.release().await?;
    assert_eq!(
        store.status(chain_id, PhaseName::Live).await?,
        PhaseStatus::Completed
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn verify_redo_still_runs_beside_a_supervisor_running_live() -> Result<()> {
    let chain_id = "running-live-verify";
    let scratch = ScratchDatabase::create(chain_id).await?;
    let supervisor = start_supervisor(&scratch, chain_id, true).await?;
    seed_redo_extents(&scratch, chain_id).await?;

    redo(
        &scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Verify),
        PhaseSet::loopback(),
    )
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    assert_eq!(
        store.status(chain_id, PhaseName::Live).await?,
        PhaseStatus::Running
    );

    supervisor.cancellation.cancel();
    supervisor.hold.notify_one();
    supervisor.task.await??;
    scratch.cleanup().await
}

#[tokio::test]
async fn a_redo_refused_for_its_range_leaves_stopped_live_as_it_was() -> Result<()> {
    let chain_id = "stopped-live-unreadable-range";
    let scratch = stopped_during_live(chain_id, Stop::BetweenBatches).await?;
    let error = runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "stopped-live-redo",
    )?
    .redo(
        &chain(chain_id)?,
        RedoPhase::Phase(PhaseName::Project),
        BlockRange::new(0, TIP + 5)?,
        CancellationToken::new(),
    )
    .await
    .expect_err("a range that ends past the readable head is refused");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity, "{error}");
    let store = PhaseStore::new(scratch.pool().clone());
    assert_eq!(
        store.status(chain_id, PhaseName::Live).await?,
        PhaseStatus::Running
    );
    assert_eq!(active_redo_count(&scratch, chain_id).await?, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn a_second_redo_is_refused_while_the_first_holds_the_phase() -> Result<()> {
    let chain_id = "stopped-live-two-redos";
    let scratch = stopped_during_live(chain_id, Stop::BetweenBatches).await?;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let blocking = Arc::new(BlockingPhase {
        name: PhaseName::Project,
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
    });
    let first = runner(
        scratch.runner(),
        phase_set_replacing(PhaseName::Project, blocking)?,
        available_capacity(),
        "stopped-live-first-redo",
    )?;
    let first_chain = chain(chain_id)?;
    let first_task = tokio::spawn(async move {
        first
            .redo(
                &first_chain,
                RedoPhase::Phase(PhaseName::Project),
                BlockRange::new(0, TIP).expect("fixed range"),
                CancellationToken::new(),
            )
            .await
    });
    tokio::pin!(first_task);
    tokio::select! {
        () = entered.notified() => {}
        result = &mut first_task => {
            result??;
            anyhow::bail!("the first redo finished before its batch was held");
        }
    }

    let error = redo(
        &scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Project),
        PhaseSet::loopback(),
    )
    .await
    .expect_err("the phase lock admits one redo at a time");
    assert_eq!(error.kind(), ErrorKind::LockHeld, "{error}");

    release.notify_one();
    first_task.await??;
    let store = PhaseStore::new(scratch.pool().clone());
    for phase in [PhaseName::Live, PhaseName::Project] {
        assert_eq!(store.status(chain_id, phase).await?, PhaseStatus::Completed);
    }
    assert_eq!(active_redo_count(&scratch, chain_id).await?, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn two_redos_started_together_leave_one_finished_redo() -> Result<()> {
    let chain_id = "stopped-live-racing-redos";
    let scratch = stopped_during_live(chain_id, Stop::BetweenBatches).await?;
    let project = RedoPhase::Phase(PhaseName::Project);
    let (first, second) = tokio::join!(
        redo(&scratch, chain_id, project, PhaseSet::loopback()),
        redo(&scratch, chain_id, project, PhaseSet::loopback()),
    );
    assert!(first.is_ok() || second.is_ok(), "{first:?} {second:?}");
    // A loser meets the winner on the Live lock or on the Project phase lock. The
    // winner holds the Project lock for as long as its row is running, so the loser
    // never reaches the phase transition against a running row.
    for error in [first, second].into_iter().filter_map(Result::err) {
        assert_eq!(error.kind(), ErrorKind::LockHeld, "{error}");
    }
    let store = PhaseStore::new(scratch.pool().clone());
    for phase in [PhaseName::Live, PhaseName::Project] {
        assert_eq!(store.status(chain_id, phase).await?, PhaseStatus::Completed);
    }
    assert_eq!(active_redo_count(&scratch, chain_id).await?, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn the_supervisor_restarts_after_a_redo_that_settled_live() -> Result<()> {
    let chain_id = "stopped-live-restart";
    let scratch = stopped_during_live(chain_id, Stop::BetweenBatches).await?;
    redo(
        &scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Project),
        PhaseSet::loopback(),
    )
    .await?;

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live = Arc::new(FunctionPhase {
        name: PhaseName::Live,
        handler: {
            let live_calls = Arc::clone(&live_calls);
            Arc::new(move |_| {
                live_calls.fetch_add(1, Ordering::SeqCst);
                Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))
            })
        },
    });
    runner(
        scratch.runner(),
        supervisor_phases(chain_id, live)?,
        available_capacity(),
        "stopped-live-restarted-supervisor",
    )?
    .run_chain(&supervised_chain(chain_id)?, CancellationToken::new())
    .await?;
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);
    let store = PhaseStore::new(scratch.pool().clone());
    for phase in PhaseName::ALL {
        assert_eq!(store.status(chain_id, phase).await?, PhaseStatus::Completed);
    }
    scratch.cleanup().await
}
