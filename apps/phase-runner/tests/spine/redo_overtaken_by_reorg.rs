//! A redo that a reorg recovery overtakes. Head publication that replaces a
//! readable block stamps a required redo on Interpret, Project and Verify. On a
//! redo already in progress the stamp raises the redo's attempt generation and
//! clears its progress. When the stamped range equals the redo's own range, the
//! generation is the only sign that the redo's work is stale.
//!
//! | Row | Ordering | Expected | Test |
//! | --- | --- | --- | --- |
//! | 1 | No stamp lands on the redo | Redo completes | `a_redo_that_nothing_overtook_completes` |
//! | 2 | Fork published after the redo's last progress write, Live settled by supervisor start-up | Completion refused, repair kept, rerun completes on the new fork | `a_fork_published_after_the_last_progress_write_keeps_the_repair` |
//! | 3 | Fork published during the redo's batch, Live settled by supervisor start-up | Progress write refused, repair kept | `a_fork_published_during_the_batch_keeps_the_repair` |
//! | 4 | Row 2 with Live settled by the redo itself after a killed supervisor | Completion refused, repair kept | `a_fork_published_after_a_redo_settled_live_keeps_the_repair` |
//! | 5 | Project redo, fork published after its last progress write, Interpret stamped by the same publication | Redo completes, the next supervisor start redoes every phase on the new fork | `a_project_redo_overtaken_at_completion_completes_when_interpret_is_stamped` |
//! | 5b | Row 5 with Project's cursor above Interpret's, so Interpret is not stamped | Completion refused, repair kept, command names the requested range | `a_project_redo_overtaken_at_completion_is_refused_when_interpret_is_not_stamped` |
//! | 5c | Project redo, fork published during its batch | Unchanged: progress write refused, then neither Project nor Interpret can start | `a_project_redo_overtaken_during_its_batch_is_left_blocked` |
//! | 6 | Verify-only redo beside a supervisor running Live, fork published after its last progress write | Completion refused, repair kept, rerun completes after the Interpret and Project repairs | `a_verify_redo_beside_live_overtaken_at_completion_keeps_the_repair` |
//! | 7 | All-phase redo overtaken in Interpret | Refused, one first command in both instructions | `an_all_phase_redo_overtaken_in_interpret_names_one_first_command` |
//! | 8 | Supervisor's own required redo overtaken at completion | That chain stops, the next supervisor start reruns the redo | `a_supervised_required_redo_overtaken_at_completion_stops_the_chain_and_heals_on_restart` |

use super::*;

const TIP: i64 = 2;

fn fork_hash(chain_id: &str, number: i64) -> String {
    format!("{chain_id}-fork-b-{number}")
}

/// When the Interpret redo publishes the competing fork.
#[derive(Clone, Copy, PartialEq)]
enum Publish {
    Never,
    DuringBatch,
    AfterProgress,
}

/// A phase beside whose redo a head publication replaces block 1 onwards.
struct ForkPublishingPhase {
    name: PhaseName,
    pool: sqlx::PgPool,
    chain_id: String,
    publish: Publish,
    /// The tip hash to publish.
    tip_hash: String,
    published: AtomicBool,
}

impl ForkPublishingPhase {
    async fn publish_fork(&self) -> RunnerResult<()> {
        self.published.store(true, Ordering::SeqCst);
        publish_heads(
            &self.pool,
            &self.chain_id,
            &HeadMarkers {
                latest: BlockMarker::new(TIP, self.tip_hash.clone())?,
                safe: None,
                finalized: None,
            },
        )
        .await
    }
}

impl Phase for ForkPublishingPhase {
    fn name(&self) -> PhaseName {
        self.name
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            if self.publish == Publish::DuringBatch {
                self.publish_fork().await?;
            }
            LoopbackPhase::new(self.name).run_batch(context).await
        })
    }

    fn has_after_progress_work(&self, _chain_id: &str) -> bool {
        self.publish == Publish::AfterProgress && !self.published.load(Ordering::SeqCst)
    }

    fn after_progress_recorded(
        &self,
        _chain_id: &str,
    ) -> phase_runner::phase::AfterProgressFuture<'_> {
        Box::pin(async move {
            self.publish_fork().await?;
            Ok(phase_runner::phase::AfterProgress::Done)
        })
    }
}

/// A chain indexed through `TIP` with a competing fork of blocks 1 and 2 observed.
async fn seed_fork(scratch: &ScratchDatabase, chain_id: &str) -> Result<()> {
    for number in 1..=TIP {
        let parent = if number == 1 {
            format!("{chain_id}-block-0")
        } else {
            fork_hash(chain_id, number - 1)
        };
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             )
             VALUES ($1, $2, $3, $4, to_timestamp($4), 'observed')",
        )
        .bind(chain_id)
        .bind(fork_hash(chain_id, number))
        .bind(parent)
        .bind(number)
        .execute(scratch.pool())
        .await?;
    }
    Ok(())
}

/// The state a supervisor leaves when it stops inside Live, then a supervisor
/// start-up that settles it. This route to a redo exists without the redo
/// settling Live itself.
async fn chain_settled_by_supervisor_start(chain_id: &str) -> Result<ScratchDatabase> {
    let scratch = ScratchDatabase::create(chain_id).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain_id).await?;
    seed_interpret_redo_presence(scratch.pool(), chain_id, TIP).await?;
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: BlockMarker::new(TIP, format!("{chain_id}-block-{TIP}"))?,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    for phase in [
        PhaseName::Ingest,
        PhaseName::Interpret,
        PhaseName::Project,
        PhaseName::Verify,
    ] {
        let hash = phase
            .writes_derived_data()
            .then_some(phase_runner::INTERPRETER_CONTENT_HASH);
        mark_completed(scratch.pool(), chain_id, phase, hash).await?;
        set_phase_extent(scratch.pool(), chain_id, phase, TIP).await?;
    }
    store
        .start_phase(chain_id, PhaseName::Live, &RunMode::Normal)
        .await?;
    // A supervisor started with a stop already pending runs start-up recovery alone.
    let stopped = CancellationToken::new();
    stopped.cancel();
    runner(
        scratch.runner(),
        PhaseSet::loopback(),
        available_capacity(),
        "overtaken-redo-start-up",
    )?
    .run_chain(&chain(chain_id)?, stopped)
    .await?;
    assert_eq!(
        store.status(chain_id, PhaseName::Live).await?,
        PhaseStatus::Completed
    );
    seed_fork(&scratch, chain_id).await?;
    Ok(scratch)
}

/// Loopback phases with `publisher` publishing the competing fork as told.
fn publishing_phases(
    scratch: &ScratchDatabase,
    chain_id: &str,
    publisher: PhaseName,
    publish: Publish,
    tip_hash: String,
) -> Result<PhaseSet> {
    let publishing = Arc::new(ForkPublishingPhase {
        name: publisher,
        pool: scratch.pool().clone(),
        chain_id: chain_id.to_owned(),
        publish,
        tip_hash,
        published: AtomicBool::new(false),
    });
    Ok(PhaseSet::new(PhaseName::ALL.map(|name| {
        if name == publisher {
            Arc::clone(&publishing) as Arc<dyn Phase>
        } else {
            Arc::new(LoopbackPhase::new(name)) as Arc<dyn Phase>
        }
    }))?)
}

/// Redo `selection` over the whole chain. `publisher` publishes the fork.
async fn redo_publishing(
    scratch: &ScratchDatabase,
    chain_id: &str,
    selection: RedoPhase,
    publisher: PhaseName,
    publish: Publish,
) -> Result<RunnerResult<()>> {
    redo_publishing_tip(
        scratch,
        chain_id,
        selection,
        publisher,
        publish,
        fork_hash(chain_id, TIP),
    )
    .await
}

/// As `redo_publishing`, publishing `tip_hash` as the new tip.
async fn redo_publishing_tip(
    scratch: &ScratchDatabase,
    chain_id: &str,
    selection: RedoPhase,
    publisher: PhaseName,
    publish: Publish,
    tip_hash: String,
) -> Result<RunnerResult<()>> {
    let phases = publishing_phases(scratch, chain_id, publisher, publish, tip_hash)?;
    Ok(runner(
        scratch.runner(),
        phases,
        available_capacity(),
        "overtaken-redo",
    )?
    .redo(
        &chain(chain_id)?,
        selection,
        BlockRange::new(0, TIP)?,
        CancellationToken::new(),
    )
    .await)
}

async fn interpret_redo(
    scratch: &ScratchDatabase,
    chain_id: &str,
    publish: Publish,
) -> RunnerResult<()> {
    redo_publishing(
        scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Interpret),
        PhaseName::Interpret,
        publish,
    )
    .await
    .expect("the fixture phases are ordered")
}

async fn current_hash(
    scratch: &ScratchDatabase,
    chain_id: &str,
    phase: PhaseName,
) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT current_block_hash FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = $2",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .fetch_one(scratch.pool())
    .await?)
}

type InterpretRow = (bool, Option<i64>, Option<i64>, Option<String>);

async fn interpret_row(scratch: &ScratchDatabase, chain_id: &str) -> Result<InterpretRow> {
    Ok(sqlx::query_as(
        "SELECT redo_in_progress, redo_from_block_number, redo_to_block_number,
                current_block_hash
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?)
}

/// The redo must fail with the rerun command, keep its marker, and finish on
/// the new fork when that command is run.
async fn assert_overtaken_completion_keeps_the_repair(
    scratch: &ScratchDatabase,
    chain_id: &str,
) -> Result<()> {
    let error = interpret_redo(scratch, chain_id, Publish::AfterProgress)
        .await
        .expect_err("a redo overtaken by a reorg recovery must not complete");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity, "{error}");
    let message = error.to_string();
    assert!(
        message.contains("was overtaken before it completed"),
        "{message}"
    );
    assert!(
        message.contains(&format!(
            "rerun `phase-runner redo --chain {chain_id} --phase interpret --from-block 0 \
             --to-block {TIP}`"
        )),
        "{message}"
    );
    let (in_progress, from, to, _) = interpret_row(scratch, chain_id).await?;
    assert_eq!((in_progress, from, to), (true, Some(0), Some(TIP)));

    interpret_redo(scratch, chain_id, Publish::Never).await?;
    let (in_progress, _, _, current_hash) = interpret_row(scratch, chain_id).await?;
    assert!(!in_progress);
    assert_eq!(current_hash, Some(fork_hash(chain_id, TIP)));
    Ok(())
}

#[tokio::test]
async fn a_redo_that_nothing_overtook_completes() -> Result<()> {
    let chain_id = "redo-not-overtaken";
    let scratch = chain_settled_by_supervisor_start(chain_id).await?;
    interpret_redo(&scratch, chain_id, Publish::Never).await?;
    let (in_progress, _, _, current_hash) = interpret_row(&scratch, chain_id).await?;
    assert!(!in_progress);
    assert_eq!(current_hash, Some(format!("{chain_id}-block-{TIP}")));
    scratch.cleanup().await
}

#[tokio::test]
async fn a_fork_published_after_the_last_progress_write_keeps_the_repair() -> Result<()> {
    let chain_id = "redo-overtaken-after-progress";
    let scratch = chain_settled_by_supervisor_start(chain_id).await?;
    assert_overtaken_completion_keeps_the_repair(&scratch, chain_id).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn a_fork_published_during_the_batch_keeps_the_repair() -> Result<()> {
    let chain_id = "redo-overtaken-during-batch";
    let scratch = chain_settled_by_supervisor_start(chain_id).await?;
    let error = interpret_redo(&scratch, chain_id, Publish::DuringBatch)
        .await
        .expect_err("progress recorded on the replaced fork must be refused");
    assert!(
        error
            .to_string()
            .contains("redo attempt superseded; progress not recorded"),
        "{error}"
    );
    let (in_progress, from, to, _) = interpret_row(&scratch, chain_id).await?;
    assert_eq!((in_progress, from, to), (true, Some(0), Some(TIP)));
    scratch.cleanup().await
}

#[tokio::test]
async fn a_fork_published_after_a_redo_settled_live_keeps_the_repair() -> Result<()> {
    let chain_id = "redo-overtaken-after-settling-live";
    let scratch =
        redo_stopped_recovery::stopped_during_live(chain_id, redo_stopped_recovery::Stop::Killed)
            .await?;
    seed_fork(&scratch, chain_id).await?;
    assert_overtaken_completion_keeps_the_repair(&scratch, chain_id).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    assert_eq!(
        store.status(chain_id, PhaseName::Live).await?,
        PhaseStatus::Completed
    );
    scratch.cleanup().await
}

/// Refused at completion with the rerun command for `phase`, and the redo kept.
async fn assert_overtaken(
    scratch: &ScratchDatabase,
    chain_id: &str,
    phase: PhaseName,
    error: &RunnerError,
) -> Result<()> {
    assert_eq!(error.kind(), ErrorKind::DataIntegrity, "{error}");
    let message = error.to_string();
    assert!(
        message.contains(&format!(
            "redo for chain {chain_id} phase {phase} was overtaken before it completed"
        )),
        "{message}"
    );
    assert!(
        message.contains(&format!(
            "rerun `phase-runner redo --chain {chain_id} --phase {phase} --from-block 0 \
             --to-block {TIP}`"
        )),
        "{message}"
    );
    type Marker = (bool, Option<i64>, Option<i64>, Option<i64>, Option<i64>);
    let marker: Marker = sqlx::query_as(
        "SELECT redo_in_progress, redo_from_block_number, redo_to_block_number,
                redo_requested_from_block_number, redo_requested_to_block_number
         FROM chain_phase_state WHERE chain_id = $1 AND phase_name = $2",
    )
    .bind(chain_id)
    .bind(phase.as_str())
    .fetch_one(scratch.pool())
    .await?;
    let requested = (phase == PhaseName::Project).then_some((0, TIP)).unzip();
    assert_eq!(
        marker,
        (true, Some(0), Some(TIP), requested.0, requested.1),
        "{phase}"
    );
    Ok(())
}

/// A supervisor that runs every phase once and returns.
async fn run_supervisor_once(scratch: &ScratchDatabase, chain_id: &str) -> Result<()> {
    let live = Arc::new(FunctionPhase {
        name: PhaseName::Live,
        handler: Arc::new(|_| Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()))),
    });
    let phases = PhaseSet::new(PhaseName::ALL.map(|name| {
        if name == PhaseName::Live {
            Arc::clone(&live) as Arc<dyn Phase>
        } else {
            Arc::new(LoopbackPhase::new(name)) as Arc<dyn Phase>
        }
    }))?;
    let mut supervised = chain(chain_id)?;
    supervised.verify_before_live = true;
    runner(
        scratch.runner(),
        phases,
        available_capacity(),
        "overtaken-supervisor-restart",
    )?
    .run_chain(&supervised, CancellationToken::new())
    .await?;
    Ok(())
}

#[tokio::test]
async fn a_project_redo_overtaken_at_completion_completes_when_interpret_is_stamped() -> Result<()>
{
    let chain_id = "redo-overtaken-project";
    let scratch = chain_settled_by_supervisor_start(chain_id).await?;
    redo_publishing(
        &scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Project),
        PhaseName::Project,
        Publish::AfterProgress,
    )
    .await??;
    // The publication stamped Interpret too. That repair is what redoes Project.
    let stamped: Vec<String> = sqlx::query_scalar(
        "SELECT phase_name FROM chain_phase_state
         WHERE chain_id = $1 AND redo_in_progress AND last_error LIKE 'required downstream redo%'
         ORDER BY phase_name",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    assert!(stamped.contains(&"interpret".to_owned()), "{stamped:?}");

    run_supervisor_once(&scratch, chain_id).await?;
    let rows: Vec<(String, String, bool, Option<String>)> = sqlx::query_as(
        "SELECT phase_name, phase_status, redo_in_progress, current_block_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project', 'verify')
         ORDER BY phase_name",
    )
    .bind(chain_id)
    .fetch_all(scratch.pool())
    .await?;
    let healed = |phase: &str| {
        (
            phase.to_owned(),
            "completed".to_owned(),
            false,
            Some(fork_hash(chain_id, TIP)),
        )
    };
    assert_eq!(
        rows,
        [healed("interpret"), healed("project"), healed("verify")]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn a_project_redo_overtaken_at_completion_is_refused_when_interpret_is_not_stamped()
-> Result<()> {
    let chain_id = "redo-overtaken-project-ahead";
    let scratch = chain_settled_by_supervisor_start(chain_id).await?;
    // Project stands one block above Interpret, and the fork replaces that block
    // alone, so the publication stamps Project and leaves Interpret unstamped.
    set_phase_extent(scratch.pool(), chain_id, PhaseName::Interpret, TIP - 1).await?;
    let tip_only = format!("{chain_id}-tip-only-fork");
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         )
         VALUES ($1, $2, $3, $4, to_timestamp($4), 'observed')",
    )
    .bind(chain_id)
    .bind(&tip_only)
    .bind(format!("{chain_id}-block-{}", TIP - 1))
    .bind(TIP)
    .execute(scratch.pool())
    .await?;

    let error = redo_publishing_tip(
        &scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Project),
        PhaseName::Project,
        Publish::AfterProgress,
        tip_only,
    )
    .await?
    .expect_err("nothing would redo Project after this completion, so it is refused");
    assert_overtaken(&scratch, chain_id, PhaseName::Project, &error).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    assert_eq!(
        store.status(chain_id, PhaseName::Interpret).await?,
        PhaseStatus::Completed
    );

    // Interpret is not waiting on a repair, so the printed command runs.
    redo_publishing(
        &scratch,
        chain_id,
        RedoPhase::Phase(PhaseName::Project),
        PhaseName::Project,
        Publish::Never,
    )
    .await??;
    assert_eq!(
        store.status(chain_id, PhaseName::Project).await?,
        PhaseStatus::Completed
    );
    assert_eq!(
        current_hash(&scratch, chain_id, PhaseName::Project).await?,
        Some(format!("{chain_id}-tip-only-fork"))
    );
    scratch.cleanup().await
}

/// Unchanged behaviour, recorded here so a change to it is noticed. A fork
/// published during the Project redo's batch is refused at the progress write.
/// Neither the Project rerun nor the Interpret repair can then start.
#[tokio::test]
async fn a_project_redo_overtaken_during_its_batch_is_left_blocked() -> Result<()> {
    let chain_id = "redo-overtaken-project-batch";
    let scratch = chain_settled_by_supervisor_start(chain_id).await?;
    let project = RedoPhase::Phase(PhaseName::Project);
    let error = redo_publishing(
        &scratch,
        chain_id,
        project,
        PhaseName::Project,
        Publish::DuringBatch,
    )
    .await?
    .expect_err("progress recorded on the replaced fork must be refused");
    assert!(
        error
            .to_string()
            .contains("redo attempt superseded; progress not recorded"),
        "{error}"
    );

    let rerun = redo_publishing(
        &scratch,
        chain_id,
        project,
        PhaseName::Project,
        Publish::Never,
    )
    .await?
    .expect_err("Project waits for the Interpret repair");
    assert!(
        rerun
            .to_string()
            .contains("prerequisite interpret is not completed"),
        "{rerun}"
    );
    let interpret = interpret_redo(&scratch, chain_id, Publish::Never)
        .await
        .expect_err("Interpret is refused while the Project redo is recorded as running");
    assert!(
        interpret
            .to_string()
            .contains("while phase project is running"),
        "{interpret}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn a_verify_redo_beside_live_overtaken_at_completion_keeps_the_repair() -> Result<()> {
    let chain_id = "redo-overtaken-verify-beside-live";
    let scratch = ScratchDatabase::create(chain_id).await?;
    let supervisor = redo_stopped_recovery::start_supervisor(&scratch, chain_id, true).await?;
    redo_stopped_recovery::seed_redo_extents(&scratch, chain_id).await?;
    seed_fork(&scratch, chain_id).await?;
    let verify = RedoPhase::Phase(PhaseName::Verify);

    // The publication stands in for the one the supervisor's Live would make.
    let error = redo_publishing(
        &scratch,
        chain_id,
        verify,
        PhaseName::Verify,
        Publish::AfterProgress,
    )
    .await?
    .expect_err("a Verify redo overtaken by a reorg recovery must not complete");
    assert_overtaken(&scratch, chain_id, PhaseName::Verify, &error).await?;
    assert!(
        error
            .to_string()
            .contains("once the Interpret and Project repairs have completed, rerun"),
        "{error}"
    );

    // The reorg also left repairs on Interpret and Project, which Verify waits
    // for. Here an Interpret redo runs them once the supervisor has stopped.
    supervisor.cancellation.cancel();
    supervisor.hold.notify_one();
    supervisor.task.await??;
    interpret_redo(&scratch, chain_id, Publish::Never).await?;
    redo_publishing(
        &scratch,
        chain_id,
        verify,
        PhaseName::Verify,
        Publish::Never,
    )
    .await??;
    let store = PhaseStore::new(scratch.pool().clone());
    assert_eq!(
        store.status(chain_id, PhaseName::Verify).await?,
        PhaseStatus::Completed
    );
    assert_eq!(
        current_hash(&scratch, chain_id, PhaseName::Verify).await?,
        Some(fork_hash(chain_id, TIP))
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn an_all_phase_redo_overtaken_in_interpret_names_one_first_command() -> Result<()> {
    let chain_id = "redo-overtaken-all";
    let scratch = chain_settled_by_supervisor_start(chain_id).await?;
    let error = redo_publishing(
        &scratch,
        chain_id,
        RedoPhase::All,
        PhaseName::Interpret,
        Publish::AfterProgress,
    )
    .await?
    .expect_err("an all-phase redo overtaken in Interpret must not complete");
    assert_overtaken(&scratch, chain_id, PhaseName::Interpret, &error).await?;
    // The all-phase recovery text follows the phase's own. Both start with the
    // same Interpret command, and the recovery text then lists the later steps.
    let message = error.to_string();
    let interpret = format!(
        "rerun `phase-runner redo --chain {chain_id} --phase interpret --from-block 0 \
         --to-block {TIP}`"
    );
    let recovery = message
        .split_once("cannot redo all phases")
        .map(|(_, recovery)| recovery)
        .unwrap_or_else(|| panic!("no all-phase recovery in {message}"));
    assert!(
        recovery.contains(&format!(
            "a pending interpret redo must be completed first; {interpret}"
        )),
        "{message}"
    );
    assert!(
        recovery.contains(&format!(
            "then rerun `phase-runner redo --chain {chain_id} --phase all --from-block 0 \
             --to-block {TIP}`"
        )),
        "{message}"
    );

    interpret_redo(&scratch, chain_id, Publish::Never).await?;
    scratch.cleanup().await
}

#[tokio::test]
async fn a_supervised_required_redo_overtaken_at_completion_stops_the_chain_and_heals_on_restart()
-> Result<()> {
    let chain_id = "redo-overtaken-supervised";
    let scratch = chain_settled_by_supervisor_start(chain_id).await?;
    // A reorg leaves required redos on Interpret, Project and Verify.
    publish_heads(
        scratch.pool(),
        chain_id,
        &HeadMarkers {
            latest: BlockMarker::new(TIP, fork_hash(chain_id, TIP))?,
            safe: None,
            finalized: None,
        },
    )
    .await?;
    let mut supervised = chain(chain_id)?;
    supervised.verify_before_live = true;

    // A second reorg, back to the first fork, overtakes the required Interpret redo.
    let phases = publishing_phases(
        &scratch,
        chain_id,
        PhaseName::Interpret,
        Publish::AfterProgress,
        format!("{chain_id}-block-{TIP}"),
    )?;
    let error = runner(
        scratch.runner(),
        phases,
        available_capacity(),
        "overtaken-supervisor",
    )?
    .run_chain(&supervised, CancellationToken::new())
    .await
    .expect_err("the overtaken required redo stops the chain");
    assert_eq!(error.kind(), ErrorKind::DataIntegrity, "{error}");
    assert!(!error.is_retryable());
    assert!(
        error
            .to_string()
            .contains("was overtaken before it completed"),
        "{error}"
    );
    let (in_progress, last_error): (bool, Option<String>) = sqlx::query_as(
        "SELECT redo_in_progress, last_error FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert!(in_progress);
    let last_error = last_error.unwrap_or_default();
    assert!(
        last_error.starts_with("required downstream redo: ")
            && last_error.contains("last attempt failed: redo for chain"),
        "{last_error}"
    );

    // The next supervisor start reruns the required redos without an operator.
    run_supervisor_once(&scratch, chain_id).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    for phase in PhaseName::ALL {
        assert_eq!(store.status(chain_id, phase).await?, PhaseStatus::Completed);
    }
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_phase_state WHERE chain_id = $1 AND redo_in_progress",
    )
    .bind(chain_id)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(pending, 0);
    scratch.cleanup().await
}
