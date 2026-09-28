use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use bigname_project::{
    BatchRequest, Engine, ErrorKind as ProjectErrorKind, Marker, ProjectError,
    RunMode as ProjectRunMode,
    families::{FamilyMode, FamilyOptions, FamilyOutcome, InputToken, RebuildRanges},
};
use sqlx::PgPool;

mod family_batch;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    metrics::RunnerMetricsFeed,
    phase::{
        AfterProgress, AfterProgressFuture, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture,
        PhaseName, PhaseProgress, RunMode,
    },
};

/// The served marker and mode of a committed batch, with the input token read before its
/// progress was recorded or the error that read returned.
type PendingFamilies = (Marker, FamilyMode, Result<InputToken, ProjectError>);

/// How the owned key families follow the served batches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FamilySettings {
    /// Whether each committed batch is followed by the families.
    pub enabled: bool,
    /// The most family blocks one family run applies or undoes. A batch starts runs one after
    /// another until the families reach its served marker, so this bounds a run, not the batch.
    pub max_blocks_per_run: u64,
    /// Whether a family failure fails the Project run as retryable, a data-integrity one included.
    /// The supervised runner retries every one, so the lag gauge shows the stall; the one-shot
    /// `redo` command turns this off, so a failure of any kind keeps its own kind, is not
    /// retried, and the command exits instead of running the served redo again forever.
    pub retry_family_failures: bool,
    /// Which work blocks a family rebuild applies several to a transaction; production keeps the
    /// switch below the chain's safe block.
    pub rebuild_ranges: RebuildRanges,
    /// How long the input token read after a served batch may take. A read that outlasts it is a
    /// transient family failure raised after the batch's progress is recorded, never a skip.
    pub token_budget: Duration,
}

impl Default for FamilySettings {
    fn default() -> Self {
        Self {
            enabled: true,
            max_blocks_per_run: bigname_project::families::MAX_BLOCKS_PER_RUN,
            retry_family_failures: true,
            rebuild_ranges: RebuildRanges::BelowSafe,
            token_budget: Duration::from_secs(30),
        }
    }
}

pub struct ProjectPhase {
    pool: PgPool,
    engine: Engine,
    hydrator: Option<bigname_project::Hydrator>,
    metrics_feed: Option<RunnerMetricsFeed>,
    families: FamilySettings,
    /// The served marker, mode and input token of each chain's last committed batch, which the
    /// owned key families follow once the runner has recorded the batch's progress.
    pending_families: Arc<Mutex<BTreeMap<String, PendingFamilies>>>,
    /// With the publication switch on, each chain's family batch that spent its budget and
    /// answered `Continue`, which the next batch of the same run resumes.
    family_continuations: Arc<Mutex<BTreeMap<String, family_batch::Continuation>>>,
}

impl ProjectPhase {
    pub fn new(pool: PgPool) -> Self {
        Self {
            engine: Engine::new(pool.clone()),
            pool,
            hydrator: None,
            metrics_feed: None,
            families: FamilySettings::default(),
            pending_families: Arc::default(),
            family_continuations: Arc::default(),
        }
    }

    pub fn with_hydration(pool: PgPool, rpc_urls: bigname_lookup::ChainRpcUrls) -> Self {
        Self {
            engine: Engine::new(pool.clone()),
            hydrator: Some(bigname_project::Hydrator::new(pool.clone(), rpc_urls)),
            pool,
            metrics_feed: None,
            families: FamilySettings::default(),
            pending_families: Arc::default(),
            family_continuations: Arc::default(),
        }
    }

    /// Whether each committed batch is followed by the owned key families; on unless turned off.
    pub fn with_families(mut self, enabled: bool) -> Self {
        self.families.enabled = enabled;
        self
    }

    /// How the owned key families follow the served batches.
    pub fn with_family_settings(mut self, settings: FamilySettings) -> Self {
        self.families = settings;
        self
    }

    /// Reports what each committed batch wrote to the metrics task.
    pub fn with_metrics_feed(mut self, feed: RunnerMetricsFeed) -> Self {
        self.metrics_feed = Some(feed);
        self
    }

    /// Reports committed batches and the steps of full-rebuild and redo runs to one feed.
    pub fn with_metrics(self, feed: RunnerMetricsFeed) -> Self {
        self.with_metrics_feed(feed.clone())
            .with_step_observer(Arc::new(feed))
    }

    /// Reports the steps of full-rebuild and redo runs, which are one long transaction.
    pub fn with_step_observer(self, observer: Arc<dyn bigname_project::StepObserver>) -> Self {
        Self {
            engine: self.engine.with_step_observer(observer),
            ..self
        }
    }

    fn report_families(&self, chain_id: &str, outcome: &FamilyOutcome) {
        if let Some(feed) = &self.metrics_feed {
            feed.project_families(chain_id, outcome);
        }
    }

    /// One budgeted family run toward the served `target` of a recorded batch, reported whether
    /// or not it failed. While the run spent its budget and moved the families, the batch's work is
    /// kept for another call, in normal mode, since in the batch's own mode an unfinished rebuild
    /// would start again; the runner makes that call after recording the phase heartbeat.
    async fn run_families_once(
        &self,
        chain_id: &str,
        (target, mode, token): PendingFamilies,
    ) -> RunnerResult<AfterProgress> {
        let token = match token {
            Ok(token) => token,
            Err(error) => {
                let standing =
                    bigname_project::families::standing(&self.pool, chain_id, &target).await;
                self.report_families(chain_id, &standing);
                return Err(self.family_error(chain_id, &target, &standing, &error));
            }
        };
        let options = FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH)
            .with_max_blocks_per_run(self.families.max_blocks_per_run)
            .with_rebuild_ranges(self.families.rebuild_ranges);
        let options = match &self.hydrator {
            Some(hydrator) => options.with_hydration(hydrator.rpc_urls().clone()),
            None => options,
        };
        let (outcome, error) =
            bigname_project::families::run(&self.pool, chain_id, &target, mode, &token, &options)
                .await;
        self.report_families(chain_id, &outcome);
        if let Some(error) = error {
            return Err(self.family_error(chain_id, &target, &outcome, &error));
        }
        if !outcome.budget_exhausted || outcome.blocks + outcome.undone_blocks == 0 {
            return Ok(AfterProgress::Done);
        }
        self.pending_families
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(chain_id.to_owned(), (target, FamilyMode::Normal, Ok(token)));
        Ok(AfterProgress::More)
    }

    /// A family failure as the error of the Project run. While the families are a shadow that
    /// nothing serves, the supervised runner retries every failure short of a configuration
    /// error, a data-integrity one included, and the family lag gauge shows the stall rather than
    /// the served publication stopping for tables nothing reads. Under the one-shot `redo` command
    /// a failure keeps its own kind and is not retried, so any family failure ends the command.
    fn family_error(
        &self,
        chain_id: &str,
        target: &Marker,
        standing: &FamilyOutcome,
        error: &ProjectError,
    ) -> RunnerError {
        let retry = self.families.retry_family_failures;
        let kind = match error.kind() {
            ProjectErrorKind::Configuration => ErrorKind::Configuration,
            ProjectErrorKind::DataIntegrity if !retry => ErrorKind::DataIntegrity,
            ProjectErrorKind::Transient | ProjectErrorKind::DataIntegrity => ErrorKind::Transient,
        };
        let marker = standing.marker.as_ref().map_or_else(
            || "no block".to_owned(),
            |marker| format!("block {} ({})", marker.number, marker.hash),
        );
        let error = RunnerError::new(
            kind,
            format!(
                "owned key families of chain {chain_id} stopped at {marker}, short of served \
                 marker block {} ({}): {error}",
                target.number, target.hash
            ),
        );
        if retry { error } else { error.not_retried() }
    }

    async fn redo_target(&self, chain_id: &str) -> RunnerResult<BlockMarker> {
        let position: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
            "SELECT current_block_number, current_block_hash
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'project'",
        )
        .bind(chain_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to load recorded project head for redo on chain {chain_id}"),
                error,
            )
        })?;
        let (number, hash) = position
            .and_then(|(number, hash)| number.zip(hash))
            .ok_or_else(|| {
                RunnerError::new(
                    ErrorKind::DataIntegrity,
                    format!(
                        "cannot redo project on chain {chain_id}: the phase has no recorded head"
                    ),
                )
            })?;
        let _recorded = BlockMarker::new(number, hash)?;
        let canonical = crate::heads::load_marker(&self.pool, chain_id, number)
            .await?
            .ok_or_else(|| {
                RunnerError::new(
                    ErrorKind::DataIntegrity,
                    format!(
                        "cannot redo project on chain {chain_id}: recorded head {number} is not readable (canonical, safe, or finalized)"
                    ),
                )
            })?;
        Ok(canonical)
    }
}

impl Phase for ProjectPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }

    fn has_after_progress_work(&self, chain_id: &str) -> bool {
        self.pending_families
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(chain_id)
    }

    fn after_progress_recorded(&self, chain_id: &str) -> AfterProgressFuture<'_> {
        let pending = self
            .pending_families
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(chain_id);
        let chain_id = chain_id.to_owned();
        Box::pin(async move {
            let Some(pending) = pending else {
                return Ok(AfterProgress::Done);
            };
            self.run_families_once(&chain_id, pending).await
        })
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            // A family run an earlier batch planned and a stop left waiting belongs to that
            // batch: this one plans its own or none, so no older run reaches the loop after it.
            self.pending_families
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&context.chain_id);
            // With the publication switch on the family loop is the batch: the served engine,
            // the served hydrator and the redo target they read do not run (TYR-36 step 7b).
            if bigname_storage::publication_source::serve_from_families() {
                return self.run_family_batch(context).await;
            }
            if let Some(hydrator) = &self.hydrator {
                hydrator
                    .require_rpc_configuration(&context.chain_id)
                    .map_err(runner_error)?;
            }
            let redo_target =
                if matches!(context.mode, RunMode::Redo(_) | RunMode::RecomputeFlags(_)) {
                    Some(self.redo_target(&context.chain_id).await?)
                } else {
                    None
                };
            let Some(available) = context.available_heads.as_ref() else {
                if let Some(range) = context.mode.range() {
                    return Err(RunnerError::new(
                        ErrorKind::DataIntegrity,
                        format!(
                            "project redo block {} for chain {} is not readable (canonical, safe, or finalized)",
                            range.to, context.chain_id
                        ),
                    ));
                }
                return Ok(PhaseBatchOutcome::Complete(PhaseProgress::default()));
            };
            let target_block = redo_target
                .as_ref()
                .map_or(available.latest.number, |marker| marker.number);
            let redo_to = context.mode.range().map(|range| range.to);
            let (affected_from_block, affected_to_block) = match context.mode {
                RunMode::Normal => {
                    let from = context.resume.current.as_ref().map_or_else(
                        || {
                            context
                                .sources
                                .iter()
                                .map(|source| source.start_block_number)
                                .min()
                                .unwrap_or(0)
                        },
                        |marker| marker.number.saturating_add(1).min(target_block),
                    );
                    (from, target_block)
                }
                RunMode::Redo(range) | RunMode::RecomputeFlags(range) => (range.from, range.to),
            };
            let outcome = self
                .engine
                .run_batch(BatchRequest {
                    chain_id: context.chain_id.clone(),
                    target_block,
                    affected_from_block,
                    affected_to_block,
                    resume_current: context.resume.current.as_ref().map(project_marker),
                    mode: if matches!(context.mode, RunMode::Normal) {
                        ProjectRunMode::Normal
                    } else {
                        ProjectRunMode::Redo
                    },
                })
                .await;
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    let audit_error = match error.generation_failure_evidence() {
                        Some(evidence) => crate::project_failure_audit::persist(
                            &self.pool,
                            &context.chain_id,
                            bigname_content_hash::INTERPRETER_CONTENT_HASH,
                            evidence,
                        )
                        .await
                        .err(),
                        None => None,
                    };
                    let failure = runner_error(error);
                    // The invariant diagnosis outlives a failed audit write.
                    return Err(match audit_error {
                        Some(audit_error) => failure
                            .with_secondary("record projection generation failure", audit_error),
                        None => failure,
                    });
                }
            };
            if let Some(feed) = &self.metrics_feed {
                feed.project_batch(&context.chain_id, &outcome.write_summary);
            }
            if self.families.enabled {
                let mode = match context.mode {
                    RunMode::Normal if context.resume.current.is_none() => FamilyMode::Rebuild,
                    RunMode::Normal => FamilyMode::Normal,
                    RunMode::Redo(range) | RunMode::RecomputeFlags(range) => FamilyMode::Redo {
                        from: range.from,
                        to: range.to,
                    },
                };
                // Read now, while a finished redo's session is still open on the Project row: the
                // runner closes it when it records this batch. The batch's writes are committed,
                // so the read is bounded: a read that fails or outlasts the bound fails the family
                // run that follows the progress write, as a transient failure.
                let budget = self.families.token_budget;
                let token = tokio::time::timeout(
                    budget,
                    bigname_project::families::input_token(&self.pool, &context.chain_id),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(ProjectError::transient(format!(
                        "the family input token did not read within {budget:?}"
                    )))
                });
                self.pending_families
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(
                        context.chain_id.clone(),
                        (outcome.current.clone(), mode, token),
                    );
            }
            if let Some(hydrator) = &self.hydrator {
                hydrator
                    .hydrate_if_canonical_head(&context.chain_id, &outcome.current)
                    .await
                    .map_err(runner_error)?;
            }
            let progress_marker = match redo_to {
                Some(block_number) => crate::heads::load_marker(
                    &self.pool,
                    &context.chain_id,
                    block_number,
                )
                .await?
                .ok_or_else(|| {
                    RunnerError::new(
                        ErrorKind::DataIntegrity,
                        format!(
                            "project redo block {block_number} for chain {} is not readable (canonical, safe, or finalized)",
                            context.chain_id
                        ),
                    )
                })?,
                None => runner_marker(outcome.current)?,
            };
            Ok(PhaseBatchOutcome::Complete(PhaseProgress {
                current: Some(progress_marker.clone()),
                target: Some(progress_marker),
                estimated_write_bytes: outcome.estimated_write_bytes,
                ..PhaseProgress::default()
            }))
        })
    }
}

fn project_marker(marker: &BlockMarker) -> Marker {
    Marker {
        number: marker.number,
        hash: marker.hash.clone(),
    }
}

fn runner_marker(marker: Marker) -> RunnerResult<BlockMarker> {
    BlockMarker::new(marker.number, marker.hash)
}

fn runner_error(error: ProjectError) -> RunnerError {
    let kind = match error.kind() {
        ProjectErrorKind::Transient => ErrorKind::Transient,
        ProjectErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        ProjectErrorKind::Configuration => ErrorKind::Configuration,
    };
    RunnerError::new(kind, error.to_string())
}
