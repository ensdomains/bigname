use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use bigname_project::{
    BatchRequest, Engine, ErrorKind as ProjectErrorKind, Marker, RunMode as ProjectRunMode,
    families::{FamilyMode, FamilyOptions, InputToken},
};
use sqlx::PgPool;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    metrics::RunnerMetricsFeed,
    phase::{
        AfterProgressFuture, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName,
        PhaseProgress, RunMode,
    },
};

/// The served marker and mode of a committed batch, with the input token read before its
/// progress was recorded or the reason that read failed.
type PendingFamilies = (Marker, FamilyMode, Result<InputToken, String>);

/// How the owned key families follow the served batches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FamilySettings {
    /// Whether each committed batch is followed by the families.
    pub enabled: bool,
    /// The most family blocks one runner cycle applies or undoes; a rebuild or a long catch-up
    /// continues on later cycles.
    pub max_blocks_per_run: u64,
    /// How long the input token read before a batch's progress may take; past it the families
    /// skip that batch and the skip is counted.
    pub token_budget: Duration,
    /// Whether a family run that spends its block budget is followed at once by another until
    /// the families reach the served marker. The one-shot `redo` command sets it, since no later
    /// batch follows it. The supervised run leaves it off: the batch's publication is already
    /// committed, and the next batch waits for one budgeted family run rather than a series.
    pub finish_each_batch: bool,
}

impl Default for FamilySettings {
    fn default() -> Self {
        Self {
            enabled: true,
            max_blocks_per_run: bigname_project::families::MAX_BLOCKS_PER_RUN,
            token_budget: Duration::from_secs(2),
            finish_each_batch: false,
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
    /// Chains whose finishing family run left the families short of the served marker.
    family_shortfalls: Arc<Mutex<BTreeMap<String, String>>>,
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
            family_shortfalls: Arc::default(),
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
            family_shortfalls: Arc::default(),
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

    /// Record a chain's families as short of `target` before a finishing run, and clear the entry
    /// only once they reach it, so a run that is abandoned midway stays reported.
    fn note_shortfall(
        &self,
        chain_id: &str,
        target: &Marker,
        outcome: Option<&bigname_project::families::FamilyOutcome>,
    ) {
        let mut shortfalls = self
            .family_shortfalls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(outcome) = outcome else {
            shortfalls.insert(
                chain_id.to_owned(),
                format!(
                    "chain {chain_id}: the family run toward block {} did not finish",
                    target.number
                ),
            );
            return;
        };
        if outcome.lag_blocks() == 0 {
            shortfalls.remove(chain_id);
            return;
        }
        let marker = outcome.marker.as_ref().map_or_else(
            || "no block".to_owned(),
            |marker| format!("block {} ({})", marker.number, marker.hash),
        );
        let reason = outcome.skipped.as_deref().unwrap_or("stopped");
        tracing::error!(
            chain_id,
            family_block = outcome.marker.as_ref().map(|marker| marker.number),
            target_block = target.number,
            target_hash = target.hash,
            reason,
            "one-shot redo left the owned key families short of the served marker; rerun the \
             same redo"
        );
        shortfalls.insert(
            chain_id.to_owned(),
            format!(
                "chain {chain_id}: families at {marker}, served marker block {} ({}): {reason}",
                target.number, target.hash
            ),
        );
    }

    fn report_families(&self, chain_id: &str, outcome: &bigname_project::families::FamilyOutcome) {
        if let Some(feed) = &self.metrics_feed {
            feed.project_families(chain_id, outcome);
        }
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

    fn after_progress_recorded(&self, chain_id: &str) -> AfterProgressFuture<'_> {
        let pending = self
            .pending_families
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(chain_id);
        let chain_id = chain_id.to_owned();
        Box::pin(async move {
            let Some((target, mode, token)) = pending else {
                return;
            };
            let options = FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH)
                .with_max_blocks_per_run(self.families.max_blocks_per_run);
            let finish = self.families.finish_each_batch;
            if finish {
                self.note_shortfall(&chain_id, &target, None);
            }
            let token = match token {
                Ok(token) => token,
                Err(reason) => {
                    let outcome =
                        bigname_project::families::skipped(&self.pool, &chain_id, &target, reason)
                            .await;
                    self.report_families(&chain_id, &outcome);
                    if finish {
                        self.note_shortfall(&chain_id, &target, Some(&outcome));
                    }
                    return;
                }
            };
            let mut outcome = bigname_project::families::apply(
                &self.pool, &chain_id, &target, mode, &token, &options,
            )
            .await;
            self.report_families(&chain_id, &outcome);
            // Later runs continue in normal mode: in the redo's own mode an unfinished rebuild
            // would start again.
            while finish
                && outcome.budget_exhausted
                && outcome.skipped.is_none()
                && outcome.blocks + outcome.undone_blocks > 0
            {
                outcome = bigname_project::families::apply(
                    &self.pool,
                    &chain_id,
                    &target,
                    FamilyMode::Normal,
                    &token,
                    &options,
                )
                .await;
                self.report_families(&chain_id, &outcome);
            }
            if finish {
                self.note_shortfall(&chain_id, &target, Some(&outcome));
            }
        })
    }

    fn after_redo(&self, chain_id: &str) -> RunnerResult<()> {
        let shortfall = self
            .family_shortfalls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(chain_id)
            .cloned();
        shortfall.map_or(Ok(()), |shortfall| {
            Err(RunnerError::new(
                ErrorKind::Transient,
                format!(
                    "family repair incomplete; the redo is recorded, rerun the same redo to \
                     finish the owned key families: {shortfall}"
                ),
            ))
        })
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
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
                // runner closes it when it records this batch. The read is bounded so it cannot
                // hold up the progress write; a failed or late read skips this batch's families,
                // is counted as a skip, and the next run sees the redo attempt it missed and
                // rebuilds.
                let token = match tokio::time::timeout(
                    self.families.token_budget,
                    bigname_project::families::input_token(&self.pool, &context.chain_id),
                )
                .await
                {
                    Ok(Ok(token)) => Ok(token),
                    Ok(Err(error)) => Err(format!("the input token did not read: {error}")),
                    Err(_) => Err(format!(
                        "the input token did not read within {:?}",
                        self.families.token_budget
                    )),
                };
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

fn runner_error(error: bigname_project::ProjectError) -> RunnerError {
    let kind = match error.kind() {
        ProjectErrorKind::Transient => ErrorKind::Transient,
        ProjectErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        ProjectErrorKind::Configuration => ErrorKind::Configuration,
    };
    RunnerError::new(kind, error.to_string())
}
