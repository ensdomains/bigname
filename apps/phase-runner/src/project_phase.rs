use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use bigname_project::{
    ErrorKind as ProjectErrorKind, Marker, ProjectError,
    families::{FamilyOutcome, RebuildRanges},
};
use sqlx::PgPool;

mod family_batch;

use crate::{
    error::{ErrorKind, RunnerError},
    heads::BlockMarker,
    metrics::RunnerMetricsFeed,
    phase::{Phase, PhaseContext, PhaseFuture, PhaseName},
};

/// Bounds and retry policy for Project's family publication loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FamilySettings {
    /// Blocks applied or undone before returning progress to the runner.
    pub max_blocks_per_run: u64,
    /// Supervised execution retries family failures; one-shot redo preserves the failure kind.
    pub retry_family_failures: bool,
    /// Rebuild blocks eligible for bounded range transactions.
    pub rebuild_ranges: RebuildRanges,
}

impl Default for FamilySettings {
    fn default() -> Self {
        Self {
            max_blocks_per_run: bigname_project::families::MAX_BLOCKS_PER_RUN,
            retry_family_failures: true,
            rebuild_ranges: RebuildRanges::BelowSafe,
        }
    }
}

pub struct ProjectPhase {
    pool: PgPool,
    hydration_rpc_urls: Option<bigname_lookup::ChainRpcUrls>,
    metrics_feed: Option<RunnerMetricsFeed>,
    families: FamilySettings,
    /// A budgeted family batch resumes after the runner records its progress.
    family_continuations: Arc<Mutex<BTreeMap<String, family_batch::Continuation>>>,
}

impl ProjectPhase {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            hydration_rpc_urls: None,
            metrics_feed: None,
            families: FamilySettings::default(),
            family_continuations: Arc::default(),
        }
    }

    pub fn with_hydration(pool: PgPool, rpc_urls: bigname_lookup::ChainRpcUrls) -> Self {
        Self {
            hydration_rpc_urls: Some(rpc_urls),
            ..Self::new(pool)
        }
    }

    pub fn with_family_settings(mut self, settings: FamilySettings) -> Self {
        self.families = settings;
        self
    }

    pub fn with_metrics_feed(mut self, feed: RunnerMetricsFeed) -> Self {
        self.metrics_feed = Some(feed);
        self
    }

    pub fn with_metrics(self, feed: RunnerMetricsFeed) -> Self {
        self.with_metrics_feed(feed)
    }

    fn report_families(&self, chain_id: &str, outcome: &FamilyOutcome) {
        if let Some(feed) = &self.metrics_feed {
            feed.project_families(chain_id, outcome);
        }
    }

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
}

impl Phase for ProjectPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Project
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(self.run_family_batch(context))
    }
}

fn project_marker(marker: &BlockMarker) -> Marker {
    Marker {
        number: marker.number,
        hash: marker.hash.clone(),
    }
}
