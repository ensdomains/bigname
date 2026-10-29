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
#[cfg(test)]
mod tests;

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
    /// Refuse to run on the hydrated chain without its hydration RPC URL. The supervised runner
    /// follows the head and needs it; the one-shot redo only undoes and replays, which never
    /// reads RPC, and leaves the refresh to the runner.
    pub require_hydration_url: bool,
    /// Most names an ENSv2 root admission change may compose again in one publication.
    pub max_admission_sweep_names: u64,
}

impl Default for FamilySettings {
    fn default() -> Self {
        Self {
            max_blocks_per_run: bigname_project::families::MAX_BLOCKS_PER_RUN,
            retry_family_failures: true,
            rebuild_ranges: RebuildRanges::BelowSafe,
            require_hydration_url: true,
            max_admission_sweep_names: bigname_project::families::MAX_ADMISSION_SWEEP_NAMES,
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
        let unstorable = is_unstorable_value(error);
        let retry =
            self.families.retry_family_failures && !unstorable && !is_sweep_cap_refusal(error);
        let kind = match error.kind() {
            ProjectErrorKind::Configuration => ErrorKind::Configuration,
            ProjectErrorKind::DataIntegrity if !retry => ErrorKind::DataIntegrity,
            ProjectErrorKind::Transient if unstorable => ErrorKind::DataIntegrity,
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

/// Whether `error` is Project's refusal of an admission sweep above the cap
/// (`crates/project/src/families/derived.rs`, `refuse_oversized_admission_sweep`). Every retry
/// would meet it again, so the supervised runner stops the chain on it once instead.
/// Temporary: the match on its wording is replaced by a typed refusal kind in the next Project
/// change that rotates the projection content hash. `tests/families_runner.rs` reaches the real
/// refusal, so a change of that wording fails there.
fn is_sweep_cap_refusal(error: &ProjectError) -> bool {
    let message = error.to_string();
    error.kind() == ProjectErrorKind::DataIntegrity
        && message.contains("admission differs from the family marker's")
        && message.contains("redo the full range of phase project alone")
}

/// Whether `error` is PostgreSQL refusing to store a value the block writes. The transaction
/// rolls back with that value still first in line, so every retry would meet it again at the same
/// head, and the supervised runner stops the chain on it once instead. The refusals are SQLSTATE
/// class 22 (data exception) and class 54 (program limit exceeded):
///
/// - a NUL in a string written to a `jsonb` value, 22P05, raised by `json_errsave_error` in
///   PostgreSQL's `src/backend/utils/adt/jsonfuncs.c`;
/// - a NUL or other invalid byte in a `text` value, 22021, raised by `report_invalid_encoding` in
///   `src/backend/utils/mb/mbutils.c`;
/// - a key longer than a btree index row may be, 54000, raised by `_bt_check_third_page` in
///   `src/backend/access/nbtree/nbtutils.c`;
/// - a key longer than any index row may be, 54000, raised by `index_form_tuple` in
///   `src/backend/access/common/indextuple.c`.
///
/// Temporary: `ProjectError` does not carry the SQLSTATE, so this matches PostgreSQL's English
/// messages, which assumes the server's `lc_messages` is English. Under another language nothing
/// matches and the error is retried as before. The SQLSTATE carried on `ProjectError` replaces
/// this match in the next Project change that rotates the projection content hash.
/// `tests/families_runner.rs` and `tests/production_live/hydration.rs` reach real refusals, so a
/// change of this wording fails there.
fn is_unstorable_value(error: &ProjectError) -> bool {
    let message = error.to_string();
    let Some((_, refusal)) = message.rsplit_once("error returned from database: ") else {
        return false;
    };
    refusal.starts_with("unsupported Unicode escape sequence")
        || refusal.starts_with("invalid byte sequence for encoding ")
        || (refusal.starts_with("index row size ") && refusal.contains(" exceeds btree version "))
        || (refusal.starts_with("index row requires ")
            && refusal.contains(" bytes, maximum size is "))
}

fn project_marker(marker: &BlockMarker) -> Marker {
    Marker {
        number: marker.number,
        hash: marker.hash.clone(),
    }
}
