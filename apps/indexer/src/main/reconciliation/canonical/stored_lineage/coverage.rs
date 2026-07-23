use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use anyhow::Result;
use bigname_adapters::StartupAdapterProgress;
use bigname_manifests::{
    ManifestRuntimeProgress, ManifestRuntimeProgressFuture, RequiredWatchedTuple,
    UncoveredWatchedTuple, find_uncovered_required_watched_tuples_in_transaction,
    load_active_manifest_abi_events_by_chain_and_source_families,
    load_earliest_known_watched_block, load_earliest_known_watched_block_with_progress,
    load_log_producing_source_families, load_stored_lineage_coverage_candidate_delta_page,
    materialize_stored_lineage_coverage_candidate,
    materialize_stored_lineage_coverage_candidate_delta_with_progress,
    materialize_stored_lineage_coverage_candidate_with_progress,
};
use bigname_storage::{
    ChainLineageBlock, StoredLineageCoverageFrontierPublication, StoredLineageCoverageProgress,
    StoredLineageCoverageProgressFuture, StoredLineageCoveragePublicationOutcome,
    begin_stored_lineage_coverage_frontier_publication,
    load_stored_lineage_coverage_frontier_header,
    stored_lineage_coverage_frontier_requirements_are_valid,
    stored_lineage_coverage_frontier_requirements_are_valid_with_progress,
};
#[path = "coverage/companions.rs"]
mod companions;
#[path = "coverage/frontier.rs"]
mod frontier;
#[path = "coverage/lineage_forks.rs"]
mod lineage_forks;
#[path = "coverage/verification.rs"]
mod verification;

use frontier::Topic0sByFamily;
use verification::*;

struct AdapterManifestProgress<'a>(&'a mut dyn StartupAdapterProgress);

impl ManifestRuntimeProgress for AdapterManifestProgress<'_> {
    fn record<'a>(&'a mut self, pool: &'a sqlx::PgPool) -> ManifestRuntimeProgressFuture<'a> {
        self.0.record(pool)
    }
}

struct AdapterStorageProgress<'a> {
    pool: &'a sqlx::PgPool,
    progress: &'a mut dyn StartupAdapterProgress,
}

impl StoredLineageCoverageProgress for AdapterStorageProgress<'_> {
    fn record<'a>(&'a mut self) -> StoredLineageCoverageProgressFuture<'a> {
        self.progress.record(self.pool)
    }
}

/// Frontier extensions verify coverage in chunks of this many blocks, so a
/// deep gap costs a handful of coverage queries once and every promotion
/// cycle afterwards reuses a durable snapshot.
pub(crate) const COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS: i64 = 131_072;
const MAX_REPORTED_UNCOVERED_TUPLES: i64 = 20;
const COVERAGE_DELTA_PAGE_SIZE: i64 = 256;
const MAX_CAS_CONFLICT_RETRIES: usize = 2;
const MAX_PUBLICATIONS_PER_PROMOTION: usize = 10_000;

/// Process-local bookkeeping around the durable frontier. The revision cache
/// only avoids rescanning child-row shape on every poll; it is never coverage
/// authority and a new process validates the saved revision before reuse.
#[derive(Default)]
pub(crate) struct ChainCoverageFrontiers {
    validated_snapshot_revisions: Mutex<BTreeMap<String, i64>>,
    promotion_epochs: Mutex<BTreeMap<String, i64>>,
    raw_code_baseline_frontiers: Mutex<BTreeMap<String, RawCodeBaselineFrontier>>,
    #[cfg(test)]
    required_tuple_range_scans: Mutex<BTreeMap<String, Vec<(i64, i64)>>>,
}

/// Process-lifetime progress of the per-chain raw-code baseline sweep: the
/// watched surface is verified in sorted-address chunks, at most a capped
/// number of addresses per poll tick, with each chunk's observations upserted
/// before the cursor advances. `completed_admission_epoch` records the epoch
/// under which the swept plan was loaded; applying a plan with a later epoch
/// starts a fresh sweep so newly watched addresses are eventually baselined
/// without ever re-arming a whole-surface fetch inside one tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RawCodeBaselineFrontier {
    /// Discovery admission epoch the in-progress sweep started under.
    pub(crate) sweep_admission_epoch: Option<i64>,
    /// Last watched address (sorted order) verified by the in-progress sweep.
    pub(crate) verified_through_address: Option<String>,
    /// Admission epoch the last finished sweep started under; a moved epoch
    /// starts a fresh sweep.
    pub(crate) completed_admission_epoch: Option<i64>,
}

impl ChainCoverageFrontiers {
    fn snapshot_revision_is_validated(&self, chain: &str, revision: i64) -> bool {
        self.validated_snapshot_revisions
            .lock()
            .expect("coverage revision lock must not be poisoned")
            .get(chain)
            .is_some_and(|validated| *validated == revision)
    }

    fn record_validated_snapshot_revision(&self, chain: &str, revision: i64) {
        self.validated_snapshot_revisions
            .lock()
            .expect("coverage revision lock must not be poisoned")
            .insert(chain.to_owned(), revision);
    }

    pub(super) fn record_promotion_epoch(&self, chain: &str, epoch: i64) {
        self.promotion_epochs
            .lock()
            .expect("promotion epoch lock must not be poisoned")
            .insert(chain.to_owned(), epoch);
    }

    pub(crate) fn take_promotion_epoch(&self, chain: &str, required: bool) -> Result<Option<i64>> {
        let epoch = self
            .promotion_epochs
            .lock()
            .expect("promotion epoch lock must not be poisoned")
            .remove(chain);
        if required && epoch.is_none() {
            anyhow::bail!(
                "stored-lineage promotion for chain {chain} is missing its verified discovery admission epoch"
            );
        }
        Ok(epoch)
    }

    pub(crate) fn raw_code_baseline_frontier(&self, chain: &str) -> RawCodeBaselineFrontier {
        self.raw_code_baseline_frontiers
            .lock()
            .expect("raw code baseline frontier lock must not be poisoned")
            .get(chain)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn store_raw_code_baseline_frontier(
        &self,
        chain: &str,
        frontier: RawCodeBaselineFrontier,
    ) {
        self.raw_code_baseline_frontiers
            .lock()
            .expect("raw code baseline frontier lock must not be poisoned")
            .insert(chain.to_owned(), frontier);
    }

    pub(crate) fn invalidate_raw_code_baseline_frontier(&self, chain: &str) {
        self.raw_code_baseline_frontiers
            .lock()
            .expect("raw code baseline frontier lock must not be poisoned")
            .remove(chain);
    }

    fn record_required_tuple_range_scan(&self, chain: &str, from_block: i64, through_block: i64) {
        #[cfg(test)]
        self.required_tuple_range_scans
            .lock()
            .expect("required tuple range scan lock must not be poisoned")
            .entry(chain.to_owned())
            .or_default()
            .push((from_block, through_block));

        #[cfg(not(test))]
        let _ = (chain, from_block, through_block);
    }
}

#[cfg(test)]
impl ChainCoverageFrontiers {
    pub(crate) async fn install_admission_epoch_verification_test_hook(
        pool: &sqlx::PgPool,
        chain: &str,
    ) -> super::admission_epoch_fence::AdmissionEpochFenceTestHook {
        super::admission_epoch_fence::install_admission_epoch_verification_test_hook(pool, chain)
            .await
    }

    pub(crate) fn take_required_tuple_range_scans_for_tests(&self, chain: &str) -> Vec<(i64, i64)> {
        self.required_tuple_range_scans
            .lock()
            .expect("required tuple range scan lock must not be poisoned")
            .remove(chain)
            .unwrap_or_default()
    }
}

/// Fail-closed coverage gate for a stored-lineage promotion path: every
/// watched log-producing tuple active over the path must have proven fetch
/// coverage in `backfill_coverage_facts` (via the chunked verified frontier),
/// no path block may have a live same-height fork, and every family-selected
/// stored log inside the path (watched emitter, in-window, family topic0)
/// must carry its raw code/transaction/receipt companions.
pub(super) async fn stored_path_has_required_raw_fact_coverage(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
    coverage_frontiers: &ChainCoverageFrontiers,
    verify_ahead_through_block: i64,
    discovery_admission_epoch: i64,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> std::result::Result<(), String> {
    if path.is_empty() {
        return Ok(());
    }
    let path_from = path_start_number(path);
    let path_through = path_end_number(path);

    let same_height_fork_numbers =
        lineage_forks::same_height_fork_lineage_numbers(pool, chain, path)
            .await
            .map_err(|error| error.to_string())?;
    if let Some(fork_block) = path
        .iter()
        .find(|block| same_height_fork_numbers.contains(&block.block_number))
    {
        return Err(format!(
            "stored lineage path over blocks {path_from}..={path_through} has a non-orphaned same-height fork at block {} ({}); repair the losing branch to orphaned before retrying",
            fork_block.block_number, fork_block.block_hash
        ));
    }

    let log_producing_source_families = load_log_producing_source_families(pool, chain)
        .await
        .map_err(|error| error.to_string())?;
    let current_topic0s_by_family =
        load_current_topic0s_by_family(pool, chain, &log_producing_source_families).await?;

    ensure_verified_coverage_frontier(
        pool,
        chain,
        coverage_frontiers,
        &log_producing_source_families,
        &current_topic0s_by_family,
        path_from,
        path_through,
        verify_ahead_through_block,
        discovery_admission_epoch,
        progress,
    )
    .await?;

    companions::ensure_selected_logs_have_raw_companions(
        pool,
        chain,
        path,
        &current_topic0s_by_family,
    )
    .await
}

/// Extend the chain's verified coverage frontier until it contains
/// `[required_from, required_through]`, verifying in
/// [`COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS`] chunks that opportunistically
/// look ahead as far as `verify_ahead_through_block` (the stored promotion
/// anchor) so subsequent cycles are O(1). A violation in the look-ahead beyond
/// the required target falls back to verifying exactly up to the target, so an
/// uncovered stretch above the target never blocks promoting a covered prefix.
#[expect(
    clippy::too_many_arguments,
    reason = "the coverage proof boundary keeps manifest, range, and admission inputs explicit"
)]
async fn ensure_verified_coverage_frontier(
    pool: &sqlx::PgPool,
    chain: &str,
    coverage_frontiers: &ChainCoverageFrontiers,
    log_producing_source_families: &[String],
    current_topic0s_by_family: &Topic0sByFamily,
    required_from: i64,
    required_through: i64,
    verify_ahead_through_block: i64,
    discovery_admission_epoch: i64,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> std::result::Result<(), String> {
    let verify_ahead_through_block = verify_ahead_through_block.max(required_through);
    let optimistic = ensure_verified_coverage_frontier_through(
        pool,
        chain,
        coverage_frontiers,
        log_producing_source_families,
        current_topic0s_by_family,
        required_from,
        required_through,
        verify_ahead_through_block,
        discovery_admission_epoch,
        progress,
    )
    .await;
    match optimistic {
        Ok(()) => Ok(()),
        Err(_look_ahead_error) if verify_ahead_through_block > required_through => {
            ensure_verified_coverage_frontier_through(
                pool,
                chain,
                coverage_frontiers,
                log_producing_source_families,
                current_topic0s_by_family,
                required_from,
                required_through,
                required_through,
                discovery_admission_epoch,
                progress,
            )
            .await
        }
        Err(error) => Err(error),
    }
}

#[expect(clippy::too_many_arguments, reason = "proof inputs stay explicit")]
async fn ensure_verified_coverage_frontier_through(
    pool: &sqlx::PgPool,
    chain: &str,
    coverage_frontiers: &ChainCoverageFrontiers,
    log_producing_source_families: &[String],
    current_topic0s_by_family: &Topic0sByFamily,
    required_from: i64,
    required_through: i64,
    verify_ahead_through_block: i64,
    discovery_admission_epoch: i64,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> std::result::Result<(), String> {
    let current_topics = frontier::canonical_topic_sets(current_topic0s_by_family);
    let mut cas_conflicts = 0;

    for _ in 0..MAX_PUBLICATIONS_PER_PROMOTION {
        let header = load_stored_lineage_coverage_frontier_header(pool, chain)
            .await
            .map_err(|error| error.to_string())?;
        let persisted_requirements_valid = match &header {
            Some(header)
                if header.is_well_formed
                    && coverage_frontiers
                        .snapshot_revision_is_validated(chain, header.snapshot_revision) =>
            {
                true
            }
            Some(header) => match progress.as_deref_mut() {
                Some(progress) => {
                    let mut bridge = AdapterStorageProgress { pool, progress };
                    stored_lineage_coverage_frontier_requirements_are_valid_with_progress(
                        pool,
                        header,
                        &mut bridge,
                    )
                    .await
                }
                None => stored_lineage_coverage_frontier_requirements_are_valid(pool, header).await,
            }
            .map_err(|error| error.to_string())?,
            None => true,
        };
        let earliest_known_watched_block = match progress.as_deref_mut() {
            Some(progress) => {
                let mut bridge = AdapterManifestProgress(progress);
                load_earliest_known_watched_block_with_progress(
                    pool,
                    chain,
                    verify_ahead_through_block,
                    log_producing_source_families,
                    &mut bridge,
                )
                .await
            }
            None => {
                load_earliest_known_watched_block(
                    pool,
                    chain,
                    verify_ahead_through_block,
                    log_producing_source_families,
                )
                .await
            }
        }
        .map_err(|error| error.to_string())?;
        record_progress(pool, progress).await?;
        let Some(plan) = frontier::plan_publication(
            header.as_ref(),
            persisted_requirements_valid,
            &current_topics,
            discovery_admission_epoch,
            earliest_known_watched_block,
            required_from,
            required_through,
            verify_ahead_through_block,
            COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS,
        )?
        else {
            if let Some(header) = header {
                coverage_frontiers
                    .record_validated_snapshot_revision(chain, header.snapshot_revision);
            }
            return Ok(());
        };

        if let Some(header) = &header
            && plan.verified_through_block > header.verified_through_block
        {
            coverage_frontiers.record_required_tuple_range_scan(
                chain,
                header.verified_through_block.saturating_add(1),
                plan.verified_through_block,
            );
        } else if header.is_none() {
            coverage_frontiers.record_required_tuple_range_scan(
                chain,
                plan.verified_from_block,
                plan.verified_through_block,
            );
        }

        let mut guard = begin_stored_lineage_coverage_frontier_publication(
            pool,
            chain,
            plan.expected_snapshot_revision,
            discovery_admission_epoch,
        )
        .await
        .map_err(|error| error.to_string())?;
        match progress.as_deref_mut() {
            Some(progress) => {
                let mut bridge = AdapterManifestProgress(progress);
                materialize_stored_lineage_coverage_candidate_with_progress(
                    guard.connection_mut(),
                    pool,
                    chain,
                    plan.verified_from_block,
                    plan.verified_through_block,
                    log_producing_source_families,
                    &mut bridge,
                )
                .await
            }
            None => {
                materialize_stored_lineage_coverage_candidate(
                    guard.connection_mut(),
                    chain,
                    plan.verified_from_block,
                    plan.verified_through_block,
                    log_producing_source_families,
                )
                .await
            }
        }
        .map_err(|error| error.to_string())?;
        super::topic_drift::materialize_topic_evidence_in_transaction(
            guard.connection_mut(),
            chain,
            current_topic0s_by_family,
            plan.verified_from_block,
            plan.verified_through_block,
            None,
        )
        .await?;
        record_progress(pool, progress).await?;

        if let Some(progress) = progress.as_deref_mut() {
            let mut bridge = AdapterManifestProgress(progress);
            materialize_stored_lineage_coverage_candidate_delta_with_progress(
                guard.connection_mut(),
                pool,
                chain,
                &plan.topic_changed_source_families,
                plan.reverify_all,
                &mut bridge,
            )
            .await
            .map_err(|error| error.to_string())?;
        }

        let mut cursor = None;
        loop {
            let page = load_stored_lineage_coverage_candidate_delta_page(
                guard.connection_mut(),
                chain,
                &plan.topic_changed_source_families,
                plan.reverify_all,
                cursor.as_ref(),
                COVERAGE_DELTA_PAGE_SIZE,
            )
            .await
            .map_err(|error| error.to_string())?;
            verify_requirements(guard.connection_mut(), chain, &page.requirements).await?;
            record_progress(pool, progress).await?;
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        record_progress(pool, progress).await?;

        let publication = StoredLineageCoverageFrontierPublication {
            discovery_admission_epoch,
            verified_from_block: plan.verified_from_block,
            verified_through_block: plan.verified_through_block,
            topic0s_by_family: current_topics.clone(),
        };
        let publication_outcome = match progress.as_deref_mut() {
            Some(progress) => {
                let mut bridge = AdapterStorageProgress { pool, progress };
                guard.publish_with_progress(&publication, &mut bridge).await
            }
            None => guard.publish(&publication).await,
        }
        .map_err(|error| error.to_string())?;
        match publication_outcome {
            StoredLineageCoveragePublicationOutcome::Published { snapshot_revision } => {
                cas_conflicts = 0;
                coverage_frontiers.record_validated_snapshot_revision(chain, snapshot_revision);
            }
            StoredLineageCoveragePublicationOutcome::Conflict => {
                cas_conflicts += 1;
                if cas_conflicts > MAX_CAS_CONFLICT_RETRIES {
                    return Err(format!(
                        "stored-lineage coverage frontier for chain {chain} changed during {} consecutive compare-and-set publication attempts; refusing this promotion until a fresh candidate is reverified",
                        cas_conflicts
                    ));
                }
            }
        }
    }

    Err(format!(
        "stored-lineage coverage frontier for chain {chain} could not reach required block {required_through} within {MAX_PUBLICATIONS_PER_PROMOTION} bounded publications"
    ))
}
