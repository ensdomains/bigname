use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use anyhow::Result;
use bigname_manifests::{
    RequiredWatchedTuple, UncoveredWatchedTuple, WatchedContract,
    find_uncovered_required_watched_tuples,
    load_active_manifest_abi_events_by_chain_and_source_families,
    load_earliest_known_watched_block, load_historical_watched_contracts_by_chain,
    load_log_producing_source_families,
    load_required_watched_tuples as load_required_watched_tuples_from_db,
    load_watched_contracts_by_chain,
};
use bigname_storage::ChainLineageBlock;
use sqlx::Row;

#[path = "coverage/companions.rs"]
mod companions;
#[path = "coverage/frontier.rs"]
mod frontier;

use frontier::{Topic0sByFamily, VerifiedCoverageState};

/// Frontier extensions verify coverage in chunks of this many blocks, so a
/// deep gap costs a handful of coverage queries once and every promotion
/// cycle afterwards is an O(1) in-memory comparison.
pub(crate) const COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS: i64 = 131_072;
const MAX_REPORTED_UNCOVERED_TUPLES: i64 = 20;

/// Process-lifetime, per-chain memo of each watched tuple interval proved
/// against `backfill_coverage_facts`. Deliberately not persisted: a restart
/// re-verifies in chunked queries. Admission changes diff against the retained
/// tuple snapshot, while interior mutability lets extension progress survive
/// refusal and error paths within a poll cycle.
#[derive(Default)]
pub(crate) struct ChainCoverageFrontiers {
    verified: Mutex<BTreeMap<String, VerifiedCoverageState>>,
    promotion_epochs: Mutex<BTreeMap<String, i64>>,
    #[cfg(test)]
    required_tuple_range_scans: Mutex<BTreeMap<String, Vec<(i64, i64)>>>,
}

impl ChainCoverageFrontiers {
    fn verified_state(&self, chain: &str) -> Option<VerifiedCoverageState> {
        self.verified
            .lock()
            .expect("coverage frontier lock must not be poisoned")
            .get(chain)
            .cloned()
    }

    fn record(&self, chain: &str, state: VerifiedCoverageState) {
        self.verified
            .lock()
            .expect("coverage frontier lock must not be poisoned")
            .insert(chain.to_owned(), state);
    }

    fn reset(&self, chain: &str) {
        self.verified
            .lock()
            .expect("coverage frontier lock must not be poisoned")
            .remove(chain);
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
    pub(crate) fn install_admission_epoch_verification_test_hook(
        chain: &str,
    ) -> super::admission_epoch_fence::AdmissionEpochFenceTestHook {
        super::admission_epoch_fence::install_admission_epoch_verification_test_hook(chain)
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
) -> std::result::Result<(), String> {
    if path.is_empty() {
        return Ok(());
    }
    let path_from = path_start_number(path);
    let path_through = path_end_number(path);

    let same_height_fork_numbers = same_height_fork_lineage_numbers(pool, chain, path)
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
) -> std::result::Result<(), String> {
    let verify_ahead_through_block = verify_ahead_through_block.max(required_through);
    let mut state = coverage_frontiers.verified_state(chain);

    if state
        .as_ref()
        .is_some_and(|existing| required_from < existing.from_block)
    {
        // A deep reorg below the proved lower bound cannot reuse a partial
        // process-local snapshot. Cold-start verification below deliberately
        // rebuilds every requirement from the earliest known watch start.
        coverage_frontiers.reset(chain);
        state = None;
    }

    if state.is_none() {
        let earliest_known_watched_block = load_earliest_known_watched_block(
            pool,
            chain,
            verify_ahead_through_block,
            log_producing_source_families,
        )
        .await
        .map_err(|error| error.to_string())?;
        let verification_from = earliest_known_watched_block
            .map_or(required_from, |earliest| required_from.min(earliest));
        state = Some(VerifiedCoverageState::empty(
            verification_from,
            current_topic0s_by_family.clone(),
            discovery_admission_epoch,
        ));
    }

    let mut state = state.expect("coverage state must be initialized");
    if state.discovery_admission_epoch != discovery_admission_epoch
        || &state.topic0s_by_family != current_topic0s_by_family
    {
        // Refresh the exact interval snapshot from watched rows, not by
        // evaluating the required-tuples CTE over earliest-watch..frontier.
        // The latter makes a near-head admission or topic change pay for the
        // chain's entire block span even though only the interval diff is
        // verified. Current rows identify unbounded watches; finite historical
        // rows retain closed authority. Their union has cost independent of
        // the distance from the earliest watch to the stored anchor.
        let (snapshot_from, current_requirements) = load_interval_requirement_snapshot(
            pool,
            chain,
            state.from_block,
            state.through_block,
            log_producing_source_families,
        )
        .await?;
        let differential =
            state.differential_requirements(&current_requirements, current_topic0s_by_family);
        verify_requirements(pool, chain, current_topic0s_by_family, &differential).await?;
        state.replace_requirements(
            snapshot_from,
            &current_requirements,
            current_topic0s_by_family.clone(),
            discovery_admission_epoch,
        );
        coverage_frontiers.record(chain, state.clone());
    }

    if required_through <= state.through_block {
        return Ok(());
    }

    while state.through_block < required_through {
        let chunk_from = state.through_block.saturating_add(1);
        let chunk_through = chunk_from
            .saturating_add(COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS - 1)
            .min(verify_ahead_through_block);
        let verification = verify_range(
            pool,
            chain,
            coverage_frontiers,
            chunk_from,
            chunk_through,
            log_producing_source_families,
            current_topic0s_by_family,
        )
        .await;
        let (verified_through, requirements) = match verification {
            Ok(requirements) => (chunk_through, requirements),
            Err(_look_ahead_error) if chunk_through > required_through => {
                // Failure beyond the promotion target must not block a covered
                // prefix or memoize the unverified look-ahead suffix.
                let requirements = verify_range(
                    pool,
                    chain,
                    coverage_frontiers,
                    chunk_from,
                    required_through,
                    log_producing_source_families,
                    current_topic0s_by_family,
                )
                .await?;
                (required_through, requirements)
            }
            Err(error) => return Err(error),
        };
        state.extend_requirements(
            &requirements,
            verified_through,
            current_topic0s_by_family.clone(),
            discovery_admission_epoch,
        );
        coverage_frontiers.record(chain, state.clone());
    }

    Ok(())
}

/// Rebuild the current requirement snapshot from interval-bearing watch rows.
/// `load_watched_contracts_by_chain` supplies every currently active row,
/// including an unbounded interval, while the historical view supplies closed
/// rows only when they have a finite end. Together they mirror the coverage
/// CTE's authority rules without evaluating a block-number range.
async fn load_interval_requirement_snapshot(
    pool: &sqlx::PgPool,
    chain: &str,
    previous_from_block: i64,
    through_block: i64,
    log_producing_source_families: &[String],
) -> std::result::Result<(i64, Vec<RequiredWatchedTuple>), String> {
    let (mut current, historical) = tokio::try_join!(
        load_watched_contracts_by_chain(pool, chain),
        load_historical_watched_contracts_by_chain(pool, chain),
    )
    .map_err(|error| error.to_string())?;
    current.extend(
        historical
            .into_iter()
            .filter(|watched| watched.active_to_block_number.is_some()),
    );

    let log_producing_source_families = log_producing_source_families
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    current
        .retain(|watched| log_producing_source_families.contains(watched.source_family.as_str()));

    let snapshot_from = current
        .iter()
        .filter_map(|watched| watched.active_from_block_number)
        .filter(|from_block| *from_block <= through_block)
        .fold(previous_from_block, i64::min);
    let requirements = current
        .into_iter()
        .filter_map(|watched| requirement_in_range(watched, snapshot_from, through_block))
        .collect();
    Ok((snapshot_from, requirements))
}

fn requirement_in_range(
    watched: WatchedContract,
    from_block: i64,
    through_block: i64,
) -> Option<RequiredWatchedTuple> {
    let required_from_block = watched
        .active_from_block_number
        .unwrap_or(from_block)
        .max(from_block);
    let required_to_block = watched
        .active_to_block_number
        .unwrap_or(through_block)
        .min(through_block);
    (required_from_block <= required_to_block).then(|| RequiredWatchedTuple {
        source_family: watched.source_family,
        address: watched.address.to_ascii_lowercase(),
        required_from_block,
        required_to_block,
    })
}

async fn verify_range(
    pool: &sqlx::PgPool,
    chain: &str,
    coverage_frontiers: &ChainCoverageFrontiers,
    from_block: i64,
    through_block: i64,
    log_producing_source_families: &[String],
    current_topic0s_by_family: &Topic0sByFamily,
) -> std::result::Result<Vec<RequiredWatchedTuple>, String> {
    let requirements = load_required_watched_tuples(
        pool,
        chain,
        coverage_frontiers,
        from_block,
        through_block,
        log_producing_source_families,
    )
    .await?;
    verify_requirements(pool, chain, current_topic0s_by_family, &requirements).await?;
    Ok(requirements)
}

/// The only block-range requirement loader used by stored-lineage coverage.
/// Keeping it behind one wrapper makes the permanent DB regression observe
/// every such query and prevents an epoch refresh from silently reintroducing
/// an earliest-watch-to-frontier scan.
async fn load_required_watched_tuples(
    pool: &sqlx::PgPool,
    chain: &str,
    coverage_frontiers: &ChainCoverageFrontiers,
    from_block: i64,
    through_block: i64,
    log_producing_source_families: &[String],
) -> std::result::Result<Vec<RequiredWatchedTuple>, String> {
    coverage_frontiers.record_required_tuple_range_scan(chain, from_block, through_block);
    load_required_watched_tuples_from_db(
        pool,
        chain,
        from_block,
        through_block,
        log_producing_source_families,
    )
    .await
    .map_err(|error| error.to_string())
}

async fn verify_requirements(
    pool: &sqlx::PgPool,
    chain: &str,
    current_topic0s_by_family: &Topic0sByFamily,
    requirements: &[RequiredWatchedTuple],
) -> std::result::Result<(), String> {
    if requirements.is_empty() {
        return Ok(());
    }
    super::topic_drift::ensure_required_topic_sets_undrifted(
        pool,
        chain,
        current_topic0s_by_family,
        requirements,
    )
    .await?;
    let violations = find_uncovered_required_watched_tuples(
        pool,
        chain,
        requirements,
        MAX_REPORTED_UNCOVERED_TUPLES,
    )
    .await
    .map_err(|error| error.to_string())?;
    if violations.is_empty() {
        return Ok(());
    }
    let from_block = requirements
        .iter()
        .map(|requirement| requirement.required_from_block)
        .min()
        .expect("non-empty requirements must have a lower bound");
    let through_block = requirements
        .iter()
        .map(|requirement| requirement.required_to_block)
        .max()
        .expect("non-empty requirements must have an upper bound");
    Err(uncovered_tuples_refusal(
        from_block,
        through_block,
        &violations,
    ))
}

fn uncovered_tuples_refusal(
    from_block: i64,
    through_block: i64,
    violations: &[UncoveredWatchedTuple],
) -> String {
    let listed = violations
        .iter()
        .map(|tuple| {
            format!(
                "(source_family {}, address {}, blocks {}..={})",
                tuple.source_family,
                tuple.address,
                tuple.required_from_block,
                tuple.required_to_block
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if violations.len() as i64 >= MAX_REPORTED_UNCOVERED_TUPLES {
        " (further violations elided)"
    } else {
        ""
    };
    format!(
        "watched tuples over blocks {from_block}..={through_block} do not form gap-free coverage from exact address- or family-scoped backfill_coverage_facts: {listed}{suffix}; run hash-pinned or Coinbase SQL backfill for the missing tuple intervals (or repair derive-backfill-coverage-facts for legacy full-payload jobs) and retry"
    )
}

/// Current manifest topic selectors per log-producing family. The frontier
/// stores them per family so a semantic change invalidates only that family's
/// tuple proofs.
async fn load_current_topic0s_by_family(
    pool: &sqlx::PgPool,
    chain: &str,
    log_producing_source_families: &[String],
) -> std::result::Result<BTreeMap<String, BTreeSet<String>>, String> {
    if log_producing_source_families.is_empty() {
        return Ok(BTreeMap::new());
    }
    let events = load_active_manifest_abi_events_by_chain_and_source_families(
        pool,
        chain,
        log_producing_source_families,
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut current_topic0s_by_family = BTreeMap::<String, BTreeSet<String>>::new();
    for event in events {
        let Some(topic0) = event.topic0 else {
            continue;
        };
        current_topic0s_by_family
            .entry(event.source_family)
            .or_default()
            .insert(topic0.to_ascii_lowercase());
    }
    Ok(current_topic0s_by_family)
}

async fn same_height_fork_lineage_numbers(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
) -> Result<BTreeSet<i64>> {
    if path.is_empty() {
        return Ok(BTreeSet::new());
    }

    let block_numbers = path
        .iter()
        .map(|block| block.block_number)
        .collect::<Vec<_>>();
    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT block_number
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number = ANY($2::BIGINT[])
          AND NOT (block_hash = ANY($3::TEXT[]))
          AND canonicality_state <> 'orphaned'::canonicality_state
        "#,
    )
    .bind(chain)
    .bind(&block_numbers)
    .bind(&block_hashes)
    .fetch_all(pool)
    .await?;

    let mut numbers = BTreeSet::new();
    for row in rows {
        numbers.insert(row.try_get("block_number")?);
    }
    Ok(numbers)
}

fn path_start_number(path: &[ChainLineageBlock]) -> i64 {
    path.first()
        .expect("stored lineage path must not be empty")
        .block_number
}

fn path_end_number(path: &[ChainLineageBlock]) -> i64 {
    path.last()
        .expect("stored lineage path must not be empty")
        .block_number
}
