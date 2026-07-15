use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use anyhow::Result;
use bigname_manifests::{
    UncoveredWatchedTuple, find_uncovered_watched_tuples,
    load_active_manifest_abi_events_by_chain_and_source_families, load_discovery_admission_epoch,
    load_log_producing_source_families,
};
use bigname_storage::ChainLineageBlock;
use sqlx::Row;

#[path = "coverage/companions.rs"]
mod companions;
#[path = "coverage/topic_drift.rs"]
mod topic_drift;

use companions::ensure_selected_logs_have_raw_companions;
use topic_drift::ensure_family_topic_sets_undrifted;

/// Frontier extensions verify coverage in chunks of this many blocks, so a
/// deep gap costs a handful of anti-join queries once and every promotion
/// cycle afterwards is an O(1) in-memory comparison.
pub(crate) const COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS: i64 = 131_072;
const MAX_REPORTED_UNCOVERED_TUPLES: i64 = 20;

/// Process-lifetime, per-chain memo of the block interval whose watched-tuple
/// coverage has been proven against `backfill_coverage_facts`. Deliberately
/// not persisted: a restart re-verifies in a handful of chunked queries.
/// Interior mutability lets verification progress survive refusal and error
/// paths within a poll cycle.
#[derive(Debug, Default)]
pub(crate) struct ChainCoverageFrontiers {
    verified: Mutex<BTreeMap<String, VerifiedCoverageInterval>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedCoverageInterval {
    from_block: i64,
    through_block: i64,
    /// Fingerprint of the manifest topic0 sets the interval was verified
    /// under; an ABI reload mid-process invalidates the memo.
    topic_set_fingerprint: String,
    /// Discovery admission epoch the interval was verified under. Every
    /// discovery_edges mutation and manifest-sync watched-surface change
    /// bumps the chain's epoch in the same transaction (in this or any other
    /// process), so watch-set growth always forces re-verification.
    discovery_admission_epoch: i64,
}

impl ChainCoverageFrontiers {
    fn verified_interval(&self, chain: &str) -> Option<VerifiedCoverageInterval> {
        self.verified
            .lock()
            .expect("coverage frontier lock must not be poisoned")
            .get(chain)
            .cloned()
    }

    fn record(&self, chain: &str, interval: VerifiedCoverageInterval) {
        self.verified
            .lock()
            .expect("coverage frontier lock must not be poisoned")
            .insert(chain.to_owned(), interval);
    }

    fn reset(&self, chain: &str) {
        self.verified
            .lock()
            .expect("coverage frontier lock must not be poisoned")
            .remove(chain);
    }
}

#[cfg(test)]
impl ChainCoverageFrontiers {
    pub(crate) fn record_verified_for_tests(
        &self,
        chain: &str,
        from_block: i64,
        through_block: i64,
        topic_set_fingerprint: &str,
        discovery_admission_epoch: i64,
    ) {
        self.record(
            chain,
            VerifiedCoverageInterval {
                from_block,
                through_block,
                topic_set_fingerprint: topic_set_fingerprint.to_owned(),
                discovery_admission_epoch,
            },
        );
    }

    pub(crate) fn verified_through_for_tests(&self, chain: &str) -> Option<i64> {
        self.verified_interval(chain)
            .map(|interval| interval.through_block)
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
    )
    .await?;

    ensure_selected_logs_have_raw_companions(pool, chain, path, &current_topic0s_by_family).await
}

/// Extend the chain's verified coverage frontier until it contains
/// `[required_from, required_through]`, verifying in
/// [`COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS`] chunks that opportunistically
/// look ahead as far as `verify_ahead_through_block` (the stored promotion
/// anchor) so subsequent cycles are O(1). A violation in the look-ahead beyond
/// the required target falls back to verifying exactly up to the target, so an
/// uncovered stretch above the target never blocks promoting a covered prefix.
#[expect(clippy::too_many_arguments)]
async fn ensure_verified_coverage_frontier(
    pool: &sqlx::PgPool,
    chain: &str,
    coverage_frontiers: &ChainCoverageFrontiers,
    log_producing_source_families: &[String],
    current_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>,
    required_from: i64,
    required_through: i64,
    verify_ahead_through_block: i64,
) -> std::result::Result<(), String> {
    let topic_set_fingerprint = topic_set_fingerprint(current_topic0s_by_family);
    let discovery_admission_epoch = load_discovery_admission_epoch(pool, chain)
        .await
        .map_err(|error| error.to_string())?;

    let mut interval = coverage_frontiers.verified_interval(chain);
    if let Some(existing) = &interval
        && (required_from < existing.from_block
            || existing.topic_set_fingerprint != topic_set_fingerprint
            || existing.discovery_admission_epoch != discovery_admission_epoch)
    {
        // The checkpoint regressed below the verified interval (deep reorg),
        // the manifest ABI event sets changed, or the discovery graph mutated
        // since verification (any process); start over rather than trusting a
        // stale memo.
        coverage_frontiers.reset(chain);
        interval = None;
    }
    if let Some(existing) = &interval
        && required_through <= existing.through_block
    {
        return Ok(());
    }

    let mut verify_ahead_through_block = verify_ahead_through_block.max(required_through);
    let extension_from = interval
        .as_ref()
        .map_or(required_from, |existing| existing.through_block + 1);
    if let Err(_look_ahead_drift) = ensure_family_topic_sets_undrifted(
        pool,
        chain,
        current_topic0s_by_family,
        extension_from,
        verify_ahead_through_block,
    )
    .await
    {
        // A drifted job intersecting only blocks above the promotion target
        // must not block promoting the covered prefix: recheck scoped to the
        // target, and if that passes, cap the look-ahead so the memo never
        // covers a span the drift guard did not clear.
        ensure_family_topic_sets_undrifted(
            pool,
            chain,
            current_topic0s_by_family,
            extension_from,
            required_through,
        )
        .await?;
        verify_ahead_through_block = required_through;
    }

    let from_block = interval
        .as_ref()
        .map_or(required_from, |existing| existing.from_block);
    let mut through_block = interval
        .as_ref()
        .map_or(required_from - 1, |existing| existing.through_block);
    while through_block < required_through {
        let chunk_from = through_block + 1;
        let chunk_through = chunk_from
            .saturating_add(COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS - 1)
            .min(verify_ahead_through_block);
        let violations = find_uncovered_watched_tuples(
            pool,
            chain,
            chunk_from,
            chunk_through,
            log_producing_source_families,
            MAX_REPORTED_UNCOVERED_TUPLES,
        )
        .await
        .map_err(|error| error.to_string())?;
        if violations.is_empty() {
            through_block = chunk_through;
            coverage_frontiers.record(
                chain,
                VerifiedCoverageInterval {
                    from_block,
                    through_block,
                    topic_set_fingerprint: topic_set_fingerprint.clone(),
                    discovery_admission_epoch,
                },
            );
            continue;
        }

        if chunk_through > required_through {
            let exact_violations = find_uncovered_watched_tuples(
                pool,
                chain,
                chunk_from,
                required_through,
                log_producing_source_families,
                MAX_REPORTED_UNCOVERED_TUPLES,
            )
            .await
            .map_err(|error| error.to_string())?;
            if exact_violations.is_empty() {
                through_block = required_through;
                coverage_frontiers.record(
                    chain,
                    VerifiedCoverageInterval {
                        from_block,
                        through_block,
                        topic_set_fingerprint: topic_set_fingerprint.clone(),
                        discovery_admission_epoch,
                    },
                );
                continue;
            }
            return Err(uncovered_tuples_refusal(
                chunk_from,
                required_through,
                &exact_violations,
            ));
        }
        return Err(uncovered_tuples_refusal(
            chunk_from,
            chunk_through,
            &violations,
        ));
    }

    Ok(())
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
        "watched tuples over blocks {from_block}..={through_block} have no single backfill_coverage_facts row containing their required interval: {listed}{suffix}; run hash-pinned or Coinbase SQL backfill for those tuples (or repair derive-backfill-coverage-facts for legacy full-payload jobs) and retry"
    )
}

/// Current manifest topic0 sets per log-producing family; also the input to
/// the frontier memo's ABI fingerprint.
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

fn topic_set_fingerprint(current_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>) -> String {
    let mut fingerprint = String::new();
    for (source_family, topic0s) in current_topic0s_by_family {
        fingerprint.push_str(source_family);
        fingerprint.push('=');
        for topic0 in topic0s {
            fingerprint.push_str(topic0);
            fingerprint.push(',');
        }
        fingerprint.push(';');
    }
    fingerprint
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
