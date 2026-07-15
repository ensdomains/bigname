use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use anyhow::Result;
use bigname_manifests::{
    UncoveredWatchedTuple, WATCHED_COVERAGE_VERIFICATION_CHUNK_BLOCKS,
    find_uncovered_watched_tuples, load_active_manifest_topic0s_by_chain_and_source_families,
    load_discovery_admission_epoch, load_log_producing_source_families,
    load_watched_contracts_by_addresses,
};
use bigname_storage::{ChainLineageBlock, ensure_backfill_family_topic_sets_undrifted};
use sqlx::Row;

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
/// no path block may have a live same-height fork, and every stored log from a
/// watched address inside the path must carry its raw code/transaction/receipt
/// companions.
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

    ensure_verified_coverage_frontier(
        pool,
        chain,
        coverage_frontiers,
        path_from,
        path_through,
        verify_ahead_through_block,
    )
    .await?;

    ensure_selected_logs_have_raw_companions(pool, chain, path).await
}

/// Extend the chain's verified coverage frontier until it contains
/// `[required_from, required_through]`, verifying in
/// [`WATCHED_COVERAGE_VERIFICATION_CHUNK_BLOCKS`] chunks that opportunistically
/// look ahead as far as `verify_ahead_through_block` (the stored promotion
/// anchor) so subsequent cycles are O(1). A violation in the look-ahead beyond
/// the required target falls back to verifying exactly up to the target, so an
/// uncovered stretch above the target never blocks promoting a covered prefix.
async fn ensure_verified_coverage_frontier(
    pool: &sqlx::PgPool,
    chain: &str,
    coverage_frontiers: &ChainCoverageFrontiers,
    required_from: i64,
    required_through: i64,
    verify_ahead_through_block: i64,
) -> std::result::Result<(), String> {
    let log_producing_source_families = load_log_producing_source_families(pool, chain)
        .await
        .map_err(|error| error.to_string())?;
    let current_topic0s_by_family = load_active_manifest_topic0s_by_chain_and_source_families(
        pool,
        chain,
        &log_producing_source_families,
    )
    .await
    .map_err(|error| error.to_string())?;
    let topic_set_fingerprint = topic_set_fingerprint(&current_topic0s_by_family);
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
    if let Err(_look_ahead_drift) = ensure_backfill_family_topic_sets_undrifted(
        pool,
        chain,
        &current_topic0s_by_family,
        extension_from,
        verify_ahead_through_block,
    )
    .await
    {
        // A drifted job intersecting only blocks above the promotion target
        // must not block promoting the covered prefix: recheck scoped to the
        // target, and if that passes, cap the look-ahead so the memo never
        // covers a span the drift guard did not clear.
        ensure_backfill_family_topic_sets_undrifted(
            pool,
            chain,
            &current_topic0s_by_family,
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
            .saturating_add(WATCHED_COVERAGE_VERIFICATION_CHUNK_BLOCKS - 1)
            .min(verify_ahead_through_block);
        let violations = find_uncovered_watched_tuples(
            pool,
            chain,
            chunk_from,
            chunk_through,
            &log_producing_source_families,
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
                &log_producing_source_families,
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

/// Every stored canonical log emitted by a watched address inside the path
/// must carry its raw code-hash, transaction, and receipt companions. Scoped
/// to the addresses that actually appear in the path's logs, so binds stay
/// proportional to the path, not the watch set.
async fn ensure_selected_logs_have_raw_companions(
    pool: &sqlx::PgPool,
    chain: &str,
    path: &[ChainLineageBlock],
) -> std::result::Result<(), String> {
    let block_hashes = path
        .iter()
        .map(|block| block.block_hash.clone())
        .collect::<Vec<_>>();
    let emitting_addresses = sqlx::query_scalar::<_, String>(
        r#"
        SELECT DISTINCT LOWER(emitting_address)
        FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = ANY($2::TEXT[])
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(&block_hashes)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    if emitting_addresses.is_empty() {
        return Ok(());
    }

    let watched_targets = emitting_addresses
        .iter()
        .map(|address| (chain.to_owned(), address.clone()))
        .collect::<Vec<_>>();
    let selected_addresses = load_watched_contracts_by_addresses(pool, &watched_targets)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|contract| contract.address.to_ascii_lowercase())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if selected_addresses.is_empty() {
        return Ok(());
    }

    let row = sqlx::query(
        r#"
        WITH selected_log_emitters AS (
            SELECT DISTINCT
                raw_logs.block_hash,
                LOWER(raw_logs.emitting_address) AS emitting_address
            FROM raw_logs
            WHERE raw_logs.chain_id = $1
              AND raw_logs.block_hash = ANY($2::TEXT[])
              AND LOWER(raw_logs.emitting_address) = ANY($3::TEXT[])
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        ),
        selected_log_transactions AS (
            SELECT DISTINCT
                raw_logs.block_hash,
                raw_logs.transaction_hash,
                raw_logs.transaction_index
            FROM raw_logs
            WHERE raw_logs.chain_id = $1
              AND raw_logs.block_hash = ANY($2::TEXT[])
              AND LOWER(raw_logs.emitting_address) = ANY($3::TEXT[])
              AND raw_logs.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
        )
        SELECT
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_emitters
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM raw_code_hashes
                    WHERE raw_code_hashes.chain_id = $1
                      AND raw_code_hashes.block_hash = selected_log_emitters.block_hash
                      AND LOWER(raw_code_hashes.contract_address) = selected_log_emitters.emitting_address
                      AND raw_code_hashes.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                )
            ) AS selected_log_emitter_missing_code_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_transactions
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM raw_transactions
                    WHERE raw_transactions.chain_id = $1
                      AND raw_transactions.block_hash = selected_log_transactions.block_hash
                      AND raw_transactions.transaction_hash = selected_log_transactions.transaction_hash
                      AND raw_transactions.transaction_index = selected_log_transactions.transaction_index
                      AND raw_transactions.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                )
            ) AS selected_log_transaction_missing_count,
            (
                SELECT COUNT(*)::BIGINT
                FROM selected_log_transactions
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM raw_receipts
                    WHERE raw_receipts.chain_id = $1
                      AND raw_receipts.block_hash = selected_log_transactions.block_hash
                      AND raw_receipts.transaction_hash = selected_log_transactions.transaction_hash
                      AND raw_receipts.transaction_index = selected_log_transactions.transaction_index
                      AND raw_receipts.canonicality_state IN (
                          'canonical'::canonicality_state,
                          'safe'::canonicality_state,
                          'finalized'::canonicality_state
                      )
                )
            ) AS selected_log_receipt_missing_count
        "#,
    )
    .bind(chain)
    .bind(&block_hashes)
    .bind(&selected_addresses)
    .fetch_one(pool)
    .await
    .map_err(|error| error.to_string())?;

    let missing_code_hashes: i64 = row
        .try_get("selected_log_emitter_missing_code_count")
        .map_err(|error| error.to_string())?;
    let missing_transactions: i64 = row
        .try_get("selected_log_transaction_missing_count")
        .map_err(|error| error.to_string())?;
    let missing_receipts: i64 = row
        .try_get("selected_log_receipt_missing_count")
        .map_err(|error| error.to_string())?;
    if missing_code_hashes != 0 || missing_transactions != 0 || missing_receipts != 0 {
        return Err(format!(
            "stored lineage selected logs over {}..={} are missing raw code/transaction/receipt companion rows (missing code: {missing_code_hashes}, transactions: {missing_transactions}, receipts: {missing_receipts}); rerun hash-pinned backfill for the selected range before retrying",
            path_start_number(path),
            path_end_number(path)
        ));
    }

    Ok(())
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
