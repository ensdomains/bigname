use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use anyhow::Result;
use bigname_manifests::{
    UncoveredWatchedTuple, find_uncovered_watched_tuples,
    load_active_manifest_abi_events_by_chain_and_source_families,
    load_log_producing_source_families, load_watched_contracts_by_addresses,
};
use bigname_storage::{ChainLineageBlock, load_completed_backfill_jobs_intersecting_range};
use serde_json::Value;
use sqlx::Row;

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

    pub(crate) fn reset(&self, chain: &str) {
        self.verified
            .lock()
            .expect("coverage frontier lock must not be poisoned")
            .remove(chain);
    }

    /// Rewind the verified frontier so re-verification covers blocks from
    /// `new_through_block + 1` — called when the watch set grows behind the
    /// frontier (discovery admits an edge whose active window starts inside
    /// the already-verified span). Rewinding past the interval start drops the
    /// memo entirely.
    pub(crate) fn clamp_verified_through(&self, chain: &str, new_through_block: i64) {
        let mut verified = self
            .verified
            .lock()
            .expect("coverage frontier lock must not be poisoned");
        let Some(interval) = verified.get_mut(chain) else {
            return;
        };
        if new_through_block < interval.from_block {
            verified.remove(chain);
        } else if new_through_block < interval.through_block {
            interval.through_block = new_through_block;
        }
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
    ) {
        self.record(
            chain,
            VerifiedCoverageInterval {
                from_block,
                through_block,
                topic_set_fingerprint: topic_set_fingerprint.to_owned(),
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
/// [`COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS`] chunks that opportunistically
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
    let current_topic0s_by_family =
        load_current_topic0s_by_family(pool, chain, &log_producing_source_families).await?;
    let topic_set_fingerprint = topic_set_fingerprint(&current_topic0s_by_family);

    let mut interval = coverage_frontiers.verified_interval(chain);
    if let Some(existing) = &interval
        && (required_from < existing.from_block
            || existing.topic_set_fingerprint != topic_set_fingerprint)
    {
        // Either the checkpoint regressed below the verified interval (deep
        // reorg) or the manifest ABI event sets changed since verification;
        // start over rather than trusting a stale memo.
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
        ensure_family_topic_sets_undrifted(
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
            .saturating_add(COVERAGE_FRONTIER_VERIFICATION_CHUNK_BLOCKS - 1)
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

/// Fail closed on topic-set drift: coverage facts assert fetches that were
/// topics-complete relative to the family's manifest ABI event set at fetch
/// time. If a family's current topic0 set differs from the set persisted in
/// any completed topic-filtered job intersecting the evaluated range — or a
/// topic-filtered job did not persist its set at all — the facts may
/// overclaim relative to the current ABI, so promotion refuses naming the
/// family. Address-enumerated hash-pinned fetches are topic-unfiltered and
/// immune.
async fn ensure_family_topic_sets_undrifted(
    pool: &sqlx::PgPool,
    chain: &str,
    current_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>,
    from_block: i64,
    to_block: i64,
) -> std::result::Result<(), String> {
    if current_topic0s_by_family.is_empty() {
        return Ok(());
    }
    let jobs = load_completed_backfill_jobs_intersecting_range(pool, chain, from_block, to_block)
        .await
        .map_err(|error| error.to_string())?;
    if jobs.is_empty() {
        return Ok(());
    }

    let relied_families = current_topic0s_by_family
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    for job in &jobs {
        if let Some(persisted_by_family) = job
            .source_identity
            .get("coinbase_sql_topic_plan")
            .and_then(|plan| plan.get("topic0s_by_source_family"))
            .and_then(Value::as_object)
        {
            for (source_family, topics) in persisted_by_family {
                if !relied_families.contains(source_family.as_str()) {
                    continue;
                }
                let persisted = topics
                    .as_array()
                    .map(|topics| {
                        topics
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_ascii_lowercase)
                            .collect::<BTreeSet<_>>()
                    })
                    .unwrap_or_default();
                let current = current_topic0s_by_family
                    .get(source_family)
                    .cloned()
                    .unwrap_or_default();
                if persisted != current {
                    return Err(format!(
                        "source family {source_family} manifest ABI topic0 set changed after completed backfill job {} was fetched (persisted {} topic0s, current {}); its coverage facts may overclaim relative to the current ABI — re-run the affected range on the current manifest before promoting",
                        job.backfill_job_id,
                        persisted.len(),
                        current.len()
                    ));
                }
            }
        }

        for scanned_family in topic_filtered_families_without_persisted_sets(&job.source_identity) {
            if relied_families.contains(scanned_family.as_str()) {
                return Err(format!(
                    "source family {scanned_family} was fetched by topic-filtered scan in completed backfill job {} without a persisted topic set; drift relative to the current manifest ABI cannot be ruled out — re-run the affected range on the current manifest before promoting",
                    job.backfill_job_id
                ));
            }
        }
    }

    Ok(())
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

/// Families a job fetched through topic-filtered generic scans whose identity
/// does not persist the topic set in force (hash-pinned generic resolver
/// scans; `coinbase_sql_topic_plan`-bearing identities are checked above).
fn topic_filtered_families_without_persisted_sets(source_identity: &Value) -> Vec<String> {
    if source_identity.get("coinbase_sql_topic_plan").is_some() {
        return Vec::new();
    }

    let mut families = Vec::new();
    if source_identity
        .get("source_identity_payload_format")
        .and_then(Value::as_str)
        == Some("generic_resolver_event_topics_v1")
        && let Some(source_family) = source_identity.get("source_family").and_then(Value::as_str)
    {
        families.push(source_family.to_owned());
    }
    for scan in source_identity
        .get("generic_topic_scans")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(source_family) = scan.get("source_family").and_then(Value::as_str) {
            families.push(source_family.to_owned());
        }
    }
    families.sort_unstable();
    families.dedup();
    families
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
