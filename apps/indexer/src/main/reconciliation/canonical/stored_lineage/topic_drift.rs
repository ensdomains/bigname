use std::collections::{BTreeMap, BTreeSet};

use bigname_manifests::RequiredWatchedTuple;
use bigname_storage::{BackfillJob, load_completed_backfill_jobs_intersecting_range};
use serde_json::Value;
use sqlx::Row;

/// Fail closed only when promotion would actually need stale topic-filtered
/// evidence. Retained jobs are audit history, so an intersecting stale job
/// with no relevant facts cannot poison the range forever. Likewise, a
/// gap-free union of current-topic or topic-unfiltered facts that replaces
/// the stale evidence makes the stale rows irrelevant to promotion.
pub(super) async fn ensure_required_topic_sets_undrifted(
    pool: &sqlx::PgPool,
    chain: &str,
    current_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>,
    required_tuples: &[RequiredWatchedTuple],
) -> std::result::Result<(), String> {
    ensure_required_topic_sets_undrifted_with_retention_generation(
        pool,
        chain,
        current_topic0s_by_family,
        required_tuples,
        None,
    )
    .await
}

/// Retention-recovery variant: only jobs captured in the current raw-log
/// generation may replace a stale topic-filtered fact. An older generation's
/// otherwise-current job is audit history, not recovery authority.
pub(crate) async fn ensure_required_topic_sets_undrifted_for_retention_generation(
    pool: &sqlx::PgPool,
    chain: &str,
    current_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>,
    required_tuples: &[RequiredWatchedTuple],
    retention_generation: i64,
) -> std::result::Result<(), String> {
    ensure_required_topic_sets_undrifted_with_retention_generation(
        pool,
        chain,
        current_topic0s_by_family,
        required_tuples,
        Some(retention_generation),
    )
    .await
}

async fn ensure_required_topic_sets_undrifted_with_retention_generation(
    pool: &sqlx::PgPool,
    chain: &str,
    current_topic0s_by_family: &BTreeMap<String, BTreeSet<String>>,
    required_tuples: &[RequiredWatchedTuple],
    retention_generation: Option<i64>,
) -> std::result::Result<(), String> {
    if current_topic0s_by_family.is_empty() || required_tuples.is_empty() {
        return Ok(());
    }
    let from_block = required_tuples
        .iter()
        .map(|tuple| tuple.required_from_block)
        .min()
        .expect("non-empty requirements must have a lower bound");
    let to_block = required_tuples
        .iter()
        .map(|tuple| tuple.required_to_block)
        .max()
        .expect("non-empty requirements must have an upper bound");
    let required_source_families = required_tuples
        .iter()
        .map(|tuple| tuple.source_family.as_str())
        .collect::<BTreeSet<_>>();
    let relevant_topic0s_by_family = current_topic0s_by_family
        .iter()
        .filter(|(family, _)| required_source_families.contains(family.as_str()))
        .map(|(family, topics)| (family.clone(), topics.clone()))
        .collect::<BTreeMap<_, _>>();
    let jobs = load_completed_backfill_jobs_intersecting_range(pool, chain, from_block, to_block)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|job| {
            retention_generation
                .is_none_or(|generation| job.raw_log_retention_generation == generation)
        })
        .collect::<Vec<_>>();
    if jobs.is_empty() {
        return Ok(());
    }

    let mut current_job_ids_by_family = BTreeMap::<String, BTreeSet<i64>>::new();
    let mut stale_jobs = Vec::<StaleTopicJob>::new();
    for job in &jobs {
        for (source_family, current_topic0s) in &relevant_topic0s_by_family {
            match topic_status(job, source_family, current_topic0s) {
                TopicStatus::CurrentOrUnfiltered => {
                    current_job_ids_by_family
                        .entry(source_family.clone())
                        .or_default()
                        .insert(job.backfill_job_id);
                }
                TopicStatus::Stale(reason) => stale_jobs.push(StaleTopicJob {
                    backfill_job_id: job.backfill_job_id,
                    source_family: source_family.clone(),
                    reason,
                }),
            }
        }
    }
    if stale_jobs.is_empty() {
        return Ok(());
    }

    let job_ids = jobs
        .iter()
        .map(|job| job.backfill_job_id)
        .collect::<Vec<_>>();
    let source_families = stale_jobs
        .iter()
        .map(|job| job.source_family.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let facts = load_topic_coverage_facts(
        pool,
        chain,
        from_block,
        to_block,
        &job_ids,
        &source_families,
    )
    .await?;
    for tuple in required_tuples {
        let current_job_ids = current_job_ids_by_family
            .get(&tuple.source_family)
            .cloned()
            .unwrap_or_default();
        let applicable_current_facts = || {
            facts.iter().filter(|fact| {
                current_job_ids.contains(&fact.backfill_job_id)
                    && fact_applies_to_tuple(fact, &tuple.source_family, &tuple.address)
            })
        };
        if facts_cover_interval(
            applicable_current_facts(),
            tuple.required_from_block,
            tuple.required_to_block,
        ) {
            continue;
        }
        if !facts_cover_interval(
            facts
                .iter()
                .filter(|fact| fact_applies_to_tuple(fact, &tuple.source_family, &tuple.address)),
            tuple.required_from_block,
            tuple.required_to_block,
        ) {
            // The ordinary coverage gate will report the concrete gap. A
            // stale job that does not complete the interval is not itself a
            // reason to replace otherwise irrelevant audit history.
            continue;
        }

        for stale_job in stale_jobs
            .iter()
            .filter(|job| job.source_family == tuple.source_family)
        {
            let stale_fact_supplies_unreplaced_coverage = facts.iter().any(|fact| {
                if fact.backfill_job_id != stale_job.backfill_job_id
                    || !fact_applies_to_tuple(fact, &tuple.source_family, &tuple.address)
                {
                    return false;
                }
                let contributed_from = fact.covered_from_block.max(tuple.required_from_block);
                let contributed_to = fact.covered_to_block.min(tuple.required_to_block);
                contributed_from <= contributed_to
                    && !facts_cover_interval(
                        applicable_current_facts(),
                        contributed_from,
                        contributed_to,
                    )
            });
            if stale_fact_supplies_unreplaced_coverage {
                return Err(stale_job.reason.clone());
            }
        }
    }

    Ok(())
}

enum TopicStatus {
    CurrentOrUnfiltered,
    Stale(String),
}

struct StaleTopicJob {
    backfill_job_id: i64,
    source_family: String,
    reason: String,
}

struct TopicCoverageFact {
    backfill_job_id: i64,
    source_family: String,
    scope: String,
    address: Option<String>,
    covered_from_block: i64,
    covered_to_block: i64,
}

fn topic_status(
    job: &BackfillJob,
    source_family: &str,
    current_topic0s: &BTreeSet<String>,
) -> TopicStatus {
    if let Some(persisted) = persisted_topic0s_by_source_family(&job.source_identity)
        .and_then(|families| families.get(source_family))
    {
        let persisted = persisted
            .as_array()
            .map(|topics| {
                topics
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_ascii_lowercase)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        if persisted != *current_topic0s {
            return TopicStatus::Stale(format!(
                "source family {source_family} manifest ABI topic0 set changed after completed backfill job {} was fetched (persisted {} topic0s, current {}); its relied-upon coverage facts may overclaim relative to the current ABI — re-run the affected range on the current manifest before promoting",
                job.backfill_job_id,
                persisted.len(),
                current_topic0s.len()
            ));
        }
    }

    if topic_filtered_families_without_persisted_sets(&job.source_identity)
        .iter()
        .any(|family| family == source_family)
    {
        return TopicStatus::Stale(format!(
            "source family {source_family} was fetched by topic-filtered scan in completed backfill job {} without a persisted topic set; drift in its relied-upon coverage facts relative to the current manifest ABI cannot be ruled out — re-run the affected range on the current manifest before promoting",
            job.backfill_job_id
        ));
    }

    TopicStatus::CurrentOrUnfiltered
}

/// Topic-filtered jobs persist their manifest topic0 sets either inside the
/// Coinbase SQL plan or at the top level for hash-pinned generic ENSv1
/// resolver and Basenames registry scans. Both shapes carry the same
/// family-to-topic-set contract.
fn persisted_topic0s_by_source_family(
    source_identity: &Value,
) -> Option<&serde_json::Map<String, Value>> {
    source_identity
        .get("coinbase_sql_topic_plan")
        .and_then(|plan| plan.get("topic0s_by_source_family"))
        .and_then(Value::as_object)
        .or_else(|| {
            source_identity
                .get("topic0s_by_source_family")
                .and_then(Value::as_object)
        })
}

async fn load_topic_coverage_facts(
    pool: &sqlx::PgPool,
    chain: &str,
    from_block: i64,
    to_block: i64,
    job_ids: &[i64],
    source_families: &[String],
) -> std::result::Result<Vec<TopicCoverageFact>, String> {
    let rows = sqlx::query(
        r#"
        SELECT
            fact.backfill_job_id,
            fact.source_family,
            fact.scope::TEXT AS scope,
            LOWER(fact.address) AS address,
            fact.covered_from_block,
            fact.covered_to_block
        FROM backfill_coverage_facts fact
        JOIN backfill_jobs fact_job
          ON fact_job.backfill_job_id = fact.backfill_job_id
        WHERE fact.chain_id = $1
          AND fact.backfill_job_id = ANY($2::BIGINT[])
          AND fact_job.status = 'completed'::backfill_lifecycle_status
          AND fact_job.chain_id = fact.chain_id
          AND fact.covered_from_block >= fact_job.range_start_block_number
          AND fact.covered_to_block <= fact_job.range_end_block_number
          AND fact.source_family = ANY($3::TEXT[])
          AND fact.covered_from_block <= $5
          AND fact.covered_to_block >= $4
        "#,
    )
    .bind(chain)
    .bind(job_ids)
    .bind(source_families)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    rows.into_iter()
        .map(|row| {
            Ok(TopicCoverageFact {
                backfill_job_id: row.try_get("backfill_job_id").map_err(|e| e.to_string())?,
                source_family: row.try_get("source_family").map_err(|e| e.to_string())?,
                scope: row.try_get("scope").map_err(|e| e.to_string())?,
                address: row.try_get("address").map_err(|e| e.to_string())?,
                covered_from_block: row
                    .try_get("covered_from_block")
                    .map_err(|e| e.to_string())?,
                covered_to_block: row.try_get("covered_to_block").map_err(|e| e.to_string())?,
            })
        })
        .collect()
}

fn fact_applies_to_tuple(
    fact: &TopicCoverageFact,
    required_source_family: &str,
    required_address: &str,
) -> bool {
    fact.source_family == required_source_family
        && (fact.scope == "family"
            || (fact.scope == "address" && fact.address.as_deref() == Some(required_address)))
}

fn facts_cover_interval<'a>(
    facts: impl IntoIterator<Item = &'a TopicCoverageFact>,
    required_from: i64,
    required_to: i64,
) -> bool {
    let mut intervals = facts
        .into_iter()
        .filter_map(|fact| {
            let from = fact.covered_from_block.max(required_from);
            let to = fact.covered_to_block.min(required_to);
            (from <= to).then_some((from, to))
        })
        .collect::<Vec<_>>();
    intervals.sort_unstable();

    let mut next_required = required_from;
    for (from, to) in intervals {
        if to < next_required {
            continue;
        }
        if from > next_required {
            return false;
        }
        if to >= required_to {
            return true;
        }
        next_required = to.saturating_add(1);
    }
    false
}

/// Families a legacy job fetched through topic-filtered generic scans whose
/// identity does not persist the topic set in force. Current hash-pinned
/// generic resolver producers persist their set at the top level;
/// `coinbase_sql_topic_plan`-bearing identities are checked above.
fn topic_filtered_families_without_persisted_sets(source_identity: &Value) -> Vec<String> {
    if persisted_topic0s_by_source_family(source_identity).is_some() {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bigname_storage::{BackfillJob, BackfillLifecycleStatus};
    use serde_json::json;
    use sqlx::types::time::OffsetDateTime;

    use crate::ens_v1_resolver::{
        SOURCE_FAMILY_ENS_V1_RESOLVER_L1, generic_resolver_record_topic0s,
    };

    use super::{TopicStatus, persisted_topic0s_by_source_family, topic_status};

    fn completed_job(source_identity: serde_json::Value) -> BackfillJob {
        BackfillJob {
            backfill_job_id: 7,
            deployment_profile: "mainnet".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            raw_log_retention_generation: 0,
            source_identity,
            scan_mode: "hash_pinned_block".to_owned(),
            range_start_block_number: 1,
            range_end_block_number: 10,
            idempotency_key: "generic-resolver-topic-test".to_owned(),
            status: BackfillLifecycleStatus::Completed,
            failure_reason: None,
            failure_metadata: json!({}),
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
            completed_at: Some(OffsetDateTime::UNIX_EPOCH),
        }
    }

    fn generic_resolver_identity(topic0s: &[String]) -> serde_json::Value {
        json!({
            "selector_kind": "source_family",
            "source_family": SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            "source_identity_payload_format": "generic_resolver_event_topics_v1",
            "topic0s_by_source_family": {
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1: topic0s,
            }
        })
    }

    fn current_generic_resolver_topic0s() -> BTreeSet<String> {
        generic_resolver_record_topic0s()
            .into_iter()
            .map(|topic0| topic0.to_ascii_lowercase())
            .collect()
    }

    #[test]
    fn topic_drift_reads_top_level_hash_pinned_scan_all_topics() {
        let source_identity = json!({
            "source_identity_payload_format": "basenames_registry_scan_all_topics_v1",
            "topic0s_by_source_family": {
                "basenames_base_registry": ["0x1234"]
            }
        });

        let topics = persisted_topic0s_by_source_family(&source_identity)
            .and_then(|families| families.get("basenames_base_registry"))
            .and_then(serde_json::Value::as_array)
            .expect("hash-pinned scan-all topics must be readable at the top level");
        assert_eq!(topics, &[json!("0x1234")]);
    }

    #[test]
    fn hash_pinned_generic_resolver_identity_accepts_its_fetched_topic_set() {
        let topic0s = generic_resolver_record_topic0s();
        let job = completed_job(generic_resolver_identity(&topic0s));

        assert!(matches!(
            topic_status(
                &job,
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
                &current_generic_resolver_topic0s(),
            ),
            TopicStatus::CurrentOrUnfiltered
        ));
    }

    #[test]
    fn hash_pinned_generic_resolver_identity_rejects_topic_drift() {
        let mut fetched_topic0s = generic_resolver_record_topic0s();
        fetched_topic0s.pop();
        let job = completed_job(generic_resolver_identity(&fetched_topic0s));

        let TopicStatus::Stale(reason) = topic_status(
            &job,
            SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            &current_generic_resolver_topic0s(),
        ) else {
            panic!("a changed generic resolver topic set must be stale");
        };
        assert!(reason.contains("manifest ABI topic0 set changed"));
    }
}
