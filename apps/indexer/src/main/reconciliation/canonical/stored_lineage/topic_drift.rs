use std::collections::{BTreeMap, BTreeSet};

use bigname_manifests::load_required_watched_tuples;
use bigname_storage::{BackfillJob, load_completed_backfill_jobs_intersecting_range};
use serde_json::Value;
use sqlx::Row;

/// Fail closed only when promotion would actually need stale topic-filtered
/// evidence. Retained jobs are audit history, so an intersecting stale job
/// with no relevant facts cannot poison the range forever. Likewise, a
/// gap-free union of current-topic or topic-unfiltered facts that replaces
/// the stale evidence makes the stale rows irrelevant to promotion.
pub(super) async fn ensure_family_topic_sets_undrifted(
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

    let mut current_job_ids_by_family = BTreeMap::<String, BTreeSet<i64>>::new();
    let mut stale_jobs = Vec::<StaleTopicJob>::new();
    for job in &jobs {
        for (source_family, current_topic0s) in current_topic0s_by_family {
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
    let required_tuples =
        load_required_watched_tuples(pool, chain, from_block, to_block, &source_families)
            .await
            .map_err(|error| error.to_string())?;

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
/// Coinbase SQL plan or at the top level for hash-pinned Basenames registry
/// scan-all. Both shapes carry the same family-to-topic-set contract.
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
            backfill_job_id,
            source_family,
            scope::TEXT AS scope,
            LOWER(address) AS address,
            covered_from_block,
            covered_to_block
        FROM backfill_coverage_facts
        WHERE chain_id = $1
          AND backfill_job_id = ANY($2::BIGINT[])
          AND source_family = ANY($3::TEXT[])
          AND covered_from_block <= $5
          AND covered_to_block >= $4
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

/// Families a job fetched through topic-filtered generic scans whose identity
/// does not persist the topic set in force (hash-pinned generic resolver
/// scans; `coinbase_sql_topic_plan`-bearing identities are checked above).
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
    use serde_json::json;

    use super::persisted_topic0s_by_source_family;

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
}
