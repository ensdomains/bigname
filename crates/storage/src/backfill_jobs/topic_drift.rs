//! Topic-set drift guard for durable coverage facts.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::load_completed_backfill_jobs_intersecting_range;

/// Fail closed on topic-set drift: coverage facts assert fetches that were topics-complete
/// relative to the family's manifest ABI event set at fetch time. If a family's current topic0
/// set differs from the set persisted in any completed topic-filtered job intersecting the
/// evaluated range—or a topic-filtered job did not persist its set—the facts may overclaim.
/// Address-enumerated hash-pinned fetches are topic-unfiltered and immune.
pub async fn ensure_backfill_family_topic_sets_undrifted(
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
        if let Some(persisted_by_family) = persisted_topic0s_by_source_family(&job.source_identity)
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

/// The persisted topic0 sets a topic-filtered job fetched under: nested in
/// `coinbase_sql_topic_plan` for Coinbase SQL jobs, or top-level for the hash-pinned Basenames
/// registry scan-all shape.
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

/// Families a job fetched through topic-filtered generic scans whose identity does not persist
/// the topic set in force. Identities with persisted topic sets are checked above.
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
