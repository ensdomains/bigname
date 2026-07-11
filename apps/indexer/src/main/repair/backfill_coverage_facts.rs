use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use bigname_storage::{
    BackfillCoverageFactDerivation, BackfillCoverageFactScope, BackfillCoverageFactWrite,
    BackfillJob, BackfillLifecycleStatus, load_backfill_job, write_backfill_coverage_facts,
};
use serde_json::Value;

use crate::backfill::{covered_block_interval, merged_covered_block_segments};

const FORMAT_SELECTED_TARGETS_DIGEST: &str = "selected_targets_digest_v1";
const FORMAT_SELECTED_TARGETS_DIGEST_WITH_GENERIC_TOPIC_SCANS: &str =
    "selected_targets_digest_with_generic_topic_scans_v1";
const FORMAT_SELECTED_TARGETS_WITH_GENERIC_TOPIC_SCANS: &str =
    "selected_targets_with_generic_topic_scans_v1";
const FORMAT_GENERIC_RESOLVER_EVENT_TOPICS: &str = "generic_resolver_event_topics_v1";
const FORMAT_BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES: &str =
    "basenames_registry_scan_all_event_signatures_v1";
const FORMAT_BASENAMES_REGISTRY_SCAN_ALL_TOPICS: &str = "basenames_registry_scan_all_topics_v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LegacyBackfillCoverageFactsOutcome {
    pub(crate) backfill_job_id: i64,
    pub(crate) address_fact_count: usize,
    pub(crate) family_fact_count: usize,
    pub(crate) inserted_fact_count: u64,
}

/// Derive coverage facts for an already-completed job from its persisted
/// verbatim-target `source_identity` (fnv1a64-era full payloads included).
/// Refused shapes: compact digests (the fetched target set is not recoverable
/// from a digest), family-scan-only identities, and identities declaring
/// `generic_topic_scans` without persisting the scanned family's targets
/// (partial address-only coverage would silently omit the family fetch) —
/// all require re-running the job on fact-writing code. Family facts for
/// `generic_topic_scans` families with persisted targets use the same
/// clamp-and-merge segments as live completion.
pub(crate) async fn derive_legacy_backfill_coverage_facts(
    pool: &sqlx::PgPool,
    backfill_job_id: i64,
) -> Result<LegacyBackfillCoverageFactsOutcome> {
    let job = load_backfill_job(pool, backfill_job_id)
        .await?
        .with_context(|| format!("missing backfill job {backfill_job_id}"))?;
    if job.status != BackfillLifecycleStatus::Completed {
        bail!(
            "backfill job {backfill_job_id} is {}; legacy coverage facts can only be derived for completed jobs",
            job.status.as_str()
        );
    }

    let facts = legacy_coverage_facts_from_source_identity(&job)?;
    let family_fact_count = facts
        .iter()
        .filter(|fact| fact.scope == BackfillCoverageFactScope::Family)
        .count();
    let address_fact_count = facts.len() - family_fact_count;

    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for legacy coverage fact derivation")?;
    let inserted_fact_count = write_backfill_coverage_facts(
        &mut transaction,
        job.backfill_job_id,
        BackfillCoverageFactDerivation::LegacyFullPayloadIdentity,
        &facts,
    )
    .await?;
    transaction
        .commit()
        .await
        .context("failed to commit legacy coverage fact derivation")?;

    Ok(LegacyBackfillCoverageFactsOutcome {
        backfill_job_id: job.backfill_job_id,
        address_fact_count,
        family_fact_count,
        inserted_fact_count,
    })
}

fn legacy_coverage_facts_from_source_identity(
    job: &BackfillJob,
) -> Result<Vec<BackfillCoverageFactWrite>> {
    let source_identity = &job.source_identity;
    let payload_format = source_identity
        .get("source_identity_payload_format")
        .and_then(Value::as_str);
    match payload_format {
        Some(FORMAT_SELECTED_TARGETS_DIGEST)
        | Some(FORMAT_SELECTED_TARGETS_DIGEST_WITH_GENERIC_TOPIC_SCANS) => bail!(
            "backfill job {} persisted a compact selected-targets digest identity ({}); the fetched target set cannot be recovered from a digest, so the job must be re-completed on fact-writing code",
            job.backfill_job_id,
            payload_format.unwrap_or_default()
        ),
        Some(FORMAT_GENERIC_RESOLVER_EVENT_TOPICS)
        | Some(FORMAT_BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES) => bail!(
            "backfill job {} persisted a family-scan identity ({}) that does not persist the family target spans needed for sound family facts, so the job must be re-completed on fact-writing code",
            job.backfill_job_id,
            payload_format.unwrap_or_default()
        ),
        // The hash-pinned scan-all fetches its persisted topic0 set across
        // every block of every range (no window skipping, unlike the Coinbase
        // SQL scan-all planner), so a full-job-range family fact is sound.
        Some(FORMAT_BASENAMES_REGISTRY_SCAN_ALL_TOPICS) => {
            let families = source_identity
                .get("topic0s_by_source_family")
                .and_then(Value::as_object)
                .with_context(|| {
                    format!(
                        "backfill job {} scan-all-topics identity must persist topic0s_by_source_family",
                        job.backfill_job_id
                    )
                })?;
            return Ok(families
                .keys()
                .map(|source_family| BackfillCoverageFactWrite {
                    source_family: source_family.clone(),
                    scope: BackfillCoverageFactScope::Family,
                    address: None,
                    covered_from_block: job.range_start_block_number,
                    covered_to_block: job.range_end_block_number,
                })
                .collect());
        }
        None | Some(FORMAT_SELECTED_TARGETS_WITH_GENERIC_TOPIC_SCANS) => {}
        Some(other) => bail!(
            "backfill job {} persisted an unsupported source_identity_payload_format {other}; coverage cannot be derived from it",
            job.backfill_job_id
        ),
    }

    let selected_targets = source_identity
        .get("selected_targets")
        .with_context(|| {
            format!(
                "backfill job {} source_identity does not carry selected_targets verbatim; coverage cannot be derived from it",
                job.backfill_job_id
            )
        })?
        .as_array()
        .with_context(|| {
            format!(
                "backfill job {} selected_targets must be a JSON array",
                job.backfill_job_id
            )
        })?;

    let family_scan_source_families = generic_topic_scan_source_families(source_identity);
    let mut family_scan_windows = BTreeMap::<String, Vec<(i64, i64)>>::new();
    let mut facts = Vec::new();
    for (index, target) in selected_targets.iter().enumerate() {
        let context = || {
            format!(
                "backfill job {} selected_targets[{index}]",
                job.backfill_job_id
            )
        };
        let source_family = target
            .get("source_family")
            .and_then(Value::as_str)
            .with_context(|| format!("{} must carry a source_family string", context()))?;
        let address = target
            .get("address")
            .and_then(Value::as_str)
            .with_context(|| format!("{} must carry an address string", context()))?;
        let effective_from_block = target
            .get("effective_from_block")
            .and_then(Value::as_i64)
            .with_context(|| format!("{} must carry effective_from_block", context()))?;
        let effective_to_block = target
            .get("effective_to_block")
            .and_then(Value::as_i64)
            .with_context(|| format!("{} must carry effective_to_block", context()))?;

        if family_scan_source_families.contains(source_family) {
            family_scan_windows
                .entry(source_family.to_owned())
                .or_default()
                .push((effective_from_block, effective_to_block));
            continue;
        }
        let Some((covered_from_block, covered_to_block)) = covered_block_interval(
            effective_from_block,
            effective_to_block,
            job.range_start_block_number,
            job.range_end_block_number,
        ) else {
            continue;
        };
        facts.push(BackfillCoverageFactWrite {
            source_family: source_family.to_owned(),
            scope: BackfillCoverageFactScope::Address,
            address: Some(address.to_ascii_lowercase()),
            covered_from_block,
            covered_to_block,
        });
    }

    // Live producers filtered the generic-scanned families' targets out of
    // the persisted selected_targets, so a declared scan without persisted
    // windows means the family's fetched span is unrecoverable. Deriving only
    // the address-scoped portion would report success while silently omitting
    // family coverage the job actually fetched — refuse instead of writing
    // partial coverage.
    for declared_family in &family_scan_source_families {
        if !family_scan_windows.contains_key(declared_family) {
            bail!(
                "backfill job {} declares a generic topic scan for source family {declared_family} but persists no selected targets for it; the address-scoped facts could be derived, but this command refuses partial coverage because the family's fetched span is unrecoverable — re-run the job on fact-writing code",
                job.backfill_job_id
            );
        }
    }

    for (source_family, windows) in family_scan_windows {
        for (covered_from_block, covered_to_block) in merged_covered_block_segments(
            windows,
            job.range_start_block_number,
            job.range_end_block_number,
        ) {
            facts.push(BackfillCoverageFactWrite {
                source_family: source_family.clone(),
                scope: BackfillCoverageFactScope::Family,
                address: None,
                covered_from_block,
                covered_to_block,
            });
        }
    }

    Ok(facts)
}

/// Source families the job fetched topics-complete for every address, per the
/// `generic_topic_scans` declarations attached to verbatim-target payloads.
fn generic_topic_scan_source_families(source_identity: &Value) -> BTreeSet<String> {
    let mut source_families = BTreeSet::new();
    for scan in source_identity
        .get("generic_topic_scans")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if scan
            .get("source_identity_payload_format")
            .and_then(Value::as_str)
            == Some(FORMAT_GENERIC_RESOLVER_EVENT_TOPICS)
            && let Some(source_family) = scan.get("source_family").and_then(Value::as_str)
        {
            source_families.insert(source_family.to_owned());
        }
    }

    source_families
}
