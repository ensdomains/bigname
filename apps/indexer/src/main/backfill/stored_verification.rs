use std::{future::Future, pin::Pin};

use anyhow::{Context, Result, bail, ensure};
use bigname_manifests::WatchedSourceSelectorPlan;
use bigname_storage::{
    BackfillJob, BackfillStoredVerification, acquire_raw_log_staging_read_guard,
    backfill_job_stored_verification_is_current, record_backfill_job_stored_verification,
};
use sqlx::Row;

use super::{BackfillBlockRange, BackfillTopicPlan};

/// Compare local and source identity evidence in large bounded buckets.
pub(super) const STORED_VERIFICATION_BUCKET_BLOCKS: i64 = 131_072;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum VerifiedRangeSource {
    StoredCandidate,
    Stored,
    Provider,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct VerifiedRangeSegment {
    pub(super) range: BackfillBlockRange,
    pub(super) source: VerifiedRangeSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StoredVerificationPlan {
    pub(super) segments: Vec<VerifiedRangeSegment>,
    planned_raw_log_input_revision: Option<i64>,
    verification_range: Option<BackfillBlockRange>,
    local_bucket_evidence: Option<Vec<StoredLogIdentityBucket>>,
}

impl StoredVerificationPlan {
    pub(super) fn minimum_provider_queries(&self, window_blocks: i64) -> Result<i64> {
        ensure!(
            window_blocks > 0,
            "provider query minimum-projection window must be positive"
        );
        ensure!(
            self.segments
                .iter()
                .all(|segment| segment.source != VerifiedRangeSource::StoredCandidate),
            "provider query projection requires independently verified stored candidates"
        );
        self.segments
            .iter()
            .filter(|segment| segment.source == VerifiedRangeSource::Provider)
            .try_fold(0_i64, |total, segment| {
                let block_count = segment
                    .range
                    .to_block
                    .checked_sub(segment.range.from_block)
                    .and_then(|distance| distance.checked_add(1))
                    .context("provider gap block count overflowed")?;
                let query_count = block_count
                    .checked_add(window_blocks - 1)
                    .context("provider query minimum projection overflowed")?
                    / window_blocks;
                total
                    .checked_add(query_count)
                    .context("provider query minimum projection total overflowed")
            })
    }

    /// Provider gaps replay until the final fenced record exists. Stored
    /// segments advance only their unprocessed checkpoint suffix.
    pub(super) fn execution_segments(
        &self,
        checkpoint_block_number: i64,
    ) -> Result<Vec<VerifiedRangeSegment>> {
        ensure!(
            self.segments
                .iter()
                .all(|segment| segment.source != VerifiedRangeSource::StoredCandidate),
            "stored verification candidates require independent source evidence before execution"
        );
        let mut execution = Vec::new();
        for segment in &self.segments {
            if segment.source == VerifiedRangeSource::Provider {
                execution.push(*segment);
            } else if segment.range.to_block > checkpoint_block_number {
                let from_block = segment.range.from_block.max(
                    checkpoint_block_number
                        .checked_add(1)
                        .context("stored verification checkpoint overflowed")?,
                );
                execution.push(VerifiedRangeSegment {
                    range: BackfillBlockRange::new(from_block, segment.range.to_block)?,
                    source: VerifiedRangeSource::Stored,
                });
            }
        }
        Ok(execution)
    }

    pub(super) fn verify_provider_evidence(
        mut self,
        evidence: StoredLogIdentityEvidence,
    ) -> Result<Self> {
        let range = self
            .verification_range
            .context("stored verification plan has no evidence range")?;
        let local = self
            .local_bucket_evidence
            .take()
            .context("stored verification plan has no local bucket evidence")?;
        let mut provider = vec![StoredLogIdentityBucket::default(); local.len()];
        for bucket in evidence.buckets {
            ensure!(
                bucket.selected_log_count > 0,
                "stored verification source returned a non-positive selected-log count"
            );
            let bucket_index = usize::try_from(bucket.bucket)
                .context("stored verification source returned a negative bucket")?;
            let slot = provider
                .get_mut(bucket_index)
                .context("stored verification source returned an out-of-range bucket")?;
            ensure!(
                *slot == StoredLogIdentityBucket::default(),
                "stored verification source returned duplicate bucket {}",
                bucket.bucket
            );
            *slot = bucket;
        }
        let sources = local
            .iter()
            .zip(provider)
            .map(|(local, provider)| {
                if local.selected_log_count == provider.selected_log_count
                    && local.digest_left == provider.digest_left
                    && local.digest_right == provider.digest_right
                {
                    VerifiedRangeSource::Stored
                } else {
                    VerifiedRangeSource::Provider
                }
            })
            .collect::<Vec<_>>();
        self.segments = coalesced_segments(range, &sources)?;
        Ok(self)
    }

    pub(super) fn is_fully_stored(&self) -> bool {
        !self.segments.is_empty()
            && self
                .segments
                .iter()
                .all(|segment| segment.source == VerifiedRangeSource::Stored)
    }
}

struct ExactStoredSelector {
    chain: String,
    source_family: String,
    address: String,
    topic0s: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct StoredLogIdentityBucket {
    pub(crate) bucket: i64,
    pub(crate) selected_log_count: i64,
    pub(crate) digest_left: u64,
    pub(crate) digest_right: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredLogIdentityEvidence {
    pub(crate) buckets: Vec<StoredLogIdentityBucket>,
    pub(crate) query_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StoredLogIdentityEvidenceRequest {
    pub(crate) chain: String,
    pub(crate) address: String,
    pub(crate) topic0s: Vec<String>,
    pub(crate) range: BackfillBlockRange,
    pub(crate) bucket_blocks: i64,
}

pub(crate) trait StoredLogIdentityEvidenceSource: Send + Sync {
    fn records_provider_query_attempts_incrementally(&self) -> bool {
        false
    }

    fn fetch_stored_log_identity_evidence<'a>(
        &'a self,
        request: StoredLogIdentityEvidenceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<StoredLogIdentityEvidence>> + Send + 'a>>;
}

pub(super) fn stored_log_identity_evidence_request(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    range: BackfillBlockRange,
) -> Result<StoredLogIdentityEvidenceRequest> {
    let selector = exact_stored_selector(source_plan, topic_plan)?;
    ensure!(
        !selector.topic0s.is_empty(),
        "stored verification cannot query a source family without current event topics"
    );
    Ok(StoredLogIdentityEvidenceRequest {
        chain: selector.chain,
        address: selector.address,
        topic0s: selector.topic0s,
        range,
        bucket_blocks: STORED_VERIFICATION_BUCKET_BLOCKS,
    })
}

/// Snapshot canonical raw-log identities under the chain mutation fence.
/// Independent source count and digest evidence must authorize bucket reuse.
pub(super) async fn plan_stored_verification(
    pool: &sqlx::PgPool,
    job: &BackfillJob,
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    range: BackfillBlockRange,
) -> Result<StoredVerificationPlan> {
    let selector = exact_stored_selector(source_plan, topic_plan)?;
    if selector.topic0s.is_empty() {
        return Ok(provider_only_plan(range));
    }

    let mut guard = acquire_raw_log_staging_read_guard(pool, &selector.chain).await?;
    ensure!(
        guard.version().retention_generation == job.raw_log_retention_generation,
        "backfill job {} captured raw-log retention generation {}, but stored verification observed {}",
        job.backfill_job_id,
        job.raw_log_retention_generation,
        guard.version().retention_generation
    );
    let rows = sqlx::query(
        r#"
        WITH selected_raw AS (
            SELECT
                ((logs.block_number - $2) / $6)::BIGINT AS bucket,
                (
                    logs.canonicality_state IN (
                        'canonical'::canonicality_state,
                        'safe'::canonicality_state,
                        'finalized'::canonicality_state
                    )
                    AND lineage.block_hash IS NOT NULL
                ) AS usable,
                md5(
                    LOWER(logs.block_hash)
                    || LOWER(logs.transaction_hash)
                    || logs.log_index::TEXT
                ) AS row_hash
            FROM raw_logs logs
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = logs.chain_id
             AND lineage.block_hash = logs.block_hash
             AND lineage.block_number = logs.block_number
             AND lineage.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
            WHERE logs.chain_id = $1
              AND logs.block_number BETWEEN $2 AND $3
              AND LOWER(logs.emitting_address) = $4
              AND LOWER(logs.topics[1]) = ANY($5::TEXT[])
              AND logs.canonicality_state <> 'orphaned'::canonicality_state
        )
        SELECT
            bucket,
            COUNT(*) FILTER (WHERE usable)::BIGINT AS selected_log_count,
            COUNT(*) FILTER (WHERE NOT usable)::BIGINT AS invalid_count,
            COALESCE(
                bit_xor(
                    ('x' || SUBSTRING(row_hash, 1, 16))::BIT(64)::BIGINT
                ) FILTER (WHERE usable),
                0
            ) AS digest_left,
            COALESCE(
                bit_xor(
                    ('x' || SUBSTRING(row_hash, 17, 16))::BIT(64)::BIGINT
                ) FILTER (WHERE usable),
                0
            ) AS digest_right
        FROM selected_raw
        GROUP BY bucket
        ORDER BY bucket
        "#,
    )
    .bind(&selector.chain)
    .bind(range.from_block)
    .bind(range.to_block)
    .bind(&selector.address)
    .bind(&selector.topic0s)
    .bind(STORED_VERIFICATION_BUCKET_BLOCKS)
    .fetch_all(guard.connection_mut())
    .await
    .with_context(|| {
        format!(
            "failed to plan stored raw-log verification for {} {} on {} over {}..={}",
            selector.source_family,
            selector.address,
            selector.chain,
            range.from_block,
            range.to_block
        )
    })?;

    let bucket_count = range
        .to_block
        .checked_sub(range.from_block)
        .and_then(|distance| distance.checked_add(STORED_VERIFICATION_BUCKET_BLOCKS))
        .context("stored verification bucket count overflowed")?
        / STORED_VERIFICATION_BUCKET_BLOCKS;
    let bucket_count =
        usize::try_from(bucket_count).context("stored verification bucket count is too large")?;
    let mut bucket_evidence = (0..bucket_count)
        .map(|bucket| StoredLogIdentityBucket {
            bucket: i64::try_from(bucket).expect("bucket count was converted from i64"),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    for row in rows {
        let bucket: i64 = row.try_get("bucket")?;
        let selected_log_count: i64 = row.try_get("selected_log_count")?;
        let invalid_count: i64 = row.try_get("invalid_count")?;
        ensure!(
            invalid_count == 0,
            "stored verification found {invalid_count} selected non-orphan raw logs without canonical lineage in bucket {bucket} for {} {}",
            selector.source_family,
            selector.address
        );
        let bucket =
            usize::try_from(bucket).context("stored verification bucket must not be negative")?;
        let slot = bucket_evidence
            .get_mut(bucket)
            .context("stored verification returned an out-of-range bucket")?;
        *slot = StoredLogIdentityBucket {
            bucket: i64::try_from(bucket).context("stored verification bucket overflowed")?,
            selected_log_count,
            digest_left: row.try_get::<i64, _>("digest_left")? as u64,
            digest_right: row.try_get::<i64, _>("digest_right")? as u64,
        };
    }
    let segments = coalesced_segments(
        range,
        &vec![VerifiedRangeSource::StoredCandidate; bucket_count],
    )?;
    let planned_raw_log_input_revision = guard.version().revision;
    guard.release().await?;

    Ok(StoredVerificationPlan {
        segments,
        planned_raw_log_input_revision: Some(planned_raw_log_input_revision),
        verification_range: Some(range),
        local_bucket_evidence: Some(bucket_evidence),
    })
}

pub(super) async fn stored_verification_is_current(
    pool: &sqlx::PgPool,
    job: &BackfillJob,
    range: BackfillBlockRange,
) -> Result<bool> {
    backfill_job_stored_verification_is_current(
        pool,
        job.backfill_job_id,
        &job.chain_id,
        range.from_block,
        range.to_block,
    )
    .await
}

/// Re-read the exact selector and persist its final fenced count and digest.
pub(super) async fn finalize_stored_verification(
    pool: &sqlx::PgPool,
    job: &BackfillJob,
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
    range: BackfillBlockRange,
    plan: &StoredVerificationPlan,
) -> Result<BackfillStoredVerification> {
    let selector = exact_stored_selector(source_plan, topic_plan)?;
    ensure!(
        !selector.topic0s.is_empty(),
        "stored verification cannot authorize a source family without current event topics"
    );
    let mut guard = acquire_raw_log_staging_read_guard(pool, &selector.chain).await?;
    ensure!(
        guard.version().retention_generation == job.raw_log_retention_generation,
        "backfill job {} captured raw-log retention generation {}, but final stored verification observed {}",
        job.backfill_job_id,
        job.raw_log_retention_generation,
        guard.version().retention_generation
    );
    let stored_segments = plan
        .segments
        .iter()
        .filter(|segment| segment.source == VerifiedRangeSource::Stored)
        .collect::<Vec<_>>();
    if !stored_segments.is_empty() {
        let planned_revision = plan
            .planned_raw_log_input_revision
            .context("stored segments are missing their fenced planning revision")?;
        ensure!(
            guard.version().revision >= planned_revision,
            "raw-log input revision moved backwards after stored verification planning"
        );
        let from_blocks = stored_segments
            .iter()
            .map(|segment| segment.range.from_block)
            .collect::<Vec<_>>();
        let to_blocks = stored_segments
            .iter()
            .map(|segment| segment.range.to_block)
            .collect::<Vec<_>>();
        let changed = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM raw_log_staging_block_revisions changed
                JOIN UNNEST($3::BIGINT[], $4::BIGINT[])
                    AS segment(from_block, to_block)
                  ON changed.block_number BETWEEN
                      segment.from_block AND segment.to_block
                WHERE changed.chain_id = $1
                  AND changed.revision > $2
            )
            "#,
        )
        .bind(&selector.chain)
        .bind(planned_revision)
        .bind(&from_blocks)
        .bind(&to_blocks)
        .fetch_one(guard.connection_mut())
        .await
        .context("failed to fence stored verification segments against later raw-log changes")?;
        ensure!(
            !changed,
            "raw-log input changed inside a locally verified stored segment after revision {planned_revision}; replan before writing coverage"
        );
    }
    let row = sqlx::query(
        r#"
        WITH matching AS (
            SELECT
                (
                    logs.canonicality_state IN (
                        'canonical'::canonicality_state,
                        'safe'::canonicality_state,
                        'finalized'::canonicality_state
                    )
                    AND lineage.block_hash IS NOT NULL
                ) AS usable,
                md5(
                    jsonb_build_array(
                        LOWER(logs.block_hash),
                        logs.block_number,
                        LOWER(logs.transaction_hash),
                        logs.transaction_index,
                        logs.log_index,
                        LOWER(logs.emitting_address),
                        logs.topics,
                        encode(logs.data, 'hex')
                    )::TEXT
                ) AS row_hash
            FROM raw_logs logs
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = logs.chain_id
             AND lineage.block_hash = logs.block_hash
             AND lineage.block_number = logs.block_number
             AND lineage.canonicality_state IN (
                 'canonical'::canonicality_state,
                 'safe'::canonicality_state,
                 'finalized'::canonicality_state
             )
            WHERE logs.chain_id = $1
              AND logs.block_number BETWEEN $2 AND $3
              AND LOWER(logs.emitting_address) = $4
              AND LOWER(logs.topics[1]) = ANY($5::TEXT[])
              AND logs.canonicality_state <> 'orphaned'::canonicality_state
        )
        SELECT
            COUNT(*) FILTER (WHERE usable)::BIGINT AS selected_log_count,
            COUNT(*) FILTER (WHERE NOT usable)::BIGINT AS invalid_count,
            LPAD(
                to_hex(
                    COALESCE(
                        bit_xor(
                            ('x' || SUBSTRING(row_hash, 1, 16))::BIT(64)::BIGINT
                        ) FILTER (WHERE usable),
                        0
                    )
                ),
                16,
                '0'
            ) || LPAD(
                to_hex(
                    COALESCE(
                        bit_xor(
                            ('x' || SUBSTRING(row_hash, 17, 16))::BIT(64)::BIGINT
                        ) FILTER (WHERE usable),
                        0
                    )
                ),
                16,
                '0'
            ) AS selected_log_digest
        FROM matching
        "#,
    )
    .bind(&selector.chain)
    .bind(range.from_block)
    .bind(range.to_block)
    .bind(&selector.address)
    .bind(&selector.topic0s)
    .fetch_one(guard.connection_mut())
    .await
    .with_context(|| {
        format!(
            "failed to finalize stored raw-log verification for {} {} on {} over {}..={}",
            selector.source_family,
            selector.address,
            selector.chain,
            range.from_block,
            range.to_block
        )
    })?;
    let invalid_count: i64 = row.try_get("invalid_count")?;
    ensure!(
        invalid_count == 0,
        "final stored verification found {invalid_count} selected non-orphan raw logs without canonical lineage"
    );
    let verification = BackfillStoredVerification {
        raw_log_input_revision: guard.version().revision,
        verified_from_block: range.from_block,
        verified_to_block: range.to_block,
        selected_log_count: row.try_get("selected_log_count")?,
        selected_log_digest: row.try_get("selected_log_digest")?,
    };
    record_backfill_job_stored_verification(
        guard.connection_mut(),
        job.backfill_job_id,
        job.raw_log_retention_generation,
        &verification,
    )
    .await?;
    guard.release().await?;
    Ok(verification)
}

fn exact_stored_selector(
    source_plan: &WatchedSourceSelectorPlan,
    topic_plan: &BackfillTopicPlan,
) -> Result<ExactStoredSelector> {
    let first = source_plan
        .selected_targets
        .first()
        .context("stored verification source plan has no selected target")?;
    ensure!(
        source_plan.selected_targets.iter().all(|target| {
            target.source_family == first.source_family
                && target.address.eq_ignore_ascii_case(&first.address)
        }),
        "stored verification accepts only one exact (source family, address) tuple"
    );
    let mut topic0s = topic_plan
        .topic0s_for_source_family(&first.source_family)
        .iter()
        .map(|topic0| topic0.to_ascii_lowercase())
        .collect::<Vec<_>>();
    topic0s.sort();
    topic0s.dedup();
    Ok(ExactStoredSelector {
        chain: source_plan.watched_chain_plan.chain.clone(),
        source_family: first.source_family.clone(),
        address: first.address.to_ascii_lowercase(),
        topic0s,
    })
}

pub(super) fn provider_only_plan(range: BackfillBlockRange) -> StoredVerificationPlan {
    StoredVerificationPlan {
        segments: vec![VerifiedRangeSegment {
            range,
            source: VerifiedRangeSource::Provider,
        }],
        planned_raw_log_input_revision: None,
        verification_range: None,
        local_bucket_evidence: None,
    }
}

pub(super) fn completed_plan() -> StoredVerificationPlan {
    StoredVerificationPlan {
        segments: Vec::new(),
        planned_raw_log_input_revision: None,
        verification_range: None,
        local_bucket_evidence: None,
    }
}

fn coalesced_segments(
    range: BackfillBlockRange,
    bucket_sources: &[VerifiedRangeSource],
) -> Result<Vec<VerifiedRangeSegment>> {
    let mut segments = Vec::<VerifiedRangeSegment>::new();
    for (bucket, source) in bucket_sources.iter().copied().enumerate() {
        let bucket = i64::try_from(bucket).context("stored verification bucket overflowed")?;
        let from_block = range
            .from_block
            .checked_add(
                bucket
                    .checked_mul(STORED_VERIFICATION_BUCKET_BLOCKS)
                    .context("stored verification bucket offset overflowed")?,
            )
            .context("stored verification bucket start overflowed")?;
        let to_block = from_block
            .checked_add(STORED_VERIFICATION_BUCKET_BLOCKS - 1)
            .unwrap_or(range.to_block)
            .min(range.to_block);
        if let Some(previous) = segments.last_mut()
            && previous.source == source
            && previous.range.to_block.checked_add(1) == Some(from_block)
        {
            previous.range.to_block = to_block;
        } else {
            segments.push(VerifiedRangeSegment {
                range: BackfillBlockRange::new(from_block, to_block)?,
                source,
            });
        }
    }
    if segments.is_empty() {
        bail!("stored verification produced no range segments");
    }
    Ok(segments)
}

#[cfg(test)]
#[path = "stored_verification/tests.rs"]
mod tests;
