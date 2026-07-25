use super::*;

#[test]
fn bucket_segments_keep_stored_and_provider_spans_exact() -> Result<()> {
    let range = BackfillBlockRange::new(10, 10 + STORED_VERIFICATION_BUCKET_BLOCKS * 4 - 2)?;
    let exact = StoredLogIdentityBucket {
        bucket: 0,
        selected_log_count: 3,
        digest_left: 7,
        digest_right: 11,
    };
    let local = vec![
        exact,
        StoredLogIdentityBucket {
            bucket: 1,
            selected_log_count: 2,
            digest_left: 13,
            digest_right: 17,
        },
        StoredLogIdentityBucket {
            bucket: 2,
            ..Default::default()
        },
        StoredLogIdentityBucket { bucket: 3, ..exact },
    ];
    let plan = StoredVerificationPlan {
        segments: vec![VerifiedRangeSegment {
            range,
            source: VerifiedRangeSource::StoredCandidate,
        }],
        planned_raw_log_input_revision: Some(1),
        verification_range: Some(range),
        local_bucket_evidence: Some(local),
    };
    let plan = plan.verify_provider_evidence(StoredLogIdentityEvidence {
        query_count: 1,
        buckets: vec![
            exact,
            StoredLogIdentityBucket {
                bucket: 1,
                selected_log_count: 1,
                digest_left: 13,
                digest_right: 17,
            },
            StoredLogIdentityBucket { bucket: 3, ..exact },
        ],
    })?;

    assert_eq!(
        plan.segments,
        vec![
            VerifiedRangeSegment {
                range: BackfillBlockRange::new(10, 10 + STORED_VERIFICATION_BUCKET_BLOCKS - 1,)?,
                source: VerifiedRangeSource::Stored,
            },
            VerifiedRangeSegment {
                range: BackfillBlockRange::new(
                    10 + STORED_VERIFICATION_BUCKET_BLOCKS,
                    10 + STORED_VERIFICATION_BUCKET_BLOCKS * 2 - 1,
                )?,
                source: VerifiedRangeSource::Provider,
            },
            VerifiedRangeSegment {
                range: BackfillBlockRange::new(
                    10 + STORED_VERIFICATION_BUCKET_BLOCKS * 2,
                    range.to_block,
                )?,
                source: VerifiedRangeSource::Stored,
            },
        ],
        "only exact local/source identity parity may authorize a bucket"
    );
    Ok(())
}

#[test]
fn partial_local_history_requires_provider_fetch() -> Result<()> {
    let range = BackfillBlockRange::new(0, 32)?;
    let partial_local_history = StoredLogIdentityBucket {
        bucket: 0,
        selected_log_count: 1,
        digest_left: 7,
        digest_right: 11,
    };
    let provider_history = StoredLogIdentityBucket {
        selected_log_count: 2,
        ..partial_local_history
    };
    let plan = StoredVerificationPlan {
        segments: vec![VerifiedRangeSegment {
            range,
            source: VerifiedRangeSource::StoredCandidate,
        }],
        planned_raw_log_input_revision: Some(1),
        verification_range: Some(range),
        local_bucket_evidence: Some(vec![partial_local_history]),
    }
    .verify_provider_evidence(StoredLogIdentityEvidence {
        buckets: vec![provider_history],
        query_count: 1,
    })?;

    assert_eq!(plan.segments[0].source, VerifiedRangeSource::Provider);
    Ok(())
}

#[test]
fn resume_replays_unproven_provider_prefix_but_clips_stored_suffix() -> Result<()> {
    let plan = StoredVerificationPlan {
        segments: vec![
            VerifiedRangeSegment {
                range: BackfillBlockRange::new(10, 19)?,
                source: VerifiedRangeSource::Provider,
            },
            VerifiedRangeSegment {
                range: BackfillBlockRange::new(20, 29)?,
                source: VerifiedRangeSource::Stored,
            },
            VerifiedRangeSegment {
                range: BackfillBlockRange::new(30, 39)?,
                source: VerifiedRangeSource::Provider,
            },
        ],
        planned_raw_log_input_revision: Some(7),
        verification_range: None,
        local_bucket_evidence: None,
    };

    assert_eq!(
        plan.execution_segments(24)?,
        vec![
            VerifiedRangeSegment {
                range: BackfillBlockRange::new(10, 19)?,
                source: VerifiedRangeSource::Provider,
            },
            VerifiedRangeSegment {
                range: BackfillBlockRange::new(25, 29)?,
                source: VerifiedRangeSource::Stored,
            },
            VerifiedRangeSegment {
                range: BackfillBlockRange::new(30, 39)?,
                source: VerifiedRangeSource::Provider,
            },
        ]
    );
    Ok(())
}
