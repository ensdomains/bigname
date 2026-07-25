use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;

const CANONICAL_EMITTER_BLOCK_INDEX: &str = "raw_logs_canonical_emitter_block_idx";
const RAW_LOG_STATE_INDEX: &str = "raw_logs_by_state_idx";

#[tokio::test]
async fn verification_scans_use_canonical_emitter_and_observed_state_indexes() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_stored_verification_explain"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for stored verification EXPLAIN test",
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            canonicality_state
        )
        SELECT
            'explain-chain',
            '0x' || LPAD(to_hex(value), 64, '0'),
            value,
            '0x' || LPAD(to_hex(value + 20000), 64, '0'),
            0,
            0,
            '0x' || LPAD(to_hex((value % 97) + 1), 40, '0'),
            ARRAY[
                '0x0000000000000000000000000000000000000000000000000000000000000001'
            ],
            CASE
                WHEN value % 10 = 0 THEN 'observed'::canonicality_state
                ELSE 'canonical'::canonicality_state
            END
        FROM generate_series(1, 10000) value
        "#,
    )
    .execute(database.pool())
    .await?;
    sqlx::query("ANALYZE raw_logs")
        .execute(database.pool())
        .await?;
    let mut transaction = database.pool().begin().await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await?;
    let chain = "explain-chain";
    let address = "0x0000000000000000000000000000000000000001";
    let topic = format!("0x{:064x}", 1);

    let identity_plan = sqlx::query_scalar::<_, String>(&format!(
        "EXPLAIN (FORMAT TEXT) {}",
        scans::LOCAL_IDENTITY_BUCKET_SCAN_SQL
    ))
    .bind(chain)
    .bind(0_i64)
    .bind(20_000_000_i64)
    .bind(address)
    .bind(vec![topic.clone()])
    .bind(STORED_VERIFICATION_BUCKET_BLOCKS)
    .fetch_all(&mut *transaction)
    .await?
    .join("\n");
    eprintln!("stored identity verification plan:\n{identity_plan}");
    assert!(
        identity_plan.contains(CANONICAL_EMITTER_BLOCK_INDEX),
        "canonical identity aggregation must use the exact-emitter partial index:\n{identity_plan}"
    );
    assert!(
        identity_plan.contains(RAW_LOG_STATE_INDEX),
        "observed-row invalid detection must use the canonicality-state index:\n{identity_plan}"
    );

    let payload_plan = sqlx::query_scalar::<_, String>(&format!(
        "EXPLAIN (FORMAT TEXT) {}",
        scans::FINAL_PAYLOAD_DIGEST_SCAN_SQL
    ))
    .bind(chain)
    .bind(0_i64)
    .bind(20_000_000_i64)
    .bind(address)
    .bind(vec![topic])
    .fetch_all(&mut *transaction)
    .await?
    .join("\n");
    eprintln!("final stored payload verification plan:\n{payload_plan}");
    assert!(
        payload_plan.contains(CANONICAL_EMITTER_BLOCK_INDEX),
        "canonical payload aggregation must use the exact-emitter partial index:\n{payload_plan}"
    );
    assert!(
        payload_plan.contains(RAW_LOG_STATE_INDEX),
        "final observed-row invalid detection must use the canonicality-state index:\n{payload_plan}"
    );

    transaction.rollback().await?;
    database.cleanup().await
}

#[tokio::test]
async fn unfenced_verification_scans_detect_in_range_mutations_before_acceptance() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_stored_verification_scan_fence"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for stored verification scan-fence test",
    )
    .await?;
    let chain = "scan-fence-chain";
    let address = "0x0000000000000000000000000000000000000001";
    let topic = format!("0x{:064x}", 1);
    let block_hash = format!("0x{:064x}", 42);
    sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state
        )
        VALUES (
            $1,
            $2,
            42,
            '0x0000000000000000000000000000000000000000000000000000000000000043',
            0,
            0,
            $3,
            ARRAY[$4::TEXT],
            decode('01', 'hex'),
            'canonical'::canonicality_state
        )
        "#,
    )
    .bind(chain)
    .bind(&block_hash)
    .bind(address)
    .bind(&topic)
    .execute(database.pool())
    .await?;
    let range = BackfillBlockRange::new(42, 42)?;

    for (phase, next_state) in [("identity", "safe"), ("payload", "finalized")] {
        let guard = acquire_raw_log_staging_read_guard(database.pool(), chain).await?;
        let scan_started_version = guard.version();
        guard.release().await?;
        let hook = scans::install_after_scan(database.pool(), phase).await;
        let scan_pool = database.pool().clone();
        let scan_topic = topic.clone();
        let mut scan = tokio::spawn(async move {
            let selector = ExactStoredSelector {
                chain: chain.to_owned(),
                source_family: "ens_v1_registry_l1".to_owned(),
                address: address.to_owned(),
                topic0s: vec![scan_topic],
            };
            match phase {
                "identity" => scans::scan_local_identity_buckets(&scan_pool, &selector, range)
                    .await
                    .map(|_| ()),
                "payload" => scans::scan_final_payload_digest(&scan_pool, &selector, range)
                    .await
                    .map(|_| ()),
                _ => unreachable!("unknown stored verification scan phase"),
            }
        });
        tokio::select! {
            () = hook.wait() => {}
            result = &mut scan => {
                panic!("stored verification scan returned before its race barrier: {result:?}");
            }
        }
        sqlx::query(
            r#"
            UPDATE raw_logs
            SET canonicality_state = $3::canonicality_state
            WHERE chain_id = $1
              AND block_hash = $2
            "#,
        )
        .bind(chain)
        .bind(&block_hash)
        .bind(next_state)
        .execute(database.pool())
        .await?;
        hook.resume();
        scan.await
            .context("stored verification scan task panicked")??;

        let mut guard = acquire_raw_log_staging_read_guard(database.pool(), chain).await?;
        assert_eq!(
            guard.version().retention_generation,
            scan_started_version.retention_generation
        );
        assert!(
            scans::range_changed_since(
                guard.connection_mut(),
                chain,
                scan_started_version.revision,
                range,
            )
            .await?,
            "the second {phase} fence must reject an in-range mutation committed while the scan was unfenced"
        );
        guard.release().await?;
    }

    database.cleanup().await
}

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
        local_bucket_evidence: Some(local.into_iter().map(|bucket| (bucket, true)).collect()),
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
        local_bucket_evidence: Some(vec![(partial_local_history, true)]),
    }
    .verify_provider_evidence(StoredLogIdentityEvidence {
        buckets: vec![provider_history],
        query_count: 1,
    })?;

    assert_eq!(plan.segments[0].source, VerifiedRangeSource::Provider);
    Ok(())
}

#[test]
fn unusable_local_lineage_requires_provider_fetch_even_when_aggregate_matches() -> Result<()> {
    let range = BackfillBlockRange::new(0, 32)?;
    let matching = StoredLogIdentityBucket {
        bucket: 0,
        selected_log_count: 1,
        digest_left: 7,
        digest_right: 11,
    };
    let plan = StoredVerificationPlan {
        segments: vec![VerifiedRangeSegment {
            range,
            source: VerifiedRangeSource::StoredCandidate,
        }],
        planned_raw_log_input_revision: Some(1),
        verification_range: Some(range),
        local_bucket_evidence: Some(vec![(matching, false)]),
    }
    .verify_provider_evidence(StoredLogIdentityEvidence {
        buckets: vec![matching],
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
