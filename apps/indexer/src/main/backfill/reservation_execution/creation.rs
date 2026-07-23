use super::*;

pub(crate) async fn create_hash_pinned_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
) -> Result<BackfillJobRecord> {
    let ranges = vec![BackfillRangeSpec {
        range_start_block_number: config.range.from_block,
        range_end_block_number: config.range.to_block,
    }];
    create_hash_pinned_backfill_job_with_ranges(pool, source_plan, config, ranges).await
}

pub(crate) async fn create_hash_pinned_backfill_job_with_progress(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<BackfillJobRecord> {
    let ranges = vec![BackfillRangeSpec {
        range_start_block_number: config.range.from_block,
        range_end_block_number: config.range.to_block,
    }];
    create_hash_pinned_backfill_job_with_ranges_with_progress(
        pool,
        source_plan,
        config,
        ranges,
        progress,
    )
    .await
}

pub(crate) async fn create_hash_pinned_backfill_job_with_ranges(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
    ranges: Vec<BackfillRangeSpec>,
) -> Result<BackfillJobRecord> {
    let source_identity = backfill_job_source_identity_payload(source_plan)?;
    create_hash_pinned_backfill_job_from_identity(
        pool,
        source_plan,
        config,
        ranges,
        source_identity,
    )
    .await
}

pub(crate) async fn create_hash_pinned_backfill_job_with_ranges_with_progress(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
    ranges: Vec<BackfillRangeSpec>,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<BackfillJobRecord> {
    let source_identity =
        backfill_job_source_identity_payload_with_progress(pool, source_plan, progress).await?;
    create_hash_pinned_backfill_job_from_identity(
        pool,
        source_plan,
        config,
        ranges,
        source_identity,
    )
    .await
}

async fn create_hash_pinned_backfill_job_from_identity(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
    ranges: Vec<BackfillRangeSpec>,
    source_identity: Value,
) -> Result<BackfillJobRecord> {
    let request = BackfillJobCreate {
        deployment_profile: config.deployment_profile.clone(),
        chain_id: source_plan.watched_chain_plan.chain.clone(),
        source_identity,
        scan_mode: HASH_PINNED_BACKFILL_SCAN_MODE.to_owned(),
        range_start_block_number: config.range.from_block,
        range_end_block_number: config.range.to_block,
        idempotency_key: config.idempotency_key.clone(),
        ranges,
    };
    if config.scope_idempotency_to_raw_log_retention_generation {
        create_generation_scoped_backfill_job(pool, &request).await
    } else {
        create_backfill_job(pool, &request).await
    }
}

pub(crate) async fn create_coinbase_sql_backfill_job(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
    coinbase_config: &CoinbaseSqlBackfillConfig,
    topic_plan: &BackfillTopicPlan,
) -> Result<BackfillJobRecord> {
    let ranges = vec![BackfillRangeSpec {
        range_start_block_number: config.range.from_block,
        range_end_block_number: config.range.to_block,
    }];
    create_coinbase_sql_backfill_job_with_ranges(
        pool,
        source_plan,
        config,
        coinbase_config,
        topic_plan,
        ranges,
    )
    .await
}

pub(crate) async fn create_coinbase_sql_backfill_job_with_ranges(
    pool: &sqlx::PgPool,
    source_plan: &WatchedSourceSelectorPlan,
    config: &BackfillJobRunConfig,
    coinbase_config: &CoinbaseSqlBackfillConfig,
    topic_plan: &BackfillTopicPlan,
    ranges: Vec<BackfillRangeSpec>,
) -> Result<BackfillJobRecord> {
    let request = BackfillJobCreate {
        deployment_profile: config.deployment_profile.clone(),
        chain_id: source_plan.watched_chain_plan.chain.clone(),
        source_identity: coinbase_sql_backfill_job_source_identity_payload(
            source_plan,
            coinbase_config,
            topic_plan,
        )?,
        scan_mode: COINBASE_SQL_BACKFILL_SCAN_MODE.to_owned(),
        range_start_block_number: config.range.from_block,
        range_end_block_number: config.range.to_block,
        idempotency_key: config.idempotency_key.clone(),
        ranges,
    };
    if config.scope_idempotency_to_raw_log_retention_generation {
        create_generation_scoped_backfill_job(pool, &request).await
    } else {
        create_backfill_job(pool, &request).await
    }
}

pub(crate) fn hash_pinned_backfill_range_specs(
    range: BackfillBlockRange,
    range_blocks: i64,
) -> Result<Vec<BackfillRangeSpec>> {
    if range_blocks <= 0 {
        bail!("hash-pinned backfill range blocks must be positive, got {range_blocks}");
    }

    let mut ranges = Vec::new();
    let mut range_start = range.from_block;
    while range_start <= range.to_block {
        let range_end = range_start
            .checked_add(range_blocks - 1)
            .unwrap_or(range.to_block)
            .min(range.to_block);
        ranges.push(BackfillRangeSpec {
            range_start_block_number: range_start,
            range_end_block_number: range_end,
        });
        if range_end == range.to_block {
            break;
        }
        range_start = range_end
            .checked_add(1)
            .context("hash-pinned backfill range start overflowed while partitioning")?;
    }

    Ok(ranges)
}

pub(crate) fn coinbase_sql_backfill_job_source_identity_payload(
    source_plan: &WatchedSourceSelectorPlan,
    coinbase_config: &CoinbaseSqlBackfillConfig,
    topic_plan: &BackfillTopicPlan,
) -> Result<Value> {
    let mut payload = if coinbase_sql_uses_basenames_registry_scan_all(source_plan, topic_plan) {
        coinbase_sql_basenames_registry_scan_all_source_identity_payload(source_plan)?
    } else {
        if watched_source_plan_uses_basenames_registry_scan_all(source_plan) {
            // A registry-family plan whose Coinbase SQL topic plan is empty
            // would fetch address-scoped, so the hash-pinned scan-all
            // identity (which asserts a topics-complete scan) must not be
            // minted for it.
            bail!(
                "Coinbase SQL registry-family backfill has an empty topic plan; \
                 refusing to mint a scan-all identity for an address-scoped fetch"
            );
        }
        backfill_job_source_identity_payload(source_plan)?
    };
    let object = payload
        .as_object_mut()
        .context("backfill source identity payload must be an object")?;
    object.insert(
        "backfill_provider".to_owned(),
        Value::String("coinbase_cdp_sql".to_owned()),
    );
    object.insert(
        "scan_mode".to_owned(),
        Value::String(COINBASE_SQL_BACKFILL_SCAN_MODE.to_owned()),
    );
    object.insert(
        "coinbase_sql_plan_version".to_owned(),
        Value::String("base_logs_v2".to_owned()),
    );
    object.insert("validation_provider_required".to_owned(), Value::Bool(true));
    object.insert(
        "coinbase_sql_validation_mode".to_owned(),
        Value::String(coinbase_config.validation_mode.as_str().to_owned()),
    );
    object.insert(
        "topic_filtering".to_owned(),
        Value::String("manifest_abi_topic0_union_v1".to_owned()),
    );
    object.insert(
        "coinbase_sql_topic_plan".to_owned(),
        topic_plan.source_identity_payload()?,
    );
    payload
        .as_object_mut()
        .context("backfill source identity payload must be an object")?
        .remove("source_identity_hash");
    let source_identity_hash =
        keccak256_json_digest(&payload).context("failed to digest Coinbase SQL source identity")?;
    payload
        .as_object_mut()
        .context("backfill source identity payload must be an object")?
        .insert(
            "source_identity_hash".to_owned(),
            Value::String(source_identity_hash),
        );

    Ok(payload)
}
