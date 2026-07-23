#[tokio::test]
async fn scoped_ens_v2_registry_sync_emits_registry_permission_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let registry_contract_instance_id = Uuid::from_u128(0x341);
    let registry_address = "0x0000000000000000000000000000000000000341";
    let account = "0x00000000000000000000000000000000000000aa";
    let block = provider_block(
        "0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
        Some("0xbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc"),
        63,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        1,
        "ens",
        "ens_v2_registry_l1",
        chain,
        "ens_v2",
        registry_contract_instance_id,
        registry_address,
        "registry",
    )
    .await?;
    sqlx::query("UPDATE manifest_versions SET manifest_payload = $2 WHERE manifest_id = $1")
        .bind(1_i64)
        .bind(test_manifest_payload())
        .execute(database.pool())
        .await?;
    insert_raw_resolver_log(
        database.pool(),
        chain,
        &block,
        registry_address,
        vec![
            ens_v2_eac_roles_changed_topic0(),
            hex_string(&abi_word_u64(0)),
            hex_string(&abi_word_address(account)),
        ],
        decode_hex_string(&encode_eac_roles_changed_log_data(
            &hex_string(&abi_word_u64(0)),
            &hex_string(&abi_word_u64(1)),
        )),
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    sync_adapter_state_from_scoped_persisted_raw_payloads(
        database.pool(),
        chain,
        std::slice::from_ref(&block.block_hash),
        &[(
            "ens_v2_registry_l1".to_owned(),
            registry_address.to_owned(),
            block.block_number,
            block.block_number,
        )],
    )
    .await?;

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE derivation_kind = 'ens_v2_permissions' AND event_kind IN ('PermissionChanged', 'RootPermissionChanged')"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "a registry-scoped adapter run must not skip the permission adapter"
    );

    database.cleanup().await
}

#[tokio::test]
async fn sync_adapter_owned_raw_log_state_backfills_reverse_claims_from_stored_raw_logs()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x342);
    let reverse_address = "0x00000000000000000000000000000000000000ae";
    let claimed_address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";
    let stored_block = provider_block(
        "0xdededededededededededededededededededededededededededededededede",
        Some("0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"),
        64,
    );

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                1,
                1,
                'ens',
                'ens_v1_reverse_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for reverse runtime bootstrap test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        "ethereum-mainnet",
        &stored_block,
        reverse_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'address' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        claimed_address.to_ascii_lowercase()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'namespace' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_name' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_name_for_address(claimed_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'source_family' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "ens_v1_reverse_l1".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'contract_role' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        REVERSE_REGISTRAR_ROLE.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'contract_instance_id' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_contract_instance_id.to_string()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->'claim_provenance'->>'emitting_address' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_address.to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT raw_fact_ref->>'block_hash' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        stored_block.block_hash
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn live_adapter_sync_continues_after_block_derived_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x343);
    let reverse_address = "0x00000000000000000000000000000000000000af";
    let claimed_address = "0x1111111111111111111111111111111111111111";
    let stored_block = provider_block(
        "0xdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdfdf",
        Some("0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"),
        65,
    );

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                11,
                1,
                'ens',
                'ens_v1_reverse_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for live adapter sync test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "ethereum-mainnet",
        reverse_address,
        Some(11),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        11,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        "ethereum-mainnet",
        &stored_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        "ethereum-mainnet",
        &stored_block,
        reverse_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let heartbeat_instance_id = "live-adapter-in-flight-progress-test";
    install_stale_indexer_heartbeat(database.pool(), heartbeat_instance_id).await?;
    let (mut progress, progress_handle) = BlockingHeartbeatProgress::new(
        heartbeat_instance_id,
        vec!["ethereum-mainnet".to_owned()],
        2,
    );
    let mut progress_ref = Some(&mut progress as &mut dyn bigname_adapters::StartupAdapterProgress);
    let mut operation = Box::pin(sync_live_adapter_state_from_persisted_raw_payloads_with_progress(
        database.pool(),
        "test",
        "ethereum-mainnet",
        std::slice::from_ref(&stored_block.block_hash),
        &mut progress_ref,
    ));
    tokio::time::timeout(tokio::time::Duration::from_secs(10), async {
        tokio::select! {
            () = progress_handle.wait_until_blocked() => Ok(()),
            result = operation.as_mut() => Err(anyhow::anyhow!(
                "live adapter sync completed before its later progress boundary blocked: {result:?}"
            )),
        }
    })
    .await
    .context("live adapter sync did not reach its later progress boundary")??;
    let heartbeat = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        heartbeat_instance_id,
    )
    .await?
    .context("live adapter progress heartbeat must remain registered")?;
    assert!(
        heartbeat.age_seconds <= 1,
        "an earlier adapter page must beat before later family work finishes"
    );
    progress_handle.resume();
    let summary = tokio::time::timeout(tokio::time::Duration::from_secs(10), operation.as_mut())
        .await
        .context("live adapter sync did not finish after progress resumed")??;
    drop(operation);

    assert!(
        progress_handle.record_count() >= 6,
        "block-derived loading plus reverse-claim loading, decoding, persistence, and family completion must each report live progress"
    );

    assert_eq!(summary.total_synced_count, 1);
    assert_eq!(
        summary.resolver_profile_authority_epoch_guard_count, 1,
        "ordinary live sync must run its cheap per-chain discovery-epoch guard"
    );
    assert_eq!(
        summary.resolver_profile_authority_scan_count, 0,
        "an ordinary live block with no discovery mutation must not scan global resolver authority"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn live_adapter_retry_recovers_committed_discovery_after_post_mutation_failure() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let manifests = TestManifestDir::new()?;
    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let resolver_address = "0x0000000000000000000000000000000000000abc";
    let registry_manifest = format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "ENSRegistry"
address = "{registry_address}"

[[contracts]]
role = "registry"
address = "{registry_address}"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "resolver"
from_role = "registry"
admission = "reachable_from_root"

{}
"#,
        test_manifest_abi_toml()
    );
    manifests.write_manifest_for_source_family("ens_v1_registry_l1", &registry_manifest)?;
    let resolver_manifest = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../manifests/mainnet/ethereum/ens/ens_v1_resolver_l1/v1.toml"),
    )?;
    manifests.write_manifest_for_source_family("ens_v1_resolver_l1", &resolver_manifest)?;
    let repository = load_manifest_repository(&manifests.path)?;
    build_manifest_runtime_state(database.pool(), &repository).await?;
    sqlx::query(
        r#"
        UPDATE resolver_profile_input_changes
        SET processed_generation = generation,
            force_reconciliation = FALSE
        "#,
    )
    .execute(database.pool())
    .await?;

    let block = provider_block(
        "0xabababababababababababababababababababababababababababababababab",
        Some("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"),
        22_800_000,
    );
    upsert_raw_blocks(
        database.pool(),
        &[provider_block_to_raw_block(
            "ethereum-mainnet",
            &block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            transaction_hash: transaction_hash_for_block(&block),
            transaction_index: 0,
            log_index: 0,
            emitting_address: registry_address.to_ascii_lowercase(),
            topics: vec![
                registry_new_resolver_topic0(),
                "0x0000000000000000000000000000000000000000000000000000000000000001".to_owned(),
            ],
            data: decode_hex_string(&encode_registry_new_resolver_log_data(resolver_address)),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let journal_before =
        bigname_storage::load_resolver_profile_authority_journal(database.pool()).await?;
    let _failure_hook = install_post_discovery_mutation_failure_for_test(database.pool()).await?;
    let error = sync_live_adapter_state_from_persisted_raw_payloads(
        database.pool(),
        "test",
        "ethereum-mainnet",
        std::slice::from_ref(&block.block_hash),
    )
    .await
    .expect_err("the injected post-mutation failure must escape before journaling");
    assert!(error.to_string().contains("injected failure"));
    assert_eq!(
        bigname_storage::load_resolver_profile_authority_journal(database.pool())
            .await?
            .revision,
        journal_before.revision,
        "the injected crash window must leave the older durable snapshot"
    );

    let retry = sync_live_adapter_state_from_persisted_raw_payloads(
        database.pool(),
        "test",
        "ethereum-mainnet",
        std::slice::from_ref(&block.block_hash),
    )
    .await?;
    assert_eq!(
        retry.resolver_profile_authority_scan_count, 1,
        "the no-op retry must turn prior epoch drift into one full authority diff"
    );
    assert_eq!(
        sqlx::query_as::<_, (bool, bool)>(
            r#"
            SELECT
                processed_generation < generation AS pending,
                force_reconciliation
            FROM resolver_profile_input_changes
            WHERE chain_id = 'ethereum-mainnet'
              AND contract_address = lower($1)
            "#,
        )
        .bind(resolver_address)
        .fetch_one(database.pool())
        .await?,
        (true, true),
        "the recovered journal diff must retain the newly discovered resolver target"
    );

    let no_op = sync_live_adapter_state_from_persisted_raw_payloads(
        database.pool(),
        "test",
        "ethereum-mainnet",
        std::slice::from_ref(&block.block_hash),
    )
    .await?;
    assert_eq!(no_op.resolver_profile_authority_scan_count, 0);
    assert!(no_op.resolver_profile_authority_epoch_guard_count >= 1);

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn post_replay_live_adapter_backlog_latches_tail_before_live_sync_resumes() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x344);
    let reverse_address = "0x00000000000000000000000000000000000000b1";
    let replay_target_claimed_address = "0x2222222222222222222222222222222222222222";
    let backlog_claimed_address = "0x3333333333333333333333333333333333333333";
    let future_claimed_address = "0x4444444444444444444444444444444444444444";
    let replay_target_block = provider_block(
        "0x1010101010101010101010101010101010101010101010101010101010101010",
        Some("0x0909090909090909090909090909090909090909090909090909090909090909"),
        10,
    );
    let backlog_block = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        Some(&replay_target_block.block_hash),
        11,
    );
    let future_block = provider_block(
        "0x1212121212121212121212121212121212121212121212121212121212121212",
        Some(&backlog_block.block_hash),
        12,
    );

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                12,
                1,
                'ens',
                'ens_v1_reverse_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for post-replay backlog test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        chain,
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        chain,
        reverse_address,
        Some(12),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        12,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_completed_block_number,
            last_replayed_at,
            raw_log_input_revision,
            raw_log_retention_generation
        )
        VALUES ('mainnet', $1, 'raw_fact_normalized_events', 1, 11, 10, 10, now(), 5, 0)
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await
    .context("failed to seed completed normalized replay cursor")?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &replay_target_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &backlog_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &replay_target_block,
        reverse_address,
        replay_target_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &backlog_block,
        reverse_address,
        backlog_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    upsert_raw_staging_input_version_for_handoff_test(database.pool(), chain, 5, 0).await?;
    upsert_raw_staging_block_revision_for_handoff_test(
        database.pool(),
        chain,
        &replay_target_block.block_hash,
        replay_target_block.block_number,
        5,
    )
    .await?;
    upsert_raw_staging_block_revision_for_handoff_test(
        database.pool(),
        chain,
        &backlog_block.block_hash,
        backlog_block.block_number,
        5,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile, chain_id, cursor_kind,
            range_start_block_number, next_block_number, target_block_number,
            last_completed_block_number, last_replayed_at
        )
        VALUES ('mainnet', $1, 'post_replay_live_adapter_backlog', 11, 12, 11, 11, now())
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await
    .context("failed to seed a legacy version-zero post-replay backlog cursor")?;

    let publication_hook =
        install_backlog_after_adapter_sync_test_hook(database.pool(), "mainnet", chain).await;
    let pool = database.pool().clone();
    let backlog = tokio::spawn(async move {
        sync_live_adapter_backlog_after_normalized_replay(&pool, "mainnet", &[chain.to_owned()])
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        publication_hook.wait_until_after_adapter_sync(),
    )
    .await
    .context("post-replay backlog did not reach its page-publication barrier")?;
    let mut replacement = database.pool().begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("raw_log_staging:{chain}"))
        .execute(replacement.as_mut())
        .await?;
    sqlx::query(
        "UPDATE raw_logs SET canonicality_state = 'safe' WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(chain)
    .bind(&backlog_block.block_hash)
    .execute(replacement.as_mut())
    .await?;
    sqlx::query("UPDATE raw_log_staging_input_revisions SET revision = 6 WHERE chain_id = $1")
        .bind(chain)
        .execute(replacement.as_mut())
        .await?;
    sqlx::query(
        "UPDATE raw_log_staging_block_revisions SET revision = 6 WHERE chain_id = $1 AND block_hash = $2",
    )
    .bind(chain)
    .bind(&backlog_block.block_hash)
    .execute(replacement.as_mut())
    .await?;
    replacement.commit().await?;
    publication_hook.resume();
    let summary = tokio::time::timeout(std::time::Duration::from_secs(10), backlog)
        .await
        .context("post-replay backlog did not resume after page-publication barrier")?
        .context("post-replay backlog task panicked")??;
    assert_eq!(summary.selected_block_count, 1);
    assert_eq!(summary.normalized_event_synced_count, 1);
    assert_eq!(summary.awaiting_replay_chain_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT raw_fact_ref->>'block_hash' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        backlog_block.block_hash
    );

    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &future_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &future_block,
        reverse_address,
        future_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let second_summary = sync_live_adapter_backlog_after_normalized_replay(
        database.pool(),
        "mainnet",
        &[chain.to_owned()],
    )
    .await?;
    assert_eq!(second_summary.selected_block_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64)>(
            r#"
            SELECT next_block_number, target_block_number, raw_log_input_revision
            FROM normalized_replay_cursors
            WHERE deployment_profile = 'mainnet'
              AND chain_id = $1
              AND cursor_kind = 'post_replay_live_adapter_backlog'
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (13, 12, 6),
        "the legacy cursor must reset to the replay baseline, retry a raced page, and retain the accepted revision"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn post_replay_final_latch_rejects_raw_changes_after_backlog_completion() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let target = 10;

    let replay_stale_chain = "replay-stale";
    insert_ready_replay_and_backlog_cursors_for_handoff_test(
        database.pool(),
        replay_stale_chain,
        target,
        1,
        0,
    )
    .await?;
    upsert_raw_staging_input_version_for_handoff_test(database.pool(), replay_stale_chain, 2, 0)
        .await?;
    upsert_raw_staging_block_revision_for_handoff_test(
        database.pool(),
        replay_stale_chain,
        "0xreplay-stale",
        target,
        2,
    )
    .await?;
    let mut replay_latched = true;
    let replay_status = latch_replay_handoff_if_stable(
        database.pool(),
        "mainnet",
        &[replay_stale_chain.to_owned()],
        &mut replay_latched,
    )
    .await?;
    assert_eq!(replay_status, ReplayHandoffLatchStatus::AwaitingReplay);
    assert!(
        !replay_latched,
        "a post-backlog mutation through the replay target must prevent the ownership latch"
    );

    let consumed_backlog_chain = "consumed-backlog-stale";
    insert_ready_replay_and_backlog_cursors_for_handoff_test(
        database.pool(),
        consumed_backlog_chain,
        target,
        1,
        0,
    )
    .await?;
    upsert_raw_staging_input_version_for_handoff_test(
        database.pool(),
        consumed_backlog_chain,
        2,
        0,
    )
    .await?;
    upsert_raw_staging_block_revision_for_handoff_test(
        database.pool(),
        consumed_backlog_chain,
        "0xconsumed-backlog-stale",
        target + 1,
        2,
    )
    .await?;
    let mut backlog_latched = true;
    let backlog_status = latch_replay_handoff_if_stable(
        database.pool(),
        "mainnet",
        &[consumed_backlog_chain.to_owned()],
        &mut backlog_latched,
    )
    .await?;
    assert_eq!(backlog_status, ReplayHandoffLatchStatus::AwaitingBacklog);
    assert!(
        !backlog_latched,
        "a replacement in the consumed post-target range must force backlog rewind before latch"
    );

    let new_tail_chain = "new-tail";
    insert_ready_replay_and_backlog_cursors_for_handoff_test(
        database.pool(),
        new_tail_chain,
        target,
        1,
        0,
    )
    .await?;
    let new_tail_block = provider_block(
        "0x1717171717171717171717171717171717171717171717171717171717171717",
        Some("0x1616161616161616161616161616161616161616161616161616161616161616"),
        target + 2,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        new_tail_chain,
        &new_tail_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        new_tail_chain,
        &new_tail_block,
        "0x0000000000000000000000000000000000000017",
        "0x1717171717171717171717171717171717171717",
        CanonicalityState::Canonical,
    )
    .await?;
    upsert_raw_staging_input_version_for_handoff_test(database.pool(), new_tail_chain, 2, 0)
        .await?;
    upsert_raw_staging_block_revision_for_handoff_test(
        database.pool(),
        new_tail_chain,
        &new_tail_block.block_hash,
        new_tail_block.block_number,
        2,
    )
    .await?;
    let mut tail_latched = true;
    let tail_status = latch_replay_handoff_if_stable(
        database.pool(),
        "mainnet",
        &[new_tail_chain.to_owned()],
        &mut tail_latched,
    )
    .await?;
    assert_eq!(tail_status, ReplayHandoffLatchStatus::AwaitingBacklog);
    assert!(
        !tail_latched,
        "a newly committed higher post-target block must be backlogged before latch"
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_handoff_multi_chain_fence_uses_one_connection_and_orders_writers_after_latch()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chains = vec!["alpha-chain".to_owned(), "beta-chain".to_owned()];
    for chain in &chains {
        insert_ready_replay_and_backlog_cursors_for_handoff_test(database.pool(), chain, 10, 1, 0)
            .await?;
        upsert_raw_staging_input_version_for_handoff_test(database.pool(), chain, 1, 0).await?;
    }

    let single_connection_pool = database.additional_pool(1).await?;
    let lock_probe_pool = database.additional_pool(3).await?;
    let latch_hook =
        install_replay_handoff_before_latch_test_hook(&single_connection_pool, "mainnet").await;
    let latch_pool = single_connection_pool.clone();
    let latch_chains = chains.clone();
    let latch = tokio::spawn(async move {
        let mut latched = false;
        let status =
            latch_replay_handoff_if_stable(&latch_pool, "mainnet", &latch_chains, &mut latched)
                .await;
        (status, latched)
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        latch_hook.wait_until_before_latch(),
    )
    .await
    .context("multi-chain handoff did not reach its guarded latch barrier")?;

    let beta_writer = tokio::spawn(commit_raw_revision_after_handoff_fence_for_test(
        database.pool().clone(),
        chains[1].clone(),
        12,
    ));

    let mut alpha_lock_probe = lock_probe_pool.begin().await?;
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("raw_log_staging:{}", chains[0]))
            .fetch_one(alpha_lock_probe.as_mut())
            .await?,
        "the final all-chain fence must own the alpha chain mutation lock"
    );
    let mut beta_lock_probe = lock_probe_pool.begin().await?;
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("raw_log_staging:{}", chains[1]))
            .fetch_one(beta_lock_probe.as_mut())
            .await?,
        "the final all-chain fence must own the beta chain mutation lock"
    );
    let mut unrelated_lock_probe = lock_probe_pool.begin().await?;
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind("raw_log_staging:unrelated-chain")
            .fetch_one(unrelated_lock_probe.as_mut())
            .await?,
        "the all-chain fence must not stop raw writers for unrelated chains"
    );
    alpha_lock_probe.rollback().await?;
    beta_lock_probe.rollback().await?;
    unrelated_lock_probe.rollback().await?;

    latch_hook.resume();
    let (status, latched) = tokio::time::timeout(std::time::Duration::from_secs(10), latch)
        .await
        .context("multi-chain handoff did not resume after its latch barrier")?
        .context("multi-chain handoff task panicked")?;
    assert_eq!(status?, ReplayHandoffLatchStatus::Latched);
    assert!(
        latched,
        "the ownership flag must flip before the fence releases"
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), beta_writer)
        .await
        .context("beta writer remained blocked after handoff fence release")?
        .context("beta writer task panicked")??;

    let mut next_cycle_latched = true;
    let next_cycle_status = latch_replay_handoff_if_stable(
        database.pool(),
        "mainnet",
        &[chains[1].clone()],
        &mut next_cycle_latched,
    )
    .await?;
    assert_eq!(
        next_cycle_status,
        ReplayHandoffLatchStatus::AwaitingBacklog,
        "the next handoff cycle must reject a post-fence raw-only commit"
    );
    assert!(!next_cycle_latched);

    let backlog_summary = sync_live_adapter_backlog_after_normalized_replay(
        database.pool(),
        "mainnet",
        &[chains[1].clone()],
    )
    .await?;
    assert_eq!(backlog_summary.selected_block_count, 1);
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i64)>(
            r#"
            SELECT next_block_number, target_block_number, raw_log_input_revision
            FROM normalized_replay_cursors
            WHERE deployment_profile = 'mainnet'
              AND chain_id = $1
              AND cursor_kind = 'post_replay_live_adapter_backlog'
            "#,
        )
        .bind(&chains[1])
        .fetch_one(database.pool())
        .await?,
        (13, 12, 2),
        "the renewed cycle must consume the post-fence raw-only block"
    );
    let renewed_status = latch_replay_handoff_if_stable(
        database.pool(),
        "mainnet",
        &[chains[1].clone()],
        &mut next_cycle_latched,
    )
    .await?;
    assert_eq!(renewed_status, ReplayHandoffLatchStatus::Latched);
    assert!(next_cycle_latched);

    lock_probe_pool.close().await;
    single_connection_pool.close().await;
    database.cleanup().await
}

#[tokio::test]
async fn post_replay_handoff_fetches_provider_gap_after_backlog() -> Result<()> {
    #[derive(Default)]
    struct CountingBacklogProgress(usize);

    impl bigname_adapters::StartupAdapterProgress for CountingBacklogProgress {
        fn record<'a>(
            &'a mut self,
            _pool: &'a PgPool,
        ) -> bigname_adapters::StartupAdapterProgressFuture<'a> {
            Box::pin(async move {
                self.0 += 1;
                Ok(())
            })
        }
    }

    let database = TestDatabase::new().await?;
    create_normalized_replay_cursor_table(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x345);
    let reverse_address = "0x00000000000000000000000000000000000000b2";
    let backlog_claimed_address = "0x5555555555555555555555555555555555555555";
    let live_claimed_address = "0x6666666666666666666666666666666666666666";
    let replay_target_block = provider_block(
        "0x1313131313131313131313131313131313131313131313131313131313131313",
        Some("0x0909090909090909090909090909090909090909090909090909090909090909"),
        10,
    );
    let backlog_block = provider_block(
        "0x1414141414141414141414141414141414141414141414141414141414141414",
        Some(&replay_target_block.block_hash),
        11,
    );
    let live_gap_block = provider_block(
        "0x1515151515151515151515151515151515151515151515151515151515151515",
        Some(&backlog_block.block_hash),
        12,
    );
    let live_head_block = provider_block(
        "0x1616161616161616161616161616161616161616161616161616161616161616",
        Some(&live_gap_block.block_hash),
        13,
    );

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES (
                13,
                1,
                'ens',
                'ens_v1_reverse_l1',
                'ethereum-mainnet',
                'ens_v1',
                'active',
                'ensip15@ens-normalize-0.1.1',
                'manifests/ens/ens_v1_reverse_l1/v1.toml',
                DEFAULT
            )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for post-replay handoff test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        chain,
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        chain,
        reverse_address,
        Some(13),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        13,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_cursors (
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_completed_block_number
        )
        VALUES ('mainnet', $1, 'raw_fact_normalized_events', 1, 11, 10, 10)
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await
    .context("failed to seed completed normalized replay cursor")?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &replay_target_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &backlog_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &backlog_block,
        reverse_address,
        backlog_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let mut progress = CountingBacklogProgress::default();
    let summary = sync_live_adapter_backlog_after_normalized_replay_with_progress(
        database.pool(),
        "mainnet",
        &[chain.to_owned()],
        &mut progress,
    )
    .await?;
    assert_eq!(summary.selected_block_count, 1);
    assert!(
        progress.0 > 1,
        "backlog adapter work and durable cursor publication must each report progress"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    let (provider, server) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            block: live_gap_block.clone(),
            logs: vec![],
        },
        ProviderBlockFixture {
            block: live_head_block.clone(),
            logs: vec![rpc_reverse_claimed_log_payload(
                &live_head_block,
                reverse_address,
                live_claimed_address,
                0,
            )],
        },
    ])
    .await?;
    let task = IntakeChainTask {
        chain: chain.to_owned(),
        addresses: vec![reverse_address.to_owned()],
        manifest_root_entry_count: 0,
        manifest_contract_entry_count: 1,
        discovery_edge_entry_count: 0,
        checkpoint: ChainCheckpoint {
            chain_id: chain.to_owned(),
            canonical_block_hash: Some(backlog_block.block_hash.clone()),
            canonical_block_number: Some(backlog_block.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
    };
    let (next_task, outcome) = reconcile_fetched_heads_with_adapter_sync(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: live_head_block.clone(),
            safe: None,
            finalized: None,
        },
        true,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await?
    .expect("provider gap reconciliation must update the live checkpoint");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::GapBackfilled
    );
    assert_eq!(
        next_task.checkpoint.canonical_block_number,
        Some(live_head_block.block_number)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM raw_logs WHERE chain_id = $1 AND block_hash = $2"
        )
        .bind(chain)
        .bind(&live_head_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT raw_fact_ref->>'block_hash'
            FROM normalized_events
            WHERE event_kind = 'ReverseChanged'
              AND raw_fact_ref->>'block_hash' = $1
            "#
        )
        .bind(&live_head_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        live_head_block.block_hash
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_adapter_owned_raw_log_state_backfills_wrapper_authority_from_stored_raw_logs()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let wrapper_contract_instance_id = Uuid::from_u128(0x352);
    let registry_contract_instance_id = Uuid::from_u128(0x353);
    let wrapper_address = "0x00000000000000000000000000000000000000dd";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let orphan_block = provider_block(
        "0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
        Some("0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"),
        63,
    );
    let stored_block = provider_block(
        "0xdededededededededededededededededededededededededededededededede",
        Some(&orphan_block.block_hash),
        64,
    );
    let dns_name = dns_encoded_eth_name("wrapped");
    let wrapped_namehash = namehash_for_dns_name(&dns_name);
    let transaction_hash = transaction_hash_for_block(&stored_block);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES
                (
                    1,
                    1,
                    'ens',
                    'ens_v1_wrapper_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_wrapper_l1/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'ens',
                    'ens_v1_registry_l1',
                    'ethereum-mainnet',
                    'ens_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/ens/ens_v1_registry_l1/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for wrapper runtime bootstrap test")?;
    insert_contract_instance(
        database.pool(),
        wrapper_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        wrapper_contract_instance_id,
        "ethereum-mainnet",
        wrapper_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "ethereum-mainnet",
        registry_address,
        Some(2),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "name_wrapper",
        wrapper_contract_instance_id,
        wrapper_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &orphan_block,
                CanonicalityState::Orphaned,
            ),
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &stored_block,
                CanonicalityState::Canonical,
            ),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: orphan_block.block_hash.clone(),
                block_number: orphan_block.block_number,
                transaction_hash: transaction_hash_for_block(&orphan_block),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"NameWrapped(bytes32,bytes,address,uint32,uint64)"),
                    wrapped_namehash.clone(),
                ],
                data: decode_hex_string(&encode_name_wrapped_log_data(&dns_name)),
                canonicality_state: CanonicalityState::Orphaned,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: wrapper_address.to_owned(),
                topics: vec![
                    keccak256_hex(b"NameWrapped(bytes32,bytes,address,uint32,uint64)"),
                    wrapped_namehash.clone(),
                ],
                data: decode_hex_string(&encode_name_wrapped_log_data(&dns_name)),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash,
                transaction_index: 0,
                log_index: 1,
                emitting_address: registry_address.to_owned(),
                topics: vec![registry_new_resolver_topic0(), wrapped_namehash],
                data: decode_hex_string(&encode_registry_new_resolver_log_data(
                    "0x00000000000000000000000000000000000000cc",
                )),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;
    sqlx::query(
        r#"
        CREATE TABLE service_loop_heartbeats (
            service_name TEXT NOT NULL,
            instance_id TEXT NOT NULL,
            scope_kind TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            heartbeat_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (service_name, instance_id, scope_kind, scope_id)
        )
        "#,
    )
    .execute(database.pool())
    .await?;
    let heartbeat_instance_id = "live-adapter-page-heartbeat-test";
    bigname_storage::register_service_loop(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        heartbeat_instance_id,
    )
    .await?;
    let heartbeat_chain_ids = watched_plan
        .iter()
        .map(|chain| chain.chain.clone())
        .collect::<Vec<_>>();
    let mut heartbeat = crate::run::startup_heartbeat::StartupHeartbeat::new(
        heartbeat_instance_id.to_owned(),
        tokio::time::Duration::ZERO,
    );
    sync_adapter_owned_raw_log_state_with_heartbeat(
        database.pool(),
        "test",
        &watched_plan,
        DEFAULT_STARTUP_DISCOVERY_PAGE_LOGS,
        &mut heartbeat,
        &heartbeat_chain_ids,
    )
    .await?;
    assert!(
        heartbeat.adapter_progress_count() > 0,
        "a live full-family re-sync must heartbeat from inside checkpointed adapter work"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_replay_adapter_checkpoints
             WHERE deployment_profile = 'test'
               AND checkpoint_scope = 'startup_adapter_sync'",
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "the broad startup pass must clean both completed ENSv1 checkpoint families"
    );

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM resources WHERE provenance->>'authority_kind' = 'wrapper'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT logical_name_id FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "ens:wrapped.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'resolver' FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "0x00000000000000000000000000000000000000cc".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1"
        )
        .bind(orphan_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        7
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_adapter_owned_raw_log_state_backfills_basenames_reverse_claims_and_authority_from_stored_raw_logs()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let reverse_contract_instance_id = Uuid::from_u128(0x361);
    let registrar_contract_instance_id = Uuid::from_u128(0x362);
    let registry_contract_instance_id = Uuid::from_u128(0x363);
    let resolver_contract_instance_id = Uuid::from_u128(0x364);
    let reverse_address = "0x0000000000d8e504002cc26e3ec46d81971c1664";
    let registrar_address = "0x03c4738ee98ae44591e1a4a4f3cab6641d95dd9a";
    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let resolver_address = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
    let claimed_address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";
    let stored_block = provider_block(
        "0xdededededededededededededededededededededededededededededededede",
        Some("0xefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef"),
        64,
    );
    let alice_namehash = namehash_for_dns_name(&dns_encoded_base_eth_name("alice"));
    let transaction_hash = transaction_hash_for_block(&stored_block);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (
                manifest_id,
                manifest_version,
                namespace,
                source_family,
                chain,
                deployment_epoch,
                rollout_status,
                normalizer_version,
                file_path,
                manifest_payload
            )
            VALUES
                (
                    1,
                    1,
                    'basenames',
                    'basenames_base_primary',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_primary/v1.toml',
                    DEFAULT
                ),
                (
                    2,
                    1,
                    'basenames',
                    'basenames_base_registrar',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registrar/v1.toml',
                    DEFAULT
                ),
                (
                    3,
                    1,
                    'basenames',
                    'basenames_base_registry',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_registry/v1.toml',
                    DEFAULT
                ),
                (
                    4,
                    1,
                    'basenames',
                    'basenames_base_resolver',
                    'base-mainnet',
                    'basenames_v1',
                    'active',
                    'ensip15@ens-normalize-0.1.1',
                    'manifests/basenames/basenames_base_resolver/v1.toml',
                    DEFAULT
                )
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for Basenames runtime bootstrap test")?;
    insert_contract_instance(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registrar_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        "root",
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        reverse_contract_instance_id,
        "base-mainnet",
        reverse_address,
        Some(1),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registrar_contract_instance_id,
        "base-mainnet",
        registrar_address,
        Some(2),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        "base-mainnet",
        registry_address,
        Some(3),
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        resolver_contract_instance_id,
        "base-mainnet",
        resolver_address,
        Some(4),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "reverse_registrar",
        reverse_contract_instance_id,
        reverse_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registrar",
        registrar_contract_instance_id,
        registrar_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        3,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        3,
        "registry",
        registry_contract_instance_id,
        registry_address,
        "none",
        None,
        None,
    )
    .await?;
    insert_manifest_discovery_rule(
        database.pool(),
        3,
        "resolver",
        "registry",
        "reachable_from_root",
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        4,
        "resolver",
        resolver_contract_instance_id,
        resolver_address,
        "none",
        None,
        None,
    )
    .await?;

    upsert_raw_blocks(
        database.pool(),
        &[provider_block_to_raw_block(
            "base-mainnet",
            &stored_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: reverse_address.to_owned(),
                topics: vec![
                    name_for_addr_changed_topic0(),
                    hex_string(&abi_word_address(claimed_address)),
                ],
                data: decode_hex_string(&encode_dynamic_string_log_data("alice.base.eth")),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: registrar_address.to_owned(),
                topics: vec![
                    basenames_name_registered_topic0(),
                    labelhash_hex("alice"),
                    hex_string(&abi_word_address(
                        "0x0000000000000000000000000000000000000001",
                    )),
                ],
                data: decode_hex_string(&encode_basenames_name_registered_log_data(
                    "alice",
                    1_700_010_000,
                )),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 2,
                emitting_address: registry_address.to_owned(),
                topics: vec![registry_new_resolver_topic0(), alice_namehash.clone()],
                data: decode_hex_string(&encode_registry_new_resolver_log_data(resolver_address)),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash: transaction_hash.clone(),
                transaction_index: 0,
                log_index: 3,
                emitting_address: resolver_address.to_owned(),
                topics: vec![
                    resolver_text_changed_with_value_topic0(),
                    alice_namehash.clone(),
                    keccak256_hex(b"com.twitter"),
                ],
                data: decode_hex_string(&encode_two_dynamic_string_log_data(
                    "com.twitter",
                    "alice",
                )),
                canonicality_state: CanonicalityState::Canonical,
            },
            RawLog {
                chain_id: "base-mainnet".to_owned(),
                block_hash: stored_block.block_hash.clone(),
                block_number: stored_block.block_number,
                transaction_hash,
                transaction_index: 0,
                log_index: 4,
                emitting_address: resolver_address.to_owned(),
                topics: vec![resolver_version_changed_topic0(), alice_namehash],
                data: decode_hex_string(&encode_resolver_version_changed_log_data(7)),
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;
    sync_adapter_owned_raw_log_state(database.pool(), &watched_plan).await?;

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged' AND namespace = 'basenames'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT source_family FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "basenames_base_primary".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM name_surfaces")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT logical_name_id FROM name_surfaces LIMIT 1")
            .fetch_one(database.pool())
            .await?,
        "basenames:alice.base.eth".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE event_kind = 'ResolverChanged'
              AND namespace = 'basenames'
              AND derivation_kind = 'ens_v1_unwrapped_authority'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE event_kind = 'ResolverChanged'
              AND source_family = 'basenames_base_registry'
              AND derivation_kind = 'ens_v1_registry_resolver_changed'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordChanged' AND namespace = 'basenames'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'RecordVersionChanged' AND namespace = 'basenames'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}
