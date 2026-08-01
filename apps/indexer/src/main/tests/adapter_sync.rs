#[tokio::test]
async fn live_adapter_wait_heartbeats_without_adapter_progress_callbacks() -> Result<()> {
    let database = TestDatabase::new().await?;
    let instance_id = "live-adapter-periodic-heartbeat";
    let chain_ids = vec!["ethereum-mainnet".to_owned()];
    install_stale_indexer_heartbeat(database.pool(), instance_id).await?;
    let mut heartbeat = crate::run::startup_heartbeat::StartupHeartbeat::new(
        instance_id.to_owned(),
        tokio::time::Duration::ZERO,
    );
    heartbeat.record(database.pool(), &chain_ids).await?;
    let before = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("initial live-adapter heartbeat must exist")?
    .heartbeat_at;

    crate::runtime::adapter_sync::await_live_adapter_sync_with_heartbeat(
        database.pool(),
        &mut heartbeat,
        &chain_ids,
        async {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            Ok::<_, anyhow::Error>(())
        },
    )
    .await?;

    let after = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("periodic live-adapter heartbeat must remain registered")?
    .heartbeat_at;
    assert!(
        after > before,
        "the runtime must beat while a live adapter has no progress callback"
    );

    database.cleanup().await
}

#[tokio::test]
async fn startup_plain_adapter_wait_does_not_emit_synthetic_progress() -> Result<()> {
    let database = TestDatabase::new().await?;
    let instance_id = "startup-adapter-boundary-heartbeat";
    let chain_ids = vec!["ethereum-mainnet".to_owned()];
    install_stale_indexer_heartbeat(database.pool(), instance_id).await?;
    let mut heartbeat = crate::run::startup_heartbeat::StartupHeartbeat::new(
        instance_id.to_owned(),
        tokio::time::Duration::ZERO,
    );
    heartbeat.record(database.pool(), &chain_ids).await?;
    let before = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("initial startup-adapter heartbeat must exist")?
    .heartbeat_at;
    let mut startup_heartbeat = Some((&mut heartbeat, chain_ids.as_slice()));

    crate::runtime::adapter_sync::await_adapter_with_optional_heartbeat(
        database.pool(),
        &mut startup_heartbeat,
        false,
        async {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            Ok::<_, anyhow::Error>(())
        },
    )
    .await?;

    let after = bigname_storage::load_service_loop_heartbeat(
        database.pool(),
        bigname_storage::INDEXER_SERVICE_NAME,
        instance_id,
    )
    .await?
    .context("startup-adapter heartbeat must remain registered")?
    .heartbeat_at;
    assert_eq!(
        after, before,
        "startup adapter waits may beat only at completed family boundaries"
    );

    database.cleanup().await
}

#[tokio::test]
async fn live_full_source_adapter_wait_fences_same_chain_raw_mutation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "live-full-source-fence";
    let instance_id = "live-full-source-fence-heartbeat";
    let chain_ids = vec![chain.to_owned()];
    install_stale_indexer_heartbeat(database.pool(), instance_id).await?;
    let pool = database.pool().clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut heartbeat = crate::run::startup_heartbeat::StartupHeartbeat::new(
            instance_id.to_owned(),
            tokio::time::Duration::ZERO,
        );
        crate::runtime::adapter_sync::await_live_full_source_adapter_with_heartbeat(
            &pool,
            chain,
            &mut heartbeat,
            &chain_ids,
            async move {
                let _ = started_tx.send(());
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                Ok::<_, anyhow::Error>(())
            },
        )
        .await
    });
    started_rx
        .await
        .context("live full-source fixture must enter the fenced adapter future")?;

    let mut competing = database.pool().begin().await?;
    let acquired =
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("raw_log_staging:{chain}"))
            .fetch_one(&mut *competing)
            .await?;
    assert!(
        !acquired,
        "same-chain raw mutation must wait until absence-based reconciliation finishes"
    );
    competing.rollback().await?;
    task.await??;

    database.cleanup().await
}

#[tokio::test]
async fn startup_full_source_adapter_wait_fences_same_chain_raw_mutation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "startup-full-source-fence";
    let pool = database.pool().clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut startup_heartbeat = None;
        crate::runtime::adapter_sync::await_full_source_adapter_with_optional_heartbeat(
            &pool,
            chain,
            &mut startup_heartbeat,
            false,
            async move {
                let _ = started_tx.send(());
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                Ok::<_, anyhow::Error>(())
            },
        )
        .await
    });
    started_rx
        .await
        .context("startup full-source fixture must enter the fenced adapter future")?;

    let mut competing = database.pool().begin().await?;
    let acquired =
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("raw_log_staging:{chain}"))
            .fetch_one(&mut *competing)
            .await?;
    assert!(
        !acquired,
        "same-chain raw mutation must wait until startup full-source interpretation finishes"
    );
    competing.rollback().await?;
    task.await??;

    database.cleanup().await
}

#[tokio::test]
async fn live_ens_v2_adapter_fences_raw_log_mutation_before_sync() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let block = provider_block(
        "0xabababababababababababababababababababababababababababababababab",
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        41,
    );
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;

    let mut raw_mutation = database.pool().begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("raw_log_staging:{chain}"))
        .execute(raw_mutation.as_mut())
        .await?;

    let pool = database.pool().clone();
    let block_hash = block.block_hash.clone();
    let mut sync = tokio::spawn(async move {
        sync_live_adapter_state_from_persisted_raw_payloads(&pool, "mainnet", chain, &[block_hash])
            .await
    });
    let completed_while_mutation_fence_was_held =
        tokio::time::timeout(std::time::Duration::from_millis(500), &mut sync)
            .await
            .is_ok();

    raw_mutation.rollback().await?;
    if !completed_while_mutation_fence_was_held {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), sync)
            .await
            .context("live adapter sync did not resume after the raw-log fence released")?
            .context("live adapter sync task panicked")?;
    }
    database.cleanup().await?;
    if completed_while_mutation_fence_was_held {
        anyhow::bail!("live ENSv2 adapter began before acquiring the raw-log mutation fence");
    }
    Ok(())
}

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
async fn permissions_startup_rederives_block_producer_before_publishing_permissions() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000000741";
    let resolver_contract_instance_id = Uuid::from_u128(0x741);
    let resource = hex_string(&abi_word_u64(42));
    let account = "0x00000000000000000000000000000000000000aa";
    let alice_dns_name = dns_encoded_eth_name("alice");
    let block = provider_block(
        "0x7474747474747474747474747474747474747474747474747474747474747474",
        Some("0x7373737373737373737373737373737373737373737373737373737373737373"),
        74,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        741,
        "ens",
        "ens_v2_resolver_l1",
        chain,
        "ens_v2",
        resolver_contract_instance_id,
        resolver_address,
        "resolver",
    )
    .await?;
    sqlx::query("UPDATE manifest_versions SET manifest_payload = $2 WHERE manifest_id = $1")
        .bind(741_i64)
        .bind(test_manifest_payload())
        .execute(database.pool())
        .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_resolver_log(
        database.pool(),
        chain,
        &block,
        resolver_address,
        vec![ens_v2_named_resource_topic0(), resource.clone()],
        decode_hex_string(&encode_dynamic_bytes_log_data(&alice_dns_name)),
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_log(
        database.pool(),
        chain,
        &block,
        resolver_address,
        vec![
            ens_v2_eac_roles_changed_topic0(),
            resource,
            hex_string(&abi_word_address(account)),
        ],
        decode_hex_string(&encode_eac_roles_changed_log_data(
            &hex_string(&abi_word_u64(0)),
            &hex_string(&abi_word_u64(1)),
        )),
        1,
        CanonicalityState::Canonical,
    )
    .await?;

    sqlx::query(
        r#"
        CREATE FUNCTION public.assert_startup_permissions_producer_current()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            observed_dns_name text;
        BEGIN
            SELECT after_state->>'dns_encoded_name'
            INTO observed_dns_name
            FROM normalized_events
            WHERE derivation_kind = 'raw_log_preimage_observation'
              AND after_state->>'source_event' = 'NamedResource';
            IF observed_dns_name IS DISTINCT FROM '0x05616c6963650365746800' THEN
                RAISE EXCEPTION
                    'permissions ran before the startup producer replay';
            END IF;
            RETURN NEW;
        END;
        $$;
        "#,
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        CREATE TRIGGER assert_startup_permissions_producer_current
        BEFORE INSERT OR UPDATE ON public.normalized_events
        FOR EACH ROW
        WHEN (NEW.derivation_kind = 'ens_v2_permissions')
        EXECUTE FUNCTION public.assert_startup_permissions_producer_current()
        "#,
    )
    .execute(database.pool())
    .await?;

    crate::runtime::adapter_sync::sync_startup_ens_v2_permissions(
        database.pool(),
        "startup-profile",
        chain,
        block.block_number,
        1_000,
        &mut None,
    )
    .await?;

    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'dns_encoded_name'
             FROM normalized_events
             WHERE derivation_kind = 'raw_log_preimage_observation'
               AND after_state->>'source_event' = 'NamedResource'",
        )
        .fetch_one(database.pool())
        .await?,
        hex_string(&alice_dns_name),
        "startup replay must publish the producer before permissions"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT logical_name_id
             FROM normalized_events
             WHERE derivation_kind = 'ens_v2_permissions'
               AND event_kind = 'PermissionChanged'",
        )
        .fetch_one(database.pool())
        .await?,
        "ens:alice.eth"
    );

    database.cleanup().await
}

#[tokio::test]
async fn live_loop_adapter_sync_tolerates_mid_sync_revision_advance() -> Result<()> {
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
    let heartbeat_instance_id = "live-reverse-revision-advance";
    install_stale_indexer_heartbeat(database.pool(), heartbeat_instance_id).await?;
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION advance_raw_revision_during_reverse_live_sync()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.event_kind = 'ReverseChanged' THEN
                UPDATE raw_log_staging_input_revisions
                SET revision = revision + 1
                WHERE chain_id = NEW.chain_id;
            END IF;
            RETURN NEW;
        END;
        $$;
        CREATE TRIGGER advance_raw_revision_during_reverse_live_sync
        AFTER INSERT ON normalized_events
        FOR EACH ROW EXECUTE FUNCTION advance_raw_revision_during_reverse_live_sync();
        "#,
    )
    .execute(database.pool())
    .await?;
    let revision_before = sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM raw_log_staging_input_revisions WHERE chain_id = 'ethereum-mainnet'",
    )
    .fetch_one(database.pool())
    .await?;
    let heartbeat_chain_ids = watched_plan
        .iter()
        .map(|chain| chain.chain.clone())
        .collect::<Vec<_>>();
    let mut heartbeat = crate::run::startup_heartbeat::StartupHeartbeat::new(
        heartbeat_instance_id.to_owned(),
        tokio::time::Duration::ZERO,
    );
    crate::runtime::adapter_sync::sync_adapter_owned_raw_log_state_live_with_heartbeat(
        database.pool(),
        &watched_plan,
        &mut heartbeat,
        &heartbeat_chain_ids,
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision
             FROM raw_log_staging_input_revisions
             WHERE chain_id = 'ethereum-mainnet'",
        )
        .fetch_one(database.pool())
        .await?,
        revision_before + 1,
        "the fixture must advance the raw-log revision from inside a non-registry family"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "live adapter sync must retain its ReverseChanged write across the tolerated revision advance"
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

    let summary = sync_live_adapter_state_from_persisted_raw_payloads(
        database.pool(),
        "test",
        "ethereum-mainnet",
        std::slice::from_ref(&stored_block.block_hash),
    )
    .await?;

    assert_eq!(summary.total_synced_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "the reverse-claim adapter must still run after block-derived event synchronization"
    );

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

    impl crate::StartupAdapterProgress for CountingBacklogProgress {
        fn record<'a>(&'a mut self, _pool: &'a PgPool) -> crate::StartupAdapterProgressFuture<'a> {
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
    let heartbeat_instance_id = "startup-adapter-wrapper-output-test";
    install_stale_indexer_heartbeat(database.pool(), heartbeat_instance_id).await?;
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
    let normalized_events = sqlx::query_as::<_, (String, String)>(
        "SELECT derivation_kind, event_kind FROM normalized_events ORDER BY derivation_kind, event_kind",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(normalized_events.len(), 8, "{normalized_events:?}");

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
    sqlx::query(
        r#"
        CREATE TABLE service_loop_heartbeats (
            service_name TEXT NOT NULL,
            instance_id TEXT NOT NULL,
            scope_kind TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL,
            heartbeat_at TIMESTAMPTZ NOT NULL,
            expected_chain_ids TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            PRIMARY KEY (service_name, instance_id, scope_kind, scope_id)
        )
        "#,
    )
    .execute(database.pool())
    .await?;
    let heartbeat_instance_id = "startup-adapter-output-test";
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
        0,
        "registry resolver changes must not be duplicated by the deleted discovery adapter"
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
