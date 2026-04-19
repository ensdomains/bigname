#[tokio::test]
async fn reconcile_fetched_heads_marks_losing_branch_orphaned_on_reorg() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(71);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for reorg reconciliation test")?;
    insert_contract_instance(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        root_contract_instance_id,
        "ethereum-mainnet",
        "0x0000000000000000000000000000000000000001",
        Some(1),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        1,
        root_contract_instance_id,
        "0x0000000000000000000000000000000000000001",
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let ancestor_block = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        41,
    );
    let losing_block = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_parent_block = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_head_block = provider_block(
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        43,
    );
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &ancestor_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &losing_block,
            CanonicalityState::Canonical,
        )],
    )
    .await?;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[provider_block_to_lineage(
            "ethereum-mainnet",
            &new_parent_block,
            CanonicalityState::Orphaned,
        )],
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &ancestor_block,
                CanonicalityState::Canonical,
            ),
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &losing_block,
                CanonicalityState::Canonical,
            ),
            provider_block_to_raw_block(
                "ethereum-mainnet",
                &new_parent_block,
                CanonicalityState::Orphaned,
            ),
        ],
    )
    .await?;
    upsert_raw_transactions(
        database.pool(),
        &[RawTransaction {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            from_address: "0x0000000000000000000000000000000000000001".to_owned(),
            to_address: Some("0x0000000000000000000000000000000000000002".to_owned()),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_code_hashes(
        database.pool(),
        &[RawCodeHash {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            contract_address: "0x0000000000000000000000000000000000000001".to_owned(),
            code_hash: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
                .to_owned(),
            code_byte_length: 32,
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_receipts(
        database.pool(),
        &[RawReceipt {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            contract_address: None,
            status: Some(true),
            gas_used: Some(21_000),
            cumulative_gas_used: Some(21_000),
            logs_bloom: losing_block.logs_bloom.clone(),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[RawLog {
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            transaction_hash: transaction_hash_for_block(&losing_block),
            transaction_index: 0,
            log_index: 0,
            emitting_address: "0x00000000000000000000000000000000000000aa".to_owned(),
            topics: vec![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ],
            data: vec![0xde, 0xad, 0xbe, 0xef],
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    sqlx::query(
            r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state
            )
            VALUES (
                'raw_log_preimage_observed:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:0:0x00000000000000000000000000000000000000aa',
                'ens',
                'PreimageObserved',
                'ens_test',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $1,
                0,
                '{"kind":"raw_log"}'::jsonb,
                'raw_log_preimage_observation',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"decoded_name":"wrapped.eth"}'::jsonb
            )
            "#,
        )
        .bind(transaction_hash_for_block(&losing_block))
        .execute(database.pool())
        .await
        .context("failed to insert normalized event for reorg reconciliation test")?;
    let losing_timestamp =
        OffsetDateTime::from_unix_timestamp(losing_block.block_timestamp_unix_secs)
            .expect("losing block timestamp must be valid");
    let token_lineage_id = Uuid::from_u128(0x7100);
    let resource_id = Uuid::from_u128(0x7200);
    let surface_binding_id = Uuid::from_u128(0x7300);
    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id,
            token_lineage_id: Some(token_lineage_id),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: "ens:reorg.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "reorg.eth".to_owned(),
            canonical_display_name: "reorg.eth".to_owned(),
            normalized_name: "reorg.eth".to_owned(),
            dns_encoded_name: vec![5, b'r', b'e', b'o', b'r', b'g', 3, b'e', b't', b'h', 0],
            namehash: "0xnamehashreorg".to_owned(),
            labelhashes: vec!["0xlabelhashreorg".to_owned()],
            normalizer_version: "ensip15@2026-04-16".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: "ens:reorg.eth".to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: losing_timestamp,
            active_to: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    sqlx::query(
        r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state
            )
            VALUES (
                'ens_v1_unwrapped_authority:ResolverChanged:resolver:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:1',
                'ens',
                'ens:reorg.eth',
                $1,
                'ResolverChanged',
                'ens_v1_registry_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                1,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{"resolver":"0x00000000000000000000000000000000000000aa"}'::jsonb,
                '{"resolver":"0x00000000000000000000000000000000000000bb"}'::jsonb
            )
            "#,
    )
    .bind(resource_id)
    .bind(transaction_hash_for_block(&losing_block))
    .execute(database.pool())
    .await
    .context("failed to insert ResolverChanged event for reorg reconciliation test")?;
    sqlx::query(
        r#"
            INSERT INTO normalized_events (
                event_identity,
                namespace,
                logical_name_id,
                resource_id,
                event_kind,
                source_family,
                manifest_version,
                source_manifest_id,
                chain_id,
                block_number,
                block_hash,
                transaction_hash,
                log_index,
                raw_fact_ref,
                derivation_kind,
                canonicality_state,
                before_state,
                after_state
            )
            VALUES
            (
                'ens_v1_unwrapped_authority:RecordChanged:record-change:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:2',
                'ens',
                'ens:reorg.eth',
                $1,
                'RecordChanged',
                'ens_v1_resolver_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                2,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"record_key":"text","record_family":"text","selector_key":null}'::jsonb
            ),
            (
                'ens_v1_unwrapped_authority:RecordChanged:record-change:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:3',
                'ens',
                'ens:reorg.eth',
                $1,
                'RecordChanged',
                'ens_v1_resolver_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                3,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"record_key":"addr:60","record_family":"addr","selector_key":"60"}'::jsonb
            ),
            (
                'ens_v1_unwrapped_authority:RecordVersionChanged:record-version:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:4',
                'ens',
                'ens:reorg.eth',
                $1,
                'RecordVersionChanged',
                'ens_v1_resolver_l1',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $2,
                4,
                '{"kind":"raw_log"}'::jsonb,
                'ens_v1_unwrapped_authority',
                'canonical'::canonicality_state,
                '{"record_version":null}'::jsonb,
                '{"record_version":7}'::jsonb
            )
            "#,
    )
    .bind(resource_id)
    .bind(transaction_hash_for_block(&losing_block))
    .execute(database.pool())
    .await
    .context("failed to insert record change events for reorg reconciliation test")?;
    tasks[0].checkpoint = advance_chain_checkpoints(
        database.pool(),
        &ChainCheckpointUpdate {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical: Some(CheckpointBlockRef {
                block_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                block_number: 42,
            }),
            safe: None,
            finalized: None,
        },
    )
    .await?;
    let (provider, server) =
        bundle_provider(vec![new_parent_block.clone(), new_head_block.clone()]).await?;

    let (next_task, outcome) = reconcile_fetched_heads(
        database.pool(),
        &tasks[0],
        &provider,
        &ProviderHeadSnapshot {
            canonical: new_head_block,
            safe: None,
            finalized: None,
        },
    )
    .await?
    .expect("reorg reconciliation must update task state");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::ReorgReconciled
    );
    assert_eq!(outcome.orphaned_block_count, 1);
    assert_eq!(next_task.checkpoint.canonical_block_number, Some(43));
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_blocks WHERE block_hash = '0x1111111111111111111111111111111111111111111111111111111111111111'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_hash = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE event_kind = 'ResolverChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordChanged' AND canonicality_state = 'orphaned'::canonicality_state"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind = 'RecordVersionChanged' AND canonicality_state = 'orphaned'::canonicality_state"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND event_kind IN ('RecordChanged', 'RecordVersionChanged') AND canonicality_state = 'canonical'::canonicality_state"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM token_lineages WHERE token_lineage_id = $1"
        )
        .bind(token_lineage_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM resources WHERE resource_id = $1"
        )
        .bind(resource_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM name_surfaces WHERE logical_name_id = 'ens:reorg.eth'"
            )
            .fetch_one(database.pool())
            .await?,
            "orphaned".to_owned()
        );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM surface_bindings WHERE surface_binding_id = $1"
        )
        .bind(surface_binding_id)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );
    assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = '0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'"
            )
            .fetch_one(database.pool())
            .await?,
            "canonical".to_owned()
        );

    server.abort();
    database.cleanup().await?;
    Ok(())
}
