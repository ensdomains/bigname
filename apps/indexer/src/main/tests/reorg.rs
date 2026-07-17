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
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
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

    let losing_resolution_trace = execution_trace_fixture(
        Uuid::from_u128(0x9001),
        "verified_resolution",
        "ens:reorg.eth:addr:60",
        &losing_block,
    );
    let losing_resolution_outcome = execution_outcome_fixture(
        &losing_resolution_trace,
        &losing_block,
        &ancestor_block,
        &ancestor_block,
        Uuid::from_u128(0x9101),
    );
    insert_execution_fixture(
        database.pool(),
        &losing_resolution_trace,
        &losing_resolution_outcome,
    )
    .await?;

    let losing_primary_trace = execution_trace_fixture(
        Uuid::from_u128(0x9002),
        "verified_primary_name",
        "ens:0x0000000000000000000000000000000000000001:60",
        &losing_block,
    );
    let losing_primary_outcome = execution_outcome_fixture(
        &losing_primary_trace,
        &ancestor_block,
        &ancestor_block,
        &losing_block,
        Uuid::from_u128(0x9103),
    );
    insert_execution_fixture(
        database.pool(),
        &losing_primary_trace,
        &losing_primary_outcome,
    )
    .await?;

    let unrelated_resolution_trace = execution_trace_fixture(
        Uuid::from_u128(0x9003),
        "verified_resolution",
        "ens:keep.eth:addr:60",
        &ancestor_block,
    );
    let unrelated_resolution_outcome = execution_outcome_fixture(
        &unrelated_resolution_trace,
        &ancestor_block,
        &ancestor_block,
        &ancestor_block,
        Uuid::from_u128(0x9105),
    );
    insert_execution_fixture(
        database.pool(),
        &unrelated_resolution_trace,
        &unrelated_resolution_outcome,
    )
    .await?;

    let out_of_scope_trace = execution_trace_fixture(
        Uuid::from_u128(0x9004),
        "declared_resolution",
        "ens:declared.eth:addr:60",
        &losing_block,
    );
    let out_of_scope_outcome = execution_outcome_fixture(
        &out_of_scope_trace,
        &losing_block,
        &losing_block,
        &losing_block,
        Uuid::from_u128(0x9107),
    );
    insert_execution_fixture(database.pool(), &out_of_scope_trace, &out_of_scope_outcome).await?;

    let execution_trace_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_traces")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution traces before reorg")?;
    let execution_step_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_steps")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution steps before reorg")?;
    let execution_outcome_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_cache_outcomes")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution cache outcomes before reorg")?;
    assert_eq!(execution_trace_count_before, 4);
    assert_eq!(execution_step_count_before, 4);
    assert_eq!(execution_outcome_count_before, 4);

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
                "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = '0x1111111111111111111111111111111111111111111111111111111111111111'"
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
    assert_eq!(
        load_execution_outcome(database.pool(), &losing_resolution_outcome.cache_key).await?,
        None
    );
    assert_eq!(
        load_execution_outcome(database.pool(), &losing_primary_outcome.cache_key).await?,
        None
    );
    assert!(
        load_execution_outcome(database.pool(), &unrelated_resolution_outcome.cache_key)
            .await?
            .is_some(),
        "unrelated verified resolution outcome must remain reusable"
    );
    assert!(
        load_execution_outcome(database.pool(), &out_of_scope_outcome.cache_key)
            .await?
            .is_some(),
        "out-of-scope execution outcome must remain reusable"
    );
    for trace_id in [
        losing_resolution_trace.execution_trace_id,
        losing_primary_trace.execution_trace_id,
        unrelated_resolution_trace.execution_trace_id,
        out_of_scope_trace.execution_trace_id,
    ] {
        assert!(
            load_execution_trace(database.pool(), trace_id)
                .await?
                .is_some(),
            "execution trace {trace_id} must remain durable after reorg invalidation"
        );
    }
    let execution_trace_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_traces")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution traces after reorg")?;
    let execution_step_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_steps")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution steps after reorg")?;
    let execution_outcome_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM execution_cache_outcomes")
            .fetch_one(database.pool())
            .await
            .context("failed to count execution cache outcomes after reorg")?;
    assert_eq!(execution_trace_count_after, execution_trace_count_before);
    assert_eq!(execution_step_count_after, execution_step_count_before);
    assert_eq!(
        execution_outcome_count_after,
        execution_outcome_count_before - 2
    );

    server.abort();
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn reorg_common_ancestor_must_be_on_current_canonical_branch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let ancestor = provider_block(
        "0x1111111111111111111111111111111111111111111111111111111111111111",
        None,
        1,
    );
    let old_two = provider_block(
        "0x2222222222222222222222222222222222222222222222222222222222222222",
        Some(&ancestor.block_hash),
        2,
    );
    let old_head = provider_block(
        "0x3333333333333333333333333333333333333333333333333333333333333333",
        Some(&old_two.block_hash),
        3,
    );
    let stray_two = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some(&ancestor.block_hash),
        2,
    );
    let new_head = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some(&stray_two.block_hash),
        3,
    );

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            provider_block_to_lineage("ethereum-mainnet", &ancestor, CanonicalityState::Canonical),
            provider_block_to_lineage("ethereum-mainnet", &old_two, CanonicalityState::Canonical),
            provider_block_to_lineage("ethereum-mainnet", &old_head, CanonicalityState::Canonical),
            provider_block_to_lineage("ethereum-mainnet", &stray_two, CanonicalityState::Observed),
        ],
    )
    .await?;

    let (provider, server) = bundle_provider(vec![stray_two.clone(), new_head.clone()]).await?;
    let reconciliation = reconcile_canonical_head(
        database.pool(),
        &provider,
        "ethereum-mainnet",
        &ChainCheckpoint {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical_block_hash: Some(old_head.block_hash.clone()),
            canonical_block_number: Some(old_head.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
        &new_head,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await?;

    assert_eq!(
        reconciliation.status,
        CanonicalReconciliationStatus::ReorgReconciled
    );
    assert_eq!(reconciliation.orphaned_block_count, 2);
    assert_eq!(
        reconciliation.raw_orphan_stop_before_hash.as_deref(),
        Some(ancestor.block_hash.as_str())
    );

    let states = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT block_hash, canonicality_state::TEXT
        FROM chain_lineage
        WHERE chain_id = 'ethereum-mainnet'
        ORDER BY block_number, block_hash
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        states,
        vec![
            (ancestor.block_hash, "canonical".to_owned()),
            (old_two.block_hash, "orphaned".to_owned()),
            (stray_two.block_hash, "canonical".to_owned()),
            (old_head.block_hash, "orphaned".to_owned()),
            (new_head.block_hash, "canonical".to_owned()),
        ]
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn awaiting_ancestor_raw_persistence_preserves_walked_orphaned_lineage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let old_head = provider_block(
        "0x3333333333333333333333333333333333333333333333333333333333333333",
        None,
        3,
    );
    let orphaned_parent = provider_block(
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        None,
        4,
    );
    let new_head = provider_block(
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some(&orphaned_parent.block_hash),
        5,
    );

    upsert_chain_lineage_blocks(
        database.pool(),
        &[
            provider_block_to_lineage("ethereum-mainnet", &old_head, CanonicalityState::Canonical),
            provider_block_to_lineage(
                "ethereum-mainnet",
                &orphaned_parent,
                CanonicalityState::Orphaned,
            ),
        ],
    )
    .await?;

    let (provider, server) =
        bundle_provider(vec![orphaned_parent.clone(), new_head.clone()]).await?;
    let reconciliation = reconcile_canonical_head(
        database.pool(),
        &provider,
        "ethereum-mainnet",
        &ChainCheckpoint {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical_block_hash: Some(old_head.block_hash.clone()),
            canonical_block_number: Some(old_head.block_number),
            safe_block_hash: None,
            safe_block_number: None,
            finalized_block_hash: None,
            finalized_block_number: None,
        },
        &new_head,
        HeaderAuditMode::Minimal,
        &[],
        &ChainCoverageFrontiers::default(),
    )
    .await?;
    assert_eq!(
        reconciliation.status,
        CanonicalReconciliationStatus::AwaitingAncestor
    );
    assert!(
        reconciliation
            .reconciled_blocks
            .iter()
            .any(|block| block.block_hash == orphaned_parent.block_hash),
        "AwaitingAncestor path must include the stored-orphaned parent"
    );

    persist_reconciled_raw_blocks(
        database.pool(),
        "ethereum-mainnet",
        &ProviderHeadSnapshot {
            canonical: new_head,
            safe: None,
            finalized: None,
        },
        &reconciliation,
        HeaderAuditMode::Minimal,
    )
    .await?;

    let state = sqlx::query_scalar::<_, String>(
        "SELECT canonicality_state::TEXT FROM chain_lineage WHERE chain_id = 'ethereum-mainnet' AND block_hash = $1",
    )
    .bind(&orphaned_parent.block_hash)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(state, "orphaned".to_owned());

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn reorg_reconcile_fetched_heads_orphans_losing_branch_rows_when_raw_block_is_missing()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(72);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for missing-raw-block reorg test")?;
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
        "0x2111111111111111111111111111111111111111111111111111111111111111",
        Some("0x0000000000000000000000000000000000000000000000000000000000000000"),
        41,
    );
    let losing_block = provider_block(
        "0x2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        Some("0x2111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_parent_block = provider_block(
        "0x2bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        Some("0x2111111111111111111111111111111111111111111111111111111111111111"),
        42,
    );
    let new_head_block = provider_block(
        "0x2ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        Some("0x2bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage WHERE block_hash = $1")
            .bind(&losing_block.block_hash)
            .fetch_one(database.pool())
            .await?,
        1
    );
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
                'raw_log_preimage_observed:1:0x2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0xtx2a:0:0x00000000000000000000000000000000000000aa',
                'ens',
                'PreimageObserved',
                'ens_test',
                1,
                1,
                'ethereum-mainnet',
                42,
                '0x2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                $1,
                0,
                '{"kind":"raw_log"}'::jsonb,
                'raw_log_preimage_observation',
                'canonical'::canonicality_state,
                '{}'::jsonb,
                '{"decoded_name":"missing-raw-block.eth"}'::jsonb
            )
            "#,
    )
    .bind(transaction_hash_for_block(&losing_block))
    .execute(database.pool())
    .await
    .context("failed to insert normalized event for missing-raw-block reorg test")?;
    let losing_timestamp =
        OffsetDateTime::from_unix_timestamp(losing_block.block_timestamp_unix_secs)
            .expect("losing block timestamp must be valid");
    let token_lineage_id = Uuid::from_u128(0x7200);
    let resource_id = Uuid::from_u128(0x7300);
    let surface_binding_id = Uuid::from_u128(0x7400);
    upsert_token_lineages(
        database.pool(),
        &[TokenLineage {
            token_lineage_id,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "missing_raw_block_reorg"}),
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
            provenance: json!({"test": "missing_raw_block_reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[NameSurface {
            logical_name_id: "ens:missing-raw-block.eth".to_owned(),
            namespace: "ens".to_owned(),
            input_name: "missing-raw-block.eth".to_owned(),
            canonical_display_name: "missing-raw-block.eth".to_owned(),
            normalized_name: "missing-raw-block.eth".to_owned(),
            dns_encoded_name: vec![
                17, b'm', b'i', b's', b's', b'i', b'n', b'g', b'-', b'r', b'a', b'w', b'-', b'b',
                b'l', b'o', b'c', b'k', 3, b'e', b't', b'h', 0,
            ],
            namehash: "0xmissingrawblocknamehash".to_owned(),
            labelhashes: vec!["0xmissingrawblocklabelhash".to_owned()],
            normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "missing_raw_block_reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    upsert_surface_bindings(
        database.pool(),
        &[SurfaceBinding {
            surface_binding_id,
            logical_name_id: "ens:missing-raw-block.eth".to_owned(),
            resource_id,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            active_from: losing_timestamp,
            active_to: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: losing_block.block_hash.clone(),
            block_number: losing_block.block_number,
            provenance: json!({"test": "missing_raw_block_reorg"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    tasks[0].checkpoint = advance_chain_checkpoints(
        database.pool(),
        &ChainCheckpointUpdate {
            chain_id: "ethereum-mainnet".to_owned(),
            canonical: Some(CheckpointBlockRef {
                block_hash: losing_block.block_hash.clone(),
                block_number: losing_block.block_number,
            }),
            safe: None,
            finalized: None,
        },
    )
    .await?;
    let (provider, server) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            block: new_parent_block.clone(),
            logs: Vec::new(),
        },
        ProviderBlockFixture {
            block: new_head_block.clone(),
            logs: Vec::new(),
        },
    ])
    .await?;

    let (_, outcome) = reconcile_fetched_heads(
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
    .expect("reorg reconciliation must update task state when chain_lineage rows are missing");

    assert_eq!(
        outcome.canonical_status,
        CanonicalReconciliationStatus::ReorgReconciled
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM chain_lineage WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_transactions WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_code_hashes WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_receipts WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM raw_logs WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonicality_state::TEXT FROM normalized_events WHERE block_hash = $1"
        )
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        "orphaned".to_owned()
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
            "SELECT canonicality_state::TEXT FROM name_surfaces WHERE logical_name_id = 'ens:missing-raw-block.eth'"
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

    server.abort();
    database.cleanup().await?;
    Ok(())
}

async fn insert_execution_fixture(
    pool: &PgPool,
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
) -> Result<()> {
    upsert_execution_trace(pool, trace).await?;
    upsert_execution_outcome(pool, outcome).await?;
    Ok(())
}

fn execution_trace_fixture(
    execution_trace_id: Uuid,
    request_type: &str,
    request_key: &str,
    block: &ProviderBlock,
) -> ExecutionTrace {
    ExecutionTrace {
        execution_trace_id,
        request_type: request_type.to_owned(),
        request_key: request_key.to_owned(),
        namespace: "ens".to_owned(),
        chain_context: json!({
            "requested_positions": [requested_chain_position(block)]
        }),
        manifest_context: json!({
            "manifest_versions": [{
                "source_family": "ens_execution",
                "manifest_version": 1
            }]
        }),
        contracts_called: json!([{
            "chain_id": "ethereum-mainnet",
            "contract_address": "0x0000000000000000000000000000000000000001",
            "selector": "0x3b3b57de"
        }]),
        gateway_digests: json!([]),
        final_payload: Some(json!({
            "status": "success",
            "request_key": request_key
        })),
        failure_payload: None,
        request_metadata: json!({
            "test": "reorg_execution_cache_invalidation"
        }),
        finished_at: Some(test_execution_timestamp(block)),
        steps: vec![ExecutionTraceStep {
            step_index: 0,
            step_kind: "execute_verified_read".to_owned(),
            input_digest: Some(format!("sha256:input-{}", execution_trace_id.simple())),
            output_digest: Some(format!("sha256:output-{}", execution_trace_id.simple())),
            latency_ms: Some(1),
            canonicality_dependency: json!({
                "ethereum-mainnet": {
                    "block_number": block.block_number,
                    "block_hash": block.block_hash
                }
            }),
            step_payload: json!({
                "request_type": request_type,
                "request_key": request_key
            }),
        }],
    }
}

#[tokio::test]
async fn silent_winning_reorg_removes_losing_ensv2_discovery_authority() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;

    let chain = "ethereum-sepolia";
    let registry_address = "0x0000000000000000000000000000000000000711";
    let losing_child_address = "0x0000000000000000000000000000000000000712";
    let later_child_address = "0x0000000000000000000000000000000000000713";
    let registry_contract_instance_id = Uuid::from_u128(0x711);
    let manifest_id = 711_i64;
    let caught_up_block = provider_block(
        "0x7110000000000000000000000000000000000000000000000000000000000010",
        Some("0x7110000000000000000000000000000000000000000000000000000000000009"),
        10,
    );
    let losing_block = provider_block(
        "0x7110000000000000000000000000000000000000000000000000000000000011",
        Some(&caught_up_block.block_hash),
        11,
    );
    let winning_block = provider_block(
        "0x7111000000000000000000000000000000000000000000000000000000000011",
        Some(&caught_up_block.block_hash),
        11,
    );
    let later_block = provider_block(
        "0x7110000000000000000000000000000000000000000000000000000000000012",
        Some(&winning_block.block_hash),
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
            $1,
            1,
            'ens',
            'ens_v2_registry_l1',
            $2,
            'ens_v2',
            'active',
            'ensip15@ens-normalize-0.1.1',
            'tests/ens_v2_registry_l1/v1.toml',
            DEFAULT
        )
        "#,
    )
    .bind(manifest_id)
    .bind(chain)
    .execute(database.pool())
    .await
    .context("failed to insert the ENSv2 registry manifest for silent-reorg repair")?;
    insert_contract_instance(
        database.pool(),
        registry_contract_instance_id,
        chain,
        "root",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        registry_contract_instance_id,
        chain,
        registry_address,
        Some(manifest_id),
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        manifest_id,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        manifest_id,
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
        manifest_id,
        "subregistry",
        "registry",
        "reachable_from_root",
    )
    .await?;

    create_complete_raw_log_staging_input_fixture(database.pool(), chain, 12).await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        chain,
        0,
        12,
        "ens_v2_registry_l1",
        &[registry_address, losing_child_address],
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let task = tasks
        .pop()
        .context("ENSv2 silent-reorg fixture must create one intake task")?;

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: caught_up_block.clone(),
        logs: vec![rpc_ens_v2_label_registered_log_payload(
            &caught_up_block,
            registry_address,
            1,
            "parent",
            0,
        )],
    }])
    .await?;
    let (task, initial_outcome) = reconcile_fetched_heads(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: caught_up_block.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("initial ENSv2 catch-up must initialize the checkpoint")?;
    assert_eq!(
        initial_outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    server.abort();

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: losing_block.clone(),
        logs: vec![rpc_ens_v2_subregistry_updated_log_payload(
            &losing_block,
            registry_address,
            losing_child_address,
            1,
            0,
        )],
    }])
    .await?;
    let (task, losing_outcome) = reconcile_fetched_heads(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: losing_block.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("losing ENSv2 live block must advance the checkpoint")?;
    assert_eq!(
        losing_outcome.canonical_status,
        CanonicalReconciliationStatus::Appended
    );
    assert!(
        bigname_manifests::load_watched_contracts(database.pool())
            .await?
            .iter()
            .any(|contract| contract.address == losing_child_address),
        "the losing live SubregistryUpdated must initially admit its child"
    );
    let losing_discovery_epoch = sqlx::query_scalar::<_, i64>(
        "SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
    server.abort();

    let (provider, server) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            block: caught_up_block.clone(),
            logs: Vec::new(),
        },
        ProviderBlockFixture {
            block: winning_block.clone(),
            logs: Vec::new(),
        },
    ])
    .await?;
    let (task, winning_outcome) = reconcile_fetched_heads(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: winning_block.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("silent winning ENSv2 block must reconcile the same-height fork")?;
    assert_eq!(
        winning_outcome.canonical_status,
        CanonicalReconciliationStatus::ReorgReconciled
    );
    assert_eq!(
        task.checkpoint.canonical_block_hash.as_deref(),
        Some(winning_block.block_hash.as_str())
    );
    let watched_after_reorg = bigname_manifests::load_watched_contracts(database.pool()).await?;
    assert!(
        watched_after_reorg
            .iter()
            .all(|contract| contract.address != losing_child_address),
        "losing-fork discovery authority must be removed before the winning checkpoint advances"
    );
    assert!(
        watched_after_reorg
            .iter()
            .any(|contract| contract.address == registry_address),
        "losing-branch cleanup must retain the manifest-declared registry root"
    );
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?
            > losing_discovery_epoch,
        "removing losing-fork authority must advance the discovery-admission epoch"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges
            WHERE chain_id = $1
              AND active_from_block_hash = $2
              AND deactivated_at IS NULL
            "#,
        )
        .bind(chain)
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0,
        "no active discovery edge may retain a losing-branch admission anchor"
    );
    server.abort();

    let refreshed_plan = load_watched_chain_plan(database.pool()).await?;
    let refreshed_task = sync_intake_chain_tasks(database.pool(), &refreshed_plan)
        .await?
        .pop()
        .context("refreshed ENSv2 watch plan must retain its registry task")?;
    assert!(
        refreshed_task
            .addresses
            .iter()
            .all(|address| address != losing_child_address),
        "the refreshed intake task must not retain the losing child"
    );

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: later_block.clone(),
        logs: vec![rpc_ens_v2_subregistry_updated_log_payload(
            &later_block,
            losing_child_address,
            later_child_address,
            2,
            0,
        )],
    }])
    .await?;
    reconcile_fetched_heads(
        database.pool(),
        &refreshed_task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: later_block,
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("later block must advance without selecting the losing child")?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM raw_logs WHERE chain_id = $1 AND emitting_address = $2",
        )
        .bind(chain)
        .bind(losing_child_address)
        .fetch_one(database.pool())
        .await?,
        0,
        "a later log from the losing child must not be selected as ENSv2 registry input"
    );

    server.abort();
    database.cleanup().await
}

#[derive(Clone, Copy)]
struct LegacyRegistrySilentReorgFixture {
    chain: &'static str,
    namespace: &'static str,
    source_family: &'static str,
    deployment_epoch: &'static str,
    manifest_id: i64,
    seed: u128,
}

fn rpc_legacy_registry_new_owner_log_payload(
    block: &ProviderBlock,
    address: &str,
    parent_node: &str,
    label: &str,
    owner: &str,
    log_index: i64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            ens_v1_new_owner_topic0(),
            parent_node,
            labelhash_hex(label),
        ],
        "data": hex_string(&abi_word_address(owner)),
    })
}

fn legacy_registry_new_owner_raw_log(
    chain: &str,
    block: &ProviderBlock,
    address: &str,
    parent_node: &str,
    label: &str,
    owner: &str,
    log_index: i64,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block.block_hash.clone(),
        block_number: block.block_number,
        transaction_hash: transaction_hash_for_block(block),
        transaction_index: 0,
        log_index,
        emitting_address: address.to_owned(),
        topics: vec![
            ens_v1_new_owner_topic0(),
            parent_node.to_owned(),
            labelhash_hex(label),
        ],
        data: abi_word_address(owner).to_vec(),
        canonicality_state: CanonicalityState::Canonical,
    }
}

#[tokio::test]
async fn silent_winning_reorg_removes_losing_legacy_registry_discovery_authority() -> Result<()> {
    for fixture in [
        LegacyRegistrySilentReorgFixture {
            chain: "ethereum-mainnet",
            namespace: "ens",
            source_family: "ens_v1_registry_l1",
            deployment_epoch: "ens_v1",
            manifest_id: 712,
            seed: 0x712,
        },
        LegacyRegistrySilentReorgFixture {
            chain: "base-mainnet",
            namespace: "basenames",
            source_family: "basenames_base_registry",
            deployment_epoch: "basenames_v1",
            manifest_id: 713,
            seed: 0x713,
        },
    ] {
        assert_silent_winning_reorg_removes_losing_legacy_registry_discovery_authority(fixture)
            .await
            .with_context(|| {
                format!(
                    "silent-reorg discovery repair failed for {} on {}",
                    fixture.source_family, fixture.chain
                )
            })?;
    }
    Ok(())
}

async fn assert_silent_winning_reorg_removes_losing_legacy_registry_discovery_authority(
    fixture: LegacyRegistrySilentReorgFixture,
) -> Result<()> {
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;

    let registry_address = format!("0x{:040x}", fixture.seed * 0x10 + 1);
    let losing_child_address = format!("0x{:040x}", fixture.seed * 0x10 + 2);
    let later_child_address = format!("0x{:040x}", fixture.seed * 0x10 + 3);
    let canonical_subregistry_address = format!("0x{:040x}", fixture.seed * 0x10 + 4);
    let canonical_descendant_address = format!("0x{:040x}", fixture.seed * 0x10 + 5);
    let retained_post_target_address = format!("0x{:040x}", fixture.seed * 0x10 + 6);
    let unprocessed_post_target_address = format!("0x{:040x}", fixture.seed * 0x10 + 7);
    let registry_contract_instance_id = Uuid::from_u128(fixture.seed);
    let initialized_block = provider_block(
        &format!("0x{:064x}", fixture.seed * 0x100 + 0x08),
        Some(&format!("0x{:064x}", fixture.seed * 0x100 + 0x07)),
        8,
    );
    let canonical_subregistry_block = provider_block(
        &format!("0x{:064x}", fixture.seed * 0x100 + 0x09),
        Some(&initialized_block.block_hash),
        9,
    );
    let caught_up_block = provider_block(
        &format!("0x{:064x}", fixture.seed * 0x100 + 0x10),
        Some(&canonical_subregistry_block.block_hash),
        10,
    );
    let losing_block = provider_block(
        &format!("0x{:064x}", fixture.seed * 0x100 + 0x11),
        Some(&caught_up_block.block_hash),
        11,
    );
    let winning_block = provider_block(
        &format!("0x{:064x}", fixture.seed * 0x1000 + 0x11),
        Some(&caught_up_block.block_hash),
        11,
    );
    let later_block = provider_block(
        &format!("0x{:064x}", fixture.seed * 0x100 + 0x12),
        Some(&winning_block.block_hash),
        12,
    );
    let post_target_block = provider_block(
        &format!("0x{:064x}", fixture.seed * 0x100 + 0x20),
        Some(&winning_block.block_hash),
        20,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        fixture.manifest_id,
        fixture.namespace,
        fixture.source_family,
        fixture.chain,
        fixture.deployment_epoch,
        registry_contract_instance_id,
        &registry_address,
        "registry",
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        fixture.manifest_id,
        registry_contract_instance_id,
        &registry_address,
    )
    .await?;
    insert_manifest_discovery_rule(
        database.pool(),
        fixture.manifest_id,
        "subregistry",
        "registry",
        "reachable_from_root",
    )
    .await?;

    create_complete_raw_log_staging_input_fixture(database.pool(), fixture.chain, 20).await?;
    // Model an existing populated database immediately after the retention
    // migration: its pre-migration corpus is generation 1 and globally
    // incomplete until family-specific current-generation evidence recovers
    // absence authority.
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retention_generation = 1,
            retained_history_complete = false,
            incomplete_since = now(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        WHERE chain_id = $1
        "#,
    )
    .bind(fixture.chain)
    .execute(database.pool())
    .await?;
    insert_completed_backfill_range_coverage_for_source_family(
        database.pool(),
        fixture.chain,
        0,
        20,
        fixture.source_family,
        &[
            &registry_address,
            &losing_child_address,
            &canonical_subregistry_address,
            &canonical_descendant_address,
            &retained_post_target_address,
            &unprocessed_post_target_address,
        ],
    )
    .await?;

    // A numerically identical completed job from an older retention
    // generation cannot authorize absence in the migrated corpus.
    sqlx::query(
        "UPDATE backfill_jobs SET raw_log_retention_generation = 0 WHERE chain_id = $1",
    )
    .bind(fixture.chain)
    .execute(database.pool())
    .await?;
    let stale_generation_error =
        ensure_legacy_registry_closure_retention_authority_for_adapters(
            database.pool(),
            fixture.chain,
            &[NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery],
            20,
        )
        .await
        .expect_err("older-generation legacy registry coverage must fail closed");
    let rendered = format!("{stale_generation_error:#}");
    assert!(
        rendered.contains("current-generation backfill coverage is missing or stale")
            && rendered.contains(&registry_address),
        "stale-generation refusal must name the uncovered registry tuple: {rendered}"
    );
    sqlx::query(
        "UPDATE backfill_jobs SET raw_log_retention_generation = 1 WHERE chain_id = $1",
    )
    .bind(fixture.chain)
    .execute(database.pool())
    .await?;

    // The proof's discovery epoch is accepted only under the writer fence.
    // Force epoch drift before the adapter reaches that fence and require an
    // explicit refusal without any absence-based mutation.
    let stale_epoch = ensure_legacy_registry_closure_retention_authority_for_adapters(
        database.pool(),
        fixture.chain,
        &[NormalizedEventReplayAdapter::EnsV1SubregistryDiscovery],
        20,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_admission_epochs (chain_id, epoch)
        VALUES ($1, $2 + 1)
        ON CONFLICT (chain_id) DO UPDATE SET epoch = EXCLUDED.epoch
        "#,
    )
    .bind(fixture.chain)
    .bind(stale_epoch)
    .execute(database.pool())
    .await?;
    let epoch_drift_error = bigname_adapters::sync_ens_v1_subregistry_discovery_through_block_with_expected_admission_epoch(
        database.pool(),
        fixture.chain,
        20,
        stale_epoch,
    )
    .await
    .expect_err("legacy registry reconciliation must reject a stale admission epoch");
    assert!(
        format!("{epoch_drift_error:#}").contains("discovery admission epoch changed"),
        "epoch-drift refusal must be explicit: {epoch_drift_error:#}"
    );

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    let mut tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;
    let task = tasks.pop().with_context(|| {
        format!(
            "{} silent-reorg fixture must create one intake task",
            fixture.source_family
        )
    })?;

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: initialized_block.clone(),
        logs: Vec::new(),
    }])
    .await?;
    let (task, initial_outcome) = reconcile_fetched_heads(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: initialized_block.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("initial legacy-registry catch-up must initialize the checkpoint")?;
    assert_eq!(
        initial_outcome.canonical_status,
        CanonicalReconciliationStatus::Initialized
    );
    server.abort();

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: canonical_subregistry_block.clone(),
        logs: vec![rpc_legacy_registry_new_owner_log_payload(
            &canonical_subregistry_block,
            &registry_address,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "canonical",
            &canonical_subregistry_address,
            0,
        )],
    }])
    .await?;
    let (_, canonical_subregistry_outcome) = reconcile_fetched_heads(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: canonical_subregistry_block.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("canonical legacy subregistry block must advance the checkpoint")?;
    assert_eq!(
        canonical_subregistry_outcome.canonical_status,
        CanonicalReconciliationStatus::Appended
    );
    server.abort();

    let canonical_plan = load_watched_chain_plan(database.pool()).await?;
    let task = sync_intake_chain_tasks(database.pool(), &canonical_plan)
        .await?
        .pop()
        .context("canonical subregistry must enter the refreshed intake plan")?;
    assert!(
        task.addresses
            .iter()
            .any(|address| address == &canonical_subregistry_address),
        "canonical subregistry must be watched before its descendant emits"
    );

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: caught_up_block.clone(),
        logs: vec![rpc_legacy_registry_new_owner_log_payload(
            &caught_up_block,
            &canonical_subregistry_address,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "descendant",
            &canonical_descendant_address,
            0,
        )],
    }])
    .await?;
    let (task, caught_up_outcome) = reconcile_fetched_heads(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: caught_up_block.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("canonical legacy descendant block must advance the checkpoint")?;
    assert_eq!(
        caught_up_outcome.canonical_status,
        CanonicalReconciliationStatus::Appended
    );
    let canonical_watched = bigname_manifests::load_watched_contracts(database.pool()).await?;
    assert!(
        canonical_watched
            .iter()
            .any(|contract| contract.address == canonical_subregistry_address),
        "canonical subregistry must be watched before the losing fork"
    );
    assert!(
        canonical_watched
            .iter()
            .any(|contract| contract.address == canonical_descendant_address),
        "canonical descendant must be watched before the losing fork"
    );
    server.abort();

    upsert_raw_logs(
        database.pool(),
        &[legacy_registry_new_owner_raw_log(
            fixture.chain,
            &post_target_block,
            &registry_address,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "retained-post-target",
            &retained_post_target_address,
            0,
        )],
    )
    .await?;
    let post_target_scope = vec![(
        fixture.source_family.to_owned(),
        registry_address.clone(),
        post_target_block.block_number,
        post_target_block.block_number,
    )];
    bigname_adapters::EnsV1SubregistryDiscoverySyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        fixture.chain,
        std::slice::from_ref(&post_target_block.block_hash),
        &post_target_scope,
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[legacy_registry_new_owner_raw_log(
            fixture.chain,
            &post_target_block,
            &registry_address,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "unprocessed-post-target",
            &unprocessed_post_target_address,
            1,
        )],
    )
    .await?;
    let watched_with_post_target =
        bigname_manifests::load_watched_contracts(database.pool()).await?;
    assert!(
        watched_with_post_target
            .iter()
            .any(|contract| contract.address == retained_post_target_address),
        "an already-reconciled edge after the live target must be preserved"
    );
    assert!(
        watched_with_post_target
            .iter()
            .all(|contract| contract.address != unprocessed_post_target_address),
        "a raw-only edge after the live target must remain future work"
    );

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: losing_block.clone(),
        logs: vec![rpc_legacy_registry_new_owner_log_payload(
            &losing_block,
            &registry_address,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "canonical",
            &losing_child_address,
            0,
        )],
    }])
    .await?;
    let (task, losing_outcome) = reconcile_fetched_heads(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: losing_block.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("losing legacy-registry live block must advance the checkpoint")?;
    assert_eq!(
        losing_outcome.canonical_status,
        CanonicalReconciliationStatus::Appended
    );
    assert!(
        bigname_manifests::load_watched_contracts(database.pool())
            .await?
            .iter()
            .any(|contract| contract.address == losing_child_address),
        "the losing live NewOwner must initially admit its child for {}",
        fixture.source_family
    );
    let watched_on_losing_fork =
        bigname_manifests::load_watched_contracts(database.pool()).await?;
    assert!(
        watched_on_losing_fork.iter().all(|contract| {
            contract.address != canonical_subregistry_address
                && contract.address != canonical_descendant_address
        }),
        "the losing replacement must temporarily close the canonical subregistry branch"
    );
    let losing_discovery_epoch = sqlx::query_scalar::<_, i64>(
        "SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1",
    )
    .bind(fixture.chain)
    .fetch_one(database.pool())
    .await?;
    server.abort();

    let (provider, server) = bundle_provider_with_fixtures(vec![
        ProviderBlockFixture {
            block: caught_up_block.clone(),
            logs: Vec::new(),
        },
        ProviderBlockFixture {
            block: winning_block.clone(),
            logs: Vec::new(),
        },
    ])
    .await?;
    let (task, winning_outcome) = reconcile_fetched_heads(
        database.pool(),
        &task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: winning_block.clone(),
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("silent winning legacy-registry block must reconcile the same-height fork")?;
    assert_eq!(
        winning_outcome.canonical_status,
        CanonicalReconciliationStatus::ReorgReconciled
    );
    assert_eq!(
        task.checkpoint.canonical_block_hash.as_deref(),
        Some(winning_block.block_hash.as_str())
    );
    let watched_after_reorg = bigname_manifests::load_watched_contracts(database.pool()).await?;
    assert!(
        watched_after_reorg
            .iter()
            .all(|contract| contract.address != losing_child_address),
        "losing-fork {} discovery authority must be removed before the winning checkpoint advances",
        fixture.source_family
    );
    assert!(
        watched_after_reorg
            .iter()
            .any(|contract| contract.address == registry_address),
        "losing-branch cleanup must retain the manifest-declared registry root"
    );
    assert!(
        watched_after_reorg
            .iter()
            .any(|contract| contract.address == canonical_subregistry_address),
        "complete repair must restore the canonical subregistry that the losing replacement closed"
    );
    assert!(
        watched_after_reorg
            .iter()
            .any(|contract| contract.address == canonical_descendant_address),
        "complete repair must replay the closed subregistry's canonical history and restore its descendant"
    );
    assert!(
        watched_after_reorg
            .iter()
            .any(|contract| contract.address == retained_post_target_address),
        "target-bounded repair must preserve an existing non-orphaned edge after the winning head"
    );
    assert!(
        watched_after_reorg
            .iter()
            .all(|contract| contract.address != unprocessed_post_target_address),
        "target-bounded repair must not admit a raw-only observation after the winning head"
    );
    assert!(
        sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1",
        )
        .bind(fixture.chain)
        .fetch_one(database.pool())
        .await?
            > losing_discovery_epoch,
        "removing losing-fork authority must advance the discovery-admission epoch"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges
            WHERE chain_id = $1
              AND active_from_block_hash = $2
              AND deactivated_at IS NULL
            "#,
        )
        .bind(fixture.chain)
        .bind(&losing_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0,
        "no active discovery edge may retain a losing-branch admission anchor"
    );
    server.abort();

    let refreshed_plan = load_watched_chain_plan(database.pool()).await?;
    let refreshed_task = sync_intake_chain_tasks(database.pool(), &refreshed_plan)
        .await?
        .pop()
        .context("refreshed legacy-registry watch plan must retain its registry task")?;
    assert!(
        refreshed_task
            .addresses
            .iter()
            .all(|address| address != &losing_child_address),
        "the refreshed intake task must not retain the losing child"
    );
    assert!(
        refreshed_task
            .addresses
            .iter()
            .any(|address| address == &canonical_subregistry_address),
        "the refreshed intake task must restore the canonical subregistry"
    );
    assert!(
        refreshed_task
            .addresses
            .iter()
            .any(|address| address == &canonical_descendant_address),
        "the refreshed intake task must restore the canonical descendant"
    );
    assert!(
        refreshed_task
            .addresses
            .iter()
            .any(|address| address == &retained_post_target_address),
        "the refreshed intake task must retain the already-reconciled post-target edge"
    );
    assert!(
        refreshed_task
            .addresses
            .iter()
            .all(|address| address != &unprocessed_post_target_address),
        "the refreshed intake task must exclude post-target raw facts not yet reconciled"
    );

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: later_block.clone(),
        logs: vec![rpc_legacy_registry_new_owner_log_payload(
            &later_block,
            &losing_child_address,
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "later",
            &later_child_address,
            0,
        )],
    }])
    .await?;
    reconcile_fetched_heads(
        database.pool(),
        &refreshed_task,
        &provider,
        &ProviderHeadSnapshot {
            canonical: later_block,
            safe: None,
            finalized: None,
        },
    )
    .await?
    .context("later block must advance without selecting the losing child")?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM raw_logs WHERE chain_id = $1 AND emitting_address = $2",
        )
        .bind(fixture.chain)
        .bind(&losing_child_address)
        .fetch_one(database.pool())
        .await?,
        0,
        "a later log from the losing child must not be selected as legacy registry input"
    );

    server.abort();
    database.cleanup().await
}

fn execution_outcome_fixture(
    trace: &ExecutionTrace,
    requested_block: &ProviderBlock,
    topology_block: &ProviderBlock,
    record_block: &ProviderBlock,
    boundary_seed: Uuid,
) -> ExecutionOutcome {
    ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: trace.request_key.clone(),
            requested_chain_positions: json!([requested_chain_position(requested_block)]),
            manifest_versions: json!([{
                "source_family": "ens_execution",
                "manifest_version": 1
            }]),
            topology_version_boundary: version_boundary_fixture(
                "ens:reorg.eth",
                boundary_seed,
                Some(9_101),
                Some("ResolverChanged"),
                topology_block,
            ),
            record_version_boundary: version_boundary_fixture(
                "ens:reorg.eth",
                Uuid::from_u128(boundary_seed.as_u128() + 1),
                Some(9_102),
                Some("RecordChanged"),
                record_block,
            ),
        },
        execution_trace_id: trace.execution_trace_id,
        request_type: trace.request_type.clone(),
        namespace: trace.namespace.clone(),
        outcome_payload: Some(json!({
            "status": "success",
            "request_key": trace.request_key
        })),
        failure_payload: None,
        finished_at: trace
            .finished_at
            .expect("execution trace fixture must be finished"),
    }
}

fn requested_chain_position(block: &ProviderBlock) -> serde_json::Value {
    json!({
        "chain_id": "ethereum-mainnet",
        "block_number": block.block_number,
        "block_hash": block.block_hash
    })
}

fn version_boundary_fixture(
    logical_name_id: &str,
    resource_id: Uuid,
    normalized_event_id: Option<i64>,
    event_kind: Option<&str>,
    block: &ProviderBlock,
) -> serde_json::Value {
    json!({
        "logical_name_id": logical_name_id,
        "resource_id": resource_id.to_string(),
        "normalized_event_id": normalized_event_id,
        "event_kind": event_kind,
        "chain_position": {
            "chain_id": "ethereum-mainnet",
            "block_number": block.block_number,
            "block_hash": block.block_hash,
            "timestamp": "2024-06-07T00:00:00Z"
        }
    })
}

fn test_execution_timestamp(block: &ProviderBlock) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(block.block_timestamp_unix_secs)
        .expect("test block timestamp must be valid")
}
