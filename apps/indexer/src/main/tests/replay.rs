#[tokio::test]
async fn replay_normalized_events_runs_full_persisted_raw_adapter_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let reverse_contract_instance_id = Uuid::from_u128(0x900);
    let reverse_address = "0x00000000000000000000000000000000000000af";
    let claimed_address = "0x1234567890abcdef1234567890abcdef12345678";
    let unrelated_claimed_address = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";
    let block = provider_block(
        "0x9090909090909090909090909090909090909090909090909090909090909090",
        Some("0x8080808080808080808080808080808080808080808080808080808080808080"),
        90,
    );
    let unrelated_block = provider_block(
        "0x9292929292929292929292929292929292929292929292929292929292929292",
        Some(&block.block_hash),
        92,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        10,
        chain,
        "ens_v1_reverse_l1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_active_replay_manifest(
        database.pool(),
        11,
        "ens",
        "ens_v1_resolver_l1",
        chain,
        "mainnet",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block,
        reverse_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &unrelated_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &unrelated_block,
        reverse_address,
        unrelated_claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: block.block_number,
                to_block: block.block_number,
            },
        },
    )
    .await?;

    assert_eq!(outcome.selected_block_count, 1);
    assert_eq!(outcome.canonical_raw_log_count, 1);
    assert_eq!(outcome.scanned_raw_log_count, 2);
    assert_eq!(outcome.matched_raw_log_count, 1);
    assert_eq!(outcome.normalized_event_synced_count, 1);
    assert_eq!(outcome.normalized_event_inserted_count, 1);
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
            "SELECT after_state->>'reverse_name' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_name_for_address(claimed_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT raw_fact_ref->>'block_hash' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        block.block_hash
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1"
        )
        .bind(&unrelated_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn full_closure_reverse_claim_replay_covers_multiple_pages() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_raw_log_staging_input_revisions_table(database.pool()).await?;
    let chain = "base-mainnet";
    let reverse_address = "0x00000000000000000000000000000000000000ab";
    let reverse_contract_instance_id = Uuid::from_u128(0x901);
    let blocks = [
        provider_block(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            Some("0x1010101010101010101010101010101010101010101010101010101010101010"),
            100,
        ),
        provider_block(
            "0x1212121212121212121212121212121212121212121212121212121212121212",
            Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
            101,
        ),
        provider_block(
            "0x1313131313131313131313131313131313131313131313131313131313131313",
            Some("0x1212121212121212121212121212121212121212121212121212121212121212"),
            102,
        ),
    ];
    let orphaned_same_height_block = provider_block(
        "0x1414141414141414141414141414141414141414141414141414141414141414",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        101,
    );
    let claimed_addresses = [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
        "0x3333333333333333333333333333333333333333",
    ];

    insert_active_replay_manifest_contract(
        database.pool(),
        901,
        "basenames",
        "basenames_base_primary",
        chain,
        "basenames_v1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    for (block, claimed_address) in blocks.iter().zip(claimed_addresses) {
        insert_raw_reverse_claimed_log(
            database.pool(),
            chain,
            block,
            reverse_address,
            claimed_address,
            CanonicalityState::Canonical,
        )
        .await?;
    }
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &blocks[2],
        reverse_address,
        "0x5555555555555555555555555555555555555555",
        CanonicalityState::Observed,
        7,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &orphaned_same_height_block,
        CanonicalityState::Orphaned,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &orphaned_same_height_block,
        reverse_address,
        "0x4444444444444444444444444444444444444444",
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_full_closure_normalized_events_from_persisted_raw_payloads(
        database.pool(),
        "mainnet",
        chain,
        "test_full_closure_reverse_claim_multiple_pages",
        100,
        102,
        &[NormalizedEventReplayAdapter::EnsV1ReverseClaim],
        1,
    )
    .await?
    .summary;

    assert_eq!(summary.scanned_log_count, 3);
    assert_eq!(summary.matched_log_count, 3);
    assert_eq!(summary.total_synced_count, 6);
    assert_eq!(summary.total_inserted_count, 6);
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, Option<i64>)>(
            r#"
            SELECT COUNT(*)::BIGINT, MIN(block_number)::BIGINT, MAX(block_number)::BIGINT
            FROM normalized_events
            WHERE chain_id = $1
              AND derivation_kind = 'ens_v1_reverse_claim'
            "#
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (6, Some(100), Some(102))
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE chain_id = $1
              AND block_hash = $2
            "#
        )
        .bind(chain)
        .bind(&orphaned_same_height_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE chain_id = $1
              AND raw_fact_ref->>'log_index' = '7'
            "#
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn full_closure_reverse_claim_replay_respects_manifest_active_from() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_raw_log_staging_input_revisions_table(database.pool()).await?;
    let chain = "base-mainnet";
    let reverse_address = "0x00000000000000000000000000000000000000ac";
    let reverse_contract_instance_id = Uuid::from_u128(0x902);
    let before_active_block = provider_block(
        "0x1515151515151515151515151515151515151515151515151515151515151515",
        Some("0x1414141414141414141414141414141414141414141414141414141414141414"),
        100,
    );
    let active_block = provider_block(
        "0x1616161616161616161616161616161616161616161616161616161616161616",
        Some(&before_active_block.block_hash),
        101,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        902,
        "basenames",
        "basenames_base_primary",
        chain,
        "basenames_v1",
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_from_block_number = $2
        WHERE contract_instance_id = $1
        "#,
    )
    .bind(reverse_contract_instance_id)
    .bind(active_block.block_number)
    .execute(database.pool())
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &before_active_block,
        reverse_address,
        "0x5555555555555555555555555555555555555555",
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &active_block,
        reverse_address,
        "0x6666666666666666666666666666666666666666",
        CanonicalityState::Canonical,
    )
    .await?;

    let summary = sync_full_closure_normalized_events_from_persisted_raw_payloads(
        database.pool(),
        "mainnet",
        chain,
        "test_full_closure_reverse_claim_manifest_start",
        before_active_block.block_number,
        active_block.block_number,
        &[NormalizedEventReplayAdapter::EnsV1ReverseClaim],
        1,
    )
    .await?
    .summary;

    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.total_synced_count, 2);
    assert_eq!(summary.total_inserted_count, 2);
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, Option<i64>)>(
            r#"
            SELECT COUNT(*)::BIGINT, MIN(block_number)::BIGINT, MAX(block_number)::BIGINT
            FROM normalized_events
            WHERE chain_id = $1
              AND derivation_kind = 'ens_v1_reverse_claim'
            "#
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        (
            2,
            Some(active_block.block_number),
            Some(active_block.block_number)
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE chain_id = $1
              AND block_hash = $2
            "#
        )
        .bind(chain)
        .bind(&before_active_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_scoped_block_range_selects_only_requested_targets() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let selected_address = "0x00000000000000000000000000000000000000a1";
    let unselected_address = "0x00000000000000000000000000000000000000b2";
    let selected_claimed_address = "0x1111111111111111111111111111111111111111";
    let unselected_claimed_address = "0x2222222222222222222222222222222222222222";
    let block = provider_block(
        "0x9393939393939393939393939393939393939393939393939393939393939393",
        Some("0x8383838383838383838383838383838383838383838383838383838383838383"),
        93,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        11,
        chain,
        "ens_v1_reverse_l1",
        Uuid::from_u128(0x911),
        selected_address,
        "reverse_registrar",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block,
        selected_address,
        selected_claimed_address,
        CanonicalityState::Canonical,
        0,
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block,
        unselected_address,
        unselected_claimed_address,
        CanonicalityState::Canonical,
        1,
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::ScopedBlockRange {
                from_block: block.block_number,
                to_block: block.block_number,
                source_scope: vec![RawFactNormalizedEventReplaySourceScope {
                    source_family: "ens_v1_reverse_l1".to_owned(),
                    address: selected_address.to_owned(),
                    from_block: block.block_number,
                    to_block: block.block_number,
                }],
            },
        },
    )
    .await?;

    assert_eq!(outcome.source_scope_target_count, 1);
    assert_eq!(outcome.selected_block_count, 1);
    assert_eq!(outcome.canonical_raw_log_count, 1);
    assert_eq!(outcome.scanned_raw_log_count, 2);
    assert_eq!(outcome.matched_raw_log_count, 1);
    assert_eq!(outcome.normalized_event_inserted_count, 1);
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
            "SELECT after_state->>'reverse_name' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_name_for_address(selected_claimed_address)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE raw_fact_ref->>'log_index' = '1'"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_scoped_generic_resolver_scope_fails_closed_for_stateful_replay()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let seed_resolver_address = "0x00000000000000000000000000000000000000c0";
    let generic_resolver_address = "0x00000000000000000000000000000000000000c1";
    let block = provider_block(
        "0x9494949494949494949494949494949494949494949494949494949494949494",
        Some("0x8484848484848484848484848484848484848484848484848484848484848484"),
        94,
    );
    let node = namehash_for_dns_name(&dns_encoded_eth_name("alice"));

    insert_active_replay_manifest_contract(
        database.pool(),
        12,
        "ens",
        "ens_v1_resolver_l1",
        chain,
        "ens_v1",
        Uuid::from_u128(0x912),
        seed_resolver_address,
        "public_resolver",
    )
    .await?;
    insert_raw_resolver_log(
        database.pool(),
        chain,
        &block,
        generic_resolver_address,
        vec![
            resolver_text_changed_with_value_topic0(),
            node,
            keccak256_hex(b"com.twitter"),
        ],
        decode_hex_string(&encode_two_dynamic_string_log_data(
            "com.twitter",
            "alice-twitter",
        )),
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_log(
        database.pool(),
        chain,
        &block,
        "0x00000000000000000000000000000000000000c2",
        vec![
            keccak256_hex(b"ApprovalForAll(address,address,bool)"),
            "0x0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            "0x0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
        ],
        Vec::new(),
        1,
        CanonicalityState::Canonical,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::ScopedBlockRange {
                from_block: block.block_number,
                to_block: block.block_number,
                source_scope: vec![RawFactNormalizedEventReplaySourceScope {
                    source_family: "ens_v1_resolver_l1".to_owned(),
                    address: "*".to_owned(),
                    from_block: block.block_number,
                    to_block: block.block_number,
                }],
            },
        },
    )
    .await
    .expect_err("stateful source-scoped replay must require full closure");

    assert!(
        format!("{error:?}").contains("block-hash and source-scoped replay are disabled"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_is_upsert_only_for_stale_selected_payloads() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let contract_instance_id = Uuid::from_u128(0x905);
    let reverse_address = "0x00000000000000000000000000000000000000a5";
    let claimed_address = "0x5555555555555555555555555555555555555555";
    let block = provider_block(
        "0xf5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5",
        Some("0x8585858585858585858585858585858585858585858585858585858585858585"),
        106,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        5,
        chain,
        "ens_v1_reverse_l1",
        contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block,
        reverse_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_stale_reverse_changed_event(
        database.pool(),
        chain,
        5,
        &block,
        reverse_address,
        claimed_address,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: block.block_number,
                to_block: block.block_number,
            },
        },
    )
    .await
    .expect_err("stale selected normalized-event payload must not be replaced");

    assert!(
        format!("{error:?}").contains("normalized event identity mismatch"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_name' FROM normalized_events"
        )
        .fetch_one(database.pool())
        .await?,
        "stale.addr.reverse"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_is_idempotent_without_checkpoint_mutation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let contract_instance_id = Uuid::from_u128(0x901);
    let watched_address = "0x00000000000000000000000000000000000000a1";
    let claimed_address = "0x1111111111111111111111111111111111111111";
    let block = provider_block(
        "0x9191919191919191919191919191919191919191919191919191919191919191",
        Some("0x8181818181818181818181818181818181818181818181818181818181818181"),
        91,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        1,
        chain,
        "ens_v1_reverse_l1",
        contract_instance_id,
        watched_address,
        "reverse_registrar",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &block,
        watched_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO chain_checkpoints (
            chain_id,
            canonical_block_hash,
            canonical_block_number,
            safe_block_hash,
            safe_block_number,
            finalized_block_hash,
            finalized_block_number
        )
        VALUES ($1, $2, $3, $2, $3, $2, $3)
        "#,
    )
    .bind(chain)
    .bind(&block.block_hash)
    .bind(block.block_number)
    .execute(database.pool())
    .await
    .context("failed to insert checkpoint guard row for replay test")?;

    let request = RawFactNormalizedEventReplayRequest {
        deployment_profile: "mainnet".to_owned(),
        chain: chain.to_owned(),
        selection: RawFactNormalizedEventReplaySelection::BlockRange {
            from_block: block.block_number,
            to_block: block.block_number,
        },
    };

    let first = replay_raw_fact_normalized_events(database.pool(), request.clone()).await?;

    assert_eq!(first.selected_block_count, 1);
    assert_eq!(first.canonical_raw_log_count, 1);
    assert_eq!(first.scanned_raw_log_count, 2);
    assert_eq!(first.matched_raw_log_count, 1);
    assert_eq!(first.normalized_event_synced_count, 1);
    assert_eq!(first.normalized_event_inserted_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT after_state->>'reverse_name' FROM normalized_events WHERE event_kind = 'ReverseChanged'"
        )
        .fetch_one(database.pool())
        .await?,
        reverse_name_for_address(claimed_address)
    );

    let second = replay_raw_fact_normalized_events(database.pool(), request).await?;

    assert_eq!(second.selected_block_count, 1);
    assert_eq!(second.canonical_raw_log_count, 1);
    assert_eq!(second.scanned_raw_log_count, 2);
    assert_eq!(second.matched_raw_log_count, 1);
    assert_eq!(second.normalized_event_synced_count, 1);
    assert_eq!(second.normalized_event_inserted_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT canonical_block_hash FROM chain_checkpoints WHERE chain_id = $1"
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        block.block_hash
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_lineage")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_full_range_name_wrapper_replay_runs_from_closure_boundary()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_adapter_checkpoint_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let wrapper_contract_instance_id = Uuid::from_u128(0x906);
    let wrapper_address = "0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401";
    let block = provider_block(
        "0x9696969696969696969696969696969696969696969696969696969696969696",
        Some("0x8686868686868686868686868686868686868686868686868686868686868686"),
        96,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        6,
        chain,
        wrapper_contract_instance_id,
        wrapper_address,
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        wrapper_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let request = RawFactNormalizedEventReplayRequest {
        deployment_profile: "mainnet".to_owned(),
        chain: chain.to_owned(),
        selection: RawFactNormalizedEventReplaySelection::BlockRange {
            from_block: block.block_number,
            to_block: block.block_number,
        },
    };

    let outcome = replay_raw_fact_normalized_events(database.pool(), request).await?;
    assert_eq!(outcome.selected_block_count, 1);
    assert_eq!(outcome.canonical_raw_log_count, 1);
    assert_eq!(outcome.matched_raw_log_count, 2);
    assert_eq!(outcome.normalized_event_synced_count, 6);
    assert_eq!(outcome.normalized_event_inserted_count, 6);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        6
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_uses_only_persisted_canonical_raw_log_inputs() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let watched_address = "0x0000000000000000000000000000000000000001";
    let claimed_address = "0x1111111111111111111111111111111111111111";
    let canonical_block = provider_block(
        "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        101,
    );
    let observed_block = provider_block(
        "0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        102,
    );
    let orphaned_block = provider_block(
        "0xc3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        103,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        2,
        chain,
        "ens_v1_reverse_l1",
        Uuid::from_u128(0x902),
        watched_address,
        "reverse_registrar",
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &canonical_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &observed_block,
        CanonicalityState::Observed,
    )
    .await?;
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &orphaned_block,
        CanonicalityState::Orphaned,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &canonical_block,
        watched_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &observed_block,
        watched_address,
        "0x2222222222222222222222222222222222222222",
        CanonicalityState::Observed,
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        chain,
        &orphaned_block,
        watched_address,
        "0x3333333333333333333333333333333333333333",
        CanonicalityState::Orphaned,
    )
    .await?;
    bigname_storage::upsert_raw_payload_cache_metadata(
        database.pool(),
        &[bigname_storage::RawPayloadCacheMetadataUpsert {
            chain_id: chain.to_owned(),
            block_hash: canonical_block.block_hash.clone(),
            payload_kind: provider::RAW_PAYLOAD_KIND_FULL_BLOCK.to_owned(),
            digest_algorithm: None,
            retained_digest: None,
            block_number: Some(canonical_block.block_number),
            payload_size_bytes: 0,
            content_type: Some(provider::JSON_RPC_PAYLOAD_CONTENT_TYPE.to_owned()),
            content_encoding: Some(provider::JSON_RPC_PAYLOAD_CONTENT_ENCODING.to_owned()),
            cache_metadata: json!({
                "source": "json-rpc",
                "method": "eth_getBlockByHash",
                "fetch_mode": "block_hash",
                "digest_scope": "json_rpc_response_body"
            }),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: 101,
                to_block: 103,
            },
        },
    )
    .await?;

    assert_eq!(outcome.selected_block_count, 1);
    assert_eq!(outcome.canonical_raw_log_count, 1);
    assert_eq!(outcome.normalized_event_inserted_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_transactions")
            .fetch_one(database.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_receipts")
            .fetch_one(database.pool())
            .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_payload_cache_metadata")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1"
        )
        .bind(&canonical_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash <> $1"
        )
        .bind(&canonical_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_rejects_deployment_profile_outside_active_manifest_scope()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let contract_instance_id = Uuid::from_u128(0x904);
    let watched_address = "0x0000000000000000000000000000000000000001";
    let block = provider_block(
        "0xe5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        105,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        4,
        chain,
        contract_instance_id,
        watched_address,
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "sepolia".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: block.block_number,
                to_block: block.block_number,
            },
        },
    )
    .await
    .expect_err("mismatched deployment profile must be rejected");

    assert!(
        format!("{error:?}")
            .contains("does not match active manifest/discovery corpus profile mainnet"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_matches_post_audit_sepolia_manifest_profile() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    insert_active_replay_manifest_contract(
        database.pool(),
        905,
        "ens",
        "ens_v2_registrar_l1",
        chain,
        "ens_v2_sepolia_post_audit",
        Uuid::from_u128(0x905),
        "0x0000000000000000000000000000000000000905",
        "registrar",
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "sepolia".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockHashes(Vec::new()),
        },
    )
    .await?;
    assert_eq!(outcome.deployment_profile, "sepolia");
    assert_eq!(outcome.selected_block_count, 0);

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "sepolia-dev".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockHashes(Vec::new()),
        },
    )
    .await
    .expect_err("legacy profile label must not match the current Sepolia manifest root");
    assert!(
        format!("{error:?}")
            .contains("does not match active manifest/discovery corpus profile sepolia"),
        "unexpected error: {error:?}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_skips_noncanonical_raw_logs_in_selected_block_hashes()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let reverse_address = "0x00000000000000000000000000000000000000d4";
    let canonical_claimed_address = "0x4444444444444444444444444444444444444444";
    let observed_claimed_address = "0x5555555555555555555555555555555555555555";
    let block = provider_block(
        "0xd4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        104,
    );

    insert_active_replay_watched_contract_with_source_family(
        database.pool(),
        3,
        chain,
        "ens_v1_reverse_l1",
        Uuid::from_u128(0x903),
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_active_replay_manifest(
        database.pool(),
        4,
        "ens",
        "ens_v1_resolver_l1",
        chain,
        "mainnet",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block,
        reverse_address,
        canonical_claimed_address,
        CanonicalityState::Canonical,
        0,
    )
    .await?;
    insert_raw_reverse_claimed_log_at_index(
        database.pool(),
        chain,
        &block,
        reverse_address,
        observed_claimed_address,
        CanonicalityState::Observed,
        1,
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockHashes(vec![
                block.block_hash.clone(),
            ]),
        },
    )
    .await?;

    assert_eq!(outcome.selected_block_count, 1);
    assert_eq!(outcome.canonical_raw_log_count, 1);
    assert_eq!(outcome.normalized_event_inserted_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM normalized_events WHERE block_hash = $1 AND log_index = 1"
        )
        .bind(&block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_block_hash_replay_fails_closed_for_stateful_authority()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let watched_address = "0x0000000000000000000000000000000000000001";
    let block = provider_block(
        "0xd5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        104,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        33,
        chain,
        Uuid::from_u128(0x933),
        watched_address,
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockHashes(vec![
                block.block_hash.clone(),
            ]),
        },
    )
    .await
    .expect_err("stateful block-hash replay must fail closed");

    assert!(
        format!("{error:?}").contains("block-hash and source-scoped replay are disabled"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_block_hash_replay_fails_closed_for_ens_v2_registry_state()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let registry_address = "0x00000000000000000000000000000000000002a0";
    let block = provider_block(
        "0xa0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a002",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        120,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        120,
        "ens",
        "ens_v2_registry_l1",
        chain,
        "ens_v2",
        Uuid::from_u128(0x120),
        registry_address,
        "registry",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_resolver_log(
        database.pool(),
        chain,
        &block,
        registry_address,
        vec![keccak256_hex(
            b"LabelRegistered(uint256,bytes32,string,address,uint64,address)",
        )],
        Vec::new(),
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockHashes(vec![
                block.block_hash.clone(),
            ]),
        },
    )
    .await
    .expect_err("ENSv2 registry stateful block-hash replay must fail closed");

    assert!(
        format!("{error:?}").contains("block-hash and source-scoped replay are disabled"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_block_range_runs_ens_v2_closure_over_closed_emitter() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    create_complete_raw_log_staging_input_fixture(database.pool(), chain, 121).await?;
    let manifest_id = 121;
    let current_registry_id = Uuid::from_u128(0x1210);
    let retired_registry_id = Uuid::from_u128(0x1211);
    let current_registry = "0x00000000000000000000000000000000000002a1";
    let retired_registry = "0x00000000000000000000000000000000000002a2";
    let block = provider_block(
        "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a121",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        121,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        manifest_id,
        "ens",
        "ens_v2_registry_l1",
        chain,
        "ens_v2",
        current_registry_id,
        current_registry,
        "registry",
    )
    .await?;
    sqlx::query("UPDATE manifest_versions SET manifest_payload = $2 WHERE manifest_id = $1")
        .bind(manifest_id)
        .bind(test_manifest_payload())
        .execute(database.pool())
        .await?;
    insert_contract_instance(database.pool(), retired_registry_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        database.pool(),
        retired_registry_id,
        chain,
        retired_registry,
        Some(manifest_id),
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_from_block_number = 100,
            active_to_block_number = 121,
            deactivated_at = now()
        WHERE contract_instance_id = $1
        "#,
    )
    .bind(retired_registry_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_to_block_number,
            deactivated_at,
            provenance
        )
        VALUES ($1, 'subregistry', $2, $3, $4, $5, 'reachable_from_root', 100, 121, now(), $6)
        "#,
    )
    .bind(chain)
    .bind(current_registry_id)
    .bind(retired_registry_id)
    .bind(format!("ens_v2_registry_subregistry:{chain}"))
    .bind(manifest_id)
    .bind(json!({
        "observation_key": "closed-emitter",
        "to_address": retired_registry,
    }))
    .execute(database.pool())
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_resolver_log(
        database.pool(),
        chain,
        &block,
        retired_registry,
        vec![format!("0x{:064x}", 0xdead_u64)],
        Vec::new(),
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: 121,
                to_block: 121,
            },
        },
    )
    .await?;

    assert_eq!(outcome.canonical_raw_log_count, 1);
    assert_eq!(
        outcome.scanned_raw_log_count, 2,
        "the stateless boundary and ENSv2 full-closure boundary must each see the retained closed-emitter fact"
    );

    database.cleanup().await
}

#[tokio::test]
async fn manual_full_closure_replay_preserves_automatic_checkpoint_and_cleans_its_own() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_normalized_replay_adapter_checkpoint_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let block_number = 170;
    let registry_address = "0x0000000000000000000000000000000000000170";
    let block = provider_block(
        "0x1701701701701701701701701701701701701701701701701701701701700170",
        Some("0x1691691691691691691691691691691691691691691691691691691691690169"),
        block_number,
    );
    insert_active_replay_manifest_contract(
        database.pool(),
        170,
        "ens",
        "ens_v1_registry_l1",
        chain,
        "ens_v1",
        Uuid::from_u128(0x170),
        registry_address,
        "registry",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        chain,
        &block,
        registry_address,
        "0x0000000000000000000000000000000000000171",
        &namehash_for_dns_name(&dns_encoded_eth_name("checkpoint")),
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_adapter_checkpoints (
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            checkpoint_scope,
            replay_start_block_number,
            replay_target_block_number,
            status,
            state_payload
        )
        VALUES (
            'mainnet',
            $1,
            'raw_fact_normalized_events',
            'ens_v1_subregistry_discovery',
            'full_closure',
            $2,
            $2,
            'running',
            '{"sentinel":"automatic"}'::JSONB
        )
        "#,
    )
    .bind(chain)
    .bind(block_number)
    .execute(database.pool())
    .await?;

    replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: block_number,
                to_block: block_number,
            },
        },
    )
    .await?;

    let automatic_checkpoint = sqlx::query_as::<_, (String, Value)>(
        r#"
        SELECT status, state_payload
        FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = 'mainnet'
          AND chain_id = $1
          AND cursor_kind = 'raw_fact_normalized_events'
          AND adapter = 'ens_v1_subregistry_discovery'
          AND checkpoint_scope = 'full_closure'
        "#,
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        automatic_checkpoint,
        ("running".to_owned(), json!({"sentinel": "automatic"})),
        "manual replay must neither resume nor mutate the automatic catch-up checkpoint"
    );
    let manual_cursor_kind =
        format!("manual_raw_fact_normalized_events:{block_number}:{block_number}");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_replay_adapter_checkpoints
            WHERE deployment_profile = 'mainnet'
              AND chain_id = $1
              AND cursor_kind = $2
            "#,
        )
        .bind(chain)
        .bind(&manual_cursor_kind)
        .fetch_one(database.pool())
        .await?,
        0,
        "a successful manual replay must clear its resumable checkpoint namespace"
    );

    database.cleanup().await
}

#[tokio::test]
async fn manual_full_closure_cleans_checkpoint_before_releasing_ownership() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_adapter_checkpoint_tables(database.pool()).await?;
    let deployment_profile = "manual-cleanup-lock-profile";
    let chain = "manual-cleanup-lock-testnet";
    let cursor_kind = "manual_raw_fact_normalized_events:171:171";
    sqlx::query(
        r#"
        INSERT INTO normalized_replay_adapter_checkpoints (
            deployment_profile,
            chain_id,
            cursor_kind,
            adapter,
            checkpoint_scope,
            replay_start_block_number,
            replay_target_block_number,
            status,
            state_payload
        )
        VALUES
            ($1, $2, $3, 'ens_v1_subregistry_discovery', 'full_closure', 171, 171, 'running', '{}'::JSONB),
            ($1, $2, $3, 'ens_v1_unwrapped_authority', 'full_closure', 171, 171, 'running', '{}'::JSONB)
        "#,
    )
    .bind(deployment_profile)
    .bind(chain)
    .bind(cursor_kind)
    .execute(database.pool())
    .await?;

    let release_hook =
        install_ownership_release_test_hook(database.pool(), deployment_profile, chain).await;
    let pool = database.pool().clone();
    let manual_replay = tokio::spawn(async move {
        sync_manual_full_closure_normalized_events_from_persisted_raw_payloads(
            &pool,
            deployment_profile,
            chain,
            cursor_kind,
            171,
            171,
            &[],
            100_000,
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        release_hook.wait_until_before_release(),
    )
    .await
    .context("manual full-closure replay did not reach the ownership-release barrier")?;

    let observation = async {
        let remaining_checkpoints = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_replay_adapter_checkpoints
            WHERE deployment_profile = $1
              AND chain_id = $2
              AND cursor_kind = $3
            "#,
        )
        .bind(deployment_profile)
        .bind(chain)
        .bind(cursor_kind)
        .fetch_one(database.pool())
        .await?;

        let ownership = sqlx::postgres::PgAdvisoryLock::new(format!(
            "bigname:indexer:full-closure-replay:{deployment_profile}:{chain}"
        ));
        let ownership_available = match ownership
            .try_acquire(database.pool().acquire().await?)
            .await?
        {
            sqlx::Either::Left(guard) => {
                guard.release_now().await?;
                true
            }
            sqlx::Either::Right(_) => false,
        };
        Ok::<_, anyhow::Error>((remaining_checkpoints, ownership_available))
    }
    .await;

    release_hook.resume();
    tokio::time::timeout(std::time::Duration::from_secs(5), manual_replay)
        .await
        .context("manual full-closure replay did not release ownership after the barrier")?
        .context("manual full-closure replay task panicked")??;
    let (remaining_checkpoints, ownership_available) = observation?;
    assert_eq!(
        remaining_checkpoints, 0,
        "manual checkpoints must be cleared before the ownership guard is released"
    );
    assert!(
        !ownership_available,
        "profile/chain ownership must remain held while successful manual checkpoints are cleared"
    );

    database.cleanup().await
}

#[tokio::test]
async fn full_closure_replay_waits_for_chain_ownership_across_checkpoint_namespaces() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let deployment_profile = "mainnet";
    let chain = "ethereum-mainnet";
    let ownership = sqlx::postgres::PgAdvisoryLock::new(format!(
        "bigname:indexer:full-closure-replay:{deployment_profile}:{chain}"
    ));
    let mut ownership_connection = database.pool().acquire().await?;
    let ownership_backend_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *ownership_connection)
        .await?;
    let ownership_guard = ownership.acquire(ownership_connection).await?;

    let pool = database.pool().clone();
    let mut manual_replay = tokio::spawn(async move {
        sync_manual_full_closure_normalized_events_from_persisted_raw_payloads(
            &pool,
            deployment_profile,
            chain,
            "manual_raw_fact_normalized_events:170:170",
            170,
            170,
            &[],
            100_000,
        )
        .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(250), &mut manual_replay)
            .await
            .is_err(),
        "manual full-closure replay must wait while automatic catch-up owns the profile/chain fence"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND pid <> $1
              AND state = 'active'
              AND query LIKE 'SELECT pg_advisory_lock(%'
            "#,
        )
        .bind(ownership_backend_pid)
        .fetch_one(database.pool())
        .await?,
        0,
        "a full-closure contender must poll instead of retaining a blocking advisory-lock statement"
    );

    ownership_guard.release_now().await?;
    tokio::time::timeout(std::time::Duration::from_secs(5), manual_replay)
        .await
        .context("manual full-closure replay did not resume after ownership release")?
        .context("manual full-closure replay task panicked")??;

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_block_hash_replay_fails_closed_for_contextual_ens_v2_links()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let registrar_address = "0x00000000000000000000000000000000000002b0";
    let block = provider_block(
        "0xb0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b002b0",
        Some("0x1111111111111111111111111111111111111111111111111111111111111111"),
        121,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        121,
        "ens",
        "ens_v2_registrar_l1",
        chain,
        "ens_v2",
        Uuid::from_u128(0x121),
        registrar_address,
        "registrar",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_resolver_log(
        database.pool(),
        chain,
        &block,
        registrar_address,
        vec![keccak256_hex(
            b"NameRenewed(uint256,string,uint64,uint64,address,bytes32,uint256)",
        )],
        Vec::new(),
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockHashes(vec![
                block.block_hash.clone(),
            ]),
        },
    )
    .await
    .expect_err("ENSv2 contextual block-hash replay must fail closed");

    assert!(
        format!("{error:?}").contains("block-hash and source-scoped replay are disabled"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_middle_stateful_block_range_fails_closed() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let watched_address = "0x0000000000000000000000000000000000000041";
    let closure_block = provider_block(
        "0x4141414141414141414141414141414141414141414141414141414141414141",
        Some("0x4040404040404040404040404040404040404040404040404040404040404040"),
        41,
    );
    let middle_block = provider_block(
        "0x4242424242424242424242424242424242424242424242424242424242424242",
        Some(&closure_block.block_hash),
        42,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        34,
        chain,
        Uuid::from_u128(0x934),
        watched_address,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &closure_block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &middle_block,
        watched_address,
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: middle_block.block_number,
                to_block: middle_block.block_number,
            },
        },
    )
    .await
    .expect_err("middle stateful replay must start from closure boundary");

    assert!(
        format!("{error:?}").contains("must start at closure boundary block 41"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_retired_emitter_sets_full_closure_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_adapter_checkpoint_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let current_wrapper_id = Uuid::from_u128(0x960);
    let retired_wrapper_id = Uuid::from_u128(0x961);
    let current_wrapper = "0x0000000000000000000000000000000000000060";
    let retired_wrapper = "0x0000000000000000000000000000000000000040";
    let retained_boundary_block = provider_block(
        "0x4040404040404040404040404040404040404040404040404040404040404040",
        Some("0x3939393939393939393939393939393939393939393939393939393939393939"),
        40,
    );
    let requested_block = provider_block(
        "0x6060606060606060606060606060606060606060606060606060606060606060",
        Some(&retained_boundary_block.block_hash),
        60,
    );

    insert_active_replay_watched_contract(
        database.pool(),
        60,
        chain,
        current_wrapper_id,
        current_wrapper,
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_from_block_number = 51
        WHERE contract_instance_id = $1
        "#,
    )
    .bind(current_wrapper_id)
    .execute(database.pool())
    .await?;
    insert_contract_instance(database.pool(), retired_wrapper_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        database.pool(),
        retired_wrapper_id,
        chain,
        retired_wrapper,
        Some(60),
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_from_block_number = 1,
            active_to_block_number = 50,
            deactivated_at = now()
        WHERE contract_instance_id = $1
        "#,
    )
    .bind(retired_wrapper_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_to_block_number,
            deactivated_at,
            provenance
        )
        VALUES (
            $1,
            'subregistry',
            $2,
            $3,
            $4,
            60,
            'reachable_from_root',
            1,
            50,
            now(),
            $5
        )
        "#,
    )
    .bind(chain)
    .bind(current_wrapper_id)
    .bind(retired_wrapper_id)
    .bind(format!("test_retired_wrapper:{chain}"))
    .bind(json!({
        "observation_key": "retired-wrapper",
        "to_address": retired_wrapper,
    }))
    .execute(database.pool())
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &retained_boundary_block,
        retired_wrapper,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_name_wrapped_log(
        database.pool(),
        chain,
        &requested_block,
        current_wrapper,
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: requested_block.block_number,
                to_block: requested_block.block_number,
            },
        },
    )
    .await
    .expect_err("retired stateful emitters must contribute to the closure boundary");

    assert!(
        format!("{error:?}").contains("must start at closure boundary block 40"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?,
        0
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_full_closure_mutates_selected_discovery_only() -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_adapter_checkpoint_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    let registry_manifest_id = 30;
    let resolver_manifest_id = 31;
    let registry_contract_instance_id = Uuid::from_u128(0x930);
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let selected_resolver = "0x00000000000000000000000000000000000000c1";
    let unselected_resolver = "0x00000000000000000000000000000000000000c2";
    let selected_block = provider_block(
        "0x7070707070707070707070707070707070707070707070707070707070707070",
        Some("0x6060606060606060606060606060606060606060606060606060606060606060"),
        70,
    );
    let unselected_block = provider_block(
        "0x7171717171717171717171717171717171717171717171717171717171717171",
        Some(&selected_block.block_hash),
        71,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        registry_manifest_id,
        "ens",
        "ens_v1_registry_l1",
        chain,
        "ens_v1",
        registry_contract_instance_id,
        registry_address,
        "registry",
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        registry_manifest_id,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_discovery_rule(
        database.pool(),
        registry_manifest_id,
        "resolver",
        "registry",
        "reachable_from_root",
    )
    .await?;
    insert_active_replay_manifest(
        database.pool(),
        resolver_manifest_id,
        "ens",
        "ens_v1_resolver_l1",
        chain,
        "ens_v1",
    )
    .await?;

    for block in [&selected_block, &unselected_block] {
        insert_chain_lineage_for_block(database.pool(), chain, block, CanonicalityState::Canonical)
            .await?;
    }
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        chain,
        &selected_block,
        registry_address,
        selected_resolver,
        &namehash_for_dns_name(&dns_encoded_eth_name("selected")),
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        chain,
        &unselected_block,
        registry_address,
        unselected_resolver,
        &namehash_for_dns_name(&dns_encoded_eth_name("unselected")),
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: selected_block.block_number,
                to_block: selected_block.block_number,
            },
        },
    )
    .await?;

    assert_eq!(outcome.selected_block_count, 1);
    assert_eq!(outcome.canonical_raw_log_count, 1);
    assert_eq!(outcome.matched_raw_log_count, 2);
    assert_eq!(outcome.normalized_event_inserted_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM discovery_edges")
            .fetch_one(database.pool())
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT lower(cia.address)
            FROM discovery_edges de
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE de.deactivated_at IS NULL
            "#
        )
        .fetch_one(database.pool())
        .await?,
        selected_resolver
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE raw_fact_ref->>'block_hash' = $1"
        )
        .bind(&unselected_block.block_hash)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE derivation_kind = 'ens_v1_registry_resolver_changed'
            "#
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_full_closure_fails_closed_after_raw_log_compaction() -> Result<()>
{
    let database = TestDatabase::new().await?;
    create_ops_catchup_backfill_job_tables(database.pool()).await?;
    create_normalized_replay_adapter_checkpoint_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    create_complete_raw_log_staging_input_fixture(database.pool(), chain, 73).await?;
    let registry_manifest_id = 32;
    let registry_contract_instance_id = Uuid::from_u128(0x932);
    let registry_address = "0x00000000000000000000000000000000000000d1";
    let resolver_address = "0x00000000000000000000000000000000000000d2";
    let block = provider_block(
        "0x7272727272727272727272727272727272727272727272727272727272727272",
        Some("0x6161616161616161616161616161616161616161616161616161616161616161"),
        72,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        registry_manifest_id,
        "ens",
        "ens_v1_registry_l1",
        chain,
        "ens_v1",
        registry_contract_instance_id,
        registry_address,
        "registry",
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        registry_manifest_id,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_discovery_rule(
        database.pool(),
        registry_manifest_id,
        "resolver",
        "registry",
        "reachable_from_root",
    )
    .await?;
    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        chain,
        &block,
        registry_address,
        resolver_address,
        &namehash_for_dns_name(&dns_encoded_eth_name("retained")),
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: block.block_number,
                to_block: block.block_number,
            },
        },
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    sqlx::query("TRUNCATE raw_logs")
        .execute(database.pool())
        .await
        .context("failed to simulate supported raw-log staging compaction")?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET revision = revision + 1,
            retention_generation = retention_generation + 1,
            retained_history_complete = false,
            incomplete_since = clock_timestamp(),
            proven_retention_generation = NULL,
            proven_discovery_admission_epoch = NULL,
            proven_through_block = NULL
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await
    .context("failed to rotate the test raw-log retention generation")?;

    let suffix_block = provider_block(
        "0x7373737373737373737373737373737373737373737373737373737373737373",
        Some(&block.block_hash),
        block.block_number + 1,
    );
    insert_chain_lineage_for_block(
        database.pool(),
        chain,
        &suffix_block,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        chain,
        &suffix_block,
        registry_address,
        "0x00000000000000000000000000000000000000d3",
        &namehash_for_dns_name(&dns_encoded_eth_name("suffix-only")),
        0,
        CanonicalityState::Canonical,
    )
    .await?;

    let error = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: suffix_block.block_number,
                to_block: suffix_block.block_number,
            },
        },
    )
    .await
    .expect_err("a retained suffix from an incomplete generation must not define full closure");

    assert!(
        format!("{error:?}").contains("incomplete raw-log retention generation"),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "failed replay must not deactivate retained discovery"
    );

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_accepts_empty_generation_zero_closure_from_block_zero()
-> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_adapter_checkpoint_tables(database.pool()).await?;
    let chain = "ethereum-mainnet";
    create_complete_raw_log_staging_input_fixture(database.pool(), chain, 0).await?;
    insert_active_replay_watched_contract(
        database.pool(),
        33,
        chain,
        Uuid::from_u128(0x933),
        "0x00000000000000000000000000000000000000d4",
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: 0,
                to_block: 0,
            },
        },
    )
    .await?;

    assert_eq!(outcome.selected_block_count, 0);
    assert_eq!(outcome.canonical_raw_log_count, 0);
    assert_eq!(outcome.normalized_event_synced_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn replay_normalized_events_uses_generic_ensv1_dynamic_resolver_scope() -> Result<()> {
    assert_dynamic_resolver_replay_scope_behavior(DynamicResolverReplayConfig {
        namespace: "ens",
        chain: "ethereum-mainnet",
        deployment_epoch: "ens_v1",
        reverse_source_family: "ens_v1_reverse_l1",
        registry_source_family: "ens_v1_registry_l1",
        resolver_source_family: "ens_v1_resolver_l1",
        in_range_raw_name: "alice.eth",
        closed_raw_name: "closed.eth",
        manifest_id_base: 300,
        uuid_base: 0x3000,
    })
    .await
}

#[tokio::test]
async fn replay_normalized_events_respects_basenames_dynamic_resolver_watch_target_range()
-> Result<()> {
    assert_dynamic_resolver_replay_scope_behavior(DynamicResolverReplayConfig {
        namespace: "basenames",
        chain: "base-mainnet",
        deployment_epoch: "basenames_v1",
        reverse_source_family: "basenames_base_primary",
        registry_source_family: "basenames_base_registry",
        resolver_source_family: "basenames_base_resolver",
        in_range_raw_name: "alice.base.eth",
        closed_raw_name: "closed.base.eth",
        manifest_id_base: 400,
        uuid_base: 0x4000,
    })
    .await
}

async fn insert_active_replay_watched_contract(
    pool: &PgPool,
    manifest_id: i64,
    chain: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> Result<()> {
    insert_active_replay_watched_contract_with_source_family(
        pool,
        manifest_id,
        chain,
        "ens_v1_wrapper_l1",
        contract_instance_id,
        address,
        "name_wrapper",
    )
    .await
}

struct DynamicResolverReplayConfig {
    namespace: &'static str,
    chain: &'static str,
    deployment_epoch: &'static str,
    reverse_source_family: &'static str,
    registry_source_family: &'static str,
    resolver_source_family: &'static str,
    in_range_raw_name: &'static str,
    closed_raw_name: &'static str,
    manifest_id_base: i64,
    uuid_base: u128,
}

async fn assert_dynamic_resolver_replay_scope_behavior(
    config: DynamicResolverReplayConfig,
) -> Result<()> {
    let database = TestDatabase::new().await?;
    create_normalized_replay_adapter_checkpoint_tables(database.pool()).await?;
    let reverse_manifest_id = config.manifest_id_base + 1;
    let registry_manifest_id = config.manifest_id_base + 2;
    let resolver_manifest_id = config.manifest_id_base + 3;
    let reverse_contract_instance_id = Uuid::from_u128(config.uuid_base + 1);
    let registry_contract_instance_id = Uuid::from_u128(config.uuid_base + 2);
    let seed_resolver_contract_instance_id = Uuid::from_u128(config.uuid_base + 3);
    let supported_resolver_contract_instance_id = Uuid::from_u128(config.uuid_base + 4);
    let pending_resolver_contract_instance_id = Uuid::from_u128(config.uuid_base + 5);
    let unsupported_resolver_contract_instance_id = Uuid::from_u128(config.uuid_base + 6);
    let reverse_address = "0x00000000000000000000000000000000000000ad";
    let registry_address = "0x00000000000000000000000000000000000000bb";
    let seed_resolver_address = "0x00000000000000000000000000000000000000c0";
    let supported_resolver_address = "0x00000000000000000000000000000000000000c1";
    let pending_resolver_address = "0x00000000000000000000000000000000000000c2";
    let unsupported_resolver_address = "0x00000000000000000000000000000000000000c3";
    let unadmitted_resolver_address = "0x00000000000000000000000000000000000000dd";
    let claimed_address = "0x0000000000000000000000000000000000001234";
    let resolver_seed_role = if config.resolver_source_family == "ens_v1_resolver_l1" {
        "public_resolver"
    } else {
        "resolver"
    };
    let supported_profile_code_hash = if config.resolver_source_family == "ens_v1_resolver_l1" {
        "0x1111111111111111111111111111111111111111111111111111111111111111"
    } else {
        "0x2222222222222222222222222222222222222222222222222222222222222222"
    };
    let unsupported_profile_code_hash =
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let reverse_node = reverse_node_for_chain(config.chain, claimed_address);
    let block_50 = provider_block(
        "0x5050505050505050505050505050505050505050505050505050505050505050",
        Some("0x4040404040404040404040404040404040404040404040404040404040404040"),
        50,
    );
    let block_51 = provider_block(
        "0x5151515151515151515151515151515151515151515151515151515151515151",
        Some(&block_50.block_hash),
        51,
    );
    let orphaned_block_51 = provider_block(
        "0x51515151515151515151515151515151515151515151515151515151515151ff",
        Some(&block_50.block_hash),
        51,
    );
    let block_52 = provider_block(
        "0x5252525252525252525252525252525252525252525252525252525252525252",
        Some(&block_51.block_hash),
        52,
    );

    insert_active_replay_manifest_contract(
        database.pool(),
        reverse_manifest_id,
        config.namespace,
        config.reverse_source_family,
        config.chain,
        config.deployment_epoch,
        reverse_contract_instance_id,
        reverse_address,
        "reverse_registrar",
    )
    .await?;
    insert_active_replay_manifest_contract(
        database.pool(),
        registry_manifest_id,
        config.namespace,
        config.registry_source_family,
        config.chain,
        config.deployment_epoch,
        registry_contract_instance_id,
        registry_address,
        "registry",
    )
    .await?;
    insert_manifest_root_contract_instance(
        database.pool(),
        registry_manifest_id,
        registry_contract_instance_id,
        registry_address,
    )
    .await?;
    insert_manifest_discovery_rule(
        database.pool(),
        registry_manifest_id,
        "resolver",
        "registry",
        "reachable_from_root",
    )
    .await?;
    insert_active_replay_manifest(
        database.pool(),
        resolver_manifest_id,
        config.namespace,
        config.resolver_source_family,
        config.chain,
        config.deployment_epoch,
    )
    .await?;
    for (contract_instance_id, address, source_manifest_id) in [
        (
            seed_resolver_contract_instance_id,
            seed_resolver_address,
            Some(resolver_manifest_id),
        ),
        (
            supported_resolver_contract_instance_id,
            supported_resolver_address,
            None,
        ),
        (
            pending_resolver_contract_instance_id,
            pending_resolver_address,
            None,
        ),
        (
            unsupported_resolver_contract_instance_id,
            unsupported_resolver_address,
            None,
        ),
    ] {
        insert_contract_instance(
            database.pool(),
            contract_instance_id,
            config.chain,
            "contract",
        )
        .await?;
        insert_active_contract_instance_address(
            database.pool(),
            contract_instance_id,
            config.chain,
            address,
            source_manifest_id,
        )
        .await?;
    }
    insert_manifest_contract_instance(
        database.pool(),
        resolver_manifest_id,
        resolver_seed_role,
        seed_resolver_contract_instance_id,
        seed_resolver_address,
        "none",
        None,
        None,
    )
    .await?;
    for contract_instance_id in [
        supported_resolver_contract_instance_id,
        pending_resolver_contract_instance_id,
    ] {
        insert_active_discovery_edge_with_range(
            database.pool(),
            config.chain,
            "resolver",
            registry_contract_instance_id,
            contract_instance_id,
            Some(registry_manifest_id),
            Some(50),
            Some(51),
        )
        .await?;
    }

    for (block, canonicality_state) in [
        (&block_50, CanonicalityState::Canonical),
        (&block_51, CanonicalityState::Canonical),
        (&orphaned_block_51, CanonicalityState::Orphaned),
        (&block_52, CanonicalityState::Canonical),
    ] {
        insert_chain_lineage_for_block(database.pool(), config.chain, block, canonicality_state)
            .await?;
    }
    upsert_raw_code_hashes(
        database.pool(),
        &[
            RawCodeHash {
                chain_id: config.chain.to_owned(),
                block_hash: block_50.block_hash.clone(),
                block_number: block_50.block_number,
                contract_address: seed_resolver_address.to_owned(),
                code_hash: supported_profile_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: config.chain.to_owned(),
                block_hash: block_50.block_hash.clone(),
                block_number: block_50.block_number,
                contract_address: supported_resolver_address.to_owned(),
                code_hash: supported_profile_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
            RawCodeHash {
                chain_id: config.chain.to_owned(),
                block_hash: block_50.block_hash.clone(),
                block_number: block_50.block_number,
                contract_address: unsupported_resolver_address.to_owned(),
                code_hash: unsupported_profile_code_hash.to_owned(),
                code_byte_length: 5,
                canonicality_state: CanonicalityState::Canonical,
            },
        ],
    )
    .await?;
    insert_raw_reverse_claimed_log(
        database.pool(),
        config.chain,
        &block_50,
        reverse_address,
        claimed_address,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        config.chain,
        &block_50,
        registry_address,
        supported_resolver_address,
        &reverse_node,
        1,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        config.chain,
        &block_50,
        registry_address,
        pending_resolver_address,
        &reverse_node,
        5,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_new_resolver_log_for_node_at_index(
        database.pool(),
        config.chain,
        &block_50,
        registry_address,
        unsupported_resolver_address,
        &reverse_node,
        8,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        supported_resolver_address,
        &reverse_node,
        config.in_range_raw_name,
        2,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        unadmitted_resolver_address,
        &reverse_node,
        "unadmitted.example",
        4,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        pending_resolver_address,
        &reverse_node,
        "pending.example",
        6,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        unsupported_resolver_address,
        &reverse_node,
        "unsupported.example",
        9,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_version_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        supported_resolver_address,
        &reverse_node,
        7,
        3,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_version_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_50,
        pending_resolver_address,
        &reverse_node,
        8,
        7,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_version_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_51,
        unsupported_resolver_address,
        &reverse_node,
        9,
        2,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_name_changed_log_for_node(
        database.pool(),
        config.chain,
        &block_52,
        supported_resolver_address,
        &reverse_node,
        config.closed_raw_name,
        0,
        CanonicalityState::Canonical,
    )
    .await?;
    insert_raw_resolver_version_changed_log_for_node(
        database.pool(),
        config.chain,
        &orphaned_block_51,
        supported_resolver_address,
        &reverse_node,
        99,
        0,
        CanonicalityState::Orphaned,
    )
    .await?;

    let outcome = replay_raw_fact_normalized_events(
        database.pool(),
        RawFactNormalizedEventReplayRequest {
            deployment_profile: "mainnet".to_owned(),
            chain: config.chain.to_owned(),
            selection: RawFactNormalizedEventReplaySelection::BlockRange {
                from_block: 50,
                to_block: 52,
            },
        },
    )
    .await?;

    assert!(outcome.normalized_event_synced_count > 0);
    assert!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
            .fetch_one(database.pool())
            .await?
            > 0
    );

    database.cleanup().await
}

#[allow(clippy::too_many_arguments)]
async fn insert_raw_new_resolver_log_for_node_at_index(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    emitting_address: &str,
    resolver: &str,
    node: &str,
    log_index: i64,
    canonicality_state: CanonicalityState,
) -> Result<()> {
    upsert_raw_blocks(
        pool,
        &[provider_block_to_raw_block(
            chain,
            block,
            canonicality_state,
        )],
    )
    .await?;
    upsert_raw_logs(
        pool,
        &[RawLog {
            chain_id: chain.to_owned(),
            block_hash: block.block_hash.clone(),
            block_number: block.block_number,
            transaction_hash: transaction_hash_for_block(block),
            transaction_index: 0,
            log_index,
            emitting_address: emitting_address.to_ascii_lowercase(),
            topics: vec![registry_new_resolver_topic0(), node.to_owned()],
            data: decode_hex_string(&encode_registry_new_resolver_log_data(resolver)),
            canonicality_state,
        }],
    )
    .await?;

    Ok(())
}

async fn insert_stale_reverse_changed_event(
    pool: &PgPool,
    chain: &str,
    source_manifest_id: i64,
    block: &ProviderBlock,
    emitting_address: &str,
    claimed_address: &str,
) -> Result<()> {
    let claimed_address = claimed_address.to_ascii_lowercase();
    let transaction_hash = transaction_hash_for_block(block);
    let event_identity = format!(
        "ens_v1_reverse_claim:ReverseChanged:{}:{}:{}:{}:{}",
        source_manifest_id, block.block_hash, transaction_hash, 0, claimed_address
    );
    let raw_fact_ref = json!({
        "kind": "raw_log",
        "chain_id": chain,
        "block_hash": block.block_hash,
        "block_number": block.block_number,
        "transaction_hash": transaction_hash,
        "transaction_index": 0,
        "log_index": 0,
        "emitting_address": emitting_address.to_ascii_lowercase(),
    });

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
            $1,
            'ens',
            'ReverseChanged',
            'ens_v1_reverse_l1',
            1,
            $2,
            $3,
            $4,
            $5,
            $6,
            0,
            $7::jsonb,
            'ens_v1_reverse_claim',
            'canonical',
            '{}'::jsonb,
            '{"source_event":"ReverseClaimed","reverse_name":"stale.addr.reverse"}'::jsonb
        )
        "#,
    )
    .bind(event_identity)
    .bind(source_manifest_id)
    .bind(chain)
    .bind(block.block_number)
    .bind(&block.block_hash)
    .bind(transaction_hash)
    .bind(raw_fact_ref.to_string())
    .execute(pool)
    .await
    .context("failed to insert stale reverse normalized event for replay test")?;

    Ok(())
}

async fn insert_active_replay_watched_contract_with_source_family(
    pool: &PgPool,
    manifest_id: i64,
    chain: &str,
    source_family: &str,
    contract_instance_id: Uuid,
    address: &str,
    role: &str,
) -> Result<()> {
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
            $3,
            $2,
            'ens_v1',
            'active',
            'ensip15@ens-normalize-0.1.1',
            ('manifests/ens/' || $3 || '/v1.toml'),
            DEFAULT
        )
        "#,
    )
    .bind(manifest_id)
    .bind(chain)
    .bind(source_family)
    .execute(pool)
    .await
    .context("failed to insert manifest_versions for replay test")?;
    insert_contract_instance(pool, contract_instance_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        pool,
        contract_instance_id,
        chain,
        address,
        Some(manifest_id),
    )
    .await?;
    insert_manifest_contract_instance(
        pool,
        manifest_id,
        role,
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_active_replay_manifest_contract(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    source_family: &str,
    chain: &str,
    deployment_epoch: &str,
    contract_instance_id: Uuid,
    address: &str,
    role: &str,
) -> Result<()> {
    insert_active_replay_manifest(
        pool,
        manifest_id,
        namespace,
        source_family,
        chain,
        deployment_epoch,
    )
    .await?;
    insert_contract_instance(pool, contract_instance_id, chain, "contract").await?;
    insert_active_contract_instance_address(
        pool,
        contract_instance_id,
        chain,
        address,
        Some(manifest_id),
    )
    .await?;
    insert_manifest_contract_instance(
        pool,
        manifest_id,
        role,
        contract_instance_id,
        address,
        "none",
        None,
        None,
    )
    .await
}

async fn insert_active_replay_manifest(
    pool: &PgPool,
    manifest_id: i64,
    namespace: &str,
    source_family: &str,
    chain: &str,
    deployment_epoch: &str,
) -> Result<()> {
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
            $2,
            $3,
            $4,
            $5,
            'active',
            'ensip15@ens-normalize-0.1.1',
            ('manifests/' || $2 || '/' || $3 || '/v1.toml'),
            DEFAULT
        )
        "#,
    )
    .bind(manifest_id)
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(deployment_epoch)
    .execute(pool)
    .await
    .context("failed to insert manifest_versions for dynamic resolver replay test")?;

    Ok(())
}
