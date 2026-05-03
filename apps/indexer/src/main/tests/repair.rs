#[tokio::test]
async fn repair_ens_v1_text_records_fetches_provider_logs_without_raw_log_staging() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000004976";
    let block = provider_block(
        "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        Some("0x8181818181818181818181818181818181818181818181818181818181818181"),
        777,
    );
    let node = namehash_for_dns_name(&dns_encoded_eth_name("alice"));

    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_generic_text_record_event(
        database.pool(),
        chain,
        &block,
        resolver_address,
        &node,
        "ens:alice.eth",
    )
    .await?;

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: block.clone(),
        logs: vec![rpc_resolver_text_changed_with_value_log_payload_for_namehash(
            &block,
            resolver_address,
            &node,
            "avatar",
            "https://euc.li/alice.eth",
            3,
        )],
    }])
    .await?;

    let outcome = repair_ens_v1_text_records_from_provider(
        database.pool(),
        &provider,
        EnsV1TextRecordRepairConfig {
            chain: chain.to_owned(),
            from_block: Some(block.block_number),
            to_block: Some(block.block_number),
            chunk_blocks: 1,
            candidate_page_size: 10,
        },
    )
    .await?;

    assert_eq!(outcome.candidate_count, 1);
    assert_eq!(outcome.fetched_log_count, 1);
    assert_eq!(outcome.matched_log_count, 1);
    assert_eq!(outcome.repaired_event_count, 1);
    assert_eq!(outcome.missing_log_count, 0);
    assert_eq!(outcome.skipped_decode_count, 0);

    let after_state = sqlx::query_scalar::<_, Value>(
        "SELECT after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(format!(
        "ens_v1_unwrapped_authority:RecordChanged:record-change:{}:{}:3",
        block.block_hash,
        transaction_hash_for_block(&block)
    ))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(after_state["record_key"], "text:avatar");
    assert_eq!(after_state["record_family"], "text");
    assert_eq!(after_state["selector_key"], "avatar");
    assert_eq!(after_state["value"], "https://euc.li/alice.eth");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        0
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn repair_ens_v1_text_records_escapes_nul_text_values_for_jsonb() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000004976";
    let block = provider_block(
        "0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2",
        Some("0x9292929292929292929292929292929292929292929292929292929292929292"),
        778,
    );
    let node = namehash_for_dns_name(&dns_encoded_eth_name("nul"));

    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_generic_text_record_event(
        database.pool(),
        chain,
        &block,
        resolver_address,
        &node,
        "ens:nul.eth",
    )
    .await?;

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: block.clone(),
        logs: vec![rpc_resolver_text_changed_with_value_log_payload_for_namehash(
            &block,
            resolver_address,
            &node,
            "avatar",
            "ipfs://avatar\0tail",
            3,
        )],
    }])
    .await?;

    let outcome = repair_ens_v1_text_records_from_provider(
        database.pool(),
        &provider,
        EnsV1TextRecordRepairConfig {
            chain: chain.to_owned(),
            from_block: Some(block.block_number),
            to_block: Some(block.block_number),
            chunk_blocks: 1,
            candidate_page_size: 10,
        },
    )
    .await?;

    assert_eq!(outcome.candidate_count, 1);
    assert_eq!(outcome.repaired_event_count, 1);

    let after_state = sqlx::query_scalar::<_, Value>(
        "SELECT after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(format!(
        "ens_v1_unwrapped_authority:RecordChanged:record-change:{}:{}:3",
        block.block_hash,
        transaction_hash_for_block(&block)
    ))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(after_state["record_key"], "text:avatar");
    assert_eq!(after_state["selector_key"], "avatar");
    assert_eq!(after_state["value"], "ipfs://avatar\\u0000tail");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM raw_logs")
            .fetch_one(database.pool())
            .await?,
        0
    );

    server.abort();
    database.cleanup().await
}

#[tokio::test]
async fn repair_ens_v1_text_records_fills_selectorized_rows_missing_value() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let resolver_address = "0x0000000000000000000000000000000000004976";
    let block = provider_block(
        "0xc3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3",
        Some("0xa3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3"),
        779,
    );
    let node = namehash_for_dns_name(&dns_encoded_eth_name("selector"));

    insert_chain_lineage_for_block(database.pool(), chain, &block, CanonicalityState::Canonical)
        .await?;
    insert_selectorized_text_record_event_without_value(
        database.pool(),
        chain,
        &block,
        resolver_address,
        &node,
        "ens:selector.eth",
        "avatar",
    )
    .await?;

    let (provider, server) = bundle_provider_with_fixtures(vec![ProviderBlockFixture {
        block: block.clone(),
        logs: vec![rpc_resolver_text_changed_with_value_log_payload_for_namehash(
            &block,
            resolver_address,
            &node,
            "avatar",
            "https://euc.li/selector.eth",
            3,
        )],
    }])
    .await?;

    let outcome = repair_ens_v1_text_records_from_provider(
        database.pool(),
        &provider,
        EnsV1TextRecordRepairConfig {
            chain: chain.to_owned(),
            from_block: Some(block.block_number),
            to_block: Some(block.block_number),
            chunk_blocks: 1,
            candidate_page_size: 10,
        },
    )
    .await?;

    assert_eq!(outcome.candidate_count, 1);
    assert_eq!(outcome.repaired_event_count, 1);
    assert_eq!(outcome.missing_log_count, 0);
    assert_eq!(outcome.skipped_decode_count, 0);

    let after_state = sqlx::query_scalar::<_, Value>(
        "SELECT after_state FROM normalized_events WHERE event_identity = $1",
    )
    .bind(format!(
        "ens_v1_unwrapped_authority:RecordChanged:record-change:{}:{}:3",
        block.block_hash,
        transaction_hash_for_block(&block)
    ))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(after_state["record_key"], "text:avatar");
    assert_eq!(after_state["selector_key"], "avatar");
    assert_eq!(after_state["value"], "https://euc.li/selector.eth");

    server.abort();
    database.cleanup().await
}

async fn insert_generic_text_record_event(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    resolver_address: &str,
    node: &str,
    logical_name_id: &str,
) -> Result<()> {
    insert_text_record_event_without_value(
        pool,
        chain,
        block,
        resolver_address,
        node,
        logical_name_id,
        "text",
        None,
    )
    .await
}

async fn insert_selectorized_text_record_event_without_value(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    resolver_address: &str,
    node: &str,
    logical_name_id: &str,
    selector_key: &str,
) -> Result<()> {
    insert_text_record_event_without_value(
        pool,
        chain,
        block,
        resolver_address,
        node,
        logical_name_id,
        &format!("text:{selector_key}"),
        Some(selector_key),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_text_record_event_without_value(
    pool: &PgPool,
    chain: &str,
    block: &ProviderBlock,
    resolver_address: &str,
    node: &str,
    logical_name_id: &str,
    record_key: &str,
    selector_key: Option<&str>,
) -> Result<()> {
    let transaction_hash = transaction_hash_for_block(block);
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            logical_name_id,
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
            $2,
            'RecordChanged',
            'ens_v1_resolver_l1',
            1,
            1,
            $3,
            $4,
            $5,
            $6,
            3,
            $7::jsonb,
            'ens_v1_unwrapped_authority',
            'canonical',
            '{}'::jsonb,
            $8::jsonb
        )
        "#,
    )
    .bind(format!(
        "ens_v1_unwrapped_authority:RecordChanged:record-change:{}:{}:3",
        block.block_hash, transaction_hash
    ))
    .bind(logical_name_id)
    .bind(chain)
    .bind(block.block_number)
    .bind(&block.block_hash)
    .bind(&transaction_hash)
    .bind(
        json!({
            "kind": "raw_log",
            "chain_id": chain,
            "block_hash": block.block_hash,
            "block_number": block.block_number,
            "transaction_hash": transaction_hash,
            "transaction_index": 0,
            "log_index": 3,
        })
        .to_string(),
    )
    .bind(
        json!({
            "record_key": record_key,
            "record_family": "text",
            "selector_key": selector_key,
            "resolver": resolver_address,
            "node": node,
        })
        .to_string(),
    )
    .execute(pool)
    .await
    .context("failed to insert generic text record event for repair test")?;

    Ok(())
}

fn rpc_resolver_text_changed_with_value_log_payload_for_namehash(
    block: &ProviderBlock,
    address: &str,
    namehash: &str,
    key: &str,
    value: &str,
    log_index: u64,
) -> Value {
    json!({
        "blockHash": block.block_hash.clone(),
        "blockNumber": format!("0x{:x}", block.block_number),
        "transactionHash": transaction_hash_for_block(block),
        "transactionIndex": "0x0",
        "logIndex": format!("0x{log_index:x}"),
        "address": address,
        "topics": [
            resolver_text_changed_with_value_topic0(),
            namehash,
            keccak256_hex(key.as_bytes())
        ],
        "data": encode_two_dynamic_string_log_data(key, value)
    })
}
