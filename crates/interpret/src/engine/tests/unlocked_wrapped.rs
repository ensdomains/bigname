use super::*;

/// Exercises an unlocked-wrapped migration through production raw-fact loading, ordinary ENSv1
/// interpretation, test-only activation, and the real transition writer.
#[tokio::test]
async fn complete_unlocked_wrapped_migration_closes_the_reactivated_registrar_at_cleanup()
-> TestResult {
    run_unlocked_wrapped_migration(true).await
}

#[tokio::test]
async fn unlocked_wrapped_migration_closes_a_registrar_materialized_at_cleanup() -> TestResult {
    run_unlocked_wrapped_migration(false).await
}

async fn run_unlocked_wrapped_migration(prior_registrar_materialized: bool) -> TestResult {
    let database = database().await?;
    let pool = database.pool();
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;

    let label = b"unlocked-wrapped";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("ens:{namehash:#x}");
    seed_lineage(pool).await?;
    seed_wrapped_predecessor_facts(
        pool,
        label,
        labelhash,
        namehash,
        prior_registrar_materialized,
    )
    .await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: SETUP_BLOCK,
            to_block: PREDECESSOR_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let wrapper_binding_id: Uuid = sqlx::query_scalar(
        "SELECT surface_binding_id FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v1' AND active_to IS NULL",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await?;

    seed_unlocked_wrapped_migration_facts(pool, label, labelhash, namehash).await?;
    let loaded = load::batch_input(
        pool,
        CHAIN,
        MIGRATION_BLOCK,
        MIGRATION_BLOCK,
        None,
        None,
        StateCacheCapacity::Entries(65_536),
    )
    .await?;
    let expected_orphaning_epoch = loaded.prior_cache.validated_orphaning_epoch;
    let prepared = prepare_schema_v2_batch_incremental(
        loaded.input,
        loaded.adapter_session,
        StateCacheCapacity::Entries(65_536),
    )?;
    let state_values = load::prior_state_values(
        pool,
        CHAIN,
        MIGRATION_BLOCK,
        prepared.state_value_requests(),
    )
    .await?;
    let (mut output, _) = prepared.finish(state_values)?;
    inject_activated_transition_for_test(&mut output)?;
    assert_eq!(output.migration_authority_transitions.len(), 1);

    // The production phase runner owns diagnostic correlation persistence. This test exercises
    // production interpretation and the identity writer, as does the direct-unwrapped gate above.
    output.migration_event_associations.clear();
    output.migration_discovery_associations.clear();
    output.migration_candidate_identity_effects.clear();
    output.migration_candidate_discovery_effects.clear();
    let expected_lineage = [(MIGRATION_BLOCK, block_hash(MIGRATION_BLOCK))];
    write::batch(
        pool,
        CHAIN,
        None,
        false,
        true,
        expected_orphaning_epoch,
        &expected_lineage,
        &output,
    )
    .await?;

    let wrapper_closed_at: Option<time::OffsetDateTime> =
        sqlx::query_scalar("SELECT active_to FROM surface_bindings WHERE surface_binding_id = $1")
            .bind(wrapper_binding_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        wrapper_closed_at,
        Some(binding_time(MIGRATION_BLOCK, 0)?),
        "ordinary NameUnwrapped interpretation closes the wrapper binding first"
    );

    let registrar_predecessors: Vec<(Uuid, time::OffsetDateTime, Option<time::OffsetDateTime>)> =
        sqlx::query_as(&format!(
            "SELECT surface_binding_id, active_from, active_to FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = 'ens_v1' AND block_number = $3
           AND COALESCE((provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1) = 0
           AND COALESCE((provenance ->> '{LOG_INDEX_KEY}')::bigint, -1) = $4"
        ))
        .bind(CHAIN)
        .bind(&logical_name_id)
        .bind(MIGRATION_BLOCK)
        .bind(if prior_registrar_materialized { 0 } else { 1 })
        .fetch_all(pool)
        .await?;
    assert_eq!(
        registrar_predecessors.len(),
        1,
        "the unlocked-wrapped transaction must resolve exactly one registrar predecessor"
    );
    assert_eq!(
        registrar_predecessors[0].1,
        binding_time(MIGRATION_BLOCK, 0)?,
        "the registrar predecessor is active from the preceding unwrap"
    );
    let first_registrar_close = registrar_predecessors[0].2;
    assert_eq!(
        first_registrar_close,
        Some(binding_time(MIGRATION_BLOCK, 1)?),
        "the transition closes the reactivated registrar at its recorded transfer cleanup"
    );

    assert_eq!(
        current_binding_count(pool, &logical_name_id, "ens_v1").await?,
        0
    );
    assert_eq!(
        current_binding_count(pool, &logical_name_id, "ens_v2").await?,
        1
    );

    write::batch(
        pool,
        CHAIN,
        Some((MIGRATION_BLOCK, MIGRATION_BLOCK)),
        true,
        true,
        expected_orphaning_epoch,
        &expected_lineage,
        &output,
    )
    .await?;
    let replayed_close: Option<time::OffsetDateTime> =
        sqlx::query_scalar("SELECT active_to FROM surface_bindings WHERE surface_binding_id = $1")
            .bind(registrar_predecessors[0].0)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        replayed_close, first_registrar_close,
        "redo and reapplication converge at the recorded cleanup"
    );
    assert_eq!(
        current_binding_count(pool, &logical_name_id, "ens_v1").await?,
        0
    );
    assert_eq!(
        current_binding_count(pool, &logical_name_id, "ens_v2").await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

async fn seed_wrapped_predecessor_facts(
    pool: &PgPool,
    label: &[u8],
    labelhash: B256,
    namehash: B256,
    prior_registrar_materialized: bool,
) -> TestResult {
    let mut dns_name = Vec::with_capacity(label.len() + 6);
    dns_name.push(u8::try_from(label.len())?);
    dns_name.extend_from_slice(label);
    dns_name.extend_from_slice(b"\x03eth\0");

    insert_transaction(pool, SETUP_BLOCK, NAME_WRAPPER).await?;
    insert_log(
        pool,
        SETUP_BLOCK,
        0,
        NAME_WRAPPER,
        NameWrapped {
            node: namehash,
            name: dns_name.clone().into(),
            owner: OWNER.parse()?,
            fuses: 0,
            expiry: 1_900_000_000,
        }
        .encode_log_data(),
    )
    .await?;

    if !prior_registrar_materialized {
        return Ok(());
    }

    // A prior unwrap/re-wrap cycle materializes the registrar identity from the admitted
    // BaseRegistrar transfer and then leaves that identity dormant beneath the live wrapper.
    insert_transaction(pool, PREDECESSOR_BLOCK, NAME_WRAPPER).await?;
    insert_log(
        pool,
        PREDECESSOR_BLOCK,
        0,
        NAME_WRAPPER,
        NameUnwrapped {
            node: namehash,
            owner: OWNER.parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        PREDECESSOR_BLOCK,
        1,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: NAME_WRAPPER.parse()?,
            to: OWNER.parse()?,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        PREDECESSOR_BLOCK,
        2,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: OWNER.parse()?,
            to: NAME_WRAPPER.parse()?,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        PREDECESSOR_BLOCK,
        3,
        NAME_WRAPPER,
        NameWrapped {
            node: namehash,
            name: dns_name.into(),
            owner: OWNER.parse()?,
            fuses: 0,
            expiry: 1_900_000_000,
        }
        .encode_log_data(),
    )
    .await
}

async fn seed_unlocked_wrapped_migration_facts(
    pool: &PgPool,
    label: &[u8],
    labelhash: B256,
    namehash: B256,
) -> TestResult {
    let controller = UNLOCKED_CONTROLLER.parse::<Address>()?;
    let mut versioned = labelhash.0;
    versioned[28..].fill(0);
    let token = U256::from_be_bytes(versioned);
    insert_transaction(pool, MIGRATION_BLOCK, ETH_REGISTRY).await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        0,
        NAME_WRAPPER,
        NameUnwrapped {
            node: namehash,
            owner: GRAVEYARD.parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        1,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: NAME_WRAPPER.parse()?,
            to: GRAVEYARD.parse()?,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        2,
        ETH_REGISTRY,
        LabelRegistered {
            tokenId: token,
            labelHash: labelhash,
            label: std::str::from_utf8(label)?.to_owned(),
            owner: OWNER.parse()?,
            expiry: 1_900_000_000,
            sender: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        3,
        ETH_REGISTRY,
        TokenResource {
            tokenId: token,
            resource: token,
        }
        .encode_log_data(),
    )
    .await
}

async fn current_binding_count(pool: &PgPool, logical_name_id: &str, arm: &str) -> TestResult<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
           AND authority_arm = $3 AND active_to IS NULL
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .bind(arm)
    .fetch_one(pool)
    .await?)
}

fn binding_time(block_number: i64, log_index: i64) -> TestResult<time::OffsetDateTime> {
    Ok(time::OffsetDateTime::from_unix_timestamp(block_number)?
        + time::Duration::microseconds(log_index))
}
