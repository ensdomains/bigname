#[test]
fn ensure_manifest_root_ready_accepts_loaded_root() -> Result<()> {
    ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::Loaded))
}

#[test]
fn ensure_manifest_root_ready_accepts_empty_root() -> Result<()> {
    ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::Empty))
}

#[test]
fn ensure_manifest_root_ready_rejects_missing_root() {
    let error = ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::MissingRoot))
        .expect_err("missing root must fail");

    assert!(
        error
            .to_string()
            .contains("refusing to boot on stale stored manifest state")
    );
    assert!(
        error.to_string().contains("/tmp/manifests does not exist"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn ensure_manifest_root_ready_rejects_invalid_root() {
    let error = ensure_manifest_root_ready(&manifest_load_summary(ManifestLoadStatus::InvalidRoot))
        .expect_err("invalid root must fail");

    assert!(
        error
            .to_string()
            .contains("refusing to boot on stale stored manifest state")
    );
    assert!(
        error
            .to_string()
            .contains("/tmp/manifests is not a directory"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn verify_stored_manifest_state_accepts_matching_active_manifest_count() -> Result<()> {
    let database = TestDatabase::new().await?;
    let admission_state = load_discovery_admission_state(database.pool()).await?;

    verify_stored_manifest_state(&synced_manifest_summary(0), &admission_state)?;

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn verify_stored_manifest_state_rejects_mismatched_active_manifest_count() -> Result<()> {
    let database = TestDatabase::new().await?;
    let admission_state = load_discovery_admission_state(database.pool()).await?;

    let error = verify_stored_manifest_state(&synced_manifest_summary(1), &admission_state)
        .expect_err("mismatched counts must fail");

    assert!(
        error.to_string().contains(
            "stored active manifest count 0 does not match the synced active manifest count 1"
        ),
        "unexpected error: {error:#}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn load_watched_contract_summary_rebuilds_counts_from_storage() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(1);
    let contract_contract_instance_id = Uuid::from_u128(2);
    let discovered_contract_instance_id = Uuid::from_u128(3);
    let implementation_contract_instance_id = Uuid::from_u128(4);
    let shadow_contract_instance_id = Uuid::from_u128(5);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES
                (1, 'ethereum-mainnet', 'active'),
                (2, 'base-mainnet', 'shadow')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for watched summary test")?;
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

    insert_contract_instance(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
        Some(1),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        discovered_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        discovered_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000cc",
        Some(1),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registry",
        contract_contract_instance_id,
        "0x00000000000000000000000000000000000000aa",
        "erc1967",
        Some(implementation_contract_instance_id),
        Some("0x00000000000000000000000000000000000000dd"),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "subregistry",
        contract_contract_instance_id,
        discovered_contract_instance_id,
        Some(1),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "proxy_implementation",
        contract_contract_instance_id,
        implementation_contract_instance_id,
        Some(1),
    )
    .await?;

    insert_contract_instance(
        database.pool(),
        shadow_contract_instance_id,
        "base-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        shadow_contract_instance_id,
        "base-mainnet",
        "0x00000000000000000000000000000000000000bb",
        Some(2),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        2,
        "registry",
        shadow_contract_instance_id,
        "0x00000000000000000000000000000000000000bb",
        "none",
        None,
        None,
    )
    .await?;

    let summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(summary.unique_contract_count, 4);
    assert_eq!(summary.source_entry_count, 4);
    assert_eq!(summary.manifest_root_count, 1);
    assert_eq!(summary.manifest_contract_count, 1);
    assert_eq!(summary.discovery_edge_count, 2);
    assert_eq!(summary.chains.len(), 1);
    assert_eq!(summary.chains[0].chain, "ethereum-mainnet");
    assert_eq!(summary.chains[0].unique_contract_count, 4);
    assert_eq!(summary.chains[0].manifest_root_count, 1);
    assert_eq!(summary.chains[0].manifest_contract_count, 1);
    assert_eq!(summary.chains[0].discovery_edge_count, 2);

    let watched_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(watched_plan.len(), 1);
    assert_eq!(
        watched_plan[0].addresses,
        vec![
            "0x0000000000000000000000000000000000000001".to_owned(),
            "0x00000000000000000000000000000000000000aa".to_owned(),
            "0x00000000000000000000000000000000000000cc".to_owned(),
            "0x00000000000000000000000000000000000000dd".to_owned(),
        ]
    );
    assert_eq!(watched_plan[0].manifest_root_entry_count, 1);
    assert_eq!(watched_plan[0].manifest_contract_entry_count, 1);
    assert_eq!(watched_plan[0].discovery_edge_entry_count, 2);

    database.cleanup().await?;
    Ok(())
}

#[test]
fn watched_chain_plan_state_counts_chains_addresses_and_entries() {
    let state = watched_chain_plan_state(&[
        WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        },
        WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: vec!["0x00000000000000000000000000000000000000bb".to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        },
    ]);

    assert_eq!(
        state,
        WatchedChainPlanState {
            chain_count: 2,
            address_count: 3,
            entry_count: 4,
        }
    );
}

#[test]
fn intake_runtime_state_counts_checkpoint_modes() {
    let state = intake_runtime_state(&[
        IntakeChainTask {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
            checkpoint: ChainCheckpoint {
                chain_id: "ethereum-mainnet".to_owned(),
                canonical_block_hash: Some(
                    "0x00000000000000000000000000000000000000000000000000000000000000aa".to_owned(),
                ),
                canonical_block_number: Some(42),
                safe_block_hash: Some(
                    "0x0000000000000000000000000000000000000000000000000000000000000099".to_owned(),
                ),
                safe_block_number: Some(41),
                finalized_block_hash: None,
                finalized_block_number: None,
            },
        },
        IntakeChainTask {
            chain: "base-mainnet".to_owned(),
            addresses: vec!["0x00000000000000000000000000000000000000bb".to_owned()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
            checkpoint: ChainCheckpoint {
                chain_id: "base-mainnet".to_owned(),
                canonical_block_hash: None,
                canonical_block_number: None,
                safe_block_hash: None,
                safe_block_number: None,
                finalized_block_hash: None,
                finalized_block_number: None,
            },
        },
    ]);

    assert_eq!(
        state,
        IntakeRuntimeState {
            chain_count: 2,
            address_count: 3,
            entry_count: 4,
            cold_start_chain_count: 1,
            resumable_chain_count: 1,
            safe_checkpoint_chain_count: 1,
            finalized_checkpoint_chain_count: 0,
        }
    );
}

#[tokio::test]
async fn sync_intake_chain_tasks_creates_missing_checkpoint_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_contract_instance_id = Uuid::from_u128(11);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for intake task sync test")?;
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
    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].chain, "ethereum-mainnet");
    assert_eq!(
        tasks[0].addresses,
        vec!["0x0000000000000000000000000000000000000001".to_owned()]
    );
    assert_eq!(checkpoint_mode(&tasks[0].checkpoint), "cold_start");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_checkpoints")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_intake_chain_tasks_preserves_manifest_contract_implementation_addresses() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let contract_address = "0x00000000000000000000000000000000000000aa";
    let implementation_address = "0x00000000000000000000000000000000000000bb";
    let contract_contract_instance_id = Uuid::from_u128(21);
    let implementation_contract_instance_id = Uuid::from_u128(22);

    sqlx::query(
        r#"
            INSERT INTO manifest_versions (manifest_id, chain, rollout_status)
            VALUES (1, 'ethereum-mainnet', 'active')
            "#,
    )
    .execute(database.pool())
    .await
    .context("failed to insert manifest_versions for implementation watch-plan test")?;
    insert_contract_instance(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        contract_contract_instance_id,
        "ethereum-mainnet",
        contract_address,
        Some(1),
    )
    .await?;
    insert_contract_instance(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        "contract",
    )
    .await?;
    insert_active_contract_instance_address(
        database.pool(),
        implementation_contract_instance_id,
        "ethereum-mainnet",
        implementation_address,
        Some(1),
    )
    .await?;
    insert_manifest_contract_instance(
        database.pool(),
        1,
        "registry",
        contract_contract_instance_id,
        contract_address,
        "erc1967",
        Some(implementation_contract_instance_id),
        Some(implementation_address),
    )
    .await?;
    insert_active_discovery_edge(
        database.pool(),
        "ethereum-mainnet",
        "proxy_implementation",
        contract_contract_instance_id,
        implementation_contract_instance_id,
        Some(1),
    )
    .await?;

    let watched_plan = load_watched_chain_plan(database.pool()).await?;

    let tasks = sync_intake_chain_tasks(database.pool(), &watched_plan).await?;

    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].chain, "ethereum-mainnet");
    assert_eq!(
        tasks[0].addresses,
        vec![
            contract_address.to_owned(),
            implementation_address.to_owned()
        ]
    );
    assert_eq!(tasks[0].manifest_root_entry_count, 0);
    assert_eq!(tasks[0].manifest_contract_entry_count, 1);
    assert_eq!(tasks[0].discovery_edge_entry_count, 1);
    assert_eq!(checkpoint_mode(&tasks[0].checkpoint), "cold_start");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM chain_checkpoints")
            .fetch_one(database.pool())
            .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

