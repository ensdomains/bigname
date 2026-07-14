use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bigname_storage::{
    RawBlock, RawLog, default_database_url, load_name_surface, load_resource, load_surface_binding,
    load_surface_bindings_by_logical_name_id, load_token_lineage,
    mark_identity_rows_range_orphaned, upsert_raw_blocks, upsert_raw_logs,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::discovery::reconcile_discovery_observation_history_by_source;
use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for ENSv2 registry tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bn_ad_ensv2_reg_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for ENSv2 registry tests")?;
        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for ENSv2 registry tests")?;
        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for ENSv2 registry tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

#[tokio::test]
async fn ens_v2_expiry_reregistration_rotates_binding_in_one_sync_batch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    fixture
        .insert_registration(database.pool(), 10, 1, 101, "alice")
        .await?;
    fixture
        .insert_registration(database.pool(), 20, 2, 202, "bob")
        .await?;

    fixture.sync(database.pool(), &[10, 20]).await?;

    assert_reregistered_surface(database.pool(), 10).await?;
    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_expiry_reregistration_rotates_persisted_binding_in_later_sync() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    fixture
        .insert_registration(database.pool(), 10, 1, 101, "alice")
        .await?;
    fixture.sync(database.pool(), &[10]).await?;

    fixture
        .insert_registration(database.pool(), 20, 2, 202, "bob")
        .await?;
    fixture.sync(database.pool(), &[20]).await?;

    assert_reregistered_surface(database.pool(), 10).await?;
    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_reorg_reanchors_orphaned_stable_identity_rows_to_winning_block() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    let losing_hash = lifecycle_branch_block_hash(10, 1);
    let winning_hash = lifecycle_branch_block_hash(10, 2);

    fixture
        .insert_registration_at_hash(database.pool(), &losing_hash, 10, 1, 101, "alice")
        .await?;
    fixture
        .sync_hashes(database.pool(), &[losing_hash.clone()], 10, 10)
        .await?;

    let losing_binding =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:fleeting.eth")
            .await?
            .into_iter()
            .next()
            .context("losing registration should create a readable binding")?;
    let resource_id = losing_binding.resource_id;
    let token_lineage_id = load_resource(database.pool(), resource_id)
        .await?
        .context("losing registration should create a readable resource")?
        .token_lineage_id
        .context("ENSv2 registry resource should carry a token lineage")?;

    let orphaned =
        mark_identity_rows_range_orphaned(database.pool(), fixture.chain, &losing_hash, None)
            .await?;
    assert_eq!(orphaned.name_surface_count, 1);
    assert_eq!(orphaned.token_lineage_count, 1);
    assert_eq!(orphaned.resource_count, 1);
    assert_eq!(orphaned.surface_binding_count, 1);

    fixture
        .insert_registration_at_hash(database.pool(), &winning_hash, 10, 1, 101, "alice")
        .await?;
    fixture
        .sync_hashes(database.pool(), &[winning_hash.clone()], 10, 10)
        .await?;

    let winning_surface = load_name_surface(database.pool(), "ens:fleeting.eth")
        .await?
        .context("winning registration should restore the readable name surface")?;
    let winning_lineage = load_token_lineage(database.pool(), token_lineage_id)
        .await?
        .context("winning registration should restore the readable token lineage")?;
    let winning_resource = load_resource(database.pool(), resource_id)
        .await?
        .context("winning registration should restore the readable resource")?;
    let winning_binding = load_surface_binding(database.pool(), losing_binding.surface_binding_id)
        .await?
        .context("winning registration should restore the readable surface binding")?;

    for (row_kind, block_hash) in [
        ("name surface", winning_surface.block_hash.as_str()),
        ("token lineage", winning_lineage.block_hash.as_str()),
        ("resource", winning_resource.block_hash.as_str()),
        ("surface binding", winning_binding.block_hash.as_str()),
    ] {
        assert_eq!(
            block_hash, winning_hash,
            "{row_kind} must use the winning observation anchor instead of reviving the losing block"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_reorg_reopens_reanchored_binding_closed_only_on_losing_branch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    let losing_registration_hash = lifecycle_branch_block_hash(10, 1);
    let losing_unregister_hash = lifecycle_branch_block_hash(20, 1);
    let winning_registration_hash = lifecycle_branch_block_hash(10, 2);

    fixture
        .insert_registration_at_hash(
            database.pool(),
            &losing_registration_hash,
            10,
            1,
            101,
            "alice",
        )
        .await?;
    fixture
        .insert_unregister_at_hash(
            database.pool(),
            &losing_unregister_hash,
            Some(&losing_registration_hash),
            20,
            1,
        )
        .await?;
    fixture
        .sync_hashes(
            database.pool(),
            &[
                losing_registration_hash.clone(),
                losing_unregister_hash.clone(),
            ],
            10,
            20,
        )
        .await?;

    let losing_binding =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:fleeting.eth")
            .await?
            .into_iter()
            .next()
            .context("losing registration should create a readable binding")?;
    assert!(
        losing_binding.active_to.is_some(),
        "losing unregister should close its binding"
    );

    bigname_storage::mark_block_derived_normalized_events_range_orphaned(
        database.pool(),
        fixture.chain,
        &losing_unregister_hash,
        None,
    )
    .await?;
    let orphaned = mark_identity_rows_range_orphaned(
        database.pool(),
        fixture.chain,
        &losing_unregister_hash,
        None,
    )
    .await?;
    assert_eq!(orphaned.name_surface_count, 1);
    assert_eq!(orphaned.token_lineage_count, 1);
    assert_eq!(orphaned.resource_count, 1);
    assert_eq!(orphaned.surface_binding_count, 1);

    fixture
        .insert_registration_at_hash(
            database.pool(),
            &winning_registration_hash,
            10,
            1,
            101,
            "alice",
        )
        .await?;
    fixture
        .sync_hashes(
            database.pool(),
            &[winning_registration_hash.clone()],
            10,
            10,
        )
        .await?;

    let winning_binding = load_surface_binding(database.pool(), losing_binding.surface_binding_id)
        .await?
        .context("winning registration should restore the stable binding")?;
    assert_eq!(winning_binding.resource_id, losing_binding.resource_id);
    assert_eq!(winning_binding.active_from, losing_binding.active_from);
    assert_eq!(winning_binding.provenance, losing_binding.provenance);
    assert_eq!(winning_binding.block_hash, winning_registration_hash);
    assert_eq!(winning_binding.block_number, 10);
    assert_eq!(
        winning_binding.active_to, None,
        "reanchoring an orphaned stable binding must not retain a losing-branch unregister close"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_orphaned_successor_reopens_predecessor_binding() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    fixture
        .insert_registration(database.pool(), 10, 1, 101, "alice")
        .await?;
    fixture.sync(database.pool(), &[10]).await?;
    fixture
        .insert_registration(database.pool(), 20, 2, 202, "bob")
        .await?;
    fixture.sync(database.pool(), &[20]).await?;

    let before_reorg =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:fleeting.eth").await?;
    assert_eq!(before_reorg.len(), 2);
    assert_eq!(before_reorg[0].active_to, Some(before_reorg[1].active_from));

    let orphaned = bigname_storage::mark_identity_rows_range_orphaned(
        database.pool(),
        fixture.chain,
        &lifecycle_block_hash(20),
        None,
    )
    .await?;
    assert_eq!(orphaned.surface_binding_count, 1);

    let readable =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:fleeting.eth").await?;
    assert_eq!(readable.len(), 1);
    assert_eq!(readable[0].block_hash, lifecycle_block_hash(10));
    assert_eq!(
        readable[0].active_to, None,
        "orphaning the successor must undo its derived predecessor closure"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_orphaned_unregister_reopens_predecessor_binding() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    fixture
        .insert_registration(database.pool(), 10, 1, 101, "alice")
        .await?;
    fixture.sync(database.pool(), &[10]).await?;

    let mut predecessor =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:fleeting.eth")
            .await?
            .pop()
            .context("registered predecessor binding should exist")?;
    predecessor.active_to = Some(
        OffsetDateTime::from_unix_timestamp(1_717_172_720)
            .context("unregister timestamp should fit")?,
    );
    upsert_surface_bindings(database.pool(), std::slice::from_ref(&predecessor)).await?;

    let losing_hash = lifecycle_block_hash(20);
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(fixture.chain, &losing_hash, 20)],
    )
    .await?;
    upsert_normalized_events_with_summary(
        database.pool(),
        &[NormalizedEvent {
            event_identity: "ensv2-release-losing".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some("ens:fleeting.eth".to_owned()),
            resource_id: Some(predecessor.resource_id),
            event_kind: EVENT_KIND_REGISTRATION_RELEASED.to_owned(),
            source_family: SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: Some(fixture.chain.to_owned()),
            block_number: Some(20),
            block_hash: Some(losing_hash.clone()),
            transaction_hash: Some("0xrelease".to_owned()),
            log_index: Some(0),
            raw_fact_ref: json!({"kind": "raw_log", "block_hash": losing_hash}),
            derivation_kind: DERIVATION_KIND_ENS_V2_REGISTRY_RESOURCE_SURFACE.to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({"status": "registered"}),
            after_state: json!({"status": "unregistered"}),
        }],
    )
    .await?;
    bigname_storage::mark_block_derived_normalized_events_range_orphaned(
        database.pool(),
        fixture.chain,
        &losing_hash,
        None,
    )
    .await?;
    let orphaned = bigname_storage::mark_identity_rows_range_orphaned(
        database.pool(),
        fixture.chain,
        &losing_hash,
        None,
    )
    .await?;
    assert_eq!(orphaned.surface_binding_count, 0);

    let readable =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:fleeting.eth").await?;
    assert_eq!(readable.len(), 1);
    assert_eq!(
        readable[0].active_to, None,
        "orphaning RegistrationReleased must undo its derived predecessor closure"
    );

    database.cleanup().await
}

async fn assert_reregistered_surface(pool: &PgPool, stable_anchor_block: i64) -> Result<()> {
    let surface_anchor: i64 =
        sqlx::query_scalar("SELECT block_number FROM name_surfaces WHERE logical_name_id = $1")
            .bind("ens:fleeting.eth")
            .fetch_one(pool)
            .await
            .context("re-registered name surface should exist")?;
    assert_eq!(surface_anchor, stable_anchor_block);

    let bindings = load_surface_bindings_by_logical_name_id(pool, "ens:fleeting.eth").await?;
    assert_eq!(bindings.len(), 2, "both lifecycle bindings should remain");
    assert_ne!(bindings[0].resource_id, bindings[1].resource_id);
    assert_eq!(
        bindings[0].active_to,
        Some(bindings[1].active_from),
        "the successor resource should close the prior binding"
    );
    assert_eq!(bindings[1].active_to, None);
    Ok(())
}

struct RegistryLifecycleFixture {
    chain: &'static str,
    address: &'static str,
    contract_instance_id: Uuid,
}

impl RegistryLifecycleFixture {
    async fn insert(pool: &PgPool) -> Result<Self> {
        let fixture = Self {
            chain: "ethereum-sepolia",
            address: "0x00000000000000000000000000000000000000a1",
            contract_instance_id: Uuid::from_u128(0x12f1),
        };
        let manifest_id = insert_test_registry_manifest(pool, fixture.chain).await?;
        insert_test_registry_contract(
            pool,
            manifest_id,
            "registry",
            fixture.contract_instance_id,
            fixture.address,
            0,
        )
        .await?;
        Ok(fixture)
    }

    async fn insert_registration(
        &self,
        pool: &PgPool,
        block_number: i64,
        token_id: u64,
        resource_id: u64,
        owner_label: &str,
    ) -> Result<()> {
        let block_hash = lifecycle_block_hash(block_number);
        self.insert_registration_at_hash(
            pool,
            &block_hash,
            block_number,
            token_id,
            resource_id,
            owner_label,
        )
        .await
    }

    async fn insert_registration_at_hash(
        &self,
        pool: &PgPool,
        block_hash: &str,
        block_number: i64,
        token_id: u64,
        resource_id: u64,
        owner_label: &str,
    ) -> Result<()> {
        upsert_raw_blocks(
            pool,
            &[test_raw_block(self.chain, block_hash, block_number)],
        )
        .await?;
        let mut logs = [
            label_registered_raw_log(
                self.chain,
                block_hash,
                block_number,
                self.address,
                0,
                "fleeting",
                token_id,
                owner_label,
            ),
            token_resource_raw_log(
                self.chain,
                block_hash,
                block_number,
                self.address,
                1,
                token_id,
                resource_id,
            ),
        ];
        for log in &mut logs {
            log.transaction_hash = format!(
                "0xregistration{block_number}{}",
                &block_hash[block_hash.len() - 8..]
            );
        }
        upsert_raw_logs(pool, &logs).await?;
        Ok(())
    }

    async fn insert_unregister_at_hash(
        &self,
        pool: &PgPool,
        block_hash: &str,
        parent_hash: Option<&str>,
        block_number: i64,
        token_id: u64,
    ) -> Result<()> {
        let mut block = test_raw_block(self.chain, block_hash, block_number);
        block.parent_hash = parent_hash.map(str::to_owned);
        upsert_raw_blocks(pool, &[block]).await?;
        let mut log = label_unregistered_raw_log(
            self.chain,
            block_hash,
            block_number,
            self.address,
            0,
            token_id,
        );
        log.transaction_hash = format!(
            "0xunregister{block_number}{}",
            &block_hash[block_hash.len() - 8..]
        );
        upsert_raw_logs(pool, &[log]).await?;
        Ok(())
    }

    async fn sync(&self, pool: &PgPool, block_numbers: &[i64]) -> Result<()> {
        let block_hashes = block_numbers
            .iter()
            .map(|block_number| lifecycle_block_hash(*block_number))
            .collect::<Vec<_>>();
        let start = *block_numbers
            .iter()
            .min()
            .context("sync needs a start block")?;
        let end = *block_numbers
            .iter()
            .max()
            .context("sync needs an end block")?;
        self.sync_hashes(pool, &block_hashes, start, end).await
    }

    async fn sync_hashes(
        &self,
        pool: &PgPool,
        block_hashes: &[String],
        start: i64,
        end: i64,
    ) -> Result<()> {
        EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
            pool,
            self.chain,
            block_hashes,
            &[(
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                self.address.to_owned(),
                start,
                end,
            )],
        )
        .await?;
        Ok(())
    }
}

#[tokio::test]
async fn ens_v2_scoped_backfill_sync_only_normalizes_selected_registry_targets() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    let selected_contract_instance_id = Uuid::from_u128(0x1201);
    let unselected_contract_instance_id = Uuid::from_u128(0x1202);
    let selected_address = "0x00000000000000000000000000000000000000a1";
    let unselected_address = "0x00000000000000000000000000000000000000b2";
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry_selected",
        selected_contract_instance_id,
        selected_address,
        0,
    )
    .await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry_unselected",
        unselected_contract_instance_id,
        unselected_address,
        0,
    )
    .await?;
    upsert_raw_blocks(database.pool(), &[test_raw_block(chain, block_hash, 42)]).await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_reserved_raw_log(chain, block_hash, 42, selected_address, 0, "alice"),
            label_reserved_raw_log(chain, block_hash, 42, unselected_address, 1, "bob"),
        ],
    )
    .await?;

    let wrong_family_summary =
        EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
            database.pool(),
            chain,
            &[block_hash.to_owned()],
            &[(
                "ens_v2_registrar_l1".to_owned(),
                selected_address.to_owned(),
                42,
                42,
            )],
        )
        .await?;
    assert_eq!(wrong_family_summary.scanned_log_count, 0);
    assert_eq!(wrong_family_summary.total_normalized_event_count, 0);
    assert_eq!(normalized_event_count(database.pool()).await?, 0);

    let summary = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        chain,
        &[block_hash.to_owned()],
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            selected_address.to_owned(),
            42,
            42,
        )],
    )
    .await?;

    assert_eq!(summary.scanned_log_count, 1);
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.total_normalized_event_count, 1);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_REGISTRATION_RESERVED),
        Some(&1)
    );
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), selected_address).await?,
        1
    );
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), unselected_address).await?,
        0
    );
    assert_eq!(normalized_event_count(database.pool()).await?, 1);

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_scoped_loader_preserves_same_address_disjoint_effective_ranges() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let address = "0x00000000000000000000000000000000000000c1";
    let first_block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let second_block_hash = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let first_contract_instance_id = Uuid::from_u128(0x1301);
    let second_contract_instance_id = Uuid::from_u128(0x1302);

    upsert_raw_blocks(
        database.pool(),
        &[
            test_raw_block(chain, first_block_hash, 40),
            test_raw_block(chain, second_block_hash, 42),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_reserved_raw_log(chain, first_block_hash, 40, address, 0, "alice"),
            label_reserved_raw_log(chain, second_block_hash, 42, address, 0, "bob"),
        ],
    )
    .await?;

    let emitters = vec![
        test_active_emitter(address, first_contract_instance_id, 1, Some(40), Some(40)),
        test_active_emitter(address, second_contract_instance_id, 2, Some(42), Some(42)),
    ];
    let source_scope = vec![
        RegistryRawLogSourceScopeTarget {
            source_family: SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            address: normalize_address(address),
            effective_from_block: 40,
            effective_to_block: 40,
        },
        RegistryRawLogSourceScopeTarget {
            source_family: SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            address: normalize_address(address),
            effective_from_block: 42,
            effective_to_block: 42,
        },
    ];
    let rows = load_registry_raw_logs(
        database.pool(),
        chain,
        &emitters,
        true,
        &[first_block_hash.to_owned(), second_block_hash.to_owned()],
        Some(&source_scope),
        RawLogCanonicalityFilter::IncludeObserved,
        None,
    )
    .await?;

    assert_eq!(
        rows.iter()
            .map(|row| (row.block_number, row.emitting_contract_instance_id))
            .collect::<Vec<_>>(),
        vec![
            (40, first_contract_instance_id),
            (42, second_contract_instance_id)
        ],
        "same-address scoped registry targets must remain range-attributed"
    );

    let narrowed_rows = load_registry_raw_logs(
        database.pool(),
        chain,
        &emitters,
        true,
        &[first_block_hash.to_owned(), second_block_hash.to_owned()],
        Some(&source_scope[1..]),
        RawLogCanonicalityFilter::IncludeObserved,
        None,
    )
    .await?;
    assert_eq!(
        narrowed_rows
            .iter()
            .map(|row| (row.block_number, row.emitting_contract_instance_id))
            .collect::<Vec<_>>(),
        vec![(42, second_contract_instance_id)]
    );
    let target_bounded_rows = load_registry_raw_logs(
        database.pool(),
        chain,
        &emitters,
        true,
        &[first_block_hash.to_owned(), second_block_hash.to_owned()],
        Some(&source_scope),
        RawLogCanonicalityFilter::IncludeObserved,
        Some(40),
    )
    .await?;
    assert_eq!(
        target_bounded_rows
            .iter()
            .map(|row| (row.block_number, row.emitting_contract_instance_id))
            .collect::<Vec<_>>(),
        vec![(40, first_contract_instance_id)]
    );

    database.cleanup().await
}

#[test]
fn ens_v2_active_emitter_selection_preserves_same_address_disjoint_ranges() {
    let address = "0x00000000000000000000000000000000000000c1";
    let first_contract_instance_id = Uuid::from_u128(0x1401);
    let second_contract_instance_id = Uuid::from_u128(0x1402);

    let emitters = preferred_emitters_by_scope(vec![
        test_active_emitter(address, first_contract_instance_id, 1, Some(40), Some(40)),
        test_active_emitter(address, second_contract_instance_id, 2, Some(42), Some(42)),
    ]);

    assert_eq!(
        emitters
            .iter()
            .map(|emitter| {
                (
                    emitter.address.clone(),
                    emitter.active_from_block_number,
                    emitter.active_to_block_number,
                    emitter.contract_instance_id,
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                normalize_address(address),
                Some(40),
                Some(40),
                first_contract_instance_id,
            ),
            (
                normalize_address(address),
                Some(42),
                Some(42),
                second_contract_instance_id,
            )
        ]
    );
}

#[test]
fn ens_v2_token_regeneration_preserves_resource_identity() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let old_token_id =
        "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
    let new_token_id =
        "0x00000000000000000000000000000000000000000000000000000000000000a2".to_owned();
    let upstream_resource =
        "0x0000000000000000000000000000000000000000000000000000000000000eac".to_owned();

    let mut registry_suffix_by_address =
        HashMap::from([(registry.clone(), "alice.eth".to_owned())]);
    let mut registry_contract_by_address =
        HashMap::from([(registry.clone(), contract_instance_id)]);
    let mut states_by_registry_token = BTreeMap::new();
    let mut linked_resource_states = BTreeMap::new();
    let mut closed_bindings = BTreeMap::new();
    let mut token_aliases = HashMap::new();
    let mut observations = Vec::new();
    let mut graph_events = Vec::new();

    {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            registry_contract_by_address: &mut registry_contract_by_address,
            states_by_registry_token: &mut states_by_registry_token,
            linked_resource_states: &mut linked_resource_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            observations: &mut observations,
            graph_events: &mut graph_events,
        };
        apply_registry_observation(
            RegistryObservation::LabelRegistered {
                token_id: old_token_id.clone(),
                labelhash: "0x0000000000000000000000000000000000000000000000000000000000000b0b"
                    .to_owned(),
                label: "bob".to_owned(),
                owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
                expiry: 1_900_000_000,
                sender: "0x0000000000000000000000000000000000000dad".to_owned(),
                reference: reference(&registry, contract_instance_id, 10, 0),
            },
            &mut context,
        )?;
    }
    {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            registry_contract_by_address: &mut registry_contract_by_address,
            states_by_registry_token: &mut states_by_registry_token,
            linked_resource_states: &mut linked_resource_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            observations: &mut observations,
            graph_events: &mut graph_events,
        };
        apply_registry_observation(
            RegistryObservation::TokenResource {
                token_id: old_token_id.clone(),
                upstream_resource: upstream_resource.clone(),
                reference: reference(&registry, contract_instance_id, 10, 1),
            },
            &mut context,
        )?;
    }
    let initial_linked_event = linked_resource_states
        .values()
        .next()
        .and_then(|state| {
            state.resource.as_ref().and_then(|link| {
                build_resource_events(state, link)
                    .into_iter()
                    .find(|event| event.event_kind == EVENT_KIND_TOKEN_RESOURCE_LINKED)
            })
        })
        .context("initial TokenResourceLinked event should be emitted")?;
    let initial_expiry_event = linked_resource_states
        .values()
        .next()
        .and_then(|state| {
            state.resource.as_ref().and_then(|link| {
                build_resource_events(state, link)
                    .into_iter()
                    .find(|event| event.event_kind == EVENT_KIND_EXPIRY_CHANGED)
            })
        })
        .context("initial synthetic ExpiryChanged event should be emitted")?;
    {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            registry_contract_by_address: &mut registry_contract_by_address,
            states_by_registry_token: &mut states_by_registry_token,
            linked_resource_states: &mut linked_resource_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            observations: &mut observations,
            graph_events: &mut graph_events,
        };
        apply_registry_observation(
            RegistryObservation::TokenRegenerated {
                old_token_id: old_token_id.clone(),
                new_token_id: new_token_id.clone(),
                reference: reference(&registry, contract_instance_id, 11, 0),
            },
            &mut context,
        )?;
    }

    let state = states_by_registry_token
        .get(&(registry.clone(), old_token_id.clone()))
        .context("state should remain keyed by the original token observation")?;
    let link = state
        .resource
        .as_ref()
        .cloned()
        .context("TokenResource should link a stable EAC resource")?;
    assert_eq!(state.token_id, new_token_id);
    assert_eq!(
        link.resource_id,
        deterministic_uuid(&format!(
            "ens-v2-resource:{}:{}:{}",
            "ethereum-sepolia", contract_instance_id, upstream_resource
        ))
    );
    assert!(graph_events.iter().any(|event| {
        event.event_kind == EVENT_KIND_TOKEN_REGENERATED
            && event.resource_id == Some(link.resource_id)
            && event.after_state["new_token_id"] == Value::String(new_token_id.clone())
    }));
    let linked_state = linked_resource_states
        .get(&link.resource_id)
        .context("linked resource state should track regenerated token")?;
    let linked_event = build_resource_events(
        linked_state,
        linked_state
            .resource
            .as_ref()
            .context("linked state should keep resource")?,
    )
    .into_iter()
    .find(|event| event.event_kind == EVENT_KIND_TOKEN_RESOURCE_LINKED)
    .context("TokenResourceLinked event should be emitted")?;
    assert_eq!(
        linked_event.after_state["token_id"],
        Value::String(old_token_id.clone())
    );
    assert_eq!(
        linked_event.after_state["current_token_id"],
        Value::String(old_token_id.clone())
    );
    assert_eq!(
        linked_event, initial_linked_event,
        "token regeneration must not rewrite the historical resource-link event"
    );
    let replayed_expiry_event = build_resource_events(linked_state, &link)
        .into_iter()
        .find(|event| event.event_kind == EVENT_KIND_EXPIRY_CHANGED)
        .context("replayed synthetic ExpiryChanged event should remain present")?;
    assert_eq!(
        replayed_expiry_event, initial_expiry_event,
        "unrelated token mutations must not mint synthetic expiry history at poll boundaries"
    );
    {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            registry_contract_by_address: &mut registry_contract_by_address,
            states_by_registry_token: &mut states_by_registry_token,
            linked_resource_states: &mut linked_resource_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            observations: &mut observations,
            graph_events: &mut graph_events,
        };
        apply_registry_observation(
            RegistryObservation::ExpiryUpdated {
                token_id: new_token_id.clone(),
                new_expiry: 2_000_000_000,
                sender: "0x0000000000000000000000000000000000000dad".to_owned(),
                reference: reference(&registry, contract_instance_id, 12, 0),
            },
            &mut context,
        )?;
    }
    let renewed_state = linked_resource_states
        .get(&link.resource_id)
        .context("renewed linked resource state should remain present")?;
    let synthetic_after_renewal = build_resource_events(renewed_state, &link)
        .into_iter()
        .find(|event| event.event_kind == EVENT_KIND_EXPIRY_CHANGED)
        .context("synthetic ExpiryChanged should remain present after renewal")?;
    assert_eq!(
        synthetic_after_renewal, initial_expiry_event,
        "real expiry updates must not rewrite the link-time expiry fact"
    );
    assert!(graph_events.iter().any(|event| {
        event.event_kind == EVENT_KIND_EXPIRY_CHANGED
            && event.block_number == Some(12)
            && event.after_state["expiry"] == 2_000_000_000_u64
    }));
    {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            registry_contract_by_address: &mut registry_contract_by_address,
            states_by_registry_token: &mut states_by_registry_token,
            linked_resource_states: &mut linked_resource_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            observations: &mut observations,
            graph_events: &mut graph_events,
        };
        apply_registry_observation(
            RegistryObservation::LabelUnregistered {
                token_id: new_token_id,
                sender: "0x0000000000000000000000000000000000000dad".to_owned(),
                reference: reference(&registry, contract_instance_id, 13, 0),
            },
            &mut context,
        )?;
    }
    let closed = closed_bindings
        .get(&link.surface_binding_id)
        .context("regenerated token unregister should close its stable binding")?;
    assert_eq!(
        closed.provenance["current_token_id"],
        Value::String(old_token_id)
    );

    Ok(())
}

#[test]
fn ens_v2_lifecycle_events_include_registry_contract_instance_id() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let token = "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
    let upstream_resource =
        "0x0000000000000000000000000000000000000000000000000000000000000ea1".to_owned();
    let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;

    let expected_registry_id = Value::String(contract_instance_id.to_string());
    let pending_grant = harness
        .graph_events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_GRANTED)
        .context("LabelRegistered should emit RegistrationGranted")?;
    assert_eq!(
        pending_grant.after_state["registry_contract_instance_id"],
        expected_registry_id
    );

    harness.apply(RegistryObservation::TokenResource {
        token_id: token.clone(),
        upstream_resource: upstream_resource.clone(),
        reference: reference(&registry, contract_instance_id, 10, 1),
    })?;
    let resource_id = deterministic_uuid(&format!(
        "ens-v2-resource:{}:{}:{}",
        "ethereum-sepolia", contract_instance_id, upstream_resource
    ));
    let linked_state = harness
        .linked_resource_states
        .get(&resource_id)
        .context("TokenResource should link a resource")?;
    let link = linked_state
        .resource
        .as_ref()
        .context("linked state should keep resource")?;
    let resource_grant = build_resource_events(linked_state, link)
        .into_iter()
        .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_GRANTED)
        .context("resource-linked state should emit RegistrationGranted")?;
    assert_eq!(
        resource_grant.after_state["registry_contract_instance_id"],
        expected_registry_id
    );

    harness.apply(RegistryObservation::ExpiryUpdated {
        token_id: token.clone(),
        new_expiry: 2_000_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 11, 0),
    })?;
    let renewal = harness
        .graph_events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_RENEWED)
        .context("ExpiryUpdated should emit RegistrationRenewed")?;
    assert_eq!(
        renewal.after_state["registry_contract_instance_id"],
        expected_registry_id
    );

    harness.apply(RegistryObservation::LabelUnregistered {
        token_id: token,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 12, 0),
    })?;
    let release = harness
        .graph_events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_RELEASED)
        .context("LabelUnregistered should emit RegistrationReleased")?;
    assert_eq!(
        release.after_state["registry_contract_instance_id"],
        expected_registry_id
    );

    Ok(())
}

#[test]
fn ens_v2_unregister_closes_binding_before_reregistering_new_resource() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let first_token =
        "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
    let second_token =
        "0x00000000000000000000000000000000000000000000000000000000000000a2".to_owned();
    let first_resource =
        "0x0000000000000000000000000000000000000000000000000000000000000ea1".to_owned();
    let second_resource =
        "0x0000000000000000000000000000000000000000000000000000000000000ea2".to_owned();
    let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: first_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: first_token.clone(),
        upstream_resource: first_resource.clone(),
        reference: reference(&registry, contract_instance_id, 10, 1),
    })?;
    harness.apply(RegistryObservation::LabelUnregistered {
        token_id: first_token.clone(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 11, 0),
    })?;
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: second_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a22".to_owned(),
        expiry: 2_000_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 12, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: second_token.clone(),
        upstream_resource: second_resource.clone(),
        reference: reference(&registry, contract_instance_id, 12, 1),
    })?;

    let first_resource_id = deterministic_uuid(&format!(
        "ens-v2-resource:{}:{}:{}",
        "ethereum-sepolia", contract_instance_id, first_resource
    ));
    let second_resource_id = deterministic_uuid(&format!(
        "ens-v2-resource:{}:{}:{}",
        "ethereum-sepolia", contract_instance_id, second_resource
    ));
    assert!(
        harness
            .linked_resource_states
            .contains_key(&first_resource_id)
    );
    assert!(
        harness
            .linked_resource_states
            .contains_key(&second_resource_id)
    );
    let closed_binding = harness
        .closed_bindings
        .values()
        .find(|binding| binding.resource_id == first_resource_id)
        .context("unregister should close the first resource binding")?;
    assert_eq!(closed_binding.logical_name_id, "ens:alice.eth".to_owned());
    assert_eq!(
        closed_binding.active_to,
        Some(
            OffsetDateTime::from_unix_timestamp(1_717_172_711).expect("test timestamp should fit")
        )
    );
    let second_link = harness
        .linked_resource_states
        .get(&second_resource_id)
        .and_then(|state| state.resource.as_ref())
        .context("second registration should have a resource link")?;
    assert!(closed_binding.active_to.is_some_and(
        |active_to| active_to <= event_position_timestamp(&second_link.linked_ref)
    ));
    assert_ne!(first_resource_id, second_resource_id);

    Ok(())
}

#[test]
fn ens_v2_unregister_closure_orders_equal_block_timestamps_after_binding_start() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let token = "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
    let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: token.clone(),
        upstream_resource: "0x0000000000000000000000000000000000000000000000000000000000000ea1"
            .to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 4),
    })?;
    let link = harness
        .linked_resource_states
        .values()
        .next()
        .and_then(|state| state.resource.as_ref())
        .cloned()
        .context("registered resource link should exist")?;
    let mut unregister_ref = reference(&registry, contract_instance_id, 11, 0);
    unregister_ref.block_timestamp = link.linked_ref.block_timestamp;
    harness.apply(RegistryObservation::LabelUnregistered {
        token_id: token,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: unregister_ref,
    })?;

    let closed = harness
        .closed_bindings
        .get(&link.surface_binding_id)
        .context("unregister should close the binding")?;
    assert!(
        closed
            .active_to
            .is_some_and(|active_to| active_to > closed.active_from),
        "next-block unregister must sort after the binding even when block timestamps are equal: {closed:?}"
    );
    Ok(())
}

#[tokio::test]
async fn ens_v2_unregister_reregister_upserts_close_before_open_successor() -> Result<()> {
    let database = TestDatabase::new().await?;
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let first_token =
        "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
    let second_token =
        "0x00000000000000000000000000000000000000000000000000000000000000a2".to_owned();
    let first_resource =
        "0x0000000000000000000000000000000000000000000000000000000000000ea1".to_owned();
    let second_resource =
        "0x0000000000000000000000000000000000000000000000000000000000000ea2".to_owned();
    let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: first_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: first_token.clone(),
        upstream_resource: first_resource.clone(),
        reference: reference(&registry, contract_instance_id, 10, 1),
    })?;

    let first_resource_id = deterministic_uuid(&format!(
        "ens-v2-resource:{}:{}:{}",
        "ethereum-sepolia", contract_instance_id, first_resource
    ));
    let first_state = harness
        .linked_resource_states
        .get(&first_resource_id)
        .cloned()
        .context("first resource state should be linked")?;
    let first_link = first_state
        .resource
        .as_ref()
        .cloned()
        .context("first state should hold resource link")?;
    upsert_token_lineages(
        database.pool(),
        &[build_token_lineage(database.pool(), &first_state, &first_link).await?],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[build_resource(database.pool(), &first_state, &first_link).await?],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[build_name_surface(database.pool(), &first_state.name, &first_state.first_ref).await?],
    )
    .await?;
    let old_open_binding = build_surface_binding(database.pool(), &first_state, &first_link)
        .await
        .context("first open binding should build")?;
    upsert_surface_bindings(database.pool(), &[old_open_binding])
        .await
        .context("old open binding should persist")?;

    harness.apply(RegistryObservation::LabelUnregistered {
        token_id: first_token.clone(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 11, 0),
    })?;
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: second_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a22".to_owned(),
        expiry: 2_000_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 12, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: second_token.clone(),
        upstream_resource: second_resource.clone(),
        reference: reference(&registry, contract_instance_id, 12, 1),
    })?;

    let second_resource_id = deterministic_uuid(&format!(
        "ens-v2-resource:{}:{}:{}",
        "ethereum-sepolia", contract_instance_id, second_resource
    ));
    let second_state = harness
        .linked_resource_states
        .get(&second_resource_id)
        .cloned()
        .context("second resource state should be linked")?;
    let second_link = second_state
        .resource
        .as_ref()
        .cloned()
        .context("second state should hold resource link")?;
    upsert_token_lineages(
        database.pool(),
        &[build_token_lineage(database.pool(), &second_state, &second_link).await?],
    )
    .await?;
    upsert_resources(
        database.pool(),
        &[build_resource(database.pool(), &second_state, &second_link).await?],
    )
    .await?;
    upsert_name_surfaces(
        database.pool(),
        &[
            build_name_surface(database.pool(), &second_state.name, &second_state.first_ref)
                .await?,
        ],
    )
    .await?;

    let closed_old_binding = harness
        .closed_bindings
        .get(&first_link.surface_binding_id)
        .cloned()
        .context("unregister should close old binding")?;
    let new_open_binding = build_surface_binding(database.pool(), &second_state, &second_link)
        .await
        .context("second open binding should build")?;
    upsert_surface_bindings_close_before_open(
        database.pool(),
        &[new_open_binding.clone(), closed_old_binding.clone()],
    )
    .await
    .context("ordered lifecycle binding upsert should close old before opening successor")?;

    let stored = load_surface_bindings_by_logical_name_id(database.pool(), "ens:alice.eth")
        .await
        .context("stored bindings should load")?;
    assert_eq!(stored.len(), 2);
    let old = stored
        .iter()
        .find(|binding| binding.resource_id == first_resource_id)
        .context("old binding should remain stored")?;
    let new = stored
        .iter()
        .find(|binding| binding.resource_id == second_resource_id)
        .context("new binding should be stored")?;
    assert!(old.active_to.is_some());
    assert!(new.active_to.is_none());
    assert!(
        old.active_to
            .is_some_and(|active_to| active_to <= new.active_from)
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_subregistry_change_retains_historical_endpoint_id_after_replacement() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let child = "0x00000000000000000000000000000000000000c1".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let token = "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
    let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token,
        subregistry: child.clone(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 11, 0),
    })?;

    let event = harness
        .graph_events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
        .context("SubregistryChanged should be emitted")?;
    assert_eq!(event.after_state["to_contract_instance_id"], Value::Null);
    assert!(
        !harness
            .registry_contract_by_address
            .contains_key("0x00000000000000000000000000000000000000c1")
    );

    let replay_source = harness.graph_events.clone();
    hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events).await?;
    assert_eq!(
        harness
            .graph_events
            .iter()
            .find(|event| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
            .context("unadmitted SubregistryChanged should remain present")?
            .after_state["to_contract_instance_id"],
        Value::Null
    );

    let target_id = Uuid::from_u128(0xc1);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind
        )
        VALUES ($1, 'ethereum-sepolia', 'contract'),
               ($2, 'ethereum-sepolia', 'contract')
        "#,
    )
    .bind(contract_instance_id)
    .bind(target_id)
    .execute(database.pool())
    .await
    .context("failed to insert initial subregistry contract instances")?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            active_from_block_hash
        )
        VALUES ($1, 'ethereum-sepolia', $2, 11, '0xblock11')
        "#,
    )
    .bind(target_id)
    .bind(&child)
    .execute(database.pool())
    .await
    .context("failed to admit the initial subregistry address")?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission,
            active_from_block_number,
            active_from_block_hash,
            provenance
        )
        VALUES (
            'ethereum-sepolia',
            'subregistry',
            $1,
            $2,
            'ens_v2_registry_l1:test',
            'admitted',
            11,
            '0xblock11',
            jsonb_build_object('to_address', $3::text)
        )
        "#,
    )
    .bind(contract_instance_id)
    .bind(target_id)
    .bind(&child)
    .execute(database.pool())
    .await
    .context("failed to insert the initial subregistry discovery edge")?;

    hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events).await?;
    let hydrated = harness
        .graph_events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
        .context("hydrated SubregistryChanged should remain present")?
        .clone();
    assert_eq!(
        hydrated.after_state["to_contract_instance_id"],
        target_id.to_string()
    );

    let replacement_id = Uuid::from_u128(0xc2);
    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET deactivated_at = GREATEST(admitted_at, now()),
            active_to_block_number = 20,
            active_to_block_hash = '0xblock20'
        WHERE to_contract_instance_id = $1
        "#,
    )
    .bind(target_id)
    .execute(database.pool())
    .await
    .context("failed to deactivate the initial subregistry edge")?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET deactivated_at = GREATEST(admitted_at, now()),
            active_to_block_number = 20,
            active_to_block_hash = '0xblock20'
        WHERE contract_instance_id = $1
        "#,
    )
    .bind(target_id)
    .execute(database.pool())
    .await
    .context("failed to deactivate the initial subregistry address")?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind
        )
        VALUES ($1, 'ethereum-sepolia', 'contract')
        "#,
    )
    .bind(replacement_id)
    .execute(database.pool())
    .await
    .context("failed to insert the replacement subregistry contract instance")?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            active_from_block_hash
        )
        VALUES ($1, 'ethereum-sepolia', $2, 20, '0xblock20')
        "#,
    )
    .bind(replacement_id)
    .bind(&child)
    .execute(database.pool())
    .await
    .context("failed to admit the replacement subregistry address")?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            admission,
            active_from_block_number,
            active_from_block_hash,
            provenance
        )
        VALUES (
            'ethereum-sepolia',
            'subregistry',
            $1,
            $2,
            'ens_v2_registry_l1:test',
            'admitted',
            20,
            '0xblock20',
            jsonb_build_object('to_address', $3::text)
        )
        "#,
    )
    .bind(contract_instance_id)
    .bind(replacement_id)
    .bind(&child)
    .execute(database.pool())
    .await
    .context("failed to insert the replacement subregistry discovery edge")?;

    let mut replayed = replay_source;
    hydrate_subregistry_event_target_ids(database.pool(), &mut replayed).await?;
    assert_eq!(
        replayed
            .iter()
            .find(|event| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED),
        Some(&hydrated),
        "later endpoint replacement must not rewrite the historical event target"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_full_history_replay_retains_each_subregistry_transition() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let child_a = "0x00000000000000000000000000000000000000c1".to_owned();
    let child_b = "0x00000000000000000000000000000000000000c2".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let token = "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        contract_instance_id,
        &registry,
        0,
    )
    .await?;

    let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token.clone(),
        subregistry: child_a,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 11, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token.clone(),
        subregistry: child_b,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 12, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token,
        subregistry: ZERO_ADDRESS.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 13, 0),
    })?;

    let replay_source = harness.graph_events.clone();
    let reconciliation = reconcile_discovery_observation_history_by_source(
        database.pool(),
        &harness.observations,
        true,
    )
    .await?;
    assert_eq!(reconciliation.inserted_edge_count, 2);
    assert_eq!(reconciliation.deactivated_edge_count, 2);

    hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events).await?;
    let changed = harness
        .graph_events
        .iter()
        .filter(|event| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
        .collect::<Vec<_>>();
    assert_eq!(changed.len(), 3);
    let first_target = changed[0].after_state["to_contract_instance_id"]
        .as_str()
        .context("first historical subregistry target should be admitted")?;
    let second_target = changed[1].after_state["to_contract_instance_id"]
        .as_str()
        .context("second historical subregistry target should be admitted")?;
    assert_ne!(first_target, second_target);
    assert_eq!(
        changed[2].after_state["to_contract_instance_id"],
        Value::Null
    );

    let edge_intervals = sqlx::query_as::<_, (i64, Option<i64>, bool)>(
        r#"
        SELECT
            active_from_block_number,
            active_to_block_number,
            deactivated_at IS NOT NULL
        FROM discovery_edges
        WHERE discovery_source = $1
        ORDER BY active_from_block_number
        "#,
    )
    .bind(format!("ens_v2_registry_subregistry:{chain}"))
    .fetch_all(database.pool())
    .await
    .context("failed to load historical subregistry edge intervals")?;
    assert_eq!(
        edge_intervals,
        vec![(11, Some(12), true), (12, Some(13), true)]
    );

    let replay_reconciliation = reconcile_discovery_observation_history_by_source(
        database.pool(),
        &harness.observations,
        true,
    )
    .await?;
    assert_eq!(replay_reconciliation.inserted_edge_count, 0);
    let edge_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM discovery_edges WHERE discovery_source = $1")
            .bind(format!("ens_v2_registry_subregistry:{chain}"))
            .fetch_one(database.pool())
            .await?;
    assert_eq!(
        edge_count, 2,
        "history replay must not duplicate edge epochs"
    );

    let mut replayed = replay_source;
    hydrate_subregistry_event_target_ids(database.pool(), &mut replayed).await?;
    assert_eq!(replayed, harness.graph_events);

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_full_closure_removes_losing_only_discovery_watch_target() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let losing_child = "0x00000000000000000000000000000000000000c1";
    let registry_id = Uuid::from_u128(0x1731);
    let block_10_hash = lifecycle_branch_block_hash(10, 0);
    let losing_11_hash = lifecycle_branch_block_hash(11, 1);
    let winning_11_hash = lifecycle_branch_block_hash(11, 2);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        registry_id,
        registry,
        0,
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[
            test_raw_block(chain, &block_10_hash, 10),
            test_raw_block(chain, &losing_11_hash, 11),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(chain, &block_10_hash, 10, registry, 0, "parent", 1, "alice"),
            subregistry_updated_raw_log(chain, &losing_11_hash, 11, registry, 0, 1, losing_child),
        ],
    )
    .await?;
    EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        chain,
        &[block_10_hash.clone(), losing_11_hash.clone()],
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            registry.to_owned(),
            10,
            11,
        )],
    )
    .await?;
    assert!(
        bigname_manifests::load_watched_contracts(database.pool())
            .await?
            .iter()
            .any(|contract| contract.address == normalize_address(losing_child))
    );

    bigname_storage::mark_raw_block_facts_range_orphaned(
        database.pool(),
        chain,
        &losing_11_hash,
        None,
    )
    .await?;
    bigname_storage::mark_raw_block_range_orphaned(database.pool(), chain, &losing_11_hash, None)
        .await?;
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(chain, &winning_11_hash, 11)],
    )
    .await?;

    sync_ens_v2_registry_resource_surface_through_block(database.pool(), chain, 11).await?;
    assert!(
        bigname_manifests::load_watched_contracts(database.pool())
            .await?
            .iter()
            .all(|contract| contract.address != normalize_address(losing_child)),
        "full canonical closure must deactivate a discovery edge absent from the winning history"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_full_closure_rebuilds_retired_registry_lifecycle_output() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let retired_registry = "0x00000000000000000000000000000000000000a1";
    let current_registry = "0x00000000000000000000000000000000000000b2";
    let registry_id = Uuid::from_u128(0x1741);
    let block_hashes = (10..=13)
        .map(|block_number| lifecycle_block_hash(block_number))
        .collect::<Vec<_>>();
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        registry_id,
        registry,
        0,
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &(10..=13)
            .map(|block_number| {
                test_raw_block(chain, &lifecycle_block_hash(block_number), block_number)
            })
            .collect::<Vec<_>>(),
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(
                chain,
                &block_hashes[0],
                10,
                registry,
                0,
                "parent",
                1,
                "alice",
            ),
            subregistry_updated_raw_log(
                chain,
                &block_hashes[1],
                11,
                registry,
                0,
                1,
                retired_registry,
            ),
            label_reserved_raw_log(chain, &block_hashes[2], 12, retired_registry, 0, "child"),
            subregistry_updated_raw_log(
                chain,
                &block_hashes[3],
                13,
                registry,
                0,
                1,
                current_registry,
            ),
        ],
    )
    .await?;

    let root_scope = [(
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
        registry.to_owned(),
        10,
        13,
    )];
    EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        chain,
        &block_hashes[..2],
        &root_scope,
    )
    .await?;
    EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        chain,
        &block_hashes[..3],
        &[
            root_scope[0].clone(),
            (
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                retired_registry.to_owned(),
                12,
                12,
            ),
        ],
    )
    .await?;
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), retired_registry).await?,
        1
    );
    EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        chain,
        &[
            block_hashes[0].clone(),
            block_hashes[1].clone(),
            block_hashes[3].clone(),
        ],
        &root_scope,
    )
    .await?;
    assert!(
        bigname_manifests::load_watched_contracts(database.pool())
            .await?
            .iter()
            .all(|contract| contract.address != normalize_address(retired_registry)),
        "the replaced registry must be retired before closure replay"
    );

    delete_normalized_events_for_emitter_for_test(database.pool(), retired_registry).await?;
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), retired_registry).await?,
        0
    );
    sync_ens_v2_registry_resource_surface_through_block(database.pool(), chain, 13).await?;
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), retired_registry).await?,
        1,
        "full closure must replay retained canonical logs from retired discovered registries"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_scoped_discovery_transition_preserves_unrelated_observation_keys() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let first_token = format!("0x{:064x}", 0xa1);
    let second_token = format!("0x{:064x}", 0xb1);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        contract_instance_id,
        &registry,
        0,
    )
    .await?;

    let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");
    for (token_id, label, block_number) in [
        (first_token.clone(), "alice", 10),
        (second_token.clone(), "bob", 11),
    ] {
        harness.apply(RegistryObservation::LabelRegistered {
            token_id,
            labelhash: labelhash(label),
            label: label.to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, block_number, 0),
        })?;
    }
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: first_token.clone(),
        subregistry: "0x00000000000000000000000000000000000000c1".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 12, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: second_token,
        subregistry: "0x00000000000000000000000000000000000000c2".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 13, 0),
    })?;
    reconcile_discovery_observation_history_by_source(database.pool(), &harness.observations, true)
        .await?;

    let before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM discovery_edges WHERE deactivated_at IS NULL")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(before, 2);

    let observation_start = harness.observations.len();
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: first_token,
        subregistry: ZERO_ADDRESS.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 14, 0),
    })?;
    reconcile_discovery_observation_history_by_source(
        database.pool(),
        &harness.observations[observation_start..],
        false,
    )
    .await?;

    let active_targets = sqlx::query_scalar::<_, String>(
        r#"
        SELECT lower(provenance ->> 'to_address')
        FROM discovery_edges
        WHERE deactivated_at IS NULL
        ORDER BY active_from_block_number
        "#,
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        active_targets,
        vec!["0x00000000000000000000000000000000000000c2".to_owned()],
        "scoped reconciliation must not deactivate the untouched token's edge"
    );

    database.cleanup().await
}

#[test]
fn ens_v2_lifecycle_skips_unnormalizable_labels_without_aborting() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1235);
    let token = "0x00000000000000000000000000000000000000000000000000000000000000a2".to_owned();
    let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token.clone(),
        labelhash: labelhash("Ni\u{200d}ck"),
        label: "Ni\u{200d}ck".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;

    assert!(harness.states_by_registry_token.is_empty());
    assert!(harness.graph_events.is_empty());

    Ok(())
}

#[test]
fn ens_v2_subregistry_zero_and_swap_deactivate_stale_child_suffixes() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let child_one = "0x00000000000000000000000000000000000000c1".to_owned();
    let child_two = "0x00000000000000000000000000000000000000c2".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let child_instance_id = Uuid::from_u128(0x5678);
    let parent_token =
        "0x00000000000000000000000000000000000000000000000000000000000000a1".to_owned();
    let child_token =
        "0x00000000000000000000000000000000000000000000000000000000000000b1".to_owned();
    let mut harness = RegistryHarness::new(&registry, contract_instance_id, "eth");

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: parent_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token.clone(),
        subregistry: child_one.clone(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 11, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(&child_one),
        Some(&"alice.eth".to_owned())
    );

    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token.clone(),
        subregistry: ZERO_ADDRESS.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 12, 0),
    })?;
    assert!(!harness.registry_suffix_by_address.contains_key(&child_one));
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: child_token.clone(),
        labelhash: labelhash("bob"),
        label: "bob".to_owned(),
        owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&child_one, child_instance_id, 13, 0),
    })?;
    assert!(
        !harness
            .states_by_registry_token
            .contains_key(&(child_one.clone(), child_token.clone()))
    );

    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token,
        subregistry: child_two.clone(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 14, 0),
    })?;
    assert!(!harness.registry_suffix_by_address.contains_key(&child_one));
    assert_eq!(
        harness.registry_suffix_by_address.get(&child_two),
        Some(&"alice.eth".to_owned())
    );
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: child_token.clone(),
        labelhash: labelhash("bob"),
        label: "bob".to_owned(),
        owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&child_one, child_instance_id, 15, 0),
    })?;
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: child_token.clone(),
        labelhash: labelhash("bob"),
        label: "bob".to_owned(),
        owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&child_two, child_instance_id, 16, 0),
    })?;
    assert!(
        !harness
            .states_by_registry_token
            .contains_key(&(child_one, child_token.clone()))
    );
    assert!(
        harness
            .states_by_registry_token
            .contains_key(&(child_two, child_token))
    );

    Ok(())
}

struct RegistryHarness {
    registry_suffix_by_address: HashMap<String, String>,
    registry_contract_by_address: HashMap<String, Uuid>,
    states_by_registry_token: BTreeMap<(String, String), RegistryNameState>,
    linked_resource_states: BTreeMap<Uuid, RegistryNameState>,
    closed_bindings: BTreeMap<Uuid, SurfaceBinding>,
    token_aliases: HashMap<(String, String), (String, String)>,
    observations: Vec<DiscoveryObservation>,
    graph_events: Vec<NormalizedEvent>,
}

impl RegistryHarness {
    fn new(registry: &str, contract_instance_id: Uuid, suffix: &str) -> Self {
        Self {
            registry_suffix_by_address: HashMap::from([(registry.to_owned(), suffix.to_owned())]),
            registry_contract_by_address: HashMap::from([(
                registry.to_owned(),
                contract_instance_id,
            )]),
            states_by_registry_token: BTreeMap::new(),
            linked_resource_states: BTreeMap::new(),
            closed_bindings: BTreeMap::new(),
            token_aliases: HashMap::new(),
            observations: Vec::new(),
            graph_events: Vec::new(),
        }
    }

    fn apply(&mut self, observation: RegistryObservation) -> Result<()> {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut self.registry_suffix_by_address,
            registry_contract_by_address: &mut self.registry_contract_by_address,
            states_by_registry_token: &mut self.states_by_registry_token,
            linked_resource_states: &mut self.linked_resource_states,
            closed_bindings: &mut self.closed_bindings,
            token_aliases: &mut self.token_aliases,
            observations: &mut self.observations,
            graph_events: &mut self.graph_events,
        };
        apply_registry_observation(observation, &mut context)
    }
}

async fn insert_test_registry_manifest(pool: &PgPool, chain: &str) -> Result<i64> {
    let manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO manifest_versions (
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
            'ens',
            $1,
            $2,
            'ens_v2_registry_scope_test',
            'active',
            'ensip15@ens-normalize-0.1.1',
            $3,
            $4::JSONB
        )
        RETURNING manifest_id
        "#,
    )
    .bind(SOURCE_FAMILY_ENS_V2_REGISTRY_L1)
    .bind(chain)
    .bind(format!(
        "test/ens_v2_registry_scope_{}_{}.toml",
        std::process::id(),
        NEXT_TEST_ID.load(Ordering::Relaxed)
    ))
    .bind(serde_json::to_string(&test_registry_manifest_payload(
        chain,
    ))?)
    .fetch_one(pool)
    .await
    .context("failed to insert scoped registry test manifest")?;
    sqlx::query(
        r#"
        INSERT INTO manifest_discovery_rules (
            manifest_id,
            edge_kind,
            from_role,
            admission
        )
        VALUES ($1, 'subregistry', 'registry', 'reachable_from_root'),
               ($1, 'resolver', 'registry', 'reachable_from_root')
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await
    .context("failed to insert scoped registry test discovery rules")?;
    Ok(manifest_id)
}

fn test_registry_manifest_payload(chain: &str) -> Value {
    json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        "chain": chain,
        "deployment_epoch": "ens_v2_registry_scope_test",
        "rollout_status": "active",
        "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [
            {
                "edge_kind": "subregistry",
                "from_role": "registry",
                "admission": "reachable_from_root"
            },
            {
                "edge_kind": "resolver",
                "from_role": "registry",
                "admission": "reachable_from_root"
            }
        ],
        "abi": {
            "events": [
                {
                    "name": "LabelRegistered",
                    "fragment": "event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender)"
                },
                {
                    "name": "LabelReserved",
                    "fragment": "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender)"
                },
                {
                    "name": "LabelReserved",
                    "fragment": "event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint256 expiry, address indexed sender)"
                },
                {
                    "name": "LabelUnregistered",
                    "fragment": "event LabelUnregistered(uint256 indexed tokenId, address indexed sender)"
                },
                {
                    "name": "ExpiryUpdated",
                    "fragment": "event ExpiryUpdated(uint256 indexed tokenId, uint64 indexed newExpiry, address indexed sender)"
                },
                {
                    "name": "SubregistryUpdated",
                    "fragment": "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)"
                },
                {
                    "name": "ResolverUpdated",
                    "fragment": "event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender)"
                },
                {
                    "name": "TokenResource",
                    "fragment": "event TokenResource(uint256 indexed tokenId, uint256 indexed resource)"
                },
                {
                    "name": "TokenRegenerated",
                    "fragment": "event TokenRegenerated(uint256 indexed oldTokenId, uint256 indexed newTokenId)"
                },
                {
                    "name": "ParentUpdated",
                    "fragment": "event ParentUpdated(address indexed parent, string label, address indexed sender)"
                },
            ]
        }
    })
}

async fn insert_test_registry_contract(
    pool: &PgPool,
    manifest_id: i64,
    role: &str,
    contract_instance_id: Uuid,
    address: &str,
    active_from_block_number: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        SELECT $1, chain, 'registry', '{}'::JSONB
        FROM manifest_versions
        WHERE manifest_id = $2
        "#,
    )
    .bind(contract_instance_id)
    .bind(manifest_id)
    .execute(pool)
    .await
    .context("failed to insert scoped registry test contract instance")?;

    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            source_manifest_id,
            provenance
        )
        SELECT $1, chain, $2, $3, manifest_id, '{}'::JSONB
        FROM manifest_versions
        WHERE manifest_id = $4
        "#,
    )
    .bind(contract_instance_id)
    .bind(normalize_address(address))
    .bind(active_from_block_number)
    .bind(manifest_id)
    .execute(pool)
    .await
    .context("failed to insert scoped registry test contract address")?;

    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id,
            declaration_kind,
            declaration_name,
            contract_instance_id,
            declared_address,
            role,
            proxy_kind
        )
        VALUES ($1, 'contract', $2, $3, $4, $2, 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(role)
    .bind(contract_instance_id)
    .bind(normalize_address(address))
    .execute(pool)
    .await
    .context("failed to insert scoped registry test manifest contract")?;

    if role == "registry" {
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address
            )
            VALUES ($1, 'root', 'registry_root', $2, $3)
            ON CONFLICT (manifest_id, declaration_kind, declaration_name) DO NOTHING
            "#,
        )
        .bind(manifest_id)
        .bind(contract_instance_id)
        .bind(normalize_address(address))
        .execute(pool)
        .await
        .context("failed to insert scoped registry test root")?;
    }

    Ok(())
}

fn test_raw_block(chain: &str, block_hash: &str, block_number: i64) -> RawBlock {
    RawBlock {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_717_172_700 + block_number)
            .expect("test timestamp should fit"),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn test_active_emitter(
    address: &str,
    contract_instance_id: Uuid,
    source_manifest_id: i64,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
) -> ActiveEmitter {
    ActiveEmitter {
        address: normalize_address(address),
        contract_instance_id,
        source_manifest_id,
        namespace: "ens".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
        manifest_version: 1,
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
        role: Some("registry".to_owned()),
        source: WatchedContractSource::ManifestContract,
        source_rank: source_rank(WatchedContractSource::ManifestContract),
        active_from_block_number,
        active_to_block_number,
    }
}

fn label_reserved_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    label: &str,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtx{block_number}{log_index}"),
        transaction_index: log_index,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex("LabelReserved(uint256,bytes32,string,uint64,address)"),
            topic_word((log_index + 1) as u64),
            labelhash(label),
            topic_address("0x0000000000000000000000000000000000000dad"),
        ],
        data: label_reserved_data(label, 1_900_000_000),
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn label_registered_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    label: &str,
    token_id: u64,
    owner_label: &str,
) -> RawLog {
    let owner = match owner_label {
        "alice" => "0x0000000000000000000000000000000000000a11",
        "bob" => "0x0000000000000000000000000000000000000b0b",
        other => panic!("unsupported lifecycle owner {other}"),
    };
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xregistration{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex("LabelRegistered(uint256,bytes32,string,address,uint64,address)"),
            topic_word(token_id),
            labelhash(label),
            topic_address("0x0000000000000000000000000000000000000dad"),
        ],
        data: label_registered_data(label, owner, 1_900_000_000 + block_number as u64),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn token_resource_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    token_id: u64,
    resource_id: u64,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xregistration{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex("TokenResource(uint256,uint256)"),
            topic_word(token_id),
            topic_word(resource_id),
        ],
        data: Vec::new(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn subregistry_updated_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    token_id: u64,
    subregistry: &str,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xsubregistry{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex("SubregistryUpdated(uint256,address,address)"),
            topic_word(token_id),
            topic_address(subregistry),
            topic_address("0x0000000000000000000000000000000000000dad"),
        ],
        data: Vec::new(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn label_unregistered_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    token_id: u64,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xunregister{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex("LabelUnregistered(uint256,address)"),
            topic_word(token_id),
            topic_address("0x0000000000000000000000000000000000000dad"),
        ],
        data: Vec::new(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn label_registered_data(label: &str, owner: &str, expiry: u64) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&word_bytes(96));
    data.extend_from_slice(&address_word_bytes(owner));
    data.extend_from_slice(&word_bytes(expiry));
    data.extend_from_slice(&word_bytes(label.len() as u64));
    data.extend_from_slice(label.as_bytes());
    while data.len() % 32 != 0 {
        data.push(0);
    }
    data
}

fn address_word_bytes(address: &str) -> [u8; 32] {
    let normalized = address.trim_start_matches("0x");
    let mut word = [0u8; 32];
    for (index, byte) in normalized.as_bytes().chunks_exact(2).enumerate() {
        let encoded = std::str::from_utf8(byte).expect("test address should be ASCII");
        word[12 + index] = u8::from_str_radix(encoded, 16).expect("test address should decode");
    }
    word
}

fn lifecycle_block_hash(block_number: i64) -> String {
    format!("0x{block_number:064x}")
}

fn lifecycle_branch_block_hash(block_number: i64, branch: u64) -> String {
    format!("0x{block_number:032x}{branch:032x}")
}

fn label_reserved_data(label: &str, expiry: u64) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&word_bytes(64));
    data.extend_from_slice(&word_bytes(expiry));
    data.extend_from_slice(&word_bytes(label.len() as u64));
    data.extend_from_slice(label.as_bytes());
    while data.len() % 32 != 0 {
        data.push(0);
    }
    data
}

fn word_bytes(value: u64) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[24..32].copy_from_slice(&value.to_be_bytes());
    word
}

fn topic_word(value: u64) -> String {
    format!("0x{value:064x}")
}

fn topic_address(address: &str) -> String {
    let normalized = address.trim_start_matches("0x").to_ascii_lowercase();
    format!("0x{normalized:0>64}")
}

async fn normalized_event_count_for_emitter(pool: &PgPool, address: &str) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM normalized_events
        WHERE raw_fact_ref->>'emitting_address' = $1
        "#,
    )
    .bind(normalize_address(address))
    .fetch_one(pool)
    .await
    .context("failed to count scoped registry normalized events by emitter")
}

async fn delete_normalized_events_for_emitter_for_test(pool: &PgPool, address: &str) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM projection_normalized_event_changes changes
        USING normalized_events events
        WHERE changes.normalized_event_id = events.normalized_event_id
          AND lower(events.raw_fact_ref ->> 'emitting_address') = $1
        "#,
    )
    .bind(normalize_address(address))
    .execute(pool)
    .await
    .context("failed to delete projection change rows for retired registry replay test")?;
    sqlx::query(
        r#"
        DELETE FROM normalized_events
        WHERE lower(raw_fact_ref ->> 'emitting_address') = $1
        "#,
    )
    .bind(normalize_address(address))
    .execute(pool)
    .await
    .context("failed to delete retired registry normalized events for replay test")?;
    Ok(())
}

async fn normalized_event_count(pool: &PgPool) -> Result<i64> {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
        .fetch_one(pool)
        .await
        .context("failed to count scoped registry normalized events")
}

fn labelhash(label: &str) -> String {
    format!("0x{}", hex_string(keccak256_bytes(label.as_bytes())))
}

fn reference(
    registry: &str,
    contract_instance_id: Uuid,
    block_number: i64,
    log_index: i64,
) -> ObservationRef {
    ObservationRef {
        chain_id: "ethereum-sepolia".to_owned(),
        block_hash: format!("0xblock{block_number}"),
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_717_172_700 + block_number)
            .expect("test timestamp should fit"),
        transaction_hash: format!("0xtx{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: registry.to_owned(),
        emitting_contract_instance_id: contract_instance_id,
        canonicality_state: CanonicalityState::Finalized,
        namespace: "ens".to_owned(),
        source_manifest_id: 1,
        source_family: SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
        manifest_version: 1,
    }
}
