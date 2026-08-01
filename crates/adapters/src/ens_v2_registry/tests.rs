use std::{
    collections::{BTreeSet, HashMap, HashSet},
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bigname_storage::{
    ChainLineageBlock, RawBlock, RawLog, default_database_url, load_name_surface, load_resource,
    load_surface_binding, load_surface_bindings_by_logical_name_id, load_token_lineage,
    mark_identity_rows_range_orphaned, upsert_chain_lineage_blocks, upsert_raw_blocks,
    upsert_raw_logs,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::*;
use super::{
    discovery::{
        ens_v2_registry_announcement_discovery_source, ens_v2_subregistry_discovery_source,
        reconcile_discovery_observation_history_by_source,
    },
    names::{discovery_observation_key, name_under_registry},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
const LIVE_TEST_DEPLOYMENT_PROFILE: &str = "sepolia";

async fn sync_ens_v2_registry_resource_surface_live_poll(
    pool: &PgPool,
    chain: &str,
    target_block_number: i64,
    block_hashes: &[String],
) -> Result<EnsV2RegistryResourceSurfaceSyncSummary> {
    super::sync_ens_v2_registry_resource_surface_live_poll(
        pool,
        LIVE_TEST_DEPLOYMENT_PROFILE,
        chain,
        target_block_number,
        block_hashes,
    )
    .await
}

#[test]
fn ens_v2_suffix_initialization_preserves_resumed_tombstones() {
    let registry = "0x00000000000000000000000000000000000000aa";
    let emitters = [test_active_emitter(
        registry,
        Uuid::from_u128(0x1711),
        7,
        Some(0),
        None,
    )];
    let mut resumed = RegistryReplayState::default();

    initialize_registry_suffixes(&mut resumed, &emitters, true);
    assert!(
        !resumed
            .registry_suffix_by_address
            .contains_key(&normalize_address(registry))
    );

    initialize_registry_suffixes(&mut resumed, &emitters, false);
    assert_eq!(
        resumed
            .registry_suffix_by_address
            .get(&normalize_address(registry))
            .map(String::as_str),
        Some("eth")
    );
}

#[test]
fn ens_v2_transfer_single_decodes_only_nonzero_ownership_moves() -> Result<()> {
    let topics = registry_event_topics();
    let registry = "0x00000000000000000000000000000000000000aa";
    let operator = "0x0000000000000000000000000000000000000f00";
    let seller = "0x0000000000000000000000000000000000000a11";
    let buyer = "0x0000000000000000000000000000000000000b0b";

    let decoded = build_registry_observations(
        &registry_raw_log_row(transfer_single_raw_log(
            "ethereum-sepolia",
            &lifecycle_block_hash(11),
            11,
            registry,
            3,
            operator,
            seller,
            buyer,
            7,
            1,
        )),
        &topics,
    )?;
    assert_eq!(decoded.len(), 1);
    assert!(matches!(
        &decoded[0],
        RegistryObservation::TokenControlTransferred {
            source_event: "TransferSingle",
            transfer_index: None,
            from,
            to,
            ..
        } if from == seller && to == buyer
    ));

    for (from, to, amount) in [
        (ZERO_ADDRESS, buyer, 1),
        (seller, ZERO_ADDRESS, 1),
        (seller, buyer, 0),
    ] {
        let ignored = build_registry_observations(
            &registry_raw_log_row(transfer_single_raw_log(
                "ethereum-sepolia",
                &lifecycle_block_hash(12),
                12,
                registry,
                0,
                operator,
                from,
                to,
                7,
                amount,
            )),
            &topics,
        )?;
        assert!(ignored.is_empty());
    }

    Ok(())
}

#[test]
fn ens_v2_transfer_batch_fans_out_without_rewriting_registration_state() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa";
    let contract_instance_id = Uuid::from_u128(0x1234);
    let operator = "0x0000000000000000000000000000000000000f00";
    let seller = "0x0000000000000000000000000000000000000a11";
    let buyer = "0x0000000000000000000000000000000000000b0b";
    let token_ids = [7_u64, 8_u64];
    let mut harness = RegistryHarness::new(registry, contract_instance_id, "eth");

    for (index, (token_id, label)) in token_ids.into_iter().zip(["first", "second"]).enumerate() {
        let token_id = format!("0x{token_id:064x}");
        harness.apply(RegistryObservation::LabelRegistered {
            token_id: token_id.clone(),
            labelhash: labelhash(label),
            label: label.to_owned(),
            owner: seller.to_owned(),
            expiry: 1_900_000_000,
            sender: operator.to_owned(),
            reference: reference(registry, contract_instance_id, 10, index as i64 * 2),
        })?;
        harness.apply(RegistryObservation::TokenResource {
            token_id,
            upstream_resource: format!("0x{:064x}", 101 + index),
            reference: reference(registry, contract_instance_id, 10, index as i64 * 2 + 1),
        })?;
    }

    let original_states = harness.states_by_registry_token.clone();
    let original_linked_states = harness.linked_resource_states.clone();
    let raw_log = registry_raw_log_row(transfer_batch_raw_log(
        "ethereum-sepolia",
        &lifecycle_block_hash(11),
        11,
        registry,
        5,
        operator,
        seller,
        buyer,
        &[7, 8],
        &[1, 1],
    ));
    let observations = build_registry_observations(&raw_log, &registry_event_topics())?;
    assert_eq!(observations.len(), 2);
    for observation in observations {
        harness.apply(observation)?;
    }

    assert_eq!(harness.states_by_registry_token, original_states);
    assert_eq!(harness.linked_resource_states, original_linked_states);
    let transfer_events = harness
        .graph_events
        .iter()
        .filter(|event| event.event_kind == EVENT_KIND_TOKEN_CONTROL_TRANSFERRED)
        .collect::<Vec<_>>();
    assert_eq!(transfer_events.len(), 2);
    assert_ne!(
        transfer_events[0].event_identity,
        transfer_events[1].event_identity
    );
    assert!(
        transfer_events
            .iter()
            .any(|event| event.event_identity.ends_with("batch:0"))
    );
    assert!(
        transfer_events
            .iter()
            .any(|event| event.event_identity.ends_with("batch:1"))
    );
    assert_ne!(
        transfer_events[0].logical_name_id,
        transfer_events[1].logical_name_id
    );
    assert_ne!(
        transfer_events[0].resource_id,
        transfer_events[1].resource_id
    );
    assert_eq!(
        transfer_events[0].raw_fact_ref,
        transfer_events[1].raw_fact_ref
    );
    assert!(transfer_events.iter().all(|event| {
        event.log_index == Some(5)
            && event.before_state["from"] == seller
            && event.after_state["to"] == buyer
            && event.after_state["source_event"] == "TransferBatch"
    }));

    let malformed = registry_raw_log_row(transfer_batch_raw_log(
        "ethereum-sepolia",
        &lifecycle_block_hash(12),
        12,
        registry,
        0,
        operator,
        seller,
        buyer,
        &[7, 8],
        &[1],
    ));
    assert!(
        build_registry_observations(&malformed, &registry_event_topics())
            .unwrap_err()
            .to_string()
            .contains("ids and values length mismatch")
    );

    Ok(())
}

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        Self::new_with_max_connections(5).await
    }

    async fn new_with_max_connections(max_connections: u32) -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for ENSv2 registry tests")?;
        let base_options = bigname_storage::stamp_projection_replay_version(base_options);
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
            .max_connections(max_connections)
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
async fn ens_v2_resumed_replay_does_not_reseed_a_removed_manifest_suffix() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1711),
        registry,
        0,
    )
    .await?;

    let (_, cold_state) = sync_ens_v2_registry_resource_surface_with_scope_and_state(
        database.pool(),
        chain,
        true,
        &[],
        None,
        RawLogCanonicalityFilter::IncludeObserved,
        None,
        None,
        true,
        true,
        false,
        None,
    )
    .await?;
    assert_eq!(
        cold_state
            .registry_suffix_by_address
            .get(&normalize_address(registry))
            .map(String::as_str),
        Some("eth")
    );

    let (_, resumed_state) = sync_ens_v2_registry_resource_surface_with_scope_and_state(
        database.pool(),
        chain,
        true,
        &[],
        None,
        RawLogCanonicalityFilter::IncludeObserved,
        None,
        Some(RegistryReplayState::default()),
        true,
        true,
        false,
        None,
    )
    .await?;
    assert!(
        !resumed_state
            .registry_suffix_by_address
            .contains_key(&normalize_address(registry)),
        "an incremental replay must preserve a prior suffix tombstone"
    );

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
        .sync_hashes(database.pool(), std::slice::from_ref(&losing_hash), 10, 10)
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
        .sync_hashes(database.pool(), std::slice::from_ref(&winning_hash), 10, 10)
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
            std::slice::from_ref(&winning_registration_hash),
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
async fn ens_v2_transfer_resync_is_idempotent_and_preserves_registration_fact() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    fixture
        .insert_registration(database.pool(), 10, 1, 101, "alice")
        .await?;
    let transfer_block_hash = lifecycle_block_hash(11);
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(fixture.chain, &transfer_block_hash, 11)],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[transfer_single_raw_log(
            fixture.chain,
            &transfer_block_hash,
            11,
            fixture.address,
            0,
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000b0b",
            1,
            1,
        )],
    )
    .await?;

    let block_hashes = [lifecycle_block_hash(10), transfer_block_hash];
    let source_scope = [(
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
        fixture.address.to_owned(),
        10,
        11,
    )];
    let first = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        fixture.chain,
        &block_hashes,
        &source_scope,
    )
    .await?;
    assert_eq!(
        first.by_kind.get(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED),
        Some(&1)
    );
    let registration_before = sqlx::query_scalar::<_, Value>(
        "SELECT after_state FROM normalized_events WHERE event_kind = 'RegistrationGranted' AND resource_id IS NOT NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        registration_before["registrant"],
        "0x0000000000000000000000000000000000000a11"
    );

    let second = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        fixture.chain,
        &block_hashes,
        &source_scope,
    )
    .await?;
    assert_eq!(second.total_normalized_event_inserted_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE event_kind = 'TokenControlTransferred'"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );
    let registration_after = sqlx::query_scalar::<_, Value>(
        "SELECT after_state FROM normalized_events WHERE event_kind = 'RegistrationGranted' AND resource_id IS NOT NULL",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(registration_after, registration_before);

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_scoped_transfer_hydrates_from_retained_raw_predecessors() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    fixture
        .insert_registration(database.pool(), 10, 1, 101, "alice")
        .await?;
    let transfer_hash = lifecycle_block_hash(11);
    let mut transfer_block = test_raw_block(fixture.chain, &transfer_hash, 11);
    transfer_block.parent_hash = Some(lifecycle_block_hash(10));
    upsert_raw_blocks(database.pool(), &[transfer_block]).await?;
    upsert_raw_logs(
        database.pool(),
        &[transfer_single_raw_log(
            fixture.chain,
            &transfer_hash,
            11,
            fixture.address,
            0,
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000b0b",
            1,
            1,
        )],
    )
    .await?;

    let summary = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        fixture.chain,
        &[transfer_hash],
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            fixture.address.to_owned(),
            11,
            11,
        )],
    )
    .await?;
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED),
        Some(&1)
    );
    let event = sqlx::query_as::<_, (Option<String>, Option<Uuid>, Value)>(
        r#"
        SELECT logical_name_id, resource_id, after_state
        FROM normalized_events
        WHERE event_kind = 'TokenControlTransferred'
        "#,
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(event.0.as_deref(), Some("ens:fleeting.eth"));
    assert!(event.1.is_some());
    assert_eq!(event.2["upstream_resource"], format!("0x{:064x}", 101));
    assert!(event.2.get("registry_hydration_pending").is_none());

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_transfer_hydration_respects_same_block_parent_edge_positions() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000a1";
    let registry_id = Uuid::from_u128(0x12f1);
    let old_parent_id = Uuid::from_u128(0x12f2);
    let new_parent_id = Uuid::from_u128(0x12f3);
    let block_hash = "0xblock10";
    let token_id = format!("0x{:064x}", 1);
    insert_finalized_reference_lineage(database.pool(), 9, 10).await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(chain, "0xblock9", 9, registry, 0, "leaf", 1, "alice"),
            token_resource_raw_log(chain, "0xblock9", 9, registry, 1, 1, 101),
        ],
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, $4, 'registry'), ($2, $4, 'registry'), ($3, $4, 'registry')
        "#,
    )
    .bind(registry_id)
    .bind(old_parent_id)
    .bind(new_parent_id)
    .bind(chain)
    .execute(database.pool())
    .await?;

    // Insert the newer edge first so discovery_edge_id order opposes event chronology.
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
            discovery_source, admission, active_from_block_number, active_from_block_hash,
            provenance
        ) VALUES (
            $1, 'subregistry', $2, $3, 'ens_v2_registry_l1:test-new-parent', 'admitted',
            10, $4, jsonb_build_object(
                'to_address', $5::TEXT, 'logical_name_id', 'ens:new.eth',
                'transaction_index', 3, 'log_index', 8
            )
        )
        "#,
    )
    .bind(chain)
    .bind(new_parent_id)
    .bind(registry_id)
    .bind(block_hash)
    .bind(registry)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
            discovery_source, admission, active_from_block_number, active_from_block_hash,
            active_to_block_number, active_to_block_hash, deactivated_at, provenance
        ) VALUES (
            $1, 'subregistry', $2, $3, 'ens_v2_registry_l1:test-old-parent', 'admitted',
            10, $4, 10, $4, now(), jsonb_build_object(
                'to_address', $5::TEXT, 'logical_name_id', 'ens:old.eth',
                'transaction_index', 0, 'log_index', 1,
                'active_to_transaction_index', 3, 'active_to_log_index', 8
            )
        )
        "#,
    )
    .bind(chain)
    .bind(old_parent_id)
    .bind(registry_id)
    .bind(block_hash)
    .bind(registry)
    .execute(database.pool())
    .await?;

    let mut harness = RegistryHarness::new(registry, registry_id, "");
    for (transaction_index, log_index) in [(0, 2), (4, 9)] {
        let mut transfer_ref = reference(registry, registry_id, 10, log_index);
        transfer_ref.transaction_index = transaction_index;
        harness.apply(RegistryObservation::TokenControlTransferred {
            token_id: token_id.clone(),
            operator: "0x0000000000000000000000000000000000000f00".to_owned(),
            from: "0x0000000000000000000000000000000000000a11".to_owned(),
            to: "0x0000000000000000000000000000000000000b0b".to_owned(),
            amount: "1".to_owned(),
            source_event: "TransferSingle",
            transfer_index: None,
            reference: transfer_ref,
        })?;
    }

    hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events).await?;
    let names = harness
        .graph_events
        .iter()
        .map(|event| event.logical_name_id.as_deref())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![Some("ens:leaf.old.eth"), Some("ens:leaf.new.eth")]
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_subregistry_target_hydration_respects_same_block_edge_positions() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let future_child = "0x00000000000000000000000000000000000000c1";
    let closed_child = "0x00000000000000000000000000000000000000c2";
    let registry_id = Uuid::from_u128(0x1301);
    let future_child_id = Uuid::from_u128(0x1302);
    let closed_child_id = Uuid::from_u128(0x1303);
    let token_id = format!("0x{:064x}", 1);
    insert_finalized_reference_lineage(database.pool(), 9, 10).await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, $4, 'registry'), ($2, $4, 'registry'), ($3, $4, 'registry')
        "#,
    )
    .bind(registry_id)
    .bind(future_child_id)
    .bind(closed_child_id)
    .bind(chain)
    .execute(database.pool())
    .await?;

    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
            discovery_source, admission, active_from_block_number, active_from_block_hash,
            provenance
        ) VALUES (
            $1, 'subregistry', $2, $3, 'ens_v2_registry_l1:test-future', 'admitted',
            10, '0xblock10', jsonb_build_object(
                'to_address', $4::TEXT, 'transaction_index', 3, 'log_index', 8
            )
        )
        "#,
    )
    .bind(chain)
    .bind(registry_id)
    .bind(future_child_id)
    .bind(future_child)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id,
            discovery_source, admission, active_from_block_number, active_from_block_hash,
            active_to_block_number, active_to_block_hash, deactivated_at, provenance
        ) VALUES (
            $1, 'subregistry', $2, $3, 'ens_v2_registry_l1:test-closed', 'admitted',
            10, '0xblock10', 10, '0xblock10', now(), jsonb_build_object(
                'to_address', $4::TEXT, 'transaction_index', 0, 'log_index', 1,
                'active_to_transaction_index', 3, 'active_to_log_index', 8
            )
        )
        "#,
    )
    .bind(chain)
    .bind(registry_id)
    .bind(closed_child_id)
    .bind(closed_child)
    .execute(database.pool())
    .await?;

    let mut harness = RegistryHarness::new(registry, registry_id, "eth");
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token_id.clone(),
        labelhash: labelhash("leaf"),
        label: "leaf".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, registry_id, 9, 0),
    })?;
    let mut before_boundary = reference(registry, registry_id, 10, 2);
    before_boundary.transaction_index = 0;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token_id.clone(),
        subregistry: future_child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: before_boundary,
    })?;
    let mut after_boundary = reference(registry, registry_id, 10, 9);
    after_boundary.transaction_index = 4;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id,
        subregistry: closed_child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: after_boundary,
    })?;

    let error = hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events)
        .await
        .expect_err("canonical events outside an edge's active position must fail loudly");
    assert!(
        error
            .to_string()
            .contains("has no matching selected-path discovery edge"),
        "unexpected hydration error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_transfer_hydration_uses_only_its_selected_ancestor_path() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    let registration_hash = lifecycle_branch_block_hash(10, 0);
    let losing_hash = lifecycle_branch_block_hash(11, 1);
    let winning_hash = lifecycle_branch_block_hash(11, 2);
    let transfer_hash = lifecycle_branch_block_hash(12, 0);
    fixture
        .insert_registration_at_hash(database.pool(), &registration_hash, 10, 1, 101, "alice")
        .await?;

    let mut losing_block = test_raw_block(fixture.chain, &losing_hash, 11);
    losing_block.parent_hash = Some(registration_hash.clone());
    losing_block.canonicality_state = CanonicalityState::Observed;
    let mut winning_block = test_raw_block(fixture.chain, &winning_hash, 11);
    winning_block.parent_hash = Some(registration_hash);
    let mut transfer_block = test_raw_block(fixture.chain, &transfer_hash, 12);
    transfer_block.parent_hash = Some(winning_hash);
    upsert_raw_blocks(
        database.pool(),
        &[losing_block, winning_block, transfer_block],
    )
    .await?;

    let mut sibling_unregister =
        label_unregistered_raw_log(fixture.chain, &losing_hash, 11, fixture.address, 0, 1);
    sibling_unregister.canonicality_state = CanonicalityState::Observed;
    let transfer = transfer_single_raw_log(
        fixture.chain,
        &transfer_hash,
        12,
        fixture.address,
        0,
        "0x0000000000000000000000000000000000000a11",
        "0x0000000000000000000000000000000000000a11",
        "0x0000000000000000000000000000000000000b0b",
        1,
        1,
    );
    upsert_raw_logs(database.pool(), &[sibling_unregister, transfer]).await?;

    let summary = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        fixture.chain,
        &[transfer_hash],
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            fixture.address.to_owned(),
            12,
            12,
        )],
    )
    .await?;
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED),
        Some(&1),
        "a non-orphaned sibling unregister must not erase selected-path transfer history"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_replay_rejects_canonical_sibling_registration_prefix() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    let common_hash = lifecycle_branch_block_hash(9, 0);
    let sibling_registration_hash = lifecycle_branch_block_hash(10, 1);
    let selected_parent_hash = lifecycle_branch_block_hash(10, 2);
    let expiry_hash = lifecycle_branch_block_hash(11, 2);

    let common_block = test_raw_block(fixture.chain, &common_hash, 9);
    let mut sibling_registration_block =
        test_raw_block(fixture.chain, &sibling_registration_hash, 10);
    sibling_registration_block.parent_hash = Some(common_hash.clone());
    sibling_registration_block.canonicality_state = CanonicalityState::Canonical;
    let mut selected_parent_block = test_raw_block(fixture.chain, &selected_parent_hash, 10);
    selected_parent_block.parent_hash = Some(common_hash);
    selected_parent_block.canonicality_state = CanonicalityState::Canonical;
    let mut expiry_block = test_raw_block(fixture.chain, &expiry_hash, 11);
    expiry_block.parent_hash = Some(selected_parent_hash);
    expiry_block.canonicality_state = CanonicalityState::Canonical;
    upsert_raw_blocks(
        database.pool(),
        &[
            common_block,
            sibling_registration_block,
            selected_parent_block,
            expiry_block,
        ],
    )
    .await?;

    let mut sibling_registration = label_registered_raw_log(
        fixture.chain,
        &sibling_registration_hash,
        10,
        fixture.address,
        0,
        "fleeting",
        1,
        "alice",
    );
    sibling_registration.canonicality_state = CanonicalityState::Canonical;
    let mut sibling_resource = token_resource_raw_log(
        fixture.chain,
        &sibling_registration_hash,
        10,
        fixture.address,
        1,
        1,
        101,
    );
    sibling_resource.canonicality_state = CanonicalityState::Canonical;
    let mut expiry = expiry_updated_raw_log(
        fixture.chain,
        &expiry_hash,
        11,
        fixture.address,
        0,
        1,
        2_000_000_000,
    );
    expiry.canonicality_state = CanonicalityState::Canonical;
    upsert_raw_logs(
        database.pool(),
        &[sibling_registration, sibling_resource, expiry],
    )
    .await?;
    refresh_test_raw_log_closure_proof(database.pool(), fixture.chain, 11).await?;

    let summary =
        EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope_canonical_only(
            database.pool(),
            fixture.chain,
            &[expiry_hash],
            &[(
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                fixture.address.to_owned(),
                11,
                11,
            )],
        )
        .await?;
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(
        summary.total_normalized_event_count, 0,
        "an expiry on the selected path must not find registration state from a canonical-marked sibling"
    );
    assert_eq!(normalized_event_count(database.pool()).await?, 0);

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_transfer_hydration_filters_unrelated_token_history_before_decode() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    fixture
        .insert_registration(database.pool(), 10, 1, 101, "alice")
        .await?;
    let registration_hash = lifecycle_block_hash(10);
    let mut malformed_unrelated = token_resource_raw_log(
        fixture.chain,
        &registration_hash,
        10,
        fixture.address,
        2,
        2,
        202,
    );
    malformed_unrelated.topics.pop();
    malformed_unrelated.topics[1] = format!("0x{}00000002", "11".repeat(28));
    upsert_raw_logs(database.pool(), &[malformed_unrelated]).await?;

    let transfer_hash = lifecycle_block_hash(11);
    let mut transfer_block = test_raw_block(fixture.chain, &transfer_hash, 11);
    transfer_block.parent_hash = Some(registration_hash);
    upsert_raw_blocks(database.pool(), &[transfer_block]).await?;
    upsert_raw_logs(
        database.pool(),
        &[transfer_single_raw_log(
            fixture.chain,
            &transfer_hash,
            11,
            fixture.address,
            0,
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000b0b",
            1,
            1,
        )],
    )
    .await?;

    let summary = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        fixture.chain,
        &[transfer_hash],
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            fixture.address.to_owned(),
            11,
            11,
        )],
    )
    .await?;
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_TOKEN_CONTROL_TRANSFERRED),
        Some(&1),
        "unrelated token history must be excluded by the batched predecessor query"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_scoped_transfer_fails_without_complete_raw_predecessors() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    let registration_hash = lifecycle_block_hash(10);
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(fixture.chain, &registration_hash, 10)],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[label_registered_raw_log(
            fixture.chain,
            &registration_hash,
            10,
            fixture.address,
            0,
            "alice",
            1,
            "alice",
        )],
    )
    .await?;
    let transfer_hash = lifecycle_block_hash(11);
    let mut transfer_block = test_raw_block(fixture.chain, &transfer_hash, 11);
    transfer_block.parent_hash = Some(registration_hash);
    upsert_raw_blocks(database.pool(), &[transfer_block]).await?;
    upsert_raw_logs(
        database.pool(),
        &[transfer_single_raw_log(
            fixture.chain,
            &transfer_hash,
            11,
            fixture.address,
            0,
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000b0b",
            1,
            1,
        )],
    )
    .await?;

    let error = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        fixture.chain,
        &[transfer_hash],
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            fixture.address.to_owned(),
            11,
            11,
        )],
    )
    .await
    .err()
    .context("cold transfer without TokenResource predecessor must fail closed")?;
    assert!(
        format!("{error:#}").contains("missing a retained non-orphaned TokenResource predecessor")
    );
    assert_eq!(normalized_event_count(database.pool()).await?, 0);

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_scoped_transfer_fails_without_registration_predecessor() -> Result<()> {
    let database = TestDatabase::new().await?;
    let fixture = RegistryLifecycleFixture::insert(database.pool()).await?;
    let transfer_hash = lifecycle_block_hash(11);
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(fixture.chain, &transfer_hash, 11)],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[transfer_single_raw_log(
            fixture.chain,
            &transfer_hash,
            11,
            fixture.address,
            0,
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000a11",
            "0x0000000000000000000000000000000000000b0b",
            1,
            1,
        )],
    )
    .await?;

    let error = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        fixture.chain,
        &[transfer_hash],
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            fixture.address.to_owned(),
            11,
            11,
        )],
    )
    .await
    .err()
    .context("cold transfer without LabelRegistered predecessor must fail closed")?;
    assert!(
        format!("{error:#}")
            .contains("missing a retained non-orphaned LabelRegistered predecessor")
    );
    assert_eq!(normalized_event_count(database.pool()).await?, 0);

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_root_only_scope_loads_the_complete_registry_event_abi() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let root = "0x00000000000000000000000000000000000000aa";
    let manifest_id = insert_test_root_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "root_registry",
        Uuid::from_u128(0x1200),
        root,
        0,
    )
    .await?;
    let block_hash = lifecycle_block_hash(10);
    upsert_raw_blocks(database.pool(), &[test_raw_block(chain, &block_hash, 10)]).await?;
    upsert_raw_logs(
        database.pool(),
        &[label_reserved_raw_log(
            chain,
            &block_hash,
            10,
            root,
            0,
            "eth",
        )],
    )
    .await?;

    let summary = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        chain,
        &[block_hash],
        &[(
            SOURCE_FAMILY_ENS_V2_ROOT_L1.to_owned(),
            root.to_owned(),
            10,
            10,
        )],
    )
    .await?;
    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_REGISTRATION_RESERVED),
        Some(&1)
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_registry_created_scope_admits_only_same_position_and_later_proxy_history()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let anchor = "0x00000000000000000000000000000000000000aa";
    let announced_registry = "0x00000000000000000000000000000000000000bb";
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1201),
        anchor,
        0,
    )
    .await?;
    let block_hash = lifecycle_block_hash(42);
    upsert_raw_blocks(database.pool(), &[test_raw_block(chain, &block_hash, 42)]).await?;
    let pre_announcement = upgraded_raw_log(
        chain,
        &block_hash,
        42,
        announced_registry,
        0,
        "0x0000000000000000000000000000000000000011",
    );
    let mut registry_created = registry_created_raw_log(chain, &block_hash, 42, announced_registry);
    registry_created.transaction_hash = pre_announcement.transaction_hash.clone();
    registry_created.log_index = 1;
    let mut post_announcement = upgraded_raw_log(
        chain,
        &block_hash,
        42,
        announced_registry,
        2,
        "0x0000000000000000000000000000000000000022",
    );
    post_announcement.transaction_hash = pre_announcement.transaction_hash.clone();
    upsert_raw_logs(
        database.pool(),
        &[pre_announcement, registry_created, post_announcement],
    )
    .await?;

    let summary =
        EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope_canonical_only(
            database.pool(),
            chain,
            &[block_hash],
            &[(
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                GENERIC_SOURCE_SCOPE_ADDRESS.to_owned(),
                42,
                42,
            )],
        )
        .await?;

    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 2);
    assert_eq!(summary.by_kind.get(EVENT_KIND_REGISTRY_CREATED), Some(&1));
    assert_eq!(
        summary.by_kind.get(EVENT_KIND_UPGRADED),
        Some(&1),
        "only the Upgraded log after RegistryCreated may be attributed to the announced registry"
    );
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), announced_registry).await?,
        2,
        "RegistryCreated and the post-announcement upgrade must remain in normalized history"
    );
    let upgraded = sqlx::query_as::<_, (i64, Value)>(
        r#"
        SELECT log_index, after_state
        FROM normalized_events
        WHERE event_kind = 'Upgraded'
          AND raw_fact_ref ->> 'emitting_address' = $1
        "#,
    )
    .bind(normalize_address(announced_registry))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        upgraded.0, 2,
        "the pre-announcement upgrade must stay raw-only"
    );
    assert_eq!(
        upgraded.1["implementation"],
        json!("0x0000000000000000000000000000000000000022")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'registry_announcement' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1,
        "RegistryCreated must also admit the announced registry instance"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_restricted_reconstruction_skips_retained_pre_announcement_logs() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let anchor = "0x00000000000000000000000000000000000000aa";
    let announced_registry = "0x00000000000000000000000000000000000000bb";
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1203),
        anchor,
        0,
    )
    .await?;

    let announcement_hash = lifecycle_block_hash(42);
    let later_hash = lifecycle_block_hash(43);
    let mut later_block = test_raw_block(chain, &later_hash, 43);
    later_block.parent_hash = Some(announcement_hash.clone());
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(chain, &announcement_hash, 42), later_block],
    )
    .await?;

    let pre_announcement = upgraded_raw_log(
        chain,
        &announcement_hash,
        42,
        announced_registry,
        0,
        "0x0000000000000000000000000000000000000011",
    );
    let mut registry_created =
        registry_created_raw_log(chain, &announcement_hash, 42, announced_registry);
    registry_created.transaction_hash = pre_announcement.transaction_hash.clone();
    registry_created.log_index = 1;
    let later_registration = label_registered_raw_log(
        chain,
        &later_hash,
        43,
        announced_registry,
        0,
        "child",
        1,
        "alice",
    );
    upsert_raw_logs(
        database.pool(),
        &[pre_announcement, registry_created, later_registration],
    )
    .await?;

    EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope_canonical_only(
        database.pool(),
        chain,
        std::slice::from_ref(&announcement_hash),
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            GENERIC_SOURCE_SCOPE_ADDRESS.to_owned(),
            42,
            42,
        )],
    )
    .await?;
    refresh_test_raw_log_closure_proof(database.pool(), chain, 43).await?;

    let summary =
        EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope_canonical_only(
            database.pool(),
            chain,
            &[later_hash],
            &[(
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                announced_registry.to_owned(),
                43,
                43,
            )],
        )
        .await?;

    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(
        summary.total_normalized_event_count, 0,
        "the unlinked registry has no suffix, but its later scoped sync must reconstruct safely"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_registry_created_wildcard_scope_skips_malformed_same_topic_log() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let anchor = "0x00000000000000000000000000000000000000aa";
    let malformed_emitter = "0x00000000000000000000000000000000000000b1";
    let announced_registry = "0x00000000000000000000000000000000000000b2";
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1202),
        anchor,
        0,
    )
    .await?;
    let block_hash = lifecycle_block_hash(43);
    upsert_raw_blocks(database.pool(), &[test_raw_block(chain, &block_hash, 43)]).await?;
    let mut malformed = registry_created_raw_log(chain, &block_hash, 43, malformed_emitter);
    malformed.data = vec![0x01];
    let mut valid = registry_created_raw_log(chain, &block_hash, 43, announced_registry);
    valid.log_index = 1;
    upsert_raw_logs(database.pool(), &[malformed, valid]).await?;

    let summary =
        EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope_canonical_only(
            database.pool(),
            chain,
            &[block_hash],
            &[(
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                GENERIC_SOURCE_SCOPE_ADDRESS.to_owned(),
                43,
                43,
            )],
        )
        .await?;

    assert_eq!(summary.matched_log_count, 1);
    assert_eq!(summary.by_kind.get(EVENT_KIND_REGISTRY_CREATED), Some(&1));
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), malformed_emitter).await?,
        0,
        "a malformed same-topic lookalike must remain raw-only"
    );
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), announced_registry).await?,
        1,
        "a malformed lookalike must not block a valid RegistryCreated announcement"
    );

    database.cleanup().await
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
async fn ens_v2_incremental_batches_match_full_replay_normalized_events() -> Result<()> {
    let incremental = TestDatabase::new().await?;
    let full_replay = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child_registry = "0x00000000000000000000000000000000000000b0";
    let incremental_hashes =
        insert_incremental_equivalence_fixture(incremental.pool(), chain, registry, child_registry)
            .await?;
    let full_replay_hashes =
        insert_incremental_equivalence_fixture(full_replay.pool(), chain, registry, child_registry)
            .await?;
    assert_eq!(incremental_hashes, full_replay_hashes);

    let mut incremental_matched_log_count = 0;
    let incremental_batches = [
        (
            vec![incremental_hashes[0].clone()],
            vec![
                (
                    SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                    GENERIC_SOURCE_SCOPE_ADDRESS.to_owned(),
                    9,
                    9,
                ),
                (
                    SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                    registry.to_owned(),
                    9,
                    9,
                ),
                (
                    SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                    child_registry.to_owned(),
                    9,
                    9,
                ),
            ],
        ),
        (
            vec![incremental_hashes[2].clone(), incremental_hashes[4].clone()],
            vec![(
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                registry.to_owned(),
                11,
                13,
            )],
        ),
        (
            vec![
                incremental_hashes[3].clone(),
                incremental_hashes[5].clone(),
                incremental_hashes[6].clone(),
            ],
            vec![(
                SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
                registry.to_owned(),
                12,
                15,
            )],
        ),
    ];
    for (block_hashes, source_scope) in incremental_batches {
        let summary =
            EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope_canonical_only(
                incremental.pool(),
                chain,
                &block_hashes,
                &source_scope,
            )
            .await?;
        incremental_matched_log_count += summary.matched_log_count;
        if summary.discovery_admission_epoch_bump_count > 0 {
            refresh_test_raw_log_closure_proof(incremental.pool(), chain, 15).await?;
        }
    }

    let full_summary =
        sync_ens_v2_registry_resource_surface_through_block(full_replay.pool(), chain, 15).await?;
    assert_eq!(incremental_matched_log_count, 12);
    assert_eq!(full_summary.matched_log_count, 12);

    let incremental_rows = normalized_event_rows_for_equivalence(incremental.pool()).await?;
    let full_replay_rows = normalized_event_rows_for_equivalence(full_replay.pool()).await?;
    assert!(
        !incremental_rows.is_empty(),
        "the differential fixture must derive normalized events"
    );
    assert_eq!(
        incremental_rows, full_replay_rows,
        "adversarial incremental batches must derive the exact normalized-event set produced by full replay"
    );
    assert_eq!(
        pending_registration_event_identity(incremental.pool(), "ens:alice.parent.eth").await?,
        pending_registration_event_identity(full_replay.pool(), "ens:alice.parent.eth").await?,
        "cold incremental replay must preserve the full-replay event identity immediately after a same-block parent edge opens"
    );

    let expected_corner_sources = BTreeSet::from([
        "ExpiryUpdated".to_owned(),
        "LabelUnregistered".to_owned(),
        "ParentUpdated".to_owned(),
        "RegistryCreated".to_owned(),
        "ResolverUpdated".to_owned(),
        "SubregistryUpdated".to_owned(),
        "TokenRegenerated".to_owned(),
    ]);
    assert_eq!(
        normalized_event_source_events(incremental.pool(), &expected_corner_sources).await?,
        expected_corner_sources,
        "every cold-state event named by the equivalence contract must contribute normalized output"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges
            WHERE chain_id = $1
              AND edge_kind IN ('resolver', 'subregistry')
              AND active_to_block_number = 15
              AND deactivated_at IS NOT NULL
              AND lower(provenance ->> 'from_address') = $2
            "#,
        )
        .bind(chain)
        .bind(normalize_address(registry))
        .fetch_one(incremental.pool())
        .await?,
        2,
        "terminal replay must retain bounded resolver and subregistry discovery history"
    );
    // Repairs must not be allowed to converge a divergent incremental derivation silently.
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT \
             FROM projection_normalized_event_changes \
             WHERE change_kind = 'content_update'",
        )
        .fetch_one(incremental.pool())
        .await?,
        0,
        "incremental equivalence must hold without normalized-event repair intervention"
    );

    incremental.cleanup().await?;
    full_replay.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_incremental_reconstruction_requires_complete_selected_ancestry() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let block_hash = lifecycle_block_hash(11);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1620),
        registry,
        0,
    )
    .await?;
    let mut block = test_raw_block(chain, &block_hash, 11);
    block.parent_hash = Some(lifecycle_block_hash(10));
    block.canonicality_state = CanonicalityState::Observed;
    upsert_raw_blocks(database.pool(), &[block]).await?;
    let mut raw_log = expiry_updated_raw_log(chain, &block_hash, 11, registry, 0, 1, 2_000_000_000);
    raw_log.canonicality_state = CanonicalityState::Observed;
    upsert_raw_logs(database.pool(), &[raw_log]).await?;

    let error = EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hash),
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            registry.to_owned(),
            11,
            11,
        )],
    )
    .await
    .err()
    .context("cold state reconstruction must reject an observed ancestry gap")?;
    assert!(
        format!("{error:#}").contains("cannot prove selected-path ancestry"),
        "unexpected selected-ancestry error: {error:#}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_registry_raw_log_prefix_rejects_canonical_only_ancestry() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let parent_hash = lifecycle_branch_block_hash(10, 0);
    let selected_hash = lifecycle_branch_block_hash(11, 0);
    let mut parent = test_raw_block(chain, &parent_hash, 10);
    parent.canonicality_state = CanonicalityState::Canonical;
    let mut selected = test_raw_block(chain, &selected_hash, 11);
    selected.parent_hash = Some(parent_hash);
    selected.canonicality_state = CanonicalityState::Canonical;
    upsert_raw_blocks(database.pool(), &[parent, selected]).await?;

    let mut before_raw =
        expiry_updated_raw_log(chain, &selected_hash, 11, registry, 0, 1, 2_000_000_000);
    before_raw.canonicality_state = CanonicalityState::Canonical;
    upsert_raw_logs(database.pool(), std::slice::from_ref(&before_raw)).await?;
    let before = registry_raw_log_row(before_raw);
    let emitters = [test_active_emitter(
        registry,
        Uuid::from_u128(0x1622),
        7,
        Some(0),
        None,
    )];
    let error = load_registry_raw_log_prefix(database.pool(), chain, &emitters, &before)
        .await
        .err()
        .context("registry raw-log prefix must reject canonical-only ancestry")?;
    assert!(
        format!("{error:#}")
            .contains("incremental prior-state reconstruction cannot prove selected-path ancestry"),
        "unexpected canonical-only ancestry refusal: {error:#}"
    );
    assert!(
        format!("{error:#}").contains("safe or finalized boundary"),
        "canonical-only ancestry must not self-certify as a stable boundary: {error:#}"
    );

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
    let root_registry_addresses = HashSet::from([registry.clone()]);
    let mut registry_contract_by_address =
        HashMap::from([(registry.clone(), contract_instance_id)]);
    let mut current_subregistry_by_parent_label = HashMap::new();
    let mut current_parent_claim_by_registry = HashMap::new();
    let mut entry_topology_by_registry_token = HashMap::new();
    let mut states_by_registry_token = BTreeMap::new();
    let mut state_keys_by_registry_namehash = HashMap::new();
    let mut linked_resource_states = BTreeMap::new();
    let mut retired_binding_states = BTreeMap::new();
    let mut closed_bindings = BTreeMap::new();
    let mut token_aliases = HashMap::new();
    let mut current_token_alias_by_canonical_key = HashMap::new();
    let mut observations = Vec::new();
    let mut graph_events = Vec::new();

    {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            root_registry_addresses: &root_registry_addresses,
            registry_contract_by_address: &mut registry_contract_by_address,
            current_subregistry_by_parent_label: &mut current_subregistry_by_parent_label,
            current_parent_claim_by_registry: &mut current_parent_claim_by_registry,
            entry_topology_by_registry_token: &mut entry_topology_by_registry_token,
            states_by_registry_token: &mut states_by_registry_token,
            state_keys_by_registry_namehash: &mut state_keys_by_registry_namehash,
            linked_resource_states: &mut linked_resource_states,
            retired_binding_states: &mut retired_binding_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            current_token_alias_by_canonical_key: &mut current_token_alias_by_canonical_key,
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
            root_registry_addresses: &root_registry_addresses,
            registry_contract_by_address: &mut registry_contract_by_address,
            current_subregistry_by_parent_label: &mut current_subregistry_by_parent_label,
            current_parent_claim_by_registry: &mut current_parent_claim_by_registry,
            entry_topology_by_registry_token: &mut entry_topology_by_registry_token,
            states_by_registry_token: &mut states_by_registry_token,
            state_keys_by_registry_namehash: &mut state_keys_by_registry_namehash,
            linked_resource_states: &mut linked_resource_states,
            retired_binding_states: &mut retired_binding_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            current_token_alias_by_canonical_key: &mut current_token_alias_by_canonical_key,
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
    let initial_registration_event = linked_resource_states
        .values()
        .next()
        .and_then(|state| {
            state.resource.as_ref().and_then(|link| {
                build_resource_events(state, link)
                    .into_iter()
                    .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_GRANTED)
            })
        })
        .context("initial RegistrationGranted event should be emitted")?;
    {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            root_registry_addresses: &root_registry_addresses,
            registry_contract_by_address: &mut registry_contract_by_address,
            current_subregistry_by_parent_label: &mut current_subregistry_by_parent_label,
            current_parent_claim_by_registry: &mut current_parent_claim_by_registry,
            entry_topology_by_registry_token: &mut entry_topology_by_registry_token,
            states_by_registry_token: &mut states_by_registry_token,
            state_keys_by_registry_namehash: &mut state_keys_by_registry_namehash,
            linked_resource_states: &mut linked_resource_states,
            retired_binding_states: &mut retired_binding_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            current_token_alias_by_canonical_key: &mut current_token_alias_by_canonical_key,
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
        Uuid::parse_str("1b3e5fe2-1f00-5c75-97c9-d2a5ccd024e2")?
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
            root_registry_addresses: &root_registry_addresses,
            registry_contract_by_address: &mut registry_contract_by_address,
            current_subregistry_by_parent_label: &mut current_subregistry_by_parent_label,
            current_parent_claim_by_registry: &mut current_parent_claim_by_registry,
            entry_topology_by_registry_token: &mut entry_topology_by_registry_token,
            states_by_registry_token: &mut states_by_registry_token,
            state_keys_by_registry_namehash: &mut state_keys_by_registry_namehash,
            linked_resource_states: &mut linked_resource_states,
            retired_binding_states: &mut retired_binding_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            current_token_alias_by_canonical_key: &mut current_token_alias_by_canonical_key,
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
    let registration_after_renewal = build_resource_events(renewed_state, &link)
        .into_iter()
        .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_GRANTED)
        .context("RegistrationGranted should remain present after renewal")?;
    assert_eq!(
        registration_after_renewal, initial_registration_event,
        "real expiry updates must not rewrite the registration-time grant fact"
    );
    assert!(graph_events.iter().any(|event| {
        event.event_kind == EVENT_KIND_EXPIRY_CHANGED
            && event.block_number == Some(12)
            && event.after_state["expiry"] == 2_000_000_000_u64
    }));
    {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut registry_suffix_by_address,
            root_registry_addresses: &root_registry_addresses,
            registry_contract_by_address: &mut registry_contract_by_address,
            current_subregistry_by_parent_label: &mut current_subregistry_by_parent_label,
            current_parent_claim_by_registry: &mut current_parent_claim_by_registry,
            entry_topology_by_registry_token: &mut entry_topology_by_registry_token,
            states_by_registry_token: &mut states_by_registry_token,
            state_keys_by_registry_namehash: &mut state_keys_by_registry_namehash,
            linked_resource_states: &mut linked_resource_states,
            retired_binding_states: &mut retired_binding_states,
            closed_bindings: &mut closed_bindings,
            token_aliases: &mut token_aliases,
            current_token_alias_by_canonical_key: &mut current_token_alias_by_canonical_key,
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
    let first_linked_state = harness
        .linked_resource_states
        .get(&first_resource_id)
        .context("first resource state should remain linked after unregister")?;
    let first_link = first_linked_state
        .resource
        .as_ref()
        .context("first resource state should retain its link")?;
    assert!(
        build_resource_events(first_linked_state, first_link)
            .iter()
            .any(|event| event.event_kind == EVENT_KIND_REGISTRATION_GRANTED),
        "unregister must retain the historical resource-specific registration grant"
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
    insert_finalized_reference_lineage(database.pool(), 10, 20).await?;

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
    let error = hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events)
        .await
        .expect_err("an unadmitted canonical subregistry target must fail loudly");
    assert!(
        error
            .to_string()
            .contains("has no matching selected-path discovery edge"),
        "unexpected hydration error: {error:#}"
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

    let orphaned_target_id = Uuid::from_u128(0xdead);
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
    .bind(orphaned_target_id)
    .execute(database.pool())
    .await
    .context("failed to insert losing-fork target contract")?;
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
            deactivated_at,
            provenance
        )
        VALUES (
            'ethereum-sepolia',
            'subregistry',
            $2,
            $1,
            'ens_v2_registry_l1:test-losing-fork',
            'admitted',
            11,
            '0xlosingfork',
            now(),
            jsonb_build_object('to_address', $3::text)
        )
        "#,
    )
    .bind(orphaned_target_id)
    .bind(contract_instance_id)
    .bind(&child)
    .execute(database.pool())
    .await
    .context("failed to insert deactivated losing-fork subregistry edge")?;

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

    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET deactivated_at = GREATEST(admitted_at, now()),
            active_to_block_number = NULL,
            active_to_block_hash = NULL
        WHERE to_contract_instance_id = $1
        "#,
    )
    .bind(target_id)
    .execute(database.pool())
    .await?;
    let mut stale_only = replay_source.clone();
    let error = hydrate_subregistry_event_target_ids(database.pool(), &mut stale_only)
        .await
        .expect_err("a canonical event with only an unbounded retracted edge must fail loudly");
    assert!(
        error
            .to_string()
            .contains("has no matching selected-path discovery edge"),
        "unexpected hydration error: {error:#}"
    );
    sqlx::query(
        "UPDATE discovery_edges SET deactivated_at = NULL WHERE to_contract_instance_id = $1",
    )
    .bind(target_id)
    .execute(database.pool())
    .await?;

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
async fn ens_v2_subregistry_hydration_ignores_bounded_observed_sibling_edge() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000c1";
    let registry_id = Uuid::from_u128(0x1751);
    let selected_target_id = Uuid::from_u128(0x1752);
    let sibling_target_id = Uuid::from_u128(0x1753);
    let finalized_hash = lifecycle_branch_block_hash(10, 0);
    let selected_start_hash = lifecycle_branch_block_hash(11, 1);
    let sibling_start_hash = lifecycle_branch_block_hash(11, 2);
    let event_hash = lifecycle_branch_block_hash(12, 1);
    let sibling_mid_hash = lifecycle_branch_block_hash(12, 2);
    let selected_close_hash = lifecycle_branch_block_hash(13, 1);
    let sibling_close_hash = lifecycle_branch_block_hash(13, 2);
    let after_close_hash = lifecycle_branch_block_hash(14, 1);

    let finalized = test_raw_block(chain, &finalized_hash, 10);
    let mut selected_start = test_raw_block(chain, &selected_start_hash, 11);
    selected_start.parent_hash = Some(finalized_hash.clone());
    selected_start.canonicality_state = CanonicalityState::Observed;
    let mut sibling_start = test_raw_block(chain, &sibling_start_hash, 11);
    sibling_start.parent_hash = Some(finalized_hash.clone());
    sibling_start.canonicality_state = CanonicalityState::Observed;
    let mut event_block = test_raw_block(chain, &event_hash, 12);
    event_block.parent_hash = Some(selected_start_hash.clone());
    event_block.canonicality_state = CanonicalityState::Observed;
    let mut sibling_mid = test_raw_block(chain, &sibling_mid_hash, 12);
    sibling_mid.parent_hash = Some(sibling_start_hash.clone());
    sibling_mid.canonicality_state = CanonicalityState::Observed;
    let mut selected_close = test_raw_block(chain, &selected_close_hash, 13);
    selected_close.parent_hash = Some(event_hash.clone());
    selected_close.canonicality_state = CanonicalityState::Observed;
    let mut sibling_close = test_raw_block(chain, &sibling_close_hash, 13);
    sibling_close.parent_hash = Some(sibling_mid_hash);
    sibling_close.canonicality_state = CanonicalityState::Observed;
    let mut after_close = test_raw_block(chain, &after_close_hash, 14);
    after_close.parent_hash = Some(selected_close_hash.clone());
    after_close.canonicality_state = CanonicalityState::Observed;
    upsert_raw_blocks(
        database.pool(),
        &[
            finalized,
            selected_start,
            sibling_start,
            event_block,
            sibling_mid,
            selected_close,
            sibling_close,
            after_close,
        ],
    )
    .await?;

    let token_id = format!("0x{:064x}", 0xa1);
    let mut harness = RegistryHarness::new(registry, registry_id, "eth");
    let mut registration_ref = reference(registry, registry_id, 10, 0);
    registration_ref.block_hash = finalized_hash;
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token_id.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: registration_ref,
    })?;
    let mut subregistry_ref = reference(registry, registry_id, 12, 0);
    subregistry_ref.block_hash = event_hash;
    subregistry_ref.canonicality_state = CanonicalityState::Observed;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id,
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: subregistry_ref,
    })?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind
        )
        VALUES ($1, $4, 'registry'),
               ($2, $4, 'contract'),
               ($3, $4, 'contract')
        "#,
    )
    .bind(registry_id)
    .bind(selected_target_id)
    .bind(sibling_target_id)
    .bind(chain)
    .execute(database.pool())
    .await
    .context("failed to insert sibling-fork subregistry contract instances")?;
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
            active_to_block_number,
            active_to_block_hash,
            deactivated_at,
            provenance
        )
        VALUES (
            $1,
            'subregistry',
            $2,
            $3,
            'ens_v2_registry_l1:selected-history',
            'admitted',
            11,
            $4,
            13,
            $5,
            now(),
            jsonb_build_object('to_address', $6::text)
        ), (
            $1,
            'subregistry',
            $2,
            $7,
            'ens_v2_registry_l1:sibling-history',
            'admitted',
            11,
            $8,
            13,
            $9,
            now(),
            jsonb_build_object('to_address', $6::text)
        )
        "#,
    )
    .bind(chain)
    .bind(registry_id)
    .bind(selected_target_id)
    .bind(selected_start_hash)
    .bind(&selected_close_hash)
    .bind(child)
    .bind(sibling_target_id)
    .bind(sibling_start_hash)
    .bind(&sibling_close_hash)
    .execute(database.pool())
    .await
    .context("failed to insert overlapping selected and sibling discovery edges")?;

    hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events).await?;
    let hydrated = harness
        .graph_events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
        .context("hydrated SubregistryChanged should remain present")?;
    assert_eq!(
        hydrated.after_state["to_contract_instance_id"],
        selected_target_id.to_string(),
        "the exact observed ancestor must win over an overlapping bounded sibling edge"
    );

    let mut after_close_event = hydrated.clone();
    after_close_event.block_number = Some(14);
    after_close_event.block_hash = Some(after_close_hash);
    sqlx::query(
        "UPDATE discovery_edges SET active_to_block_hash = $2 WHERE to_contract_instance_id = $1",
    )
    .bind(selected_target_id)
    .bind(&sibling_close_hash)
    .execute(database.pool())
    .await?;
    hydrate_subregistry_event_target_ids(
        database.pool(),
        std::slice::from_mut(&mut after_close_event),
    )
    .await?;
    assert_eq!(
        after_close_event.after_state["to_contract_instance_id"],
        selected_target_id.to_string(),
        "a sibling-only close must not deactivate the selected-path historical edge"
    );

    sqlx::query(
        "UPDATE discovery_edges SET active_to_block_hash = $2 WHERE to_contract_instance_id = $1",
    )
    .bind(selected_target_id)
    .bind(selected_close_hash)
    .execute(database.pool())
    .await?;
    hydrate_subregistry_event_target_ids(
        database.pool(),
        std::slice::from_mut(&mut after_close_event),
    )
    .await?;
    assert_eq!(
        after_close_event.after_state["to_contract_instance_id"],
        Value::Null,
        "a selected-path close must deactivate the historical edge after its boundary"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_subregistry_hydration_requires_stable_ancestry_and_excludes_canonical_sibling()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000c1";
    let registry_id = Uuid::from_u128(0x1754);
    let selected_target_id = Uuid::from_u128(0x1755);
    let sibling_target_id = Uuid::from_u128(0x1756);
    let common_hash = lifecycle_branch_block_hash(9, 0);
    let selected_start_hash = lifecycle_branch_block_hash(10, 1);
    let sibling_start_hash = lifecycle_branch_block_hash(10, 2);
    let event_hash = lifecycle_branch_block_hash(11, 1);

    let mut common = test_raw_block(chain, &common_hash, 9);
    common.canonicality_state = CanonicalityState::Canonical;
    let mut selected_start = test_raw_block(chain, &selected_start_hash, 10);
    selected_start.parent_hash = Some(common_hash.clone());
    selected_start.canonicality_state = CanonicalityState::Canonical;
    let mut sibling_start = test_raw_block(chain, &sibling_start_hash, 10);
    sibling_start.parent_hash = Some(common_hash.clone());
    sibling_start.canonicality_state = CanonicalityState::Canonical;
    let mut event_block = test_raw_block(chain, &event_hash, 11);
    event_block.parent_hash = Some(selected_start_hash.clone());
    event_block.canonicality_state = CanonicalityState::Canonical;
    upsert_raw_blocks(
        database.pool(),
        &[common, selected_start, sibling_start, event_block],
    )
    .await?;

    let token_id = format!("0x{:064x}", 0xa2);
    let mut harness = RegistryHarness::new(registry, registry_id, "eth");
    let mut registration_ref = reference(registry, registry_id, 9, 0);
    registration_ref.block_hash = common_hash.clone();
    registration_ref.canonicality_state = CanonicalityState::Canonical;
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token_id.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: registration_ref,
    })?;
    let mut subregistry_ref = reference(registry, registry_id, 11, 0);
    subregistry_ref.block_hash = event_hash;
    subregistry_ref.canonicality_state = CanonicalityState::Canonical;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id,
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: subregistry_ref,
    })?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind
        )
        VALUES ($1, $4, 'registry'),
               ($2, $4, 'contract'),
               ($3, $4, 'contract')
        "#,
    )
    .bind(registry_id)
    .bind(selected_target_id)
    .bind(sibling_target_id)
    .bind(chain)
    .execute(database.pool())
    .await
    .context("failed to insert canonical-sibling subregistry contract instances")?;
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
            $1,
            'subregistry',
            $2,
            $3,
            'ens_v2_registry_l1:selected-canonical-history',
            'admitted',
            10,
            $4,
            jsonb_build_object('to_address', $5::text)
        ), (
            $1,
            'subregistry',
            $2,
            $6,
            'ens_v2_registry_l1:sibling-canonical-history',
            'admitted',
            10,
            $7,
            jsonb_build_object('to_address', $5::text)
        )
        "#,
    )
    .bind(chain)
    .bind(registry_id)
    .bind(selected_target_id)
    .bind(&selected_start_hash)
    .bind(child)
    .bind(sibling_target_id)
    .bind(sibling_start_hash)
    .execute(database.pool())
    .await
    .context("failed to insert selected and canonical-sibling discovery edges")?;

    let error = hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events)
        .await
        .err()
        .context("subregistry hydration must reject canonical-only ancestry")?;
    assert!(
        format!("{error:#}")
            .contains("subregistry-target hydration cannot prove selected-path ancestry"),
        "unexpected canonical-only subregistry ancestry refusal: {error:#}"
    );
    assert!(
        format!("{error:#}").contains("safe or finalized boundary"),
        "canonical-only subregistry ancestry must fail at the stable-boundary guard: {error:#}"
    );

    upsert_raw_blocks(database.pool(), &[test_raw_block(chain, &common_hash, 9)]).await?;
    hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events).await?;
    let hydrated = harness
        .graph_events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
        .context("hydrated SubregistryChanged should remain present")?;
    assert_eq!(
        hydrated.after_state["to_contract_instance_id"],
        selected_target_id.to_string(),
        "a finalized boundary must admit only the selected-path edge, not its canonical sibling"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_orphaned_positionless_discovery_start_builds_valid_tombstone() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000c1";
    let start_hash = lifecycle_branch_block_hash(10, 1);
    let registry_id = Uuid::from_u128(0x1759);
    let child_id = Uuid::from_u128(0x175a);

    let mut orphaned_start = test_raw_block(chain, &start_hash, 10);
    orphaned_start.canonicality_state = CanonicalityState::Orphaned;
    upsert_chain_lineage_blocks(
        database.pool(),
        &[raw_block_to_test_lineage(&orphaned_start)],
    )
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
        VALUES ($1, $3, 'registry'), ($2, $3, 'registry')
        "#,
    )
    .bind(registry_id)
    .bind(child_id)
    .bind(chain)
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
            admission,
            active_from_block_number,
            active_from_block_hash,
            provenance
        ) VALUES (
            $1,
            'subregistry',
            $2,
            $3,
            $4,
            'admitted',
            10,
            $5,
            jsonb_build_object(
                'observation_key', 'legacy-positionless-edge',
                'from_address', $6::TEXT,
                'to_address', $7::TEXT
            )
        )
        "#,
    )
    .bind(chain)
    .bind(registry_id)
    .bind(child_id)
    .bind(ens_v2_subregistry_discovery_source(chain))
    .bind(&start_hash)
    .bind(registry)
    .bind(child)
    .execute(database.pool())
    .await?;

    let tombstones =
        super::discovery::load_orphaned_discovery_start_tombstones(database.pool(), chain).await?;
    assert_eq!(tombstones.len(), 1);
    assert_eq!(
        bigname_manifests::discovery_observation_evm_event_position(&tombstones[0].provenance)?,
        None,
        "a legacy positionless edge must produce a positionless tombstone"
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
    insert_finalized_reference_lineage(database.pool(), 10, 13).await?;
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
async fn ens_v2_repeated_same_subregistry_hydrates_from_continuous_edge_epoch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let child = "0x00000000000000000000000000000000000000c1".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1749);
    let token_id = format!("0x{:064x}", 0xa1);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_finalized_reference_lineage(database.pool(), 10, 12).await?;
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
        token_id: token_id.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;
    for block_number in [11, 12] {
        harness.apply(RegistryObservation::SubregistryUpdated {
            token_id: token_id.clone(),
            subregistry: child.clone(),
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(&registry, contract_instance_id, block_number, 0),
        })?;
    }
    let reconciliation = reconcile_discovery_observation_history_by_source(
        database.pool(),
        &harness.observations,
        true,
    )
    .await?;
    assert_eq!(
        reconciliation.inserted_edge_count, 1,
        "repeating one endpoint must preserve a single continuous discovery epoch"
    );

    hydrate_subregistry_event_target_ids(database.pool(), &mut harness.graph_events).await?;
    let hydrated_targets = harness
        .graph_events
        .iter()
        .filter(|event| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
        .map(|event| event.after_state["to_contract_instance_id"].clone())
        .collect::<Vec<_>>();
    assert_eq!(hydrated_targets.len(), 2);
    assert!(
        hydrated_targets
            .iter()
            .all(|target| target.as_str() == hydrated_targets[0].as_str()),
        "the later same-endpoint event must hydrate from the still-active edge instead of erasing the subtree"
    );
    assert!(hydrated_targets[0].is_string());

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_deep_discovery_history_replays_in_bounded_chunks() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let registry_id = Uuid::from_u128(0x1750);
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
    let discovery_source = format!("ens_v2_registry_subregistry:{chain}");
    let depth = 257usize;
    let mut parent = registry.to_owned();
    let mut observations = Vec::with_capacity(depth);
    for index in 0..depth {
        let child = format!("0x{:040x}", index + 1);
        observations.push(DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: parent,
            to_address: child.clone(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: discovery_source.clone(),
            active_from_block_number: Some(index as i64 + 1),
            active_from_block_hash: Some(format!("0x{:064x}", index + 1)),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: json!({
                "provider": "unit-test",
                "observation_key": format!("deep-{index}"),
            }),
        });
        parent = child;
    }

    let reconciliation = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        reconcile_discovery_observation_history_by_source(database.pool(), &observations, true),
    )
    .await
    .context("deep discovery replay exceeded its bounded verification window")??;
    assert_eq!(reconciliation.inserted_edge_count, depth);
    assert_eq!(reconciliation.active_edge_count, depth);
    assert_eq!(
        reconciliation.admission_epoch_bump_count, 3,
        "257 ordered transitions must commit as three bounded chunks"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        depth as i64
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_discovery_retry_distinguishes_same_block_repeated_assignment_positions()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child_a = "0x00000000000000000000000000000000000000a1";
    let child_b = "0x00000000000000000000000000000000000000b1";
    let registry_id = Uuid::from_u128(0x1757);
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
    let discovery_source = format!("ens_v2_registry_subregistry:{chain}");
    let mut observations = vec![
        same_block_discovery_observation(
            chain,
            registry,
            &discovery_source,
            "retry-key",
            child_a,
            1,
        ),
        same_block_discovery_observation(
            chain,
            registry,
            &discovery_source,
            "retry-key",
            child_b,
            2,
        ),
    ];
    for log_index in 3..=128 {
        observations.push(same_block_discovery_observation(
            chain,
            registry,
            &discovery_source,
            &format!("unrelated-{log_index}"),
            &format!("0x{log_index:040x}"),
            log_index,
        ));
    }
    observations.push(same_block_discovery_observation(
        chain,
        registry,
        &discovery_source,
        "retry-key",
        child_a,
        130,
    ));
    assert_eq!(observations.len(), 129);

    let committed_first_chunk = observations[..128]
        .iter()
        .cloned()
        .map(|observation| vec![observation])
        .collect::<Vec<_>>();
    bigname_manifests::reconcile_scoped_discovery_observation_transitions(
        database.pool(),
        &discovery_source,
        &committed_first_chunk,
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT lower(provenance ->> 'to_address')
            FROM discovery_edges
            WHERE discovery_source = $1
              AND provenance ->> 'observation_key' = 'retry-key'
              AND deactivated_at IS NULL
            "#,
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        normalize_address(child_b),
        "the simulated first committed chunk must stop at the intermediate assignment"
    );

    let retry =
        reconcile_discovery_observation_history_by_source(database.pool(), &observations, false)
            .await?;
    assert_eq!(retry.inserted_edge_count, 1);
    assert_eq!(retry.deactivated_edge_count, 1);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT lower(provenance ->> 'to_address')
            FROM discovery_edges
            WHERE discovery_source = $1
              AND provenance ->> 'observation_key' = 'retry-key'
              AND deactivated_at IS NULL
            "#,
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        normalize_address(child_a),
        "retry must apply the later same-block A observation instead of mistaking it for log 1"
    );

    let intervals = sqlx::query_as::<_, (i64, Option<i64>, Option<i64>, bool)>(
        r#"
        SELECT
            (provenance ->> 'log_index')::BIGINT,
            (provenance ->> 'active_to_transaction_index')::BIGINT,
            (provenance ->> 'active_to_log_index')::BIGINT,
            deactivated_at IS NULL
        FROM discovery_edges
        WHERE discovery_source = $1
          AND provenance ->> 'observation_key' = 'retry-key'
        ORDER BY (provenance ->> 'log_index')::BIGINT
        "#,
    )
    .bind(&discovery_source)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        intervals,
        vec![
            (1, Some(0), Some(2), false),
            (2, Some(0), Some(130), false),
            (130, None, None, true),
        ],
        "discovery provenance must retain full start and terminal EVM positions"
    );

    let historical_retry = bigname_manifests::reconcile_scoped_discovery_observation_transitions(
        database.pool(),
        &discovery_source,
        &[vec![observations[1].clone()]],
    )
    .await?;
    assert_eq!(historical_retry.inserted_edge_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges
            WHERE discovery_source = $1
              AND provenance ->> 'observation_key' = 'retry-key'
            "#,
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        3,
        "terminal-only provenance must not become part of the persisted start identity"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_discovery_retry_distinguishes_repeated_same_block_tombstones() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child_a = "0x00000000000000000000000000000000000000a1";
    let child_b = "0x00000000000000000000000000000000000000b1";
    let registry_id = Uuid::from_u128(0x1758);
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
    let discovery_source = format!("ens_v2_registry_subregistry:{chain}");
    let history = [
        same_block_discovery_observation(
            chain,
            registry,
            &discovery_source,
            "tombstone-key",
            child_a,
            1,
        ),
        same_block_discovery_observation(
            chain,
            registry,
            &discovery_source,
            "tombstone-key",
            ZERO_ADDRESS,
            2,
        ),
        same_block_discovery_observation(
            chain,
            registry,
            &discovery_source,
            "tombstone-key",
            child_b,
            3,
        ),
        same_block_discovery_observation(
            chain,
            registry,
            &discovery_source,
            "tombstone-key",
            ZERO_ADDRESS,
            4,
        ),
    ];
    let first_pair = history[..2]
        .iter()
        .cloned()
        .map(|observation| vec![observation])
        .collect::<Vec<_>>();
    bigname_manifests::reconcile_scoped_discovery_observation_transitions(
        database.pool(),
        &discovery_source,
        &first_pair,
    )
    .await?;

    reconcile_discovery_observation_history_by_source(database.pool(), &history, false).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges
            WHERE discovery_source = $1
              AND provenance ->> 'observation_key' = 'tombstone-key'
              AND deactivated_at IS NULL
            "#,
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        0,
        "the later tombstone must not be skipped because an earlier terminal used the same block"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT
                (provenance ->> 'active_to_transaction_index')::BIGINT,
                (provenance ->> 'active_to_log_index')::BIGINT
            FROM discovery_edges
            WHERE discovery_source = $1
              AND provenance ->> 'observation_key' = 'tombstone-key'
              AND provenance ->> 'log_index' = '3'
            "#,
        )
        .bind(&discovery_source)
        .fetch_one(database.pool())
        .await?,
        (0, 4)
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_live_poll_cache_is_incremental_and_rehydrates_on_unsafe_anchors() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let observed_child = "0x00000000000000000000000000000000000000c0";
    let child_a = "0x00000000000000000000000000000000000000c1";
    let child_b = "0x00000000000000000000000000000000000000c2";
    let registry_id = Uuid::from_u128(0x1751);
    let block_9_hash = lifecycle_branch_block_hash(9, 0);
    let block_10_hash = lifecycle_branch_block_hash(10, 0);
    let losing_11_hash = lifecycle_branch_block_hash(11, 1);
    let winning_11_hash = lifecycle_branch_block_hash(11, 2);
    let block_12_hash = lifecycle_branch_block_hash(12, 0);
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
    sqlx::query(
        "UPDATE raw_log_staging_input_revisions SET proven_through_block = 10 WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    let mut observed_block_10 = test_raw_block(chain, &block_10_hash, 10);
    observed_block_10.parent_hash = Some(block_9_hash.clone());
    observed_block_10.canonicality_state = CanonicalityState::Observed;
    let mut observed_registration =
        label_registered_raw_log(chain, &block_10_hash, 10, registry, 0, "parent", 1, "alice");
    observed_registration.canonicality_state = CanonicalityState::Observed;
    let mut observed_subregistry =
        subregistry_updated_raw_log(chain, &block_10_hash, 10, registry, 1, 1, observed_child);
    observed_subregistry.canonicality_state = CanonicalityState::Observed;
    let mut losing_block_11 = test_raw_block(chain, &losing_11_hash, 11);
    losing_block_11.parent_hash = Some(block_10_hash.clone());
    upsert_raw_blocks(
        database.pool(),
        &[
            test_raw_block(chain, &block_9_hash, 9),
            observed_block_10,
            losing_block_11,
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[observed_registration, observed_subregistry],
    )
    .await?;
    insert_completed_registry_coverage(
        database.pool(),
        chain,
        &[
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, registry),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, observed_child),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child_a),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child_b),
        ],
        0,
        12,
    )
    .await?;
    let first = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        std::slice::from_ref(&block_10_hash),
    )
    .await?;
    assert_eq!(first.scanned_log_count, 2);
    assert!(
        bigname_manifests::load_watched_contracts(database.pool())
            .await?
            .iter()
            .all(|contract| contract.address != normalize_address(observed_child)),
        "SubregistryUpdated must retain topology without admitting an event source"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges
            WHERE discovery_source = $1
              AND lower(provenance ->> 'to_address') = $2
              AND deactivated_at IS NULL
            "#,
        )
        .bind(format!("ens_v2_registry_subregistry:{chain}"))
        .bind(normalize_address(observed_child))
        .fetch_one(database.pool())
        .await?,
        1,
        "the topology edge must remain active"
    );

    let unchanged_head = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        std::slice::from_ref(&block_10_hash),
    )
    .await?;
    assert_eq!(
        unchanged_head.scanned_log_count, 0,
        "an unchanged selected head with unchanged raw input must preserve and reuse its cache"
    );

    invalidate_live_registry_replay_state(database.pool(), chain);
    let observed_fallback = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        std::slice::from_ref(&block_10_hash),
    )
    .await?;
    assert_eq!(observed_fallback.scanned_log_count, 2);
    assert!(
        bigname_manifests::load_watched_contracts(database.pool())
            .await?
            .iter()
            .all(|contract| contract.address != normalize_address(observed_child)),
        "full live fallback must not turn topology into indexability"
    );

    let revision_before_overlap = sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM raw_log_staging_input_revisions WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
    let mut overlapping_registration =
        label_registered_raw_log(chain, &block_10_hash, 10, registry, 0, "parent", 1, "alice");
    overlapping_registration.canonicality_state = CanonicalityState::Observed;
    let mut overlapping_subregistry =
        subregistry_updated_raw_log(chain, &block_10_hash, 10, registry, 1, 1, observed_child);
    overlapping_subregistry.canonicality_state = CanonicalityState::Observed;
    upsert_raw_logs(
        database.pool(),
        &[overlapping_registration, overlapping_subregistry],
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM raw_log_staging_input_revisions WHERE chain_id = $1"
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?,
        revision_before_overlap,
        "an overlap poll that refreshes only observed_at must not invalidate cached adapter input"
    );

    upsert_raw_logs(
        database.pool(),
        &[subregistry_updated_raw_log(
            chain,
            &losing_11_hash,
            11,
            registry,
            0,
            1,
            child_a,
        )],
    )
    .await?;

    let second = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        11,
        std::slice::from_ref(&losing_11_hash),
    )
    .await?;
    assert_eq!(
        second.scanned_log_count, 1,
        "an unscoped advancing poll must consume only newly selected logs"
    );
    assert!(
        bigname_manifests::load_watched_contracts(database.pool())
            .await?
            .iter()
            .all(|contract| contract.address != normalize_address(child_a)),
        "an advancing topology update must not admit its child as an event source"
    );

    // A newly admitted log at the cached height cannot be proven absent from
    // the prior pass, so the same-height poll must rehydrate instead of
    // advancing an unchanged cache anchor.
    upsert_raw_logs(
        database.pool(),
        &[subregistry_updated_raw_log(
            chain,
            &losing_11_hash,
            11,
            registry,
            1,
            1,
            child_b,
        )],
    )
    .await?;
    let same_height = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        11,
        std::slice::from_ref(&losing_11_hash),
    )
    .await?;
    assert_eq!(same_height.scanned_log_count, 4);
    let active_targets = sqlx::query_scalar::<_, String>(
        r#"
        SELECT lower(provenance ->> 'to_address')
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(format!("ens_v2_registry_subregistry:{chain}"))
    .fetch_all(database.pool())
    .await?;
    assert_eq!(active_targets, vec![normalize_address(child_b)]);

    let mut block_12 = test_raw_block(chain, &block_12_hash, 12);
    block_12.parent_hash = Some(losing_11_hash.clone());
    upsert_raw_blocks(database.pool(), &[block_12]).await?;
    upsert_raw_logs(
        database.pool(),
        &[subregistry_updated_raw_log(
            chain,
            &losing_11_hash,
            11,
            registry,
            2,
            1,
            child_a,
        )],
    )
    .await?;
    let mixed_position_page = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        12,
        &[losing_11_hash.clone(), block_12_hash.clone()],
    )
    .await?;
    assert_eq!(
        mixed_position_page.scanned_log_count, 5,
        "a page containing any position at or below the cache anchor must fully rehydrate"
    );
    invalidate_live_registry_replay_state(database.pool(), chain);
    let restarted = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        12,
        std::slice::from_ref(&block_12_hash),
    )
    .await?;
    assert_eq!(
        restarted.scanned_log_count, 5,
        "a process-local cache miss must recover from retained non-orphaned history"
    );

    bigname_storage::mark_raw_block_facts_range_orphaned(
        database.pool(),
        chain,
        &losing_11_hash,
        Some(&block_10_hash),
    )
    .await?;
    bigname_storage::mark_raw_block_range_orphaned(
        database.pool(),
        chain,
        &losing_11_hash,
        Some(&block_10_hash),
    )
    .await?;
    let mut winning_block_11 = test_raw_block(chain, &winning_11_hash, 11);
    winning_block_11.parent_hash = Some(block_10_hash.clone());
    upsert_raw_blocks(database.pool(), &[winning_block_11]).await?;
    let rebuilt = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        11,
        std::slice::from_ref(&winning_11_hash),
    )
    .await?;
    assert_eq!(rebuilt.scanned_log_count, 2);
    let active_targets = sqlx::query_scalar::<_, String>(
        r#"
        SELECT lower(provenance ->> 'to_address')
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(format!("ens_v2_registry_subregistry:{chain}"))
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        active_targets,
        vec![normalize_address(observed_child)],
        "a stale lineage fallback must discard orphaned discoveries while preserving selected observed facts"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_incremental_live_parent_claim_uses_cached_current_link() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let grandparent = "0x00000000000000000000000000000000000000aa";
    let parent = "0x00000000000000000000000000000000000000bb";
    let stale_child = "0x00000000000000000000000000000000000000cc";
    let current_child = "0x00000000000000000000000000000000000000dd";
    let block_10_hash = lifecycle_branch_block_hash(10, 0);
    let block_11_hash = lifecycle_branch_block_hash(11, 0);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1791),
        grandparent,
        0,
    )
    .await?;
    let mut block_11 = test_raw_block(chain, &block_11_hash, 11);
    block_11.parent_hash = Some(block_10_hash.clone());
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(chain, &block_10_hash, 10), block_11],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(chain, &block_10_hash, 10, grandparent, 0, "old", 1, "alice"),
            subregistry_updated_raw_log(chain, &block_10_hash, 10, grandparent, 1, 1, parent),
            registry_created_raw_log_at(chain, &block_10_hash, 10, parent, 2),
            registry_created_raw_log_at(chain, &block_10_hash, 10, stale_child, 3),
            registry_created_raw_log_at(chain, &block_10_hash, 10, current_child, 4),
            parent_updated_raw_log(chain, &block_10_hash, 10, parent, 5, grandparent, "old"),
            label_registered_raw_log(chain, &block_10_hash, 10, parent, 6, "victim", 1, "alice"),
            subregistry_updated_raw_log(chain, &block_10_hash, 10, parent, 7, 1, stale_child),
            label_registered_raw_log(
                chain,
                &block_10_hash,
                10,
                grandparent,
                8,
                "renamed",
                2,
                "alice",
            ),
            subregistry_updated_raw_log(chain, &block_10_hash, 10, grandparent, 9, 2, parent),
            parent_updated_raw_log(
                chain,
                &block_10_hash,
                10,
                parent,
                10,
                grandparent,
                "renamed",
            ),
            label_registered_raw_log(chain, &block_10_hash, 10, parent, 11, "victim", 2, "alice"),
            subregistry_updated_raw_log(chain, &block_10_hash, 10, parent, 12, 2, current_child),
            parent_updated_raw_log(
                chain,
                &block_10_hash,
                10,
                current_child,
                13,
                parent,
                "victim",
            ),
            parent_updated_raw_log(chain, &block_11_hash, 11, stale_child, 0, parent, "victim"),
            label_registered_raw_log(
                chain,
                &block_11_hash,
                11,
                stale_child,
                1,
                "spoof",
                1,
                "alice",
            ),
            label_registered_raw_log(
                chain,
                &block_11_hash,
                11,
                current_child,
                2,
                "valid",
                1,
                "alice",
            ),
        ],
    )
    .await?;
    insert_completed_registry_coverage(
        database.pool(),
        chain,
        &[
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, grandparent),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, parent),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, stale_child),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, current_child),
        ],
        0,
        11,
    )
    .await?;

    let initial = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        std::slice::from_ref(&block_10_hash),
    )
    .await?;
    assert_eq!(initial.scanned_log_count, 14);
    let incremental = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        11,
        std::slice::from_ref(&block_11_hash),
    )
    .await?;
    assert_eq!(incremental.scanned_log_count, 3);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE logical_name_id = 'ens:spoof.victim.renamed.eth'"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "incremental live replay must reject a stale child claim using the cached current link"
    );
    let valid_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE logical_name_id = 'ens:valid.victim.renamed.eth' AND event_kind = 'RegistrationGranted'",
    )
    .fetch_one(database.pool())
    .await?;
    let observed_history = sqlx::query_as::<_, (String, Option<String>, Value)>(
        "SELECT event_kind, logical_name_id, after_state FROM normalized_events ORDER BY block_number, log_index, event_kind",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        valid_count, 1,
        "incremental live replay must retain the cached matching child claim and parent topology: {observed_history:?}"
    );

    invalidate_live_registry_replay_state(database.pool(), chain);
    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        11,
        std::slice::from_ref(&block_11_hash),
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM normalized_events WHERE logical_name_id = 'ens:spoof.victim.renamed.eth'"
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "cold replay and incremental live replay must apply the same current-link gate"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_incremental_live_bidirectional_changes_rotate_persisted_name_bindings() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let parent = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let block_hashes = (10..=13)
        .map(|block_number| lifecycle_branch_block_hash(block_number, 0))
        .collect::<Vec<_>>();
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x17a0),
        parent,
        0,
    )
    .await?;
    let mut blocks = Vec::new();
    for (offset, block_hash) in block_hashes.iter().enumerate() {
        let block_number = 10 + i64::try_from(offset).context("block offset exceeds i64")?;
        let mut block = test_raw_block(chain, block_hash, block_number);
        if offset > 0 {
            block.parent_hash = Some(block_hashes[offset - 1].clone());
        }
        blocks.push(block);
    }
    upsert_raw_blocks(database.pool(), &blocks).await?;
    upsert_raw_logs(
        database.pool(),
        &[
            registry_created_raw_log_at(chain, &block_hashes[0], 10, child, 0),
            label_registered_raw_log(chain, &block_hashes[0], 10, parent, 1, "victim", 1, "alice"),
            subregistry_updated_raw_log(chain, &block_hashes[0], 10, parent, 2, 1, child),
            parent_updated_raw_log(chain, &block_hashes[0], 10, child, 3, parent, "victim"),
            label_registered_raw_log(chain, &block_hashes[0], 10, child, 4, "leaf", 2, "alice"),
            token_resource_raw_log(chain, &block_hashes[0], 10, child, 5, 2, 200),
            parent_updated_raw_log(chain, &block_hashes[1], 11, child, 0, ZERO_ADDRESS, ""),
            parent_updated_raw_log(chain, &block_hashes[2], 12, child, 0, parent, "victim"),
            subregistry_updated_raw_log(chain, &block_hashes[3], 13, parent, 0, 1, ZERO_ADDRESS),
        ],
    )
    .await?;
    insert_completed_registry_coverage(
        database.pool(),
        chain,
        &[
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, parent),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child),
        ],
        0,
        13,
    )
    .await?;

    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        std::slice::from_ref(&block_hashes[0]),
    )
    .await?;
    let initial =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:leaf.victim.eth").await?;
    assert_eq!(initial.len(), 1);
    assert!(initial[0].active_to.is_none());
    let initial_binding_id = initial[0].surface_binding_id;

    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        11,
        std::slice::from_ref(&block_hashes[1]),
    )
    .await?;
    let detached =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:leaf.victim.eth").await?;
    assert_eq!(detached.len(), 1);
    assert!(
        detached[0].active_to.is_some(),
        "clearing the child claim must close the persisted name binding"
    );

    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        12,
        std::slice::from_ref(&block_hashes[2]),
    )
    .await?;
    let restored =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:leaf.victim.eth").await?;
    assert_eq!(restored.len(), 2);
    let restored_open = restored
        .iter()
        .find(|binding| binding.active_to.is_none())
        .context("restored binding epoch")?;
    assert_ne!(restored_open.surface_binding_id, initial_binding_id);

    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        13,
        std::slice::from_ref(&block_hashes[3]),
    )
    .await?;
    let unlinked =
        load_surface_bindings_by_logical_name_id(database.pool(), "ens:leaf.victim.eth").await?;
    assert!(
        unlinked.iter().all(|binding| binding.active_to.is_some()),
        "parent unlink must close whichever rebound epoch is current"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_live_poll_rehydrates_for_a_lower_id_log_committed_after_the_cache() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child_a = "0x00000000000000000000000000000000000000c1";
    let child_b = "0x00000000000000000000000000000000000000c2";
    let block_10_hash = lifecycle_branch_block_hash(10, 0);
    let block_11_hash = lifecycle_branch_block_hash(11, 0);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1752),
        registry,
        0,
    )
    .await?;

    let mut block_11 = test_raw_block(chain, &block_11_hash, 11);
    block_11.parent_hash = Some(block_10_hash.clone());
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(chain, &block_10_hash, 10), block_11],
    )
    .await?;

    let reserved_lower_raw_log_id = sqlx::query_scalar::<_, i64>(
        "SELECT nextval(pg_get_serial_sequence('raw_logs', 'raw_log_id'))::BIGINT",
    )
    .fetch_one(database.pool())
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(chain, &block_10_hash, 10, registry, 0, "parent", 1, "alice"),
            subregistry_updated_raw_log(chain, &block_10_hash, 10, registry, 1, 1, child_a),
        ],
    )
    .await?;
    insert_completed_registry_coverage(
        database.pool(),
        chain,
        &[
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, registry),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child_a),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child_b),
        ],
        0,
        11,
    )
    .await?;

    let initial = live::sync_ens_v2_registry_resource_surface_live_poll_with_tiny_cache(
        database.pool(),
        LIVE_TEST_DEPLOYMENT_PROFILE,
        chain,
        10,
        std::slice::from_ref(&block_10_hash),
    )
    .await?;
    assert_eq!(initial.scanned_log_count, 2);
    let checkpoint_revision = sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM raw_log_staging_input_revisions WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
    invalidate_live_registry_replay_state(database.pool(), chain);

    let late_log = subregistry_updated_raw_log(chain, &block_10_hash, 10, registry, 2, 1, child_b);
    insert_raw_log_with_id(database.pool(), reserved_lower_raw_log_id, &late_log).await?;
    let current_revision = sqlx::query_scalar::<_, i64>(
        "SELECT revision FROM raw_log_staging_input_revisions WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
    assert!(current_revision > checkpoint_revision);
    let maximum_raw_log_id =
        sqlx::query_scalar::<_, i64>("SELECT MAX(raw_log_id)::BIGINT FROM raw_logs")
            .fetch_one(database.pool())
            .await?;
    assert!(
        reserved_lower_raw_log_id < maximum_raw_log_id,
        "the regression must commit a lower identity after the cache's higher identity watermark"
    );

    let advanced = live::sync_ens_v2_registry_resource_surface_live_poll_with_tiny_cache(
        database.pool(),
        LIVE_TEST_DEPLOYMENT_PROFILE,
        chain,
        11,
        std::slice::from_ref(&block_11_hash),
    )
    .await?;
    assert_eq!(
        advanced.scanned_log_count, 3,
        "a lower-id log committed at the cached height must force exact-path rehydration"
    );
    let active_targets = sqlx::query_scalar::<_, String>(
        r#"
        SELECT lower(provenance ->> 'to_address')
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(format!("ens_v2_registry_subregistry:{chain}"))
    .fetch_all(database.pool())
    .await?;
    assert_eq!(active_targets, vec![normalize_address(child_b)]);
    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_discovery_epoch_drift_aborts_before_destructive_finalization() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000c1";
    let block_hash = lifecycle_branch_block_hash(10, 0);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1761),
        registry,
        0,
    )
    .await?;
    upsert_raw_blocks(database.pool(), &[test_raw_block(chain, &block_hash, 10)]).await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(chain, &block_hash, 10, registry, 0, "parent", 1, "alice"),
            subregistry_updated_raw_log(chain, &block_hash, 10, registry, 1, 1, child),
        ],
    )
    .await?;
    insert_completed_registry_coverage(
        database.pool(),
        chain,
        &[
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, registry),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child),
        ],
        0,
        12,
    )
    .await?;
    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        std::slice::from_ref(&block_hash),
    )
    .await?;
    let current_epoch =
        bigname_manifests::load_discovery_admission_epoch(database.pool(), chain).await?;
    let stale_epoch = current_epoch
        .checked_sub(1)
        .context("discovery fixture did not advance its admission epoch")?;
    let error = reconcile_discovery_observation_history_for_chain(
        database.pool(),
        chain,
        &[],
        true,
        Some(10),
        Some(stale_epoch),
    )
    .await
    .err()
    .context("stale discovery proof must abort full-source finalization")?;
    assert!(
        format!("{error:#}").contains("discovery admission epoch changed"),
        "unexpected discovery epoch refusal: {error:#}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(format!("ens_v2_registry_subregistry:{chain}"))
        .fetch_one(database.pool())
        .await?,
        1,
        "epoch drift must be detected before absence-based edge deactivation"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_first_raw_log_insert_starts_with_incomplete_retained_history() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "fresh-ens-v2-chain";
    let registry = "0x00000000000000000000000000000000000000aa";
    let block_hash = lifecycle_branch_block_hash(1, 0);
    upsert_raw_blocks(database.pool(), &[test_raw_block(chain, &block_hash, 1)]).await?;
    upsert_raw_logs(
        database.pool(),
        &[label_registered_raw_log(
            chain,
            &block_hash,
            1,
            registry,
            0,
            "parent",
            1,
            "alice",
        )],
    )
    .await?;
    let state = sqlx::query_as::<_, (i64, bool, Option<i64>)>(
        r#"
        SELECT retention_generation, retained_history_complete, proven_through_block
        FROM raw_log_staging_input_revisions
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(state, (0, false, None));

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_full_source_sync_completes_with_two_pool_connections() -> Result<()> {
    let database = TestDatabase::new_with_max_connections(2).await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let root_registry = "0x00000000000000000000000000000000000000bb";
    let block_hash = lifecycle_branch_block_hash(10, 0);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1765),
        registry,
        0,
    )
    .await?;
    let root_manifest_id = insert_test_root_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        root_manifest_id,
        "root",
        Uuid::from_u128(0x1766),
        root_registry,
        0,
    )
    .await?;
    upsert_raw_blocks(database.pool(), &[test_raw_block(chain, &block_hash, 10)]).await?;
    upsert_raw_logs(
        database.pool(),
        &[label_registered_raw_log(
            chain,
            &block_hash,
            10,
            registry,
            0,
            "parent",
            1,
            "alice",
        )],
    )
    .await?;
    insert_completed_registry_coverage(
        database.pool(),
        chain,
        &[
            (SOURCE_FAMILY_ENS_V2_ROOT_L1, root_registry),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, registry),
        ],
        0,
        10,
    )
    .await?;

    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sync_ens_v2_registry_resource_surface_through_block(database.pool(), chain, 10),
    )
    .await
    .context("full ENSv2 sync exhausted a two-connection pool")??;

    database.cleanup().await
}

#[tokio::test]
async fn raw_log_payload_update_rotates_retention_but_canonicality_update_does_not() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "raw-log-update-chain";
    let registry = "0x00000000000000000000000000000000000000aa";
    let block_hash = lifecycle_branch_block_hash(1, 0);
    upsert_raw_blocks(database.pool(), &[test_raw_block(chain, &block_hash, 1)]).await?;
    upsert_raw_logs(
        database.pool(),
        &[label_registered_raw_log(
            chain,
            &block_hash,
            1,
            registry,
            0,
            "parent",
            1,
            "alice",
        )],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retained_history_complete = true,
            incomplete_since = NULL,
            proven_retention_generation = retention_generation,
            proven_discovery_admission_epoch = 0,
            proven_through_block = 1
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    let before = sqlx::query_as::<_, (i64, i64, bool, Option<i64>, Option<i64>, Option<i64>)>(
        r#"
        SELECT
            revision,
            retention_generation,
            retained_history_complete,
            proven_retention_generation,
            proven_discovery_admission_epoch,
            proven_through_block
        FROM raw_log_staging_input_revisions
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;

    sqlx::query(
        "UPDATE raw_logs SET canonicality_state = 'observed'::canonicality_state WHERE chain_id = $1",
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    let after_canonicality =
        sqlx::query_as::<_, (i64, i64, bool, Option<i64>, Option<i64>, Option<i64>)>(
            r#"
            SELECT
                revision,
                retention_generation,
                retained_history_complete,
                proven_retention_generation,
                proven_discovery_admission_epoch,
                proven_through_block
            FROM raw_log_staging_input_revisions
            WHERE chain_id = $1
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?;
    assert_eq!(after_canonicality.0, before.0 + 1);
    assert_eq!(after_canonicality.1, before.1);
    assert_eq!(
        (
            after_canonicality.2,
            after_canonicality.3,
            after_canonicality.4,
            after_canonicality.5,
        ),
        (before.2, before.3, before.4, before.5),
        "canonicality changes keep the retained fact and its closure proof while invalidating replay caches"
    );

    sqlx::query("UPDATE raw_logs SET data = '0x01' WHERE chain_id = $1")
        .bind(chain)
        .execute(database.pool())
        .await?;
    let after_payload =
        sqlx::query_as::<_, (i64, i64, bool, Option<i64>, Option<i64>, Option<i64>)>(
            r#"
            SELECT
                revision,
                retention_generation,
                retained_history_complete,
                proven_retention_generation,
                proven_discovery_admission_epoch,
                proven_through_block
            FROM raw_log_staging_input_revisions
            WHERE chain_id = $1
            "#,
        )
        .bind(chain)
        .fetch_one(database.pool())
        .await?;
    assert_eq!(after_payload.0, after_canonicality.0 + 1);
    assert_eq!(after_payload.1, after_canonicality.1 + 1);
    assert!(!after_payload.2);
    assert_eq!(
        (after_payload.3, after_payload.4, after_payload.5),
        (None, None, None)
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_floor_ignores_earlier_unrelated_source_logs() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let unrelated = "0x00000000000000000000000000000000000000ff";
    let block_1_hash = lifecycle_branch_block_hash(1, 0);
    let block_10_hash = lifecycle_branch_block_hash(10, 0);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1762),
        registry,
        10,
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[
            test_raw_block(chain, &block_1_hash, 1),
            test_raw_block(chain, &block_10_hash, 10),
        ],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(
                chain,
                &block_1_hash,
                1,
                unrelated,
                0,
                "unrelated",
                1,
                "alice",
            ),
            label_registered_raw_log(chain, &block_10_hash, 10, registry, 0, "parent", 1, "alice"),
        ],
    )
    .await?;
    let summary = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        std::slice::from_ref(&block_10_hash),
    )
    .await?;
    assert_eq!(
        summary.scanned_log_count, 1,
        "an unrelated earlier raw log must neither lower the ENSv2 closure floor nor enter replay"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_live_poll_target_to_cache_anchor_path_is_delta_bounded() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let mut blocks = Vec::new();
    let mut parent_hash = None;
    for block_number in 1..=512 {
        let block_hash = lifecycle_branch_block_hash(block_number, 0);
        let mut block = test_raw_block(chain, &block_hash, block_number);
        block.parent_hash = parent_hash.replace(block_hash);
        blocks.push(block);
    }
    upsert_raw_blocks(database.pool(), &blocks).await?;
    let target_hash = blocks
        .last()
        .context("deep lineage fixture must have a target")?
        .block_hash
        .clone();
    let delta_path =
        live::load_selected_registry_path_to_floor(database.pool(), chain, 512, &target_hash, 511)
            .await?;
    assert_eq!(
        delta_path.len(),
        2,
        "an advancing cache probe must load only target and cached anchor, not full ancestry"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_cold_live_poll_rejects_a_lineage_gap_above_its_raw_log_closure_floor() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000c1";
    let block_10_hash = lifecycle_branch_block_hash(10, 0);
    let missing_11_hash = lifecycle_branch_block_hash(11, 0);
    let block_12_hash = lifecycle_branch_block_hash(12, 0);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1754),
        registry,
        0,
    )
    .await?;
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(chain, &block_10_hash, 10)],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(chain, &block_10_hash, 10, registry, 0, "parent", 1, "alice"),
            subregistry_updated_raw_log(chain, &block_10_hash, 10, registry, 1, 1, child),
        ],
    )
    .await?;
    insert_completed_registry_coverage(
        database.pool(),
        chain,
        &[
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, registry),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child),
        ],
        0,
        12,
    )
    .await?;
    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        std::slice::from_ref(&block_10_hash),
    )
    .await?;
    invalidate_live_registry_replay_state(database.pool(), chain);

    let mut block_12 = test_raw_block(chain, &block_12_hash, 12);
    block_12.parent_hash = Some(missing_11_hash);
    upsert_raw_blocks(database.pool(), &[block_12]).await?;
    let error = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        12,
        std::slice::from_ref(&block_12_hash),
    )
    .await
    .err()
    .context("cold live replay must reject a lineage gap above its closure floor")?;
    assert!(
        format!("{error:#}").contains("not parent-contiguous through closure floor 10"),
        "unexpected lineage-gap refusal: {error:#}"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(format!("ens_v2_registry_subregistry:{chain}"))
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_incremental_live_unregister_persists_the_binding_close() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let block_10_hash = lifecycle_branch_block_hash(10, 0);
    let block_11_hash = lifecycle_branch_block_hash(11, 0);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1755),
        registry,
        0,
    )
    .await?;
    let mut block_11 = test_raw_block(chain, &block_11_hash, 11);
    block_11.parent_hash = Some(block_10_hash.clone());
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(chain, &block_10_hash, 10), block_11],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(chain, &block_10_hash, 10, registry, 0, "parent", 1, "alice"),
            token_resource_raw_log(chain, &block_10_hash, 10, registry, 1, 1, 101),
        ],
    )
    .await?;
    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        std::slice::from_ref(&block_10_hash),
    )
    .await?;
    let open_binding = load_surface_bindings_by_logical_name_id(database.pool(), "ens:parent.eth")
        .await?
        .into_iter()
        .next()
        .context("initial live registration must materialize a binding")?;
    assert_eq!(open_binding.active_to, None);

    upsert_raw_logs(
        database.pool(),
        &[label_unregistered_raw_log(
            chain,
            &block_11_hash,
            11,
            registry,
            0,
            1,
        )],
    )
    .await?;
    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        11,
        std::slice::from_ref(&block_11_hash),
    )
    .await?;
    let closed_binding = load_surface_binding(database.pool(), open_binding.surface_binding_id)
        .await?
        .context("incremental unregister must retain the binding row")?;
    assert!(
        closed_binding.active_to.is_some(),
        "unregister-only incremental replay must persist its binding close"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_live_poll_hydrates_only_the_selected_target_ancestor_path() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child_a = "0x00000000000000000000000000000000000000d1";
    let child_b = "0x00000000000000000000000000000000000000d2";
    let child_c = "0x00000000000000000000000000000000000000d3";
    let registry_id = Uuid::from_u128(0x1761);
    let fork_a_10_hash = lifecycle_branch_block_hash(10, 10);
    let fork_b_10_hash = lifecycle_branch_block_hash(10, 20);
    let fork_a_11_hash = lifecycle_branch_block_hash(11, 10);
    let fork_a_12_hash = lifecycle_branch_block_hash(12, 10);
    let fork_a_13_hash = lifecycle_branch_block_hash(13, 10);
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

    let mut fork_a_10 = test_raw_block(chain, &fork_a_10_hash, 10);
    fork_a_10.canonicality_state = CanonicalityState::Observed;
    let mut fork_b_10 = test_raw_block(chain, &fork_b_10_hash, 10);
    fork_b_10.canonicality_state = CanonicalityState::Observed;
    let mut fork_a_11 = test_raw_block(chain, &fork_a_11_hash, 11);
    fork_a_11.parent_hash = Some(fork_a_10_hash.clone());
    upsert_raw_blocks(database.pool(), &[fork_a_10, fork_b_10, fork_a_11]).await?;

    let mut fork_a_registration = label_registered_raw_log(
        chain,
        &fork_a_10_hash,
        10,
        registry,
        0,
        "parent",
        1,
        "alice",
    );
    fork_a_registration.canonicality_state = CanonicalityState::Observed;
    let mut fork_b_registration =
        label_registered_raw_log(chain, &fork_b_10_hash, 10, registry, 0, "parent", 1, "bob");
    fork_b_registration.canonicality_state = CanonicalityState::Observed;
    let mut fork_b_discovery =
        subregistry_updated_raw_log(chain, &fork_b_10_hash, 10, registry, 1, 1, child_b);
    fork_b_discovery.canonicality_state = CanonicalityState::Observed;
    upsert_raw_logs(
        database.pool(),
        &[
            fork_a_registration,
            fork_b_registration,
            fork_b_discovery,
            subregistry_updated_raw_log(chain, &fork_a_11_hash, 11, registry, 0, 1, child_a),
        ],
    )
    .await?;
    insert_completed_registry_coverage(
        database.pool(),
        chain,
        &[
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, registry),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child_a),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child_b),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child_c),
        ],
        0,
        13,
    )
    .await?;

    let ambiguity = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        10,
        &[fork_a_10_hash.clone(), fork_b_10_hash.clone()],
    )
    .await
    .err()
    .context("a live-poll target with two selected hashes must fail closed")?;
    assert!(format!("{ambiguity:#}").contains("exactly one non-orphaned hash"));

    let summary = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        11,
        std::slice::from_ref(&fork_a_11_hash),
    )
    .await?;
    assert_eq!(
        summary.scanned_log_count, 2,
        "full hydration must read only fork A's registration and child update"
    );
    let active_targets = sqlx::query_scalar::<_, String>(
        r#"
        SELECT lower(provenance ->> 'to_address')
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(format!("ens_v2_registry_subregistry:{chain}"))
    .fetch_all(database.pool())
    .await?;
    assert_eq!(active_targets, vec![normalize_address(child_a)]);

    bigname_storage::mark_raw_block_facts_range_orphaned(
        database.pool(),
        chain,
        &fork_b_10_hash,
        None,
    )
    .await?;
    bigname_storage::mark_raw_block_range_orphaned(database.pool(), chain, &fork_b_10_hash, None)
        .await?;
    let mut fork_a_12 = test_raw_block(chain, &fork_a_12_hash, 12);
    fork_a_12.parent_hash = Some(fork_a_11_hash.clone());
    let mut fork_a_13 = test_raw_block(chain, &fork_a_13_hash, 13);
    fork_a_13.parent_hash = Some(fork_a_12_hash.clone());
    upsert_raw_blocks(database.pool(), &[fork_a_12, fork_a_13]).await?;
    upsert_raw_logs(
        database.pool(),
        &[subregistry_updated_raw_log(
            chain,
            &fork_a_12_hash,
            12,
            registry,
            0,
            1,
            child_c,
        )],
    )
    .await?;
    let advanced = sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        13,
        std::slice::from_ref(&fork_a_13_hash),
    )
    .await?;
    assert_eq!(
        advanced.scanned_log_count, 1,
        "incremental replay must include an intermediate ancestor log omitted by the selected target block"
    );
    let active_targets = sqlx::query_scalar::<_, String>(
        r#"
        SELECT lower(provenance ->> 'to_address')
        FROM discovery_edges
        WHERE discovery_source = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(format!("ens_v2_registry_subregistry:{chain}"))
    .fetch_all(database.pool())
    .await?;
    assert_eq!(active_targets, vec![normalize_address(child_c)]);

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_plain_full_sync_preserves_parent_link_after_retention_rotation() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000c1";
    let block_10_hash = lifecycle_branch_block_hash(10, 0);
    let block_11_hash = lifecycle_branch_block_hash(11, 0);
    let manifest_id = insert_test_registry_manifest(database.pool(), chain).await?;
    insert_test_registry_contract(
        database.pool(),
        manifest_id,
        "registry",
        Uuid::from_u128(0x1730),
        registry,
        0,
    )
    .await?;
    let mut block_11 = test_raw_block(chain, &block_11_hash, 11);
    block_11.parent_hash = Some(block_10_hash.clone());
    upsert_raw_blocks(
        database.pool(),
        &[test_raw_block(chain, &block_10_hash, 10), block_11],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            label_registered_raw_log(chain, &block_10_hash, 10, registry, 0, "parent", 1, "alice"),
            subregistry_updated_raw_log(chain, &block_10_hash, 10, registry, 1, 1, child),
        ],
    )
    .await?;
    EnsV2RegistryResourceSurfaceSyncSummary::sync_for_block_hashes_with_source_scope(
        database.pool(),
        chain,
        std::slice::from_ref(&block_10_hash),
        &[(
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
            registry.to_owned(),
            10,
            10,
        )],
    )
    .await?;
    assert_eq!(
        active_subregistry_edge_count(database.pool(), chain, child).await?,
        1
    );

    sqlx::query("TRUNCATE raw_logs")
        .execute(database.pool())
        .await
        .context("failed to rotate the raw-log corpus")?;
    upsert_raw_logs(
        database.pool(),
        &[transfer_single_raw_log(
            chain,
            &block_11_hash,
            11,
            registry,
            0,
            "0x0000000000000000000000000000000000000f00",
            ZERO_ADDRESS,
            "0x0000000000000000000000000000000000000b0b",
            2,
            1,
        )],
    )
    .await?;

    sync_ens_v2_registry_resource_surface_through_block(database.pool(), chain, 11).await?;
    assert_eq!(
        active_subregistry_edge_count(database.pool(), chain, child).await?,
        1,
        "a retained suffix cannot prove that an omitted historical parent link is absent"
    );

    invalidate_live_registry_replay_state(database.pool(), chain);
    sync_ens_v2_registry_resource_surface_live_poll(
        database.pool(),
        chain,
        11,
        std::slice::from_ref(&block_11_hash),
    )
    .await?;
    assert_eq!(
        active_subregistry_edge_count(database.pool(), chain, child).await?,
        1,
        "cold live hydration from a retained suffix cannot prove historical parent-link absence"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_full_closure_does_not_index_unannounced_subregistry_emitters() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa";
    let retired_registry = "0x00000000000000000000000000000000000000a1";
    let current_registry = "0x00000000000000000000000000000000000000b2";
    let registry_id = Uuid::from_u128(0x1741);
    let block_hashes = (10..=13).map(lifecycle_block_hash).collect::<Vec<_>>();
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
    let blocks = (10..=13)
        .map(|block_number| {
            let mut block =
                test_raw_block(chain, &lifecycle_block_hash(block_number), block_number);
            if block_number > 10 {
                block.parent_hash = Some(lifecycle_block_hash(block_number - 1));
            }
            block
        })
        .collect::<Vec<_>>();
    upsert_raw_blocks(database.pool(), &blocks).await?;
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
    refresh_test_raw_log_closure_proof(database.pool(), chain, 13).await?;
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
    refresh_test_raw_log_closure_proof(database.pool(), chain, 13).await?;
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), retired_registry).await?,
        0,
        "a parent link alone must not make the unannounced child an event source"
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
        "the unannounced child must remain outside the watched plan before and after replacement"
    );

    delete_normalized_events_for_emitter_for_test(database.pool(), retired_registry).await?;
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), retired_registry).await?,
        0
    );
    refresh_test_raw_log_closure_proof(database.pool(), chain, 13).await?;
    sync_ens_v2_registry_resource_surface_through_block(database.pool(), chain, 13).await?;
    assert_eq!(
        normalized_event_count_for_emitter(database.pool(), retired_registry).await?,
        0,
        "full closure must not infer indexability from retained parent-link history"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_scoped_discovery_transition_preserves_unrelated_observation_keys() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1234);
    let first_token = versioned_label_token("alice", 0xa1);
    let second_token = versioned_label_token("bob", 0xb1);
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
    let scoped_summary = reconcile_discovery_observation_history_by_source(
        database.pool(),
        &harness.observations[observation_start..],
        false,
    )
    .await?;
    assert_eq!(
        scoped_summary.active_edge_count, 1,
        "scoped summaries must report the source-wide surviving active edge count"
    );

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

#[tokio::test]
async fn ens_v2_unregister_persists_terminal_subregistry_discovery_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let registry = "0x00000000000000000000000000000000000000aa".to_owned();
    let subregistry = "0x00000000000000000000000000000000000000c1".to_owned();
    let contract_instance_id = Uuid::from_u128(0x1236);
    let token_id = format!("0x{:064x}", 0xa1);
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
        token_id: token_id.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 10, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token_id.clone(),
        subregistry: subregistry.clone(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 11, 0),
    })?;
    reconcile_discovery_observation_history_by_source(database.pool(), &harness.observations, true)
        .await?;

    let active_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM discovery_edges WHERE deactivated_at IS NULL")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(active_before, 1);

    let observation_start = harness.observations.len();
    harness.apply(RegistryObservation::LabelUnregistered {
        token_id,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&registry, contract_instance_id, 12, 0),
    })?;
    reconcile_discovery_observation_history_by_source(
        database.pool(),
        &harness.observations[observation_start..],
        false,
    )
    .await?;

    let persisted_boundary = sqlx::query_as::<_, (i64, Option<i64>, String)>(
        r#"
        SELECT COUNT(*)::BIGINT,
               max(edge.active_to_block_number),
               min(lower(target.address))
        FROM discovery_edges edge
        JOIN contract_instance_addresses target
          ON target.contract_instance_id = edge.to_contract_instance_id
        WHERE edge.edge_kind = $1
        "#,
    )
    .bind(SUBREGISTRY_EDGE_KIND)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        persisted_boundary,
        (1, Some(12), subregistry),
        "unregister must close the persisted discovery interval at its terminal block"
    );
    let active_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM discovery_edges WHERE deactivated_at IS NULL")
            .fetch_one(database.pool())
            .await?;
    assert_eq!(active_after, 0);

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
    harness.apply(RegistryObservation::ParentUpdated {
        parent: registry.clone(),
        label: "alice".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&child_one, child_instance_id, 11, 1),
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
    harness.apply(RegistryObservation::ParentUpdated {
        parent: registry.clone(),
        label: "alice".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(&child_two, child_instance_id, 14, 1),
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

#[test]
fn ens_v2_announced_registry_parent_claim_cannot_spoof_name_surface() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let parent_instance_id = Uuid::from_u128(0x1781);
    let child_instance_id = Uuid::from_u128(0x1782);
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");

    harness.apply(RegistryObservation::RegistryCreated {
        reference: reference(child, child_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 2, 0),
    })?;
    let child_token = format!("0x{:064x}", 1);
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: child_token.clone(),
        labelhash: labelhash("spoof"),
        label: "spoof".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 3, 0),
    })?;

    assert!(!harness.registry_suffix_by_address.contains_key(child));
    assert!(
        !harness
            .states_by_registry_token
            .contains_key(&(child.to_owned(), child_token))
    );
    assert!(
        harness
            .graph_events
            .iter()
            .all(|event| event.logical_name_id.is_none()),
        "an announced child's parent claim must not surface names without a parent-side link"
    );
    assert_eq!(
        harness
            .graph_events
            .iter()
            .filter(|event| event.event_kind == EVENT_KIND_PARENT_CHANGED)
            .count(),
        1,
        "the child-side claim must remain in contract history even though it grants no reachability"
    );

    let parent_token = format!("0x{:064x}", 2);
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: parent_token.clone(),
        labelhash: labelhash("victim"),
        label: "victim".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 4, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token,
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 5, 0),
    })?;
    harness
        .registry_suffix_by_address
        .insert(child.to_owned(), "stale.eth".to_owned());
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 6, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"victim.eth".to_owned()),
        "a child ParentUpdated event must still correct its suffix once the parent links it"
    );

    Ok(())
}

#[test]
fn ens_v2_bidirectional_parent_claim_selects_one_alias_and_moves_binding() -> Result<()> {
    let parent_a = "0x00000000000000000000000000000000000000aa";
    let parent_b = "0x00000000000000000000000000000000000000ab";
    let child = "0x00000000000000000000000000000000000000bb";
    let grandchild = "0x00000000000000000000000000000000000000cc";
    let resolver = "0x00000000000000000000000000000000000000dd";
    let parent_a_instance_id = Uuid::from_u128(0x1795);
    let parent_b_instance_id = Uuid::from_u128(0x1796);
    let child_instance_id = Uuid::from_u128(0x1797);
    let grandchild_instance_id = Uuid::from_u128(0x179d);
    let mut harness = RegistryHarness::new(parent_a, parent_a_instance_id, "eth");
    harness.root_registry_addresses.insert(parent_b.to_owned());
    harness
        .registry_suffix_by_address
        .insert(parent_b.to_owned(), "alt".to_owned());
    harness
        .registry_contract_by_address
        .insert(parent_b.to_owned(), parent_b_instance_id);
    harness
        .registry_contract_by_address
        .insert(grandchild.to_owned(), grandchild_instance_id);

    for (parent, instance_id, token_id, label, block_number) in [
        (
            parent_a,
            parent_a_instance_id,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001",
            "Alpha",
            1,
        ),
        (
            parent_b,
            parent_b_instance_id,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000001",
            "beta",
            3,
        ),
    ] {
        harness.apply(RegistryObservation::LabelRegistered {
            token_id: token_id.to_owned(),
            labelhash: labelhash(label),
            label: label.to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(parent, instance_id, block_number, 0),
        })?;
        harness.apply(RegistryObservation::SubregistryUpdated {
            token_id: token_id.to_owned(),
            subregistry: child.to_owned(),
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(parent, instance_id, block_number + 1, 0),
        })?;
    }
    assert!(
        !harness.registry_suffix_by_address.contains_key(child),
        "parent pointers alone must not choose between aliases"
    );

    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent_a.to_owned(),
        label: "Alpha".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 5, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"alpha.eth".to_owned()),
        "the canonical suffix must use normalized text while the topology key stays raw"
    );
    let initial_parent_event = harness
        .graph_events
        .iter()
        .rfind(|event| event.event_kind == EVENT_KIND_PARENT_CHANGED)
        .context("initial mixed-case parent claim history")?;
    assert_eq!(initial_parent_event.after_state["label"], "Alpha");
    assert_eq!(
        initial_parent_event.after_state["registry_name"],
        "alpha.eth"
    );
    let first_child_token = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccc00000001";
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: first_child_token.to_owned(),
        labelhash: labelhash("first"),
        label: "first".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 6, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: first_child_token.to_owned(),
        upstream_resource: format!("0x{:064x}", 0xf1a57),
        reference: reference(child, child_instance_id, 6, 1),
    })?;
    harness.apply(RegistryObservation::ResolverUpdated {
        token_id: first_child_token.to_owned(),
        resolver: resolver.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 6, 2),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: first_child_token.to_owned(),
        subregistry: grandchild.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 6, 3),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: child.to_owned(),
        label: "first".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(grandchild, grandchild_instance_id, 6, 4),
    })?;
    let old_binding_id = harness
        .states_by_registry_token
        .get(&(child.to_owned(), first_child_token.to_owned()))
        .and_then(|state| state.resource.as_ref())
        .map(|link| link.surface_binding_id)
        .context("first alias binding")?;

    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent_b.to_owned(),
        label: "beta".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 7, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"beta.alt".to_owned()),
        "the child's current claim must select the canonical alias"
    );
    assert_eq!(
        harness.registry_suffix_by_address.get(grandchild),
        Some(&"first.beta.alt".to_owned()),
        "descendant suffixes must move with the selected canonical ancestor alias"
    );
    let moved_state = harness
        .states_by_registry_token
        .get(&(child.to_owned(), first_child_token.to_owned()))
        .context("moved child state")?;
    assert_eq!(
        moved_state.full_name, "first.beta.alt",
        "moving the canonical claim must move an already-linked name"
    );
    assert_ne!(
        moved_state
            .resource
            .as_ref()
            .context("moved resource link")?
            .surface_binding_id,
        old_binding_id,
        "the moved name needs a distinct current surface-binding epoch"
    );
    assert!(
        harness.closed_bindings.contains_key(&old_binding_id),
        "moving the claim must close the old canonical surface binding"
    );
    assert!(harness.graph_events.iter().any(|event| {
        event.event_kind == EVENT_KIND_REGISTRATION_RELEASED
            && event.logical_name_id.as_deref() == Some("ens:first.alpha.eth")
            && event.after_state["terminal_reason"] == "registry_name_binding_changed"
    }));
    let moved_link = moved_state
        .resource
        .as_ref()
        .context("moved resource link")?;
    assert!(
        build_resource_events(moved_state, moved_link)
            .iter()
            .any(|event| {
                event.event_kind == EVENT_KIND_REGISTRATION_GRANTED
                    && event.logical_name_id.as_deref() == Some("ens:first.beta.alt")
            })
    );
    assert!(harness.graph_events.iter().any(|event| {
        event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED
            && event.logical_name_id.as_deref() == Some("ens:first.beta.alt")
            && event.after_state["to_contract_instance_id"] == grandchild_instance_id.to_string()
            && event.after_state["source_event"] == "ParentUpdated"
    }));
    assert!(harness.graph_events.iter().any(|event| {
        event.event_kind == EVENT_KIND_RESOLVER_CHANGED
            && event.logical_name_id.as_deref() == Some("ens:first.beta.alt")
            && event.after_state["resolver"] == resolver
            && event.after_state["source_event"] == "ParentUpdated"
    }));
    assert!(harness.graph_events.iter().any(|event| {
        event.event_kind == EVENT_KIND_PARENT_CHANGED
            && event.after_state["registry_contract_instance_id"]
                == grandchild_instance_id.to_string()
            && event.after_state["registry_name"] == "first.beta.alt"
    }));
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddd00000001".to_owned(),
        labelhash: labelhash("second"),
        label: "second".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 8, 0),
    })?;

    let surfaced = harness
        .graph_events
        .iter()
        .filter_map(|event| event.logical_name_id.as_deref())
        .collect::<BTreeSet<_>>();
    assert!(surfaced.contains("ens:first.alpha.eth"));
    assert!(surfaced.contains("ens:second.beta.alt"));
    assert!(!surfaced.contains("ens:second.alpha.eth"));
    let moved = harness
        .graph_events
        .iter()
        .rfind(|event| event.event_kind == EVENT_KIND_PARENT_CHANGED)
        .context("moved parent claim history")?;
    assert_eq!(moved.before_state["registry_name"], "alpha.eth");
    assert_eq!(moved.after_state["registry_name"], "beta.alt");

    Ok(())
}

#[test]
fn ens_v2_bidirectional_binding_retracts_when_either_side_breaks() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let other_parent = "0x00000000000000000000000000000000000000ab";
    let child = "0x00000000000000000000000000000000000000bb";
    let parent_instance_id = Uuid::from_u128(0x1798);
    let child_instance_id = Uuid::from_u128(0x1799);
    let token_id = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");
    harness
        .root_registry_addresses
        .insert(other_parent.to_owned());
    harness
        .registry_suffix_by_address
        .insert(other_parent.to_owned(), "alt".to_owned());

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token_id.to_owned(),
        labelhash: labelhash("victim"),
        label: "victim".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token_id.to_owned(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 2, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 3, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"victim.eth".to_owned())
    );
    let child_token = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccc00000001";
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: child_token.to_owned(),
        labelhash: labelhash("leaf"),
        label: "leaf".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 3, 1),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: child_token.to_owned(),
        upstream_resource: format!("0x{:064x}", 0x1ea5),
        reference: reference(child, child_instance_id, 3, 2),
    })?;
    let first_link = harness
        .states_by_registry_token
        .get(&(child.to_owned(), child_token.to_owned()))
        .and_then(|state| state.resource.as_ref())
        .cloned()
        .context("linked child binding")?;

    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token_id.to_owned(),
        subregistry: ZERO_ADDRESS.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 4, 0),
    })?;
    assert!(!harness.registry_suffix_by_address.contains_key(child));
    let detached_state = harness
        .states_by_registry_token
        .get(&(child.to_owned(), child_token.to_owned()))
        .context("detached child state")?;
    assert!(!detached_state.name_reachable);
    assert!(
        harness
            .closed_bindings
            .contains_key(&first_link.surface_binding_id),
        "parent unlink must close the existing child-name binding"
    );
    assert!(
        harness
            .linked_resource_states
            .get(&first_link.resource_id)
            .is_some_and(|state| !state.name_reachable),
        "the resource history must remain retained without becoming a current name binding"
    );
    harness.apply(RegistryObservation::ResolverUpdated {
        token_id: child_token.to_owned(),
        resolver: "0x0000000000000000000000000000000000000e50".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 4, 1),
    })?;
    let detached_resolver_event = harness
        .graph_events
        .iter()
        .rfind(|event| event.event_kind == EVENT_KIND_RESOLVER_CHANGED)
        .context("detached resolver history")?;
    assert!(
        detached_resolver_event.logical_name_id.is_none(),
        "events observed while detached must not remain anchored to the old canonical name"
    );
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token_id.to_owned(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 5, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"victim.eth".to_owned()),
        "restoring the parent pointer must reactivate the unchanged matching child claim"
    );
    let restored_state = harness
        .states_by_registry_token
        .get(&(child.to_owned(), child_token.to_owned()))
        .context("restored child state")?;
    assert!(restored_state.name_reachable);
    assert_ne!(
        restored_state
            .resource
            .as_ref()
            .context("restored child link")?
            .surface_binding_id,
        first_link.surface_binding_id,
        "returning to the same canonical name must open a new binding epoch"
    );

    harness.apply(RegistryObservation::ParentUpdated {
        parent: other_parent.to_owned(),
        label: "elsewhere".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 6, 0),
    })?;
    assert!(
        !harness.registry_suffix_by_address.contains_key(child),
        "a child re-claim must retract the old suffix while the old parent still points back"
    );
    let rejected = harness
        .graph_events
        .iter()
        .rfind(|event| event.event_kind == EVENT_KIND_PARENT_CHANGED)
        .context("rejected re-claim history")?;
    assert_eq!(rejected.before_state["registry_name"], "victim.eth");
    assert!(rejected.after_state["registry_name"].is_null());

    Ok(())
}

#[test]
fn ens_v2_detached_replacement_cannot_resurrect_stale_registration() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let parent_instance_id = Uuid::from_u128(0x179e);
    let child_instance_id = Uuid::from_u128(0x179f);
    let parent_token = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";
    let old_child_token = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000001";
    let replacement_child_token =
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccc00000001";
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: parent_token.to_owned(),
        labelhash: labelhash("branch"),
        label: "branch".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token.to_owned(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 2, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "branch".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 3, 0),
    })?;
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: old_child_token.to_owned(),
        labelhash: labelhash("Foo"),
        label: "Foo".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 4, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: old_child_token.to_owned(),
        upstream_resource: format!("0x{:064x}", 0x1ea7),
        reference: reference(child, child_instance_id, 4, 1),
    })?;
    let old_binding_id = harness
        .states_by_registry_token
        .get(&(child.to_owned(), old_child_token.to_owned()))
        .and_then(|state| state.resource.as_ref())
        .map(|link| link.surface_binding_id)
        .context("old reachable child binding")?;

    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token.to_owned(),
        subregistry: ZERO_ADDRESS.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 5, 0),
    })?;
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: replacement_child_token.to_owned(),
        labelhash: labelhash("foo"),
        label: "foo".to_owned(),
        owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 6, 0),
    })?;
    assert!(
        !harness
            .states_by_registry_token
            .contains_key(&(child.to_owned(), old_child_token.to_owned())),
        "a detached replacement must retire the stale reachable token"
    );
    assert!(
        !harness
            .states_by_registry_token
            .contains_key(&(child.to_owned(), replacement_child_token.to_owned())),
        "a pre-link replacement remains outside reachable name history"
    );

    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token.to_owned(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 7, 0),
    })?;
    assert!(
        harness
            .states_by_registry_token
            .keys()
            .all(|(registry, _)| registry != child),
        "relinking must not resurrect the token replaced while detached"
    );
    assert!(harness.closed_bindings.contains_key(&old_binding_id));
    assert!(harness.graph_events.iter().all(|event| {
        event.event_kind != EVENT_KIND_REGISTRATION_GRANTED
            || event.after_state["token_id"] != replacement_child_token
    }));

    Ok(())
}

#[test]
fn ens_v2_replacement_retires_raw_labels_with_the_same_normalized_name() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa";
    let contract_instance_id = Uuid::from_u128(0x17a0);
    let old_token = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";
    let replacement_token = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000001";
    let mut harness = RegistryHarness::new(registry, contract_instance_id, "eth");

    for (token_id, label, block_number) in [(old_token, "Foo", 1), (replacement_token, "foo", 2)] {
        harness.apply(RegistryObservation::LabelRegistered {
            token_id: token_id.to_owned(),
            labelhash: labelhash(label),
            label: label.to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(registry, contract_instance_id, block_number, 0),
        })?;
    }

    assert!(
        !harness
            .states_by_registry_token
            .contains_key(&(registry.to_owned(), old_token.to_owned())),
        "a raw-distinct predecessor with the same normalized name must be retired"
    );
    let current = harness
        .states_by_registry_token
        .get(&(registry.to_owned(), replacement_token.to_owned()))
        .context("normalized replacement state")?;
    assert_eq!(current.name.logical_name_id, "ens:foo.eth");
    assert_eq!(harness.states_by_registry_token.len(), 1);

    Ok(())
}

#[test]
fn ens_v2_normalized_label_replacement_retires_the_old_raw_topology_pointer() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let child_one = "0x00000000000000000000000000000000000000b1";
    let child_two = "0x00000000000000000000000000000000000000b2";
    let parent_instance_id = Uuid::from_u128(0x17a1);
    let child_one_instance_id = Uuid::from_u128(0x17a2);
    let child_two_instance_id = Uuid::from_u128(0x17a3);
    let old_token = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";
    let replacement_token = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000001";
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");
    harness
        .registry_contract_by_address
        .insert(child_one.to_owned(), child_one_instance_id);
    harness
        .registry_contract_by_address
        .insert(child_two.to_owned(), child_two_instance_id);

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: old_token.to_owned(),
        labelhash: labelhash("Foo"),
        label: "Foo".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: old_token.to_owned(),
        subregistry: child_one.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 2, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "Foo".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child_one, child_one_instance_id, 3, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child_one),
        Some(&"foo.eth".to_owned())
    );

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: replacement_token.to_owned(),
        labelhash: labelhash("foo"),
        label: "foo".to_owned(),
        owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 4, 0),
    })?;
    assert!(
        !harness.registry_suffix_by_address.contains_key(child_one),
        "the superseded raw-label pointer must stop authorizing its child"
    );
    assert!(
        !harness
            .current_subregistry_by_parent_label
            .contains_key(&(parent.to_owned(), "Foo".to_owned()))
    );
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: replacement_token.to_owned(),
        subregistry: child_two.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 5, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "foo".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child_two, child_two_instance_id, 6, 0),
    })?;
    assert!(!harness.registry_suffix_by_address.contains_key(child_one));
    assert_eq!(
        harness.registry_suffix_by_address.get(child_two),
        Some(&"foo.eth".to_owned()),
        "only the newest normalized-name winner may carry the suffix"
    );

    let stale_leaf_token = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccc00000001";
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: stale_leaf_token.to_owned(),
        labelhash: labelhash("leaf"),
        label: "leaf".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child_one, child_one_instance_id, 7, 0),
    })?;
    assert!(
        !harness
            .states_by_registry_token
            .contains_key(&(child_one.to_owned(), stale_leaf_token.to_owned())),
        "the superseded child must not mint a fresh binding under the shared normalized suffix"
    );

    Ok(())
}

#[test]
fn ens_v2_topology_survives_until_an_unreachable_parent_becomes_named() -> Result<()> {
    let root = "0x00000000000000000000000000000000000000aa";
    let parent = "0x00000000000000000000000000000000000000bb";
    let child = "0x00000000000000000000000000000000000000cc";
    let root_instance_id = Uuid::from_u128(0x179a);
    let parent_instance_id = Uuid::from_u128(0x179b);
    let child_instance_id = Uuid::from_u128(0x179c);
    let parent_token = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000001";
    let root_token = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";
    let mut harness = RegistryHarness::new(root, root_instance_id, "eth");
    harness
        .registry_contract_by_address
        .insert(parent.to_owned(), parent_instance_id);
    harness
        .registry_contract_by_address
        .insert(child.to_owned(), child_instance_id);

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: parent_token.to_owned(),
        labelhash: labelhash("nested"),
        label: "nested".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token.to_owned(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 2, 0),
    })?;
    assert!(
        !harness.registry_suffix_by_address.contains_key(parent)
            && !harness.registry_suffix_by_address.contains_key(child)
    );

    harness.apply(RegistryObservation::ParentUpdated {
        parent: root.to_owned(),
        label: "branch".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 3, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "nested".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 4, 0),
    })?;
    assert!(
        !harness.registry_suffix_by_address.contains_key(parent)
            && !harness.registry_suffix_by_address.contains_key(child),
        "child-side claims alone must remain unreachable"
    );

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: root_token.to_owned(),
        labelhash: labelhash("branch"),
        label: "branch".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(root, root_instance_id, 5, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: root_token.to_owned(),
        subregistry: parent.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(root, root_instance_id, 6, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"nested.branch.eth".to_owned()),
        "the earlier parent topology must remain available after its registry becomes reachable"
    );
    let child_parent_event = harness
        .graph_events
        .iter()
        .rfind(|event| {
            event.event_kind == EVENT_KIND_PARENT_CHANGED
                && event.after_state["registry_contract_instance_id"]
                    == child_instance_id.to_string()
        })
        .context("reactivated child ParentChanged history")?;
    assert_eq!(
        child_parent_event.after_state["registry_name"], "nested.branch.eth",
        "ancestor activation must publish the retained descendant claim without another ParentUpdated"
    );

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccc00000001".to_owned(),
        labelhash: labelhash("leaf"),
        label: "leaf".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 7, 0),
    })?;
    assert!(harness.graph_events.iter().any(|event| {
        event.logical_name_id.as_deref() == Some("ens:leaf.nested.branch.eth")
            && event.event_kind == EVENT_KIND_REGISTRATION_GRANTED
    }));

    Ok(())
}

#[test]
fn ens_v2_parent_claim_requires_current_subregistry_mapping() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let stale_child = "0x00000000000000000000000000000000000000bb";
    let current_child = "0x00000000000000000000000000000000000000cc";
    let parent_instance_id = Uuid::from_u128(0x1783);
    let stale_child_instance_id = Uuid::from_u128(0x1784);
    let current_child_instance_id = Uuid::from_u128(0x178b);
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");
    let first_token = format!("0x{:064x}", 1);
    let second_token = format!("0x{:064x}", 2);

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: first_token.clone(),
        labelhash: labelhash("victim"),
        label: "victim".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: first_token,
        subregistry: stale_child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 2, 0),
    })?;

    harness
        .registry_suffix_by_address
        .insert(parent.to_owned(), "renamed.eth".to_owned());
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: second_token.clone(),
        labelhash: labelhash("victim"),
        label: "victim".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 3, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: second_token,
        subregistry: current_child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 4, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(current_child, current_child_instance_id, 4, 1),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(stale_child, stale_child_instance_id, 5, 0),
    })?;

    assert!(
        !harness.registry_suffix_by_address.contains_key(stale_child),
        "a retained same-label state must not override the parent's current subregistry pointer"
    );
    assert_eq!(
        harness.registry_suffix_by_address.get(current_child),
        Some(&"victim.renamed.eth".to_owned())
    );

    Ok(())
}

#[test]
fn ens_v2_parent_claim_suffix_is_removed_when_parent_unlinks_after_resuffix() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let parent_instance_id = Uuid::from_u128(0x1785);
    let child_instance_id = Uuid::from_u128(0x1786);
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");
    let parent_token = format!("0x{:064x}", 1);

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: parent_token.clone(),
        labelhash: labelhash("victim"),
        label: "victim".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token.clone(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 2, 0),
    })?;
    harness
        .registry_suffix_by_address
        .insert(parent.to_owned(), "renamed.eth".to_owned());
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 3, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"victim.renamed.eth".to_owned())
    );

    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token,
        subregistry: ZERO_ADDRESS.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 4, 0),
    })?;

    assert!(
        !harness.registry_suffix_by_address.contains_key(child),
        "unlinking must remove a claim-written suffix even after the parent was re-suffixed"
    );

    Ok(())
}

#[test]
fn ens_v2_unlinking_one_label_preserves_same_child_other_current_suffix() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let parent_instance_id = Uuid::from_u128(0x178a);
    let child_instance_id = Uuid::from_u128(0x178c);
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");
    let alpha_token =
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001".to_owned();
    let beta_token =
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000001".to_owned();

    for (block_number, token_id, label) in [
        (1, alpha_token.clone(), "alpha"),
        (3, beta_token.clone(), "beta"),
    ] {
        harness.apply(RegistryObservation::LabelRegistered {
            token_id: token_id.clone(),
            labelhash: labelhash(label),
            label: label.to_owned(),
            owner: "0x0000000000000000000000000000000000000a11".to_owned(),
            expiry: 1_900_000_000,
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(parent, parent_instance_id, block_number, 0),
        })?;
        harness.apply(RegistryObservation::SubregistryUpdated {
            token_id,
            subregistry: child.to_owned(),
            sender: "0x0000000000000000000000000000000000000dad".to_owned(),
            reference: reference(parent, parent_instance_id, block_number + 1, 0),
        })?;
    }
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "beta".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 5, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"beta.eth".to_owned())
    );

    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: alpha_token,
        subregistry: ZERO_ADDRESS.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 6, 0),
    })?;

    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"beta.eth".to_owned()),
        "unlinking alpha must not remove the suffix supplied by the surviving beta link"
    );

    Ok(())
}

#[test]
fn ens_v2_parent_claim_detachment_clears_suffix_and_preserves_history() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let parent_instance_id = Uuid::from_u128(0x1787);
    let child_instance_id = Uuid::from_u128(0x1788);
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");
    let parent_token = format!("0x{:064x}", 1);

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: parent_token.clone(),
        labelhash: labelhash("victim"),
        label: "victim".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token,
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 2, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 3, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"victim.eth".to_owned())
    );

    harness.apply(RegistryObservation::ParentUpdated {
        parent: ZERO_ADDRESS.to_owned(),
        label: String::new(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 4, 0),
    })?;

    assert!(
        !harness.registry_suffix_by_address.contains_key(child),
        "a child changing its current parent claim must lose its previous suffix"
    );
    let parent_events = harness
        .graph_events
        .iter()
        .filter(|event| event.event_kind == EVENT_KIND_PARENT_CHANGED)
        .collect::<Vec<_>>();
    assert_eq!(
        parent_events.len(),
        2,
        "every ParentUpdated claim must remain visible in contract history"
    );
    assert!(parent_events[1].after_state["parent"].is_null());
    assert!(parent_events[1].after_state["registry_name"].is_null());

    Ok(())
}

#[test]
fn ens_v2_parent_claim_rejects_expired_current_subregistry_mapping() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let parent_instance_id = Uuid::from_u128(0x1789);
    let child_instance_id = Uuid::from_u128(0x1790);
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");
    let parent_token = format!("0x{:064x}", 1);

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: parent_token.clone(),
        labelhash: labelhash("victim"),
        label: "victim".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_717_172_705,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token,
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 2, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 6, 0),
    })?;

    assert!(
        !harness.registry_suffix_by_address.contains_key(child),
        "getSubregistry returns zero once the parent label expires"
    );
    assert_eq!(
        harness
            .graph_events
            .iter()
            .filter(|event| event.event_kind == EVENT_KIND_PARENT_CHANGED)
            .count(),
        1,
        "the rejected claim must remain visible in contract history"
    );
    assert!(
        harness
            .graph_events
            .iter()
            .find(|event| event.event_kind == EVENT_KIND_PARENT_CHANGED)
            .expect("rejected claim history")
            .after_state["registry_name"]
            .is_null(),
        "a rejected claim must not publish an authoritative registry name"
    );

    harness.apply(RegistryObservation::ExpiryUpdated {
        token_id: format!("0x{:064x}", 1),
        new_expiry: 2_000_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 7, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 8, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"victim.eth".to_owned()),
        "renewing the current parent label must make its unchanged pointer valid again"
    );

    Ok(())
}

#[test]
fn ens_v2_parent_claim_stops_authorizing_names_after_parent_label_expiry() -> Result<()> {
    let parent = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let parent_instance_id = Uuid::from_u128(0x1791);
    let child_instance_id = Uuid::from_u128(0x1792);
    let mut harness = RegistryHarness::new(parent, parent_instance_id, "eth");
    let parent_token = format!("0x{:064x}", 1);
    let live_child_token = format!("0x{:064x}", 2);
    let late_child_token = format!("0x{:064x}", 3);

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: parent_token.clone(),
        labelhash: labelhash("victim"),
        label: "victim".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_717_172_705,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: parent_token,
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(parent, parent_instance_id, 2, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: parent.to_owned(),
        label: "victim".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 3, 0),
    })?;
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"victim.eth".to_owned())
    );

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: live_child_token.clone(),
        labelhash: labelhash("live"),
        label: "live".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 4, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: live_child_token.clone(),
        upstream_resource: format!("0x{:064x}", 0x1eaf),
        reference: reference(child, child_instance_id, 4, 1),
    })?;
    let live_binding_id = harness
        .states_by_registry_token
        .get(&(child.to_owned(), live_child_token.clone()))
        .and_then(|state| state.resource.as_ref())
        .map(|link| link.surface_binding_id)
        .context("live pre-expiry binding")?;

    harness.apply(RegistryObservation::ResolverUpdated {
        token_id: live_child_token,
        resolver: "0x0000000000000000000000000000000000000e50".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 6, 0),
    })?;
    assert!(
        !harness.registry_suffix_by_address.contains_key(child),
        "the first later event must retract a suffix whose parent label has expired"
    );
    assert!(
        harness.closed_bindings.contains_key(&live_binding_id),
        "time-based expiry revalidation must close an existing descendant binding"
    );
    let post_expiry_resolver = harness
        .graph_events
        .iter()
        .rfind(|event| event.event_kind == EVENT_KIND_RESOLVER_CHANGED)
        .context("post-expiry resolver history")?;
    assert!(post_expiry_resolver.logical_name_id.is_none());

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: late_child_token.clone(),
        labelhash: labelhash("spoof"),
        label: "spoof".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, child_instance_id, 7, 0),
    })?;

    assert!(
        !harness
            .states_by_registry_token
            .contains_key(&(child.to_owned(), late_child_token)),
        "a suffix granted before expiry must not authorize later child names"
    );
    assert!(
        harness
            .graph_events
            .iter()
            .all(|event| event.logical_name_id.as_deref() != Some("ens:spoof.victim.eth"))
    );

    Ok(())
}

#[test]
fn ens_v2_name_derivation_rechecks_every_ancestor_link_expiry() {
    let root = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let grandchild = "0x00000000000000000000000000000000000000cc";
    let suffixes = HashMap::from([
        (root.to_owned(), "eth".to_owned()),
        (child.to_owned(), "branch.eth".to_owned()),
        (grandchild.to_owned(), "nested.branch.eth".to_owned()),
    ]);
    let roots = HashSet::from([root.to_owned()]);
    let mut current_links = HashMap::from([
        (
            (root.to_owned(), "branch".to_owned()),
            CurrentSubregistryLink {
                subregistry: child.to_owned(),
                expiry: Some(1_717_172_705),
            },
        ),
        (
            (child.to_owned(), "nested".to_owned()),
            CurrentSubregistryLink {
                subregistry: grandchild.to_owned(),
                expiry: Some(1_900_000_000),
            },
        ),
    ]);
    let current_claims = HashMap::from([
        (
            child.to_owned(),
            CurrentParentClaim {
                parent: root.to_owned(),
                label: "branch".to_owned(),
            },
        ),
        (
            grandchild.to_owned(),
            CurrentParentClaim {
                parent: child.to_owned(),
                label: "nested".to_owned(),
            },
        ),
    ]);
    let event_ref = reference(grandchild, Uuid::from_u128(0x1793), 6, 0);

    assert_eq!(
        name_under_registry(
            grandchild,
            "spoof",
            &suffixes,
            &roots,
            &current_links,
            &current_claims,
            &event_ref,
        ),
        None,
        "an expired ancestor must make the complete descendant path unreachable"
    );

    current_links
        .get_mut(&(root.to_owned(), "branch".to_owned()))
        .expect("root-to-child link")
        .expiry = Some(2_000_000_000);
    assert_eq!(
        name_under_registry(
            grandchild,
            "spoof",
            &suffixes,
            &roots,
            &current_links,
            &current_claims,
            &event_ref,
        ),
        Some("spoof.nested.branch.eth".to_owned()),
        "renewing the ancestor must restore the unchanged descendant path"
    );
}

#[test]
fn ens_v2_terminal_registry_state_compacts_tokens_aliases_and_suffixes() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let contract_instance_id = Uuid::from_u128(0x1771);
    let mut harness = RegistryHarness::new(registry, contract_instance_id, "eth");
    let mut current_token = format!("0x{:064x}", 1);
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: current_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: current_token.clone(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 0),
    })?;
    harness.apply(RegistryObservation::ParentUpdated {
        parent: registry.to_owned(),
        label: "alice".to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(child, Uuid::from_u128(0x1772), 2, 1),
    })?;

    for block_number in 3..=12 {
        let next_token = format!("0x{block_number:064x}");
        harness.apply(RegistryObservation::TokenRegenerated {
            old_token_id: current_token,
            new_token_id: next_token.clone(),
            reference: reference(registry, contract_instance_id, block_number, 0),
        })?;
        current_token = next_token;
        assert_eq!(
            harness.token_aliases.len(),
            1,
            "regeneration must retain only the current token alias"
        );
    }
    assert_eq!(harness.states_by_registry_token.len(), 1);
    assert_eq!(
        harness.registry_suffix_by_address.get(child),
        Some(&"alice.eth".to_owned())
    );

    harness.apply(RegistryObservation::LabelUnregistered {
        token_id: current_token,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 13, 0),
    })?;
    assert!(harness.states_by_registry_token.is_empty());
    assert!(harness.state_keys_by_registry_namehash.is_empty());
    assert!(harness.token_aliases.is_empty());
    assert!(harness.current_token_alias_by_canonical_key.is_empty());
    assert!(!harness.registry_suffix_by_address.contains_key(child));
    Ok(())
}

#[test]
fn ens_v2_reserved_unregister_preserves_reserved_before_state() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa";
    let contract_instance_id = Uuid::from_u128(0x1776);
    let token = format!("0x{:064x}", 1);
    let mut harness = RegistryHarness::new(registry, contract_instance_id, "eth");
    harness.apply(RegistryObservation::LabelReserved {
        token_id: token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::LabelUnregistered {
        token_id: token,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 0),
    })?;
    let released = harness
        .graph_events
        .iter()
        .find(|event| event.event_kind == EVENT_KIND_REGISTRATION_RELEASED)
        .context("reserved unregister should emit RegistrationReleased")?;
    assert_eq!(released.before_state["status"], "reserved");
    assert_eq!(released.after_state["status"], "unregistered");
    Ok(())
}

#[test]
fn ens_v2_subregistry_observation_key_is_independent_of_warm_state() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let contract_instance_id = Uuid::from_u128(0x1777);
    let token = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";

    let mut cold = RegistryHarness::new(registry, contract_instance_id, "eth");
    cold.apply(RegistryObservation::SubregistryUpdated {
        token_id: token.to_owned(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 0),
    })?;

    let mut warm = RegistryHarness::new(registry, contract_instance_id, "eth");
    warm.apply(RegistryObservation::LabelRegistered {
        token_id: token.to_owned(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 1, 0),
    })?;
    warm.apply(RegistryObservation::SubregistryUpdated {
        token_id: token.to_owned(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 0),
    })?;

    let cold_key = cold.observations[0].provenance["observation_key"]
        .as_str()
        .context("cold observation should carry a key")?;
    let warm_key = warm.observations[0].provenance["observation_key"]
        .as_str()
        .context("warm observation should carry a key")?;
    assert_eq!(cold_key, warm_key);
    assert!(cold_key.ends_with("00000000"));
    Ok(())
}

#[test]
fn ens_v2_unregister_emits_discovery_tombstones_for_attached_targets() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let resolver = "0x00000000000000000000000000000000000000cc";
    let contract_instance_id = Uuid::from_u128(0x1772);
    let token = format!("0x{:064x}", 1);
    let mut harness = RegistryHarness::new(registry, contract_instance_id, "eth");
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: token.clone(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 0),
    })?;
    harness.apply(RegistryObservation::ResolverUpdated {
        token_id: token.clone(),
        resolver: resolver.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 1),
    })?;

    let observation_start = harness.observations.len();
    harness.apply(RegistryObservation::LabelUnregistered {
        token_id: token,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 3, 0),
    })?;
    let terminal = &harness.observations[observation_start..];
    assert_eq!(
        terminal.len(),
        2,
        "unregister must close both attached discovery roles"
    );
    assert_eq!(
        terminal
            .iter()
            .map(|observation| observation.edge_kind.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([SUBREGISTRY_EDGE_KIND, RESOLVER_EDGE_KIND])
    );
    assert!(terminal.iter().all(|observation| {
        normalize_address(&observation.to_address) == ZERO_ADDRESS
            && observation.active_from_block_number == Some(3)
            && observation.provenance["source_event"] == "LabelUnregistered"
            && observation.provenance["tombstone"] == true
    }));
    let terminal_role_events = harness
        .graph_events
        .iter()
        .filter(|event| {
            matches!(
                event.event_kind.as_str(),
                EVENT_KIND_SUBREGISTRY_CHANGED | EVENT_KIND_RESOLVER_CHANGED
            ) && event.after_state["source_event"] == "LabelUnregistered"
        })
        .collect::<Vec<_>>();
    assert_eq!(terminal_role_events.len(), 2);
    assert!(terminal_role_events.iter().all(|event| {
        event.logical_name_id.as_deref() == Some("ens:alice.eth")
            && event.resource_id.is_none()
            && (event.after_state["subregistry"].is_null()
                || event.after_state["resolver"].is_null())
    }));
    Ok(())
}

#[test]
fn ens_v2_replacement_registration_retires_prior_discovery_targets() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa";
    let child = "0x00000000000000000000000000000000000000bb";
    let resolver = "0x00000000000000000000000000000000000000cc";
    let contract_instance_id = Uuid::from_u128(0x1773);
    let first_token = format!("0x{:064x}", 1);
    let second_token = format!("0x{:064x}", 2);
    let mut harness = RegistryHarness::new(registry, contract_instance_id, "eth");
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: first_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::SubregistryUpdated {
        token_id: first_token.clone(),
        subregistry: child.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 0),
    })?;
    harness.apply(RegistryObservation::ResolverUpdated {
        token_id: first_token,
        resolver: resolver.to_owned(),
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 1),
    })?;

    let observation_start = harness.observations.len();
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: second_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
        expiry: 2_000_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 3, 0),
    })?;
    let terminal = &harness.observations[observation_start..];
    assert_eq!(
        terminal.len(),
        2,
        "replacement registration must close both prior discovery roles"
    );
    assert!(terminal.iter().all(|observation| {
        normalize_address(&observation.to_address) == ZERO_ADDRESS
            && observation.active_from_block_number == Some(3)
            && observation.provenance["source_event"] == "LabelRegistered"
            && observation.provenance["tombstone"] == true
    }));
    assert_eq!(
        harness.states_by_registry_token.len(),
        1,
        "replacement registration must retire the prior token state"
    );
    assert!(
        harness
            .states_by_registry_token
            .contains_key(&(registry.to_owned(), second_token))
    );
    Ok(())
}

#[test]
fn ens_v2_replacement_reservation_closes_prior_surface_binding() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa";
    let contract_instance_id = Uuid::from_u128(0x1774);
    let first_token = format!("0x{:064x}", 1);
    let second_token = format!("0x{:064x}", 2);
    let upstream_resource = format!("0x{:064x}", 0xa11ce);
    let mut harness = RegistryHarness::new(registry, contract_instance_id, "eth");
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: first_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: first_token,
        upstream_resource,
        reference: reference(registry, contract_instance_id, 1, 1),
    })?;
    assert!(harness.closed_bindings.is_empty());

    harness.apply(RegistryObservation::LabelReserved {
        token_id: second_token,
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        expiry: 2_000_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 0),
    })?;

    assert_eq!(
        harness.closed_bindings.len(),
        1,
        "replacement reservation must close the prior readable resource binding"
    );
    assert!(
        harness
            .closed_bindings
            .values()
            .all(|binding| binding.active_to.is_some())
    );
    let surface_unbound = harness
        .graph_events
        .iter()
        .find(|event| {
            event.event_kind == EVENT_KIND_SURFACE_UNBOUND
                && event.after_state["source_event"] == "LabelReserved"
        })
        .context("replacement reservation must emit orphan-repairable SurfaceUnbound evidence")?;
    assert_eq!(
        surface_unbound.logical_name_id.as_deref(),
        Some("ens:alice.eth")
    );
    assert!(surface_unbound.resource_id.is_some());
    Ok(())
}

#[test]
fn ens_v2_replacement_registration_defers_prior_close_to_successor_binding() -> Result<()> {
    let registry = "0x00000000000000000000000000000000000000aa";
    let contract_instance_id = Uuid::from_u128(0x1775);
    let first_token = format!("0x{:064x}", 1);
    let second_token = format!("0x{:064x}", 2);
    let mut harness = RegistryHarness::new(registry, contract_instance_id, "eth");
    harness.apply(RegistryObservation::LabelRegistered {
        token_id: first_token.clone(),
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000a11".to_owned(),
        expiry: 1_900_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 1, 0),
    })?;
    harness.apply(RegistryObservation::TokenResource {
        token_id: first_token,
        upstream_resource: format!("0x{:064x}", 0xa11ce),
        reference: reference(registry, contract_instance_id, 1, 1),
    })?;

    harness.apply(RegistryObservation::LabelRegistered {
        token_id: second_token,
        labelhash: labelhash("alice"),
        label: "alice".to_owned(),
        owner: "0x0000000000000000000000000000000000000b0b".to_owned(),
        expiry: 2_000_000_000,
        sender: "0x0000000000000000000000000000000000000dad".to_owned(),
        reference: reference(registry, contract_instance_id, 2, 0),
    })?;

    assert!(
        harness.closed_bindings.is_empty(),
        "replacement registration must let its following TokenResource binding close the prior epoch without a gap"
    );
    Ok(())
}

struct RegistryHarness {
    registry_suffix_by_address: HashMap<String, String>,
    root_registry_addresses: HashSet<String>,
    registry_contract_by_address: HashMap<String, Uuid>,
    current_subregistry_by_parent_label: HashMap<(String, String), CurrentSubregistryLink>,
    current_parent_claim_by_registry: HashMap<String, CurrentParentClaim>,
    entry_topology_by_registry_token: HashMap<(String, String), RegistryEntryTopology>,
    states_by_registry_token: BTreeMap<(String, String), RegistryNameState>,
    state_keys_by_registry_namehash: HashMap<(String, String), BTreeSet<(String, String)>>,
    linked_resource_states: BTreeMap<Uuid, RegistryNameState>,
    retired_binding_states: BTreeMap<Uuid, RegistryNameState>,
    closed_bindings: BTreeMap<Uuid, SurfaceBinding>,
    token_aliases: HashMap<(String, String), (String, String)>,
    current_token_alias_by_canonical_key: HashMap<(String, String), (String, String)>,
    observations: Vec<DiscoveryObservation>,
    graph_events: Vec<NormalizedEvent>,
}

impl RegistryHarness {
    fn new(registry: &str, contract_instance_id: Uuid, suffix: &str) -> Self {
        Self {
            registry_suffix_by_address: HashMap::from([(registry.to_owned(), suffix.to_owned())]),
            root_registry_addresses: HashSet::from([registry.to_owned()]),
            registry_contract_by_address: HashMap::from([(
                registry.to_owned(),
                contract_instance_id,
            )]),
            current_subregistry_by_parent_label: HashMap::new(),
            current_parent_claim_by_registry: HashMap::new(),
            entry_topology_by_registry_token: HashMap::new(),
            states_by_registry_token: BTreeMap::new(),
            state_keys_by_registry_namehash: HashMap::new(),
            linked_resource_states: BTreeMap::new(),
            retired_binding_states: BTreeMap::new(),
            closed_bindings: BTreeMap::new(),
            token_aliases: HashMap::new(),
            current_token_alias_by_canonical_key: HashMap::new(),
            observations: Vec::new(),
            graph_events: Vec::new(),
        }
    }

    fn apply(&mut self, observation: RegistryObservation) -> Result<()> {
        let mut context = RegistryObservationContext {
            registry_suffix_by_address: &mut self.registry_suffix_by_address,
            root_registry_addresses: &self.root_registry_addresses,
            registry_contract_by_address: &mut self.registry_contract_by_address,
            current_subregistry_by_parent_label: &mut self.current_subregistry_by_parent_label,
            current_parent_claim_by_registry: &mut self.current_parent_claim_by_registry,
            entry_topology_by_registry_token: &mut self.entry_topology_by_registry_token,
            states_by_registry_token: &mut self.states_by_registry_token,
            state_keys_by_registry_namehash: &mut self.state_keys_by_registry_namehash,
            linked_resource_states: &mut self.linked_resource_states,
            retired_binding_states: &mut self.retired_binding_states,
            closed_bindings: &mut self.closed_bindings,
            token_aliases: &mut self.token_aliases,
            current_token_alias_by_canonical_key: &mut self.current_token_alias_by_canonical_key,
            observations: &mut self.observations,
            graph_events: &mut self.graph_events,
        };
        apply_registry_observation(observation, &mut context)
    }
}

async fn insert_incremental_equivalence_fixture(
    pool: &PgPool,
    chain: &str,
    registry: &str,
    child_registry: &str,
) -> Result<Vec<String>> {
    let resolver = "0x00000000000000000000000000000000000000b1";
    let subregistry = "0x00000000000000000000000000000000000000c1";
    let manifest_id = insert_test_registry_manifest(pool, chain).await?;
    insert_test_resolver_manifest(pool, chain).await?;
    insert_test_registry_contract(
        pool,
        manifest_id,
        "registry",
        Uuid::from_u128(0x1610),
        registry,
        0,
    )
    .await?;
    let child_contract_instance_id = Uuid::from_u128(0x1611);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        VALUES ($1, $2, 'registry', '{}'::JSONB)
        "#,
    )
    .bind(child_contract_instance_id)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            active_from_block_hash,
            source_manifest_id,
            provenance
        )
        VALUES ($1, $2, $3, 9, $4, $5, '{}'::JSONB)
        "#,
    )
    .bind(child_contract_instance_id)
    .bind(chain)
    .bind(normalize_address(child_registry))
    .bind(lifecycle_block_hash(9))
    .bind(manifest_id)
    .execute(pool)
    .await?;

    let block_hashes = (9..=15).map(lifecycle_block_hash).collect::<Vec<_>>();
    let mut blocks = Vec::with_capacity(block_hashes.len());
    for (offset, block_hash) in block_hashes.iter().enumerate() {
        let block_number = 9 + i64::try_from(offset).context("fixture offset exceeds i64")?;
        let mut block = test_raw_block(chain, block_hash, block_number);
        if offset > 0 {
            block.parent_hash = Some(block_hashes[offset - 1].clone());
        }
        blocks.push(block);
    }
    upsert_raw_blocks(pool, &blocks).await?;
    upsert_raw_logs(
        pool,
        &[
            registry_created_raw_log(chain, &block_hashes[0], 9, child_registry),
            label_registered_raw_log(
                chain,
                &block_hashes[0],
                9,
                registry,
                1,
                "parent",
                1,
                "alice",
            ),
            token_resource_raw_log(chain, &block_hashes[0], 9, registry, 2, 1, 101),
            subregistry_updated_raw_log(chain, &block_hashes[0], 9, registry, 3, 1, child_registry),
            parent_updated_raw_log(
                chain,
                &block_hashes[0],
                9,
                child_registry,
                4,
                registry,
                "parent",
            ),
            label_registered_raw_log(
                chain,
                &block_hashes[0],
                9,
                child_registry,
                5,
                "alice",
                10,
                "alice",
            ),
            token_resource_raw_log(chain, &block_hashes[0], 9, child_registry, 6, 10, 110),
            expiry_updated_raw_log(chain, &block_hashes[2], 11, registry, 0, 1, 2_000_000_000),
            resolver_updated_raw_log(chain, &block_hashes[3], 12, registry, 0, 1, resolver),
            subregistry_updated_raw_log(chain, &block_hashes[4], 13, registry, 0, 1, subregistry),
            token_regenerated_raw_log(chain, &block_hashes[5], 14, registry, 0, 1, 2),
            label_unregistered_raw_log(chain, &block_hashes[6], 15, registry, 0, 2),
        ],
    )
    .await?;
    let announcement_source = ens_v2_registry_announcement_discovery_source(chain);
    bigname_manifests::reconcile_scoped_discovery_observation_transitions(
        pool,
        &announcement_source,
        &[vec![DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: normalize_address(registry),
            to_address: normalize_address(child_registry),
            edge_kind: REGISTRY_ANNOUNCEMENT_EDGE_KIND.to_owned(),
            discovery_source: announcement_source.clone(),
            active_from_block_number: Some(9),
            active_from_block_hash: Some(block_hashes[0].clone()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: json!({
                "source": "raw_log",
                "source_event": "RegistryCreated",
                "observation_key": format!("registry-announcement:{}", normalize_address(child_registry)),
                "from_address": normalize_address(registry),
                "to_address": normalize_address(child_registry),
                "chain_id": chain,
                "block_hash": block_hashes[0],
                "block_number": 9,
                "transaction_hash": "0xregistrycreated9",
                "transaction_index": 0,
                "log_index": 0,
                "tombstone": false,
            }),
        }]],
    )
    .await?;
    let discovery_source = ens_v2_subregistry_discovery_source(chain);
    let token_id = format!("0x{:064x}", 1);
    let observation_key = discovery_observation_key(registry, &token_id);
    bigname_manifests::reconcile_scoped_discovery_observation_transitions(
        pool,
        &discovery_source,
        &[vec![DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: normalize_address(registry),
            to_address: normalize_address(child_registry),
            edge_kind: SUBREGISTRY_EDGE_KIND.to_owned(),
            discovery_source: discovery_source.clone(),
            active_from_block_number: Some(9),
            active_from_block_hash: Some(block_hashes[0].clone()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: json!({
                "source": "raw_log",
                "source_event": "SubregistryUpdated",
                "observation_key": observation_key,
                "token_id": token_id,
                "from_address": normalize_address(registry),
                "to_address": normalize_address(child_registry),
                "logical_name_id": "ens:parent.eth",
                "resource_id": Value::Null,
                "chain_id": chain,
                "block_hash": block_hashes[0],
                "block_number": 9,
                "transaction_hash": "0xsubregistry9",
                "transaction_index": 0,
                "log_index": 3,
                "tombstone": false,
            }),
        }]],
    )
    .await?;
    let admitted_child_contract_instance_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT to_contract_instance_id
        FROM discovery_edges
        WHERE discovery_source = $1
          AND lower(provenance ->> 'to_address') = $2
        "#,
    )
    .bind(&discovery_source)
    .bind(normalize_address(child_registry))
    .fetch_one(pool)
    .await?;
    assert_eq!(
        admitted_child_contract_instance_id,
        child_contract_instance_id
    );
    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET active_to_block_number = 13,
            active_to_block_hash = $2,
            deactivated_at = clock_timestamp()
        WHERE discovery_source = $1
          AND to_contract_instance_id = $3
        "#,
    )
    .bind(&discovery_source)
    .bind(&block_hashes[4])
    .bind(admitted_child_contract_instance_id)
    .execute(pool)
    .await?;
    insert_completed_registry_coverage(
        pool,
        chain,
        &[
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, registry),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, child_registry),
            (SOURCE_FAMILY_ENS_V2_REGISTRY_L1, subregistry),
            (SOURCE_FAMILY_ENS_V2_RESOLVER_L1, resolver),
        ],
        0,
        15,
    )
    .await?;
    refresh_test_raw_log_closure_proof(pool, chain, 15).await?;
    Ok(block_hashes)
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
               ($1, 'resolver', 'registry', 'reachable_from_root'),
               ($1, 'registry_announcement', 'registry', 'reachable_from_root')
        "#,
    )
    .bind(manifest_id)
    .execute(pool)
    .await
    .context("failed to insert scoped registry test discovery rules")?;
    establish_test_raw_log_closure_proof(pool, chain).await?;
    Ok(manifest_id)
}

async fn insert_test_root_manifest(pool: &PgPool, chain: &str) -> Result<i64> {
    let mut payload = test_registry_manifest_payload(chain);
    payload["source_family"] = json!(SOURCE_FAMILY_ENS_V2_ROOT_L1);
    payload["deployment_epoch"] = json!("ens_v2_root_scope_test");
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
            'ens_v2_root_scope_test',
            'active',
            'ensip15@ens-normalize-0.1.1',
            $3,
            $4::JSONB
        )
        RETURNING manifest_id
        "#,
    )
    .bind(SOURCE_FAMILY_ENS_V2_ROOT_L1)
    .bind(chain)
    .bind(format!(
        "test/ens_v2_root_scope_{}_{}.toml",
        std::process::id(),
        NEXT_TEST_ID.load(Ordering::Relaxed)
    ))
    .bind(serde_json::to_string(&payload)?)
    .fetch_one(pool)
    .await
    .context("failed to insert root registry test manifest")?;
    establish_test_raw_log_closure_proof(pool, chain).await?;
    Ok(manifest_id)
}

async fn insert_test_resolver_manifest(pool: &PgPool, chain: &str) -> Result<i64> {
    let mut payload = test_registry_manifest_payload(chain);
    payload["source_family"] = json!(SOURCE_FAMILY_ENS_V2_RESOLVER_L1);
    payload["roots"] = json!([]);
    payload["contracts"] = json!([]);
    payload["discovery_rules"] = json!([]);
    sqlx::query_scalar::<_, i64>(
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
    .bind(SOURCE_FAMILY_ENS_V2_RESOLVER_L1)
    .bind(chain)
    .bind(format!(
        "test/ens_v2_resolver_scope_{}_{}.toml",
        std::process::id(),
        NEXT_TEST_ID.load(Ordering::Relaxed)
    ))
    .bind(serde_json::to_string(&payload)?)
    .fetch_one(pool)
    .await
    .context("failed to insert resolver-history test manifest")
}

async fn establish_test_raw_log_closure_proof(pool: &PgPool, chain: &str) -> Result<()> {
    // Unit fixtures insert already-complete raw-log pages directly instead of
    // running the indexer's generation-bound bootstrap. Establish the proof
    // tuple explicitly so tests which are not about bootstrap closure can use
    // the production reconstruction and full-source guards.
    sqlx::query(
        r#"
        INSERT INTO discovery_admission_epochs (chain_id, epoch)
        VALUES ($1, 0)
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await
    .context("failed to establish test discovery-admission epoch")?;
    sqlx::query(
        r#"
        INSERT INTO raw_log_staging_input_revisions (
            chain_id,
            revision,
            retention_generation,
            retained_history_complete,
            incomplete_since,
            proven_retention_generation,
            proven_discovery_admission_epoch,
            proven_through_block
        )
        VALUES ($1, 0, 0, true, NULL, 0, 0, 9223372036854775807)
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain)
    .execute(pool)
    .await
    .context("failed to establish test raw-log closure proof")?;
    Ok(())
}

async fn refresh_test_raw_log_closure_proof(
    pool: &PgPool,
    chain: &str,
    through_block: i64,
) -> Result<()> {
    let discovery_epoch = bigname_manifests::load_discovery_admission_epoch(pool, chain).await?;
    sqlx::query(
        r#"
        UPDATE raw_log_staging_input_revisions
        SET retained_history_complete = true,
            incomplete_since = NULL,
            proven_retention_generation = retention_generation,
            proven_discovery_admission_epoch = $2,
            proven_through_block = $3
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .bind(discovery_epoch)
    .bind(through_block)
    .execute(pool)
    .await
    .context("failed to refresh test raw-log closure proof")?;
    Ok(())
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
            },
            {
                "edge_kind": "registry_announcement",
                "from_role": "registry",
                "admission": "reachable_from_root"
            }
        ],
        "abi": {
            "events": [
                {
                    "name": "RegistryCreated",
                    "fragment": "event RegistryCreated()"
                },
                {
                    "name": "Upgraded",
                    "fragment": "event Upgraded(address indexed implementation)"
                },
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
                {
                    "name": "TransferSingle",
                    "fragment": "event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)"
                },
                {
                    "name": "TransferBatch",
                    "fragment": "event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)"
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

fn same_block_discovery_observation(
    chain: &str,
    registry: &str,
    discovery_source: &str,
    observation_key: &str,
    to_address: &str,
    log_index: i64,
) -> DiscoveryObservation {
    DiscoveryObservation {
        chain: chain.to_owned(),
        from_address: registry.to_owned(),
        to_address: to_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(10),
        active_from_block_hash: Some(lifecycle_branch_block_hash(10, 0)),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: json!({
            "provider": "unit-test",
            "observation_key": observation_key,
            "from_address": registry,
            "to_address": to_address,
            "transaction_index": 0,
            "log_index": log_index,
        }),
    }
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

async fn insert_finalized_reference_lineage(
    pool: &PgPool,
    from_block: i64,
    through_block: i64,
) -> Result<()> {
    let blocks = (from_block..=through_block)
        .map(|block_number| {
            let mut block = test_raw_block(
                "ethereum-sepolia",
                &format!("0xblock{block_number}"),
                block_number,
            );
            if block_number > from_block {
                block.parent_hash = Some(format!("0xblock{}", block_number - 1));
            }
            raw_block_to_test_lineage(&block)
        })
        .collect::<Vec<_>>();
    upsert_chain_lineage_blocks(pool, &blocks)
        .await
        .context("failed to insert finalized ENSv2 reference lineage")?;
    Ok(())
}

fn raw_block_to_test_lineage(block: &RawBlock) -> ChainLineageBlock {
    ChainLineageBlock {
        chain_id: block.chain_id.clone(),
        block_hash: block.block_hash.clone(),
        parent_hash: block.parent_hash.clone(),
        block_number: block.block_number,
        block_timestamp: block.block_timestamp,
        logs_bloom: block.logs_bloom.clone(),
        transactions_root: block.transactions_root.clone(),
        receipts_root: block.receipts_root.clone(),
        state_root: block.state_root.clone(),
        canonicality_state: block.canonicality_state,
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
        activation_positions: Vec::new(),
    }
}

fn registry_event_topics() -> crate::adapter_manifest::ActiveManifestEventTopic0sBySignature {
    crate::adapter_manifest::ActiveManifestEventTopic0sBySignature::new(
        ABI_EVENT_SIGNATURES
            .into_iter()
            .map(|signature| (signature.to_owned(), keccak_signature_hex(signature)))
            .collect(),
    )
}

fn registry_raw_log_row(raw_log: RawLog) -> RegistryRawLogRow {
    let block_timestamp = OffsetDateTime::from_unix_timestamp(1_717_172_700 + raw_log.block_number)
        .expect("test timestamp should fit");
    RegistryRawLogRow {
        chain_id: raw_log.chain_id,
        block_hash: raw_log.block_hash,
        block_number: raw_log.block_number,
        block_timestamp,
        transaction_hash: raw_log.transaction_hash,
        transaction_index: raw_log.transaction_index,
        log_index: raw_log.log_index,
        emitting_address: raw_log.emitting_address,
        topics: raw_log.topics,
        data: raw_log.data,
        canonicality_state: raw_log.canonicality_state,
        emitting_contract_instance_id: Uuid::from_u128(0x1234),
        source_manifest_id: 1,
        namespace: "ens".to_owned(),
        source_family: SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
        manifest_version: 1,
        normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
    }
}

fn registry_created_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
) -> RawLog {
    registry_created_raw_log_at(chain, block_hash, block_number, emitting_address, 0)
}

fn registry_created_raw_log_at(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xregistrycreated{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![keccak_signature_hex(ABI_EVENT_REGISTRY_CREATED_SIGNATURE)],
        data: Vec::new(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn upgraded_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    implementation: &str,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xupgraded{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex(ABI_EVENT_UPGRADED_SIGNATURE),
            topic_address(implementation),
        ],
        data: Vec::new(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn transfer_single_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    operator: &str,
    from: &str,
    to: &str,
    token_id: u64,
    amount: u64,
) -> RawLog {
    let mut data = Vec::new();
    data.extend_from_slice(&word_bytes(token_id));
    data.extend_from_slice(&word_bytes(amount));
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xtransfer{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex(ABI_EVENT_TRANSFER_SINGLE_SIGNATURE),
            topic_address(operator),
            topic_address(from),
            topic_address(to),
        ],
        data,
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn transfer_batch_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    operator: &str,
    from: &str,
    to: &str,
    token_ids: &[u64],
    amounts: &[u64],
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xbatchtransfer{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex(ABI_EVENT_TRANSFER_BATCH_SIGNATURE),
            topic_address(operator),
            topic_address(from),
            topic_address(to),
        ],
        data: transfer_batch_data(token_ids, amounts),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn transfer_batch_data(token_ids: &[u64], amounts: &[u64]) -> Vec<u8> {
    let ids_offset = 64_u64;
    let values_offset = ids_offset + 32 * (1 + token_ids.len() as u64);
    let mut data = Vec::new();
    data.extend_from_slice(&word_bytes(ids_offset));
    data.extend_from_slice(&word_bytes(values_offset));
    data.extend_from_slice(&word_bytes(token_ids.len() as u64));
    for token_id in token_ids {
        data.extend_from_slice(&word_bytes(*token_id));
    }
    data.extend_from_slice(&word_bytes(amounts.len() as u64));
    for amount in amounts {
        data.extend_from_slice(&word_bytes(*amount));
    }
    data
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

#[allow(clippy::too_many_arguments)]
fn expiry_updated_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    token_id: u64,
    new_expiry: u64,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xexpiry{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex("ExpiryUpdated(uint256,uint64,address)"),
            topic_word(token_id),
            topic_word(new_expiry),
            topic_address("0x0000000000000000000000000000000000000dad"),
        ],
        data: Vec::new(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn token_regenerated_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    old_token_id: u64,
    new_token_id: u64,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xregenerated{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex("TokenRegenerated(uint256,uint256)"),
            topic_word(old_token_id),
            topic_word(new_token_id),
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

#[allow(clippy::too_many_arguments)]
fn parent_updated_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    parent: &str,
    label: &str,
) -> RawLog {
    let mut data = Vec::new();
    data.extend_from_slice(&word_bytes(32));
    data.extend_from_slice(&word_bytes(label.len() as u64));
    data.extend_from_slice(label.as_bytes());
    while data.len() % 32 != 0 {
        data.push(0);
    }
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xparent{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex(ABI_EVENT_PARENT_UPDATED_SIGNATURE),
            topic_address(parent),
            topic_address("0x0000000000000000000000000000000000000dad"),
        ],
        data,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn resolver_updated_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    log_index: i64,
    token_id: u64,
    resolver: &str,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xresolver{block_number}"),
        transaction_index: 0,
        log_index,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex("ResolverUpdated(uint256,address,address)"),
            topic_word(token_id),
            topic_address(resolver),
            topic_address("0x0000000000000000000000000000000000000dad"),
        ],
        data: Vec::new(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

async fn insert_raw_log_with_id(pool: &PgPool, raw_log_id: i64, raw_log: &RawLog) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO raw_logs (
            raw_log_id,
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state
        )
        OVERRIDING SYSTEM VALUE
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)
        "#,
    )
    .bind(raw_log_id)
    .bind(&raw_log.chain_id)
    .bind(&raw_log.block_hash)
    .bind(raw_log.block_number)
    .bind(&raw_log.transaction_hash)
    .bind(raw_log.transaction_index)
    .bind(raw_log.log_index)
    .bind(&raw_log.emitting_address)
    .bind(&raw_log.topics)
    .bind(&raw_log.data)
    .bind(raw_log.canonicality_state.as_str())
    .execute(pool)
    .await
    .context("failed to insert raw log with a reserved lower identity")?;
    Ok(())
}

async fn insert_completed_registry_coverage(
    pool: &PgPool,
    chain: &str,
    sources: &[(&str, &str)],
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    let backfill_job_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO backfill_jobs (
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status,
            completed_at,
            raw_log_retention_generation
        )
        VALUES (
            'sepolia',
            $1,
            '{}'::JSONB,
            'logs',
            $2,
            $3,
            $4,
            'completed',
            clock_timestamp(),
            (
                SELECT retention_generation
                FROM raw_log_staging_input_revisions
                WHERE chain_id = $1
            )
        )
        RETURNING backfill_job_id
        "#,
    )
    .bind(chain)
    .bind(from_block)
    .bind(to_block)
    .bind(format!(
        "ensv2-registry-closure-{}-{}",
        std::process::id(),
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
    ))
    .fetch_one(pool)
    .await
    .context("failed to insert completed ENSv2 registry coverage job")?;
    for (source_family, address) in sources {
        sqlx::query(
            r#"
            INSERT INTO backfill_coverage_facts (
                backfill_job_id,
                chain_id,
                source_family,
                scope,
                address,
                covered_from_block,
                covered_to_block,
                derivation
            )
            VALUES ($1, $2, $3, 'address', $4, $5, $6, 'job_completion')
            "#,
        )
        .bind(backfill_job_id)
        .bind(chain)
        .bind(source_family)
        .bind(normalize_address(address))
        .bind(from_block)
        .bind(to_block)
        .execute(pool)
        .await
        .context("failed to insert ENSv2 registry coverage fact")?;
    }
    Ok(())
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

async fn active_subregistry_edge_count(pool: &PgPool, chain: &str, address: &str) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM discovery_edges
        WHERE chain_id = $1
          AND edge_kind = 'subregistry'
          AND lower(provenance ->> 'to_address') = $2
          AND deactivated_at IS NULL
        "#,
    )
    .bind(chain)
    .bind(normalize_address(address))
    .fetch_one(pool)
    .await
    .context("failed to count active ENSv2 subregistry edges")
}

async fn normalized_event_count(pool: &PgPool) -> Result<i64> {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM normalized_events")
        .fetch_one(pool)
        .await
        .context("failed to count scoped registry normalized events")
}

async fn pending_registration_event_identity(pool: &PgPool, name: &str) -> Result<String> {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT event_identity
        FROM normalized_events
        WHERE logical_name_id = $1
          AND event_kind = 'RegistrationGranted'
          AND after_state ->> 'resource_pending' = 'true'
        "#,
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .context("failed to load same-block registration event identity")
}

async fn normalized_event_rows_for_equivalence(pool: &PgPool) -> Result<Vec<String>> {
    let rows = sqlx::query_scalar::<_, String>(
        r#"
        SELECT jsonb_build_object(
            'event_identity', event_identity,
            'namespace', namespace,
            'logical_name_id', logical_name_id,
            'resource_id', resource_id::TEXT,
            'event_kind', event_kind,
            'source_family', source_family,
            'manifest_version', manifest_version,
            'source_manifest_id', source_manifest_id,
            'chain_id', chain_id,
            'block_number', block_number,
            'block_hash', block_hash,
            'transaction_hash', transaction_hash,
            'log_index', log_index,
            'raw_fact_ref', raw_fact_ref,
            'derivation_kind', derivation_kind,
            'canonicality_state', canonicality_state::TEXT,
            'before_state', before_state,
            'after_state', after_state
        )::TEXT
        FROM normalized_events
        "#,
    )
    .fetch_all(pool)
    .await?;
    let contract_instances = contract_instance_stable_keys_for_equivalence(pool).await?;
    let mut normalized = Vec::with_capacity(rows.len());
    for row in rows {
        let mut value = serde_json::from_str::<Value>(&row)?;
        normalize_contract_instance_ids_for_equivalence(&mut value, &contract_instances);
        normalized.push(serde_json::to_string(&value)?);
    }
    normalized.sort();
    Ok(normalized)
}

async fn contract_instance_stable_keys_for_equivalence(
    pool: &PgPool,
) -> Result<BTreeMap<String, String>> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT DISTINCT ON (contract_instance_id)
            contract_instance_id::TEXT,
            chain_id || ':' || lower(address)
        FROM contract_instance_addresses
        ORDER BY contract_instance_id, (deactivated_at IS NULL) DESC, admitted_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

fn normalize_contract_instance_ids_for_equivalence(
    value: &mut Value,
    contract_instances: &BTreeMap<String, String>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                normalize_contract_instance_ids_for_equivalence(value, contract_instances);
            }
        }
        Value::Object(fields) => {
            for value in fields.values_mut() {
                normalize_contract_instance_ids_for_equivalence(value, contract_instances);
            }
        }
        Value::String(value) => {
            for (id, stable_key) in contract_instances {
                if value.contains(id) {
                    *value = value.replace(id, &format!("<contract:{stable_key}>"));
                }
            }
        }
        _ => {}
    }
}

async fn normalized_event_source_events(
    pool: &PgPool,
    expected: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    let expected = expected.iter().cloned().collect::<Vec<_>>();
    let rows = sqlx::query_scalar::<_, String>(
        r#"
        SELECT DISTINCT after_state ->> 'source_event'
        FROM normalized_events
        WHERE after_state ->> 'source_event' = ANY($1::TEXT[])
        "#,
    )
    .bind(expected)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

fn labelhash(label: &str) -> String {
    format!("0x{}", hex_string(keccak256_bytes(label.as_bytes())))
}

fn versioned_label_token(label: &str, version: u32) -> String {
    let labelhash = labelhash(label);
    format!("{}{:08x}", &labelhash[..58], version)
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
