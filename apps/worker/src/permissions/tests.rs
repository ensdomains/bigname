use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use bigname_storage::{
    CanonicalityState, NormalizedEvent, PermissionScope, PermissionsCurrentResourceSummary,
    PermissionsCurrentRow, RawBlock, Resource, default_database_url, load_permissions_current,
    load_permissions_current_resource_summary, upsert_normalized_events,
    upsert_permissions_current_resource_summary, upsert_permissions_current_rows,
    upsert_raw_blocks, upsert_resources,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::types::time::OffsetDateTime;
use uuid::Uuid;

use super::canonicality::format_timestamp;
use super::{
    EVENT_KIND_PERMISSION_CHANGED, PERMISSIONS_CURRENT_DERIVATION_KIND,
    PERMISSIONS_ENUMERATION_BASIS, rebuild_permissions_current,
};

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
            .context("failed to parse database URL for worker permissions_current tests")?;
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bg_wp_{}_{}_{}",
            std::process::id(),
            sequence,
            &Uuid::new_v4().simple().to_string()[..8]
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for worker permissions_current tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect worker permissions_current test pool")?;

        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for worker permissions_current tests")?;

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
async fn keyed_rebuild_keeps_active_rows_and_drops_revoked_rows() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7100);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm0064", 100, 1_776_100_100),
            raw_block("ethereum-mainnet", "0xperm0065", 101, 1_776_100_101),
            raw_block("ethereum-mainnet", "0xperm0066", 102, 1_776_100_102),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            permission_event(
                "grant-resource",
                resource_id,
                "0x0000000000000000000000000000000000000abc",
                json!({"kind": "resource"}),
                json!(["set_records"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 1})),
                None,
                100,
                0,
            ),
            permission_event(
                "grant-resolver",
                resource_id,
                "0x0000000000000000000000000000000000000abc",
                json!({
                    "kind": "resolver",
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000def"
                }),
                json!(["set_resolver"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 2})),
                None,
                101,
                0,
            ),
            permission_event(
                "revoke-resource",
                resource_id,
                "0x0000000000000000000000000000000000000abc",
                json!({"kind": "resource"}),
                json!([]),
                None,
                Some(json!({"kind": "normalized_event", "normalized_event_id": 3})),
                102,
                0,
            ),
        ],
    )
    .await?;

    let summary =
        rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.requested_resource_count, 1);
    assert_eq!(summary.upserted_row_count, 1);
    assert_eq!(summary.deleted_row_count, 0);

    let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].scope,
        PermissionScope::Resolver {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
        }
    );
    assert_eq!(rows[0].effective_powers, json!(["set_resolver"]));
    assert_eq!(rows[0].provenance["normalized_event_ids"], json!([2]));
    assert_eq!(
        rows[0].coverage["enumeration_basis"],
        json!(PERMISSIONS_ENUMERATION_BASIS)
    );

    database.cleanup().await
}

#[tokio::test]
async fn keyed_rebuild_moves_permission_rows_on_subject_change() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7200);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm006e", 110, 1_776_100_110),
            raw_block("ethereum-mainnet", "0xperm006f", 111, 1_776_100_111),
            raw_block("ethereum-mainnet", "0xperm0070", 112, 1_776_100_112),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            permission_event(
                "grant-old-subject",
                resource_id,
                "0x0000000000000000000000000000000000000aaa",
                json!({"kind": "resource"}),
                json!(["set_records"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 10})),
                None,
                110,
                0,
            ),
            permission_event(
                "revoke-old-subject",
                resource_id,
                "0x0000000000000000000000000000000000000aaa",
                json!({"kind": "resource"}),
                json!([]),
                None,
                Some(json!({"kind": "normalized_event", "normalized_event_id": 11})),
                111,
                0,
            ),
            permission_event(
                "grant-new-subject",
                resource_id,
                "0x0000000000000000000000000000000000000bbb",
                json!({"kind": "resource"}),
                json!(["set_records"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 12})),
                None,
                112,
                0,
            ),
        ],
    )
    .await?;

    let summary =
        rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.upserted_row_count, 1);

    let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].subject,
        "0x0000000000000000000000000000000000000bbb"
    );
    assert_eq!(rows[0].scope, PermissionScope::Resource);
    assert_eq!(rows[0].effective_powers, json!(["set_records"]));

    database.cleanup().await
}

#[tokio::test]
async fn keyed_rebuild_projects_resolver_scope_provenance_and_chain_positions() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x7300);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm0078", 120, 1_776_100_120),
            raw_block("ethereum-mainnet", "0xperm0079", 121, 1_776_100_121),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            permission_event(
                "resolver-grant-1",
                resource_id,
                "0x0000000000000000000000000000000000000abc",
                json!({
                    "kind": "resolver",
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000dEf"
                }),
                json!(["set_resolver"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 20})),
                None,
                120,
                0,
            ),
            permission_event(
                "resolver-grant-2",
                resource_id,
                "0x0000000000000000000000000000000000000abc",
                json!({
                    "kind": "resolver",
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000def"
                }),
                json!(["set_resolver", "set_records"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 21})),
                None,
                121,
                0,
            ),
        ],
    )
    .await?;

    let summary =
        rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.upserted_row_count, 1);

    let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].scope,
        PermissionScope::Resolver {
            chain_id: "ethereum-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000def".to_owned(),
        }
    );
    assert_eq!(rows[0].provenance["normalized_event_ids"], json!([1, 2]));
    assert_eq!(
        rows[0].chain_positions["ethereum-mainnet"]["block_number"],
        json!(121)
    );
    assert_eq!(
        rows[0].chain_positions["ethereum-mainnet"]["timestamp"],
        json!(format_timestamp(timestamp(1_776_100_121)))
    );
    assert_eq!(rows[0].last_recomputed_at, timestamp(1_776_100_121));

    database.cleanup().await
}

#[tokio::test]
async fn permissions_current_keyed_rebuild_projects_basenames_resolver_scope_from_permission_changed_rows()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x73b0);

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("base-mainnet", "0xperm008c", 140, 1_776_100_140),
            raw_block("base-mainnet", "0xperm008d", 141, 1_776_100_141),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            permission_event_with_context(
                "basenames-resolver-grant-1",
                "basenames",
                "basenames_base_registry",
                "base-mainnet",
                3,
                resource_id,
                "0x0000000000000000000000000000000000000abc",
                json!({
                    "kind": "resolver",
                    "chain_id": "base-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000AbC"
                }),
                json!(["resolver_control"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 40})),
                None,
                140,
                0,
            ),
            permission_event_with_context(
                "basenames-resolver-grant-2",
                "basenames",
                "basenames_base_resolver",
                "base-mainnet",
                4,
                resource_id,
                "0x0000000000000000000000000000000000000abc",
                json!({
                    "kind": "resolver",
                    "chain_id": "base-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000abc"
                }),
                json!(["resolver_control", "resource_control"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 41})),
                None,
                141,
                0,
            ),
        ],
    )
    .await?;

    let summary =
        rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.upserted_row_count, 1);

    let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].scope,
        PermissionScope::Resolver {
            chain_id: "base-mainnet".to_owned(),
            resolver_address: "0x0000000000000000000000000000000000000abc".to_owned(),
        }
    );
    assert_eq!(
        rows[0].effective_powers,
        json!(["resolver_control", "resource_control"])
    );
    assert_eq!(rows[0].provenance["normalized_event_ids"], json!([1, 2]));
    assert_eq!(
        rows[0].coverage["source_classes_considered"],
        json!(["basenames_base_registry", "basenames_base_resolver"])
    );
    assert_eq!(
        rows[0].chain_positions["base-mainnet"]["block_number"],
        json!(141)
    );

    database.cleanup().await
}

#[tokio::test]
async fn ensv2_registry_and_root_permission_events_project_registry_vocabulary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let registry_resource_id = Uuid::from_u128(0x73c0);
    let root_resource_id = Uuid::from_u128(0x73c1);
    let registry_subject = "0x0000000000000000000000000000000000000aaa";
    let root_subject = "0x0000000000000000000000000000000000000bbb";
    let registry_upstream_resource =
        "0x00000000000000000000000000000000000000000000000000000000000073c0";
    let root_upstream_resource =
        "0x0000000000000000000000000000000000000000000000000000000000000000";

    seed_resources(database.pool(), &[registry_resource_id, root_resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm008e", 142, 1_776_100_142),
            raw_block("ethereum-mainnet", "0xperm008f", 143, 1_776_100_143),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            ensv2_permission_event(
                "ensv2-registry-permission",
                "PermissionChanged",
                "ens_v2_registry_l1",
                registry_resource_id,
                registry_upstream_resource,
                registry_subject,
                json!({
                    "kind": "registry",
                    "chain_id": "ethereum-mainnet",
                    "registry_address": "0x0000000000000000000000000000000000000eee"
                }),
                json!(["set_subregistry", "admin_set_subregistry"]),
                json!(["set_subregistry"]),
                json!(["admin_set_subregistry"]),
                "0x0000000000000000000000000000000000000000000000000000000000100000",
                "0x0000000000000000000000000010000000000000000000000000000000100000",
                142,
                0,
            ),
            ensv2_permission_event(
                "ensv2-root-permission",
                "RootPermissionChanged",
                "ens_v2_registry_l1",
                root_resource_id,
                root_upstream_resource,
                root_subject,
                json!({
                    "kind": "registry_root",
                    "chain_id": "ethereum-mainnet",
                    "registry_address": "0x0000000000000000000000000000000000000eee"
                }),
                json!(["registrar", "register_reserved"]),
                json!(["registrar"]),
                json!(["register_reserved"]),
                "0x0000000000000000000000000000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000000000000000000000000000011",
                143,
                0,
            ),
        ],
    )
    .await?;

    let summary = rebuild_permissions_current(database.pool(), None).await?;
    assert_eq!(summary.requested_resource_count, 2);
    assert_eq!(summary.upserted_row_count, 2);

    let registry_rows =
        load_permissions_current(database.pool(), registry_resource_id, None, None).await?;
    assert_eq!(registry_rows.len(), 1);
    let registry_row = registry_rows
        .first()
        .context("missing registry-scope ENSv2 permission row")?;
    assert_eq!(registry_row.subject, registry_subject);
    assert_eq!(registry_row.scope, PermissionScope::Registry);
    assert_eq!(
        registry_row.effective_powers,
        json!(["set_subregistry", "admin_set_subregistry"])
    );
    assert_eq!(registry_row.provenance["normalized_event_ids"], json!([1]));

    let root_rows = load_permissions_current(database.pool(), root_resource_id, None, None).await?;
    assert_eq!(root_rows.len(), 1);
    let root_row = root_rows
        .first()
        .context("missing root ENSv2 permission row")?;
    assert_eq!(root_row.subject, root_subject);
    assert_eq!(root_row.scope, PermissionScope::Root);
    assert_eq!(
        root_row.effective_powers,
        json!(["registrar", "register_reserved"])
    );
    assert_eq!(
        root_row.inheritance_path,
        json!([{
            "kind": "registry_root_fallback",
            "chain_id": "ethereum-mainnet",
            "registry_address": "0x0000000000000000000000000000000000000eee",
            "upstream_resource": root_upstream_resource
        }])
    );
    assert_eq!(root_row.provenance["normalized_event_ids"], json!([2]));

    database.cleanup().await
}

#[tokio::test]
async fn full_rebuild_publishes_summary_for_ensv2_registration_without_role_events() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x73d0);
    let registry_contract_instance_id = Uuid::from_u128(0xe201);
    let upstream_resource = "0x00000000000000000000000000000000000000000000000000000000000073d0";

    upsert_resources(
        database.pool(),
        &[Resource {
            resource_id,
            token_lineage_id: None,
            chain_id: "ethereum-sepolia".to_owned(),
            block_hash: "0xensv2registration0092".to_owned(),
            block_number: 146,
            provenance: json!({
                "source_family": "ens_v2_registry_l1",
                "chain_id": "ethereum-sepolia",
                "upstream_resource": upstream_resource,
                "registry_contract_instance_id": registry_contract_instance_id,
            }),
            canonicality_state: CanonicalityState::Finalized,
        }],
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[raw_block(
            "ethereum-sepolia",
            "0xensv2registration0092",
            146,
            1_776_100_146,
        )],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[NormalizedEvent {
            event_identity: "ensv2-registration-without-role-events".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: Some("ens:zero-role.eth".to_owned()),
            resource_id: Some(resource_id),
            event_kind: "RegistrationGranted".to_owned(),
            source_family: "ens_v2_registry_l1".to_owned(),
            manifest_version: 2,
            source_manifest_id: None,
            chain_id: Some("ethereum-sepolia".to_owned()),
            block_number: Some(146),
            block_hash: Some("0xensv2registration0092".to_owned()),
            transaction_hash: Some("0xensv2registrationtx0092".to_owned()),
            log_index: Some(0),
            raw_fact_ref: json!({"kind": "raw_log"}),
            derivation_kind: "ens_v2_registry_resource_surface".to_owned(),
            canonicality_state: CanonicalityState::Finalized,
            before_state: json!({}),
            after_state: json!({
                "registrant": "0x0000000000000000000000000000000000000abc",
                "registry_contract_instance_id": registry_contract_instance_id,
                "upstream_resource": upstream_resource,
            }),
        }],
    )
    .await?;

    let rebuild = rebuild_permissions_current(database.pool(), None).await?;
    let resource_summary =
        load_permissions_current_resource_summary(database.pool(), resource_id).await?;
    assert_eq!(
        (rebuild.requested_resource_count, resource_summary.is_some()),
        (1, true),
        "full rebuild must select an ENSv2 registration resource and publish its zero-row permission summary"
    );

    database.cleanup().await
}

#[tokio::test]
async fn root_permission_changed_revocation_to_empty_drops_root_row() -> Result<()> {
    let database = TestDatabase::new().await?;
    let root_resource_id = Uuid::from_u128(0x73c2);
    let root_subject = "0x0000000000000000000000000000000000000bbb";
    let root_upstream_resource =
        "0x0000000000000000000000000000000000000000000000000000000000000000";

    seed_resources(database.pool(), &[root_resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm0090", 144, 1_776_100_144),
            raw_block("ethereum-mainnet", "0xperm0091", 145, 1_776_100_145),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            ensv2_permission_event(
                "ensv2-root-permission-grant",
                "RootPermissionChanged",
                "ens_v2_registry_l1",
                root_resource_id,
                root_upstream_resource,
                root_subject,
                json!({
                    "kind": "registry_root",
                    "chain_id": "ethereum-mainnet",
                    "registry_address": "0x0000000000000000000000000000000000000eee"
                }),
                json!(["registrar"]),
                json!([]),
                json!(["registrar"]),
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x0000000000000000000000000000000000000000000000000000000000000001",
                144,
                0,
            ),
            ensv2_permission_event(
                "ensv2-root-permission-revoke",
                "RootPermissionChanged",
                "ens_v2_registry_l1",
                root_resource_id,
                root_upstream_resource,
                root_subject,
                json!({
                    "kind": "registry_root",
                    "chain_id": "ethereum-mainnet",
                    "registry_address": "0x0000000000000000000000000000000000000eee"
                }),
                json!([]),
                json!(["registrar"]),
                json!(["registrar"]),
                "0x0000000000000000000000000000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                145,
                0,
            ),
        ],
    )
    .await?;

    let summary =
        rebuild_permissions_current(database.pool(), Some(&root_resource_id.to_string())).await?;
    assert_eq!(summary.requested_resource_count, 1);
    assert_eq!(summary.upserted_row_count, 0);
    assert_eq!(summary.deleted_row_count, 0);

    let rows = load_permissions_current(database.pool(), root_resource_id, None, None).await?;
    assert!(
        rows.is_empty(),
        "latest RootPermissionChanged with no effective powers must drop the root row"
    );

    database.cleanup().await
}

#[tokio::test]
async fn wrapper_scope_fuses_mask_resource_control_powers() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x73b1);
    let subject = "0x0000000000000000000000000000000000000abc";

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm0096", 150, 1_776_100_150),
            raw_block("ethereum-mainnet", "0xperm0097", 151, 1_776_100_151),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            permission_event(
                "wrapper-resource-grant",
                resource_id,
                subject,
                json!({"kind": "resource"}),
                json!(["resource_control"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 50})),
                None,
                150,
                0,
            ),
            permission_scope_event("wrapper-fuses-set", resource_id, 8, 151, 0),
        ],
    )
    .await?;

    let summary =
        rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.requested_resource_count, 1);
    assert_eq!(summary.upserted_row_count, 0);

    let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
    assert!(
        rows.is_empty(),
        "CANNOT_SET_RESOLVER must fail closed instead of publishing resource_control"
    );
    let resource_summary = load_permissions_current_resource_summary(database.pool(), resource_id)
        .await?
        .expect("zero-holder wrapper must still publish a resource summary");
    assert_eq!(resource_summary.authority_kind.as_deref(), Some("wrapper"));
    assert_eq!(resource_summary.coverage["status"], "unsupported");
    assert_eq!(
        resource_summary.coverage["unsupported_reason"],
        "ensv1_wrapper_holder_permissions_not_projected"
    );
    assert_eq!(
        resource_summary.chain_positions["ethereum-mainnet"]["block_number"],
        151
    );
    assert_eq!(resource_summary.canonicality_summary["status"], "finalized");

    database.cleanup().await
}

#[tokio::test]
async fn wrapped_eth_2ld_parent_cannot_control_keeps_owner_resource_control() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x73b2);
    let subject = "0x0000000000000000000000000000000000000abc";
    let parent_cannot_control = 1_i64 << 16;
    let is_dot_eth = 1_i64 << 17;

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm0098", 152, 1_776_100_152),
            raw_block("ethereum-mainnet", "0xperm0099", 153, 1_776_100_153),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            permission_event(
                "wrapped-eth-owner-resource-grant",
                resource_id,
                subject,
                json!({"kind": "resource"}),
                json!(["resource_control"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 51})),
                None,
                152,
                0,
            ),
            permission_scope_event(
                "wrapped-eth-default-fuses",
                resource_id,
                parent_cannot_control | is_dot_eth,
                153,
                0,
            ),
        ],
    )
    .await?;

    let summary =
        rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.upserted_row_count, 1);

    let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].subject, subject);
    assert_eq!(rows[0].scope, PermissionScope::Resource);
    assert_eq!(rows[0].effective_powers, json!(["resource_control"]));

    database.cleanup().await
}

#[tokio::test]
async fn cannot_set_resolver_masks_resolver_control() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x73b3);
    let subject = "0x0000000000000000000000000000000000000abc";
    let cannot_set_resolver = 8;

    seed_resources(database.pool(), &[resource_id]).await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm009a", 154, 1_776_100_154),
            raw_block("ethereum-mainnet", "0xperm009b", 155, 1_776_100_155),
        ],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            permission_event(
                "wrapper-resolver-control-grant",
                resource_id,
                subject,
                json!({
                    "kind": "resolver",
                    "chain_id": "ethereum-mainnet",
                    "resolver_address": "0x0000000000000000000000000000000000000def"
                }),
                json!(["resolver_control"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 52})),
                None,
                154,
                0,
            ),
            permission_scope_event(
                "wrapper-cannot-set-resolver",
                resource_id,
                cannot_set_resolver,
                155,
                0,
            ),
        ],
    )
    .await?;

    let summary =
        rebuild_permissions_current(database.pool(), Some(&resource_id.to_string())).await?;
    assert_eq!(summary.upserted_row_count, 0);

    let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
    assert!(
        rows.is_empty(),
        "CANNOT_SET_RESOLVER must mask resolver_control"
    );

    database.cleanup().await
}

#[tokio::test]
async fn malformed_wrapper_fuse_modifiers_fail_closed() -> Result<()> {
    let database = TestDatabase::new().await?;
    let missing_fuses_resource_id = Uuid::from_u128(0x73b4);
    let malformed_fuses_resource_id = Uuid::from_u128(0x73b5);
    let subject = "0x0000000000000000000000000000000000000abc";

    seed_resources(
        database.pool(),
        &[missing_fuses_resource_id, malformed_fuses_resource_id],
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm009c", 156, 1_776_100_156),
            raw_block("ethereum-mainnet", "0xperm009d", 157, 1_776_100_157),
            raw_block("ethereum-mainnet", "0xperm009e", 158, 1_776_100_158),
            raw_block("ethereum-mainnet", "0xperm009f", 159, 1_776_100_159),
        ],
    )
    .await?;

    let mut missing_fuses = permission_scope_event(
        "wrapper-missing-fuses",
        missing_fuses_resource_id,
        0,
        157,
        0,
    );
    missing_fuses.after_state = json!({
        "namehash": "0xwrapped",
        "authority_kind": "name_wrapper",
        "authority_key": "wrapped"
    });
    let mut malformed_fuses = permission_scope_event(
        "wrapper-malformed-fuses",
        malformed_fuses_resource_id,
        0,
        159,
        0,
    );
    malformed_fuses.after_state = json!({
        "fuses": "not-an-integer",
        "namehash": "0xwrapped",
        "authority_kind": "name_wrapper",
        "authority_key": "wrapped"
    });

    seed_permission_events(
        database.pool(),
        &[
            permission_event(
                "wrapper-missing-fuse-proof-grant",
                missing_fuses_resource_id,
                subject,
                json!({"kind": "resource"}),
                json!(["resource_control"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 53})),
                None,
                156,
                0,
            ),
            missing_fuses,
            permission_event(
                "wrapper-malformed-fuse-proof-grant",
                malformed_fuses_resource_id,
                subject,
                json!({"kind": "resource"}),
                json!(["resource_control"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 54})),
                None,
                158,
                0,
            ),
            malformed_fuses,
        ],
    )
    .await?;

    let summary = rebuild_permissions_current(database.pool(), None).await?;
    assert_eq!(summary.upserted_row_count, 0);

    for resource_id in [missing_fuses_resource_id, malformed_fuses_resource_id] {
        let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
        assert!(
            rows.is_empty(),
            "unprovable wrapper fuse state must not publish full authoritative unmasked powers"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn keyed_rebuild_keeps_visible_rows_when_projection_build_fails() -> Result<()> {
    let database = TestDatabase::new().await?;
    let resource_id = Uuid::from_u128(0x73c0);
    let subject = "0x0000000000000000000000000000000000000abc";

    seed_resources(database.pool(), &[resource_id]).await?;
    upsert_permissions_current_rows(
        database.pool(),
        &[PermissionsCurrentRow {
            resource_id,
            subject: subject.to_owned(),
            scope: PermissionScope::Resource,
            effective_powers: json!(["set_records"]),
            grant_source: json!({}),
            revocation_source: None,
            inheritance_path: json!([]),
            transfer_behavior: json!({}),
            provenance: json!({"derivation_kind": PERMISSIONS_CURRENT_DERIVATION_KIND}),
            coverage: json!({"enumeration_basis": PERMISSIONS_ENUMERATION_BASIS}),
            chain_positions: json!({}),
            canonicality_summary: json!({"status": "finalized", "chains": {}}),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_776_100_001),
        }],
    )
    .await?;
    upsert_permissions_current_resource_summary(
        database.pool(),
        &PermissionsCurrentResourceSummary {
            resource_id,
            authority_kind: Some("registrar".to_owned()),
            root_resource_id: None,
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["permissions_current"],
                "enumeration_basis": "resource_permissions",
                "unsupported_reason": null,
            }),
            provenance: json!({"derivation_kind": "preexisting_test_projection"}),
            chain_positions: json!({}),
            canonicality_summary: json!({"status": "finalized", "chains": {}}),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_776_100_001),
        },
    )
    .await?;

    let mut malformed = permission_event(
        "malformed-scope",
        resource_id,
        subject,
        json!({"kind": "resource"}),
        json!(["set_records"]),
        Some(json!({"kind": "normalized_event", "normalized_event_id": 1})),
        None,
        150,
        0,
    );
    malformed.after_state = json!({
        "subject": subject,
        "effective_powers": ["set_records"],
        "grant_source": {"kind": "normalized_event", "normalized_event_id": 1},
        "revocation_source": Value::Null,
        "inheritance_path": [{
            "kind": "resource_authority",
            "resource_id": resource_id
        }],
        "transfer_behavior": {
            "kind": "resource_rebound"
        }
    });
    seed_permission_events(database.pool(), &[malformed]).await?;

    let error = rebuild_permissions_current(database.pool(), Some(&resource_id.to_string()))
        .await
        .expect_err("rebuild should fail when permission scope is missing");
    assert!(
        error
            .to_string()
            .contains("permission event after_state.scope must be an object")
    );

    let rows = load_permissions_current(database.pool(), resource_id, None, None).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].subject, subject);
    assert_eq!(rows[0].scope, PermissionScope::Resource);
    let resource_summary = load_permissions_current_resource_summary(database.pool(), resource_id)
        .await?
        .expect("failed keyed rebuild must retain the visible resource summary");
    assert_eq!(
        resource_summary.authority_kind.as_deref(),
        Some("registrar")
    );

    database.cleanup().await
}

#[tokio::test]
async fn full_rebuild_keeps_visible_rows_when_projection_build_fails() -> Result<()> {
    let database = TestDatabase::new().await?;
    let visible_resource_id = Uuid::from_u128(0x73c1);
    let failing_resource_id = Uuid::from_u128(0x73c2);
    let subject = "0x0000000000000000000000000000000000000abc";

    seed_resources(database.pool(), &[visible_resource_id, failing_resource_id]).await?;
    upsert_permissions_current_rows(
        database.pool(),
        &[PermissionsCurrentRow {
            resource_id: visible_resource_id,
            subject: subject.to_owned(),
            scope: PermissionScope::Resource,
            effective_powers: json!(["set_records"]),
            grant_source: json!({}),
            revocation_source: None,
            inheritance_path: json!([]),
            transfer_behavior: json!({}),
            provenance: json!({"derivation_kind": PERMISSIONS_CURRENT_DERIVATION_KIND}),
            coverage: json!({"enumeration_basis": PERMISSIONS_ENUMERATION_BASIS}),
            chain_positions: json!({}),
            canonicality_summary: json!({"status": "finalized", "chains": {}}),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_776_100_001),
        }],
    )
    .await?;
    upsert_permissions_current_resource_summary(
        database.pool(),
        &PermissionsCurrentResourceSummary {
            resource_id: visible_resource_id,
            authority_kind: Some("registrar".to_owned()),
            root_resource_id: None,
            coverage: json!({
                "status": "full",
                "exhaustiveness": "authoritative",
                "source_classes_considered": ["permissions_current"],
                "enumeration_basis": "resource_permissions",
                "unsupported_reason": null,
            }),
            provenance: json!({"derivation_kind": "preexisting_test_projection"}),
            chain_positions: json!({}),
            canonicality_summary: json!({"status": "finalized", "chains": {}}),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_776_100_001),
        },
    )
    .await?;

    let mut malformed = permission_event(
        "full-rebuild-malformed-scope",
        failing_resource_id,
        subject,
        json!({"kind": "resource"}),
        json!(["set_records"]),
        Some(json!({"kind": "normalized_event", "normalized_event_id": 60})),
        None,
        160,
        0,
    );
    malformed.after_state = json!({
        "subject": subject,
        "effective_powers": ["set_records"],
        "grant_source": {"kind": "normalized_event", "normalized_event_id": 60},
        "revocation_source": Value::Null,
        "inheritance_path": [{
            "kind": "resource_authority",
            "resource_id": failing_resource_id
        }],
        "transfer_behavior": {
            "kind": "resource_rebound"
        }
    });
    seed_permission_events(database.pool(), &[malformed]).await?;

    let error = rebuild_permissions_current(database.pool(), None)
        .await
        .expect_err("full rebuild should fail when permission scope is missing");
    assert!(
        error
            .to_string()
            .contains("permission event after_state.scope must be an object")
    );

    let rows = load_permissions_current(database.pool(), visible_resource_id, None, None).await?;
    assert_eq!(
        rows.len(),
        1,
        "failed full rebuild must not publish an empty permissions_current table"
    );
    let resource_summary =
        load_permissions_current_resource_summary(database.pool(), visible_resource_id)
            .await?
            .expect("failed full rebuild must retain the visible resource summary");
    assert_eq!(
        resource_summary.authority_kind.as_deref(),
        Some("registrar")
    );
    assert_eq!(
        resource_summary.provenance["derivation_kind"],
        "preexisting_test_projection"
    );

    database.cleanup().await
}

#[tokio::test]
async fn full_rebuild_clears_stale_rows_and_partitions_by_resource_id() -> Result<()> {
    let database = TestDatabase::new().await?;
    let first_resource_id = Uuid::from_u128(0x7400);
    let second_resource_id = Uuid::from_u128(0x7401);
    let missing_resource_id = Uuid::from_u128(0x7402);
    let stale_resource_id = Uuid::from_u128(0x74ff);

    seed_resources(
        database.pool(),
        &[first_resource_id, second_resource_id, stale_resource_id],
    )
    .await?;
    seed_raw_blocks(
        database.pool(),
        &[
            raw_block("ethereum-mainnet", "0xperm0082", 130, 1_776_100_130),
            raw_block("ethereum-mainnet", "0xperm0083", 131, 1_776_100_131),
        ],
    )
    .await?;
    upsert_permissions_current_rows(
        database.pool(),
        &[PermissionsCurrentRow {
            resource_id: stale_resource_id,
            subject: "0x0000000000000000000000000000000000000bad".to_owned(),
            scope: PermissionScope::Resource,
            effective_powers: json!(["stale"]),
            grant_source: json!({}),
            revocation_source: None,
            inheritance_path: json!([]),
            transfer_behavior: json!({}),
            provenance: json!({"derivation_kind": PERMISSIONS_CURRENT_DERIVATION_KIND}),
            coverage: json!({"enumeration_basis": PERMISSIONS_ENUMERATION_BASIS}),
            chain_positions: json!({}),
            canonicality_summary: json!({"status": "finalized", "chains": {}}),
            manifest_version: 1,
            last_recomputed_at: timestamp(1_776_100_001),
        }],
    )
    .await?;
    seed_permission_events(
        database.pool(),
        &[
            permission_event(
                "resource-a",
                first_resource_id,
                "0x0000000000000000000000000000000000000abc",
                json!({"kind": "resource"}),
                json!(["set_records"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 30})),
                None,
                130,
                0,
            ),
            permission_event(
                "resource-b",
                second_resource_id,
                "0x0000000000000000000000000000000000000abc",
                json!({"kind": "resource"}),
                json!(["set_records"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 31})),
                None,
                131,
                0,
            ),
            permission_event(
                "missing-resource",
                missing_resource_id,
                "0x0000000000000000000000000000000000000def",
                json!({"kind": "resource"}),
                json!(["set_records"]),
                Some(json!({"kind": "normalized_event", "normalized_event_id": 32})),
                None,
                131,
                1,
            ),
        ],
    )
    .await?;

    let summary = rebuild_permissions_current(database.pool(), None).await?;
    assert_eq!(summary.requested_resource_count, 2);
    assert_eq!(summary.upserted_row_count, 2);
    assert_eq!(summary.deleted_row_count, 1);

    let first_rows =
        load_permissions_current(database.pool(), first_resource_id, None, None).await?;
    let second_rows =
        load_permissions_current(database.pool(), second_resource_id, None, None).await?;
    let stale_rows =
        load_permissions_current(database.pool(), stale_resource_id, None, None).await?;
    assert_eq!(first_rows.len(), 1);
    assert_eq!(second_rows.len(), 1);
    assert!(stale_rows.is_empty());
    assert_ne!(first_rows[0].resource_id, second_rows[0].resource_id);
    assert_eq!(first_rows[0].provenance["normalized_event_ids"], json!([1]));
    assert_eq!(
        second_rows[0].provenance["normalized_event_ids"],
        json!([2])
    );

    database.cleanup().await
}

async fn seed_resources(pool: &PgPool, resource_ids: &[Uuid]) -> Result<()> {
    let resources = resource_ids
        .iter()
        .enumerate()
        .map(|(index, resource_id)| Resource {
            resource_id: *resource_id,
            token_lineage_id: None,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: format!("0xresource{index:02x}"),
            block_number: 20_000 + index as i64,
            provenance: json!({"source": "worker_permissions_current_test"}),
            canonicality_state: CanonicalityState::Finalized,
        })
        .collect::<Vec<_>>();
    upsert_resources(pool, &resources).await?;
    Ok(())
}

async fn seed_raw_blocks(pool: &PgPool, blocks: &[RawBlock]) -> Result<()> {
    upsert_raw_blocks(pool, blocks).await?;
    Ok(())
}

async fn seed_permission_events(pool: &PgPool, events: &[NormalizedEvent]) -> Result<()> {
    upsert_normalized_events(pool, events).await?;
    Ok(())
}

fn raw_block(chain_id: &str, block_hash: &str, block_number: i64, unix_timestamp: i64) -> RawBlock {
    RawBlock {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: timestamp(unix_timestamp),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

#[allow(clippy::too_many_arguments)]
fn permission_event(
    event_identity: &str,
    resource_id: Uuid,
    subject: &str,
    scope: Value,
    effective_powers: Value,
    grant_source: Option<Value>,
    revocation_source: Option<Value>,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    permission_event_with_context(
        event_identity,
        "ens",
        "ens_v1_unwrapped_authority",
        "ethereum-mainnet",
        1,
        resource_id,
        subject,
        scope,
        effective_powers,
        grant_source,
        revocation_source,
        block_number,
        log_index,
    )
}

#[allow(clippy::too_many_arguments)]
fn permission_event_with_context(
    event_identity: &str,
    namespace: &str,
    source_family: &str,
    chain_id: &str,
    manifest_version: i64,
    resource_id: Uuid,
    subject: &str,
    scope: Value,
    effective_powers: Value,
    grant_source: Option<Value>,
    revocation_source: Option<Value>,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: namespace.to_owned(),
        logical_name_id: Some(format!("{namespace}:{resource_id}")),
        resource_id: Some(resource_id),
        event_kind: EVENT_KIND_PERMISSION_CHANGED.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version,
        source_manifest_id: None,
        chain_id: Some(chain_id.to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xperm{block_number:04x}")),
        transaction_hash: Some(format!("0xtx{block_number:04x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": chain_id,
            "block_number": block_number,
            "log_index": log_index
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "subject": subject,
            "scope": scope,
            "effective_powers": effective_powers,
            "grant_source": grant_source,
            "revocation_source": revocation_source,
            "inheritance_path": [{
                "kind": "resource_authority",
                "resource_id": resource_id
            }],
            "transfer_behavior": {
                "kind": "resource_rebound"
            }
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn ensv2_permission_event(
    event_identity: &str,
    event_kind: &str,
    source_family: &str,
    resource_id: Uuid,
    upstream_resource: &str,
    subject: &str,
    scope: Value,
    effective_powers: Value,
    before_effective_powers: Value,
    changed_powers: Value,
    old_role_bitmap: &str,
    role_bitmap: &str,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    let root_resource =
        upstream_resource == "0x0000000000000000000000000000000000000000000000000000000000000000";
    let inheritance_path = if root_resource {
        json!([{
            "kind": "registry_root_fallback",
            "chain_id": "ethereum-mainnet",
            "registry_address": "0x0000000000000000000000000000000000000eee",
            "upstream_resource": upstream_resource
        }])
    } else {
        json!([])
    };

    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: None,
        resource_id: Some(resource_id),
        event_kind: event_kind.to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 2,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xperm{block_number:04x}")),
        transaction_hash: Some(format!("0xtx{block_number:04x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_hash": format!("0xperm{block_number:04x}"),
            "block_number": block_number,
            "transaction_hash": format!("0xtx{block_number:04x}"),
            "transaction_index": 0,
            "log_index": log_index,
            "emitting_address": "0x0000000000000000000000000000000000000eee"
        }),
        derivation_kind: "ens_v2_permissions".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({
            "subject": subject,
            "role_bitmap": old_role_bitmap,
            "effective_powers": before_effective_powers
        }),
        after_state: json!({
            "subject": subject,
            "scope": scope,
            "effective_powers": effective_powers,
            "grant_source": {
                "kind": "raw_log",
                "source_event": "EACRolesChanged",
                "upstream_resource": upstream_resource,
                "root_resource": root_resource,
                "changed_powers": changed_powers,
                "registry_contract_instance_id": "11111111-1111-5111-8111-111111111111"
            },
            "revocation_source": Value::Null,
            "inheritance_path": inheritance_path,
            "transfer_behavior": {},
            "source_event": "EACRolesChanged",
            "upstream_resource": upstream_resource,
            "role_bitmap": role_bitmap,
            "old_role_bitmap": old_role_bitmap,
            "root_resource": root_resource,
            "selector": {
                "kind": if root_resource { "root" } else { "unknown" },
                "key": Value::Null,
                "hash": Value::Null,
                "normalized_name": Value::Null,
                "dns_encoded_name": Value::Null
            },
            "registry_contract_instance_id": "11111111-1111-5111-8111-111111111111"
        }),
    }
}

fn permission_scope_event(
    event_identity: &str,
    resource_id: Uuid,
    fuses: i64,
    block_number: i64,
    log_index: i64,
) -> NormalizedEvent {
    NormalizedEvent {
        event_identity: event_identity.to_owned(),
        namespace: "ens".to_owned(),
        logical_name_id: Some(format!("ens:{resource_id}")),
        resource_id: Some(resource_id),
        event_kind: "PermissionScopeChanged".to_owned(),
        source_family: "ens_v1_wrapper_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: None,
        chain_id: Some("ethereum-mainnet".to_owned()),
        block_number: Some(block_number),
        block_hash: Some(format!("0xperm{block_number:04x}")),
        transaction_hash: Some(format!("0xtx{block_number:04x}")),
        log_index: Some(log_index),
        raw_fact_ref: json!({
            "kind": "raw_log",
            "chain_id": "ethereum-mainnet",
            "block_number": block_number,
            "log_index": log_index
        }),
        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "fuses": fuses,
            "namehash": "0xwrapped",
            "authority_kind": "name_wrapper",
            "authority_key": "wrapped"
        }),
    }
}

fn timestamp(value: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(value).expect("timestamp must be valid")
}
