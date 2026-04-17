use std::{
    fs,
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_storage::default_database_url;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    query_scalar,
};
use uuid::Uuid;

use super::*;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
const TEST_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new() -> Result<Self> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bigname-manifests-tests-{}-{unique}-{sequence}",
            std::process::id(),
        ));
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create test directory {}", path.display()))?;
        Ok(Self { path })
    }

    fn write_manifest(
        &self,
        namespace: &str,
        source_family: &str,
        version_tag: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let directory = self.path.join(namespace).join(source_family);
        fs::create_dir_all(&directory)
            .with_context(|| format!("failed to create {}", directory.display()))?;
        let path = directory.join(format!("{version_tag}.toml"));
        fs::write(&path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

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
            .context("failed to parse database URL for manifest integration tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bigname_manifests_test_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for manifest integration tests")?;

        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect manifest integration test pool")?;

        TEST_MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for manifest integration tests")?;

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

fn manifest_contents(
    rollout_status: &str,
    root_address: &str,
    contract_address: &str,
    implementation: Option<&str>,
) -> String {
    let implementation = implementation
        .map(|value| format!("implementation = \"{value}\"\n"))
        .unwrap_or_default();
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "{rollout_status}"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "{root_address}"

[[contracts]]
role = "registry"
address = "{contract_address}"
proxy_kind = "erc1967"
{implementation}

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    )
}

async fn load_single_contract_instance_for_address(
    pool: &PgPool,
    chain: &str,
    address: &str,
) -> Result<Uuid> {
    query_scalar::<_, Uuid>(
        r#"
            SELECT contract_instance_id
            FROM contract_instance_addresses
            WHERE chain_id = $1
              AND address = $2
            ORDER BY (deactivated_at IS NULL) DESC, admitted_at DESC
            LIMIT 1
            "#,
    )
    .bind(chain)
    .bind(normalize_address(address))
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load contract instance for {chain} {address}"))
}

#[test]
fn reports_missing_root() -> Result<()> {
    let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "bigname-manifests-missing-{}-{}-{sequence}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos()
    ));

    let repository = load_repository(&root)?;

    assert_eq!(repository.summary().status, ManifestLoadStatus::MissingRoot);
    assert!(repository.manifests().is_empty());

    Ok(())
}

#[test]
fn loads_valid_repository_manifest() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;

    let repository = load_repository(&test_dir.path)?;

    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.summary().namespace_count, 1);
    assert_eq!(repository.summary().source_family_count, 1);
    assert_eq!(repository.summary().manifest_count, 1);
    assert_eq!(repository.manifests().len(), 1);
    assert_eq!(repository.manifests()[0].version_tag, "v1");
    assert_eq!(repository.manifests()[0].manifest.namespace, "ens");

    Ok(())
}

#[test]
fn rejects_namespace_mismatch() -> Result<()> {
    let test_dir = TestDir::new()?;
    let path = test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        r#"
manifest_version = 1
namespace = "basenames"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "uts46-v1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000000"

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000000"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#,
    )?;

    let error = load_repository(&test_dir.path).expect_err("namespace mismatch must fail");
    assert!(
        error.to_string().contains("does not match directory"),
        "unexpected error for {}: {error:#}",
        path.display()
    );

    Ok(())
}

#[tokio::test]
async fn reuses_contract_instance_ids_across_inactive_gaps() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    let first_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    let empty_dir = TestDir::new()?;
    sync_repository(database.pool(), &load_repository(&empty_dir.path)?).await?;
    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
            )
            .bind(first_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            0
        );

    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    let reused_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    assert_eq!(first_contract_instance_id, reused_contract_instance_id);
    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1"
            )
            .bind(first_contract_instance_id)
            .fetch_one(database.pool())
            .await?,
            2
        );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn keeps_proxy_instance_stable_across_implementation_churn() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let proxy_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;
    let first_implementation_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000dd",
    )
    .await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000EE"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let proxy_after_churn = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;
    let second_implementation_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000ee",
    )
    .await?;

    assert_eq!(proxy_contract_instance_id, proxy_after_churn);
    assert_ne!(first_implementation_id, second_implementation_id);
    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
            )
            .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
            .fetch_one(database.pool())
            .await?,
            1
        );
    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
            )
            .bind(first_implementation_id)
            .fetch_one(database.pool())
            .await?,
            0
        );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rotates_successor_addresses_and_persists_migration_continuity() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let original_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000aa",
    )
    .await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000BB",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let successor_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        "0x00000000000000000000000000000000000000bb",
    )
    .await?;
    assert_ne!(
        original_contract_instance_id,
        successor_contract_instance_id
    );
    assert_eq!(
        query_scalar::<_, i64>(
            r#"
                SELECT COUNT(*)::BIGINT
                FROM discovery_edges
                WHERE discovery_source = $1
                  AND edge_kind = $2
                  AND from_contract_instance_id = $3
                  AND to_contract_instance_id = $4
                  AND deactivated_at IS NULL
                "#
        )
        .bind(MANIFEST_SUCCESSOR_DISCOVERY_SOURCE)
        .bind(MANIFEST_SUCCESSOR_EDGE_KIND)
        .bind(original_contract_instance_id)
        .bind(successor_contract_instance_id)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn watched_plan_does_not_expand_migration_edges() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000BB",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    assert_eq!(
            query_scalar::<_, i64>(
                "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = $1 AND deactivated_at IS NULL"
            )
            .bind(MANIFEST_SUCCESSOR_EDGE_KIND)
            .fetch_one(database.pool())
            .await?,
            1
        );

    let watched_summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(watched_summary.unique_contract_count, 3);
    assert_eq!(watched_summary.manifest_root_count, 1);
    assert_eq!(watched_summary.manifest_contract_count, 1);
    assert_eq!(watched_summary.discovery_edge_count, 1);

    let watched_chain_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000bb".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }]
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rebuilds_watched_plan_from_active_contract_instance_address_ranges() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let persistence_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: "0x00000000000000000000000000000000000000AA".to_owned(),
            to_address: "0x00000000000000000000000000000000000000CC".to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: "unit-test".to_owned(),
            active_from_block_number: Some(123),
            active_from_block_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "subregistry",
            }),
        },
    )
    .await?;
    assert_eq!(persistence_summary.admitted_edge_count, 1);
    assert_eq!(persistence_summary.inserted_edge_count, 1);
    assert!(
        persistence_summary.admitted_edges[0]
            .to_contract_instance_id
            .is_some()
    );

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert_eq!(watched_contracts.len(), 4);
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == "0x0000000000000000000000000000000000000001"
            && contract.source == WatchedContractSource::ManifestRoot
    }));
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == "0x00000000000000000000000000000000000000aa"
            && contract.source == WatchedContractSource::ManifestContract
    }));
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == "0x00000000000000000000000000000000000000dd"
            && contract.source == WatchedContractSource::DiscoveryEdge
    }));
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == "0x00000000000000000000000000000000000000cc"
            && contract.source == WatchedContractSource::DiscoveryEdge
    }));

    let watched_summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(watched_summary.unique_contract_count, 4);
    assert_eq!(watched_summary.manifest_root_count, 1);
    assert_eq!(watched_summary.manifest_contract_count, 1);
    assert_eq!(watched_summary.discovery_edge_count, 2);

    let watched_chain_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(
        watched_chain_plan,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x00000000000000000000000000000000000000aa".to_owned(),
                "0x00000000000000000000000000000000000000cc".to_owned(),
                "0x00000000000000000000000000000000000000dd".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 2,
        }]
    );

    database.cleanup().await?;
    Ok(())
}
