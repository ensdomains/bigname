use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_manifests::{load_repository, sync_repository};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::inspect_manifest_corpus;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestManifestRoot(PathBuf);

impl TestManifestRoot {
    fn new() -> Result<Self> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "bigname-worker-manifest-corpus-{}-{unique}-{sequence}",
            std::process::id()
        ));
        let directory = root.join("ens/ens_v2_registry_l1");
        fs::create_dir_all(&directory)?;
        fs::write(directory.join("v1.toml"), manifest_contents())?;
        Ok(Self(root))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestManifestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn manifest_contents() -> &'static str {
    r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-sepolia"
deployment_epoch = "ens_v2_sepolia_dev"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000001"

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000002"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
}

async fn test_database() -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new("worker_manifest_corpus_inspection")
            .admin_database("postgres")
            .pool_max_connections(5)
            .parse_context("failed to parse database URL for manifest corpus test")
            .admin_connect_context("failed to connect manifest corpus admin pool")
            .pool_connect_context("failed to connect manifest corpus test pool"),
        &bigname_storage::MIGRATOR,
        "failed to migrate manifest corpus test database",
    )
    .await
}

#[tokio::test]
async fn disk_and_database_active_manifest_corpus_match_bidirectionally() -> Result<()> {
    let database = test_database().await?;
    let root = TestManifestRoot::new()?;

    let missing = inspect_manifest_corpus(database.pool(), Some(root.path())).await?;
    assert!(!missing.complete());
    assert_eq!(missing.missing_active_manifests.len(), 1);
    assert!(missing.unexpected_active_manifests.is_empty());

    sqlx::query(
        r#"
        INSERT INTO manifest_versions
            (manifest_version, namespace, source_family, chain, deployment_epoch,
             rollout_status, normalizer_version, file_path, manifest_payload)
        VALUES
            (1, 'basenames', 'unexpected', 'base-mainnet', 'unexpected',
             'active', 'n', 'unexpected.toml', '{}'::jsonb)
        "#,
    )
    .execute(database.pool())
    .await?;
    let both_directions = inspect_manifest_corpus(database.pool(), Some(root.path())).await?;
    assert!(!both_directions.complete());
    assert_eq!(both_directions.missing_active_manifests.len(), 1);
    assert_eq!(both_directions.unexpected_active_manifests.len(), 1);

    sqlx::query("DELETE FROM manifest_versions")
        .execute(database.pool())
        .await?;
    let repository = load_repository(root.path())?;
    sync_repository(database.pool(), &repository).await?;
    let matching = inspect_manifest_corpus(database.pool(), Some(root.path())).await?;
    assert!(matching.complete());
    assert!(matching.verified);
    assert_eq!(matching.expected_active_manifest_count, 1);
    assert_eq!(matching.database_active_manifest_count, 1);

    sqlx::query("UPDATE manifest_versions SET manifest_payload = '{}'::jsonb")
        .execute(database.pool())
        .await?;
    let mismatched = inspect_manifest_corpus(database.pool(), Some(root.path())).await?;
    assert!(!mismatched.complete());
    assert_eq!(mismatched.mismatched_manifest_payloads.len(), 1);

    let unanchored = inspect_manifest_corpus(database.pool(), None).await?;
    assert!(unanchored.complete());
    assert!(!unanchored.repository_supplied);
    assert!(!unanchored.verified);

    database.cleanup().await
}
