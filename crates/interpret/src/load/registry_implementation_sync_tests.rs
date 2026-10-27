//! What Interpret loads for the migration family's registry implementation roles across manifest
//! synchronization. Synchronization replaces a manifest's declaration rows, so a dropped
//! declaration loads nothing, and each live declaration loads its own range: two declarations
//! of one repeatable role are two admissions, each from its own start.
use std::path::{Path, PathBuf};

use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

type TestResult<T = ()> = anyhow::Result<T>;

const CHAIN: &str = "ethereum-mainnet";
const MIGRATION: &str = "ethereum/ens/ens_v2_migration_l1/v1.toml";
const HEAD: i64 = 26_300_000;
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const WRAPPER: &str = "0xe86a4ef4d7a210aaa1d6a9594fc7add7cd2cf888";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/UserRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const USER: &str = "0xfaa23592aca10a6d8875b18ea86954e8eb6d8d72";
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet/LockedMigrationController_Proxy.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const LOCKED: &str = "0x3cea18620883a69ed349b23328a3b6302598e51a";
// The 2026-10-02 WrapperRegistryImpl, from its receipt block.
// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deployments/mainnet-redeploy-20261023/WrapperRegistryImpl.json:L2 @ ens_v2_mainnet_20261002@4f47cbcc)
const OLD_WRAPPER: &str = "0xcf3edb3f5501de63b57254d910b6782ee370039b";
const OLD_WRAPPER_START: i64 = 26_216_324;

/// (address, active from, active to) of every admission the loader returns for `role`.
type Ranges = Vec<(String, Option<i64>, Option<i64>)>;

fn copy_tree(from: &Path, to: &Path) -> TestResult {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

struct Profile {
    root: PathBuf,
    current: String,
}

impl Profile {
    fn new() -> TestResult<Self> {
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet");
        let root = std::env::temp_dir().join(format!(
            "bigname-registry-implementation-sync-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        copy_tree(&source, &root)?;
        let current = std::fs::read_to_string(root.join(MIGRATION))?;
        Ok(Self { root, current })
    }

    /// The checked-in migration manifest, plus one further wrapper implementation when `old`.
    fn write(&self, old: bool) -> TestResult {
        let extra = format!(
            "\n[[contracts]]\nrole = \"wrapper_registry_implementation\"\naddress = \"{OLD_WRAPPER}\"\nproxy_kind = \"none\"\nstart_block = {OLD_WRAPPER_START}\n"
        );
        let contents = if old {
            self.current
                .replacen("\n[[abi.events]]", &format!("{extra}\n[[abi.events]]"), 1)
        } else {
            self.current.clone()
        };
        std::fs::write(self.root.join(MIGRATION), contents)?;
        Ok(())
    }
}

impl Drop for Profile {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

async fn database() -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(
        "interpret_registry_implementation_sync",
    ))
    .await?;
    database.create_phase_schema().await?;
    for statement in [
        include_str!("../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../storage/schema/baseline/02_raw_facts.sql"),
        include_str!("../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../../storage/schema/baseline/06_projections.sql"),
        include_str!("../../../storage/schema/baseline/07_labels.sql"),
        include_str!("../../../storage/schema/baseline/08_heartbeats.sql"),
        include_str!("../../../storage/schema/baseline/09_divergence.sql"),
        include_str!("../../../storage/schema/baseline/10_phase_state.sql"),
        include_str!("../../../storage/schema/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../storage/schema/baseline/12_project_generation_failures.sql"),
        include_str!("../../../storage/schema/baseline/13_interpret_decode_skips.sql"),
        include_str!("../../../storage/schema/baseline/14_discovery_watch_admissions.sql"),
    ] {
        sqlx::raw_sql(statement).execute(database.pool()).await?;
    }
    // Retiring a declared address closes its range at the recorded chain head.
    sqlx::raw_sql(&format!(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
             canonicality_state)
         VALUES ('{CHAIN}', 'head', {HEAD}, to_timestamp({HEAD}), 'canonical');
         INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ('{CHAIN}', 'head', {HEAD});"
    ))
    .execute(database.pool())
    .await?;
    Ok(database)
}

async fn sync_and_load(
    database: &TestDatabase,
    profile: &Profile,
    role: &str,
) -> TestResult<Ranges> {
    sync_schema_v2_repository(database.pool(), &load_repository(&profile.root)?).await?;
    let mut connection = database.pool().acquire().await?;
    let mut ranges: Ranges = super::load_admissions(&mut connection, CHAIN, HEAD + 1)
        .await?
        .into_iter()
        .filter(|admission| admission.role.as_deref() == Some(role))
        .map(|admission| {
            (
                admission.address,
                admission.active_from_block,
                admission.active_to_block,
            )
        })
        .collect();
    ranges.sort();
    Ok(ranges)
}

fn range(address: &str, from: i64, to: Option<i64>) -> (String, Option<i64>, Option<i64>) {
    (address.to_owned(), Some(from), to)
}

#[tokio::test]
async fn each_declaration_loads_its_own_range_across_synchronization() -> TestResult {
    let database = database().await?;
    let profile = Profile::new()?;
    let wrapper = "wrapper_registry_implementation";

    // The checked-in manifest: one live row per role, from its receipt block.
    profile.write(false)?;
    assert_eq!(
        sync_and_load(&database, &profile, wrapper).await?,
        [range(WRAPPER, 26_277_138, None)]
    );
    assert_eq!(
        sync_and_load(&database, &profile, "user_registry_implementation").await?,
        [range(USER, 26_277_137, None)]
    );
    assert_eq!(
        sync_and_load(&database, &profile, "locked_migration_controller").await?,
        [range(LOCKED, 26_277_141, None)]
    );

    // A further pair: two live rows under one repeatable role, each from its own start.
    profile.write(true)?;
    assert_eq!(
        sync_and_load(&database, &profile, wrapper).await?,
        [
            range(OLD_WRAPPER, OLD_WRAPPER_START, None),
            range(WRAPPER, 26_277_138, None),
        ]
    );

    // Dropping it closes its address at the head and loads nothing for it.
    profile.write(false)?;
    assert_eq!(
        sync_and_load(&database, &profile, wrapper).await?,
        [range(WRAPPER, 26_277_138, None)]
    );
    let closed: Option<i64> = sqlx::query_scalar(
        "SELECT active_to_block_number FROM contract_instance_addresses
         WHERE chain_id = $1 AND lower(address) = $2",
    )
    .bind(CHAIN)
    .bind(OLD_WRAPPER)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(closed, Some(HEAD));

    // Declaring it again loads the closed range and a new one after it.
    profile.write(true)?;
    assert_eq!(
        sync_and_load(&database, &profile, wrapper).await?,
        [
            range(OLD_WRAPPER, OLD_WRAPPER_START, Some(HEAD)),
            range(OLD_WRAPPER, HEAD + 1, None),
            range(WRAPPER, 26_277_138, None),
        ]
    );
    Ok(())
}
