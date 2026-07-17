use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use bigname_storage::default_database_url;
use sqlx::{
    PgPool, Row,
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

    fn write_manifest_for_chain_combo(
        &self,
        chain_combo: &str,
        namespace: &str,
        source_family: &str,
        version_tag: &str,
        contents: &str,
    ) -> Result<PathBuf> {
        let directory = self
            .path
            .join(chain_combo)
            .join(namespace)
            .join(source_family);
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

async fn reconcile_discovery_observations(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
) -> Result<DiscoveryReconciliationSummary> {
    super::reconcile_discovery_observations(
        pool,
        discovery_source,
        observations,
        FullDiscoveryReconciliationOptions::default(),
    )
    .await
}

async fn reconcile_discovery_observations_through_block(
    pool: &PgPool,
    discovery_source: &str,
    observations: &[DiscoveryObservation],
    through_block_number: i64,
) -> Result<DiscoveryReconciliationSummary> {
    super::reconcile_discovery_observations(
        pool,
        discovery_source,
        observations,
        FullDiscoveryReconciliationOptions {
            through_block_number: Some(through_block_number),
            expected_admission_epoch: None,
        },
    )
    .await
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
normalizer_version = "ensip15@ens-normalize-0.1.1"

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

fn start_block_manifest_contents(
    root_start_block: Option<i64>,
    contract_start_block: Option<i64>,
    omitted_contract_address: &str,
) -> String {
    let root_start_block = root_start_block
        .map(|start_block| format!("start_block = {start_block}\n"))
        .unwrap_or_default();
    let contract_start_block = contract_start_block
        .map(|start_block| format!("start_block = {start_block}\n"))
        .unwrap_or_default();
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000001"
{root_start_block}

[[contracts]]
role = "registry"
address = "0x0000000000000000000000000000000000000002"
proxy_kind = "none"
{contract_start_block}

[[contracts]]
role = "omitted_start"
address = "{omitted_contract_address}"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

"#
    )
}

fn simple_contract_start_block_manifest_contents() -> String {
    r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_reverse_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []
discovery_rules = []

[capability_flags]

[[contracts]]
role = "reverse_registrar"
address = "0x0000000000000000000000000000000000000042"
proxy_kind = "none"
start_block = 4242
"#
    .to_owned()
}

fn abi_manifest_contents() -> String {
    r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v2"
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

[[abi.events]]
name = "SubregistryUpdated"
fragment = "event SubregistryUpdated(uint256 indexed node, address registry, address sender)"
emitter_roles = ["registry"]
normalized_events = ["SubregistryChanged"]
status = "supported"
notes = "adapter-owned registry resource link input"

[[abi.calls]]
name = "resolver"
fragment = "function resolver(bytes32 node) view returns (address)"
target_roles = ["registry"]
status = "shadow"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    .to_owned()
}

fn registry_manifest_contents(rollout_status: &str) -> String {
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_registry_l1"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "{rollout_status}"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = {{ status = "supported", notes = "registry-controlled child surfaces are authoritative inputs" }}

[[roots]]
name = "ENSRegistry"
address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"

[[contracts]]
role = "registry"
address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"
"#
    )
}

fn execution_manifest_contents(rollout_status: &str) -> String {
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_execution"
chain = "ethereum-mainnet"
deployment_epoch = "ens_v1"
rollout_status = "{rollout_status}"
normalizer_version = "ensip15@ens-normalize-0.1.1"
roots = []
discovery_rules = []

[capability_flags]
verified_resolution = {{ status = "shadow", notes = "shadow execution traces and cache ownership are tracked before public verified-resolution reads ship" }}

[[contracts]]
role = "universal_resolver"
address = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe"
proxy_kind = "none"
"#
    )
}

fn checked_in_manifest_contents(
    namespace: &str,
    source_family: &str,
    version_tag: &str,
) -> Result<String> {
    for chain_combo in ["ethereum", "base"] {
        let path = checked_in_manifest_root("manifests/mainnet")
            .join(chain_combo)
            .join(namespace)
            .join(source_family)
            .join(format!("{version_tag}.toml"));
        if path.exists() {
            return fs::read_to_string(&path)
                .with_context(|| format!("failed to read checked-in manifest {}", path.display()));
        }
    }

    bail!(
        "failed to find checked-in manifest {namespace}/{source_family}/{version_tag}.toml in mainnet chain-combo roots"
    );
}

fn checked_in_manifest_root(profile_root: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(profile_root)
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

async fn load_manifest_rollout_statuses(
    pool: &PgPool,
    namespace: &str,
) -> Result<Vec<(String, String)>> {
    let rows = sqlx::query(
        r#"
        SELECT source_family, rollout_status::TEXT AS rollout_status
        FROM manifest_versions
        WHERE namespace = $1
        ORDER BY source_family, chain, deployment_epoch, manifest_version
        "#,
    )
    .bind(namespace)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load manifest rollout statuses for {namespace}"))?;

    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("source_family")
                    .context("failed to read source_family")?,
                row.try_get("rollout_status")
                    .context("failed to read rollout_status")?,
            ))
        })
        .collect()
}

async fn load_capability_flags_for_source_family(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
) -> Result<BTreeMap<String, CapabilityFlag>> {
    let rows = sqlx::query(
        r#"
        SELECT mcf.capability_name, mcf.status::TEXT AS status, mcf.notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.namespace = $1
          AND mv.source_family = $2
        ORDER BY mcf.capability_name
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .fetch_all(pool)
    .await
    .with_context(|| format!("failed to load capability flags for {namespace}/{source_family}"))?;

    rows.into_iter()
        .map(|row| {
            let capability_name = row
                .try_get::<String, _>("capability_name")
                .context("failed to read capability_name")?;
            let status = row
                .try_get::<String, _>("status")
                .context("failed to read capability status")?;
            let notes = row.try_get("notes").context("failed to read notes")?;
            Ok((
                capability_name,
                CapabilityFlag {
                    status: CapabilitySupportStatus::from_db_value(&status)?,
                    notes,
                },
            ))
        })
        .collect()
}

async fn load_capability_flags_for_source_family_version(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
    manifest_version: i64,
) -> Result<BTreeMap<String, CapabilityFlag>> {
    let rows = sqlx::query(
        r#"
        SELECT mcf.capability_name, mcf.status::TEXT AS status, mcf.notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.namespace = $1
          AND mv.source_family = $2
          AND mv.manifest_version = $3
        ORDER BY mcf.capability_name
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .bind(manifest_version)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load capability flags for {namespace}/{source_family} v{manifest_version}"
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let capability_name = row
                .try_get::<String, _>("capability_name")
                .context("failed to read capability_name")?;
            let status = row
                .try_get::<String, _>("status")
                .context("failed to read capability status")?;
            let notes = row.try_get("notes").context("failed to read notes")?;
            Ok((
                capability_name,
                CapabilityFlag {
                    status: CapabilitySupportStatus::from_db_value(&status)?,
                    notes,
                },
            ))
        })
        .collect()
}

async fn active_manifest_id_for_source_family(
    pool: &PgPool,
    namespace: &str,
    source_family: &str,
) -> Result<i64> {
    query_scalar::<_, i64>(
        r#"
        SELECT manifest_id
        FROM manifest_versions
        WHERE namespace = $1
          AND source_family = $2
          AND rollout_status = 'active'
        ORDER BY manifest_version DESC
        LIMIT 1
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load active manifest_id for {namespace}/{source_family}"))
}

struct RawCodeHashObservation<'a> {
    chain: &'a str,
    block_hash: &'a str,
    block_number: i64,
    contract_address: &'a str,
    code_hash: &'a str,
    code_byte_length: i64,
    canonicality_state: &'a str,
}

async fn insert_raw_code_hash_observation(
    pool: &PgPool,
    observation: RawCodeHashObservation<'_>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO raw_code_hashes (
            chain_id,
            block_hash,
            block_number,
            contract_address,
            code_hash,
            code_byte_length,
            canonicality_state
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::canonicality_state)
        "#,
    )
    .bind(observation.chain)
    .bind(observation.block_hash)
    .bind(observation.block_number)
    .bind(normalize_address(observation.contract_address))
    .bind(observation.code_hash)
    .bind(observation.code_byte_length)
    .bind(observation.canonicality_state)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to insert raw code hash for {}/{}",
            observation.chain, observation.contract_address
        )
    })?;

    Ok(())
}

async fn insert_test_chain_lineage_state(
    pool: &PgPool,
    chain: &str,
    block_hash: &str,
    block_number: i64,
    canonicality_state: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, to_timestamp($3), $4::canonicality_state)
        "#,
    )
    .bind(chain)
    .bind(block_hash)
    .bind(block_number)
    .bind(canonicality_state)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to insert test lineage block {block_number} ({block_hash}) on {chain}")
    })?;
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "coverage trust fixtures keep job bounds and fact bounds explicit at each call site"
)]
async fn insert_untrusted_backfill_coverage_fact(
    pool: &PgPool,
    chain: &str,
    job_status: &str,
    job_from_block: i64,
    job_to_block: i64,
    source_family: &str,
    address: &str,
    fact_from_block: i64,
    fact_to_block: i64,
) -> Result<()> {
    let idempotency_key =
        format!("untrusted-coverage-{job_status}-{job_from_block}-{job_to_block}-{address}");
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
            completed_at
        )
        VALUES (
            'test',
            $1,
            '{}'::jsonb,
            'logs',
            $2,
            $3,
            $4,
            $5::backfill_lifecycle_status,
            CASE WHEN $5 = 'completed' THEN now() ELSE NULL END
        )
        RETURNING backfill_job_id
        "#,
    )
    .bind(chain)
    .bind(job_from_block)
    .bind(job_to_block)
    .bind(idempotency_key)
    .bind(job_status)
    .fetch_one(pool)
    .await?;
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
        VALUES ($1, $2, $3, 'address', lower($4), $5, $6, 'job_completion')
        "#,
    )
    .bind(backfill_job_id)
    .bind(chain)
    .bind(source_family)
    .bind(address)
    .bind(fact_from_block)
    .bind(fact_to_block)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_manifest_normalized_event(
    pool: &PgPool,
    event_identity: &str,
    event_kind: &str,
    source_family: &str,
    manifest_version: i64,
    source_manifest_id: Option<i64>,
    canonicality_state: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO normalized_events (
            event_identity,
            namespace,
            event_kind,
            source_family,
            manifest_version,
            source_manifest_id,
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
            $3,
            $4,
            $5,
            $6::jsonb,
            'manifest_sync',
            $7::canonicality_state,
            $8::jsonb,
            $9::jsonb
        )
        "#,
    )
    .bind(event_identity)
    .bind(event_kind)
    .bind(source_family)
    .bind(manifest_version)
    .bind(source_manifest_id)
    .bind(serde_json::json!({ "event_identity": event_identity }).to_string())
    .bind(canonicality_state)
    .bind(serde_json::json!({ "before": event_identity }).to_string())
    .bind(serde_json::json!({ "after": event_identity }).to_string())
    .execute(pool)
    .await
    .with_context(|| format!("failed to insert normalized event {event_identity}"))?;

    Ok(())
}

fn watched_contract_for_test(
    chain: &str,
    source_family: &str,
    address: &str,
    contract_instance_id: Uuid,
    source: WatchedContractSource,
    active_from_block_number: Option<i64>,
    active_to_block_number: Option<i64>,
) -> WatchedContract {
    WatchedContract {
        chain: chain.to_owned(),
        source_family: source_family.to_owned(),
        address: normalize_address(address),
        contract_instance_id,
        source,
        source_manifest_id: Some(1),
        active_from_block_number,
        active_to_block_number,
    }
}

#[test]
fn source_family_selector_filters_targets_and_builds_chain_plan() -> Result<()> {
    let registry_a = Uuid::from_u128(1);
    let registry_b = Uuid::from_u128(2);
    let registrar = Uuid::from_u128(3);
    let other_chain_registry = Uuid::from_u128(4);
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registrar_l1",
            "0x0000000000000000000000000000000000000002",
            registrar,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000001",
            registry_a,
            WatchedContractSource::ManifestRoot,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000003",
            registry_b,
            WatchedContractSource::DiscoveryEdge,
            Some(90),
            Some(150),
        ),
        watched_contract_for_test(
            "base-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000004",
            other_chain_registry,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
    ];

    let plan = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        100,
        120,
    )?;

    assert_eq!(plan.selector_kind, WatchedSourceSelectorKind::SourceFamily);
    assert_eq!(plan.source_family.as_deref(), Some("ens_v2_registry_l1"));
    assert_eq!(
        plan.watched_chain_plan,
        WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![
                "0x0000000000000000000000000000000000000001".to_owned(),
                "0x0000000000000000000000000000000000000003".to_owned(),
            ],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 0,
            discovery_edge_entry_count: 1,
        }
    );
    assert_eq!(
        plan.selected_targets,
        vec![
            WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: registry_a,
                address: "0x0000000000000000000000000000000000000001".to_owned(),
                effective_from_block: 100,
                effective_to_block: 120,
            },
            WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: registry_b,
                address: "0x0000000000000000000000000000000000000003".to_owned(),
                effective_from_block: 100,
                effective_to_block: 120,
            },
        ]
    );

    Ok(())
}

#[test]
fn large_whole_active_source_identity_uses_compact_selected_target_digest() -> Result<()> {
    let selected_targets = (0..10_001)
        .map(|index| WatchedBackfillTarget {
            source_family: "basenames_base_registry".to_owned(),
            contract_instance_id: Uuid::from_u128(index as u128 + 1),
            address: format!("0x{index:040x}"),
            effective_from_block: index as i64,
            effective_to_block: index as i64 + 10,
        })
        .collect::<Vec<_>>();
    let plan = WatchedSourceSelectorPlan {
        chain: "base-mainnet".to_owned(),
        selector_kind: WatchedSourceSelectorKind::WholeActiveWatchedChain,
        source_family: None,
        requested_watched_targets: Vec::new(),
        selected_targets,
        watched_chain_plan: WatchedChainPlan {
            chain: "base-mainnet".to_owned(),
            addresses: Vec::new(),
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 0,
            discovery_edge_entry_count: 0,
        },
    };

    let payload = plan.source_identity_payload();

    assert_eq!(
        payload
            .get("source_identity_payload_format")
            .and_then(serde_json::Value::as_str),
        Some("selected_targets_digest_v1")
    );
    assert!(payload.get("selected_targets").is_none());
    assert_eq!(
        payload
            .get("selected_target_count")
            .and_then(serde_json::Value::as_u64),
        Some(plan.selected_targets.len() as u64)
    );
    assert!(
        payload
            .get("selected_targets_digest")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|digest| digest.starts_with("keccak256:0x"))
    );
    assert!(
        payload
            .get("source_identity_hash")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|digest| digest.starts_with("fnv1a64:"))
    );
    assert_eq!(plan.source_identity_payload(), payload);

    let mut drifted_plan = plan.clone();
    drifted_plan
        .selected_targets
        .last_mut()
        .expect("test plan has selected targets")
        .effective_to_block += 1;
    let drifted_payload = drifted_plan.source_identity_payload();
    assert_ne!(
        drifted_payload.get("selected_targets_digest"),
        payload.get("selected_targets_digest")
    );
    assert_ne!(
        drifted_payload.get("source_identity_hash"),
        payload.get("source_identity_hash")
    );

    Ok(())
}

#[test]
fn watched_selector_dynamic_resolver_backfill() -> Result<()> {
    let ens_resolver_a = Uuid::from_u128(30);
    let ens_resolver_b = Uuid::from_u128(10);
    let ens_closed_resolver = Uuid::from_u128(20);
    let ens_future_resolver = Uuid::from_u128(40);
    let ens_registry = Uuid::from_u128(50);
    let basenames_resolver = Uuid::from_u128(60);
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            "0x0000000000000000000000000000000000000030",
            ens_resolver_a,
            WatchedContractSource::DiscoveryEdge,
            Some(110),
            Some(180),
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            "0x0000000000000000000000000000000000000010",
            ens_resolver_b,
            WatchedContractSource::DiscoveryEdge,
            Some(90),
            Some(150),
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            "0x0000000000000000000000000000000000000020",
            ens_closed_resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(10),
            Some(99),
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            "0x0000000000000000000000000000000000000040",
            ens_future_resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(181),
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_registry_l1",
            "0x0000000000000000000000000000000000000050",
            ens_registry,
            WatchedContractSource::ManifestRoot,
            None,
            None,
        ),
        watched_contract_for_test(
            "base-mainnet",
            "basenames_base_resolver",
            "0x0000000000000000000000000000000000000060",
            basenames_resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(500),
            Some(700),
        ),
    ];

    let ens_plan = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_resolver_l1".to_owned()),
        100,
        175,
    )?;
    assert_eq!(
        ens_plan.selected_targets,
        vec![
            WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: ens_resolver_b,
                address: "0x0000000000000000000000000000000000000010".to_owned(),
                effective_from_block: 100,
                effective_to_block: 150,
            },
            WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: ens_resolver_a,
                address: "0x0000000000000000000000000000000000000030".to_owned(),
                effective_from_block: 110,
                effective_to_block: 175,
            },
        ]
    );
    let mut sorted_targets = ens_plan.selected_targets.clone();
    sorted_targets.sort();
    assert_eq!(ens_plan.selected_targets, sorted_targets);

    let basenames_plan = resolve_watched_source_selector(
        &watched_contracts,
        "base-mainnet",
        WatchedSourceSelector::SourceFamily("basenames_base_resolver".to_owned()),
        600,
        650,
    )?;
    assert_eq!(
        basenames_plan.selected_targets,
        vec![WatchedBackfillTarget {
            source_family: "basenames_base_resolver".to_owned(),
            contract_instance_id: basenames_resolver,
            address: "0x0000000000000000000000000000000000000060".to_owned(),
            effective_from_block: 600,
            effective_to_block: 650,
        }]
    );

    let empty_family_error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily(String::new()),
        100,
        175,
    )
    .expect_err("empty source-family selector must fail before job creation");
    assert!(
        empty_family_error
            .to_string()
            .contains("source_family  found no active watched targets"),
        "unexpected empty source-family error: {empty_family_error:#}"
    );

    let unknown_family_error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("unknown_resolver_family".to_owned()),
        100,
        175,
    )
    .expect_err("unknown source-family selector must fail before job creation");
    assert!(
        unknown_family_error
            .to_string()
            .contains("source_family unknown_resolver_family found no active watched targets"),
        "unexpected unknown source-family error: {unknown_family_error:#}"
    );

    Ok(())
}

#[test]
fn watched_selector_preserves_duplicate_identity_effective_ranges() -> Result<()> {
    let resolver = Uuid::from_u128(70);
    let resolver_address = "0x0000000000000000000000000000000000000070";
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            resolver_address,
            resolver,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v1_resolver_l1",
            resolver_address,
            resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(123),
            None,
        ),
    ];

    let plan = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_resolver_l1".to_owned()),
        100,
        200,
    )?;

    assert_eq!(
        plan.watched_chain_plan,
        WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![normalize_address(resolver_address)],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 1,
        }
    );
    assert_eq!(
        plan.selected_targets,
        vec![
            WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: resolver,
                address: normalize_address(resolver_address),
                effective_from_block: 100,
                effective_to_block: 200,
            },
            WatchedBackfillTarget {
                source_family: "ens_v1_resolver_l1".to_owned(),
                contract_instance_id: resolver,
                address: normalize_address(resolver_address),
                effective_from_block: 123,
                effective_to_block: 200,
            },
        ]
    );

    Ok(())
}

#[test]
fn explicit_watched_target_set_is_normalized_sorted_and_validated() -> Result<()> {
    let registry = Uuid::from_u128(30);
    let registrar = Uuid::from_u128(10);
    let resolver = Uuid::from_u128(20);
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000030",
            registry,
            WatchedContractSource::ManifestContract,
            Some(25),
            Some(125),
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registrar_l1",
            "0x0000000000000000000000000000000000000010",
            registrar,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_resolver_l1",
            "0x0000000000000000000000000000000000000020",
            resolver,
            WatchedContractSource::DiscoveryEdge,
            Some(110),
            Some(190),
        ),
    ];

    let plan = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![
            registry.into(),
            registrar.into(),
            registry.into(),
        ]),
        100,
        150,
    )?;

    assert_eq!(
        plan.requested_watched_targets,
        vec![
            WatchedTargetIdentity {
                contract_instance_id: registrar,
            },
            WatchedTargetIdentity {
                contract_instance_id: registry,
            },
        ]
    );
    assert_eq!(
        plan.selected_targets,
        vec![
            WatchedBackfillTarget {
                source_family: "ens_v2_registrar_l1".to_owned(),
                contract_instance_id: registrar,
                address: "0x0000000000000000000000000000000000000010".to_owned(),
                effective_from_block: 100,
                effective_to_block: 150,
            },
            WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: registry,
                address: "0x0000000000000000000000000000000000000030".to_owned(),
                effective_from_block: 100,
                effective_to_block: 125,
            },
        ]
    );
    assert_eq!(
        plan.source_identity_payload()["selector_kind"],
        "watched_target_set"
    );

    let error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![Uuid::from_u128(99).into()]),
        100,
        150,
    )
    .expect_err("unknown explicit watched target must fail");
    assert!(
        error
            .to_string()
            .contains("is not active for chain ethereum-mainnet"),
        "unexpected explicit target validation error: {error:#}"
    );

    Ok(())
}

#[test]
fn watched_selector_validation_prevents_cross_chain_leakage() {
    let registry = Uuid::from_u128(40);
    let watched_contracts = vec![watched_contract_for_test(
        "ethereum-sepolia",
        "ens_v2_registry_l1",
        "0x0000000000000000000000000000000000000040",
        registry,
        WatchedContractSource::ManifestContract,
        None,
        None,
    )];

    let family_error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        1,
        10,
    )
    .expect_err("source-family selector must not leak targets from another chain");
    assert!(
        family_error
            .to_string()
            .contains("found no active watched targets for chain ethereum-mainnet"),
        "unexpected source-family validation error: {family_error:#}"
    );

    let target_error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::WatchedTargetSet(vec![registry.into()]),
        1,
        10,
    )
    .expect_err("explicit selector must not leak targets from another chain");
    assert!(
        target_error
            .to_string()
            .contains("is not active for chain ethereum-mainnet"),
        "unexpected explicit target validation error: {target_error:#}"
    );
}

#[test]
fn watched_selector_rejects_conflicting_target_metadata() {
    let registry = Uuid::from_u128(50);
    let watched_contracts = vec![
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000050",
            registry,
            WatchedContractSource::ManifestRoot,
            None,
            None,
        ),
        watched_contract_for_test(
            "ethereum-mainnet",
            "ens_v2_registry_l1",
            "0x0000000000000000000000000000000000000051",
            registry,
            WatchedContractSource::ManifestContract,
            None,
            None,
        ),
    ];

    let error = resolve_watched_source_selector(
        &watched_contracts,
        "ethereum-mainnet",
        WatchedSourceSelector::WholeActiveWatchedChain,
        1,
        10,
    )
    .expect_err("conflicting metadata for one target identity must fail");
    assert!(
        error.to_string().contains("source identity conflict"),
        "unexpected conflict error: {error:#}"
    );
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
    assert!(repository.manifests()[0].manifest.abi.is_empty());

    Ok(())
}

#[test]
fn loads_chain_combo_repository_manifests() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest_for_chain_combo(
        "ethereum",
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
    assert_eq!(
        repository.manifests()[0].relative_path,
        PathBuf::from("ethereum/ens/ens_v2_registry_l1/v1.toml")
    );

    Ok(())
}

#[test]
fn parses_optional_start_block_on_roots_and_contracts() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &start_block_manifest_contents(
            Some(12_345),
            Some(23_456),
            "0x0000000000000000000000000000000000000003",
        ),
    )?;

    let repository = load_repository(&test_dir.path)?;
    let manifest = &repository.manifests()[0].manifest;

    assert_eq!(manifest.roots[0].start_block, Some(12_345));
    assert_eq!(manifest.contracts[0].start_block, Some(23_456));
    assert_eq!(manifest.contracts[1].start_block, None);

    Ok(())
}

#[test]
fn loads_manifest_abi_fragments() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v1", &abi_manifest_contents())?;

    let repository = load_repository(&test_dir.path)?;
    let abi = &repository.manifests()[0].manifest.abi;

    assert_eq!(abi.events.len(), 1);
    assert_eq!(abi.events[0].name, "SubregistryUpdated");
    assert_eq!(abi.events[0].emitter_roles, ["registry"]);
    assert_eq!(abi.events[0].normalized_events, ["SubregistryChanged"]);
    assert_eq!(
        abi.events[0].status,
        Some(CapabilitySupportStatus::Supported)
    );
    assert_eq!(abi.calls.len(), 1);
    assert_eq!(abi.calls[0].name, "resolver");
    assert_eq!(abi.calls[0].target_roles, ["registry"]);
    assert_eq!(abi.calls[0].status, Some(CapabilitySupportStatus::Shadow));
    let parsed_event = abi.events[0].parsed_event_view()?;
    assert_eq!(
        parsed_event.canonical_signature(),
        "SubregistryUpdated(uint256,address,address)"
    );
    assert!(
        parsed_event
            .topic0()
            .is_some_and(|topic0| topic0.starts_with("0x") && topic0.len() == 66)
    );
    let parsed_call = abi.calls[0].parsed_function_view()?;
    assert_eq!(parsed_call.canonical_signature(), "resolver(bytes32)");
    assert_eq!(parsed_call.selector().len(), 10);

    Ok(())
}

#[test]
fn normalize_address_uses_alloy_for_standard_hex_without_tightening_fallbacks() {
    assert_eq!(
        normalize_address("0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E"),
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    assert_eq!(normalize_address("NOT-A-HEX-ADDRESS"), "not-a-hex-address");
    assert_eq!(normalize_address("0xABC"), "0xabc");
    assert_eq!(
        normalize_address("00000000000000000000000000000000000000AA"),
        "00000000000000000000000000000000000000aa"
    );
}

#[test]
fn rejects_invalid_manifest_abi_fragment() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &abi_manifest_contents().replacen("event SubregistryUpdated", "SubregistryUpdated", 1),
    )?;

    let error =
        load_repository(&test_dir.path).expect_err("non-event ABI fragment must fail validation");
    assert!(
        error.to_string().contains("must use an event fragment"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_manifest_abi_unknown_roles() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &abi_manifest_contents().replacen(
            r#"emitter_roles = ["registry"]"#,
            r#"emitter_roles = ["missing_registry"]"#,
            1,
        ),
    )?;

    let error =
        load_repository(&test_dir.path).expect_err("unknown ABI emitter role must fail validation");
    assert!(
        error.to_string().contains("unknown emitter role"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_negative_start_block_values() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &start_block_manifest_contents(
            Some(-1),
            Some(23_456),
            "0x0000000000000000000000000000000000000003",
        ),
    )?;

    let error = load_repository(&test_dir.path)
        .expect_err("negative start_block must fail manifest parsing");
    assert!(
        error.to_string().contains("failed to parse manifest TOML"),
        "unexpected error: {error:#}"
    );
    assert!(
        format!("{error:#}").contains("start_block must be a non-negative integer"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_unsupported_authored_discovery_rule_admission_literals() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x0000000000000000000000000000000000000002",
            Some("0x0000000000000000000000000000000000000003"),
        )
        .replacen(
            "admission = \"reachable_from_root\"",
            "admission = \"manifest_declared\"",
            1,
        ),
    )?;

    let error = load_repository(&test_dir.path)
        .expect_err("unsupported discovery_rules[].admission must fail manifest parsing");
    assert!(
        error.to_string().contains("failed to parse manifest TOML"),
        "unexpected error: {error:#}"
    );
    assert!(
        format!("{error:#}")
            .contains("unsupported authored discovery_rules[].admission \"manifest_declared\""),
        "unexpected error: {error:#}"
    );

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
normalizer_version = "ensip15@ens-normalize-0.1.1"

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

#[test]
fn rejects_chain_combo_mismatch() -> Result<()> {
    let test_dir = TestDir::new()?;
    let path = test_dir.write_manifest_for_chain_combo(
        "base",
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

    let error = load_repository(&test_dir.path).expect_err("chain combo mismatch must fail");
    assert!(
        error.to_string().contains("does not match chain directory"),
        "unexpected error for {}: {error:#}",
        path.display()
    );

    Ok(())
}

#[test]
fn rejects_manifest_version_tag_mismatch() -> Result<()> {
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v2",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            "0x00000000000000000000000000000000000000AA",
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;

    let error = load_repository(&test_dir.path).expect_err("version tag mismatch must fail");
    assert!(
        error
            .to_string()
            .contains("manifest_version 1 does not match version tag v2"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_duplicate_contract_roles() -> Result<()> {
    let test_dir = TestDir::new()?;
    let contents = manifest_contents(
        "active",
        "0x0000000000000000000000000000000000000001",
        "0x00000000000000000000000000000000000000AA",
        Some("0x00000000000000000000000000000000000000DD"),
    ) + r#"
[[contracts]]
role = "registry"
address = "0x00000000000000000000000000000000000000BB"
proxy_kind = "none"
"#;
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v1", &contents)?;

    let error = load_repository(&test_dir.path).expect_err("duplicate roles must fail");
    assert!(
        error
            .to_string()
            .contains("duplicates contract role registry"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn rejects_unsupported_normalizer_version() -> Result<()> {
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
        )
        .replacen(
            "normalizer_version = \"ensip15@ens-normalize-0.1.1\"",
            "normalizer_version = \"ensip15@unknown\"",
            1,
        ),
    )?;

    let error = load_repository(&test_dir.path).expect_err("unsupported normalizer must fail");
    assert!(
        error
            .to_string()
            .contains("unsupported normalizer_version ensip15@unknown"),
        "unexpected error: {error:#}"
    );

    Ok(())
}

#[test]
fn checked_in_sepolia_manifests_load_as_alternate_profile() -> Result<()> {
    let main_repository = load_repository(checked_in_manifest_root("manifests/mainnet"))?;
    let sepolia_repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;

    assert_eq!(
        sepolia_repository.summary().status,
        ManifestLoadStatus::Loaded
    );
    assert_eq!(sepolia_repository.summary().namespace_count, 1);
    assert_eq!(sepolia_repository.summary().source_family_count, 4);
    assert_eq!(sepolia_repository.summary().manifest_count, 9);

    let sepolia_source_versions = sepolia_repository
        .manifests()
        .iter()
        .map(|loaded_manifest| {
            (
                loaded_manifest.manifest.source_family.as_str(),
                loaded_manifest.version_tag.as_str(),
                loaded_manifest.manifest.manifest_version,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sepolia_source_versions,
        vec![
            ("ens_v2_registrar_l1", "v1", 1),
            ("ens_v2_registrar_l1", "v2", 2),
            ("ens_v2_registrar_l1", "v3", 3),
            ("ens_v2_registry_l1", "v1", 1),
            ("ens_v2_registry_l1", "v2", 2),
            ("ens_v2_resolver_l1", "v1", 1),
            ("ens_v2_resolver_l1", "v2", 2),
            ("ens_v2_root_l1", "v1", 1),
            ("ens_v2_root_l1", "v2", 2),
        ]
    );
    assert!(!main_repository.manifests().iter().any(|loaded_manifest| {
        loaded_manifest
            .relative_path
            .starts_with("ethereum/ens/ens_v2_root_l1")
    }));
    assert!(
        !sepolia_repository
            .manifests()
            .iter()
            .any(|loaded_manifest| {
                loaded_manifest
                    .relative_path
                    .starts_with("ethereum/ens/ens_v1_registry_l1")
            })
    );

    for loaded_manifest in sepolia_repository.manifests() {
        assert_eq!(loaded_manifest.manifest.namespace, "ens");
        assert_eq!(loaded_manifest.manifest.chain, "ethereum-sepolia");
        if loaded_manifest.manifest.deployment_epoch == "ens_v2_sepolia_dev" {
            assert_eq!(
                loaded_manifest.manifest.rollout_status,
                RolloutStatus::Deprecated
            );
        } else {
            assert_eq!(
                loaded_manifest.manifest.deployment_epoch,
                "ens_v2_sepolia_post_audit"
            );
            assert_eq!(
                loaded_manifest.manifest.rollout_status,
                RolloutStatus::Active
            );
        }
    }

    let manifests_by_source_family_version = sepolia_repository
        .manifests()
        .iter()
        .map(|loaded_manifest| {
            (
                (
                    loaded_manifest.manifest.source_family.as_str(),
                    loaded_manifest.manifest.manifest_version,
                ),
                &loaded_manifest.manifest,
            )
        })
        .collect::<BTreeMap<_, _>>();

    let root_manifest = manifests_by_source_family_version[&("ens_v2_root_l1", 2)];
    assert_eq!(root_manifest.roots.len(), 1);
    assert_eq!(root_manifest.roots[0].name, "RootRegistry");
    assert_eq!(
        normalize_address(&root_manifest.roots[0].address),
        "0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c"
    );
    assert_eq!(root_manifest.contracts.len(), 1);
    assert_eq!(root_manifest.contracts[0].role, "root_registry");
    assert_eq!(
        normalize_address(&root_manifest.contracts[0].address),
        "0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c"
    );
    assert_eq!(
        root_manifest
            .abi
            .events
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "LabelRegistered",
            "LabelReserved",
            "LabelUnregistered",
            "ExpiryUpdated",
            "SubregistryUpdated",
            "ResolverUpdated",
            "TokenResource",
            "TransferSingle",
            "TransferBatch",
            "EACRolesChanged",
            "TokenRegenerated",
            "ParentUpdated",
        ]
    );

    let registry_manifest = manifests_by_source_family_version[&("ens_v2_registry_l1", 2)];
    assert_eq!(registry_manifest.roots.len(), 1);
    assert_eq!(registry_manifest.roots[0].name, "ETHRegistry");
    assert_eq!(
        normalize_address(&registry_manifest.roots[0].address),
        "0x67b728a792e789a8978b30cf1b3b641f19354b43"
    );
    assert_eq!(registry_manifest.contracts.len(), 1);
    assert_eq!(registry_manifest.contracts[0].role, "registry");
    assert_eq!(
        normalize_address(&registry_manifest.contracts[0].address),
        "0x67b728a792e789a8978b30cf1b3b641f19354b43"
    );
    assert_eq!(
        registry_manifest
            .abi
            .events
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "LabelRegistered",
            "LabelReserved",
            "LabelUnregistered",
            "ExpiryUpdated",
            "SubregistryUpdated",
            "ResolverUpdated",
            "TokenResource",
            "TransferSingle",
            "TransferBatch",
            "EACRolesChanged",
            "TokenRegenerated",
            "ParentUpdated",
        ]
    );

    let registrar_manifest_v1 = manifests_by_source_family_version[&("ens_v2_registrar_l1", 1)];
    assert_eq!(
        registrar_manifest_v1.rollout_status,
        RolloutStatus::Deprecated
    );
    assert_eq!(
        registrar_manifest_v1.capability_flags["exact_name_profile"].status,
        CapabilitySupportStatus::Shadow
    );

    let registrar_manifest_v2 = manifests_by_source_family_version[&("ens_v2_registrar_l1", 2)];
    assert_eq!(
        registrar_manifest_v2.rollout_status,
        RolloutStatus::Deprecated
    );

    let registrar_manifest = manifests_by_source_family_version[&("ens_v2_registrar_l1", 3)];
    assert_eq!(registrar_manifest.roots.len(), 1);
    assert_eq!(registrar_manifest.roots[0].name, "ETHRegistrar");
    assert_eq!(
        normalize_address(&registrar_manifest.roots[0].address),
        "0xa4449a0dd2b83007553d9b1d28b583a46a805a30"
    );
    assert_eq!(registrar_manifest.contracts.len(), 1);
    assert_eq!(registrar_manifest.contracts[0].role, "registrar");
    assert_eq!(
        normalize_address(&registrar_manifest.contracts[0].address),
        "0xa4449a0dd2b83007553d9b1d28b583a46a805a30"
    );
    assert_eq!(
        registrar_manifest.capability_flags["exact_name_profile"].status,
        CapabilitySupportStatus::Supported
    );
    assert_eq!(
        registrar_manifest
            .abi
            .events
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        vec!["NameRegistered", "NameRenewed"]
    );
    let name_registered = registrar_manifest
        .abi
        .events
        .iter()
        .find(|event| event.name == "NameRegistered")
        .expect("post-audit registrar manifest must declare NameRegistered")
        .parsed_event()?;
    assert!(
        name_registered
            .inputs
            .iter()
            .any(|input| input.name == "referrer" && input.indexed),
        "post-audit NameRegistered referrer must be indexed"
    );
    let name_renewed = registrar_manifest
        .abi
        .events
        .iter()
        .find(|event| event.name == "NameRenewed")
        .expect("post-audit registrar manifest must declare NameRenewed")
        .parsed_event()?;
    assert!(
        name_renewed
            .inputs
            .iter()
            .any(|input| input.name == "referrer" && input.indexed),
        "post-audit NameRenewed referrer must be indexed"
    );
    assert_eq!(
        name_renewed.inputs.last().map(|input| input.name.as_str()),
        Some("amount")
    );

    let resolver_manifest = manifests_by_source_family_version[&("ens_v2_resolver_l1", 2)];
    assert!(resolver_manifest.roots.is_empty());
    assert!(resolver_manifest.contracts.is_empty());
    assert!(resolver_manifest.discovery_rules.is_empty());
    assert_eq!(
        resolver_manifest
            .abi
            .events
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "AddressChanged",
            "TextChanged",
            "ContenthashChanged",
            "NameChanged",
            "VersionChanged",
            "AliasChanged",
            "NamedResource",
            "NamedTextResource",
            "NamedAddrResource",
            "EACRolesChanged",
        ]
    );

    let admitted_addresses = sepolia_repository
        .manifests()
        .iter()
        .flat_map(|loaded_manifest| {
            loaded_manifest
                .manifest
                .roots
                .iter()
                .map(|root| root.address.as_str())
                .chain(
                    loaded_manifest
                        .manifest
                        .contracts
                        .iter()
                        .map(|contract| contract.address.as_str()),
                )
        })
        .map(normalize_address)
        .collect::<Vec<_>>();
    assert!(!admitted_addresses.contains(&"0xe566a1fbaf30ff7c39828fe99f955fc55544cb9c".to_owned()));
    assert!(!admitted_addresses.contains(&"0xd25f66dd4ff61486c2c5c1e6201a23576698d3df".to_owned()));

    Ok(())
}

#[test]
fn checked_in_ens_v2_public_resolver_boundary_separates_watch_from_profile_admission() -> Result<()>
{
    let manifests_doc = fs::read_to_string(checked_in_manifest_root("docs/manifests.md"))?;
    assert!(
        manifests_doc.contains(
            "Resolver observations can discovery-admit `PublicResolverV2` as a watch-only contract instance and retain configured normalized facts, but they publish no selectors, cache values, or authoritative record coverage without explicit ENSv2 resolver-profile admission. A current-emitter `RecordVersionChanged` may remain only as an explicit `resolver_family_pending` boundary; non-current resolver emitters are always excluded."
        ),
        "ENSv2 docs must distinguish generic resolver watching from resolver-profile admission"
    );

    let resolver_manifest = fs::read_to_string(checked_in_manifest_root(
        "manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v2.toml",
    ))?;
    assert!(
        resolver_manifest.contains(
            "PublicResolverV2 can be discovery-watched for configured generic record events but is not an admitted resolver profile; projection publishes no record values, retains only a current-emitter pending version boundary, and excludes non-current emitters."
        ),
        "the active ENSv2 resolver manifest must state the watch-only PublicResolverV2 boundary"
    );

    Ok(())
}

#[tokio::test]
async fn ens_v2_public_resolver_discovery_is_watch_only_without_profile_capabilities() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let public_resolver_address = "0xd25f66dd4ff61486c2c5c1e6201a23576698d3df";
    let permissioned_resolver_address = "0x0000000000000000000000000000000000000201";
    for (address, block_number, block_hash) in [
        (
            public_resolver_address,
            100_i64,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        (
            permissioned_resolver_address,
            110_i64,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ),
    ] {
        let summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: "ethereum-sepolia".to_owned(),
                from_address: registry_address.to_owned(),
                to_address: address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "ens_v2_registry_resolver:ethereum-sepolia".to_owned(),
                active_from_block_number: Some(block_number),
                active_from_block_hash: Some(block_hash.to_owned()),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "resolver",
                    "observation_key": format!("registry:{registry_address}:resolver:{address}"),
                }),
            },
        )
        .await?;
        assert_eq!(summary.admitted_edge_count, 1);
        assert_eq!(summary.inserted_edge_count, 1);
    }

    let resolver_manifest_id =
        active_manifest_id_for_source_family(database.pool(), "ens", "ens_v2_resolver_l1").await?;
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    for address in [public_resolver_address, permissioned_resolver_address] {
        let address = normalize_address(address);
        assert!(watched_contracts.iter().any(|contract| {
            contract.chain == "ethereum-sepolia"
                && contract.source_family == "ens_v2_resolver_l1"
                && contract.address == address
                && contract.source == WatchedContractSource::DiscoveryEdge
                && contract.source_manifest_id == Some(resolver_manifest_id)
        }));
    }

    let public_resolver_direct_declaration_count = query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM manifest_contract_instances mci
        JOIN manifest_versions mv ON mv.manifest_id = mci.manifest_id
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = mci.contract_instance_id
        WHERE mv.rollout_status = 'active'
          AND cia.chain_id = 'ethereum-sepolia'
          AND cia.address = $1
        "#,
    )
    .bind(normalize_address(public_resolver_address))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(public_resolver_direct_declaration_count, 0);
    assert!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v2_resolver_l1",)
            .await?
            .is_empty(),
        "resolver-edge watching must not promote ENSv2 resolver-profile capabilities"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn syncing_sepolia_profile_replaces_main_profile_without_mixing() -> Result<()> {
    let database = TestDatabase::new().await?;
    let main_repository = load_repository(checked_in_manifest_root("manifests/mainnet"))?;
    let sepolia_repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;

    assert_eq!(main_repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(
        sepolia_repository.summary().status,
        ManifestLoadStatus::Loaded
    );
    sync_repository(database.pool(), &main_repository).await?;

    let summary = sync_repository(database.pool(), &sepolia_repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 9);
    assert_eq!(summary.active_manifest_count, 4);
    assert_eq!(summary.root_count, 7);
    assert_eq!(summary.contract_count, 7);
    assert_eq!(summary.capability_count, 8);
    assert_eq!(summary.discovery_rule_count, 6);
    assert_eq!(
        summary.removed_manifest_count,
        main_repository.manifests().len()
    );

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![
            ("ens_v2_registrar_l1".to_owned(), "deprecated".to_owned()),
            ("ens_v2_registrar_l1".to_owned(), "deprecated".to_owned()),
            ("ens_v2_registrar_l1".to_owned(), "active".to_owned()),
            ("ens_v2_registry_l1".to_owned(), "deprecated".to_owned()),
            ("ens_v2_registry_l1".to_owned(), "active".to_owned()),
            ("ens_v2_resolver_l1".to_owned(), "deprecated".to_owned()),
            ("ens_v2_resolver_l1".to_owned(), "active".to_owned()),
            ("ens_v2_root_l1".to_owned(), "deprecated".to_owned()),
            ("ens_v2_root_l1".to_owned(), "active".to_owned()),
        ]
    );
    assert_eq!(
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v2_registry_l1",
            2,
        )
        .await?,
        BTreeMap::from([(
            "declared_children".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Supported,
                notes: Some(
                    "post-audit Sepolia ETHRegistry and discovered user registries are authoritative declared child inputs within the selected profile"
                        .to_owned(),
                ),
            },
        )])
    );
    assert_eq!(
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v2_registrar_l1",
            1
        )
        .await?,
        BTreeMap::from([
            (
                "exact_name_profile".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Shadow,
                    notes: Some(
                        "sepolia-dev registrar lifecycle facts are admitted before product reads depend on them"
                            .to_owned(),
                    ),
                },
            ),
            (
                "name_history".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Shadow,
                    notes: Some("sepolia-dev registrar history remains downstream work".to_owned()),
                },
            ),
        ])
    );
    assert_eq!(
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v2_registrar_l1",
            3
        )
        .await?,
        BTreeMap::from([
            (
                "exact_name_profile".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Supported,
                    notes: Some(
                        "selected post-audit Sepolia exact-name profile reads are supported from admitted ETHRegistry and ETHRegistrar sources only"
                            .to_owned(),
                    ),
                },
            ),
            (
                "name_history".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Shadow,
                    notes: Some(
                        "post-audit Sepolia registrar history remains downstream work".to_owned(),
                    ),
                },
            ),
        ])
    );
    assert_eq!(
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v2_registrar_l1",
            2
        )
        .await?,
        BTreeMap::from([
            (
                "exact_name_profile".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Supported,
                    notes: Some(
                        "selected sepolia-dev exact-name profile reads are supported from admitted ETHRegistry and ETHRegistrar sources only"
                            .to_owned(),
                    ),
                },
            ),
            (
                "name_history".to_owned(),
                CapabilityFlag {
                    status: CapabilitySupportStatus::Shadow,
                    notes: Some("sepolia-dev registrar history remains downstream work".to_owned()),
                },
            ),
        ])
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v2_root_l1").await?,
        BTreeMap::new()
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v2_resolver_l1")
            .await?,
        BTreeMap::new()
    );
    assert!(
        load_manifest_rollout_statuses(database.pool(), "basenames")
            .await?
            .is_empty()
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(active_manifests.len(), 4);
    assert!(
        active_manifests
            .iter()
            .all(|manifest| manifest.chain == "ethereum-sepolia")
    );
    assert!(
        active_manifests
            .iter()
            .all(|manifest| manifest.deployment_epoch == "ens_v2_sepolia_post_audit")
    );
    assert!(
        !active_manifests
            .iter()
            .any(|manifest| manifest.source_family.starts_with("ens_v1_"))
    );
    assert!(active_manifests.iter().any(|manifest| {
        manifest.source_family == "ens_v2_registrar_l1"
            && manifest.manifest_version == 3
            && manifest.capability_flags["exact_name_profile"].status
                == CapabilitySupportStatus::Supported
    }));

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert!(
        watched_contracts
            .iter()
            .all(|contract| contract.chain == "ethereum-sepolia")
    );
    assert!(!watched_contracts.iter().any(|contract| {
        contract.address == normalize_address("0x7e4b2d59938930168024201752ee5503df402303")
    }));
    assert!(!watched_contracts.iter().any(|contract| {
        contract.address == normalize_address("0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E")
    }));

    // Simulate a database that observed resolver discovery while the old
    // sepolia-dev registry profile was selected. The retained edge remains
    // useful replay history, but the now-deprecated source and target
    // manifests must not make it coverage-authoritative in the post-audit
    // profile.
    let (deprecated_registry_manifest_id, deprecated_epoch) = sqlx::query_as::<_, (i64, String)>(
        r#"
            SELECT manifest_id, deployment_epoch
            FROM manifest_versions
            WHERE chain = 'ethereum-sepolia'
              AND source_family = 'ens_v2_registry_l1'
              AND rollout_status = 'deprecated'
            ORDER BY manifest_version DESC
            LIMIT 1
            "#,
    )
    .fetch_one(database.pool())
    .await?;
    let deprecated_registry_instance_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT contract_instance_id
        FROM manifest_contract_instances
        WHERE manifest_id = $1
        ORDER BY contract_instance_id
        LIMIT 1
        "#,
    )
    .bind(deprecated_registry_manifest_id)
    .fetch_one(database.pool())
    .await?;
    let deprecated_resolver_manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT manifest_id
        FROM manifest_versions
        WHERE chain = 'ethereum-sepolia'
          AND deployment_epoch = $1
          AND source_family = 'ens_v2_resolver_l1'
          AND rollout_status = 'deprecated'
        ORDER BY manifest_version DESC
        LIMIT 1
        "#,
    )
    .bind(&deprecated_epoch)
    .fetch_one(database.pool())
    .await?;
    let deprecated_discovered_instance_id = Uuid::new_v4();
    let deprecated_discovered_address = "0x0000000000000000000000000000000000000d3a";
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        VALUES ($1, 'ethereum-sepolia', 'resolver', '{}'::jsonb)
        "#,
    )
    .bind(deprecated_discovered_instance_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            deactivated_at,
            active_from_block_number,
            active_to_block_number,
            source_manifest_id,
            provenance
        )
        VALUES ($1, 'ethereum-sepolia', lower($2), now(), 11163403, 11164000, $3, '{}'::jsonb)
        "#,
    )
    .bind(deprecated_discovered_instance_id)
    .bind(deprecated_discovered_address)
    .bind(deprecated_resolver_manifest_id)
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
            deactivated_at,
            active_from_block_number,
            active_to_block_number,
            provenance
        )
        VALUES (
            'ethereum-sepolia',
            'resolver',
            $1,
            $2,
            'legacy-sepolia-dev-test',
            $3,
            'reachable_from_root',
            now(),
            11163403,
            11164000,
            '{}'::jsonb
        )
        "#,
    )
    .bind(deprecated_registry_instance_id)
    .bind(deprecated_discovered_instance_id)
    .bind(deprecated_registry_manifest_id)
    .execute(database.pool())
    .await?;

    assert_eq!(
        load_watched_chain_plan(database.pool()).await?,
        vec![WatchedChainPlan {
            chain: "ethereum-sepolia".to_owned(),
            addresses: vec![
                "0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c".to_owned(),
                "0x67b728a792e789a8978b30cf1b3b641f19354b43".to_owned(),
                "0xa4449a0dd2b83007553d9b1d28b583a46a805a30".to_owned(),
            ],
            manifest_root_entry_count: 3,
            manifest_contract_entry_count: 3,
            discovery_edge_entry_count: 0,
        }]
    );

    assert_eq!(
        load_required_watched_tuples(
            database.pool(),
            "ethereum-sepolia",
            11_163_403,
            11_164_000,
            &[
                "ens_v2_registrar_l1".to_owned(),
                "ens_v2_registry_l1".to_owned(),
                "ens_v2_resolver_l1".to_owned(),
                "ens_v2_root_l1".to_owned(),
            ],
        )
        .await?,
        vec![
            RequiredWatchedTuple {
                source_family: "ens_v2_registrar_l1".to_owned(),
                address: "0xa4449a0dd2b83007553d9b1d28b583a46a805a30".to_owned(),
                required_from_block: 11_163_403,
                required_to_block: 11_164_000,
            },
            RequiredWatchedTuple {
                source_family: "ens_v2_registry_l1".to_owned(),
                address: "0x67b728a792e789a8978b30cf1b3b641f19354b43".to_owned(),
                required_from_block: 11_163_403,
                required_to_block: 11_164_000,
            },
            RequiredWatchedTuple {
                source_family: "ens_v2_root_l1".to_owned(),
                address: "0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c".to_owned(),
                required_from_block: 11_163_403,
                required_to_block: 11_164_000,
            },
        ],
        "deprecated sepolia-dev declarations and discovery intervals must not create coverage requirements"
    );
    let deprecated_address = normalize_address(deprecated_discovered_address);
    assert!(
        load_historical_watched_contracts_by_chain(database.pool(), "ethereum-sepolia")
            .await?
            .iter()
            .all(|contract| contract.address != deprecated_address),
        "deprecated-profile discovery intervals are audit evidence, not historical replay authority"
    );
    assert!(
        load_watched_contracts_by_addresses(
            database.pool(),
            &[("ethereum-sepolia".to_owned(), deprecated_address.clone())],
        )
        .await?
        .is_empty(),
        "deprecated-profile intervals must not enter scoped current watch reads"
    );

    let watched_summary = load_watched_contract_summary(database.pool()).await?;
    assert_eq!(watched_summary.unique_contract_count, 3);
    assert_eq!(watched_summary.source_entry_count, 6);
    assert_eq!(watched_summary.manifest_root_count, 3);
    assert_eq!(watched_summary.manifest_contract_count, 3);
    assert_eq!(watched_summary.discovery_edge_count, 0);

    let admission_state = load_discovery_admission_state(database.pool()).await?;
    assert_eq!(admission_state.active_manifest_count, 4);
    assert_eq!(admission_state.active_root_count, 3);
    assert_eq!(admission_state.active_contract_count, 3);
    assert_eq!(admission_state.active_rule_count, 3);
    assert!(admission_state.has_authoritative_address(
        "ethereum-sepolia",
        "0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c"
    ));
    assert!(admission_state.has_authoritative_address(
        "ethereum-sepolia",
        "0x67b728a792e789a8978b30cf1b3b641f19354b43"
    ));
    assert!(admission_state.has_authoritative_address(
        "ethereum-sepolia",
        "0xa4449a0dd2b83007553d9b1d28b583a46a805a30"
    ));
    assert!(!admission_state.has_authoritative_address(
        "ethereum-sepolia",
        "0x7e4b2d59938930168024201752ee5503df402303"
    ));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn active_manifest_abi_events_derive_topics_from_payload() -> Result<()> {
    let database = TestDatabase::new().await?;
    let sepolia_repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &sepolia_repository).await?;

    let registry_manifest_id =
        active_manifest_id_for_source_family(database.pool(), "ens", "ens_v2_registry_l1").await?;
    let events = load_active_manifest_abi_events(database.pool(), &[registry_manifest_id]).await?;

    assert_eq!(events.len(), 12);
    let label_registered = events
        .iter()
        .find(|event| event.name == "LabelRegistered")
        .expect("registry manifest must declare LabelRegistered ABI");
    assert_eq!(label_registered.manifest_id, registry_manifest_id);
    assert_eq!(label_registered.source_family, "ens_v2_registry_l1");
    assert_eq!(
        label_registered.canonical_signature,
        "LabelRegistered(uint256,bytes32,string,address,uint64,address)"
    );
    assert!(
        label_registered
            .topic0
            .as_ref()
            .is_some_and(|topic0| topic0.starts_with("0x") && topic0.len() == 66)
    );
    assert_eq!(label_registered.emitter_roles, ["registry"]);
    assert_eq!(label_registered.normalized_events, ["RegistrationGranted"]);

    for (name, signature) in [
        (
            "TransferSingle",
            "TransferSingle(address,address,address,uint256,uint256)",
        ),
        (
            "TransferBatch",
            "TransferBatch(address,address,address,uint256[],uint256[])",
        ),
    ] {
        let transfer = events
            .iter()
            .find(|event| event.name == name)
            .unwrap_or_else(|| panic!("registry manifest must declare {name} ABI"));
        assert_eq!(transfer.canonical_signature, signature);
        assert_eq!(transfer.emitter_roles, ["registry"]);
        assert_eq!(transfer.normalized_events, ["TokenControlTransferred"]);
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn syncs_start_blocks_into_watch_plan_and_bootstrap_targets() -> Result<()> {
    let database = TestDatabase::new().await?;
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &start_block_manifest_contents(
            Some(120),
            Some(100),
            "0x0000000000000000000000000000000000000003",
        ),
    )?;
    let repository = load_repository(&test_dir.path)?;

    sync_repository(database.pool(), &repository).await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_to_block_number = 160
        WHERE chain_id = 'ethereum-mainnet'
          AND address = $1
        "#,
    )
    .bind("0x0000000000000000000000000000000000000002")
    .execute(database.pool())
    .await
    .context("failed to constrain registry active range")?;

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    let root = watched_contracts
        .iter()
        .find(|contract| contract.address == "0x0000000000000000000000000000000000000001")
        .expect("root target must be watched");
    let registry = watched_contracts
        .iter()
        .find(|contract| contract.address == "0x0000000000000000000000000000000000000002")
        .expect("registry target must be watched");
    let unknown_start = watched_contracts
        .iter()
        .find(|contract| contract.address == "0x0000000000000000000000000000000000000003")
        .expect("unknown-start target must be watched");

    assert_eq!(root.active_from_block_number, Some(120));
    assert_eq!(root.active_to_block_number, None);
    assert_eq!(registry.active_from_block_number, Some(100));
    assert_eq!(registry.active_to_block_number, Some(160));
    assert_eq!(unknown_start.active_from_block_number, None);
    assert_eq!(unknown_start.active_to_block_number, None);

    let stored_registry_start = query_scalar::<_, Option<i64>>(
        r#"
        SELECT active_from_block_number
        FROM contract_instance_addresses
        WHERE chain_id = 'ethereum-mainnet'
          AND address = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind("0x0000000000000000000000000000000000000002")
    .fetch_one(database.pool())
    .await
    .context("failed to load stored registry start")?;
    assert_eq!(stored_registry_start, Some(100));

    let selector_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        50,
        180,
    )
    .await?;
    assert!(
        selector_plan
            .selected_targets
            .contains(&WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: root.contract_instance_id,
                address: root.address.clone(),
                effective_from_block: 120,
                effective_to_block: 180,
            })
    );
    assert!(
        selector_plan
            .selected_targets
            .contains(&WatchedBackfillTarget {
                source_family: "ens_v2_registry_l1".to_owned(),
                contract_instance_id: registry.contract_instance_id,
                address: registry.address.clone(),
                effective_from_block: 100,
                effective_to_block: 160,
            })
    );

    let bootstrap_targets =
        load_manifest_declared_bootstrap_targets(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(bootstrap_targets.len(), 2);
    assert!(bootstrap_targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v2_registry_l1".to_owned(),
        contract_instance_id: root.contract_instance_id,
        address: root.address.clone(),
        effective_from_block: 120,
        effective_to_block: None,
    }));
    assert!(bootstrap_targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v2_registry_l1".to_owned(),
        contract_instance_id: registry.contract_instance_id,
        address: registry.address.clone(),
        effective_from_block: 100,
        effective_to_block: Some(160),
    }));
    assert!(
        !bootstrap_targets
            .iter()
            .any(|target| target.address == unknown_start.address)
    );
    let skipped_targets =
        load_manifest_skipped_bootstrap_targets(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(
        skipped_targets,
        vec![ManifestBootstrapSkippedTarget {
            source_family: "ens_v2_registry_l1".to_owned(),
            contract_instance_id: unknown_start.contract_instance_id,
            address: unknown_start.address.clone(),
            skip_reason: "unknown_start".to_owned(),
        }]
    );
    let mut sorted_targets = bootstrap_targets.clone();
    sorted_targets.sort();
    assert_eq!(bootstrap_targets, sorted_targets);
    assert!(
        load_manifest_declared_bootstrap_targets(database.pool(), "base-mainnet")
            .await?
            .is_empty()
    );
    assert!(
        load_manifest_skipped_bootstrap_targets(database.pool(), "base-mainnet")
            .await?
            .is_empty()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn simple_contract_start_block_persists_to_active_address_row() -> Result<()> {
    let database = TestDatabase::new().await?;
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "ens",
        "ens_v1_reverse_l1",
        "v1",
        &simple_contract_start_block_manifest_contents(),
    )?;

    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let active_from_block_number = query_scalar::<_, Option<i64>>(
        r#"
        SELECT active_from_block_number
        FROM contract_instance_addresses
        WHERE chain_id = 'ethereum-mainnet'
          AND address = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind("0x0000000000000000000000000000000000000042")
    .fetch_one(database.pool())
    .await
    .context("failed to load simple contract active start block")?;
    assert_eq!(active_from_block_number, Some(4242));

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    let watched_contract = watched_contracts
        .iter()
        .find(|contract| contract.address == "0x0000000000000000000000000000000000000042")
        .expect("simple contract must enter the watched plan");
    assert_eq!(watched_contract.active_from_block_number, Some(4242));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn rejects_conflicting_active_start_blocks_for_same_contract_instance() -> Result<()> {
    let database = TestDatabase::new().await?;
    let test_dir = TestDir::new()?;
    let v1 = start_block_manifest_contents(
        Some(100),
        None,
        "0x0000000000000000000000000000000000000003",
    );
    let v2 = v1
        .replacen("manifest_version = 1", "manifest_version = 2", 1)
        .replacen("start_block = 100", "start_block = 200", 1);
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v1", &v1)?;
    test_dir.write_manifest("ens", "ens_v2_registry_l1", "v2", &v2)?;
    let repository = load_repository(&test_dir.path)?;

    let error = sync_repository(database.pool(), &repository)
        .await
        .expect_err("conflicting active start blocks must fail sync");
    assert!(
        error
            .to_string()
            .contains("conflicting start_block declarations"),
        "unexpected conflict error: {error:#}"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn checked_in_registry_manifests_admit_resolver_discovery() -> Result<()> {
    for case in [
        (
            "ens",
            "ens_v1_registry_l1",
            "ens_v1_resolver_l1",
            "v3",
            3_u64,
            9_usize,
            "ethereum-mainnet",
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0xF29100983E058B709F3D539b0c765937B804AC15",
            22_764_828,
            22_764_928,
            22_764_850,
            [
                "(upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f)",
                "(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89 @ ens_v1@91c966f)",
                "(upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174 @ ens_v1@91c966f)",
            ],
        ),
        (
            "basenames",
            "basenames_base_registry",
            "basenames_base_resolver",
            "v2",
            2_u64,
            2_usize,
            "base-mainnet",
            "0xb94704422c2a1e396835a571837aa5ae53285a95",
            "0xC6d566A56A1aFf6508b41f6c90ff131615583BCD",
            100,
            200,
            123,
            [
                "(upstream: .refs/basenames/README.md:L28 @ basenames@1809bbc)",
                "(upstream: .refs/basenames/src/L2/Registry.sol:L113 @ basenames@1809bbc)",
                "(upstream: .refs/basenames/src/L2/Registry.sol:L132 @ basenames@1809bbc)",
            ],
        ),
    ] {
        let (
            namespace,
            source_family,
            resolver_source_family,
            registry_version_tag,
            registry_manifest_version,
            expected_contract_count,
            chain,
            registry_address,
            resolver_address,
            resolver_range_start,
            resolver_range_end,
            resolver_discovery_from,
            citations,
        ) = case;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;
        let manifest =
            checked_in_manifest_contents(namespace, source_family, registry_version_tag)?;

        for citation in citations {
            assert!(
                manifest.contains(citation),
                "{namespace}/{source_family}/{registry_version_tag} manifest is missing upstream citation {citation}"
            );
        }

        let resolver_manifest =
            checked_in_manifest_contents(namespace, resolver_source_family, "v1")?;
        test_dir.write_manifest(namespace, source_family, registry_version_tag, &manifest)?;
        test_dir.write_manifest(namespace, resolver_source_family, "v1", &resolver_manifest)?;
        let repository = load_repository(&test_dir.path)?;
        assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
        assert_eq!(repository.manifests().len(), 2);

        let loaded_manifest = &repository
            .manifests()
            .iter()
            .find(|loaded_manifest| {
                loaded_manifest.manifest.source_family == source_family
                    && loaded_manifest.manifest.manifest_version == registry_manifest_version
            })
            .expect("registry manifest must load")
            .manifest;
        assert_eq!(loaded_manifest.manifest_version, registry_manifest_version);
        assert_eq!(loaded_manifest.rollout_status, RolloutStatus::Active);
        assert_eq!(
            loaded_manifest.discovery_rules,
            vec![
                DiscoveryRule {
                    edge_kind: "subregistry".to_owned(),
                    from_role: "registry".to_owned(),
                    admission: "reachable_from_root".to_owned(),
                },
                DiscoveryRule {
                    edge_kind: "resolver".to_owned(),
                    from_role: "registry".to_owned(),
                    admission: "reachable_from_root".to_owned(),
                },
            ]
        );

        let summary = sync_repository(database.pool(), &repository).await?;
        assert_eq!(summary.status, ManifestSyncStatus::Synced);
        assert_eq!(summary.synced_manifest_count, 2);
        assert_eq!(summary.active_manifest_count, 2);
        assert_eq!(summary.root_count, 1);
        assert_eq!(summary.contract_count, expected_contract_count);
        assert_eq!(summary.capability_count, 1);
        assert_eq!(summary.discovery_rule_count, 2);

        let active_manifests =
            load_active_manifests_for_namespace(database.pool(), namespace).await?;
        assert_eq!(active_manifests.len(), 2);
        assert!(active_manifests.iter().any(|manifest| {
            manifest.source_family == source_family
                && manifest.manifest_version == registry_manifest_version
        }));
        assert!(active_manifests.iter().any(|manifest| {
            manifest.source_family == resolver_source_family && manifest.manifest_version == 1
        }));
        let registry_manifest_id =
            active_manifest_id_for_source_family(database.pool(), namespace, source_family).await?;
        let resolver_manifest_id = active_manifest_id_for_source_family(
            database.pool(),
            namespace,
            resolver_source_family,
        )
        .await?;

        let admission_state = load_discovery_admission_state(database.pool()).await?;
        assert_eq!(admission_state.active_manifest_count, 2);
        assert_eq!(admission_state.active_rule_count, 2);
        assert!(admission_state.has_authoritative_address(chain, registry_address));
        assert!(admission_state.has_authoritative_address(chain, resolver_address));

        let persistence_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "registry_resolver_observation".to_owned(),
                active_from_block_number: Some(resolver_discovery_from),
                active_from_block_hash: Some(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                ),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "resolver",
                }),
            },
        )
        .await?;
        assert_eq!(persistence_summary.admitted_edge_count, 1);
        assert_eq!(persistence_summary.inserted_edge_count, 1);
        assert_eq!(persistence_summary.admitted_edges[0].edge_kind, "resolver");
        assert_eq!(
            persistence_summary.admitted_edges[0].admission,
            "reachable_from_root"
        );
        assert_eq!(persistence_summary.admitted_edges[0].from_role, "registry");
        assert_eq!(
            persistence_summary.admitted_edges[0].source_manifest_id,
            registry_manifest_id
        );

        let resolver_address = normalize_address(resolver_address);
        let watched_contracts = load_watched_contracts(database.pool()).await?;
        assert!(watched_contracts.iter().any(|contract| {
            contract.chain == chain
                && contract.source_family == resolver_source_family
                && contract.address == resolver_address
                && contract.source == WatchedContractSource::DiscoveryEdge
                && contract.source_manifest_id == Some(resolver_manifest_id)
        }));
        let resolver_contract_instance_id = persistence_summary.admitted_edges[0]
            .to_contract_instance_id
            .expect("resolver discovery must admit a target contract instance");
        let resolver_source_plan = load_watched_source_selector_plan(
            database.pool(),
            chain,
            WatchedSourceSelector::SourceFamily(resolver_source_family.to_owned()),
            resolver_range_start,
            resolver_range_end,
        )
        .await?;
        assert!(resolver_source_plan.selected_targets.iter().any(|target| {
            target.source_family == resolver_source_family
                && target.contract_instance_id == resolver_contract_instance_id
                && target.address == resolver_address
                && target.effective_from_block == resolver_range_start
                && target.effective_to_block == resolver_range_end
        }));
        assert!(resolver_source_plan.selected_targets.iter().any(|target| {
            target.source_family == resolver_source_family
                && target.contract_instance_id == resolver_contract_instance_id
                && target.address == resolver_address
                && target.effective_from_block == resolver_discovery_from
                && target.effective_to_block == resolver_range_end
        }));
        let discovery_edge = sqlx::query(
            r#"
            SELECT source_manifest_id, provenance
            FROM discovery_edges
            WHERE edge_kind = 'resolver'
              AND deactivated_at IS NULL
            "#,
        )
        .fetch_one(database.pool())
        .await?;
        assert_eq!(
            discovery_edge
                .try_get::<Option<i64>, _>("source_manifest_id")?
                .expect("resolver discovery edge must retain source manifest provenance"),
            registry_manifest_id
        );
        assert!(
            !discovery_edge
                .try_get::<serde_json::Value, _>("provenance")?
                .as_object()
                .expect("resolver discovery provenance must be an object")
                .contains_key(PROPAGATED_ROLE_PROVENANCE_FIELD)
        );

        database.cleanup().await?;
    }

    Ok(())
}

#[tokio::test]
async fn checked_in_ens_registry_v3_admits_current_and_old_registry_targets() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let registry_v2_manifest = checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v2")?;
    let registry_v3_manifest = checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?;

    for citation in [
        "(upstream: .refs/ens_subgraph/subgraph.yaml:L15 @ ens_subgraph@723f1b6)",
        "(upstream: .refs/ens_subgraph/subgraph.yaml:L39 @ ens_subgraph@723f1b6)",
        "(upstream: .refs/ens_subgraph/subgraph.yaml:L42 @ ens_subgraph@723f1b6)",
        "(upstream: .refs/ens_subgraph/subgraph.yaml:L44 @ ens_subgraph@723f1b6)",
    ] {
        assert!(
            registry_v3_manifest.contains(citation),
            "ENSv1 registry v3 manifest is missing upstream citation {citation}"
        );
    }

    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v2", &registry_v2_manifest)?;
    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v3", &registry_v3_manifest)?;

    let repository = load_repository(&test_dir.path)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.manifests().len(), 2);

    let manifests_by_version = repository
        .manifests()
        .iter()
        .map(|loaded_manifest| {
            (
                loaded_manifest.manifest.manifest_version,
                &loaded_manifest.manifest,
            )
        })
        .collect::<BTreeMap<_, _>>();
    let registry_v2 = manifests_by_version[&2_u64];
    let registry_v3 = manifests_by_version[&3_u64];

    assert_eq!(registry_v2.rollout_status, RolloutStatus::Deprecated);
    assert_eq!(registry_v3.rollout_status, RolloutStatus::Active);
    assert_eq!(registry_v3.roots.len(), 1);
    assert_eq!(registry_v3.roots[0].start_block, Some(9_380_380));
    assert_eq!(registry_v3.contracts.len(), 2);

    let current_contract = registry_v3
        .contracts
        .iter()
        .find(|contract| contract.role == "registry")
        .expect("current registry role must be present");
    let old_contract = registry_v3
        .contracts
        .iter()
        .find(|contract| contract.role == "registry_old")
        .expect("old registry role must be present");
    assert_eq!(
        normalize_address(&current_contract.address),
        "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e"
    );
    assert_eq!(current_contract.start_block, Some(9_380_380));
    assert_eq!(
        normalize_address(&old_contract.address),
        "0x314159265dd8dbb310642f98f50c066173c1259b"
    );
    assert_eq!(old_contract.start_block, Some(3_327_417));
    assert_eq!(
        registry_v3.capability_flags, registry_v2.capability_flags,
        "v3 must not change ENSv1 registry capability flags"
    );
    let registry_v3_capability_flags = registry_v3.capability_flags.clone();

    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 1);
    assert_eq!(summary.root_count, 2);
    assert_eq!(summary.contract_count, 3);
    assert_eq!(summary.capability_count, 2);
    assert_eq!(summary.discovery_rule_count, 4);

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![
            ("ens_v1_registry_l1".to_owned(), "deprecated".to_owned()),
            ("ens_v1_registry_l1".to_owned(), "active".to_owned()),
        ]
    );
    assert_eq!(
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v1_registry_l1",
            3
        )
        .await?,
        load_capability_flags_for_source_family_version(
            database.pool(),
            "ens",
            "ens_v1_registry_l1",
            2
        )
        .await?
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(active_manifests.len(), 1);
    assert_eq!(active_manifests[0].source_family, "ens_v1_registry_l1");
    assert_eq!(active_manifests[0].manifest_version, 3);
    assert_eq!(
        active_manifests[0].capability_flags,
        registry_v3_capability_flags
    );

    let current_registry = normalize_address("0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E");
    let old_registry = normalize_address("0x314159265dd8dbb310642f98f50c066173c1259b");
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert_eq!(watched_contracts.len(), 3);

    let watched_current_root = watched_contracts
        .iter()
        .find(|contract| {
            contract.address == current_registry
                && contract.source == WatchedContractSource::ManifestRoot
        })
        .expect("current registry root must be watched");
    let watched_current_contract = watched_contracts
        .iter()
        .find(|contract| {
            contract.address == current_registry
                && contract.source == WatchedContractSource::ManifestContract
        })
        .expect("current registry contract must be watched");
    let watched_old_contract = watched_contracts
        .iter()
        .find(|contract| {
            contract.address == old_registry
                && contract.source == WatchedContractSource::ManifestContract
        })
        .expect("old registry contract must be watched");

    assert_eq!(
        watched_current_root.contract_instance_id,
        watched_current_contract.contract_instance_id
    );
    assert_ne!(
        watched_current_contract.contract_instance_id,
        watched_old_contract.contract_instance_id
    );
    assert_eq!(
        watched_current_contract.active_from_block_number,
        Some(9_380_380)
    );
    assert_eq!(
        watched_old_contract.active_from_block_number,
        Some(3_327_417)
    );

    let current_contract_instance_id = watched_current_contract.contract_instance_id;
    let old_contract_instance_id = watched_old_contract.contract_instance_id;

    assert_eq!(
        load_watched_chain_plan(database.pool()).await?,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![current_registry.clone(), old_registry.clone()],
            manifest_root_entry_count: 1,
            manifest_contract_entry_count: 2,
            discovery_edge_entry_count: 0,
        }]
    );

    let selector_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v1_registry_l1".to_owned()),
        0,
        10_000_000,
    )
    .await?;
    assert_eq!(selector_plan.selected_targets.len(), 2);
    assert!(
        selector_plan
            .selected_targets
            .contains(&WatchedBackfillTarget {
                source_family: "ens_v1_registry_l1".to_owned(),
                contract_instance_id: current_contract_instance_id,
                address: current_registry.clone(),
                effective_from_block: 9_380_380,
                effective_to_block: 10_000_000,
            })
    );
    assert!(
        selector_plan
            .selected_targets
            .contains(&WatchedBackfillTarget {
                source_family: "ens_v1_registry_l1".to_owned(),
                contract_instance_id: old_contract_instance_id,
                address: old_registry.clone(),
                effective_from_block: 3_327_417,
                effective_to_block: 10_000_000,
            })
    );

    let bootstrap_targets =
        load_manifest_declared_bootstrap_targets(database.pool(), "ethereum-mainnet").await?;
    assert_eq!(bootstrap_targets.len(), 2);
    assert!(bootstrap_targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v1_registry_l1".to_owned(),
        contract_instance_id: current_contract_instance_id,
        address: current_registry,
        effective_from_block: 9_380_380,
        effective_to_block: None,
    }));
    assert!(bootstrap_targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v1_registry_l1".to_owned(),
        contract_instance_id: old_contract_instance_id,
        address: old_registry,
        effective_from_block: 3_327_417,
        effective_to_block: None,
    }));
    assert!(
        load_manifest_skipped_bootstrap_targets(database.pool(), "ethereum-mainnet")
            .await?
            .is_empty()
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ens_v1_resolver_public_resolver_profile_admission_keeps_unknowns_watch_only() -> Result<()>
{
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let registry_manifest = checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?;
    let resolver_manifest = checked_in_manifest_contents("ens", "ens_v1_resolver_l1", "v1")?;
    test_dir.write_manifest("ens", "ens_v1_registry_l1", "v3", &registry_manifest)?;
    test_dir.write_manifest("ens", "ens_v1_resolver_l1", "v1", &resolver_manifest)?;

    let repository = load_repository(&test_dir.path)?;
    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.active_manifest_count, 2);

    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let public_resolver_seed_address = "0xF29100983E058B709F3D539b0c765937B804AC15";
    let supported_resolver_address = "0x0000000000000000000000000000000000000201";
    let pending_resolver_address = "0x0000000000000000000000000000000000000202";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000000203";
    let public_resolver_code_hash = "keccak256:ens-v1-public-resolver-compatible";
    let unsupported_code_hash = "keccak256:unknown-resolver";

    let seed_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        public_resolver_seed_address,
    )
    .await?;

    let supported_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: supported_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(100),
            active_from_block_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "public-resolver-code-hash-match",
            }),
        },
    )
    .await?;
    let pending_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: pending_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(110),
            active_from_block_hash: Some(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "pending-code-hash",
            }),
        },
    )
    .await?;
    let unsupported_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: unsupported_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(120),
            active_from_block_hash: Some(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "unsupported-code-hash",
            }),
        },
    )
    .await?;

    assert_eq!(supported_summary.admitted_edge_count, 1);
    assert_eq!(pending_summary.admitted_edge_count, 1);
    assert_eq!(unsupported_summary.admitted_edge_count, 1);
    let supported_contract_instance_id = supported_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("supported resolver discovery must admit a target");
    let pending_contract_instance_id = pending_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("pending resolver discovery must admit a target");
    let unsupported_contract_instance_id = unsupported_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("unsupported resolver discovery must admit a target");

    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1111111111111111111111111111111111111111111111111111111111111111",
            block_number: 100,
            contract_address: public_resolver_seed_address,
            code_hash: public_resolver_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x2222222222222222222222222222222222222222222222222222222222222222",
            block_number: 110,
            contract_address: supported_resolver_address,
            code_hash: public_resolver_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x3333333333333333333333333333333333333333333333333333333333333333",
            block_number: 120,
            contract_address: unsupported_resolver_address,
            code_hash: unsupported_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    for (address, contract_instance_id) in [
        (supported_resolver_address, supported_contract_instance_id),
        (pending_resolver_address, pending_contract_instance_id),
        (
            unsupported_resolver_address,
            unsupported_contract_instance_id,
        ),
    ] {
        let address = normalize_address(address);
        assert!(
            watched_contracts.iter().any(|contract| {
                contract.source_family == "ens_v1_resolver_l1"
                    && contract.address == address
                    && contract.contract_instance_id == contract_instance_id
                    && contract.source == WatchedContractSource::DiscoveryEdge
            }),
            "dynamic resolver {address} must remain an admitted watch target"
        );
    }

    let admissions = load_ens_v1_public_resolver_profile_admissions(database.pool()).await?;
    assert_eq!(admissions.len(), 118);

    assert_ens_v1_profile_admission_rows_with_statuses(
        &admissions,
        EnsV1ProfileAdmissionStatusExpectation {
            address: public_resolver_seed_address,
            profile: "public_resolver_compatible",
            fact_statuses: latest_public_resolver_fact_statuses(),
            admission_basis: "manifest_public_resolver_seed",
            contract_instance_id: seed_contract_instance_id,
            observed_code_hash: Some(public_resolver_code_hash),
            matched_code_hash: Some(public_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert_ens_v1_profile_admission_rows_with_statuses(
        &admissions,
        EnsV1ProfileAdmissionStatusExpectation {
            address: supported_resolver_address,
            profile: "public_resolver_compatible",
            fact_statuses: latest_public_resolver_fact_statuses(),
            admission_basis: "code_hash_match",
            contract_instance_id: supported_contract_instance_id,
            observed_code_hash: Some(public_resolver_code_hash),
            matched_code_hash: Some(public_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert_profile_admission_rows(
        &admissions,
        EnsV1ProfileAdmissionExpectation {
            address: pending_resolver_address,
            profile: "public_resolver_compatible",
            fact_families: default_public_resolver_fact_families(),
            status: "pending",
            admission_basis: "code_hash_pending",
            contract_instance_id: pending_contract_instance_id,
            observed_code_hash: None,
            matched_code_hash: None,
            matched_contract_instance_id: None,
        },
    );
    assert_profile_admission_rows(
        &admissions,
        EnsV1ProfileAdmissionExpectation {
            address: unsupported_resolver_address,
            profile: "public_resolver_compatible",
            fact_families: default_public_resolver_fact_families(),
            status: "unsupported",
            admission_basis: "code_hash_mismatch",
            contract_instance_id: unsupported_contract_instance_id,
            observed_code_hash: Some(unsupported_code_hash),
            matched_code_hash: None,
            matched_contract_instance_id: None,
        },
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn scoped_resolver_profile_rejects_unadmitted_code_hash_target() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?,
    )?;
    test_dir.write_manifest(
        "ens",
        "ens_v1_resolver_l1",
        "v1",
        &checked_in_manifest_contents("ens", "ens_v1_resolver_l1", "v1")?,
    )?;

    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let public_resolver_seed_address = "0xF29100983E058B709F3D539b0c765937B804AC15";
    let admitted_resolver_address = "0x0000000000000000000000000000000000000241";
    let unadmitted_resolver_address = "0x0000000000000000000000000000000000000242";
    let public_resolver_code_hash = "keccak256:ens-v1-scoped-public-resolver-compatible";

    let seed_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        public_resolver_seed_address,
    )
    .await?;
    let admitted_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: admitted_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(140),
            active_from_block_hash: Some(
                "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "scoped-supported-resolver",
            }),
        },
    )
    .await?;
    let admitted_contract_instance_id = admitted_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("admitted resolver discovery must create a target");

    for (block_number, address) in [
        (100, public_resolver_seed_address),
        (140, admitted_resolver_address),
        (150, unadmitted_resolver_address),
    ] {
        let block_hash = format!("0x{block_number:064x}");
        insert_raw_code_hash_observation(
            database.pool(),
            RawCodeHashObservation {
                chain: "ethereum-mainnet",
                block_hash: &block_hash,
                block_number,
                contract_address: address,
                code_hash: public_resolver_code_hash,
                code_byte_length: 1,
                canonicality_state: "canonical",
            },
        )
        .await?;
    }

    let admissions = load_ens_v1_public_resolver_profile_admissions_for_targets(
        database.pool(),
        &[
            (
                "ethereum-mainnet".to_owned(),
                admitted_resolver_address.to_owned(),
            ),
            (
                "ethereum-mainnet".to_owned(),
                unadmitted_resolver_address.to_owned(),
            ),
        ],
    )
    .await?;

    assert_eq!(admissions.len(), 14);
    assert_ens_v1_profile_admission_rows_with_statuses(
        &admissions,
        EnsV1ProfileAdmissionStatusExpectation {
            address: admitted_resolver_address,
            profile: "public_resolver_compatible",
            fact_statuses: latest_public_resolver_fact_statuses(),
            admission_basis: "code_hash_match",
            contract_instance_id: admitted_contract_instance_id,
            observed_code_hash: Some(public_resolver_code_hash),
            matched_code_hash: Some(public_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert!(
        admissions
            .iter()
            .all(|admission| admission.address != normalize_address(unadmitted_resolver_address)),
        "unadmitted target must not graduate to a scoped resolver profile"
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ens_v1_known_legacy_resolver_profile_does_not_flatten_latest_capabilities() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    test_dir.write_manifest(
        "ens",
        "ens_v1_resolver_l1",
        "v1",
        &checked_in_manifest_contents("ens", "ens_v1_resolver_l1", "v1")?,
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let legacy_resolver_address = "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41";
    let legacy_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        legacy_resolver_address,
    )
    .await?;
    let admissions = load_ens_v1_public_resolver_profile_admissions(database.pool()).await?;
    let legacy_rows = admissions
        .iter()
        .filter(|admission| admission.address == normalize_address(legacy_resolver_address))
        .collect::<Vec<_>>();
    assert_eq!(legacy_rows.len(), 14);
    assert!(
        legacy_rows
            .iter()
            .all(|row| row.profile == "public_resolver_legacy_multicoin_dns")
    );
    assert!(legacy_rows.iter().all(
        |row| row.contract_instance_id == legacy_contract_instance_id
            && row.source == WatchedContractSource::ManifestContract
            && row.admission_basis == "first_party_known_resolver_admission"
    ));
    let statuses = legacy_rows
        .iter()
        .map(|row| (row.fact_family.as_str(), row.status.as_str()))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(statuses["resolver_record"], "unsupported");
    assert_eq!(statuses["resolver_record:addr"], "supported");
    assert_eq!(statuses["resolver_record:multicoin_addr"], "supported");
    assert_eq!(statuses["resolver_record:name"], "supported");
    assert_eq!(statuses["resolver_record:text"], "supported");
    assert_eq!(statuses["resolver_record:abi"], "supported");
    assert_eq!(statuses["resolver_record:contenthash"], "supported");
    assert_eq!(statuses["resolver_record:dns"], "supported");
    assert_eq!(statuses["resolver_record:interface"], "supported");
    assert_eq!(statuses["resolver_record:data"], "unsupported");
    assert_eq!(statuses["resolver_authorization"], "supported");
    assert_eq!(statuses["resolver_record_version"], "unsupported");
    assert_eq!(
        statuses["resolver_feature:name_wrapper_aware"],
        "unsupported"
    );
    assert_eq!(
        statuses["resolver_feature:default_coin_type"],
        "unsupported"
    );
    assert!(
        legacy_rows
            .iter()
            .all(|row| row.observed_code_hash.is_none())
    );

    database.cleanup().await?;
    Ok(())
}

struct ProfileAdmissionExpectation<'a> {
    address: &'a str,
    chain: &'a str,
    source_family: &'a str,
    profile: &'a str,
    fact_families: BTreeSet<&'a str>,
    status: &'a str,
    admission_basis: &'a str,
    contract_instance_id: Uuid,
    observed_code_hash: Option<&'a str>,
    matched_code_hash: Option<&'a str>,
    matched_contract_instance_id: Option<Uuid>,
}

struct EnsV1ProfileAdmissionExpectation<'a> {
    address: &'a str,
    profile: &'a str,
    fact_families: BTreeSet<&'a str>,
    status: &'a str,
    admission_basis: &'a str,
    contract_instance_id: Uuid,
    observed_code_hash: Option<&'a str>,
    matched_code_hash: Option<&'a str>,
    matched_contract_instance_id: Option<Uuid>,
}

struct EnsV1ProfileAdmissionStatusExpectation<'a> {
    address: &'a str,
    profile: &'a str,
    fact_statuses: BTreeMap<&'a str, &'a str>,
    admission_basis: &'a str,
    contract_instance_id: Uuid,
    observed_code_hash: Option<&'a str>,
    matched_code_hash: Option<&'a str>,
    matched_contract_instance_id: Option<Uuid>,
}

fn assert_profile_admission_rows(
    admissions: &[ResolverProfileAdmission],
    expectation: EnsV1ProfileAdmissionExpectation<'_>,
) {
    assert_profile_admission_rows_for_profile(
        admissions,
        ProfileAdmissionExpectation {
            address: expectation.address,
            chain: "ethereum-mainnet",
            source_family: "ens_v1_resolver_l1",
            profile: expectation.profile,
            fact_families: expectation.fact_families,
            status: expectation.status,
            admission_basis: expectation.admission_basis,
            contract_instance_id: expectation.contract_instance_id,
            observed_code_hash: expectation.observed_code_hash,
            matched_code_hash: expectation.matched_code_hash,
            matched_contract_instance_id: expectation.matched_contract_instance_id,
        },
    );
}

fn assert_ens_v1_profile_admission_rows_with_statuses(
    admissions: &[ResolverProfileAdmission],
    expectation: EnsV1ProfileAdmissionStatusExpectation<'_>,
) {
    assert_profile_admission_rows_with_statuses(
        admissions,
        "ethereum-mainnet",
        "ens_v1_resolver_l1",
        expectation.address,
        expectation.profile,
        expectation.fact_statuses,
        expectation.admission_basis,
        expectation.contract_instance_id,
        expectation.observed_code_hash,
        expectation.matched_code_hash,
        expectation.matched_contract_instance_id,
    );
}

#[allow(clippy::too_many_arguments)]
fn assert_profile_admission_rows_with_statuses(
    admissions: &[ResolverProfileAdmission],
    chain: &str,
    source_family: &str,
    address: &str,
    profile: &str,
    fact_statuses: BTreeMap<&str, &str>,
    admission_basis: &str,
    contract_instance_id: Uuid,
    observed_code_hash: Option<&str>,
    matched_code_hash: Option<&str>,
    matched_contract_instance_id: Option<Uuid>,
) {
    let address = normalize_address(address);
    let rows = admissions
        .iter()
        .filter(|admission| admission.address == address)
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), fact_statuses.len());
    assert_eq!(
        rows.iter()
            .map(|admission| admission.fact_family.as_str())
            .collect::<BTreeSet<_>>(),
        fact_statuses.keys().copied().collect::<BTreeSet<_>>()
    );

    for row in rows {
        assert_eq!(row.chain, chain);
        assert_eq!(row.source_family, source_family);
        assert_eq!(row.contract_instance_id, contract_instance_id);
        assert_eq!(row.profile, profile);
        assert_eq!(row.status.as_str(), fact_statuses[row.fact_family.as_str()]);
        assert_eq!(row.admission_basis, admission_basis);
        assert_eq!(row.observed_code_hash.as_deref(), observed_code_hash);
        assert_eq!(row.matched_code_hash.as_deref(), matched_code_hash);
        assert_eq!(
            row.matched_contract_instance_id,
            matched_contract_instance_id
        );
    }
}

fn assert_profile_admission_rows_for_profile(
    admissions: &[ResolverProfileAdmission],
    expectation: ProfileAdmissionExpectation<'_>,
) {
    let address = normalize_address(expectation.address);
    let rows = admissions
        .iter()
        .filter(|admission| admission.address == address)
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), expectation.fact_families.len());
    assert_eq!(
        rows.iter()
            .map(|admission| admission.fact_family.as_str())
            .collect::<BTreeSet<_>>(),
        expectation.fact_families
    );

    for row in rows {
        assert_eq!(row.chain, expectation.chain);
        assert_eq!(row.source_family, expectation.source_family);
        assert_eq!(row.contract_instance_id, expectation.contract_instance_id);
        assert_eq!(row.profile, expectation.profile);
        assert_eq!(row.status, expectation.status);
        assert_eq!(row.admission_basis, expectation.admission_basis);
        assert_eq!(
            row.observed_code_hash.as_deref(),
            expectation.observed_code_hash
        );
        assert_eq!(
            row.matched_code_hash.as_deref(),
            expectation.matched_code_hash
        );
        assert_eq!(
            row.matched_contract_instance_id,
            expectation.matched_contract_instance_id
        );
    }
}

fn default_public_resolver_fact_families() -> BTreeSet<&'static str> {
    BTreeSet::from([
        "resolver_authorization",
        "resolver_record",
        "resolver_record_version",
    ])
}

fn latest_public_resolver_fact_families() -> BTreeSet<&'static str> {
    BTreeSet::from([
        "resolver_authorization",
        "resolver_feature:default_coin_type",
        "resolver_feature:name_wrapper_aware",
        "resolver_record",
        "resolver_record:abi",
        "resolver_record:addr",
        "resolver_record:contenthash",
        "resolver_record:data",
        "resolver_record:dns",
        "resolver_record:interface",
        "resolver_record:multicoin_addr",
        "resolver_record:name",
        "resolver_record:text",
        "resolver_record_version",
    ])
}

fn latest_public_resolver_fact_statuses() -> BTreeMap<&'static str, &'static str> {
    latest_public_resolver_fact_families()
        .into_iter()
        .map(|fact_family| {
            let status = if fact_family == "resolver_record:data" {
                "unsupported"
            } else {
                "supported"
            };
            (fact_family, status)
        })
        .collect()
}

#[tokio::test]
async fn basenames_l2_resolver_profile_admission_keeps_unknowns_watch_only() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let registry_manifest =
        checked_in_manifest_contents("basenames", "basenames_base_registry", "v2")?;
    let resolver_manifest =
        checked_in_manifest_contents("basenames", "basenames_base_resolver", "v1")?;

    for citation in [
        "(upstream: .refs/basenames/README.md:L34 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L22 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L29 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L182 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L193 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L209 @ basenames@1809bbc)",
        "(upstream: .refs/basenames/src/L2/L2Resolver.sol:L225 @ basenames@1809bbc)",
    ] {
        assert!(
            resolver_manifest.contains(citation),
            "Basenames resolver manifest is missing upstream citation {citation}"
        );
    }
    assert!(!resolver_manifest.contains("public_resolver_compatible"));
    assert!(!resolver_manifest.contains("record-version"));

    test_dir.write_manifest(
        "basenames",
        "basenames_base_registry",
        "v2",
        &registry_manifest,
    )?;
    test_dir.write_manifest(
        "basenames",
        "basenames_base_resolver",
        "v1",
        &resolver_manifest,
    )?;

    let repository = load_repository(&test_dir.path)?;
    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.active_manifest_count, 2);

    let registry_address = "0xb94704422c2a1e396835a571837aa5ae53285a95";
    let l2_resolver_seed_address = "0xC6d566A56A1aFf6508b41f6c90ff131615583BCD";
    let supported_resolver_address = "0x0000000000000000000000000000000000000301";
    let pending_resolver_address = "0x0000000000000000000000000000000000000302";
    let unsupported_resolver_address = "0x0000000000000000000000000000000000000303";
    let l2_resolver_code_hash = "keccak256:basenames-l2-resolver-compatible";
    let unsupported_code_hash = "keccak256:basenames-unknown-resolver";

    let seed_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "base-mainnet",
        l2_resolver_seed_address,
    )
    .await?;

    let supported_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "base-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: supported_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(100),
            active_from_block_hash: Some(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "l2-resolver-code-hash-match",
            }),
        },
    )
    .await?;
    let pending_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "base-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: pending_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(110),
            active_from_block_hash: Some(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "pending-code-hash",
            }),
        },
    )
    .await?;
    let unsupported_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "base-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: unsupported_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "registry_resolver_observation".to_owned(),
            active_from_block_number: Some(120),
            active_from_block_hash: Some(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            ),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "kind": "resolver",
                "case": "unsupported-code-hash",
            }),
        },
    )
    .await?;

    assert_eq!(supported_summary.admitted_edge_count, 1);
    assert_eq!(pending_summary.admitted_edge_count, 1);
    assert_eq!(unsupported_summary.admitted_edge_count, 1);
    let supported_contract_instance_id = supported_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("supported resolver discovery must admit a target");
    let pending_contract_instance_id = pending_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("pending resolver discovery must admit a target");
    let unsupported_contract_instance_id = unsupported_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("unsupported resolver discovery must admit a target");

    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "base-mainnet",
            block_hash: "0x1111111111111111111111111111111111111111111111111111111111111111",
            block_number: 100,
            contract_address: l2_resolver_seed_address,
            code_hash: l2_resolver_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "base-mainnet",
            block_hash: "0x2222222222222222222222222222222222222222222222222222222222222222",
            block_number: 110,
            contract_address: supported_resolver_address,
            code_hash: l2_resolver_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "base-mainnet",
            block_hash: "0x3333333333333333333333333333333333333333333333333333333333333333",
            block_number: 120,
            contract_address: unsupported_resolver_address,
            code_hash: unsupported_code_hash,
            code_byte_length: 1,
            canonicality_state: "canonical",
        },
    )
    .await?;

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    for (address, contract_instance_id) in [
        (supported_resolver_address, supported_contract_instance_id),
        (pending_resolver_address, pending_contract_instance_id),
        (
            unsupported_resolver_address,
            unsupported_contract_instance_id,
        ),
    ] {
        let address = normalize_address(address);
        assert!(
            watched_contracts.iter().any(|contract| {
                contract.source_family == "basenames_base_resolver"
                    && contract.address == address
                    && contract.contract_instance_id == contract_instance_id
                    && contract.source == WatchedContractSource::DiscoveryEdge
            }),
            "dynamic Basenames resolver {address} must remain an admitted watch target"
        );
    }

    let admissions = load_basenames_l2_resolver_profile_admissions(database.pool()).await?;
    assert_eq!(admissions.len(), 8);
    assert!(
        admissions
            .iter()
            .all(|admission| admission.profile != "public_resolver_compatible")
    );
    assert!(
        admissions
            .iter()
            .all(|admission| admission.fact_family != "resolver_record_version")
    );
    let fact_families = BTreeSet::from(["resolver_authorization", "resolver_record"]);

    assert_profile_admission_rows_for_profile(
        &admissions,
        ProfileAdmissionExpectation {
            address: l2_resolver_seed_address,
            chain: "base-mainnet",
            source_family: "basenames_base_resolver",
            profile: "l2_resolver_compatible",
            fact_families: fact_families.clone(),
            status: "supported",
            admission_basis: "manifest_l2_resolver_seed",
            contract_instance_id: seed_contract_instance_id,
            observed_code_hash: Some(l2_resolver_code_hash),
            matched_code_hash: Some(l2_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert_profile_admission_rows_for_profile(
        &admissions,
        ProfileAdmissionExpectation {
            address: supported_resolver_address,
            chain: "base-mainnet",
            source_family: "basenames_base_resolver",
            profile: "l2_resolver_compatible",
            fact_families: fact_families.clone(),
            status: "supported",
            admission_basis: "code_hash_match",
            contract_instance_id: supported_contract_instance_id,
            observed_code_hash: Some(l2_resolver_code_hash),
            matched_code_hash: Some(l2_resolver_code_hash),
            matched_contract_instance_id: Some(seed_contract_instance_id),
        },
    );
    assert_profile_admission_rows_for_profile(
        &admissions,
        ProfileAdmissionExpectation {
            address: pending_resolver_address,
            chain: "base-mainnet",
            source_family: "basenames_base_resolver",
            profile: "l2_resolver_compatible",
            fact_families: fact_families.clone(),
            status: "pending",
            admission_basis: "code_hash_pending",
            contract_instance_id: pending_contract_instance_id,
            observed_code_hash: None,
            matched_code_hash: None,
            matched_contract_instance_id: None,
        },
    );
    assert_profile_admission_rows_for_profile(
        &admissions,
        ProfileAdmissionExpectation {
            address: unsupported_resolver_address,
            chain: "base-mainnet",
            source_family: "basenames_base_resolver",
            profile: "l2_resolver_compatible",
            fact_families,
            status: "unsupported",
            admission_basis: "code_hash_mismatch",
            contract_instance_id: unsupported_contract_instance_id,
            observed_code_hash: Some(unsupported_code_hash),
            matched_code_hash: None,
            matched_contract_instance_id: None,
        },
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn dynamic_resolver_backfill_selector_loads_edge_address_intersections() -> Result<()> {
    for case in [
        (
            "ens",
            "ens_v1_registry_l1",
            "ens_v1_resolver_l1",
            "ethereum-mainnet",
            "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E",
            "0x0000000000000000000000000000000000000101",
            "0x0000000000000000000000000000000000000102",
            "0x0000000000000000000000000000000000000103",
        ),
        (
            "basenames",
            "basenames_base_registry",
            "basenames_base_resolver",
            "base-mainnet",
            "0xb94704422c2a1e396835a571837aa5ae53285a95",
            "0x0000000000000000000000000000000000000201",
            "0x0000000000000000000000000000000000000202",
            "0x0000000000000000000000000000000000000203",
        ),
    ] {
        let (
            namespace,
            registry_source_family,
            resolver_source_family,
            chain,
            registry_address,
            selected_resolver_address,
            closed_resolver_address,
            deactivated_resolver_address,
        ) = case;
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;
        let registry_version_tag = if registry_source_family == "ens_v1_registry_l1" {
            "v3"
        } else {
            "v2"
        };

        test_dir.write_manifest(
            namespace,
            registry_source_family,
            registry_version_tag,
            &checked_in_manifest_contents(namespace, registry_source_family, registry_version_tag)?,
        )?;
        test_dir.write_manifest(
            namespace,
            resolver_source_family,
            "v1",
            &checked_in_manifest_contents(namespace, resolver_source_family, "v1")?,
        )?;
        sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

        let selected_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: selected_resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "dynamic-resolver-backfill-selector-test".to_owned(),
                active_from_block_number: Some(100),
                active_from_block_hash: Some(
                    "0x1111111111111111111111111111111111111111111111111111111111111111".to_owned(),
                ),
                active_to_block_number: Some(220),
                active_to_block_hash: Some(
                    "0x2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
                ),
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "selected-resolver",
                }),
            },
        )
        .await?;
        let selected_contract_instance_id = selected_summary.admitted_edges[0]
            .to_contract_instance_id
            .expect("selected resolver discovery must admit a target contract instance");
        sqlx::query(
            r#"
            UPDATE contract_instance_addresses
            SET active_from_block_number = 150,
                active_to_block_number = 190
            WHERE contract_instance_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(selected_contract_instance_id)
        .execute(database.pool())
        .await?;

        let closed_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: closed_resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "dynamic-resolver-backfill-selector-test".to_owned(),
                active_from_block_number: Some(20),
                active_from_block_hash: Some(
                    "0x3333333333333333333333333333333333333333333333333333333333333333".to_owned(),
                ),
                active_to_block_number: Some(90),
                active_to_block_hash: Some(
                    "0x4444444444444444444444444444444444444444444444444444444444444444".to_owned(),
                ),
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "closed-resolver",
                }),
            },
        )
        .await?;
        let closed_contract_instance_id = closed_summary.admitted_edges[0]
            .to_contract_instance_id
            .expect("closed resolver discovery must admit a target contract instance");

        let deactivated_summary = persist_discovery_observation(
            database.pool(),
            &DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: deactivated_resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "dynamic-resolver-backfill-selector-test".to_owned(),
                active_from_block_number: Some(150),
                active_from_block_hash: Some(
                    "0x5555555555555555555555555555555555555555555555555555555555555555".to_owned(),
                ),
                active_to_block_number: Some(190),
                active_to_block_hash: Some(
                    "0x6666666666666666666666666666666666666666666666666666666666666666".to_owned(),
                ),
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "kind": "deactivated-resolver",
                }),
            },
        )
        .await?;
        let deactivated_contract_instance_id = deactivated_summary.admitted_edges[0]
            .to_contract_instance_id
            .expect("deactivated resolver discovery must admit a target contract instance");
        sqlx::query(
            r#"
            UPDATE discovery_edges
            SET deactivated_at = now()
            WHERE to_contract_instance_id = $1
              AND deactivated_at IS NULL
            "#,
        )
        .bind(deactivated_contract_instance_id)
        .execute(database.pool())
        .await?;

        let watched_contracts = load_watched_contracts(database.pool()).await?;
        let selected_watched_contract = watched_contracts
            .iter()
            .find(|contract| contract.contract_instance_id == selected_contract_instance_id)
            .expect("selected resolver discovery target must be in the watched plan");
        assert_eq!(selected_watched_contract.chain, chain);
        assert_eq!(
            selected_watched_contract.source_family,
            resolver_source_family
        );
        assert_eq!(
            selected_watched_contract.address,
            normalize_address(selected_resolver_address)
        );
        assert_eq!(
            selected_watched_contract.source,
            WatchedContractSource::DiscoveryEdge
        );
        assert_eq!(
            selected_watched_contract.active_from_block_number,
            Some(150)
        );
        assert_eq!(selected_watched_contract.active_to_block_number, Some(190));
        assert!(
            watched_contracts
                .iter()
                .all(|contract| contract.contract_instance_id != deactivated_contract_instance_id),
            "deactivated resolver discovery edge must not remain in the watched plan"
        );

        let selected_plan = load_watched_source_selector_plan(
            database.pool(),
            chain,
            WatchedSourceSelector::SourceFamily(resolver_source_family.to_owned()),
            120,
            175,
        )
        .await?;
        let mut sorted_targets = selected_plan.selected_targets.clone();
        sorted_targets.sort();
        assert_eq!(selected_plan.selected_targets, sorted_targets);
        assert_eq!(
            selected_plan
                .selected_targets
                .iter()
                .find(|target| target.contract_instance_id == selected_contract_instance_id),
            Some(&WatchedBackfillTarget {
                source_family: resolver_source_family.to_owned(),
                contract_instance_id: selected_contract_instance_id,
                address: normalize_address(selected_resolver_address),
                effective_from_block: 150,
                effective_to_block: 175,
            })
        );
        assert!(
            selected_plan
                .selected_targets
                .iter()
                .all(
                    |target| target.contract_instance_id != closed_contract_instance_id
                        && target.contract_instance_id != deactivated_contract_instance_id
                ),
            "closed and deactivated resolver discovery targets must not be selected"
        );

        for (range_start, range_end) in [(100, 149), (191, 220)] {
            let out_of_range_plan = load_watched_source_selector_plan(
                database.pool(),
                chain,
                WatchedSourceSelector::SourceFamily(resolver_source_family.to_owned()),
                range_start,
                range_end,
            )
            .await;
            if namespace == "ens" {
                let error = out_of_range_plan
                    .expect_err("ENS low historical resolver ranges have no active targets");
                assert!(
                    error.to_string().contains(
                        "watched source selector source_family ens_v1_resolver_l1 found no active watched targets"
                    ),
                    "unexpected ENS out-of-range selector error: {error:#}"
                );
            } else {
                let out_of_range_plan = out_of_range_plan?;
                assert!(
                    out_of_range_plan
                        .selected_targets
                        .iter()
                        .all(|target| target.contract_instance_id != selected_contract_instance_id),
                    "resolver target must not be selected outside the edge/address intersection"
                );
            }
        }

        database.cleanup().await?;
    }

    Ok(())
}

#[tokio::test]
async fn resolver_discovery_edges_do_not_become_transitive_registry_parents() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?,
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let resolver_address = "0x00000000000000000000000000000000000000CC";
    let child_address = "0x00000000000000000000000000000000000000DD";
    let summary = reconcile_discovery_observations(
        database.pool(),
        "unit-test-registry-observations",
        &[
            DiscoveryObservation {
                chain: "ethereum-mainnet".to_owned(),
                from_address: registry_address.to_owned(),
                to_address: resolver_address.to_owned(),
                edge_kind: "resolver".to_owned(),
                discovery_source: "unit-test-registry-observations".to_owned(),
                active_from_block_number: Some(123),
                active_from_block_hash: Some(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                ),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "observation_key": "resolver-edge",
                }),
            },
            DiscoveryObservation {
                chain: "ethereum-mainnet".to_owned(),
                from_address: resolver_address.to_owned(),
                to_address: child_address.to_owned(),
                edge_kind: "subregistry".to_owned(),
                discovery_source: "unit-test-registry-observations".to_owned(),
                active_from_block_number: Some(124),
                active_from_block_hash: Some(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                ),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "observation_key": "resolver-as-parent",
                }),
            },
        ],
    )
    .await?;

    assert_eq!(summary.active_edge_count, 1);
    assert_eq!(summary.admitted_edge_count, 1);
    assert_eq!(summary.inserted_edge_count, 1);
    assert_eq!(summary.admitted_edges[0].edge_kind, "resolver");
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE edge_kind = 'subregistry' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert!(
        !sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT provenance FROM discovery_edges WHERE edge_kind = 'resolver' AND deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?
        .as_object()
        .expect("resolver discovery provenance must be an object")
        .contains_key(PROPAGATED_ROLE_PROVENANCE_FIELD)
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn scoped_discovery_reconciliation_keeps_unrelated_active_addresses() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v3",
        &checked_in_manifest_contents("ens", "ens_v1_registry_l1", "v3")?,
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let discovery_source = "unit-test-scoped-registry-observations";
    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let first_child_address = "0x0000000000000000000000000000000000000101";
    let stale_child_address = "0x0000000000000000000000000000000000000102";
    let replacement_child_address = "0x0000000000000000000000000000000000000103";
    let observation =
        |observation_key: &str, to_address: &str, block_number: i64| DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: to_address.to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: discovery_source.to_owned(),
            active_from_block_number: Some(block_number),
            active_from_block_hash: Some(format!("0x{block_number:064x}")),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "observation_key": observation_key,
            }),
        };

    let initial_summary = reconcile_discovery_observations(
        database.pool(),
        discovery_source,
        &[
            observation("edge-a", first_child_address, 100),
            observation("edge-b", stale_child_address, 101),
        ],
    )
    .await?;
    assert_eq!(initial_summary.inserted_edge_count, 2);

    let first_child_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        first_child_address,
    )
    .await?;
    let stale_child_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        stale_child_address,
    )
    .await?;

    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET deactivated_at = now()
        WHERE to_contract_instance_id = $1
          AND deactivated_at IS NULL
        "#,
    )
    .bind(stale_child_id)
    .execute(database.pool())
    .await?;

    let scoped_summary = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation("edge-a", replacement_child_address, 102)],
    )
    .await?;
    assert_eq!(scoped_summary.inserted_edge_count, 1);
    assert_eq!(scoped_summary.deactivated_edge_count, 1);

    let replacement_child_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        replacement_child_address,
    )
    .await?;
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
        )
        .bind(first_child_id)
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
        )
        .bind(replacement_child_id)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
        )
        .bind(stale_child_id)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn scoped_discovery_transition_summary_accumulates_before_an_empty_final_transition()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let discovery_source = "unit-test-batched-summary";
    let observation = DiscoveryObservation {
        chain: "ethereum-sepolia".to_owned(),
        from_address: "0x67b728a792e789a8978b30cf1b3b641f19354b43".to_owned(),
        to_address: "0x0000000000000000000000000000000000000ba1".to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(10),
        active_from_block_hash: Some(format!("0x{:064x}", 10)),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "batched-summary-edge",
        }),
    };
    let transitions = [vec![observation], Vec::new()];

    let summary = reconcile_scoped_discovery_observation_transitions(
        database.pool(),
        discovery_source,
        &transitions,
    )
    .await?;

    assert_eq!(summary.active_edge_count, 1);
    assert_eq!(summary.admitted_edge_count, 1);
    assert_eq!(summary.admitted_edges.len(), 1);
    assert_eq!(summary.inserted_edge_count, 1);
    assert_eq!(summary.deactivated_edge_count, 0);
    assert_eq!(summary.admission_epoch_bump_count, 1);

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_keeps_newer_assignment_current() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let older_address = "0x0000000000000000000000000000000000000a11";
    let newer_address = "0x0000000000000000000000000000000000000b12";
    let observation = |to_address: &str, block_number: i64| DiscoveryObservation {
        chain: "ethereum-sepolia".to_owned(),
        from_address: registry_address.to_owned(),
        to_address: to_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(block_number),
        active_from_block_hash: Some(format!("0x{block_number:064x}")),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "registry-resource-7-subregistry",
        }),
    };

    reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(newer_address, 12)],
    )
    .await?;
    reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(older_address, 11)],
    )
    .await?;

    let older_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-sepolia",
        older_address,
    )
    .await?;
    let newer_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-sepolia",
        newer_address,
    )
    .await?;
    let older_row = sqlx::query(
        r#"
        SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL AS active
        FROM discovery_edges
        WHERE discovery_source = $1
          AND to_contract_instance_id = $2
        "#,
    )
    .bind(discovery_source)
    .bind(older_contract_instance_id)
    .fetch_one(database.pool())
    .await?;
    let newer_row = sqlx::query(
        r#"
        SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL AS active
        FROM discovery_edges
        WHERE discovery_source = $1
          AND to_contract_instance_id = $2
        "#,
    )
    .bind(discovery_source)
    .bind(newer_contract_instance_id)
    .fetch_one(database.pool())
    .await?;

    assert_eq!(
        older_row.try_get::<Option<i64>, _>("active_from_block_number")?,
        Some(11)
    );
    assert_eq!(
        older_row.try_get::<Option<i64>, _>("active_to_block_number")?,
        Some(12)
    );
    assert!(!older_row.try_get::<bool, _>("active")?);
    assert_eq!(
        newer_row.try_get::<Option<i64>, _>("active_from_block_number")?,
        Some(12)
    );
    assert_eq!(
        newer_row.try_get::<Option<i64>, _>("active_to_block_number")?,
        None
    );
    assert!(newer_row.try_get::<bool, _>("active")?);
    assert_eq!(
        query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges
            WHERE active_from_block_number IS NOT NULL
              AND active_to_block_number IS NOT NULL
              AND active_to_block_number < active_from_block_number
            "#,
        )
        .fetch_one(database.pool())
        .await?,
        0,
        "discovery reconciliation must never create a negative block interval"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_keeps_later_same_block_assignment_current() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let earlier_address = "0x0000000000000000000000000000000000000c13";
    let later_address = "0x0000000000000000000000000000000000000d14";
    let block_number = 10;
    let block_hash = format!("0x{block_number:064x}");
    let observation = |to_address: &str, log_index: i64| DiscoveryObservation {
        chain: "ethereum-sepolia".to_owned(),
        from_address: registry_address.to_owned(),
        to_address: to_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(block_number),
        active_from_block_hash: Some(block_hash.clone()),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "registry-resource-same-block-chronology-subregistry",
            "transaction_index": 0,
            "log_index": log_index,
        }),
    };

    reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(later_address, 2)],
    )
    .await?;
    reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(earlier_address, 1)],
    )
    .await?;

    let intervals = sqlx::query_as::<_, (String, i64, Option<i64>, bool)>(
        r#"
        SELECT cia.address,
               de.active_from_block_number,
               de.active_to_block_number,
               de.deactivated_at IS NULL
        FROM discovery_edges de
        JOIN contract_instance_addresses cia
          ON cia.contract_instance_id = de.to_contract_instance_id
        WHERE de.discovery_source = $1
        ORDER BY de.provenance ->> 'log_index'
        "#,
    )
    .bind(discovery_source)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        intervals,
        vec![
            (earlier_address.to_owned(), 10, Some(10), false),
            (later_address.to_owned(), 10, None, true),
        ],
        "same-block replay must order assignments by transaction and log position"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_orders_same_block_tombstones() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let child_address = "0x0000000000000000000000000000000000000e15";
    let block_number = 10;
    let block_hash = format!("0x{block_number:064x}");
    let observation = |to_address: &str, log_index: i64| DiscoveryObservation {
        chain: "ethereum-sepolia".to_owned(),
        from_address: registry_address.to_owned(),
        to_address: to_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(block_number),
        active_from_block_hash: Some(block_hash.clone()),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "registry-resource-same-block-tombstone-subregistry",
            "transaction_index": 0,
            "log_index": log_index,
        }),
    };

    reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(child_address, 2)],
    )
    .await?;
    let earlier_tombstone = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(ZERO_ADDRESS, 1)],
    )
    .await?;
    assert_eq!(earlier_tombstone.deactivated_edge_count, 0);
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(discovery_source)
        .fetch_one(database.pool())
        .await?,
        1,
        "an earlier same-block tombstone must not close a later assignment"
    );

    let later_tombstone = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(ZERO_ADDRESS, 3)],
    )
    .await?;
    assert_eq!(later_tombstone.deactivated_edge_count, 1);
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, bool)>(
            r#"
            SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL
            FROM discovery_edges
            WHERE discovery_source = $1
            "#,
        )
        .bind(discovery_source)
        .fetch_one(database.pool())
        .await?,
        (10, Some(10), false),
        "a later same-block tombstone must close the assignment"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_rejects_incomplete_event_position() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let observation = DiscoveryObservation {
        chain: "ethereum-sepolia".to_owned(),
        from_address: "0x67b728a792e789a8978b30cf1b3b641f19354b43".to_owned(),
        to_address: "0x0000000000000000000000000000000000000f16".to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(10),
        active_from_block_hash: Some(format!("0x{:064x}", 10)),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "incomplete-event-position",
            "transaction_index": 0,
        }),
    };

    let error =
        reconcile_scoped_discovery_observations(database.pool(), discovery_source, &[observation])
            .await
            .expect_err("partial transaction/log provenance must fail closed");
    assert!(
        error
            .to_string()
            .contains("must carry both transaction_index and log_index"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1"
        )
        .bind(discovery_source)
        .fetch_one(database.pool())
        .await?,
        0,
        "invalid chronology metadata must not mutate discovery state"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_inserts_middle_assignment_without_overlap() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let observation = |to_address: &str, block_number: i64| DiscoveryObservation {
        chain: "ethereum-sepolia".to_owned(),
        from_address: registry_address.to_owned(),
        to_address: to_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(block_number),
        active_from_block_hash: Some(format!("0x{block_number:064x}")),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "registry-resource-chronology-subregistry",
        }),
    };

    for (address, block_number) in [
        ("0x00000000000000000000000000000000000000c3", 30),
        ("0x00000000000000000000000000000000000000a1", 10),
        ("0x00000000000000000000000000000000000000b2", 20),
    ] {
        reconcile_scoped_discovery_observations(
            database.pool(),
            discovery_source,
            &[observation(address, block_number)],
        )
        .await?;
    }

    let intervals = sqlx::query_as::<_, (i64, Option<i64>, bool)>(
        r#"
        SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL
        FROM discovery_edges
        WHERE discovery_source = $1
        ORDER BY active_from_block_number
        "#,
    )
    .bind(discovery_source)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        intervals,
        vec![
            (10, Some(20), false),
            (20, Some(30), false),
            (30, None, true)
        ],
        "out-of-order discovery replay must form adjacent non-overlapping intervals"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_inserts_middle_same_block_assignment_without_overlap()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let block_number = 10;
    let block_hash = format!("0x{block_number:064x}");
    let observation = |to_address: &str, log_index: i64| DiscoveryObservation {
        chain: "ethereum-sepolia".to_owned(),
        from_address: registry_address.to_owned(),
        to_address: to_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(block_number),
        active_from_block_hash: Some(block_hash.clone()),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "registry-resource-same-block-chronology-subregistry",
            "transaction_index": 0,
            "log_index": log_index,
        }),
    };

    for (address, log_index) in [
        ("0x00000000000000000000000000000000000000c3", 30),
        ("0x00000000000000000000000000000000000000a1", 10),
        ("0x00000000000000000000000000000000000000b2", 20),
    ] {
        reconcile_scoped_discovery_observations(
            database.pool(),
            discovery_source,
            &[observation(address, log_index)],
        )
        .await?;
    }

    let intervals = sqlx::query_as::<_, (i64, Option<i64>, bool)>(
        r#"
        SELECT (provenance ->> 'log_index')::BIGINT,
               (provenance ->> 'active_to_log_index')::BIGINT,
               deactivated_at IS NULL
        FROM discovery_edges
        WHERE discovery_source = $1
        ORDER BY (provenance ->> 'log_index')::BIGINT
        "#,
    )
    .bind(discovery_source)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        intervals,
        vec![
            (10, Some(20), false),
            (20, Some(30), false),
            (30, None, true)
        ],
        "out-of-order same-block replay must form adjacent non-overlapping event intervals"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_ignores_older_tombstone_but_closes_same_block()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let child_address = "0x0000000000000000000000000000000000000c21";
    let observation = |to_address: &str, block_number: i64| DiscoveryObservation {
        chain: chain.to_owned(),
        from_address: registry_address.to_owned(),
        to_address: to_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(block_number),
        active_from_block_hash: Some(format!("0x{block_number:064x}")),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "out-of-order-tombstone",
        }),
    };

    reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(child_address, 20)],
    )
    .await?;
    let epoch_after_insert = load_discovery_admission_epoch(database.pool(), chain).await?;
    let older_tombstone = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(ZERO_ADDRESS, 10)],
    )
    .await?;
    assert_eq!(older_tombstone.deactivated_edge_count, 0);
    assert_eq!(
        load_discovery_admission_epoch(database.pool(), chain).await?,
        epoch_after_insert,
        "a chronology no-op must not invalidate the coverage frontier"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, bool)>(
            r#"
            SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL
            FROM discovery_edges
            WHERE discovery_source = $1
            "#,
        )
        .bind(discovery_source)
        .fetch_one(database.pool())
        .await?,
        (20, None, true)
    );

    let same_block_tombstone = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(ZERO_ADDRESS, 20)],
    )
    .await?;
    assert_eq!(same_block_tombstone.deactivated_edge_count, 1);
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, bool)>(
            r#"
            SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL
            FROM discovery_edges
            WHERE discovery_source = $1
            "#,
        )
        .bind(discovery_source)
        .fetch_one(database.pool())
        .await?,
        (20, Some(20), false),
        "block-inclusive discovery intervals may start and end in the same block"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_same_address_reset_does_not_create_a_second_epoch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let child_address = "0x0000000000000000000000000000000000000c31";
    let observation = |to_address: &str, block_number: i64| DiscoveryObservation {
        chain: chain.to_owned(),
        from_address: registry_address.to_owned(),
        to_address: to_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(block_number),
        active_from_block_hash: Some(format!("0x{block_number:064x}")),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "same-address-reset",
        }),
    };

    reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(child_address, 10)],
    )
    .await?;
    let epoch_after_insert = load_discovery_admission_epoch(database.pool(), chain).await?;
    let reset = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(child_address, 20)],
    )
    .await?;
    assert_eq!(reset.inserted_edge_count, 0);
    assert_eq!(reset.deactivated_edge_count, 0);
    assert_eq!(
        load_discovery_admission_epoch(database.pool(), chain).await?,
        epoch_after_insert,
        "same-assignment reset must not force historical coverage re-verification"
    );
    let full_reset = reconcile_discovery_observations_through_block(
        database.pool(),
        discovery_source,
        &[observation(child_address, 20)],
        20,
    )
    .await?;
    assert_eq!(full_reset.inserted_edge_count, 0);
    assert_eq!(full_reset.deactivated_edge_count, 0);
    assert_eq!(
        load_discovery_admission_epoch(database.pool(), chain).await?,
        epoch_after_insert,
        "a full-source restart must preserve the continuous same-address assignment"
    );

    reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(ZERO_ADDRESS, 30)],
    )
    .await?;
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, bool)>(
            r#"
            SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL
            FROM discovery_edges
            WHERE discovery_source = $1
            "#,
        )
        .bind(discovery_source)
        .fetch_all(database.pool())
        .await?,
        vec![(10, Some(30), false)],
        "the eventual tombstone must close the one continuous assignment"
    );

    database.cleanup().await
}

#[tokio::test]
async fn discovery_edge_reactivation_matches_the_complete_identity() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let child_address = "0x0000000000000000000000000000000000000c41";
    let observation = DiscoveryObservation {
        chain: chain.to_owned(),
        from_address: registry_address.to_owned(),
        to_address: child_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(10),
        active_from_block_hash: Some(format!("0x{:064x}", 10)),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "complete-edge-identity",
        }),
    };

    reconcile_discovery_observations(
        database.pool(),
        discovery_source,
        std::slice::from_ref(&observation),
    )
    .await?;
    let exact_edge_id = sqlx::query_scalar::<_, i64>(
        "SELECT discovery_edge_id FROM discovery_edges WHERE discovery_source = $1",
    )
    .bind(discovery_source)
    .fetch_one(database.pool())
    .await?;
    let terminal_hash = format!("0x{:064x}", 20);
    let near_collision_edge_id = sqlx::query_scalar::<_, i64>(
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
            active_from_block_hash,
            active_to_block_number,
            active_to_block_hash,
            admitted_at,
            deactivated_at,
            provenance
        )
        SELECT
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_from_block_hash,
            20,
            $2,
            admitted_at,
            now(),
            provenance || '{"near_collision": true}'::JSONB
        FROM discovery_edges
        WHERE discovery_edge_id = $1
        RETURNING discovery_edge_id
        "#,
    )
    .bind(exact_edge_id)
    .bind(&terminal_hash)
    .fetch_one(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET active_to_block_number = 20,
            active_to_block_hash = $2,
            deactivated_at = now()
        WHERE discovery_edge_id = $1
        "#,
    )
    .bind(exact_edge_id)
    .bind(&terminal_hash)
    .execute(database.pool())
    .await?;

    let summary = reconcile_discovery_observations(
        database.pool(),
        discovery_source,
        std::slice::from_ref(&observation),
    )
    .await?;
    assert_eq!(
        summary.inserted_edge_count, 1,
        "reactivating an exact retained epoch counts as graph growth"
    );
    assert_eq!(summary.active_edge_count, 1);
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, Option<String>, bool)>(
            r#"
            SELECT
                discovery_edge_id,
                active_to_block_number,
                active_to_block_hash,
                deactivated_at IS NULL
            FROM discovery_edges
            WHERE discovery_edge_id IN ($1, $2)
            ORDER BY discovery_edge_id
            "#,
        )
        .bind(exact_edge_id)
        .bind(near_collision_edge_id)
        .fetch_all(database.pool())
        .await?,
        vec![
            (exact_edge_id, None, None, true),
            (near_collision_edge_id, Some(20), Some(terminal_hash), false,),
        ],
        "reactivation must clear only the row matching all ten identity fields"
    );

    database.cleanup().await
}

#[tokio::test]
async fn discovery_edge_reactivation_batches_across_the_statement_boundary() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let discovery_source = "unit-test-bulk-reactivation";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let observations = (1..=1001)
        .map(|index| DiscoveryObservation {
            chain: "ethereum-sepolia".to_owned(),
            from_address: registry_address.to_owned(),
            to_address: format!("0x{index:040x}"),
            edge_kind: "subregistry".to_owned(),
            discovery_source: discovery_source.to_owned(),
            active_from_block_number: Some(10),
            active_from_block_hash: Some(format!("0x{:064x}", 10)),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "observation_key": format!("bulk-reactivation-{index}"),
            }),
        })
        .collect::<Vec<_>>();

    let initial =
        reconcile_discovery_observations(database.pool(), discovery_source, &observations).await?;
    assert_eq!(initial.inserted_edge_count, observations.len());

    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET active_to_block_number = 20,
            active_to_block_hash = $2,
            deactivated_at = now(),
            provenance = provenance || jsonb_build_object(
                'active_to_transaction_index', 0,
                'active_to_log_index', 1
            )
        WHERE discovery_source = $1
        "#,
    )
    .bind(discovery_source)
    .bind(format!("0x{:064x}", 20))
    .execute(database.pool())
    .await?;

    let reactivated =
        reconcile_discovery_observations(database.pool(), discovery_source, &observations).await?;
    assert_eq!(reactivated.inserted_edge_count, observations.len());
    assert_eq!(reactivated.active_edge_count, observations.len());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges
            WHERE discovery_source = $1
              AND deactivated_at IS NULL
              AND active_to_block_number IS NULL
              AND NOT (provenance ? 'active_to_transaction_index')
              AND NOT (provenance ? 'active_to_log_index')
            "#,
        )
        .bind(discovery_source)
        .fetch_one(database.pool())
        .await?,
        observations.len() as i64,
    );

    database.cleanup().await
}

#[tokio::test]
async fn full_discovery_reconciliation_through_older_boundary_preserves_newer_live_epoch()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let older_address = "0x0000000000000000000000000000000000000d10";
    let newer_address = "0x0000000000000000000000000000000000000d20";
    let older_hash = format!("0x{:064x}", 10);
    let newer_hash = format!("0x{:064x}", 20);
    insert_test_chain_lineage_state(database.pool(), chain, &older_hash, 10, "canonical").await?;
    insert_test_chain_lineage_state(database.pool(), chain, &newer_hash, 20, "canonical").await?;
    let observation =
        |to_address: &str, block_number: i64, block_hash: &str| DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: registry_address.to_owned(),
            to_address: to_address.to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: discovery_source.to_owned(),
            active_from_block_number: Some(block_number),
            active_from_block_hash: Some(block_hash.to_owned()),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "observation_key": "bounded-full-reconciliation",
            }),
        };

    reconcile_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(newer_address, 20, &newer_hash)],
    )
    .await?;
    reconcile_discovery_observations_through_block(
        database.pool(),
        discovery_source,
        &[observation(older_address, 10, &older_hash)],
        10,
    )
    .await?;

    let older_id =
        load_single_contract_instance_for_address(database.pool(), chain, older_address).await?;
    let newer_id =
        load_single_contract_instance_for_address(database.pool(), chain, newer_address).await?;
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, bool)>(
            r#"
            SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL
            FROM discovery_edges
            WHERE discovery_source = $1
              AND to_contract_instance_id = $2
            "#,
        )
        .bind(discovery_source)
        .bind(older_id)
        .fetch_one(database.pool())
        .await?,
        (10, Some(20), false),
        "the older selected state must be retained as closed history"
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, bool)>(
            r#"
            SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL
            FROM discovery_edges
            WHERE discovery_source = $1
              AND to_contract_instance_id = $2
            "#,
        )
        .bind(discovery_source)
        .bind(newer_id)
        .fetch_one(database.pool())
        .await?,
        (20, None, true),
        "a bounded full reconciliation must preserve a newer non-orphaned assignment"
    );

    database.cleanup().await
}

#[tokio::test]
async fn full_discovery_reconciliation_preserves_newer_same_address_epoch() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let child_address = "0x0000000000000000000000000000000000000d30";
    let older_hash = format!("0x{:064x}", 30);
    let newer_hash = format!("0x{:064x}", 40);
    insert_test_chain_lineage_state(database.pool(), chain, &older_hash, 30, "canonical").await?;
    insert_test_chain_lineage_state(database.pool(), chain, &newer_hash, 40, "canonical").await?;
    let observation = |block_number: i64, block_hash: &str| DiscoveryObservation {
        chain: chain.to_owned(),
        from_address: registry_address.to_owned(),
        to_address: child_address.to_owned(),
        edge_kind: "subregistry".to_owned(),
        discovery_source: discovery_source.to_owned(),
        active_from_block_number: Some(block_number),
        active_from_block_hash: Some(block_hash.to_owned()),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "unit-test",
            "observation_key": "bounded-same-address-reconciliation",
        }),
    };

    reconcile_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(40, &newer_hash)],
    )
    .await?;
    reconcile_discovery_observations_through_block(
        database.pool(),
        discovery_source,
        &[observation(30, &older_hash)],
        30,
    )
    .await?;

    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, bool)>(
            r#"
            SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL
            FROM discovery_edges
            WHERE discovery_source = $1
            ORDER BY active_from_block_number
            "#,
        )
        .bind(discovery_source)
        .fetch_all(database.pool())
        .await?,
        vec![(30, Some(40), false), (40, None, true)],
        "same-address epochs must remain chronological across a bounded full replay"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_rejects_orphaned_same_source_ancestry() -> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let losing_child = "0x0000000000000000000000000000000000000e40";
    let inadmissible_grandchild = "0x0000000000000000000000000000000000000e41";
    let losing_hash = format!("0x{:064x}", 140);
    let later_hash = format!("0x{:064x}", 141);
    insert_test_chain_lineage_state(database.pool(), chain, &losing_hash, 140, "canonical").await?;
    insert_test_chain_lineage_state(database.pool(), chain, &later_hash, 141, "canonical").await?;
    let observation =
        |key: &str, from_address: &str, to_address: &str, block_number: i64, block_hash: &str| {
            DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: from_address.to_owned(),
                to_address: to_address.to_owned(),
                edge_kind: "subregistry".to_owned(),
                discovery_source: discovery_source.to_owned(),
                active_from_block_number: Some(block_number),
                active_from_block_hash: Some(block_hash.to_owned()),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "observation_key": key,
                }),
            }
        };

    let initial = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(
            "orphaned-ancestry-parent",
            registry_address,
            losing_child,
            140,
            &losing_hash,
        )],
    )
    .await?;
    assert_eq!(initial.inserted_edge_count, 1);
    sqlx::query(
        r#"
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain)
    .bind(&losing_hash)
    .execute(database.pool())
    .await?;
    let epoch_before_descendant = load_discovery_admission_epoch(database.pool(), chain).await?;

    let descendant = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(
            "orphaned-ancestry-descendant",
            losing_child,
            inadmissible_grandchild,
            141,
            &later_hash,
        )],
    )
    .await?;

    assert_eq!(descendant.active_edge_count, 1);
    assert_eq!(descendant.admitted_edge_count, 0);
    assert_eq!(descendant.inserted_edge_count, 0);
    assert_eq!(descendant.admission_epoch_bump_count, 0);
    assert_eq!(
        load_discovery_admission_epoch(database.pool(), chain).await?,
        epoch_before_descendant,
        "an orphaned same-source edge must not authorize a descendant or bump the admission epoch"
    );
    assert_eq!(
        query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges edge
            JOIN contract_instance_addresses address
              ON address.contract_instance_id = edge.to_contract_instance_id
            WHERE edge.discovery_source = $1
              AND address.address = $2
            "#,
        )
        .bind(discovery_source)
        .bind(inadmissible_grandchild)
        .fetch_one(database.pool())
        .await?,
        0,
        "the orphaned ancestry window must not materialize a descendant edge"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_removes_orphaned_newer_parent_and_descendant() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let losing_child = "0x0000000000000000000000000000000000000e20";
    let losing_grandchild = "0x0000000000000000000000000000000000000e21";
    let winning_child = "0x0000000000000000000000000000000000000e10";
    let losing_hash = format!("0x{:064x}", 120);
    let winning_hash = format!("0x{:064x}", 110);
    insert_test_chain_lineage_state(database.pool(), chain, &losing_hash, 120, "canonical").await?;
    insert_test_chain_lineage_state(database.pool(), chain, &winning_hash, 110, "canonical")
        .await?;
    let observation =
        |key: &str, from_address: &str, to_address: &str, block_number: i64, block_hash: &str| {
            DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: from_address.to_owned(),
                to_address: to_address.to_owned(),
                edge_kind: "subregistry".to_owned(),
                discovery_source: discovery_source.to_owned(),
                active_from_block_number: Some(block_number),
                active_from_block_hash: Some(block_hash.to_owned()),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "observation_key": key,
                }),
            }
        };

    let initial = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[
            observation(
                "orphaned-parent",
                registry_address,
                losing_child,
                120,
                &losing_hash,
            ),
            observation(
                "orphaned-descendant",
                losing_child,
                losing_grandchild,
                120,
                &losing_hash,
            ),
        ],
    )
    .await?;
    assert_eq!(initial.inserted_edge_count, 2);
    sqlx::query(
        r#"
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain)
    .bind(&losing_hash)
    .execute(database.pool())
    .await?;

    let rebuilt = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(
            "orphaned-parent",
            registry_address,
            winning_child,
            110,
            &winning_hash,
        )],
    )
    .await?;
    assert_eq!(rebuilt.inserted_edge_count, 1);
    assert_eq!(rebuilt.deactivated_edge_count, 2);
    assert_eq!(
        sqlx::query_as::<_, (i64, Option<i64>, bool)>(
            r#"
            SELECT active_from_block_number, active_to_block_number, deactivated_at IS NULL
            FROM discovery_edges
            WHERE discovery_source = $1
            ORDER BY active_from_block_number, discovery_edge_id
            "#,
        )
        .bind(discovery_source)
        .fetch_all(database.pool())
        .await?,
        vec![(110, None, true), (120, None, false), (120, None, false)],
        "orphaned starts must close without manufacturing an inverted canonical interval"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_does_not_bound_orphan_before_same_block_start()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let losing_child = "0x0000000000000000000000000000000000000e31";
    let winning_child = "0x0000000000000000000000000000000000000e30";
    let block_number = 120;
    let losing_hash = format!("0x{:064x}", 1201);
    let winning_hash = format!("0x{:064x}", 1200);
    let observation =
        |to_address: &str, block_hash: &str, transaction_index: i64, log_index: i64| {
            DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: to_address.to_owned(),
                edge_kind: "subregistry".to_owned(),
                discovery_source: discovery_source.to_owned(),
                active_from_block_number: Some(block_number),
                active_from_block_hash: Some(block_hash.to_owned()),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "observation_key": "same-block-orphan-chronology",
                    "transaction_index": transaction_index,
                    "log_index": log_index,
                }),
            }
        };

    insert_test_chain_lineage_state(
        database.pool(),
        chain,
        &losing_hash,
        block_number,
        "canonical",
    )
    .await?;
    reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(losing_child, &losing_hash, 2, 3)],
    )
    .await?;
    sqlx::query(
        r#"
        UPDATE chain_lineage
        SET canonicality_state = 'orphaned'::canonicality_state
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain)
    .bind(&losing_hash)
    .execute(database.pool())
    .await?;
    insert_test_chain_lineage_state(
        database.pool(),
        chain,
        &winning_hash,
        block_number,
        "canonical",
    )
    .await?;

    let rebuilt = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(winning_child, &winning_hash, 1, 9)],
    )
    .await?;
    assert_eq!(rebuilt.inserted_edge_count, 1);
    assert_eq!(rebuilt.deactivated_edge_count, 1);
    assert_eq!(
        sqlx::query_as::<
            _,
            (
                String,
                i64,
                i64,
                i64,
                Option<i64>,
                Option<i64>,
                Option<i64>,
                bool,
            ),
        >(
            r#"
            SELECT cia.address,
                   de.active_from_block_number,
                   (de.provenance ->> 'transaction_index')::BIGINT,
                   (de.provenance ->> 'log_index')::BIGINT,
                   de.active_to_block_number,
                   (de.provenance ->> 'active_to_transaction_index')::BIGINT,
                   (de.provenance ->> 'active_to_log_index')::BIGINT,
                   de.deactivated_at IS NULL
            FROM discovery_edges de
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
            WHERE de.discovery_source = $1
            ORDER BY de.deactivated_at IS NULL DESC
            "#,
        )
        .bind(discovery_source)
        .fetch_all(database.pool())
        .await?,
        vec![
            (winning_child.to_owned(), 120, 1, 9, None, None, None, true,),
            (losing_child.to_owned(), 120, 2, 3, None, None, None, false,),
        ],
        "orphan removal must not materialize a terminal event position before its start"
    );

    database.cleanup().await
}

#[tokio::test]
async fn scoped_discovery_reconciliation_preserves_descendants_with_an_alternate_incoming_path()
-> Result<()> {
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "ens_v2_registry_subregistry:ethereum-sepolia";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let alternate_parent = "0x0000000000000000000000000000000000000f10";
    let shared_registry = "0x0000000000000000000000000000000000000f20";
    let descendant = "0x0000000000000000000000000000000000000f30";
    let observation =
        |key: &str, from_address: &str, to_address: &str, block_number: i64| DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: from_address.to_owned(),
            to_address: to_address.to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: discovery_source.to_owned(),
            active_from_block_number: Some(block_number),
            active_from_block_hash: Some(format!("0x{block_number:064x}")),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "observation_key": key,
            }),
        };

    let initial = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[
            observation("alternate-parent", registry_address, alternate_parent, 10),
            observation("direct-parent", registry_address, shared_registry, 10),
            observation("alternate-incoming", alternate_parent, shared_registry, 11),
            observation("descendant", shared_registry, descendant, 12),
        ],
    )
    .await?;
    assert_eq!(initial.inserted_edge_count, 4);

    let removal = reconcile_scoped_discovery_observations(
        database.pool(),
        discovery_source,
        &[observation(
            "direct-parent",
            registry_address,
            ZERO_ADDRESS,
            20,
        )],
    )
    .await?;
    assert_eq!(removal.deactivated_edge_count, 1);
    assert_eq!(
        query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM discovery_edges
            WHERE discovery_source = $1
              AND deactivated_at IS NULL
              AND provenance ->> 'observation_key' IN ('alternate-incoming', 'descendant')
            "#,
        )
        .bind(discovery_source)
        .fetch_one(database.pool())
        .await?,
        2,
        "removing one incoming edge must preserve the descendant branch while the target registry remains reachable through another active subregistry edge"
    );

    database.cleanup().await
}

#[tokio::test]
async fn discovery_writers_fence_the_admission_epoch_before_mutating_the_watched_set() -> Result<()>
{
    let database = TestDatabase::new().await?;
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;

    let chain = "ethereum-sepolia";
    let discovery_source = "epoch-fence-order-test";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    sqlx::query(
        r#"
        CREATE FUNCTION assert_epoch_fenced_before_discovery_edge_write()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $function$
        BEGIN
            PERFORM epoch
            FROM discovery_admission_epochs
            WHERE chain_id = NEW.chain_id
            FOR UPDATE NOWAIT;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'missing admission-epoch fence row for %', NEW.chain_id;
            END IF;
            RETURN NEW;
        EXCEPTION
            WHEN lock_not_available THEN
                RAISE EXCEPTION 'discovery edge mutation reached the database before the admission-epoch writer fence';
        END
        $function$
        "#,
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        CREATE TRIGGER assert_epoch_fenced_before_discovery_edge_write
        BEFORE INSERT OR UPDATE ON discovery_edges
        FOR EACH ROW
        EXECUTE FUNCTION assert_epoch_fenced_before_discovery_edge_write()
        "#,
    )
    .execute(database.pool())
    .await?;

    let mut promotion_fence = database.pool().begin().await?;
    sqlx::query("SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1 FOR SHARE")
        .bind(chain)
        .execute(&mut *promotion_fence)
        .await?;

    let writer_pool = database.pool().clone();
    let writer = tokio::spawn(async move {
        reconcile_scoped_discovery_observations(
            &writer_pool,
            discovery_source,
            &[DiscoveryObservation {
                chain: chain.to_owned(),
                from_address: registry_address.to_owned(),
                to_address: "0x0000000000000000000000000000000000000fa1".to_owned(),
                edge_kind: "subregistry".to_owned(),
                discovery_source: discovery_source.to_owned(),
                active_from_block_number: Some(10),
                active_from_block_hash: Some(format!("0x{:064x}", 10)),
                active_to_block_number: None,
                active_to_block_hash: None,
                provenance: serde_json::json!({
                    "provider": "unit-test",
                    "observation_key": "epoch-fence-order",
                }),
            }],
        )
        .await
    });

    let mut writer_waited_on_epoch = false;
    for _ in 0..200 {
        writer_waited_on_epoch = query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND wait_event_type = 'Lock'
                  AND query ILIKE '%discovery_admission_epochs%'
            )
            "#,
        )
        .fetch_one(database.pool())
        .await?;
        if writer_waited_on_epoch || writer.is_finished() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    promotion_fence.rollback().await?;
    assert!(
        writer_waited_on_epoch,
        "the discovery writer must wait on the admission epoch before any watched-set write"
    );
    writer.await.context("discovery writer task failed")??;

    database.cleanup().await
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
async fn removing_manifest_deactivates_active_discovery_edges_from_that_source() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v1",
        &registry_manifest_contents("active"),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let persistence_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E".to_owned(),
            to_address: "0x00000000000000000000000000000000000000CC".to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: "manifest-removal-cleanup-test".to_owned(),
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
    let child_contract_instance_id = persistence_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("persisted discovery edge must admit a target contract instance");

    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        1
    );

    let empty_dir = TestDir::new()?;
    let summary = sync_repository(database.pool(), &load_repository(&empty_dir.path)?).await?;
    assert_eq!(summary.removed_manifest_count, 1);
    assert_eq!(summary.cleared_discovery_edge_count, 1);
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM contract_instance_addresses WHERE contract_instance_id = $1 AND deactivated_at IS NULL"
        )
        .bind(child_contract_instance_id)
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn persist_discovery_observation_ignores_zero_address_targets() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v1",
        &registry_manifest_contents("active"),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: "ethereum-mainnet".to_owned(),
            from_address: "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E".to_owned(),
            to_address: ZERO_ADDRESS.to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: "zero-address-discovery-test".to_owned(),
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

    assert_eq!(summary.admitted_edge_count, 0);
    assert_eq!(summary.inserted_edge_count, 0);
    assert!(summary.admitted_edges.is_empty());
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE deactivated_at IS NULL"
        )
        .fetch_one(database.pool())
        .await?,
        0
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn sync_repository_rejects_invalid_proxy_shape_declarations() -> Result<()> {
    let cases = [
        (
            "proxy_kind_none_with_implementation",
            manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                Some("0x00000000000000000000000000000000000000DD"),
            )
            .replacen("proxy_kind = \"erc1967\"", "proxy_kind = \"none\"", 1),
            "cannot declare implementation when proxy_kind = \"none\"",
        ),
        (
            "proxied_contract_without_implementation",
            manifest_contents(
                "active",
                "0x0000000000000000000000000000000000000001",
                "0x00000000000000000000000000000000000000AA",
                None,
            ),
            "must declare implementation when proxy_kind = \"erc1967\"",
        ),
    ];

    for (case_name, contents, expected_error) in cases {
        let test_dir = TestDir::new()?;
        let database = TestDatabase::new().await?;
        test_dir.write_manifest("ens", "ens_v2_registry_l1", "v1", &contents)?;

        let repository = load_repository(&test_dir.path)?;
        let error = sync_repository(database.pool(), &repository)
            .await
            .expect_err("invalid proxy shape must fail manifest sync");
        assert!(
            error.to_string().contains(expected_error),
            "unexpected sync error for {case_name}: {error:#}"
        );

        database.cleanup().await?;
    }

    Ok(())
}

#[tokio::test]
async fn shadow_execution_family_persists_without_entering_active_views() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v1",
        &registry_manifest_contents("active"),
    )?;
    test_dir.write_manifest(
        "ens",
        "ens_execution",
        "v1",
        &execution_manifest_contents("shadow"),
    )?;

    let summary = sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 1);
    assert_eq!(summary.root_count, 1);
    assert_eq!(summary.contract_count, 2);
    assert_eq!(summary.capability_count, 2);
    assert_eq!(summary.discovery_rule_count, 1);

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![
            ("ens_execution".to_owned(), "shadow".to_owned()),
            ("ens_v1_registry_l1".to_owned(), "active".to_owned()),
        ]
    );

    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v1_registry_l1")
            .await?,
        BTreeMap::from([(
            "declared_children".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Supported,
                notes: Some(
                    "registry-controlled child surfaces are authoritative inputs".to_owned(),
                ),
            },
        )])
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_execution").await?,
        BTreeMap::from([(
            "verified_resolution".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Shadow,
                notes: Some(
                    "shadow execution traces and cache ownership are tracked before public verified-resolution reads ship"
                        .to_owned(),
                ),
            },
        )])
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(active_manifests.len(), 1);
    assert_eq!(active_manifests[0].source_family, "ens_v1_registry_l1");
    assert!(
        !active_manifests[0]
            .capability_flags
            .contains_key("verified_resolution")
    );

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert!(!watched_contracts.iter().any(|contract| {
        contract.address == normalize_address("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe")
    }));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn active_execution_family_is_admitted_with_owned_capability_and_watch_target() -> Result<()>
{
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v1",
        &registry_manifest_contents("active"),
    )?;
    test_dir.write_manifest(
        "ens",
        "ens_execution",
        "v1",
        &execution_manifest_contents("active"),
    )?;

    let summary = sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 2);
    assert_eq!(summary.root_count, 1);
    assert_eq!(summary.contract_count, 2);
    assert_eq!(summary.capability_count, 2);
    assert_eq!(summary.discovery_rule_count, 1);

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(
        active_manifests
            .iter()
            .map(|manifest| manifest.source_family.as_str())
            .collect::<Vec<_>>(),
        vec!["ens_execution", "ens_v1_registry_l1"]
    );
    assert_eq!(
        active_manifests[0].capability_flags,
        BTreeMap::from([(
            "verified_resolution".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Shadow,
                notes: Some(
                    "shadow execution traces and cache ownership are tracked before public verified-resolution reads ship"
                        .to_owned(),
                ),
            },
        )])
    );
    assert_eq!(
        active_manifests[1].capability_flags,
        BTreeMap::from([(
            "declared_children".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Supported,
                notes: Some(
                    "registry-controlled child surfaces are authoritative inputs".to_owned(),
                ),
            },
        )])
    );

    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == normalize_address("0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe")
            && contract.source == WatchedContractSource::ManifestContract
    }));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn checked_in_reverse_manifest_is_admitted_as_authoritative_watch_target() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    test_dir.write_manifest(
        "ens",
        "ens_v1_reverse_l1",
        "v1",
        &checked_in_manifest_contents("ens", "ens_v1_reverse_l1", "v1")?,
    )?;

    let repository = load_repository(&test_dir.path)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.manifests().len(), 1);
    assert_eq!(
        repository.manifests()[0].manifest.source_family,
        "ens_v1_reverse_l1"
    );

    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.active_manifest_count, 1);
    assert_eq!(summary.root_count, 0);
    assert_eq!(summary.contract_count, 1);
    assert_eq!(summary.capability_count, 0);
    assert_eq!(summary.discovery_rule_count, 0);

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![("ens_v1_reverse_l1".to_owned(), "active".to_owned())]
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v1_reverse_l1")
            .await?,
        BTreeMap::new()
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(active_manifests.len(), 1);
    assert_eq!(active_manifests[0].source_family, "ens_v1_reverse_l1");
    assert!(active_manifests[0].capability_flags.is_empty());

    let reverse_registrar = normalize_address("0xa58E81fe9b61B5c3fE2AFD33CF304c454AbFc7Cb");
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert_eq!(watched_contracts.len(), 1);
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == reverse_registrar
            && contract.source == WatchedContractSource::ManifestContract
    }));

    assert_eq!(
        load_watched_chain_plan(database.pool()).await?,
        vec![WatchedChainPlan {
            chain: "ethereum-mainnet".to_owned(),
            addresses: vec![reverse_registrar.clone()],
            manifest_root_entry_count: 0,
            manifest_contract_entry_count: 1,
            discovery_edge_entry_count: 0,
        }]
    );

    let admission_state = load_discovery_admission_state(database.pool()).await?;
    assert!(admission_state.has_authoritative_address("ethereum-mainnet", &reverse_registrar));

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn checked_in_wrapper_and_resolver_manifests_admit_phase4_input_families() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let wrapper_manifest = checked_in_manifest_contents("ens", "ens_v1_wrapper_l1", "v1")?;
    let resolver_manifest = checked_in_manifest_contents("ens", "ens_v1_resolver_l1", "v1")?;

    for citation in [
        "(upstream: .refs/ens_v1/deployments/mainnet/NameWrapper.json:L2 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L27 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L35 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L37 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L38 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L54 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L80 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L90 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L102 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L138 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L140 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L479 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L500 @ ens_v1@91c966f)",
    ] {
        assert!(
            wrapper_manifest.contains(citation),
            "wrapper manifest is missing upstream citation {citation}"
        );
    }
    for citation in [
        "(upstream: .refs/ens_v1/deployments/mainnet/PublicResolver.json:L2 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L5 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L6 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L7 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L8 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L9 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L10 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L11 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L12 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L13 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L20 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L31 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L66 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L75 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L114 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L131 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/PublicResolver.sol:L150 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L17 @ ens_v1@91c966f)",
        "(upstream: .refs/ens_v1/contracts/resolvers/ResolverBase.sol:L23 @ ens_v1@91c966f)",
    ] {
        assert!(
            resolver_manifest.contains(citation),
            "resolver manifest is missing upstream citation {citation}"
        );
    }
    for admission_basis in [
        "first-party app known-resolver admissions supplied for this change",
        "(upstream: .refs/ens_app_v3/src/constants/resolverAddressData.ts:L32 @ ens_app_v3@7175858)",
        "public_resolver_4976fb03",
        "0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41",
        "no VersionableResolver",
        "no default coin-type fallback claim",
    ] {
        assert!(
            resolver_manifest.contains(admission_basis),
            "resolver manifest is missing admission basis {admission_basis}"
        );
    }

    test_dir.write_manifest("ens", "ens_v1_wrapper_l1", "v1", &wrapper_manifest)?;
    test_dir.write_manifest("ens", "ens_v1_resolver_l1", "v1", &resolver_manifest)?;

    let repository = load_repository(&test_dir.path)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.manifests().len(), 2);

    let manifests_by_source_family = repository
        .manifests()
        .iter()
        .map(|loaded_manifest| {
            (
                loaded_manifest.manifest.source_family.as_str(),
                &loaded_manifest.manifest,
            )
        })
        .collect::<BTreeMap<_, _>>();

    let wrapper = manifests_by_source_family["ens_v1_wrapper_l1"];
    assert_eq!(wrapper.chain, "ethereum-mainnet");
    assert_eq!(wrapper.deployment_epoch, "ens_v1");
    assert_eq!(wrapper.rollout_status, RolloutStatus::Active);
    assert!(wrapper.roots.is_empty());
    assert!(wrapper.discovery_rules.is_empty());
    assert!(wrapper.capability_flags.is_empty());
    assert_eq!(wrapper.contracts.len(), 1);
    assert_eq!(wrapper.contracts[0].role, "name_wrapper");
    assert_eq!(
        normalize_address(&wrapper.contracts[0].address),
        "0xd4416b13d2b3a9abae7acd5d6c2bbdbe25686401"
    );
    assert_eq!(wrapper.contracts[0].proxy_kind, "none");

    let resolver = manifests_by_source_family["ens_v1_resolver_l1"];
    assert_eq!(resolver.chain, "ethereum-mainnet");
    assert_eq!(resolver.deployment_epoch, "ens_v1");
    assert_eq!(resolver.rollout_status, RolloutStatus::Active);
    assert!(resolver.roots.is_empty());
    assert!(resolver.discovery_rules.is_empty());
    assert!(resolver.capability_flags.is_empty());
    assert_eq!(resolver.contracts.len(), 7);
    let resolver_contracts = resolver
        .contracts
        .iter()
        .map(|contract| (contract.role.as_str(), normalize_address(&contract.address)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        resolver_contracts["public_resolver"],
        "0xf29100983e058b709f3d539b0c765937b804ac15"
    );
    assert_eq!(
        resolver_contracts["public_resolver_4976fb03"],
        "0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41"
    );
    assert!(
        resolver
            .contracts
            .iter()
            .all(|contract| contract.proxy_kind == "none")
    );

    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 2);
    assert_eq!(summary.root_count, 0);
    assert_eq!(summary.contract_count, 8);
    assert_eq!(summary.capability_count, 0);
    assert_eq!(summary.discovery_rule_count, 0);

    assert_eq!(
        load_manifest_rollout_statuses(database.pool(), "ens").await?,
        vec![
            ("ens_v1_resolver_l1".to_owned(), "active".to_owned()),
            ("ens_v1_wrapper_l1".to_owned(), "active".to_owned()),
        ]
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v1_wrapper_l1")
            .await?,
        BTreeMap::new()
    );
    assert_eq!(
        load_capability_flags_for_source_family(database.pool(), "ens", "ens_v1_resolver_l1")
            .await?,
        BTreeMap::new()
    );

    let active_manifests = load_active_manifests_for_namespace(database.pool(), "ens").await?;
    assert_eq!(
        active_manifests
            .iter()
            .map(|manifest| manifest.source_family.as_str())
            .collect::<Vec<_>>(),
        vec!["ens_v1_resolver_l1", "ens_v1_wrapper_l1"]
    );
    assert!(
        active_manifests
            .iter()
            .all(|manifest| manifest.capability_flags.is_empty())
    );

    let wrapper_address = normalize_address("0xD4416b13d2b3a9aBae7AcD5D6C2BbDBE25686401");
    let resolver_addresses = BTreeSet::from([
        normalize_address("0xF29100983E058B709F3D539b0c765937B804AC15"),
        normalize_address("0x231b0Ee14048e9dCcD1d247744d114a4EB5E8E63"),
        normalize_address("0x4976fb03C32e5B8cfe2b6cCB31c09Ba78EBaBa41"),
        normalize_address("0xDaaF96c344f63131acadD0Ea35170E7892d3dfBA"),
        normalize_address("0x226159d592E2b063810a10Ebf6dcbADA94Ed68b8"),
        normalize_address("0x5FfC014343cd971B7eb70732021E26C35B744cc4"),
        normalize_address("0x1da022710dF5002339274AaDEe8D58218e9D6AB5"),
    ]);
    let watched_contracts = load_watched_contracts(database.pool()).await?;
    assert_eq!(watched_contracts.len(), 8);
    assert!(watched_contracts.iter().any(|contract| {
        contract.source_family == "ens_v1_wrapper_l1"
            && contract.address == wrapper_address
            && contract.source == WatchedContractSource::ManifestContract
    }));
    let watched_resolver_addresses = watched_contracts
        .iter()
        .filter(|contract| contract.source_family == "ens_v1_resolver_l1")
        .inspect(|contract| assert_eq!(contract.source, WatchedContractSource::ManifestContract))
        .map(|contract| contract.address.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(watched_resolver_addresses, resolver_addresses);

    let chain_plan = load_watched_chain_plan(database.pool()).await?;
    assert_eq!(chain_plan.len(), 1);
    assert_eq!(chain_plan[0].chain, "ethereum-mainnet");
    let expected_plan_addresses = resolver_addresses
        .iter()
        .cloned()
        .chain(std::iter::once(wrapper_address.clone()))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        chain_plan[0]
            .addresses
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        expected_plan_addresses
    );
    assert_eq!(chain_plan[0].manifest_root_entry_count, 0);
    assert_eq!(chain_plan[0].manifest_contract_entry_count, 8);
    assert_eq!(chain_plan[0].discovery_edge_entry_count, 0);

    let admission_state = load_discovery_admission_state(database.pool()).await?;
    assert!(admission_state.has_authoritative_address("ethereum-mainnet", &wrapper_address));
    for resolver_address in resolver_addresses {
        assert!(admission_state.has_authoritative_address("ethereum-mainnet", &resolver_address));
    }

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn checked_in_basenames_manifests_reuse_l1_resolver_address_across_active_families()
-> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    for source_family in [
        "basenames_base_primary",
        "basenames_base_registrar",
        "basenames_base_registry",
        "basenames_base_resolver",
        "basenames_execution",
        "basenames_l1_compat",
    ] {
        test_dir.write_manifest(
            "basenames",
            source_family,
            "v1",
            &checked_in_manifest_contents("basenames", source_family, "v1")?,
        )?;
    }
    test_dir.write_manifest(
        "basenames",
        "basenames_execution",
        "v2",
        &checked_in_manifest_contents("basenames", "basenames_execution", "v2")?,
    )?;
    test_dir.write_manifest(
        "basenames",
        "basenames_base_registry",
        "v2",
        &checked_in_manifest_contents("basenames", "basenames_base_registry", "v2")?,
    )?;

    let repository = load_repository(&test_dir.path)?;
    assert_eq!(repository.summary().status, ManifestLoadStatus::Loaded);
    assert_eq!(repository.manifests().len(), 8);
    assert!(
        !repository
            .manifests()
            .iter()
            .any(|manifest| { manifest.manifest.source_family == "basenames_offchain" })
    );

    let summary = sync_repository(database.pool(), &repository).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 8);
    assert_eq!(summary.active_manifest_count, 6);
    assert_eq!(summary.contract_count, 10);

    let active_manifests =
        load_active_manifests_for_namespace(database.pool(), "basenames").await?;
    assert_eq!(active_manifests.len(), 6);
    assert!(active_manifests.iter().any(|manifest| {
        manifest.source_family == "basenames_l1_compat" && manifest.manifest_version == 1
    }));
    assert!(active_manifests.iter().any(|manifest| {
        manifest.source_family == "basenames_execution" && manifest.manifest_version == 2
    }));
    assert!(active_manifests.iter().any(|manifest| {
        manifest.source_family == "basenames_base_registry" && manifest.manifest_version == 2
    }));

    let shared_l1_resolver = normalize_address("0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31");
    let shared_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        &shared_l1_resolver,
    )
    .await?;

    assert_eq!(
        query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM contract_instance_addresses
            WHERE chain_id = $1
              AND address = $2
              AND deactivated_at IS NULL
            "#
        )
        .bind("ethereum-mainnet")
        .bind(&shared_l1_resolver)
        .fetch_one(database.pool())
        .await?,
        1
    );
    assert_eq!(
        query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            WHERE mv.namespace = 'basenames'
              AND mv.chain = 'ethereum-mainnet'
              AND mv.source_family IN ('basenames_l1_compat', 'basenames_execution')
              AND mci.declaration_kind = 'contract'
              AND mci.declared_address = $1
              AND mci.contract_instance_id = $2
            "#
        )
        .bind(&shared_l1_resolver)
        .bind(shared_contract_instance_id)
        .fetch_one(database.pool())
        .await?,
        3
    );
    assert_eq!(
        query_scalar::<_, i64>(
            r#"
            SELECT COUNT(DISTINCT mci.contract_instance_id)::BIGINT
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            WHERE mv.namespace = 'basenames'
              AND mv.chain = 'ethereum-mainnet'
              AND mv.source_family IN ('basenames_l1_compat', 'basenames_execution')
              AND mci.declaration_kind = 'contract'
              AND mci.declared_address = $1
            "#
        )
        .bind(&shared_l1_resolver)
        .fetch_one(database.pool())
        .await?,
        1
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn basenames_execution_promotion_prefers_later_active_manifest_version() -> Result<()> {
    let database = TestDatabase::new().await?;
    let test_dir = TestDir::new()?;
    test_dir.write_manifest(
        "basenames",
        "basenames_execution",
        "v1",
        &checked_in_manifest_contents("basenames", "basenames_execution", "v1")?,
    )?;
    test_dir.write_manifest(
        "basenames",
        "basenames_execution",
        "v2",
        &checked_in_manifest_contents("basenames", "basenames_execution", "v2")?,
    )?;

    let summary = sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    assert_eq!(summary.status, ManifestSyncStatus::Synced);
    assert_eq!(summary.synced_manifest_count, 2);
    assert_eq!(summary.active_manifest_count, 1);
    assert_eq!(summary.contract_count, 2);
    assert_eq!(summary.capability_count, 2);

    let active_manifests =
        load_active_manifests_for_namespace(database.pool(), "basenames").await?;
    assert_eq!(active_manifests.len(), 1);
    assert_eq!(active_manifests[0].source_family, "basenames_execution");
    assert_eq!(active_manifests[0].manifest_version, 2);
    assert_eq!(
        active_manifests[0].capability_flags,
        BTreeMap::from([(
            "verified_resolution".to_owned(),
            CapabilityFlag {
                status: CapabilitySupportStatus::Supported,
                notes: Some(
                    "supports the frozen exact-surface transport-assisted direct-path public Basenames verified-resolution class"
                        .to_owned(),
                ),
            },
        )])
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
async fn manifest_drift_views_materialize_active_alert_inputs() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;

    let root_address = "0x0000000000000000000000000000000000000001";
    let proxy_address = "0x00000000000000000000000000000000000000AA";
    let first_implementation = "0x00000000000000000000000000000000000000DD";
    let second_implementation = "0x00000000000000000000000000000000000000EE";
    let inactive_execution = "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe";

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            root_address,
            proxy_address,
            Some(first_implementation),
        ),
    )?;
    test_dir.write_manifest(
        "ens",
        "ens_execution",
        "v1",
        &execution_manifest_contents("shadow"),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let proxy_contract_instance_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        proxy_address,
    )
    .await?;
    let first_implementation_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        first_implementation,
    )
    .await?;

    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &manifest_contents(
            "active",
            root_address,
            proxy_address,
            Some(second_implementation),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let proxy_after_churn = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        proxy_address,
    )
    .await?;
    let second_implementation_id = load_single_contract_instance_for_address(
        database.pool(),
        "ethereum-mainnet",
        second_implementation,
    )
    .await?;
    assert_eq!(proxy_contract_instance_id, proxy_after_churn);
    assert_ne!(first_implementation_id, second_implementation_id);

    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1000000000000000000000000000000000000000000000000000000000000000",
            block_number: 100,
            contract_address: root_address,
            code_hash: "0xroot",
            code_byte_length: 32,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1010000000000000000000000000000000000000000000000000000000000000",
            block_number: 101,
            contract_address: proxy_address,
            code_hash: "0xproxy-old",
            code_byte_length: 64,
            canonicality_state: "canonical",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1020000000000000000000000000000000000000000000000000000000000000",
            block_number: 102,
            contract_address: proxy_address,
            code_hash: "0xproxy-current",
            code_byte_length: 65,
            canonicality_state: "finalized",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1030000000000000000000000000000000000000000000000000000000000000",
            block_number: 103,
            contract_address: second_implementation,
            code_hash: "0ximpl-current",
            code_byte_length: 96,
            canonicality_state: "safe",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1040000000000000000000000000000000000000000000000000000000000000",
            block_number: 104,
            contract_address: first_implementation,
            code_hash: "0ximpl-stale",
            code_byte_length: 96,
            canonicality_state: "finalized",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1050000000000000000000000000000000000000000000000000000000000000",
            block_number: 105,
            contract_address: inactive_execution,
            code_hash: "0xinactive",
            code_byte_length: 128,
            canonicality_state: "finalized",
        },
    )
    .await?;
    insert_raw_code_hash_observation(
        database.pool(),
        RawCodeHashObservation {
            chain: "ethereum-mainnet",
            block_hash: "0x1060000000000000000000000000000000000000000000000000000000000000",
            block_number: 106,
            contract_address: root_address,
            code_hash: "0xorphan-root",
            code_byte_length: 33,
            canonicality_state: "orphaned",
        },
    )
    .await?;

    let manifest_id =
        active_manifest_id_for_source_family(database.pool(), "ens", "ens_v2_registry_l1").await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:source",
        "SourceManifestUpdated",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "finalized",
    )
    .await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:proxy",
        "ProxyImplementationChanged",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "canonical",
    )
    .await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:capability",
        "CapabilityChanged",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "safe",
    )
    .await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:orphan",
        "SourceManifestUpdated",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "orphaned",
    )
    .await?;
    insert_manifest_normalized_event(
        database.pool(),
        "manifest:registry:not-alert-material",
        "NameRecordChanged",
        "ens_v2_registry_l1",
        1,
        Some(manifest_id),
        "finalized",
    )
    .await?;

    let drift_inputs = load_manifest_drift_inputs(database.pool()).await?;
    assert_eq!(drift_inputs.active_manifests.len(), 1);
    assert_eq!(
        drift_inputs.active_manifests[0].source_family,
        "ens_v2_registry_l1"
    );
    assert_eq!(
        drift_inputs.active_manifests[0].manifest_payload["rollout_status"],
        "active"
    );
    assert_eq!(
        drift_inputs.active_manifests[0].manifest_payload["source_family"],
        "ens_v2_registry_l1"
    );

    assert_eq!(drift_inputs.declared_contracts.len(), 2);
    assert!(drift_inputs.declared_contracts.iter().any(|entry| {
        entry.declaration_kind == DECLARATION_KIND_ROOT
            && entry.declaration_name == "RootRegistry"
            && entry.declared_address == normalize_address(root_address)
            && entry.implementation_contract_instance_id.is_none()
    }));
    let declared_proxy = drift_inputs
        .declared_contracts
        .iter()
        .find(|entry| entry.declaration_kind == DECLARATION_KIND_CONTRACT)
        .expect("active manifest contract declaration must be present");
    assert_eq!(declared_proxy.declaration_name, "registry");
    assert_eq!(
        declared_proxy.contract_instance_id,
        proxy_contract_instance_id
    );
    assert_eq!(
        declared_proxy.declared_address,
        normalize_address(proxy_address)
    );
    assert_eq!(declared_proxy.proxy_kind.as_deref(), Some("erc1967"));
    assert_eq!(
        declared_proxy.implementation_contract_instance_id,
        Some(second_implementation_id)
    );
    assert_eq!(
        declared_proxy.declared_implementation_address.as_deref(),
        Some(normalize_address(second_implementation).as_str())
    );

    assert_eq!(drift_inputs.proxy_implementation_edges.len(), 1);
    let proxy_edge = &drift_inputs.proxy_implementation_edges[0];
    assert_eq!(
        proxy_edge.proxy_contract_instance_id,
        proxy_contract_instance_id
    );
    assert_eq!(
        proxy_edge.implementation_contract_instance_id,
        second_implementation_id
    );
    assert_eq!(
        proxy_edge.proxy_address.as_deref(),
        Some(normalize_address(proxy_address).as_str())
    );
    assert_eq!(
        proxy_edge.implementation_address.as_deref(),
        Some(normalize_address(second_implementation).as_str())
    );
    assert_eq!(proxy_edge.proxy_kind.as_deref(), Some("erc1967"));
    assert_eq!(
        proxy_edge.admission,
        MANIFEST_PROXY_IMPLEMENTATION_ADMISSION
    );

    let code_hashes_by_address = drift_inputs
        .code_hash_observations
        .iter()
        .map(|observation| (observation.address.as_str(), observation))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(code_hashes_by_address.len(), 3);
    assert_eq!(
        code_hashes_by_address[normalize_address(root_address).as_str()].code_hash,
        "0xroot"
    );
    assert_eq!(
        code_hashes_by_address[normalize_address(proxy_address).as_str()].code_hash,
        "0xproxy-current"
    );
    assert_eq!(
        code_hashes_by_address[normalize_address(second_implementation).as_str()].code_hash,
        "0ximpl-current"
    );
    assert!(!code_hashes_by_address.contains_key(normalize_address(first_implementation).as_str()));
    assert!(!code_hashes_by_address.contains_key(normalize_address(inactive_execution).as_str()));

    assert_eq!(
        drift_inputs
            .normalized_manifest_events
            .iter()
            .map(|event| event.event_kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            "CapabilityChanged",
            "ProxyImplementationChanged",
            "SourceManifestUpdated",
        ]
    );
    assert!(
        drift_inputs
            .normalized_manifest_events
            .iter()
            .all(|event| event.canonicality_state != "orphaned")
    );

    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM manifest_versions WHERE namespace = 'ens'"
        )
        .fetch_one(database.pool())
        .await?,
        2
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
        .fetch_one(database.pool())
        .await?,
        1
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
            && contract.source_family == "ens_v2_registry_l1"
    }));
    assert!(watched_contracts.iter().any(|contract| {
        contract.address == "0x00000000000000000000000000000000000000cc"
            && contract.source == WatchedContractSource::DiscoveryEdge
            && contract.source_family == "ens_v2_registry_l1"
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

    let selected_source_family_plan = load_watched_source_selector_plan(
        database.pool(),
        "ethereum-mainnet",
        WatchedSourceSelector::SourceFamily("ens_v2_registry_l1".to_owned()),
        100,
        200,
    )
    .await?;
    assert_eq!(
        selected_source_family_plan.watched_chain_plan,
        watched_chain_plan[0]
    );
    assert_eq!(selected_source_family_plan.selected_targets.len(), 4);
    assert!(
        selected_source_family_plan
            .selected_targets
            .iter()
            .all(|target| target.source_family == "ens_v2_registry_l1")
    );
    let mut sorted_targets = selected_source_family_plan.selected_targets.clone();
    sorted_targets.sort();
    assert_eq!(selected_source_family_plan.selected_targets, sorted_targets);
    assert_eq!(
        selected_source_family_plan
            .selected_targets
            .iter()
            .find(|target| { target.address == "0x00000000000000000000000000000000000000cc" })
            .map(|target| (target.effective_from_block, target.effective_to_block)),
        Some((123, 200))
    );

    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn byte_identical_manifest_sync_preserves_discovery_admission_epoch() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
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

    sync_repository(database.pool(), &repository).await?;
    let epoch_after_initial_sync = load_discovery_admission_epoch(database.pool(), chain).await?;

    sync_repository(database.pool(), &repository).await?;
    assert_eq!(
        load_discovery_admission_epoch(database.pool(), chain).await?,
        epoch_after_initial_sync,
        "reloading the same repository must not invalidate retained-history authority"
    );

    database.cleanup().await
}

#[tokio::test]
async fn manifest_sync_bumps_epoch_for_authority_mutations_and_repairs() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let source_family = "ens_v2_registry_l1";
    let registry_address = "0x0000000000000000000000000000000000000002";
    let write_repository = |contract_start_block| -> Result<ManifestRepository> {
        test_dir.write_manifest(
            "ens",
            source_family,
            "v1",
            &start_block_manifest_contents(
                Some(100),
                Some(contract_start_block),
                "0x0000000000000000000000000000000000000003",
            ),
        )?;
        load_repository(&test_dir.path)
    };
    let repository = write_repository(200)?;
    sync_repository(database.pool(), &repository).await?;

    let epoch_before_capability_repair =
        load_discovery_admission_epoch(database.pool(), chain).await?;
    sqlx::query(
        r#"
        DELETE FROM manifest_capability_flags
        WHERE manifest_id = (
            SELECT manifest_id
            FROM manifest_versions
            WHERE chain = $1 AND source_family = $2
        )
        "#,
    )
    .bind(chain)
    .bind(source_family)
    .execute(database.pool())
    .await?;
    sync_repository(database.pool(), &repository).await?;
    let epoch_after_capability_repair =
        load_discovery_admission_epoch(database.pool(), chain).await?;
    assert!(
        epoch_after_capability_repair > epoch_before_capability_repair,
        "repairing persisted capability authority must invalidate retained coverage"
    );

    sqlx::query(
        r#"
        DELETE FROM manifest_discovery_rules
        WHERE manifest_id = (
            SELECT manifest_id
            FROM manifest_versions
            WHERE chain = $1 AND source_family = $2
        )
        "#,
    )
    .bind(chain)
    .bind(source_family)
    .execute(database.pool())
    .await?;
    sync_repository(database.pool(), &repository).await?;
    let epoch_after_rule_repair = load_discovery_admission_epoch(database.pool(), chain).await?;
    assert!(
        epoch_after_rule_repair > epoch_after_capability_repair,
        "repairing persisted discovery-rule authority must invalidate retained coverage"
    );

    sqlx::query(
        r#"
        UPDATE manifest_contract_instances
        SET abi_ref = 'stale-test-abi'
        WHERE manifest_id = (
            SELECT manifest_id
            FROM manifest_versions
            WHERE chain = $1 AND source_family = $2
        )
          AND declaration_kind = 'root'
        "#,
    )
    .bind(chain)
    .bind(source_family)
    .execute(database.pool())
    .await?;
    sync_repository(database.pool(), &repository).await?;
    let epoch_after_entry_repair = load_discovery_admission_epoch(database.pool(), chain).await?;
    assert!(
        epoch_after_entry_repair > epoch_after_rule_repair,
        "repairing a persisted manifest entry must invalidate retained coverage"
    );

    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_from_block_number = 1
        WHERE chain_id = $1
          AND address = lower($2)
          AND deactivated_at IS NULL
        "#,
    )
    .bind(chain)
    .bind(registry_address)
    .execute(database.pool())
    .await?;
    sync_repository(database.pool(), &repository).await?;
    let epoch_after_range_repair = load_discovery_admission_epoch(database.pool(), chain).await?;
    assert!(
        epoch_after_range_repair > epoch_after_entry_repair,
        "repairing a manifest-declared active range must invalidate retained coverage"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<i64>>(
            r#"
            SELECT active_from_block_number
            FROM contract_instance_addresses
            WHERE chain_id = $1
              AND address = lower($2)
              AND deactivated_at IS NULL
            "#,
        )
        .bind(chain)
        .bind(registry_address)
        .fetch_one(database.pool())
        .await?,
        Some(200)
    );

    let changed_repository = write_repository(201)?;
    sync_repository(database.pool(), &changed_repository).await?;
    let epoch_after_manifest_change =
        load_discovery_admission_epoch(database.pool(), chain).await?;
    assert!(
        epoch_after_manifest_change > epoch_after_range_repair,
        "a real repository manifest mutation must invalidate retained coverage"
    );

    let empty_dir = TestDir::new()?;
    sync_repository(database.pool(), &load_repository(&empty_dir.path)?).await?;
    assert!(
        load_discovery_admission_epoch(database.pool(), chain).await? > epoch_after_manifest_change,
        "removing a stored manifest must invalidate retained coverage"
    );

    database.cleanup().await
}

#[tokio::test]
async fn discovery_admission_epoch_bumps_on_every_edge_mutation() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
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
    assert_eq!(
        load_discovery_admission_epoch(database.pool(), chain).await?,
        0
    );

    // Manifest sync inserts the managed erc1967 proxy edge — the epoch must
    // move in the same pass.
    sync_repository(database.pool(), &repository).await?;
    let epoch_after_managed_insert = load_discovery_admission_epoch(database.pool(), chain).await?;
    assert!(
        epoch_after_managed_insert >= 1,
        "managed-edge insert must bump the discovery admission epoch, got {epoch_after_managed_insert}"
    );

    // A reconciled observation insert bumps again.
    let summary = reconcile_discovery_observations(
        database.pool(),
        "epoch-test-observation",
        &[DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: "0x00000000000000000000000000000000000000AA".to_owned(),
            to_address: "0x00000000000000000000000000000000000000BB".to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: "epoch-test-observation".to_owned(),
            active_from_block_number: Some(10),
            active_from_block_hash: None,
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "epoch-test",
                "observation_key": "epoch-test-edge",
            }),
        }],
    )
    .await?;
    assert_eq!(summary.inserted_edge_count, 1);
    let epoch_after_observation_insert =
        load_discovery_admission_epoch(database.pool(), chain).await?;
    assert!(
        epoch_after_observation_insert > epoch_after_managed_insert,
        "observation insert must bump the epoch ({epoch_after_managed_insert} -> {epoch_after_observation_insert})"
    );

    // Reconciling the same source with no remaining observations deactivates
    // the edge — deactivation bumps too.
    let summary =
        reconcile_discovery_observations(database.pool(), "epoch-test-observation", &[]).await?;
    assert_eq!(summary.deactivated_edge_count, 1);
    let epoch_after_deactivation = load_discovery_admission_epoch(database.pool(), chain).await?;
    assert!(
        epoch_after_deactivation > epoch_after_observation_insert,
        "edge deactivation must bump the epoch ({epoch_after_observation_insert} -> {epoch_after_deactivation})"
    );

    database.cleanup().await
}

#[tokio::test]
async fn coverage_reads_ignore_facts_without_completed_containing_jobs() -> Result<()> {
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let source_family = "ens_v1_registry_l1";
    let pending_address = "0x0000000000000000000000000000000000000d01";
    let outside_job_address = "0x0000000000000000000000000000000000000d02";
    insert_untrusted_backfill_coverage_fact(
        database.pool(),
        chain,
        "pending",
        100,
        120,
        source_family,
        pending_address,
        100,
        120,
    )
    .await?;
    insert_untrusted_backfill_coverage_fact(
        database.pool(),
        chain,
        "completed",
        105,
        115,
        source_family,
        outside_job_address,
        100,
        120,
    )
    .await?;

    let requirements = vec![
        RequiredWatchedTuple {
            source_family: source_family.to_owned(),
            address: normalize_address(pending_address),
            required_from_block: 100,
            required_to_block: 120,
        },
        RequiredWatchedTuple {
            source_family: source_family.to_owned(),
            address: normalize_address(outside_job_address),
            required_from_block: 100,
            required_to_block: 120,
        },
    ];
    assert_eq!(
        find_uncovered_required_watched_tuples(database.pool(), chain, &requirements, 10,).await?,
        requirements
            .iter()
            .map(|requirement| UncoveredWatchedTuple {
                source_family: requirement.source_family.clone(),
                address: requirement.address.clone(),
                required_from_block: requirement.required_from_block,
                required_to_block: requirement.required_to_block,
            })
            .collect::<Vec<_>>(),
        "pending jobs and facts outside their completed job range are not promotion authority"
    );

    database.cleanup().await
}

#[tokio::test]
async fn closed_historical_discovery_interval_remains_required_for_coverage() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let registry_address = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
    let resolver_address = "0x0000000000000000000000000000000000000c01";
    let unbounded_resolver_address = "0x0000000000000000000000000000000000000c02";
    let registry_source_family = "ens_v1_registry_l1";
    let resolver_source_family = "ens_v1_resolver_l1";

    test_dir.write_manifest(
        "ens",
        registry_source_family,
        "v3",
        &checked_in_manifest_contents("ens", registry_source_family, "v3")?,
    )?;
    test_dir.write_manifest(
        "ens",
        resolver_source_family,
        "v1",
        &checked_in_manifest_contents("ens", resolver_source_family, "v1")?,
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: registry_address.to_owned(),
            to_address: resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "historical-coverage-test".to_owned(),
            active_from_block_number: Some(100),
            active_from_block_hash: Some(format!("0x{:064x}", 100)),
            active_to_block_number: Some(160),
            active_to_block_hash: Some(format!("0x{:064x}", 160)),
            provenance: serde_json::json!({
                "provider": "unit-test",
                "observation_key": "historical-resolver",
            }),
        },
    )
    .await?;
    let resolver_contract_instance_id = summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("resolver discovery must admit a target contract instance");
    let unbounded_summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: registry_address.to_owned(),
            to_address: unbounded_resolver_address.to_owned(),
            edge_kind: "resolver".to_owned(),
            discovery_source: "unbounded-historical-coverage-test".to_owned(),
            active_from_block_number: Some(110),
            active_from_block_hash: Some(format!("0x{:064x}", 110)),
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: serde_json::json!({
                "provider": "unit-test",
                "observation_key": "unbounded-historical-resolver",
            }),
        },
    )
    .await?;
    let unbounded_resolver_contract_instance_id = unbounded_summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("unbounded resolver discovery must admit a target contract instance");

    let scoped_targets = vec![
        (chain.to_owned(), normalize_address(resolver_address)),
        (
            chain.to_owned(),
            normalize_address(unbounded_resolver_address),
        ),
    ];
    let current_resolvers = load_watched_contracts_by_source_family_and_addresses(
        database.pool(),
        resolver_source_family,
        &scoped_targets,
    )
    .await?;
    assert_eq!(current_resolvers.len(), 2);
    assert!(current_resolvers.iter().all(|contract| {
        contract.source == WatchedContractSource::DiscoveryEdge
            && contract.source_family == resolver_source_family
    }));
    assert!(current_resolvers.iter().any(|contract| {
        contract.contract_instance_id == resolver_contract_instance_id
            && contract.address == normalize_address(resolver_address)
    }));
    assert!(current_resolvers.iter().any(|contract| {
        contract.contract_instance_id == unbounded_resolver_contract_instance_id
            && contract.address == normalize_address(unbounded_resolver_address)
    }));

    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET deactivated_at = now()
        WHERE to_contract_instance_id = $1
          AND edge_kind = 'resolver'
        "#,
    )
    .bind(resolver_contract_instance_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET deactivated_at = now()
        WHERE to_contract_instance_id = $1
          AND edge_kind = 'resolver'
        "#,
    )
    .bind(unbounded_resolver_contract_instance_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET deactivated_at = now()
        WHERE contract_instance_id = $1
        "#,
    )
    .bind(unbounded_resolver_contract_instance_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET deactivated_at = now()
        WHERE contract_instance_id = $1
        "#,
    )
    .bind(resolver_contract_instance_id)
    .execute(database.pool())
    .await?;
    let current_after_close = load_watched_contracts_by_chain(database.pool(), chain).await?;
    assert!(current_after_close.iter().all(|contract| {
        contract.address != normalize_address(resolver_address)
            && contract.address != normalize_address(unbounded_resolver_address)
    }));
    assert!(
        load_watched_contracts_by_addresses(database.pool(), &scoped_targets)
            .await?
            .is_empty(),
        "scoped current reads must exclude both bounded and unbounded deactivated intervals"
    );

    let historical_after_close =
        load_historical_watched_contracts_by_chain(database.pool(), chain).await?;
    assert!(historical_after_close.iter().any(|contract| {
        contract.contract_instance_id == resolver_contract_instance_id
            && contract.address == normalize_address(resolver_address)
            && contract.source == WatchedContractSource::DiscoveryEdge
            && contract.source_family == resolver_source_family
            && contract.active_from_block_number == Some(100)
            && contract.active_to_block_number == Some(160)
    }));
    assert!(historical_after_close.iter().all(|contract| {
        contract.contract_instance_id != unbounded_resolver_contract_instance_id
            && contract.address != normalize_address(unbounded_resolver_address)
    }));
    let required = load_required_watched_tuples(
        database.pool(),
        chain,
        120,
        180,
        &[resolver_source_family.to_owned()],
    )
    .await?;
    assert_eq!(
        required,
        vec![RequiredWatchedTuple {
            source_family: resolver_source_family.to_owned(),
            address: normalize_address(resolver_address),
            required_from_block: 120,
            required_to_block: 160,
        }],
        "the public required-tuple loader must retain the closed historical interval"
    );

    let uncovered = find_uncovered_watched_tuples(
        database.pool(),
        chain,
        120,
        180,
        &[resolver_source_family.to_owned()],
        10,
    )
    .await?;
    assert_eq!(
        uncovered,
        vec![UncoveredWatchedTuple {
            source_family: resolver_source_family.to_owned(),
            address: normalize_address(resolver_address),
            required_from_block: 120,
            required_to_block: 160,
        }],
        "a later deactivation must not erase a historically authoritative interval that intersects the promoted range"
    );

    database.cleanup().await
}

#[tokio::test]
async fn ens_v2_recovery_targets_keep_closed_discovery_identity_and_exact_interval() -> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let registry_address = "0x00000000000000000000000000000000000000aa";
    let child_address = "0x00000000000000000000000000000000000000cc";
    test_dir.write_manifest(
        "ens",
        "ens_v2_registry_l1",
        "v1",
        &format!(
            r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v2_registry_l1"
chain = "{chain}"
deployment_epoch = "ens_v2"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "RootRegistry"
address = "0x0000000000000000000000000000000000000001"
start_block = 90

[[contracts]]
role = "registry"
address = "{registry_address}"
proxy_kind = "none"
start_block = 100

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[abi.events]]
name = "SubregistryUpdated"
fragment = "event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender)"
"#
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    let summary = persist_discovery_observation(
        database.pool(),
        &DiscoveryObservation {
            chain: chain.to_owned(),
            from_address: registry_address.to_owned(),
            to_address: child_address.to_owned(),
            edge_kind: "subregistry".to_owned(),
            discovery_source: "recovery-target-test".to_owned(),
            active_from_block_number: Some(120),
            active_from_block_hash: Some(format!("0x{:064x}", 120)),
            active_to_block_number: Some(160),
            active_to_block_hash: Some(format!("0x{:064x}", 160)),
            provenance: serde_json::json!({
                "provider": "unit-test",
                "observation_key": "closed-child",
            }),
        },
    )
    .await?;
    let child_contract_instance_id = summary.admitted_edges[0]
        .to_contract_instance_id
        .expect("child discovery must admit a contract identity");
    sqlx::query(
        r#"
        UPDATE discovery_edges
        SET deactivated_at = now()
        WHERE to_contract_instance_id = $1
          AND edge_kind = 'subregistry'
        "#,
    )
    .bind(child_contract_instance_id)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET deactivated_at = now()
        WHERE contract_instance_id = $1
        "#,
    )
    .bind(child_contract_instance_id)
    .execute(database.pool())
    .await?;

    let targets =
        load_ens_v2_retained_history_recovery_targets(database.pool(), chain, 200).await?;
    assert!(targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v2_registry_l1".to_owned(),
        contract_instance_id: child_contract_instance_id,
        address: normalize_address(child_address),
        effective_from_block: 120,
        effective_to_block: Some(160),
    }));
    let automatic_targets =
        load_ens_v2_authoritative_discovery_bootstrap_targets(database.pool(), chain, 200).await?;
    assert!(automatic_targets.contains(&ManifestBootstrapTarget {
        source_family: "ens_v2_registry_l1".to_owned(),
        contract_instance_id: child_contract_instance_id,
        address: normalize_address(child_address),
        effective_from_block: 120,
        effective_to_block: Some(160),
    }));

    let mut child_plan = load_historical_watched_source_selector_plan(
        database.pool(),
        chain,
        WatchedSourceSelector::WatchedTargetSet(vec![child_contract_instance_id.into()]),
        120,
        160,
    )
    .await?;
    child_plan.selected_targets.retain(|target| {
        target.source_family == "ens_v2_registry_l1"
            && target.contract_instance_id == child_contract_instance_id
    });
    assert_eq!(
        child_plan.selected_targets,
        vec![WatchedBackfillTarget {
            source_family: "ens_v2_registry_l1".to_owned(),
            contract_instance_id: child_contract_instance_id,
            address: normalize_address(child_address),
            effective_from_block: 120,
            effective_to_block: 160,
        }]
    );

    database.cleanup().await
}

#[tokio::test]
async fn bounded_and_unbounded_deactivated_manifest_addresses_match_historical_coverage()
-> Result<()> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-mainnet";
    let source_family = "ens_v2_registry_l1";
    let replaced_address = "0x00000000000000000000000000000000000000AA";
    test_dir.write_manifest(
        "ens",
        source_family,
        "v1",
        &manifest_contents(
            "active",
            "0x0000000000000000000000000000000000000001",
            replaced_address,
            Some("0x00000000000000000000000000000000000000DD"),
        ),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;

    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET deactivated_at = now(),
            active_to_block_number = 150,
            active_to_block_hash = $3
        WHERE chain_id = $1
          AND address = lower($2)
        "#,
    )
    .bind(chain)
    .bind(replaced_address)
    .bind(format!("0x{:064x}", 150))
    .execute(database.pool())
    .await?;

    let normalized_replaced_address = normalize_address(replaced_address);
    assert!(
        load_watched_contracts_by_chain(database.pool(), chain)
            .await?
            .iter()
            .all(|contract| contract.address != normalized_replaced_address),
        "a bounded deactivated declaration is no longer a current watch target"
    );
    assert!(
        load_watched_contracts_by_addresses(
            database.pool(),
            &[(chain.to_owned(), normalized_replaced_address.clone())],
        )
        .await?
        .is_empty(),
        "scoped current reads must use the same deactivation filter"
    );
    assert!(
        load_historical_watched_contracts_by_chain(database.pool(), chain)
            .await?
            .iter()
            .any(|contract| {
                contract.address == normalized_replaced_address
                    && contract.source == WatchedContractSource::ManifestContract
                    && contract.active_to_block_number == Some(150)
            }),
        "a bounded deactivated declaration remains historical replay authority"
    );
    assert!(
        load_required_watched_tuples(
            database.pool(),
            chain,
            100,
            200,
            &[source_family.to_owned()],
        )
        .await?
        .iter()
        .any(|tuple| {
            tuple.address == normalized_replaced_address && tuple.required_to_block == 150
        }),
        "coverage must retain the same bounded declaration interval as the historical view"
    );

    sqlx::query(
        r#"
        UPDATE contract_instance_addresses
        SET active_to_block_number = NULL,
            active_to_block_hash = NULL
        WHERE chain_id = $1
          AND address = lower($2)
        "#,
    )
    .bind(chain)
    .bind(replaced_address)
    .execute(database.pool())
    .await?;

    let required = load_required_watched_tuples(
        database.pool(),
        chain,
        100,
        200,
        &[source_family.to_owned()],
    )
    .await?;
    assert!(
        required
            .iter()
            .all(|tuple| tuple.address != normalized_replaced_address),
        "a deactivated declaration without a terminal block cannot become an unbounded historical coverage requirement"
    );
    assert!(
        load_historical_watched_contracts_by_chain(database.pool(), chain)
            .await?
            .iter()
            .all(|contract| contract.address != normalized_replaced_address),
        "historical replay and coverage must both reject an unbounded deactivated declaration"
    );

    database.cleanup().await
}

// ---------------------------------------------------------------------------
// Streamed full-source reconciliation parity (#168)
//
// Each scenario builds two identically seeded databases, reconciles one with
// the in-memory `reconcile_discovery_observations` and the other with the
// streamed temp-table variant over the identical observation set, and
// asserts identical summaries and byte-identical canonicalized
// `discovery_edges` outcomes (contract-instance UUIDs differ across
// databases, so edges are canonicalized onto their addresses).
// ---------------------------------------------------------------------------

const STREAMED_PARITY_SOURCE: &str = "streamed-parity-observations";
const STREAMED_PARITY_CHAIN: &str = "ethereum-mainnet";
const STREAMED_PARITY_REGISTRY: &str = "0x00000000000C2E074eC69A0dFb2997BA6C7d2E1E";
const STREAMED_PARITY_OWNER_A: &str = "0x00000000000000000000000000000000000000Aa";
const STREAMED_PARITY_OWNER_B: &str = "0x00000000000000000000000000000000000000Bb";
const STREAMED_PARITY_OWNER_C: &str = "0x00000000000000000000000000000000000000Cc";
const STREAMED_PARITY_RESOLVER: &str = "0x0000000000000000000000000000000000000011";
const STREAMED_PARITY_STRANGER: &str = "0x0000000000000000000000000000000000000099";
const STREAMED_PARITY_ZERO: &str = "0x0000000000000000000000000000000000000000";

fn streamed_parity_manifest_contents() -> String {
    format!(
        r#"
manifest_version = 1
namespace = "ens"
source_family = "ens_v1_registry_l1"
chain = "{STREAMED_PARITY_CHAIN}"
deployment_epoch = "ens_v1"
rollout_status = "active"
normalizer_version = "ensip15@ens-normalize-0.1.1"

[capability_flags]
declared_children = "supported"

[[roots]]
name = "ENSRegistry"
address = "{STREAMED_PARITY_REGISTRY}"

[[contracts]]
role = "registry"
address = "{STREAMED_PARITY_REGISTRY}"
proxy_kind = "none"

[[discovery_rules]]
edge_kind = "subregistry"
from_role = "registry"
admission = "reachable_from_root"

[[discovery_rules]]
edge_kind = "resolver"
from_role = "registry"
admission = "reachable_from_root"
"#
    )
}

fn streamed_parity_observation(
    key: &str,
    from_address: &str,
    to_address: &str,
    edge_kind: &str,
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
) -> DiscoveryObservation {
    DiscoveryObservation {
        chain: STREAMED_PARITY_CHAIN.to_owned(),
        from_address: from_address.to_owned(),
        to_address: to_address.to_owned(),
        edge_kind: edge_kind.to_owned(),
        discovery_source: STREAMED_PARITY_SOURCE.to_owned(),
        active_from_block_number: Some(block_number),
        active_from_block_hash: Some(format!("0x{block_number:064x}")),
        active_to_block_number: None,
        active_to_block_hash: None,
        provenance: serde_json::json!({
            "provider": "streamed-parity-test",
            "observation_key": key,
            "transaction_index": transaction_index,
            "log_index": log_index,
        }),
    }
}

struct VecDiscoveryObservationPageSource {
    items: Vec<(String, DiscoveryObservation)>,
    page_limit: usize,
}

impl VecDiscoveryObservationPageSource {
    fn new(observations: &[DiscoveryObservation], page_limit: usize) -> Self {
        let mut items = observations
            .iter()
            .map(|observation| {
                let key = observation
                    .provenance
                    .get("observation_key")
                    .and_then(serde_json::Value::as_str)
                    .expect("parity observations carry observation keys")
                    .to_owned();
                (key, observation.clone())
            })
            .collect::<Vec<_>>();
        items.sort_by(|(left, _), (right, _)| left.cmp(right));
        Self { items, page_limit }
    }
}

impl DiscoveryObservationPageSource for VecDiscoveryObservationPageSource {
    async fn load_page(
        &self,
        after_key: Option<&str>,
        limit: i64,
    ) -> Result<Vec<(String, DiscoveryObservation)>> {
        let limit = usize::try_from(limit.max(0))?.min(self.page_limit.max(1));
        Ok(self
            .items
            .iter()
            .filter(|(key, _)| after_key.is_none_or(|after_key| key.as_str() > after_key))
            .take(limit)
            .cloned()
            .collect())
    }
}

/// One `discovery_edges` row canonicalized onto addresses instead of the
/// per-database contract-instance UUIDs, with timestamps reduced to the
/// active/deactivated state (wall-clock `deactivated_at`/`admitted_at`
/// values cannot match across runs).
type CanonicalDiscoveryEdgeRow = (
    String,         // chain_id
    String,         // edge_kind
    Option<String>, // from address
    Option<String>, // to address
    Option<i64>,    // source_manifest_id
    String,         // admission
    Option<i64>,    // active_from_block_number
    Option<String>, // active_from_block_hash
    Option<i64>,    // active_to_block_number
    Option<String>, // active_to_block_hash
    bool,           // is_active
    String,         // provenance (jsonb text, includes terminal positions)
);

async fn load_canonical_discovery_edges(
    pool: &PgPool,
    discovery_source: &str,
) -> Result<Vec<CanonicalDiscoveryEdgeRow>> {
    sqlx::query_as::<_, CanonicalDiscoveryEdgeRow>(
        r#"
        SELECT
            de.chain_id,
            de.edge_kind,
            (
                SELECT cia.address
                FROM contract_instance_addresses cia
                WHERE cia.contract_instance_id = de.from_contract_instance_id
                ORDER BY (cia.deactivated_at IS NULL) DESC, cia.admitted_at DESC
                LIMIT 1
            ) AS from_address,
            (
                SELECT cia.address
                FROM contract_instance_addresses cia
                WHERE cia.contract_instance_id = de.to_contract_instance_id
                ORDER BY (cia.deactivated_at IS NULL) DESC, cia.admitted_at DESC
                LIMIT 1
            ) AS to_address,
            de.source_manifest_id,
            de.admission,
            de.active_from_block_number,
            de.active_from_block_hash,
            de.active_to_block_number,
            de.active_to_block_hash,
            (de.deactivated_at IS NULL) AS is_active,
            de.provenance::TEXT AS provenance
        FROM discovery_edges de
        WHERE de.discovery_source = $1
        ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
        "#,
    )
    .bind(discovery_source)
    .fetch_all(pool)
    .await
    .context("failed to load canonical discovery edges for parity comparison")
}

struct StreamedParityFixture {
    seed_observation_sets: Vec<Vec<DiscoveryObservation>>,
    observations: Vec<DiscoveryObservation>,
}

async fn setup_streamed_parity_database(fixture: &StreamedParityFixture) -> Result<TestDatabase> {
    let test_dir = TestDir::new()?;
    let database = TestDatabase::new().await?;
    test_dir.write_manifest(
        "ens",
        "ens_v1_registry_l1",
        "v1",
        &streamed_parity_manifest_contents(),
    )?;
    sync_repository(database.pool(), &load_repository(&test_dir.path)?).await?;
    for seed_observations in &fixture.seed_observation_sets {
        reconcile_discovery_observations(
            database.pool(),
            STREAMED_PARITY_SOURCE,
            seed_observations,
        )
        .await?;
    }
    Ok(database)
}

async fn assert_streamed_reconciliation_parity(
    fixture: StreamedParityFixture,
) -> Result<(
    DiscoveryReconciliationSummary,
    Vec<CanonicalDiscoveryEdgeRow>,
)> {
    let in_memory_database = setup_streamed_parity_database(&fixture).await?;
    let streamed_database = setup_streamed_parity_database(&fixture).await?;

    let in_memory_summary = reconcile_discovery_observations(
        in_memory_database.pool(),
        STREAMED_PARITY_SOURCE,
        &fixture.observations,
    )
    .await?;
    // Tiny pages and batches exercise every internal paging loop.
    let page_source = VecDiscoveryObservationPageSource::new(&fixture.observations, 2);
    let streamed_summary = reconcile_discovery_observations_streamed_with_options(
        streamed_database.pool(),
        STREAMED_PARITY_SOURCE,
        &page_source,
        StreamedDiscoveryReconciliationOptions {
            max_deactivations_override: None,
            coarse_deactivation_cap_override: None,
            observation_page_limit: 2,
            mutation_batch_size: 2,
        },
    )
    .await?;

    assert!(
        streamed_summary.admitted_edges.is_empty(),
        "the streamed summary intentionally returns no admitted edges"
    );
    assert_eq!(
        streamed_summary.active_edge_count, in_memory_summary.active_edge_count,
        "active edge counts must match"
    );
    assert_eq!(
        streamed_summary.admitted_edge_count, in_memory_summary.admitted_edge_count,
        "admitted edge counts must match"
    );
    assert_eq!(
        streamed_summary.inserted_edge_count, in_memory_summary.inserted_edge_count,
        "inserted edge counts must match"
    );
    assert_eq!(
        streamed_summary.deactivated_edge_count, in_memory_summary.deactivated_edge_count,
        "deactivated edge counts must match"
    );
    assert_eq!(
        streamed_summary.admission_epoch_bump_count, in_memory_summary.admission_epoch_bump_count,
        "admission epoch bump counts must match"
    );

    let in_memory_edges =
        load_canonical_discovery_edges(in_memory_database.pool(), STREAMED_PARITY_SOURCE).await?;
    let streamed_edges =
        load_canonical_discovery_edges(streamed_database.pool(), STREAMED_PARITY_SOURCE).await?;
    assert_eq!(
        streamed_edges, in_memory_edges,
        "streamed reconciliation must produce identical discovery_edges outcomes"
    );
    assert_eq!(
        load_discovery_admission_epoch(streamed_database.pool(), STREAMED_PARITY_CHAIN).await?,
        load_discovery_admission_epoch(in_memory_database.pool(), STREAMED_PARITY_CHAIN).await?,
        "admission epochs must match"
    );

    in_memory_database.cleanup().await?;
    streamed_database.cleanup().await?;
    Ok((in_memory_summary, streamed_edges))
}

#[tokio::test]
async fn streamed_reconciliation_parity_fresh_inserts() -> Result<()> {
    let (summary, edges) = assert_streamed_reconciliation_parity(StreamedParityFixture {
        seed_observation_sets: Vec::new(),
        observations: vec![
            streamed_parity_observation(
                "k-eth",
                STREAMED_PARITY_REGISTRY,
                STREAMED_PARITY_OWNER_A,
                "subregistry",
                10,
                0,
                1,
            ),
            streamed_parity_observation(
                "k-sub",
                STREAMED_PARITY_OWNER_A,
                STREAMED_PARITY_OWNER_B,
                "subregistry",
                11,
                0,
                1,
            ),
            streamed_parity_observation(
                "k-res",
                STREAMED_PARITY_REGISTRY,
                STREAMED_PARITY_RESOLVER,
                "resolver",
                12,
                0,
                2,
            ),
        ],
    })
    .await?;

    assert_eq!(summary.inserted_edge_count, 3);
    assert_eq!(summary.deactivated_edge_count, 0);
    assert_eq!(summary.active_edge_count, 3);
    assert_eq!(edges.len(), 3);
    Ok(())
}

#[tokio::test]
async fn streamed_reconciliation_parity_steady_state_no_op() -> Result<()> {
    let observations = vec![
        streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_A,
            "subregistry",
            10,
            0,
            1,
        ),
        streamed_parity_observation(
            "k-sub",
            STREAMED_PARITY_OWNER_A,
            STREAMED_PARITY_OWNER_B,
            "subregistry",
            11,
            0,
            1,
        ),
        streamed_parity_observation(
            "k-res",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_RESOLVER,
            "resolver",
            12,
            0,
            2,
        ),
    ];
    let (summary, edges) = assert_streamed_reconciliation_parity(StreamedParityFixture {
        seed_observation_sets: vec![observations.clone()],
        observations,
    })
    .await?;

    assert_eq!(summary.inserted_edge_count, 0);
    assert_eq!(summary.deactivated_edge_count, 0);
    assert_eq!(summary.active_edge_count, 3);
    assert!(edges.iter().all(|edge| edge.10), "every edge stays active");
    Ok(())
}

#[tokio::test]
async fn streamed_reconciliation_parity_to_address_change() -> Result<()> {
    let (summary, edges) = assert_streamed_reconciliation_parity(StreamedParityFixture {
        seed_observation_sets: vec![vec![streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_A,
            "subregistry",
            10,
            0,
            1,
        )]],
        observations: vec![streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_C,
            "subregistry",
            12,
            0,
            1,
        )],
    })
    .await?;

    assert_eq!(summary.inserted_edge_count, 1);
    assert_eq!(summary.deactivated_edge_count, 1);
    assert_eq!(summary.active_edge_count, 1);
    let replaced = edges
        .iter()
        .find(|edge| edge.3.as_deref() == Some("0x00000000000000000000000000000000000000aa"))
        .expect("the replaced edge is retained as history");
    assert!(!replaced.10, "the replaced edge is deactivated");
    assert_eq!(replaced.8, Some(12), "terminal anchors at the new epoch");
    Ok(())
}

#[tokio::test]
async fn streamed_reconciliation_parity_zero_address_terminal() -> Result<()> {
    let (summary, edges) = assert_streamed_reconciliation_parity(StreamedParityFixture {
        seed_observation_sets: vec![vec![streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_A,
            "subregistry",
            10,
            0,
            1,
        )]],
        observations: vec![streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_ZERO,
            "subregistry",
            12,
            0,
            3,
        )],
    })
    .await?;

    assert_eq!(summary.inserted_edge_count, 0);
    assert_eq!(summary.deactivated_edge_count, 1);
    assert_eq!(summary.active_edge_count, 0);
    let tombstoned = &edges[0];
    assert!(!tombstoned.10);
    assert_eq!(
        tombstoned.8,
        Some(12),
        "the direct tombstone terminal anchors the closed window"
    );
    let provenance: serde_json::Value = serde_json::from_str(&tombstoned.11)?;
    assert_eq!(provenance["active_to_transaction_index"].as_i64(), Some(0));
    assert_eq!(provenance["active_to_log_index"].as_i64(), Some(3));
    Ok(())
}

#[tokio::test]
async fn streamed_reconciliation_parity_orphan_edge_without_observation() -> Result<()> {
    let (summary, edges) = assert_streamed_reconciliation_parity(StreamedParityFixture {
        seed_observation_sets: vec![vec![
            streamed_parity_observation(
                "k-eth",
                STREAMED_PARITY_REGISTRY,
                STREAMED_PARITY_OWNER_A,
                "subregistry",
                10,
                0,
                1,
            ),
            streamed_parity_observation(
                "k-com",
                STREAMED_PARITY_REGISTRY,
                STREAMED_PARITY_OWNER_B,
                "subregistry",
                11,
                0,
                2,
            ),
        ]],
        observations: vec![streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_A,
            "subregistry",
            10,
            0,
            1,
        )],
    })
    .await?;

    assert_eq!(summary.inserted_edge_count, 0);
    assert_eq!(summary.deactivated_edge_count, 1);
    assert_eq!(summary.active_edge_count, 1);
    let orphaned = edges
        .iter()
        .find(|edge| edge.3.as_deref() == Some("0x00000000000000000000000000000000000000bb"))
        .expect("the observation-less edge exists");
    assert!(!orphaned.10, "the observation-less edge is deactivated");
    assert_eq!(orphaned.8, None, "no observation means no terminal anchor");
    assert_eq!(orphaned.9, None);
    Ok(())
}

#[tokio::test]
async fn streamed_reconciliation_parity_cascade_chain_inherits_terminal() -> Result<()> {
    let (summary, edges) = assert_streamed_reconciliation_parity(StreamedParityFixture {
        seed_observation_sets: vec![vec![
            streamed_parity_observation(
                "k-eth",
                STREAMED_PARITY_REGISTRY,
                STREAMED_PARITY_OWNER_A,
                "subregistry",
                10,
                0,
                1,
            ),
            streamed_parity_observation(
                "k-sub",
                STREAMED_PARITY_OWNER_A,
                STREAMED_PARITY_OWNER_B,
                "subregistry",
                11,
                0,
                1,
            ),
        ]],
        observations: vec![
            streamed_parity_observation(
                "k-eth",
                STREAMED_PARITY_REGISTRY,
                STREAMED_PARITY_ZERO,
                "subregistry",
                20,
                0,
                5,
            ),
            streamed_parity_observation(
                "k-sub",
                STREAMED_PARITY_OWNER_A,
                STREAMED_PARITY_OWNER_B,
                "subregistry",
                11,
                0,
                1,
            ),
        ],
    })
    .await?;

    assert_eq!(summary.inserted_edge_count, 0);
    assert_eq!(summary.deactivated_edge_count, 2);
    assert_eq!(summary.active_edge_count, 0);
    for edge in &edges {
        assert!(!edge.10, "the whole chain is deactivated");
        assert_eq!(
            edge.8,
            Some(20),
            "the child inherits the removed parent's terminal state"
        );
    }
    Ok(())
}

#[tokio::test]
async fn streamed_reconciliation_parity_derived_contract_recursion() -> Result<()> {
    // The child observation's key sorts before the parent's, so the child
    // pages through the walk before its from-address becomes an active
    // contract: only the derived-contract revisit pass can admit it.
    let (summary, edges) = assert_streamed_reconciliation_parity(StreamedParityFixture {
        seed_observation_sets: Vec::new(),
        observations: vec![
            streamed_parity_observation(
                "k-0-child",
                STREAMED_PARITY_OWNER_A,
                STREAMED_PARITY_OWNER_B,
                "subregistry",
                11,
                0,
                2,
            ),
            streamed_parity_observation(
                "k-1-parent",
                STREAMED_PARITY_REGISTRY,
                STREAMED_PARITY_OWNER_A,
                "subregistry",
                10,
                0,
                1,
            ),
        ],
    })
    .await?;

    assert_eq!(summary.inserted_edge_count, 2);
    assert_eq!(summary.active_edge_count, 2);
    assert_eq!(edges.len(), 2);
    Ok(())
}

#[tokio::test]
async fn streamed_reconciliation_parity_ignores_never_admitted_addresses() -> Result<()> {
    let (summary, edges) = assert_streamed_reconciliation_parity(StreamedParityFixture {
        seed_observation_sets: Vec::new(),
        observations: vec![streamed_parity_observation(
            "k-stranger",
            STREAMED_PARITY_STRANGER,
            STREAMED_PARITY_OWNER_B,
            "subregistry",
            10,
            0,
            1,
        )],
    })
    .await?;

    assert_eq!(summary.admitted_edge_count, 0);
    assert_eq!(summary.inserted_edge_count, 0);
    assert_eq!(summary.deactivated_edge_count, 0);
    assert_eq!(summary.active_edge_count, 0);
    assert!(edges.is_empty());
    Ok(())
}

#[tokio::test]
async fn streamed_reconciliation_parity_retains_newer_successor_epoch() -> Result<()> {
    // The retained set already holds a newer epoch for the observation key:
    // the older desired assignment materializes as a closed historical
    // epoch and the newer edge stays active, in both variants.
    let (summary, edges) = assert_streamed_reconciliation_parity(StreamedParityFixture {
        seed_observation_sets: vec![vec![streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_A,
            "subregistry",
            20,
            0,
            1,
        )]],
        observations: vec![streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_B,
            "subregistry",
            10,
            0,
            1,
        )],
    })
    .await?;

    assert_eq!(summary.inserted_edge_count, 1);
    assert_eq!(summary.deactivated_edge_count, 0);
    assert_eq!(summary.active_edge_count, 1);
    let historical = edges
        .iter()
        .find(|edge| edge.3.as_deref() == Some("0x00000000000000000000000000000000000000bb"))
        .expect("the older epoch is materialized");
    assert!(!historical.10, "the older epoch closes at the successor");
    assert_eq!(historical.8, Some(20));
    let retained = edges
        .iter()
        .find(|edge| edge.3.as_deref() == Some("0x00000000000000000000000000000000000000aa"))
        .expect("the newer epoch is retained");
    assert!(retained.10, "the newer epoch stays active");
    Ok(())
}

#[tokio::test]
async fn streamed_reconcile_diffs_stored_edges_equal_to_their_specs() -> Result<()> {
    // Spec-equality proof: an edge inserted by `insert_reconciled_discovery_
    // edges` must diff as EQUAL to the spec that produced it, so re-running
    // the identical observation set through the streamed reconcile on the
    // same database is a byte-level no-op.
    let observations = vec![
        streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_A,
            "subregistry",
            10,
            0,
            1,
        ),
        streamed_parity_observation(
            "k-sub",
            STREAMED_PARITY_OWNER_A,
            STREAMED_PARITY_OWNER_B,
            "subregistry",
            11,
            0,
            1,
        ),
        streamed_parity_observation(
            "k-res",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_RESOLVER,
            "resolver",
            12,
            0,
            2,
        ),
    ];
    let fixture = StreamedParityFixture {
        seed_observation_sets: vec![observations.clone()],
        observations: observations.clone(),
    };
    let database = setup_streamed_parity_database(&fixture).await?;
    let seeded_edges =
        load_canonical_discovery_edges(database.pool(), STREAMED_PARITY_SOURCE).await?;
    assert_eq!(seeded_edges.len(), 3);

    let page_source = VecDiscoveryObservationPageSource::new(&observations, 2);
    let summary = reconcile_discovery_observations_streamed(
        database.pool(),
        STREAMED_PARITY_SOURCE,
        &page_source,
    )
    .await?;
    assert_eq!(
        summary.inserted_edge_count, 0,
        "stored edges must diff as equal to their producing specs"
    );
    assert_eq!(summary.deactivated_edge_count, 0);
    assert_eq!(summary.active_edge_count, 3);
    assert_eq!(summary.admission_epoch_bump_count, 0);
    assert_eq!(
        load_canonical_discovery_edges(database.pool(), STREAMED_PARITY_SOURCE).await?,
        seeded_edges,
        "a no-op streamed reconcile must leave every stored edge untouched"
    );

    database.cleanup().await
}

#[tokio::test]
async fn streamed_reconcile_guard_aborts_deactivation_floods_unless_overridden() -> Result<()> {
    let seed_observations = vec![
        streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_A,
            "subregistry",
            10,
            0,
            1,
        ),
        streamed_parity_observation(
            "k-com",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_B,
            "subregistry",
            11,
            0,
            1,
        ),
        streamed_parity_observation(
            "k-org",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_C,
            "subregistry",
            12,
            0,
            1,
        ),
    ];
    let fixture = StreamedParityFixture {
        seed_observation_sets: vec![seed_observations],
        observations: Vec::new(),
    };
    let database = setup_streamed_parity_database(&fixture).await?;

    // An empty retained set would deactivate every active edge: over the
    // guard bound the reconcile must abort before mutating anything.
    let empty_source = VecDiscoveryObservationPageSource::new(&[], 2);
    let guard_error = reconcile_discovery_observations_streamed_with_options(
        database.pool(),
        STREAMED_PARITY_SOURCE,
        &empty_source,
        StreamedDiscoveryReconciliationOptions {
            max_deactivations_override: Some(2),
            coarse_deactivation_cap_override: None,
            observation_page_limit: 2,
            mutation_batch_size: 2,
        },
    )
    .await
    .expect_err("a deactivation flood over the guard bound must abort");
    assert!(
        guard_error.to_string().contains("would deactivate 3"),
        "unexpected guard error: {guard_error:#}"
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(STREAMED_PARITY_SOURCE)
        .fetch_one(database.pool())
        .await?,
        3,
        "an aborted reconcile must roll back without mutating edges"
    );

    // Raising the override past the diff permits the same reconcile.
    let summary = reconcile_discovery_observations_streamed_with_options(
        database.pool(),
        STREAMED_PARITY_SOURCE,
        &empty_source,
        StreamedDiscoveryReconciliationOptions {
            max_deactivations_override: Some(3),
            coarse_deactivation_cap_override: None,
            observation_page_limit: 2,
            mutation_batch_size: 2,
        },
    )
    .await?;
    assert_eq!(summary.deactivated_edge_count, 3);
    assert_eq!(summary.active_edge_count, 0);

    database.cleanup().await
}

#[tokio::test]
async fn streamed_reconcile_guard_ignores_chronology_retained_candidates() -> Result<()> {
    // The retained set holds a newer epoch for the observation key: the
    // older desired assignment closes as history and the newer edge stays
    // active. That candidate is chronology-retained, not a deactivation, so
    // even a zero-deactivation precise guard must pass.
    let fixture = StreamedParityFixture {
        seed_observation_sets: vec![vec![streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_A,
            "subregistry",
            20,
            0,
            1,
        )]],
        observations: vec![streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_B,
            "subregistry",
            10,
            0,
            1,
        )],
    };
    let database = setup_streamed_parity_database(&fixture).await?;

    let page_source = VecDiscoveryObservationPageSource::new(&fixture.observations, 2);
    let summary = reconcile_discovery_observations_streamed_with_options(
        database.pool(),
        STREAMED_PARITY_SOURCE,
        &page_source,
        StreamedDiscoveryReconciliationOptions {
            max_deactivations_override: Some(0),
            coarse_deactivation_cap_override: None,
            observation_page_limit: 2,
            mutation_batch_size: 2,
        },
    )
    .await?;
    assert_eq!(summary.deactivated_edge_count, 0);
    assert_eq!(
        summary.inserted_edge_count, 1,
        "the older epoch still materializes as a closed historical edge"
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(STREAMED_PARITY_SOURCE)
        .fetch_one(database.pool())
        .await?,
        1,
        "the chronology-retained newer epoch stays active"
    );

    database.cleanup().await
}

#[tokio::test]
async fn streamed_reconcile_coarse_cap_aborts_before_loading_candidates() -> Result<()> {
    let seed_observations = vec![
        streamed_parity_observation(
            "k-eth",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_A,
            "subregistry",
            10,
            0,
            1,
        ),
        streamed_parity_observation(
            "k-com",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_B,
            "subregistry",
            11,
            0,
            1,
        ),
        streamed_parity_observation(
            "k-org",
            STREAMED_PARITY_REGISTRY,
            STREAMED_PARITY_OWNER_C,
            "subregistry",
            12,
            0,
            1,
        ),
    ];
    let fixture = StreamedParityFixture {
        seed_observation_sets: vec![seed_observations],
        observations: Vec::new(),
    };
    let database = setup_streamed_parity_database(&fixture).await?;

    let empty_source = VecDiscoveryObservationPageSource::new(&[], 2);
    let coarse_error = reconcile_discovery_observations_streamed_with_options(
        database.pool(),
        STREAMED_PARITY_SOURCE,
        &empty_source,
        StreamedDiscoveryReconciliationOptions {
            max_deactivations_override: None,
            coarse_deactivation_cap_override: Some(2),
            observation_page_limit: 2,
            mutation_batch_size: 2,
        },
    )
    .await
    .expect_err("a candidate flood over the coarse cap must abort before loading");
    assert!(
        coarse_error.to_string().contains("candidate load cap"),
        "unexpected coarse cap error: {coarse_error:#}"
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM discovery_edges WHERE discovery_source = $1 AND deactivated_at IS NULL"
        )
        .bind(STREAMED_PARITY_SOURCE)
        .fetch_one(database.pool())
        .await?,
        3,
        "an aborted reconcile must roll back without mutating edges"
    );

    database.cleanup().await
}
