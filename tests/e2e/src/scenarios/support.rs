use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::Address;
use anyhow::Result;

use crate::harness::{
    anvil::Anvil,
    basenames::BasenamesDeployment,
    db::HarnessDb,
    ens_v1::{self, EnsV1Deployment},
    ens_v2::{self, EnsV2Deployment},
    ens_v2_migration::{self, EnsV2MigrationDeployment},
    manifests, perturb, pipeline, repo_root,
};

pub struct PipelineRun {
    pub db: HarnessDb,
    pub api: pipeline::ProjectionReader,
    pub manifests_root: std::path::PathBuf,
    _scratch: TempDir,
}

pub struct DerivedRun {
    pub db: HarnessDb,
    _scratch: TempDir,
}

pub struct ConnectedMigrationHarness {
    pub anvil: Anvil,
    pub ens_v1: EnsV1Deployment,
    pub ens_v2: EnsV2Deployment,
    pub migration: EnsV2MigrationDeployment,
}

pub struct UnlockedMigrationPath {
    pub target_owner: Address,
    pub expiry: u64,
    pub child_owner_after_clear: Address,
}

pub struct LockedMigrationPath {
    pub target_owner: Address,
    pub expiry: u64,
    pub wrapper_registry: Address,
    pub bridged: ens_v1::WrappedNameData,
    pub blocked: ens_v1::WrappedNameData,
    pub bridged_owner: Address,
    pub blocked_owner: Address,
}

pub async fn deploy_connected_migration_harness() -> Result<ConnectedMigrationHarness> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();
    let ens_v1 = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    let ens_v2 = ens_v2::deploy_ens_v2(&rpc, &root).await?;
    let migration =
        ens_v2_migration::deploy_ens_v2_migration(&rpc, &root, &ens_v1, &ens_v2).await?;
    Ok(ConnectedMigrationHarness {
        anvil,
        ens_v1,
        ens_v2,
        migration,
    })
}

pub async fn create_unlocked_migration_path(
    harness: &ConnectedMigrationHarness,
) -> Result<UnlockedMigrationPath> {
    const LABEL: &str = "unlock-migration";
    const CHILD: &str = "child.unlock-migration.eth";
    let rpc = harness.anvil.client();
    let accounts = rpc.accounts().await?;
    let target_owner = accounts[1];
    let child_owner = accounts[2];
    ens_v1::register_eth_name(
        &rpc,
        &harness.ens_v1,
        LABEL,
        target_owner,
        365 * 24 * 60 * 60,
        harness.ens_v1.public_resolver.address,
    )
    .await?;
    ens_v1::create_subname(
        &rpc,
        &harness.ens_v1,
        target_owner,
        "unlock-migration.eth",
        "child",
        child_owner,
    )
    .await?;
    let expiry = ens_v1::eth_name_expiry(&rpc, &harness.ens_v1, LABEL).await?;
    ens_v1::wrap_eth_2ld(
        &rpc,
        &harness.ens_v1,
        target_owner,
        LABEL,
        target_owner,
        0,
        harness.ens_v1.public_resolver.address,
    )
    .await?;
    let wrapped = ens_v1::wrapped_name_data(&rpc, &harness.ens_v1, "unlock-migration.eth").await?;
    assert_eq!(wrapped.fuses & ens_v1::CANNOT_UNWRAP, 0);
    ens_v2_migration::reserve_eth_label(&rpc, &harness.ens_v2, LABEL, expiry).await?;
    ens_v2_migration::migrate_unlocked_wrapped(
        &rpc,
        &harness.ens_v1,
        &harness.migration,
        target_owner,
        LABEL,
        target_owner,
    )
    .await?;
    ens_v2_migration::graveyard_clear(&rpc, &harness.migration, harness.ens_v2.deployer, &[CHILD])
        .await?;

    assert_eq!(
        ens_v2_migration::registry_owner(&rpc, harness.ens_v2.eth_registry.address, LABEL,).await?,
        target_owner
    );
    assert_eq!(
        ens_v2_migration::registry_subregistry(&rpc, harness.ens_v2.eth_registry.address, LABEL,)
            .await?,
        Address::ZERO
    );
    assert_eq!(
        ens_v2_migration::ens_v1_registry_owner(&rpc, &harness.ens_v1, "unlock-migration.eth",)
            .await?,
        harness.migration.graveyard.address
    );
    let child_owner_after_clear =
        ens_v2_migration::ens_v1_registry_owner(&rpc, &harness.ens_v1, CHILD).await?;
    assert_eq!(child_owner_after_clear, harness.migration.graveyard.address);
    assert_eq!(
        ens_v2_migration::ens_v1_registry_resolver(&rpc, &harness.ens_v1, CHILD).await?,
        Address::ZERO
    );
    Ok(UnlockedMigrationPath {
        target_owner,
        expiry,
        child_owner_after_clear,
    })
}

pub async fn create_locked_migration_path(
    harness: &ConnectedMigrationHarness,
) -> Result<LockedMigrationPath> {
    const LABEL: &str = "locked-migration";
    const PARENT: &str = "locked-migration.eth";
    let rpc = harness.anvil.client();
    let accounts = rpc.accounts().await?;
    let target_owner = accounts[3];
    let child_owner = accounts[4];
    ens_v1::register_eth_name(
        &rpc,
        &harness.ens_v1,
        LABEL,
        target_owner,
        365 * 24 * 60 * 60,
        harness.ens_v1.public_resolver.address,
    )
    .await?;
    let expiry = ens_v1::eth_name_expiry(&rpc, &harness.ens_v1, LABEL).await?;
    ens_v1::wrap_eth_2ld(
        &rpc,
        &harness.ens_v1,
        target_owner,
        LABEL,
        target_owner,
        0,
        harness.ens_v1.public_resolver.address,
    )
    .await?;
    ens_v1::set_wrapper_fuses(
        &rpc,
        &harness.ens_v1,
        target_owner,
        PARENT,
        ens_v1::CANNOT_UNWRAP as u16,
    )
    .await?;
    assert_ne!(
        ens_v1::wrapped_name_data(&rpc, &harness.ens_v1, PARENT)
            .await?
            .fuses
            & ens_v1::CANNOT_UNWRAP,
        0
    );
    for label in ["bridged", "blocked"] {
        ens_v1::set_wrapped_subnode_owner(
            &rpc,
            &harness.ens_v1,
            target_owner,
            ens_v1::WrappedSubnodeOwner {
                parent: PARENT,
                label,
                owner: child_owner,
                fuses: 0,
                expiry,
            },
        )
        .await?;
    }
    ens_v1::set_child_fuses(
        &rpc,
        &harness.ens_v1,
        target_owner,
        PARENT,
        "bridged",
        ens_v1::PARENT_CANNOT_CONTROL,
        expiry,
    )
    .await?;
    let bridged =
        ens_v1::wrapped_name_data(&rpc, &harness.ens_v1, "bridged.locked-migration.eth").await?;
    let blocked =
        ens_v1::wrapped_name_data(&rpc, &harness.ens_v1, "blocked.locked-migration.eth").await?;
    assert_ne!(bridged.fuses & ens_v1::PARENT_CANNOT_CONTROL, 0);
    assert_eq!(bridged.fuses & ens_v1::IS_DOT_ETH, 0);
    assert_eq!(blocked.fuses & ens_v1::PARENT_CANNOT_CONTROL, 0);
    assert_eq!(blocked.fuses & ens_v1::IS_DOT_ETH, 0);
    assert!(bridged.expiry > rpc.block_timestamp().await? as u64);
    assert!(blocked.expiry > rpc.block_timestamp().await? as u64);
    let bridged_owner = ens_v2_migration::ens_v1_registry_owner(
        &rpc,
        &harness.ens_v1,
        "bridged.locked-migration.eth",
    )
    .await?;
    let blocked_owner = ens_v2_migration::ens_v1_registry_owner(
        &rpc,
        &harness.ens_v1,
        "blocked.locked-migration.eth",
    )
    .await?;
    assert_ne!(bridged_owner, Address::ZERO);
    assert_ne!(blocked_owner, Address::ZERO);
    ens_v2_migration::reserve_eth_label(&rpc, &harness.ens_v2, LABEL, expiry).await?;
    let receipt = ens_v2_migration::migrate_locked_wrapped(
        &rpc,
        &harness.ens_v1,
        &harness.migration,
        target_owner,
        LABEL,
        target_owner,
    )
    .await?;
    let wrapper_registry = ens_v2_migration::proxy_deployed_address(&rpc, &receipt).await?;
    assert_eq!(
        ens_v2_migration::registry_owner(&rpc, harness.ens_v2.eth_registry.address, LABEL,).await?,
        target_owner
    );
    assert_eq!(
        ens_v2_migration::registry_subregistry(&rpc, harness.ens_v2.eth_registry.address, LABEL,)
            .await?,
        wrapper_registry
    );
    assert_eq!(
        ens_v2_migration::wrapper_registry_node(&rpc, wrapper_registry).await?,
        ens_v1::namehash(PARENT)
    );
    assert_eq!(
        ens_v2_migration::registry_expiry(&rpc, wrapper_registry, "bridged").await?,
        0
    );
    assert_eq!(
        ens_v2_migration::registry_expiry(&rpc, wrapper_registry, "blocked").await?,
        0
    );
    assert_ne!(
        ens_v2_migration::ens_v1_registry_owner(
            &rpc,
            &harness.ens_v1,
            "bridged.locked-migration.eth",
        )
        .await?,
        Address::ZERO
    );
    assert_ne!(
        ens_v2_migration::ens_v1_registry_owner(
            &rpc,
            &harness.ens_v1,
            "blocked.locked-migration.eth",
        )
        .await?,
        Address::ZERO
    );
    Ok(LockedMigrationPath {
        target_owner,
        expiry,
        wrapper_registry,
        bridged,
        blocked,
        bridged_owner,
        blocked_owner,
    })
}

#[derive(Clone, Copy)]
struct LocalChain<'a> {
    anvil: &'a Anvil,
    id: &'static str,
}

async fn ingest_local_chains<F>(
    chains: &[LocalChain<'_>],
    mine_margin: bool,
    ready_sql: Option<&str>,
    generate_profile: F,
) -> Result<PipelineRun>
where
    F: FnOnce(&std::path::Path, &std::path::Path) -> Result<manifests::LocalProfile>,
{
    if mine_margin {
        for chain in chains {
            chain.anvil.client().mine(2).await?;
        }
    }

    let mut targets = Vec::with_capacity(chains.len());
    for chain in chains {
        targets.push((chain.id, chain.anvil.client().block_number().await?));
    }

    let repo_root = repo_root();
    let scratch = TempDir::create()?;
    let profile = generate_profile(scratch.path(), &repo_root)?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = chains
        .iter()
        .map(|chain| (chain.id, chain.anvil.url.as_str()))
        .collect::<Vec<_>>();
    pipeline::run_fixture_spines_through_targets(
        &repo_root,
        &db.url,
        &db.pool,
        &profile.root,
        &chain_rpc_urls,
        &targets,
        ready_sql,
    )
    .await?;
    let api = pipeline::ProjectionReader::start(&repo_root, &db.url, &chain_rpc_urls).await?;
    Ok(PipelineRun {
        db,
        api,
        manifests_root: profile.root,
        _scratch: scratch,
    })
}

async fn replay_full_local_chain<F>(
    chain: LocalChain<'_>,
    generate_profile: F,
) -> Result<DerivedRun>
where
    F: FnOnce(&std::path::Path, &std::path::Path) -> Result<manifests::LocalProfile>,
{
    let repo_root = repo_root();
    let head = chain.anvil.client().block_number().await?;
    let scratch = TempDir::create()?;
    let profile = generate_profile(scratch.path(), &repo_root)?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = [(chain.id, chain.anvil.url.as_str())];
    pipeline::run_full_fixture_replay(
        &repo_root,
        &db.url,
        &profile.root,
        pipeline::FullFixtureReplayTarget {
            chain_rpc_urls: &chain_rpc_urls,
            chain: chain.id,
            block_range: 0..=head,
        },
    )
    .await?;
    Ok(DerivedRun {
        db,
        _scratch: scratch,
    })
}

#[derive(Clone, Copy)]
enum EnsV1RawFactPath {
    UpfrontFixture,
    RpcIngest,
}

async fn derive_ens_v1_on_rpc_alias(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    path: EnsV1RawFactPath,
) -> Result<DerivedRun> {
    const CHAIN: &str = "ethereum-e2e-rpc";

    let root = repo_root();
    let head = anvil.client().block_number().await?;
    let scratch = TempDir::create()?;
    let profile =
        manifests::generate_local_profile(scratch.path(), &root, &deployment.manifest_targets())?;
    profile.retarget_chain("ethereum-mainnet", CHAIN)?;
    let db = HarnessDb::create().await?;
    match path {
        EnsV1RawFactPath::UpfrontFixture => {
            let chain_rpc_urls = [(CHAIN, anvil.url.as_str())];
            pipeline::run_fixture_spines_through_targets(
                &root,
                &db.url,
                &db.pool,
                &profile.root,
                &chain_rpc_urls,
                &[(CHAIN, head)],
                None,
            )
            .await?;
        }
        EnsV1RawFactPath::RpcIngest => {
            pipeline::run_rpc_ingest_redo(
                &root,
                &db.url,
                &db.pool,
                &profile.root,
                CHAIN,
                &anvil.url,
                0,
                head,
            )
            .await?;
            pipeline::run_existing_raw_spine(
                &root,
                &db.url,
                &db.pool,
                &profile.root,
                CHAIN,
                &anvil.url,
                head,
            )
            .await?;
        }
    }
    Ok(DerivedRun {
        db,
        _scratch: scratch,
    })
}

pub async fn derive_ens_v1_from_upfront_facts(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
) -> Result<DerivedRun> {
    derive_ens_v1_on_rpc_alias(anvil, deployment, EnsV1RawFactPath::UpfrontFixture).await
}

pub async fn derive_ens_v1_from_rpc_ingest(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
) -> Result<DerivedRun> {
    derive_ens_v1_on_rpc_alias(anvil, deployment, EnsV1RawFactPath::RpcIngest).await
}

/// Readiness predicate for an exactly identified canonical normalized event.
/// Scenarios with additional constraints should keep spelling out those
/// constraints so this helper does not weaken their stop condition.
pub fn canonical_event_ready_sql(
    surface_id: &str,
    event_kind: &str,
    record_key: Option<&str>,
) -> String {
    fn quoted(value: &str) -> String {
        value.replace('\'', "''")
    }

    let logical_name_id = quoted(&schema_v2_logical_name_id(surface_id));
    let event_kind = quoted(event_kind);
    let record_key =
        record_key.map(|key| format!(" AND after_state->>'record_key' = '{}'", quoted(key)));
    format!(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = '{logical_name_id}' \
         AND event_kind = '{event_kind}'{} \
         AND canonicality_state = 'canonical')",
        record_key.as_deref().unwrap_or_default()
    )
}

/// Convert the scenario-friendly `namespace:name` notation to the stable
/// schema-v2 logical-name identity. Schema-v2 stores the readable name on the
/// name surface and keys cross-phase identity by the onchain namehash.
pub fn schema_v2_logical_name_id(surface_id: &str) -> String {
    let (namespace, name) = surface_id
        .split_once(':')
        .expect("scenario logical-name notation must contain a namespace");
    if name.starts_with("0x") && name.len() == 66 {
        return surface_id.to_owned();
    }
    format!("{namespace}:{:#x}", crate::harness::ens_v1::namehash(name))
}

/// Replay the chain as it stands through the full fixture-backed spine. The
/// generated deployment profile mirrors
/// every shipped mainnet ENSv1 family manifest version with addresses
/// re-pointed at the local deployment.
pub async fn ingest_and_serve(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    let chains = [LocalChain {
        anvil,
        id: "ethereum-mainnet",
    }];
    ingest_local_chains(&chains, true, ready_sql, |scratch, repo_root| {
        manifests::generate_local_profile(scratch, repo_root, &deployment.manifest_targets())
    })
    .await
}

/// Ingest without advancing the chain first. Control runs that must observe
/// the exact same head as a perturbed run (route snapshots embed
/// `chain_positions`) need this variant — `ingest_and_serve`'s margin mining
/// would move the head between runs of the same chain.
pub async fn ingest_at_current_head(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    let chains = [LocalChain {
        anvil,
        id: "ethereum-mainnet",
    }];
    ingest_local_chains(&chains, false, ready_sql, |scratch, repo_root| {
        manifests::generate_local_profile(scratch, repo_root, &deployment.manifest_targets())
    })
    .await
}

pub async fn ingest_basenames_and_serve(
    base_anvil: &Anvil,
    deployment: &BasenamesDeployment,
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    let chains = [LocalChain {
        anvil: base_anvil,
        id: "base-mainnet",
    }];
    ingest_local_chains(&chains, true, ready_sql, |scratch, repo_root| {
        manifests::generate_local_basenames_profile(
            scratch,
            repo_root,
            &deployment.manifest_targets(),
        )
    })
    .await
}

pub async fn ingest_basenames_at_current_head(
    base_anvil: &Anvil,
    deployment: &BasenamesDeployment,
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    let chains = [LocalChain {
        anvil: base_anvil,
        id: "base-mainnet",
    }];
    ingest_local_chains(&chains, false, ready_sql, |scratch, repo_root| {
        manifests::generate_local_basenames_profile(
            scratch,
            repo_root,
            &deployment.manifest_targets(),
        )
    })
    .await
}

pub async fn ingest_ens_v2_sepolia_and_serve(
    sepolia_anvil: &Anvil,
    deployment: &EnsV2Deployment,
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    let chains = [LocalChain {
        anvil: sepolia_anvil,
        id: "ethereum-sepolia",
    }];
    ingest_local_chains(&chains, true, ready_sql, |scratch, repo_root| {
        manifests::generate_local_sepolia_profile(
            scratch,
            repo_root,
            &deployment.manifest_targets(),
        )
    })
    .await
}

pub async fn ingest_ens_v1_v2_migration_sepolia_and_serve(
    sepolia_anvil: &Anvil,
    ens_v1: &EnsV1Deployment,
    ens_v2: &EnsV2Deployment,
    migration: &EnsV2MigrationDeployment,
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    let chains = [LocalChain {
        anvil: sepolia_anvil,
        id: "ethereum-sepolia",
    }];
    ingest_local_chains(&chains, true, ready_sql, |scratch, repo_root| {
        manifests::generate_local_sepolia_migration_profile(
            scratch,
            repo_root,
            &ens_v1.manifest_targets(),
            &ens_v2.manifest_targets(),
            &ens_v2_migration::migration_manifest_targets(migration),
            &ens_v2_migration::migration_correlation_addresses(ens_v1),
        )
    })
    .await
}

/// Replay both mainnet deployment-profile chains into one corpus with the full
/// composed deployment profile (ENSv1 + Basenames + the Ethereum-chain glue
/// families).
pub async fn ingest_mainnet_composed_and_serve(
    eth_anvil: &Anvil,
    ens_deployment: &EnsV1Deployment,
    base_anvil: &Anvil,
    basenames_deployment: &BasenamesDeployment,
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    let chains = [
        LocalChain {
            anvil: eth_anvil,
            id: "ethereum-mainnet",
        },
        LocalChain {
            anvil: base_anvil,
            id: "base-mainnet",
        },
    ];
    ingest_local_chains(&chains, true, ready_sql, |scratch, repo_root| {
        manifests::generate_local_mainnet_composed_profile(
            scratch,
            repo_root,
            &ens_deployment.manifest_targets(),
            &basenames_deployment.manifest_targets(),
        )
    })
    .await
}

pub async fn ingest_with_successive_replay_and_serve<F, Fut>(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    after_first_replay: F,
) -> Result<PipelineRun>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<pipeline::ReplayCompletion>>,
{
    let repo_root = repo_root();
    let rpc = anvil.client();
    rpc.mine(2).await?;

    let scratch = TempDir::create()?;
    let profile = manifests::generate_local_profile(
        scratch.path(),
        &repo_root,
        &deployment.manifest_targets(),
    )?;

    let db = HarnessDb::create().await?;
    pipeline::run_fixture_spine_with_midpoint(
        &repo_root,
        &db.url,
        &db.pool,
        &profile.root,
        &anvil.url,
        after_first_replay,
    )
    .await?;
    let chain_rpc_urls = [("ethereum-mainnet", anvil.url.as_str())];
    let api = pipeline::ProjectionReader::start(&repo_root, &db.url, &chain_rpc_urls).await?;
    Ok(PipelineRun {
        db,
        api,
        manifests_root: profile.root,
        _scratch: scratch,
    })
}

pub async fn serve_existing_db(
    db: HarnessDb,
    scratch: TempDir,
    anvil: &Anvil,
) -> Result<PipelineRun> {
    let chain_rpc_urls = [("ethereum-mainnet", anvil.url.as_str())];
    let api = pipeline::ProjectionReader::start(&repo_root(), &db.url, &chain_rpc_urls).await?;
    Ok(PipelineRun {
        db,
        api,
        manifests_root: scratch.path().join("manifests-e2e"),
        _scratch: scratch,
    })
}

/// Materialize the fixture facts and execute interpret/project without
/// constructing a projection reader.
pub async fn replay_full_corpus_projections(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
) -> Result<DerivedRun> {
    replay_full_local_chain(
        LocalChain {
            anvil,
            id: "ethereum-mainnet",
        },
        |scratch, repo_root| {
            manifests::generate_local_profile(scratch, repo_root, &deployment.manifest_targets())
        },
    )
    .await
}

pub async fn route_snapshots(
    run: &PipelineRun,
    subjects: &perturb::RouteSnapshotSubjects,
) -> Result<perturb::RouteSnapshots> {
    perturb::route_snapshots(&run.api, subjects).await
}

/// Scratch dir that lives as long as the generated deployment profile.
pub struct TempDir(std::path::PathBuf);

static NEXT_TEMP_DIR_ID: AtomicU64 = AtomicU64::new(0);

impl TempDir {
    pub fn create() -> Result<Self> {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        loop {
            let id = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "bigname-e2e-{}-{created_at}-{id}",
                std::process::id()
            ));
            match std::fs::create_dir(&dir) {
                Ok(()) => return Ok(Self(dir)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{TempDir, canonical_event_ready_sql, schema_v2_logical_name_id};

    #[test]
    fn temp_dirs_created_concurrently_are_distinct() {
        let handles = (0..32)
            .map(|_| std::thread::spawn(TempDir::create))
            .collect::<Vec<_>>();
        let dirs = handles
            .into_iter()
            .map(|handle| handle.join().expect("temp-dir thread panicked").unwrap())
            .collect::<Vec<_>>();
        let paths = dirs.iter().map(TempDir::path).collect::<BTreeSet<_>>();

        assert_eq!(paths.len(), dirs.len());
        assert!(paths.iter().all(|path| path.is_dir()));
    }

    #[test]
    fn canonical_event_readiness_adds_only_requested_record_key() {
        assert_eq!(
            canonical_event_ready_sql("ens:o'hare.eth", "RecordChanged", Some("text:it's")),
            "SELECT EXISTS (SELECT 1 FROM normalized_events WHERE logical_name_id = \
             'ens:0x28d7ef2fa333511772cc70752f8d9122e2150117f42fdd2bb6577eb89dc1d263' \
             AND event_kind = 'RecordChanged' AND \
             after_state->>'record_key' = 'text:it''s' AND canonicality_state = 'canonical')"
        );
        assert!(
            !canonical_event_ready_sql(
                "ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
                "RegistrationGranted",
                None
            )
            .contains("record_key")
        );
        assert_eq!(
            schema_v2_logical_name_id(
                "ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec"
            ),
            "ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec"
        );
    }
}
