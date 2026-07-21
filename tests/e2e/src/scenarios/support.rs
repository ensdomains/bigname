use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::Address;
use anyhow::Result;
use sqlx::types::Uuid;

use crate::harness::{
    anvil::Anvil, basenames::BasenamesDeployment, db::HarnessDb, ens_v1::EnsV1Deployment,
    ens_v2::EnsV2Deployment, manifests, perturb, pipeline, repo_root,
};

pub struct PipelineRun {
    pub db: HarnessDb,
    pub api: pipeline::ApiServer,
    _scratch: TempDir,
}

pub struct BackfillRun {
    pub db: HarnessDb,
    _scratch: TempDir,
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

    let mut checkpoints = Vec::with_capacity(chains.len());
    for chain in chains {
        checkpoints.push((chain.id, chain.anvil.client().block_number().await?));
    }

    let repo_root = repo_root();
    let scratch = TempDir::create()?;
    let profile = generate_profile(scratch.path(), &repo_root)?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = chains
        .iter()
        .map(|chain| (chain.id, chain.anvil.url.as_str()))
        .collect::<Vec<_>>();
    pipeline::indexer_run_until_chain_checkpoints(
        &repo_root,
        &db.url,
        &db.pool,
        &profile.root,
        &chain_rpc_urls,
        &checkpoints,
        ready_sql,
    )
    .await?;
    pipeline::worker_replay_all_current_projections(&repo_root, &db.url).await?;
    let api = pipeline::ApiServer::start(&repo_root, &db.url, &chain_rpc_urls).await?;
    Ok(PipelineRun {
        db,
        api,
        _scratch: scratch,
    })
}

async fn backfill_local_chain<F>(
    chain: LocalChain<'_>,
    idempotency_key: &str,
    replay_projections: bool,
    generate_profile: F,
) -> Result<BackfillRun>
where
    F: FnOnce(&std::path::Path, &std::path::Path) -> Result<manifests::LocalProfile>,
{
    let repo_root = repo_root();
    let head = chain.anvil.client().block_number().await?;
    let scratch = TempDir::create()?;
    let profile = generate_profile(scratch.path(), &repo_root)?;
    let db = HarnessDb::create().await?;
    let chain_rpc_urls = [(chain.id, chain.anvil.url.as_str())];
    pipeline::indexer_backfill_with_chain_rpc_urls(
        &repo_root,
        &db.url,
        &profile.root,
        pipeline::ChainBackfillTarget {
            chain_rpc_urls: &chain_rpc_urls,
            chain: chain.id,
            block_range: 0..=head,
            idempotency_key,
        },
    )
    .await?;
    if replay_projections {
        pipeline::worker_replay_all_current_projections(&repo_root, &db.url).await?;
    }
    Ok(BackfillRun {
        db,
        _scratch: scratch,
    })
}

/// SQL scalar expression that compares the latest non-orphaned code-hash
/// observations used by resolver-profile admission for two addresses.
pub fn resolver_code_hash_comparison_sql(
    candidate: Address,
    profile_seed: Address,
    expect_match: bool,
) -> String {
    let comparison = if expect_match { "=" } else { "<>" };
    format!(
        "COALESCE((SELECT candidate.code_hash {comparison} seed.code_hash \
         FROM LATERAL ( \
             SELECT code_hash FROM raw_code_hashes \
             WHERE chain_id = 'ethereum-mainnet' \
               AND lower(contract_address) = '{candidate:#x}' \
               AND canonicality_state <> 'orphaned' \
             ORDER BY block_number DESC, \
               CASE canonicality_state \
                 WHEN 'finalized' THEN 4 WHEN 'safe' THEN 3 \
                 WHEN 'canonical' THEN 2 WHEN 'observed' THEN 1 ELSE 0 \
               END DESC, raw_code_hash_id DESC \
             LIMIT 1 \
         ) candidate \
         CROSS JOIN LATERAL ( \
             SELECT code_hash FROM raw_code_hashes \
             WHERE chain_id = 'ethereum-mainnet' \
               AND lower(contract_address) = '{profile_seed:#x}' \
               AND canonicality_state <> 'orphaned' \
             ORDER BY block_number DESC, \
               CASE canonicality_state \
                 WHEN 'finalized' THEN 4 WHEN 'safe' THEN 3 \
                 WHEN 'canonical' THEN 2 WHEN 'observed' THEN 1 ELSE 0 \
               END DESC, raw_code_hash_id DESC \
             LIMIT 1 \
         ) seed), FALSE)"
    )
}

/// Readiness predicate for an exactly identified canonical normalized event.
/// Scenarios with additional constraints should keep spelling out those
/// constraints so this helper does not weaken their stop condition.
pub fn canonical_event_ready_sql(
    logical_name_id: &str,
    event_kind: &str,
    record_key: Option<&str>,
) -> String {
    fn quoted(value: &str) -> String {
        value.replace('\'', "''")
    }

    let logical_name_id = quoted(logical_name_id);
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

/// Ingest the chain as it stands (live intake to the current head, then a
/// full projection replay) and serve the API. The manifest profile mirrors
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

pub async fn ingest_and_serve_with_ens_execution(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    universal_resolver: &crate::harness::artifacts::Deployed,
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    // Keep the normal post-event margin so the selected head is deliberately
    // newer than name_current's last-event block. Verified execution and
    // explain readback must still share the selected-snapshot cache identity.
    let chains = [LocalChain {
        anvil,
        id: "ethereum-mainnet",
    }];
    ingest_local_chains(&chains, true, ready_sql, |scratch, repo_root| {
        let mut targets = deployment.manifest_targets();
        targets.insert(
            "universal_resolver",
            (universal_resolver.address, universal_resolver.block_number),
        );
        manifests::generate_local_profile(scratch, repo_root, &targets)
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

/// Ingest BOTH mainnet-profile chains into one corpus: the ENSv1 ethereum
/// anvil and the Basenames base anvil run under one live session with the
/// full composed profile (ENSv1 + Basenames + the ethereum-chain glue
/// families), waiting each chain's canonical checkpoint before serving.
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

/// Base twin of the backfill helpers: derive the Basenames chain via
/// backfill and rebuild projections without a live run (no API — backfill
/// promotes no canonical checkpoint).
pub async fn backfill_basenames_and_replay_projections(
    base_anvil: &Anvil,
    deployment: &BasenamesDeployment,
    idempotency_key: &str,
) -> Result<BackfillRun> {
    backfill_local_chain(
        LocalChain {
            anvil: base_anvil,
            id: "base-mainnet",
        },
        idempotency_key,
        true,
        |scratch, repo_root| {
            manifests::generate_local_basenames_profile(
                scratch,
                repo_root,
                &deployment.manifest_targets(),
            )
        },
    )
    .await
}

/// Backfill one discovery-admitted Basenames target into an existing corpus.
/// This is the second phase for scenarios that must distinguish an adapter
/// admission rejection from an address-filter transport exclusion.
pub async fn backfill_basenames_watched_target_and_replay_projections(
    run: &BackfillRun,
    base_anvil: &Anvil,
    contract_instance_id: Uuid,
    block_range: std::ops::RangeInclusive<u64>,
    idempotency_key: &str,
) -> Result<()> {
    let repo_root = repo_root();
    let chain_rpc_urls = [("base-mainnet", base_anvil.url.as_str())];
    let profile_root = run._scratch.path().join("manifests-e2e");
    pipeline::indexer_backfill_watched_target_with_chain_rpc_urls(
        &repo_root,
        &run.db.url,
        &profile_root,
        pipeline::ChainBackfillTarget {
            chain_rpc_urls: &chain_rpc_urls,
            chain: "base-mainnet",
            block_range,
            idempotency_key,
        },
        contract_instance_id,
    )
    .await?;
    pipeline::worker_replay_all_current_projections(&repo_root, &run.db.url).await?;
    Ok(())
}

pub async fn ingest_with_restart_and_serve<F, Fut>(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    after_first_checkpoint: F,
) -> Result<PipelineRun>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<pipeline::RestartCompletion>>,
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
    pipeline::indexer_run_restart_after_first_checkpoint(
        &repo_root,
        &db.url,
        &db.pool,
        &profile.root,
        &anvil.url,
        after_first_checkpoint,
    )
    .await?;
    pipeline::worker_replay_all_current_projections(&repo_root, &db.url).await?;
    let chain_rpc_urls = [("ethereum-mainnet", anvil.url.as_str())];
    let api = pipeline::ApiServer::start(&repo_root, &db.url, &chain_rpc_urls).await?;
    Ok(PipelineRun {
        db,
        api,
        _scratch: scratch,
    })
}

pub async fn backfill_normalized_events(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    idempotency_key: &str,
) -> Result<BackfillRun> {
    backfill_local_chain(
        LocalChain {
            anvil,
            id: "ethereum-mainnet",
        },
        idempotency_key,
        false,
        |scratch, repo_root| {
            manifests::generate_local_profile(scratch, repo_root, &deployment.manifest_targets())
        },
    )
    .await
}

pub async fn serve_existing_db(
    db: HarnessDb,
    scratch: TempDir,
    anvil: &Anvil,
) -> Result<PipelineRun> {
    let chain_rpc_urls = [("ethereum-mainnet", anvil.url.as_str())];
    let api = pipeline::ApiServer::start(&repo_root(), &db.url, &chain_rpc_urls).await?;
    Ok(PipelineRun {
        db,
        api,
        _scratch: scratch,
    })
}

/// Backfill-derive the chain and rebuild projections without a live run.
/// Used where live re-ingest of a chain wedges the run loop (see the
/// preimage-reveal review point); API serving is impossible on this path
/// because backfill does not promote canonical checkpoints.
pub async fn backfill_and_replay_projections(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    idempotency_key: &str,
) -> Result<BackfillRun> {
    backfill_local_chain(
        LocalChain {
            anvil,
            id: "ethereum-mainnet",
        },
        idempotency_key,
        true,
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

/// Scratch dir that lives as long as the pipeline run (the indexer reads the
/// generated manifest profile from it).
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

    use super::{TempDir, canonical_event_ready_sql};

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
             'ens:o''hare.eth' AND event_kind = 'RecordChanged' AND \
             after_state->>'record_key' = 'text:it''s' AND canonicality_state = 'canonical')"
        );
        assert!(
            !canonical_event_ready_sql("ens:alice.eth", "RegistrationGranted", None)
                .contains("record_key")
        );
    }
}
