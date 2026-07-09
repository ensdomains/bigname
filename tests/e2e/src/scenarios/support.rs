use anyhow::Result;

use crate::harness::{
    anvil::Anvil, db::HarnessDb, ens_v1::EnsV1Deployment, manifests, perturb, pipeline, repo_root,
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

/// Ingest the chain as it stands (live intake to the current head, then a
/// full projection replay) and serve the API. The manifest profile mirrors
/// every shipped mainnet ENSv1 family manifest version with addresses
/// re-pointed at the local deployment.
pub async fn ingest_and_serve(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    anvil.client().mine(2).await?;
    ingest_at_current_head(anvil, deployment, ready_sql).await
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
    let repo_root = repo_root();
    let rpc = anvil.client();
    let head = rpc.block_number().await?;

    let scratch = TempDir::create()?;
    let profile = manifests::generate_local_profile(
        scratch.path(),
        &repo_root,
        &deployment.manifest_targets(),
    )?;

    let db = HarnessDb::create().await?;
    pipeline::indexer_run_until_checkpoint(
        &repo_root,
        &db.url,
        &db.pool,
        &profile.root,
        &anvil.url,
        head,
        ready_sql,
    )
    .await?;
    pipeline::worker_replay_all_current_projections(&repo_root, &db.url).await?;
    let api = pipeline::ApiServer::start(&repo_root, &db.url).await?;
    Ok(PipelineRun {
        db,
        api,
        _scratch: scratch,
    })
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
    let api = pipeline::ApiServer::start(&repo_root, &db.url).await?;
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
    let repo_root = repo_root();
    let head = anvil.client().block_number().await?;

    let scratch = TempDir::create()?;
    let profile = manifests::generate_local_profile(
        scratch.path(),
        &repo_root,
        &deployment.manifest_targets(),
    )?;

    let db = HarnessDb::create().await?;
    pipeline::indexer_backfill(
        &repo_root,
        &db.url,
        &profile.root,
        &anvil.url,
        0,
        head,
        idempotency_key,
    )
    .await?;
    Ok(BackfillRun {
        db,
        _scratch: scratch,
    })
}

pub async fn serve_existing_db(db: HarnessDb, scratch: TempDir) -> Result<PipelineRun> {
    let api = pipeline::ApiServer::start(&repo_root(), &db.url).await?;
    Ok(PipelineRun {
        db,
        api,
        _scratch: scratch,
    })
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

impl TempDir {
    pub fn create() -> Result<Self> {
        let dir = std::env::temp_dir().join(format!(
            "bigname-e2e-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .subsec_nanos()
        ));
        std::fs::create_dir_all(&dir)?;
        Ok(Self(dir))
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
