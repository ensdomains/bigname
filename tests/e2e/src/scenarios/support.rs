use anyhow::Result;

use crate::harness::{
    anvil::Anvil, db::HarnessDb, ens_v1::EnsV1Deployment, manifests, pipeline, repo_root,
};

pub struct PipelineRun {
    pub db: HarnessDb,
    pub api: pipeline::ApiServer,
    _scratch: TempDir,
}

/// Ingest the chain as it stands (live intake to the current head, then a
/// full projection replay) and serve the API. `activate_families` opts
/// specific source families out of the shipped rollout status — see
/// `manifests::generate_local_profile_with_activation`.
pub async fn ingest_and_serve(
    anvil: &Anvil,
    deployment: &EnsV1Deployment,
    activate_families: &[&str],
    ready_sql: Option<&str>,
) -> Result<PipelineRun> {
    let repo_root = repo_root();
    let rpc = anvil.client();
    rpc.mine(2).await?;
    let head = rpc.block_number().await?;

    let scratch = TempDir::create()?;
    let profile = manifests::generate_local_profile_with_activation(
        scratch.path(),
        &repo_root,
        &deployment.manifest_targets(),
        activate_families,
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
