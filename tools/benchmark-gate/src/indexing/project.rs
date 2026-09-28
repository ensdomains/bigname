//! Project benchmarks use the operator redo path so repeated runs receive fresh attempts.
use super::IndexingInput;
use anyhow::{Context, Result};
use phase_runner::{
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, SourceRole, TimingConfig},
    database::RunnerDatabase,
    phase::{BlockRange, LoopbackPhase, Phase, PhaseName, PhaseSet},
    project_phase::{FamilySettings, ProjectPhase},
    runner::{PhaseRunner, RedoPhase},
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) async fn replay(pool: &PgPool, input: &IndexingInput, from: i64) -> Result<usize> {
    let database = RunnerDatabase::connect_with_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .application_name("benchmark-project"),
        5,
    )
    .await?;
    let project = match &input.hydration_rpc_urls {
        Some(urls) => ProjectPhase::with_hydration(database.pool().clone(), urls.clone()),
        None => ProjectPhase::new(database.pool().clone()),
    }
    .with_family_settings(FamilySettings {
        retry_family_failures: false,
        ..FamilySettings::default()
    });
    let project: Arc<dyn Phase> = Arc::new(project);
    let phases = PhaseSet::new(PhaseName::ALL.map(|name| {
        if name == PhaseName::Project {
            project.clone()
        } else {
            Arc::new(LoopbackPhase::new(name)) as Arc<dyn Phase>
        }
    }))?;
    let runner = PhaseRunner::new(
        database,
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "benchmark-project",
        TimingConfig::default(),
    )?;
    let descriptors: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT source_key, source_kind, seed_basis, start_block_number FROM bigname_phase.ingest_cursors WHERE chain_id=$1 ORDER BY source_key")
        .bind(&input.chain_id).fetch_all(pool).await?;
    let endpoint = input
        .hydration_rpc_urls
        .as_ref()
        .and_then(|urls| urls.url_for(&input.chain_id))
        .unwrap_or("http://127.0.0.1:1");
    let sources = descriptors
        .into_iter()
        .map(|(key, kind, basis, start)| {
            SourceConfig::new_with_role(
                &input.chain_id,
                key,
                kind,
                SeedBasis::parse(&basis)?,
                start,
                SourceRole::Intake,
                endpoint,
            )
        })
        .collect::<phase_runner::error::RunnerResult<Vec<_>>>()?;
    // The runner validates the copied intake descriptors. Endpoints are not contacted:
    // only Project is dispatched; no intake or other phase is run by this measurement.
    runner
        .redo(
            &ChainConfig::new(&input.chain_id, sources, false)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(from, input.head_block)?,
            CancellationToken::new(),
        )
        .await
        .context("Project benchmark redo failed")?;
    // Redo, including a full rebuild, replays retained hydration observations without RPC.
    Ok(0)
}
