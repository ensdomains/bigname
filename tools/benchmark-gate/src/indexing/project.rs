//! Project benchmarks use the operator redo path so repeated runs receive fresh attempts.
use super::IndexingInput;
use anyhow::{Context, Result};
use phase_runner::{
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, TimingConfig},
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
    // Only Project is dispatched; no intake or other phase is run by this measurement.
    runner
        .redo(
            &ChainConfig::new(&input.chain_id, vec![], false)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(from, input.head_block)?,
            CancellationToken::new(),
        )
        .await
        .context("Project benchmark redo failed")?;
    // Redo, including a full rebuild, replays retained hydration observations without RPC.
    Ok(0)
}
