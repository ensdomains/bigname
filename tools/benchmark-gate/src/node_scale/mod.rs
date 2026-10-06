//! Local feature validation is distinct from the production benchmark profile.

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use clap::{Args, Subcommand};
use uuid::Uuid;

mod events;
mod manifests;
mod oracle;
mod phases;
mod prepare;
mod raw;
mod recipe;
mod recipe_validation;
mod samples;
mod seed;

#[derive(Debug, Args)]
pub(crate) struct NodeScaleArgs {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Seal a deterministic raw corpus and retargeted production manifests.
    Prepare {
        #[arg(long)]
        names: u32,
        #[arg(long)]
        directory: PathBuf,
    },
    /// Load a sealed corpus into an empty, marked disposable database.
    Seed {
        #[command(flatten)]
        database: DatabaseArgs,
        #[arg(long)]
        directory: PathBuf,
        #[arg(long)]
        head: i64,
        /// Append a later raw epoch after the previous epoch is published.
        #[arg(long)]
        append: bool,
    },
    /// Measure initial Interpret over retained raw fixture events.
    Interpret {
        #[command(flatten)]
        database: DatabaseArgs,
        #[arg(long)]
        directory: PathBuf,
        #[arg(long)]
        head: i64,
    },
    /// Measure initial Project over actually interpreted fixture events.
    Project {
        #[command(flatten)]
        database: DatabaseArgs,
        #[arg(long)]
        directory: PathBuf,
        #[arg(long)]
        head: i64,
    },
}

#[derive(Debug, Args)]
struct DatabaseArgs {
    #[arg(long, env = "BIGNAME_BENCHMARK_DATABASE_URL")]
    database_url: String,
    #[arg(long)]
    expected_database_name: String,
    #[arg(long)]
    disposable_marker: Uuid,
    #[arg(long, required = true)]
    allow_disposable_copy_writes: bool,
    /// Per-statement bound for a local fidelity run; the outer controller also bounds each phase.
    #[arg(long, default_value_t = 120, value_parser = clap::value_parser!(u64).range(1..=300))]
    statement_timeout_seconds: u64,
}

impl DatabaseArgs {
    async fn connect(&self) -> Result<sqlx::PgPool> {
        crate::database::connect_disposable_copy(
            &self.database_url,
            8,
            Duration::from_secs(self.statement_timeout_seconds),
            &self.expected_database_name,
            self.disposable_marker,
        )
        .await
    }
}

pub(crate) async fn run(args: NodeScaleArgs, report: Option<&std::path::Path>) -> Result<()> {
    let head = crate::begin_release_run()?;
    crate::require_release_profile()?;
    let build = crate::wrapper_build_attestation()?;
    let started = std::time::Instant::now();
    let result = match args.command {
        Command::Prepare { names, directory } => {
            serde_json::to_value(prepare::run(names, &directory)?)?
        }
        Command::Seed {
            database,
            directory,
            head,
            append,
        } => {
            let pool = database.connect().await?;
            let result = seed::run(&pool, &directory, head, append).await;
            pool.close().await;
            serde_json::to_value(result?)?
        }
        Command::Interpret {
            database,
            directory,
            head,
        } => {
            run_phase(
                database,
                phase_runner::phase::PhaseName::Interpret,
                &directory,
                head,
            )
            .await?
        }
        Command::Project {
            database,
            directory,
            head,
        } => {
            run_phase(
                database,
                phase_runner::phase::PhaseName::Project,
                &directory,
                head,
            )
            .await?
        }
    };
    crate::finish_release_run(&head)?;
    crate::emit_report(
        &serde_json::json!({
            "source_head": head, "source_tree_clean": true,
            "interpreter_content_hash": bigname_content_hash::INTERPRETER_CONTENT_HASH,
            "cargo_profile": crate::compiler_attestation::cargo_profile(),
            "benchmark_binary_sha256": build.benchmark_binary_sha256,
            "api_binary_sha256": build.locally_built_api_binary_sha256,
            "rustc_version": build.rustc_version, "rustflags": build.rustflags,
            "cargo_encoded_rustflags": build.cargo_encoded_rustflags,
            "elapsed_seconds": started.elapsed().as_secs_f64(),
            "feature_gate_complete": false, "results": result,
        }),
        report,
    )
}

async fn run_phase(
    database: DatabaseArgs,
    phase: phase_runner::phase::PhaseName,
    directory: &std::path::Path,
    head: i64,
) -> Result<serde_json::Value> {
    let pool = database.connect().await?;
    let result = phases::run(&pool, phase, directory, head).await;
    pool.close().await;
    Ok(serde_json::to_value(result?)?)
}
