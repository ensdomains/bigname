mod backfill;
mod canonicality;
mod data_completeness;
mod execution_trace;
mod formatting;
mod manifest_drift;
mod stored_lineage;
mod watch_plan;

#[cfg(test)]
mod tests;

use anyhow::{Context, Result};
use bigname_storage::DatabaseConfig;
use clap::{Args, Subcommand, ValueEnum};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{path::PathBuf, str::FromStr};
use uuid::Uuid;

pub(crate) use manifest_drift::render_manifest_drift_alert_observations;

#[derive(Args, Debug)]
pub(crate) struct InspectArgs {
    #[command(subcommand)]
    pub(crate) command: InspectCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum InspectCommand {
    #[command(about = "Inspect one persisted backfill job and its child ranges")]
    BackfillJob(InspectBackfillJobArgs),
    #[command(
        about = "Inspect canonicality, durable raw fact counts, and retained payload-cache metadata for one block hash"
    )]
    Canonicality(InspectCanonicalityArgs),
    #[command(
        about = "Check whether this database is data-complete enough to serve: reconciliation frontier, watch-set code-observation coverage, replay and projection cursors, and projection content"
    )]
    DataCompleteness(InspectDataCompletenessArgs),
    #[command(about = "Inspect one persisted execution trace and its ordered steps")]
    ExecutionTrace(InspectExecutionTraceArgs),
    #[command(about = "Inspect stored manifest drift and proxy implementation alert observations")]
    ManifestDrift(InspectManifestDriftArgs),
    #[command(about = "List stored lineage rows for a bounded chain block range")]
    StoredLineageRange(InspectStoredLineageRangeArgs),
    #[command(about = "Inspect the read-only runtime watch plan derived from active manifests")]
    WatchPlan(InspectWatchPlanArgs),
}

#[derive(Args, Debug)]
pub(crate) struct InspectBackfillJobArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) backfill_job_id: i64,
}

#[derive(Args, Debug)]
pub(crate) struct InspectCanonicalityArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) chain_id: String,
    #[arg(long)]
    pub(crate) block_hash: String,
}

#[derive(Args, Debug)]
pub(crate) struct InspectDataCompletenessArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) json: bool,
    #[arg(long)]
    pub(crate) fail_on_incomplete: bool,
    #[arg(long)]
    pub(crate) max_head_lag_blocks: Option<i64>,
    /// Optional on-disk manifest profile root used as the external active-corpus authority.
    #[arg(long)]
    pub(crate) manifests_root: Option<PathBuf>,
    /// Raw-log retention contract to verify.
    #[arg(long, value_enum, default_value_t = RetentionMode::Minimal)]
    pub(crate) retention_mode: RetentionMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum RetentionMode {
    Minimal,
    LogAudit,
}

impl RetentionMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Minimal => "minimal",
            Self::LogAudit => "log-audit",
        }
    }
}

#[derive(Args, Debug)]
pub(crate) struct InspectExecutionTraceArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) execution_trace_id: Uuid,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct InspectManifestDriftArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct InspectStoredLineageRangeArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) chain_id: String,
    #[arg(long)]
    pub(crate) range_start_block_number: i64,
    #[arg(long)]
    pub(crate) range_end_block_number: i64,
}

#[derive(Args, Debug)]
pub(crate) struct InspectWatchPlanArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long)]
    pub(crate) json: bool,
}

pub(crate) async fn inspect_command(args: InspectArgs) -> Result<()> {
    match args.command {
        InspectCommand::BackfillJob(args) => backfill::inspect_backfill_job(args).await,
        InspectCommand::Canonicality(args) => canonicality::inspect_canonicality(args).await,
        InspectCommand::DataCompleteness(args) => {
            data_completeness::inspect_data_completeness(args).await
        }
        InspectCommand::ExecutionTrace(args) => {
            execution_trace::inspect_execution_trace(args).await
        }
        InspectCommand::ManifestDrift(args) => manifest_drift::inspect_manifest_drift(args).await,
        InspectCommand::StoredLineageRange(args) => {
            stored_lineage::inspect_stored_lineage_range(args).await
        }
        InspectCommand::WatchPlan(args) => watch_plan::inspect_watch_plan(args).await,
    }
}

pub(crate) async fn connect_read_only(config: &DatabaseConfig) -> Result<sqlx::PgPool> {
    let database_url = config
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| bigname_storage::default_database_url().to_owned());
    let options = PgConnectOptions::from_str(&database_url)
        .context("failed to parse PostgreSQL URL for read-only inspect connection")?
        .options([("default_transaction_read_only", "on")]);

    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect_with(options)
        .await
        .context("failed to connect to PostgreSQL for read-only inspect")
}
