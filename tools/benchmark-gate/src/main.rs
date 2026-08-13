use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use uuid::Uuid;

mod api_load;
mod budgets;
mod database;
mod indexing;
mod smoke;

use budgets::{BudgetProfile, BudgetsFile};

const BUDGETS_PATH: &str = "benchmarks/release-gate.toml";

#[derive(Debug, Parser)]
#[command(name = "bigname-benchmark-gate")]
#[command(about = "Run the on-demand production-scale indexing and API release gate")]
struct Cli {
    #[arg(long)]
    report: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Mutate a disposable production-shaped copy to benchmark Interpret and Project.
    Index(IndexArgs),
    /// Load a drained API while keeping the corpus database connection read-only.
    Api(ApiArgs),
    /// Prove both halves against an isolated database created by scripts/test-db.
    Smoke(SmokeArgs),
    /// Parse and validate the checked-in budgets without running a benchmark.
    CheckBudgets,
}

#[derive(Debug, Args)]
struct IndexArgs {
    #[arg(long, env = "BIGNAME_BENCHMARK_DATABASE_URL")]
    database_url: String,
    #[arg(long)]
    chain: String,
    #[arg(long)]
    head_block: i64,
    #[arg(long)]
    walk_from_block: i64,
    #[arg(long)]
    walk_to_block: i64,
    #[arg(
        long,
        help = "Exact current_database() value expected after connecting to the disposable copy"
    )]
    expected_database_name: String,
    #[arg(
        long,
        help = "UUID stored by the documented disposable-copy preparation step"
    )]
    disposable_marker: Uuid,
    #[arg(
        long,
        help = "JSON-RPC URL used by the production Project canonical-head hydration step"
    )]
    chain_rpc_url: String,
    #[arg(
        long,
        required = true,
        help = "Required acknowledgement that this database is a disposable copy and may be rewritten"
    )]
    allow_disposable_copy_writes: bool,
}

#[derive(Debug, Args)]
struct ApiArgs {
    #[arg(long, env = "BIGNAME_BENCHMARK_DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "BIGNAME_BENCHMARK_API_BASE_URL")]
    api_base_url: String,
}

#[derive(Debug, Args)]
struct SmokeArgs {
    #[arg(
        long,
        env = "BIGNAME_BENCHMARK_API_BINARY",
        default_value = "target/release/bigname-api"
    )]
    api_binary: PathBuf,
}

#[derive(Debug, Serialize)]
struct GateReport<T: Serialize> {
    head_sha: String,
    source_tree_clean: bool,
    cargo_profile: String,
    interpreter_content_hash: &'static str,
    budgets: &'static str,
    budget_profile: &'static str,
    database: String,
    database_host: String,
    api_base_url: Option<String>,
    green: bool,
    results: T,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let budgets = BudgetsFile::load(Path::new(BUDGETS_PATH))?;
    match cli.command {
        Command::CheckBudgets => {
            println!("benchmark budgets are valid: {BUDGETS_PATH}");
            Ok(())
        }
        Command::Index(args) => {
            let head_sha = begin_release_run()?;
            require_release_profile()?;
            let profile = budgets.profile(BudgetProfile::Production);
            let timeout =
                Duration::from_secs(profile.project_rebuild_max_seconds.saturating_add(60));
            let pool = database::connect_disposable_copy(&args.database_url, 8, timeout).await?;
            let database_name = database::database_identity(&pool).await?;
            let database_host = database::database_host(&pool).await?;
            database::require_database_identity(&database_name, &args.expected_database_name)?;
            database::require_disposable_marker(&pool, args.disposable_marker).await?;
            let rpc_urls = bigname_lookup::ChainRpcUrls::from_entries(&[format!(
                "{}={}",
                args.chain, args.chain_rpc_url
            )])?;
            let results = indexing::run(
                &pool,
                &indexing::IndexingInput {
                    chain_id: args.chain,
                    head_block: args.head_block,
                    walk_from_block: args.walk_from_block,
                    walk_to_block: args.walk_to_block,
                    hydration_rpc_urls: Some(rpc_urls),
                },
                profile,
            )
            .await?;
            pool.close().await;
            finish_release_run(&head_sha)?;
            let report = GateReport {
                head_sha,
                source_tree_clean: true,
                cargo_profile: cargo_profile(),
                interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH,
                budgets: BUDGETS_PATH,
                budget_profile: "production",
                database: database_name,
                database_host,
                api_base_url: None,
                green: results.green,
                results,
            };
            emit_report(&report, cli.report.as_deref())?;
            if !report.green {
                bail!("indexing benchmark gate is red");
            }
            Ok(())
        }
        Command::Api(args) => {
            let head_sha = begin_release_run()?;
            require_release_profile()?;
            let profile = budgets.profile(BudgetProfile::Production);
            let pool = database::connect_read_only(&args.database_url, 8).await?;
            let database_name = database::database_identity(&pool).await?;
            let database_host = database::database_host(&pool).await?;
            let results =
                api_load::run(&pool, &args.api_base_url, Some(&head_sha), profile).await?;
            pool.close().await;
            finish_release_run(&head_sha)?;
            let report = GateReport {
                head_sha,
                source_tree_clean: true,
                cargo_profile: cargo_profile(),
                interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH,
                budgets: BUDGETS_PATH,
                budget_profile: "production",
                database: database_name,
                database_host,
                api_base_url: Some(args.api_base_url),
                green: results.green,
                results,
            };
            emit_report(&report, cli.report.as_deref())?;
            if !report.green {
                bail!("API benchmark gate is red");
            }
            Ok(())
        }
        Command::Smoke(args) => {
            let database_host = smoke::configured_database_host()?;
            let results =
                smoke::run(&args.api_binary, budgets.profile(BudgetProfile::Smoke)).await?;
            let report = GateReport {
                head_sha: git_head(),
                source_tree_clean: worktree_is_clean()?,
                cargo_profile: cargo_profile(),
                interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH,
                budgets: BUDGETS_PATH,
                budget_profile: "smoke",
                database: "isolated scripts/test-db database".to_owned(),
                database_host,
                api_base_url: None,
                green: results.green,
                results,
            };
            emit_report(&report, cli.report.as_deref())?;
            if !report.green {
                bail!("benchmark smoke gate is red");
            }
            Ok(())
        }
    }
}

fn git_head() -> String {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn require_clean_worktree() -> Result<()> {
    ensure!(
        worktree_is_clean()?,
        "production benchmark commands require a clean worktree"
    );
    Ok(())
}

fn begin_release_run() -> Result<String> {
    require_clean_worktree()?;
    let head = git_head();
    ensure!(
        head != "unknown",
        "production benchmark could not identify HEAD"
    );
    let built_head = std::env::var("BIGNAME_BENCHMARK_BUILT_HEAD")
        .context("production benchmark must be launched by scripts/benchmark-gate")?;
    ensure!(
        built_head == head,
        "benchmark binary was built from {built_head}, but runtime HEAD is {head}"
    );
    Ok(head)
}

const fn cargo_profile_for_debug_assertions(debug_assertions: bool) -> &'static str {
    if debug_assertions { "dev" } else { "release" }
}

fn cargo_profile() -> String {
    cargo_profile_for_debug_assertions(cfg!(debug_assertions)).to_owned()
}

fn require_release_profile() -> Result<()> {
    ensure!(
        cargo_profile() == "release",
        "production benchmark commands require the release Cargo profile"
    );
    let launched_profile = std::env::var("BIGNAME_BENCHMARK_CARGO_PROFILE")
        .context("production benchmark must be launched by scripts/benchmark-gate")?;
    ensure!(
        launched_profile == "release",
        "production benchmark wrapper used Cargo profile {launched_profile:?}; release is required"
    );
    Ok(())
}

fn finish_release_run(expected_head: &str) -> Result<()> {
    require_clean_worktree()?;
    ensure!(
        git_head() == expected_head,
        "benchmark source HEAD changed while the production gate was running"
    );
    Ok(())
}

fn worktree_is_clean() -> Result<bool> {
    let output = std::process::Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=normal"])
        .output()
        .context("failed to inspect benchmark source worktree")?;
    ensure!(
        output.status.success(),
        "git could not inspect benchmark source worktree"
    );
    Ok(output.stdout.is_empty())
}

fn emit_report<T: Serialize>(report: &GateReport<T>, path: Option<&Path>) -> Result<()> {
    let json =
        serde_json::to_string_pretty(report).context("failed to serialize benchmark report")?;
    println!("{json}");
    if let Some(path) = path {
        fs::write(path, format!("{json}\n"))
            .with_context(|| format!("failed to write benchmark report to {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indexing_requires_disposable_copy_acknowledgement() {
        let result = Cli::try_parse_from([
            "bigname-benchmark-gate",
            "index",
            "--database-url",
            "postgres://localhost/bigname",
            "--chain",
            "ethereum-mainnet",
            "--head-block",
            "10",
            "--walk-from-block",
            "1",
            "--walk-to-block",
            "10",
            "--expected-database-name",
            "bigname-benchmark-copy",
            "--disposable-marker",
            "00000000-0000-4000-8000-000000000001",
            "--chain-rpc-url",
            "http://127.0.0.1:8545",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn reported_cargo_profile_comes_from_compiled_assertion_mode() {
        assert_eq!(cargo_profile_for_debug_assertions(true), "dev");
        assert_eq!(cargo_profile_for_debug_assertions(false), "release");
        assert_eq!(
            cargo_profile(),
            cargo_profile_for_debug_assertions(cfg!(debug_assertions))
        );
    }

    #[test]
    fn a_binary_not_launched_by_the_release_wrapper_is_rejected() {
        assert!(require_release_profile().is_err());
    }
}
