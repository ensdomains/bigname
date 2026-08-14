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
mod compiler_attestation;
mod database;
mod indexing;
mod smoke;

use budgets::{BudgetProfile, BudgetsFile};
use compiler_attestation::cargo_profile;

const BUDGETS_PATH: &str = "benchmarks/release-gate.toml";
const COMPILED_HEAD: Option<&str> = option_env!("BIGNAME_BENCHMARK_BUILT_HEAD");

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
struct SmokeArgs {}

#[derive(Debug, Serialize)]
struct GateReport<T: Serialize> {
    head_sha: String,
    source_tree_clean: bool,
    cargo_profile: String,
    rustc_version: String,
    rustflags: String,
    cargo_encoded_rustflags: String,
    benchmark_binary_sha256: String,
    locally_built_api_binary_sha256: String,
    interpreter_content_hash: &'static str,
    budgets: &'static str,
    budget_profile: &'static str,
    database: String,
    database_host: String,
    api_base_url: Option<String>,
    green: bool,
    results: T,
}

#[derive(Debug)]
struct WrapperBuildAttestation {
    rustc_version: String,
    rustflags: String,
    cargo_encoded_rustflags: String,
    benchmark_binary_sha256: String,
    locally_built_api_binary_sha256: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    require_compiled_head(COMPILED_HEAD)?;
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
            let pool = database::connect_disposable_copy(
                &args.database_url,
                8,
                timeout,
                &args.expected_database_name,
                args.disposable_marker,
            )
            .await?;
            let database_name = database::database_identity(&pool).await?;
            let database_host = database::database_host(&pool).await?;
            database::require_database_identity(&database_name, &args.expected_database_name)?;
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
            let build = wrapper_build_attestation()?;
            let report = GateReport {
                head_sha,
                source_tree_clean: true,
                cargo_profile: cargo_profile(),
                rustc_version: build.rustc_version,
                rustflags: build.rustflags,
                cargo_encoded_rustflags: build.cargo_encoded_rustflags,
                benchmark_binary_sha256: build.benchmark_binary_sha256,
                locally_built_api_binary_sha256: build.locally_built_api_binary_sha256,
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
            let build = wrapper_build_attestation()?;
            let report = GateReport {
                head_sha,
                source_tree_clean: true,
                cargo_profile: cargo_profile(),
                rustc_version: build.rustc_version,
                rustflags: build.rustflags,
                cargo_encoded_rustflags: build.cargo_encoded_rustflags,
                benchmark_binary_sha256: build.benchmark_binary_sha256,
                locally_built_api_binary_sha256: build.locally_built_api_binary_sha256,
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
        Command::Smoke(_) => {
            let api_binary = std::env::var("BIGNAME_BENCHMARK_API_BINARY")
                .map(PathBuf::from)
                .context("benchmark smoke must use the API binary resolved by the wrapper")?;
            let expected_api_digest = std::env::var("BIGNAME_BENCHMARK_API_BINARY_SHA256")
                .context("benchmark wrapper did not attest the smoke API binary digest")?;
            require_file_sha256(&api_binary, &expected_api_digest, "smoke API binary")?;
            let database_host = smoke::configured_database_host()?;
            let results = smoke::run(&api_binary, budgets.profile(BudgetProfile::Smoke)).await?;
            let build = wrapper_build_attestation()?;
            let report = GateReport {
                head_sha: git_head(),
                source_tree_clean: worktree_is_clean()?,
                cargo_profile: cargo_profile(),
                rustc_version: build.rustc_version,
                rustflags: build.rustflags,
                cargo_encoded_rustflags: build.cargo_encoded_rustflags,
                benchmark_binary_sha256: build.benchmark_binary_sha256,
                locally_built_api_binary_sha256: build.locally_built_api_binary_sha256,
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
    let compiled_head = require_compiled_head(COMPILED_HEAD)?;
    let launched_head = std::env::var("BIGNAME_BENCHMARK_BUILT_HEAD")
        .context("production benchmark must be launched by scripts/benchmark-gate")?;
    ensure!(
        launched_head == compiled_head,
        "benchmark wrapper attested {launched_head}, but the binary embeds {compiled_head}"
    );
    ensure!(
        compiled_head == head,
        "benchmark binary was compiled from {compiled_head}, but runtime HEAD is {head}"
    );
    Ok(head)
}

fn require_compiled_head(compiled_head: Option<&str>) -> Result<&str> {
    let compiled_head = compiled_head.context(
        "benchmark binary has no compile-time source SHA; build it through scripts/benchmark-gate",
    )?;
    ensure!(
        !compiled_head.trim().is_empty(),
        "benchmark binary has an empty compile-time source SHA"
    );
    Ok(compiled_head)
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
    for name in [
        "BIGNAME_BENCHMARK_RUSTFLAGS",
        "BIGNAME_BENCHMARK_CARGO_ENCODED_RUSTFLAGS",
    ] {
        let value = std::env::var(name)
            .with_context(|| format!("production benchmark wrapper did not attest {name}"))?;
        ensure!(
            value.is_empty(),
            "production benchmark wrapper reported non-empty {name}"
        );
    }
    compiler_attestation::require_no_compiler_overrides(|name| std::env::var(name).ok())?;
    Ok(())
}

fn wrapper_build_attestation() -> Result<WrapperBuildAttestation> {
    let required = |name: &str| {
        std::env::var(name).with_context(|| format!("benchmark wrapper did not attest {name}"))
    };
    let rustc_version = required("BIGNAME_BENCHMARK_RUSTC_VERSION")?;
    ensure!(
        !rustc_version.trim().is_empty(),
        "benchmark wrapper reported an empty rustc version"
    );
    let benchmark_binary_sha256 = required("BIGNAME_BENCHMARK_BINARY_SHA256")?;
    let locally_built_api_binary_sha256 = required("BIGNAME_BENCHMARK_API_BINARY_SHA256")?;
    for (label, digest) in [
        ("benchmark binary", benchmark_binary_sha256.as_str()),
        (
            "locally built API binary",
            locally_built_api_binary_sha256.as_str(),
        ),
    ] {
        ensure!(
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "benchmark wrapper reported an invalid {label} SHA-256 digest"
        );
    }
    let current_exe = std::env::current_exe().context("failed to identify benchmark executable")?;
    require_file_sha256(&current_exe, &benchmark_binary_sha256, "benchmark binary")?;
    Ok(WrapperBuildAttestation {
        rustc_version,
        rustflags: required("BIGNAME_BENCHMARK_RUSTFLAGS")?,
        cargo_encoded_rustflags: required("BIGNAME_BENCHMARK_CARGO_ENCODED_RUSTFLAGS")?,
        benchmark_binary_sha256,
        locally_built_api_binary_sha256,
    })
}

fn require_file_sha256(path: &Path, expected: &str, label: &str) -> Result<()> {
    let output = std::process::Command::new("sha256sum")
        .arg(path)
        .output()
        .with_context(|| format!("failed to hash {label} at {}", path.display()))?;
    ensure!(output.status.success(), "sha256sum failed for {label}");
    let stdout = String::from_utf8(output.stdout)
        .with_context(|| format!("sha256sum returned non-UTF-8 output for {label}"))?;
    let actual = stdout
        .split_whitespace()
        .next()
        .context("sha256sum returned no digest")?;
    ensure!(
        actual == expected,
        "{label} digest {actual} does not match wrapper attestation {expected}"
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
    fn smoke_refuses_a_cli_api_binary_override() {
        let result = Cli::try_parse_from([
            "bigname-benchmark-gate",
            "smoke",
            "--api-binary",
            "/tmp/not-the-wrapper-binary",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn executable_digest_must_match_the_wrapper_attestation() {
        let path = std::env::temp_dir().join(format!(
            "bigname-benchmark-digest-test-{}",
            std::process::id()
        ));
        fs::write(&path, b"abc").unwrap();
        let expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assert!(require_file_sha256(&path, expected, "test binary").is_ok());
        let error = require_file_sha256(&path, &"0".repeat(64), "test binary")
            .unwrap_err()
            .to_string();
        fs::remove_file(path).unwrap();
        assert!(error.contains("does not match wrapper attestation"));
    }

    #[test]
    fn a_binary_not_launched_by_the_release_wrapper_is_rejected() {
        assert!(require_release_profile().is_err());
    }

    #[test]
    fn a_binary_without_a_compile_time_source_sha_is_rejected() {
        assert!(require_compiled_head(None).is_err());
        assert!(require_compiled_head(Some("")).is_err());
        assert_eq!(require_compiled_head(Some("abc123")).unwrap(), "abc123");
    }
}
