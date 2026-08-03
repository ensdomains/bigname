use std::{collections::BTreeSet, path::PathBuf, str::FromStr, time::Duration};

use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

use crate::{
    config::{
        CapacityConfig, ChainConfig, RuntimeConfig, SeedBasis, SourceConfig, TimingConfig,
        group_sources,
    },
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
    runner::RedoPhase,
};

#[derive(Debug, Parser)]
#[command(name = "phase-runner")]
#[command(about = "Run the per-chain indexing phases")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Install the fresh schema-v2 baseline into an empty phase schema.
    InitSchema(InitSchemaArgs),
    /// Supervise every configured chain.
    Run(RunArgs),
    /// Run one phase over an explicit block range.
    Redo(RedoArgs),
    /// Move the published latest head back to an exact stored ancestor.
    Rewind(RewindArgs),
}

#[derive(Clone, Debug, Args)]
struct InitSchemaArgs {
    #[arg(long, env = "BIGNAME_DATABASE_URL")]
    database_url: String,
}

#[derive(Clone, Debug, Args)]
struct ConnectionArgs {
    #[arg(long, env = "BIGNAME_DATABASE_URL")]
    database_url: String,

    #[arg(long, env = "BIGNAME_PHASE_RUNNER_INSTANCE_ID")]
    instance_id: Option<String>,
}

#[derive(Clone, Debug, Args)]
struct CapacityArgs {
    #[arg(long, env = "BIGNAME_PHASE_RUNNER_DATABASE_MAX_BYTES")]
    database_max_bytes: Option<u64>,

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_MINIMUM_FREE_DISK_BYTES",
        default_value_t = 0
    )]
    minimum_free_disk_bytes: u64,

    #[arg(long, env = "BIGNAME_PHASE_RUNNER_WRITABLE_PATH", default_value = ".")]
    writable_path: PathBuf,

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_CAPACITY_POLL_MS",
        default_value_t = 5_000
    )]
    capacity_poll_ms: u64,
}

#[derive(Clone, Debug, Args)]
struct TimingArgs {
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_INITIAL_BACKOFF_MS",
        default_value_t = 1_000
    )]
    initial_backoff_ms: u64,

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_MAXIMUM_BACKOFF_MS",
        default_value_t = 30_000
    )]
    maximum_backoff_ms: u64,

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_LIVE_POLL_MS",
        default_value_t = 1_000
    )]
    live_poll_ms: u64,
}

#[derive(Clone, Debug, Args)]
struct ManifestArgs {
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_MANIFESTS_ROOT",
        default_value = "manifests/mainnet"
    )]
    manifests_root: PathBuf,
}

#[derive(Debug, Args)]
struct RunArgs {
    #[command(flatten)]
    connection: ConnectionArgs,

    #[arg(long, env = "BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL")]
    verification_database_url: String,

    #[command(flatten)]
    capacity: CapacityArgs,

    #[command(flatten)]
    timing: TimingArgs,

    #[command(flatten)]
    manifests: ManifestArgs,

    #[arg(
        long = "chain",
        env = "BIGNAME_PHASE_RUNNER_CHAINS",
        value_delimiter = ','
    )]
    chains: Vec<String>,

    #[arg(
        long = "source",
        env = "BIGNAME_PHASE_RUNNER_SOURCES",
        value_delimiter = ',',
        help = "CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK=URL_ENV"
    )]
    sources: Vec<String>,

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_VERIFY_BEFORE_LIVE",
        value_delimiter = ','
    )]
    verify_before_live: Vec<String>,

    #[arg(
        long = "hydration-rpc",
        env = "BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS",
        value_delimiter = ',',
        help = "CHAIN=HTTP_URL used only for hash-pinned multicall hydration"
    )]
    hydration_rpc_urls: Vec<String>,
}

#[derive(Debug, Args)]
struct RedoArgs {
    #[command(flatten)]
    connection: ConnectionArgs,

    #[arg(long, env = "BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL")]
    verification_database_url: Option<String>,

    #[command(flatten)]
    capacity: CapacityArgs,

    #[command(flatten)]
    timing: TimingArgs,

    #[command(flatten)]
    manifests: ManifestArgs,

    #[arg(long)]
    chain: String,

    #[arg(
        long,
        help = "ingest, interpret, project, verify, live, or recompute-flags"
    )]
    phase: String,

    #[arg(long)]
    from_block: i64,

    #[arg(long)]
    to_block: i64,

    #[arg(
        long = "source",
        env = "BIGNAME_PHASE_RUNNER_SOURCES",
        value_delimiter = ',',
        help = "CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK=URL_ENV"
    )]
    sources: Vec<String>,

    #[arg(
        long = "hydration-rpc",
        env = "BIGNAME_PHASE_RUNNER_HYDRATION_RPC_URLS",
        value_delimiter = ',',
        help = "CHAIN=HTTP_URL used only for hash-pinned multicall hydration"
    )]
    hydration_rpc_urls: Vec<String>,
}

#[derive(Clone, Debug, Args)]
struct RewindArgs {
    #[command(flatten)]
    connection: ConnectionArgs,

    #[arg(long)]
    chain: String,

    #[arg(long)]
    ancestor_block: i64,

    #[arg(long)]
    ancestor_hash: String,
}

pub enum ResolvedCommand {
    InitSchema {
        database_url: String,
    },
    Run {
        database_url: String,
        verification_database_url: String,
        manifests_root: PathBuf,
        runtime: RuntimeConfig,
        hydration_rpc_urls: bigname_lookup::ChainRpcUrls,
    },
    Redo {
        database_url: String,
        verification_database_url: Option<String>,
        manifests_root: PathBuf,
        instance_id: String,
        chain: ChainConfig,
        capacity: CapacityConfig,
        timing: TimingConfig,
        phase: RedoPhase,
        range: BlockRange,
        hydration_rpc_urls: bigname_lookup::ChainRpcUrls,
    },
    Rewind {
        database_url: String,
        chain_id: String,
        ancestor: crate::heads::BlockMarker,
    },
}

impl Cli {
    pub fn resolve(self) -> RunnerResult<ResolvedCommand> {
        match self.command {
            Command::InitSchema(args) => Ok(ResolvedCommand::InitSchema {
                database_url: args.database_url,
            }),
            Command::Run(args) => resolve_run(args),
            Command::Redo(args) => resolve_redo(args),
            Command::Rewind(args) => resolve_rewind(args),
        }
    }
}

fn resolve_rewind(args: RewindArgs) -> RunnerResult<ResolvedCommand> {
    if args.chain.trim().is_empty() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "rewind chain must not be empty",
        ));
    }
    Ok(ResolvedCommand::Rewind {
        database_url: args.connection.database_url,
        chain_id: args.chain,
        ancestor: crate::heads::BlockMarker::new(args.ancestor_block, args.ancestor_hash)?,
    })
}

fn resolve_run(args: RunArgs) -> RunnerResult<ResolvedCommand> {
    if args.chains.is_empty() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "at least one --chain must be configured",
        ));
    }
    let sources = args
        .sources
        .iter()
        .map(|source| parse_source(source))
        .collect::<RunnerResult<Vec<_>>>()?;
    let verify_before_live = args.verify_before_live.into_iter().collect::<BTreeSet<_>>();
    for chain in &verify_before_live {
        if !args.chains.contains(chain) {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!("verify-before-live names unconfigured chain {chain:?}"),
            ));
        }
    }
    let chains = group_sources(&args.chains, sources, &verify_before_live)?;
    let capacity = resolve_capacity(args.capacity)?;
    let timing = resolve_timing(args.timing)?;
    let instance_id = resolve_instance_id(args.connection.instance_id)?;
    let runtime = RuntimeConfig::new(instance_id, chains, capacity, timing)?;
    let hydration_rpc_urls = resolve_hydration_rpc_urls(&args.hydration_rpc_urls)?;
    Ok(ResolvedCommand::Run {
        database_url: args.connection.database_url,
        verification_database_url: args.verification_database_url,
        manifests_root: args.manifests.manifests_root,
        runtime,
        hydration_rpc_urls,
    })
}

fn resolve_redo(args: RedoArgs) -> RunnerResult<ResolvedCommand> {
    let phase = parse_redo_phase(&args.phase)?;
    let range = BlockRange::new(args.from_block, args.to_block)?;
    let sources = args
        .sources
        .iter()
        .map(|source| parse_source(source))
        .collect::<RunnerResult<Vec<_>>>()?;
    if sources.iter().any(|source| source.chain_id != args.chain) {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "redo source belongs to a different chain",
        ));
    }
    if phase == RedoPhase::Phase(PhaseName::Ingest) && sources.is_empty() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "redo ingest requires at least one --source",
        ));
    }
    if phase == RedoPhase::Phase(PhaseName::Verify) && args.verification_database_url.is_none() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "verify redo requires --verification-database-url backed by a SELECT-only role",
        ));
    }
    Ok(ResolvedCommand::Redo {
        database_url: args.connection.database_url,
        verification_database_url: args.verification_database_url,
        manifests_root: args.manifests.manifests_root,
        instance_id: resolve_instance_id(args.connection.instance_id)?,
        chain: ChainConfig::new(args.chain, sources, false)?,
        capacity: resolve_capacity(args.capacity)?,
        timing: resolve_timing(args.timing)?,
        phase,
        range,
        hydration_rpc_urls: resolve_hydration_rpc_urls(&args.hydration_rpc_urls)?,
    })
}

fn resolve_hydration_rpc_urls(entries: &[String]) -> RunnerResult<bigname_lookup::ChainRpcUrls> {
    bigname_lookup::ChainRpcUrls::from_entries(entries).map_err(|error| {
        RunnerError::new(
            ErrorKind::Configuration,
            format!("invalid hydration RPC configuration: {error:#}"),
        )
    })
}

fn resolve_instance_id(instance_id: Option<String>) -> RunnerResult<String> {
    let instance_id = instance_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    if instance_id.trim().is_empty() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "runner instance id must not be empty",
        ));
    }
    Ok(instance_id)
}

fn resolve_capacity(args: CapacityArgs) -> RunnerResult<CapacityConfig> {
    if args.capacity_poll_ms == 0 {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "capacity poll interval must be positive",
        ));
    }
    Ok(CapacityConfig {
        database_max_bytes: args.database_max_bytes,
        minimum_free_disk_bytes: args.minimum_free_disk_bytes,
        writable_path: args.writable_path,
        poll_interval: Duration::from_millis(args.capacity_poll_ms),
    })
}

fn resolve_timing(args: TimingArgs) -> RunnerResult<TimingConfig> {
    let timing = TimingConfig {
        initial_backoff: Duration::from_millis(args.initial_backoff_ms),
        maximum_backoff: Duration::from_millis(args.maximum_backoff_ms),
        live_poll_interval: Duration::from_millis(args.live_poll_ms),
    };
    timing.validate()?;
    Ok(timing)
}

fn parse_redo_phase(value: &str) -> RunnerResult<RedoPhase> {
    if value == "recompute-flags" {
        return Ok(RedoPhase::RecomputeFlags);
    }
    PhaseName::from_str(value).map(RedoPhase::Phase)
}

fn parse_source(specification: &str) -> RunnerResult<SourceConfig> {
    let (descriptor, environment_name) = specification
        .split_once('=')
        .ok_or_else(|| invalid_source("missing =URL_ENV", specification))?;
    if environment_name.trim().is_empty() {
        return Err(invalid_source(
            "URL environment name is empty",
            specification,
        ));
    }
    let fields = descriptor.split(':').collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(invalid_source(
            "expected CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK=URL_ENV",
            specification,
        ));
    }
    let endpoint = std::env::var(environment_name).map_err(|_| {
        RunnerError::new(
            ErrorKind::Configuration,
            format!(
                "source {} for chain {} requires environment variable {environment_name}",
                fields[1], fields[0]
            ),
        )
    })?;
    let start_block_number = fields[4]
        .parse::<i64>()
        .map_err(|_| invalid_source("START_BLOCK is not an integer", specification))?;
    SourceConfig::new(
        fields[0],
        fields[1],
        fields[2],
        SeedBasis::parse(fields[3])?,
        start_block_number,
        endpoint,
    )
}

fn invalid_source(reason: &str, specification: &str) -> RunnerError {
    let descriptor = specification
        .split_once('=')
        .map(|(descriptor, _)| descriptor)
        .unwrap_or(specification);
    RunnerError::new(
        ErrorKind::Configuration,
        format!("invalid source descriptor {descriptor:?}: {reason}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redo_cli_carries_canonical_head_hydration_rpc() {
        let command = Cli::try_parse_from([
            "phase-runner",
            "redo",
            "--database-url",
            "postgres://phase-runner.invalid/fresh",
            "--chain",
            "ethereum-mainnet",
            "--phase",
            "project",
            "--from-block",
            "42",
            "--to-block",
            "42",
            "--hydration-rpc",
            "ethereum-mainnet=http://hydration.invalid",
        ])
        .expect("redo hydration RPC option must parse")
        .resolve()
        .expect("redo hydration RPC option must resolve");

        match command {
            ResolvedCommand::Redo {
                hydration_rpc_urls, ..
            } => assert_eq!(
                hydration_rpc_urls.url_for("ethereum-mainnet"),
                Some("http://hydration.invalid")
            ),
            _ => panic!("expected redo command"),
        }
    }

    #[test]
    fn verify_redo_requires_a_separate_verification_database_url() {
        let command = Cli::try_parse_from([
            "phase-runner",
            "redo",
            "--database-url",
            "postgres://phase-runner.invalid/fresh",
            "--chain",
            "ethereum-mainnet",
            "--phase",
            "verify",
            "--from-block",
            "42",
            "--to-block",
            "42",
        ])
        .expect("verify redo without the reader URL must parse before semantic validation");
        let error = match command.resolve() {
            Ok(_) => panic!("verify redo must reject a missing verification database URL"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(error.to_string().contains("SELECT-only role"));
    }
}
