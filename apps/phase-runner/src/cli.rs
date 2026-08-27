use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

use crate::{
    config::{
        COMPILED_CHAIN_NAMESPACES, CapacityConfig, ChainConfig, RuntimeConfig, SourceConfig,
        TimingConfig, group_sources, validate_deployment_table_set,
    },
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
    runner::RedoPhase,
};

#[path = "cli_inspect.rs"]
mod inspect_resolution;
#[path = "cli_label_preimages.rs"]
mod label_preimages;
#[path = "cli_monitoring.rs"]
mod monitoring;
#[path = "cli_source.rs"]
mod source;
#[path = "cli_attestation.rs"]
mod watch_set_attestation;

use {
    monitoring::{HeartbeatArgs, MonitoringArgs},
    source::parse_source,
};

#[cfg(test)]
#[path = "cli/tests.rs"]
mod tests;

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
    /// Read bounded schema-v2 lineage and raw-event inspection windows.
    Inspect(inspect_resolution::InspectArgs),
    /// Manage verified label preimages.
    LabelPreimages(label_preimages::LabelPreimagesArgs),
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
    #[arg(long, env = "BIGNAME_PHASE_RUNNER_INTERPRETER_STATE_CACHE_ENTRIES")]
    interpreter_state_cache_entries: Option<usize>,
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
    monitoring: MonitoringArgs,

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
        help = "CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK[:ROLE]=URL_ENV"
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

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_REDO_METRICS_BIND_ADDR",
        default_value = "127.0.0.1:0",
        help = "metrics listener; defaults to an ephemeral loopback port"
    )]
    metrics_bind_addr: SocketAddr,

    #[command(flatten)]
    heartbeat: HeartbeatArgs,

    #[command(flatten)]
    capacity: CapacityArgs,

    #[command(flatten)]
    timing: TimingArgs,

    #[command(flatten)]
    manifests: ManifestArgs,

    #[arg(
        long = "chain",
        value_delimiter = ',',
        required_unless_present = "all_chains",
        conflicts_with = "all_chains"
    )]
    chains: Vec<String>,

    #[arg(
        long,
        conflicts_with = "chains",
        help = "redo every chain admitted by the synchronized manifest profile"
    )]
    all_chains: bool,

    #[arg(
        long,
        help = "ingest, interpret, project, verify, live, recompute-flags, or all"
    )]
    phase: String,

    #[arg(long)]
    from_block: i64,

    #[arg(long)]
    to_block: i64,

    #[arg(
        long,
        value_name = "TOKEN|CHAIN=TOKEN",
        action = clap::ArgAction::Append,
        help = "attest manifest-authority coverage after the required historical fetch or a no-widening review"
    )]
    attest_watch_set_coverage: Vec<String>,

    #[arg(
        long = "source",
        env = "BIGNAME_PHASE_RUNNER_SOURCES",
        value_delimiter = ',',
        help = "CHAIN:KEY:KIND:SEED_BASIS:START_BLOCK[:ROLE]=URL_ENV"
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
        metrics_bind_addr: SocketAddr,
        heartbeat_stale_after_secs: i64,
        manifests_root: PathBuf,
        runtime: RuntimeConfig,
        hydration_rpc_urls: bigname_lookup::ChainRpcUrls,
    },
    Redo {
        database_url: String,
        verification_database_url: Option<String>,
        metrics_bind_addr: SocketAddr,
        heartbeat_stale_after_secs: i64,
        manifests_root: PathBuf,
        instance_id: String,
        chains: RedoChains,
        capacity: CapacityConfig,
        timing: TimingConfig,
        phase: RedoPhase,
        range: BlockRange,
        watch_set_coverage_attestations: BTreeMap<String, String>,
        hydration_rpc_urls: bigname_lookup::ChainRpcUrls,
    },
    Rewind {
        database_url: String,
        chain_id: String,
        ancestor: crate::heads::BlockMarker,
    },
    Inspect {
        database_url: String,
        request: crate::inspect::InspectionRequest,
    },
    LabelPreimagesImportEnsRainbow {
        database_url: String,
        batch_size: Option<i64>,
        limit: Option<i64>,
    },
}

pub enum RedoChains {
    Explicit(Vec<ChainConfig>),
    All { sources: Vec<SourceConfig> },
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
            Command::Inspect(args) => inspect_resolution::resolve(args),
            Command::LabelPreimages(args) => label_preimages::resolve(args),
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
    validate_heartbeat_threshold(args.monitoring.heartbeat.heartbeat_stale_after_secs)?;
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
        metrics_bind_addr: args.monitoring.metrics_bind_addr,
        heartbeat_stale_after_secs: args.monitoring.heartbeat.heartbeat_stale_after_secs,
        manifests_root: args.manifests.manifests_root,
        runtime,
        hydration_rpc_urls,
    })
}

fn resolve_redo(args: RedoArgs) -> RunnerResult<ResolvedCommand> {
    validate_heartbeat_threshold(args.heartbeat.heartbeat_stale_after_secs)?;
    let phase = parse_redo_phase(&args.phase)?;
    let range = BlockRange::new(args.from_block, args.to_block)?;
    let watch_set_coverage_attestations = watch_set_attestation::resolve(
        &args.attest_watch_set_coverage,
        args.all_chains,
        &args.chains,
    )?;
    let sources = args
        .sources
        .iter()
        .map(|source| parse_source(source))
        .collect::<RunnerResult<Vec<_>>>()?;
    if phase.requires_intake_sources() && sources.is_empty() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "ingest, interpret, project, verify, or all-phase redo requires at least one \
             intake-capable --source",
        ));
    }
    if phase.requires_verify() && args.verification_database_url.is_none() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "verify or all-phase redo requires --verification-database-url backed by a SELECT-only role",
        ));
    }
    let chains = if args.all_chains {
        RedoChains::All { sources }
    } else {
        RedoChains::Explicit(resolve_explicit_redo_chains(
            args.chains,
            sources,
            phase.requires_intake_sources(),
        )?)
    };
    if let RedoChains::Explicit(chains) = &chains {
        validate_deployment_table_set(chains, COMPILED_CHAIN_NAMESPACES.iter().copied())?;
    }
    Ok(ResolvedCommand::Redo {
        database_url: args.connection.database_url,
        verification_database_url: args.verification_database_url,
        metrics_bind_addr: args.metrics_bind_addr,
        heartbeat_stale_after_secs: args.heartbeat.heartbeat_stale_after_secs,
        manifests_root: args.manifests.manifests_root,
        instance_id: resolve_instance_id(args.connection.instance_id)?,
        chains,
        capacity: resolve_capacity(args.capacity)?,
        timing: resolve_timing(args.timing)?,
        phase,
        range,
        watch_set_coverage_attestations,
        hydration_rpc_urls: resolve_hydration_rpc_urls(&args.hydration_rpc_urls)?,
    })
}

fn validate_heartbeat_threshold(heartbeat_stale_after_secs: i64) -> RunnerResult<()> {
    if heartbeat_stale_after_secs <= 0 {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "heartbeat stale threshold must be positive",
        ));
    }
    Ok(())
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
        interpreter_state_cache_entries: args
            .interpreter_state_cache_entries
            .unwrap_or(bigname_interpret::DEFAULT_INTERPRETER_STATE_CACHE_ENTRIES),
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
    if value == "all" {
        return Ok(RedoPhase::All);
    }
    PhaseName::from_str(value).map(RedoPhase::Phase)
}

fn resolve_explicit_redo_chains(
    chain_ids: Vec<String>,
    sources: Vec<SourceConfig>,
    require_sources: bool,
) -> RunnerResult<Vec<ChainConfig>> {
    let configured = chain_ids.iter().cloned().collect::<BTreeSet<_>>();
    if configured.len() != chain_ids.len() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "redo configures a chain more than once",
        ));
    }
    let mut by_chain = BTreeMap::<String, Vec<SourceConfig>>::new();
    for source in sources {
        if !configured.contains(&source.chain_id) {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "redo source belongs to unconfigured chain {:?}",
                    source.chain_id
                ),
            ));
        }
        by_chain
            .entry(source.chain_id.clone())
            .or_default()
            .push(source);
    }
    chain_ids
        .into_iter()
        .map(|chain_id| {
            let sources = by_chain.remove(&chain_id).unwrap_or_default();
            let chain = ChainConfig::new(chain_id, sources, false)?;
            if require_sources {
                chain.require_intake_sources()?;
            }
            Ok(chain)
        })
        .collect()
}

pub async fn resolve_all_redo_chains(
    pool: &sqlx::PgPool,
    sources: Vec<SourceConfig>,
    require_sources: bool,
) -> RunnerResult<Vec<ChainConfig>> {
    let chain_ids: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT chain_id
         FROM manifest_versions
         WHERE rollout_status = 'active'
         ORDER BY chain_id",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| RunnerError::database("failed to list admitted redo chains", error))?;
    if chain_ids.is_empty() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "all-chains redo found no active manifest chains",
        ));
    }
    resolve_explicit_redo_chains(chain_ids, sources, require_sources)
}

pub fn validate_redo_attestation_chains(
    attestations: &BTreeMap<String, String>,
    chains: &[ChainConfig],
) -> RunnerResult<()> {
    watch_set_attestation::validate_resolved_chains(attestations, chains)
}
