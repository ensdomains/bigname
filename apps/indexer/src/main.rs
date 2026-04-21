#[path = "main/backfill.rs"]
mod backfill;
#[cfg(test)]
#[allow(dead_code, unused_imports)]
#[path = "main/tests/backfill.rs"]
mod backfill_tests;
mod provider;
#[path = "main/reconciliation.rs"]
mod reconciliation;
#[path = "main/runtime.rs"]
mod runtime;
#[cfg(test)]
#[path = "main/tests.rs"]
mod tests;

use std::path::PathBuf;

use anyhow::Result;
use backfill::{BackfillBlockRange, run_hash_pinned_backfill_range};
use bigname_manifests::{
    ManifestLoadStatus, ManifestLoadSummary, ManifestSyncStatus, ManifestSyncSummary,
    WatchedChainPlan, load_watched_chain_plan, load_watched_contract_summary,
};
#[allow(unused_imports)]
use bigname_storage::{
    CanonicalityState, ChainCheckpoint, ChainCheckpointUpdate, CheckpointBlockRef, DatabaseConfig,
    RawCodeHash, RawLog, RawReceipt, RawTransaction, advance_chain_checkpoints,
    upsert_chain_lineage_blocks, upsert_raw_blocks, upsert_raw_code_hashes, upsert_raw_logs,
    upsert_raw_receipts, upsert_raw_transactions,
};
use clap::{Args, Parser, Subcommand};
#[allow(unused_imports)]
use provider::{JsonRpcProvider, ProviderBlock, ProviderHeadSnapshot, ProviderRegistry};
#[allow(unused_imports)]
use reconciliation::*;
#[allow(unused_imports)]
use runtime::*;
#[allow(unused_imports)]
use sha3::{Digest, Keccak256};
use tracing::info;

const MAX_PARENT_FETCH_DEPTH: usize = 32;
#[derive(Parser, Debug)]
#[command(
    name = "bigname-indexer",
    about = "Bootstrap indexer process for bigname"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Run(RunArgs),
    Backfill(BackfillArgs),
}

#[derive(Args, Debug)]
struct RunArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests"
    )]
    manifests_root: PathBuf,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_POLL_INTERVAL_SECS",
        default_value_t = 5_u64
    )]
    poll_interval_secs: u64,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    chain_rpc_urls: Vec<String>,
}

#[derive(Args, Debug)]
struct BackfillArgs {
    #[command(flatten)]
    database: DatabaseConfig,
    #[arg(
        long,
        env = "BIGNAME_INDEXER_MANIFESTS_ROOT",
        default_value = "manifests"
    )]
    manifests_root: PathBuf,
    #[arg(
        long = "chain-rpc-url",
        env = "BIGNAME_INDEXER_CHAIN_RPC_URLS",
        value_delimiter = ','
    )]
    chain_rpc_urls: Vec<String>,
    #[arg(long)]
    chain: String,
    #[arg(long)]
    from_block: i64,
    #[arg(long)]
    to_block: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing("bigname-indexer");

    match Cli::parse().command {
        Command::Run(args) => run(args).await,
        Command::Backfill(args) => run_backfill(args).await,
    }
}

async fn run(args: RunArgs) -> Result<()> {
    let manifest_repository = load_manifest_repository(&args.manifests_root)?;
    let manifest_summary = manifest_repository.summary().clone();
    log_manifest_summary(&manifest_summary);
    ensure_manifest_root_ready(&manifest_summary)?;

    let pool = bigname_storage::connect(&args.database).await?;
    let manifest_runtime_state = build_manifest_runtime_state(&pool, &manifest_repository).await?;
    sync_adapter_owned_raw_log_state(&pool, &manifest_runtime_state.watched_chain_plan).await?;
    log_manifest_runtime_state(&manifest_runtime_state);
    log_watched_chain_plan("startup", &manifest_runtime_state.watched_chain_plan);
    let watched_chain_plan_state =
        watched_chain_plan_state(&manifest_runtime_state.watched_chain_plan);
    let intake_chain_tasks =
        sync_intake_chain_tasks(&pool, &manifest_runtime_state.watched_chain_plan).await?;
    log_intake_chain_tasks("startup", &intake_chain_tasks);
    let intake_runtime_state = intake_runtime_state(&intake_chain_tasks);
    let provider_registry = ProviderRegistry::from_chain_rpc_urls(&args.chain_rpc_urls)?;
    log_provider_registry("startup", &intake_chain_tasks, &provider_registry);

    info!(
        service = "indexer",
        phase = bigname_domain::bootstrap_phase(),
        manifest_loader_status = bigname_manifests::bootstrap_status(),
        manifests_root = %manifest_runtime_state.manifest_summary.root.display(),
        manifests_status = manifest_runtime_state.manifest_summary.status.as_str(),
        manifest_namespace_count = manifest_runtime_state.manifest_summary.namespace_count,
        manifest_source_family_count = manifest_runtime_state.manifest_summary.source_family_count,
        manifest_count = manifest_runtime_state.manifest_summary.manifest_count,
        manifest_sync_status = manifest_runtime_state.sync_summary.status.as_str(),
        synced_manifest_count = manifest_runtime_state.sync_summary.synced_manifest_count,
        synced_active_manifest_count = manifest_runtime_state.sync_summary.active_manifest_count,
        synced_root_count = manifest_runtime_state.sync_summary.root_count,
        synced_contract_count = manifest_runtime_state.sync_summary.contract_count,
        synced_capability_count = manifest_runtime_state.sync_summary.capability_count,
        synced_discovery_rule_count = manifest_runtime_state.sync_summary.discovery_rule_count,
        removed_manifest_count = manifest_runtime_state.sync_summary.removed_manifest_count,
        cleared_discovery_edge_count = manifest_runtime_state.sync_summary.cleared_discovery_edge_count,
        stored_active_manifest_count = manifest_runtime_state.discovery_admission.active_manifest_count,
        stored_active_root_count = manifest_runtime_state.discovery_admission.active_root_count,
        stored_active_contract_count = manifest_runtime_state.discovery_admission.active_contract_count,
        stored_active_rule_count = manifest_runtime_state.discovery_admission.active_rule_count,
        normalized_event_sync_total_count = manifest_runtime_state.manifest_normalized_event_summary.total_synced_count,
        normalized_event_inserted_total_count = manifest_runtime_state.manifest_normalized_event_summary.total_inserted_count,
        normalized_event_kind_count = manifest_runtime_state.manifest_normalized_event_summary.by_kind.len(),
        source_manifest_updated_event_count = manifest_normalized_event_kind_count(
            &manifest_runtime_state.manifest_normalized_event_summary,
            "SourceManifestUpdated"
        ),
        capability_changed_event_count = manifest_normalized_event_kind_count(
            &manifest_runtime_state.manifest_normalized_event_summary,
            "CapabilityChanged"
        ),
        proxy_implementation_changed_event_count = manifest_normalized_event_kind_count(
            &manifest_runtime_state.manifest_normalized_event_summary,
            "ProxyImplementationChanged"
        ),
        watched_entry_count_total = manifest_runtime_state.watched_contract_summary.source_entry_count,
        watched_manifest_root_entry_count = manifest_runtime_state.watched_contract_summary.manifest_root_count,
        watched_manifest_contract_entry_count = manifest_runtime_state.watched_contract_summary.manifest_contract_count,
        watched_discovery_edge_entry_count = manifest_runtime_state.watched_contract_summary.discovery_edge_count,
        watched_chain_count = manifest_runtime_state.watched_contract_summary.chains.len(),
        watched_runtime_chain_count = watched_chain_plan_state.chain_count,
        watched_runtime_address_count = watched_chain_plan_state.address_count,
        watched_runtime_entry_count = watched_chain_plan_state.entry_count,
        intake_runtime_chain_count = intake_runtime_state.chain_count,
        intake_runtime_address_count = intake_runtime_state.address_count,
        intake_runtime_entry_count = intake_runtime_state.entry_count,
        intake_cold_start_chain_count = intake_runtime_state.cold_start_chain_count,
        intake_resumable_chain_count = intake_runtime_state.resumable_chain_count,
        intake_safe_checkpoint_chain_count = intake_runtime_state.safe_checkpoint_chain_count,
        intake_finalized_checkpoint_chain_count = intake_runtime_state.finalized_checkpoint_chain_count,
        rpc_configured_chain_count = provider_registry.configured_chain_count(),
        watched_plan_refresh_interval_secs = args.poll_interval_secs,
        adapter_status = bigname_adapters::bootstrap_status(),
        poll_interval_secs = args.poll_interval_secs,
        "indexer booted"
    );

    run_poll_loop(
        &pool,
        args.manifests_root,
        manifest_runtime_state,
        intake_chain_tasks,
        &provider_registry,
        args.poll_interval_secs,
    )
    .await
}

async fn run_backfill(args: BackfillArgs) -> Result<()> {
    let range = BackfillBlockRange::new(args.from_block, args.to_block)?;
    let manifest_repository = load_manifest_repository(&args.manifests_root)?;
    let manifest_summary = manifest_repository.summary().clone();
    log_manifest_summary(&manifest_summary);
    ensure_manifest_root_ready(&manifest_summary)?;

    let pool = bigname_storage::connect(&args.database).await?;
    let manifest_runtime_state = build_manifest_runtime_state(&pool, &manifest_repository).await?;
    log_manifest_runtime_state(&manifest_runtime_state);
    log_watched_chain_plan("backfill", &manifest_runtime_state.watched_chain_plan);

    let provider_registry = ProviderRegistry::from_chain_rpc_urls(&args.chain_rpc_urls)?;
    info!(
        service = "indexer",
        command = "backfill",
        chain = %args.chain,
        from_block = range.from_block,
        to_block = range.to_block,
        rpc_configured_chain_count = provider_registry.configured_chain_count(),
        "provider registry loaded for hash-pinned backfill"
    );

    let watched_chain = manifest_runtime_state
        .watched_chain_plan
        .iter()
        .find(|chain| chain.chain == args.chain)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "chain {} is not in the active watched chain plan; refusing backfill of non-watched chain",
                args.chain
            )
        })?;
    let provider = provider_registry.provider_for(&args.chain).ok_or_else(|| {
        anyhow::anyhow!(
            "no RPC provider configured for watched chain {}; pass --chain-rpc-url {}=<url>",
            args.chain,
            args.chain
        )
    })?;

    run_hash_pinned_backfill_range(&pool, watched_chain, provider, range).await?;
    Ok(())
}
