use anyhow::{Result, ensure};
use bigname_storage::DatabaseConfig;
use clap::Args;

#[derive(Args, Debug)]
pub(crate) struct RepairCoverageRecoveryRearmArgs {
    #[command(flatten)]
    pub(crate) database: DatabaseConfig,
    #[arg(long, env = "BIGNAME_INDEXER_DEPLOYMENT_PROFILE")]
    pub(crate) deployment_profile: String,
    #[arg(long)]
    pub(crate) chain: String,
    #[arg(long = "raw-log-retention-generation")]
    pub(crate) raw_log_retention_generation: i64,
    #[arg(long = "source-family")]
    pub(crate) source_family: String,
    #[arg(long)]
    pub(crate) address: String,
    #[arg(long = "from-block")]
    pub(crate) from_block: i64,
    #[arg(long = "to-block")]
    pub(crate) to_block: i64,
}

pub(crate) async fn run(args: RepairCoverageRecoveryRearmArgs) -> Result<()> {
    let (pool, _rederive_guard) =
        bigname_storage::connect_with_base_normalized_rederive_writer_guard(
            &args.database,
            "bigname-indexer",
        )
        .await?;
    let key = bigname_storage::CoverageRecoveryFailureKey {
        deployment_profile: args.deployment_profile,
        chain_id: args.chain,
        raw_log_retention_generation: args.raw_log_retention_generation,
        source_family: args.source_family,
        emitting_address: args.address.to_ascii_lowercase(),
        required_from_block: args.from_block,
        required_to_block: args.to_block,
    };
    ensure!(
        bigname_storage::rearm_terminal_coverage_recovery_failure(&pool, &key).await?,
        "no terminal automatic coverage-recovery failure matched the exact deployment, chain, generation, source, address, and interval"
    );
    tracing::info!(
        service = "indexer",
        command = "repair coverage-recovery-rearm",
        deployment_profile = %key.deployment_profile,
        chain = %key.chain_id,
        raw_log_retention_generation = key.raw_log_retention_generation,
        source_family = %key.source_family,
        address = %key.emitting_address,
        from_block = key.required_from_block,
        to_block = key.required_to_block,
        "terminal automatic coverage recovery interval re-armed"
    );
    Ok(())
}
