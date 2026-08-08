use clap::{Args, Subcommand};

use super::ResolvedCommand;
use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::BlockRange,
};

#[derive(Clone, Debug, Args)]
pub(super) struct InspectArgs {
    #[arg(long, env = "BIGNAME_DATABASE_URL")]
    database_url: String,

    #[command(subcommand)]
    window: InspectWindow,
}

#[derive(Clone, Debug, Subcommand)]
enum InspectWindow {
    /// Show stored block identities, canonicality labels, and fact counts.
    BlockCanonicality(InspectRangeArgs),
    /// Show stored lineage and optional audited header fields.
    StoredLineage(InspectRangeArgs),
    /// Show retained raw logs with transaction, receipt, and normalized-event context.
    RawEvents(InspectRangeArgs),
}

#[derive(Clone, Debug, Args)]
struct InspectRangeArgs {
    #[arg(long)]
    chain: String,

    #[arg(long)]
    from_block: i64,

    #[arg(long)]
    to_block: i64,
}

pub(super) fn resolve(args: InspectArgs) -> RunnerResult<ResolvedCommand> {
    let (kind, chain_id, range) = match args.window {
        InspectWindow::BlockCanonicality(args) => {
            let (chain_id, range) = resolve_range(args)?;
            (
                crate::inspect::InspectionKind::BlockCanonicality,
                chain_id,
                range,
            )
        }
        InspectWindow::StoredLineage(args) => {
            let (chain_id, range) = resolve_range(args)?;
            (
                crate::inspect::InspectionKind::StoredLineage,
                chain_id,
                range,
            )
        }
        InspectWindow::RawEvents(args) => {
            let (chain_id, range) = resolve_range(args)?;
            (crate::inspect::InspectionKind::RawEvents, chain_id, range)
        }
    };
    Ok(ResolvedCommand::Inspect {
        database_url: args.database_url,
        request: crate::inspect::InspectionRequest {
            kind,
            chain_id,
            range,
        },
    })
}

fn resolve_range(args: InspectRangeArgs) -> RunnerResult<(String, BlockRange)> {
    if args.chain.trim().is_empty() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            "inspect chain must not be empty",
        ));
    }
    Ok((args.chain, BlockRange::new(args.from_block, args.to_block)?))
}
