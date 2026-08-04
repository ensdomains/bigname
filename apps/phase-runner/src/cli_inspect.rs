use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::BlockRange,
};

use super::{InspectArgs, InspectRangeArgs, InspectWindow, ResolvedCommand};

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
