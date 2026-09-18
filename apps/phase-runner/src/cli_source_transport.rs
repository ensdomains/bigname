use clap::Args;

#[derive(Clone, Debug, Args)]
pub(super) struct SourceTransportArgs {
    #[arg(long, env = "BIGNAME_DATABASE_URL")]
    pub(super) database_url: String,
    #[arg(long)]
    pub(super) from_source: String,
    #[arg(long)]
    pub(super) to_source: String,
    /// Attest both endpoints belong to the same node, not an independent source.
    #[arg(long, required = true)]
    attest_same_node: bool,
}

use super::{ErrorKind, ResolvedCommand, RewindArgs, RunnerError, RunnerResult};

pub(super) fn resolve_rewind(args: RewindArgs) -> RunnerResult<ResolvedCommand> {
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
