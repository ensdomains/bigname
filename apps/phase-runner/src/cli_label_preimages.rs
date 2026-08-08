use clap::{Args, Subcommand};

use super::ResolvedCommand;
use crate::error::RunnerResult;

#[derive(Debug, Args)]
pub(super) struct LabelPreimagesArgs {
    #[command(subcommand)]
    command: LabelPreimagesCommand,
}

#[derive(Debug, Subcommand)]
enum LabelPreimagesCommand {
    /// Import verified label preimages from the loaded ENS rainbow table.
    ImportEnsRainbow(ImportEnsRainbowArgs),
}

#[derive(Debug, Args)]
struct ImportEnsRainbowArgs {
    #[arg(long, env = "BIGNAME_DATABASE_URL")]
    database_url: String,

    #[arg(long, value_parser = parse_positive_i64)]
    batch_size: Option<i64>,

    #[arg(long, value_parser = parse_non_negative_i64)]
    limit: Option<i64>,
}

pub(super) fn resolve(args: LabelPreimagesArgs) -> RunnerResult<ResolvedCommand> {
    match args.command {
        LabelPreimagesCommand::ImportEnsRainbow(args) => {
            Ok(ResolvedCommand::LabelPreimagesImportEnsRainbow {
                database_url: args.database_url,
                batch_size: args.batch_size,
                limit: args.limit,
            })
        }
    }
}

fn parse_positive_i64(value: &str) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|error| format!("must be a positive integer: {error}"))?;
    if parsed > 0 {
        Ok(parsed)
    } else {
        Err("must be a positive integer".to_owned())
    }
}

fn parse_non_negative_i64(value: &str) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|error| format!("must be a non-negative integer: {error}"))?;
    if parsed >= 0 {
        Ok(parsed)
    } else {
        Err("must be a non-negative integer".to_owned())
    }
}
