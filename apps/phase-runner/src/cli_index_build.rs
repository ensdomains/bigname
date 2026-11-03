//! The session settings `run` and `redo` rebuild the walk index set scripts' dropped
//! `normalized_events` indexes with before Project (`docs/deployment.md`).
use clap::Args;

use crate::walk_index_restore::IndexBuildSettings;

#[derive(Clone, Debug, Args)]
pub(super) struct IndexBuildArgs {
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_RESTORE_MAINTENANCE_WORK_MEM",
        value_parser = memory_size,
        help = "maintenance_work_mem for rebuilding the dropped normalized_events indexes, such \
                as 1GB; the server's value when unset"
    )]
    restore_maintenance_work_mem: Option<String>,

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_RESTORE_PARALLEL_MAINTENANCE_WORKERS",
        value_parser = clap::value_parser!(u16).range(0..=1024),
        help = "max_parallel_maintenance_workers for rebuilding the dropped normalized_events \
                indexes; the server's value when unset"
    )]
    restore_parallel_maintenance_workers: Option<u16>,
}

impl From<IndexBuildArgs> for IndexBuildSettings {
    fn from(args: IndexBuildArgs) -> Self {
        Self {
            maintenance_work_mem: args.restore_maintenance_work_mem,
            max_parallel_maintenance_workers: args.restore_parallel_maintenance_workers,
        }
    }
}

/// A whole number with an optional PostgreSQL memory unit. PostgreSQL checks the range.
fn memory_size(value: &str) -> Result<String, String> {
    let digits = value.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit = &value[digits.len()..];
    if !digits.is_empty()
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && ["", "B", "kB", "MB", "GB", "TB"].contains(&unit)
    {
        Ok(value.to_owned())
    } else {
        Err(format!(
            "{value:?} is not a PostgreSQL memory size such as 512MB or 2GB"
        ))
    }
}
