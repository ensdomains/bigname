//! The owned key family flags of `run` and `redo`.
use clap::Args;

use crate::project_phase::FamilySettings;

#[derive(Clone, Debug, Args)]
pub(super) struct ProjectFamiliesArgs {
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_PROJECT_FAMILIES_MAX_BLOCKS",
        default_value_t = bigname_project::families::MAX_BLOCKS_PER_RUN,
        help = "most owned key family blocks one Project batch applies or undoes"
    )]
    project_families_max_blocks: u64,
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_PROJECT_FAMILIES_MAX_RANGE_EVENTS",
        default_value_t = bigname_project::families::MAX_RANGE_EVENTS,
        help = "most interpreted events one rebuild range applies"
    )]
    project_families_max_range_events: u64,
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_PROJECT_FAMILIES_COMPOSE_BLOCK_SPANS",
        default_value_t = bigname_project::families::COMPOSE_BUILDER_BLOCK_SPANS,
        value_parser = block_spans,
        help = "block spans a rebuild's history catalogue build scans the events in, each one \
                transaction"
    )]
    project_families_compose_block_spans: u16,
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_PROJECT_FAMILIES_COMPOSE_KEY_PARTITIONS",
        default_value_t = bigname_project::families::COMPOSE_BUILDER_KEY_PARTITIONS,
        value_parser = key_partitions,
        help = "key partitions of a rebuild's history catalogue build, each one transaction per \
                step, a power of two"
    )]
    project_families_compose_key_partitions: u16,
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_PROJECT_FAMILIES_COMPOSE_WORK_MEM",
        help = "work_mem of each history catalogue build transaction, a Postgres memory value \
                such as 256MB, the server's setting when unset"
    )]
    project_families_compose_work_mem: Option<String>,
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_PROJECT_FAMILIES_COMPOSE_WORKERS",
        default_value_t = bigname_project::families::COMPOSE_WORKERS,
        value_parser = clap::value_parser!(u16).range(1..),
        help = "workers composing a rebuild's composition pass names stream side by side, each \
                on a database connection of its own"
    )]
    project_families_compose_workers: u16,
}

/// The connections `run` gives its pool: two a chain and at least four, plus one for each
/// composition pass worker of each chain, since every chain's rebuild may compose at once.
pub fn run_connections(chains: usize, workers: u16) -> u32 {
    let chains = u32::try_from(chains).unwrap_or(u32::MAX);
    chains
        .saturating_mul(2)
        .max(4)
        .saturating_add(u32::from(workers).saturating_mul(chains))
}

/// The connections `redo` gives its pool: four, plus one for each composition pass worker.
pub fn redo_connections(workers: u16) -> u32 {
    4 + u32::from(workers)
}

fn block_spans(value: &str) -> Result<u16, String> {
    let spans: u16 = value.parse().map_err(|error| format!("{value}: {error}"))?;
    let most = bigname_project::families::MAX_COMPOSE_BUILDER_BLOCK_SPANS;
    if (1..=most).contains(&spans) {
        Ok(spans)
    } else {
        Err(format!("{value} is not from 1 to {most}"))
    }
}

fn key_partitions(value: &str) -> Result<u16, String> {
    let partitions: u16 = value.parse().map_err(|error| format!("{value}: {error}"))?;
    let most = bigname_project::families::MAX_COMPOSE_BUILDER_KEY_PARTITIONS;
    if partitions.is_power_of_two() && partitions <= most {
        Ok(partitions)
    } else {
        Err(format!("{value} is not a power of two from 1 to {most}"))
    }
}

impl From<ProjectFamiliesArgs> for FamilySettings {
    fn from(args: ProjectFamiliesArgs) -> Self {
        Self {
            max_blocks_per_run: args.project_families_max_blocks.max(1),
            max_range_events: args.project_families_max_range_events.max(1),
            compose_block_spans: args.project_families_compose_block_spans,
            compose_key_partitions: args.project_families_compose_key_partitions,
            compose_work_mem: args.project_families_compose_work_mem,
            compose_workers: args.project_families_compose_workers,
            ..Self::default()
        }
    }
}
