//! The owned key family flags of `run` and `redo`.
use clap::Args;

use crate::project_phase::FamilySettings;

#[derive(Clone, Copy, Debug, Args)]
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
        help = "most interpreted events one rebuild range, or one catalogue window of the \
                composition pass, applies"
    )]
    project_families_max_range_events: u64,
}

impl From<ProjectFamiliesArgs> for FamilySettings {
    fn from(args: ProjectFamiliesArgs) -> Self {
        Self {
            max_blocks_per_run: args.project_families_max_blocks.max(1),
            max_range_events: args.project_families_max_range_events.max(1),
            ..Self::default()
        }
    }
}
