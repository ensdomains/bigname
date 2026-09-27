//! The owned key family flags of `run` and `redo`.
use clap::Args;

use crate::project_phase::FamilySettings;

#[derive(Clone, Copy, Debug, Args)]
pub(super) struct ProjectFamiliesArgs {
    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_PROJECT_FAMILIES",
        default_value_t = true,
        action = clap::ArgAction::Set,
        help = "follow each committed Project batch with the owned key families: true or false"
    )]
    project_families: bool,

    #[arg(
        long,
        env = "BIGNAME_PHASE_RUNNER_PROJECT_FAMILIES_MAX_BLOCKS",
        default_value_t = bigname_project::families::MAX_BLOCKS_PER_RUN,
        help = "most owned key family blocks one runner cycle applies or undoes"
    )]
    project_families_max_blocks: u64,
}

impl From<ProjectFamiliesArgs> for FamilySettings {
    fn from(args: ProjectFamiliesArgs) -> Self {
        Self {
            enabled: args.project_families,
            max_blocks_per_run: args.project_families_max_blocks.max(1),
            ..Self::default()
        }
    }
}
