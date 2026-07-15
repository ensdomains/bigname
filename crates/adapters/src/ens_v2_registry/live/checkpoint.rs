mod payload;
mod persistence;

pub(super) use persistence::{
    LiveRegistryReplayCheckpointHeader, LiveRegistryReplayCheckpointLoad,
    clear_live_registry_replay_checkpoint, load_live_registry_replay_checkpoint,
    load_live_registry_replay_checkpoint_header, stage_live_registry_replay_checkpoint,
};
pub(in crate::ens_v2_registry) use persistence::{
    StagedLiveRegistryReplayCheckpoint, clear_live_registry_replay_checkpoints_for_chain,
    finalize_live_registry_replay_checkpoint,
};

pub(in crate::ens_v2_registry) const LIVE_REGISTRY_REPLAY_CHECKPOINT_ADAPTER: &str =
    "ens_v2_registry_resource_surface";
pub(in crate::ens_v2_registry) const LIVE_REGISTRY_REPLAY_CHECKPOINT_CURSOR_KIND: &str =
    "live_poll";
pub(in crate::ens_v2_registry) const LIVE_REGISTRY_REPLAY_CHECKPOINT_SCOPE: &str =
    "selected_path_state_v1";
