use crate::{backfill::BackfillAdapterSyncMode, runtime::RuntimeWatchScope};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexerRunMode {
    pub(crate) adapter_sync_mode: BackfillAdapterSyncMode,
    /// Target scope for startup bootstrap backfill, where a wide selected-target set turns into
    /// address-filtered range scans and costs provider calls per target.
    pub(crate) bootstrap_watch_scope: RuntimeWatchScope,
    /// Target scope for the live tailer. Live intake fetches every log in a block by block hash and
    /// filters client-side, so a wide scope costs no extra *log* fetches — narrowing it only drops
    /// discovered emitters on the floor. It is not free of all provider calls: the capped raw-code
    /// baseline sweep issues an `eth_getCode` per watched address that still lacks a baseline
    /// observation, so that cost does scale with the scope.
    pub(crate) live_watch_scope: RuntimeWatchScope,
    pub(crate) startup_backfill_adapter_sync_mode: BackfillAdapterSyncMode,
    pub(crate) sync_adapter_before_startup_backfill: bool,
    pub(crate) sync_adapter_after_startup_backfill: bool,
    pub(crate) sync_discovery_adapters_after_startup_backfill: bool,
    pub(crate) normalized_replay_catchup_enabled: bool,
    pub(crate) live_poll_adapter_sync_enabled: bool,
    pub(crate) live_poll_adapter_sync_after_normalized_replay_catchup: bool,
    pub(crate) discovery_refresh_enabled: bool,
    pub(crate) resolver_profile_convergence_enabled: bool,
    pub(crate) broad_runtime_refresh_enabled: bool,
}

impl IndexerRunMode {
    pub(crate) fn new(
        adapter_sync_mode: BackfillAdapterSyncMode,
        normalized_replay_catchup_requested: bool,
    ) -> Self {
        let normalized_replay_catchup_enabled = normalized_replay_catchup_requested
            && adapter_sync_mode == BackfillAdapterSyncMode::Auto;
        let bootstrap_watch_scope = match adapter_sync_mode {
            BackfillAdapterSyncMode::Inline => RuntimeWatchScope::ActiveWatchedChain,
            BackfillAdapterSyncMode::Auto | BackfillAdapterSyncMode::RawOnly => {
                RuntimeWatchScope::ManifestDeclaredOnly
            }
        };
        let live_poll_adapter_sync_enabled = adapter_sync_mode != BackfillAdapterSyncMode::RawOnly
            && !(adapter_sync_mode == BackfillAdapterSyncMode::Auto
                && normalized_replay_catchup_enabled);
        let live_poll_adapter_sync_after_normalized_replay_catchup =
            adapter_sync_mode == BackfillAdapterSyncMode::Auto && normalized_replay_catchup_enabled;

        Self {
            adapter_sync_mode,
            bootstrap_watch_scope,
            live_watch_scope: RuntimeWatchScope::ActiveWatchedChain,
            startup_backfill_adapter_sync_mode: adapter_sync_mode
                .startup_hash_pinned_backfill_mode(),
            // A fresh chain has no retained-history proof until its
            // generation-bound bootstrap facts and coverage are committed.
            // Inline mode therefore performs its broad absence-based sync
            // only after bootstrap drains.
            sync_adapter_before_startup_backfill: false,
            // Preserve inline's broad post-bootstrap pass. Auto uses the
            // focused discovery-family pass below instead of synchronously
            // deriving all seven adapter families. Raw-only never touches
            // adapter-owned state.
            sync_adapter_after_startup_backfill: adapter_sync_mode
                == BackfillAdapterSyncMode::Inline,
            sync_discovery_adapters_after_startup_backfill: adapter_sync_mode
                == BackfillAdapterSyncMode::Auto,
            normalized_replay_catchup_enabled,
            live_poll_adapter_sync_enabled,
            live_poll_adapter_sync_after_normalized_replay_catchup,
            discovery_refresh_enabled: true,
            resolver_profile_convergence_enabled: adapter_sync_mode
                != BackfillAdapterSyncMode::RawOnly,
            broad_runtime_refresh_enabled: adapter_sync_mode == BackfillAdapterSyncMode::Inline,
        }
    }
}
