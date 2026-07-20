#[test]
fn auto_normalized_replay_catchup_owns_live_adapter_sync() {
    let auto_with_catchup = IndexerRunMode::new(BackfillAdapterSyncMode::Auto, true);
    assert!(!auto_with_catchup.live_poll_adapter_sync_enabled);
    assert!(auto_with_catchup.live_poll_adapter_sync_after_normalized_replay_catchup);
    assert!(auto_with_catchup.normalized_replay_catchup_enabled);
    assert_eq!(
        auto_with_catchup.bootstrap_watch_scope,
        RuntimeWatchScope::ManifestDeclaredOnly
    );
    assert_eq!(
        auto_with_catchup.startup_backfill_adapter_sync_mode,
        BackfillAdapterSyncMode::RawOnly
    );

    let auto_without_catchup = IndexerRunMode::new(BackfillAdapterSyncMode::Auto, false);
    assert!(auto_without_catchup.live_poll_adapter_sync_enabled);
    assert!(!auto_without_catchup.live_poll_adapter_sync_after_normalized_replay_catchup);
    assert!(!auto_without_catchup.normalized_replay_catchup_enabled);

    let inline = IndexerRunMode::new(BackfillAdapterSyncMode::Inline, true);
    assert!(inline.live_poll_adapter_sync_enabled);
    assert!(!inline.live_poll_adapter_sync_after_normalized_replay_catchup);
    assert!(!inline.normalized_replay_catchup_enabled);
    assert_eq!(
        inline.bootstrap_watch_scope,
        RuntimeWatchScope::ActiveWatchedChain
    );
    assert_eq!(
        inline.startup_backfill_adapter_sync_mode,
        BackfillAdapterSyncMode::Inline
    );
    assert!(inline.broad_runtime_refresh_enabled);
    assert!(!inline.sync_adapter_before_startup_backfill);
    assert!(inline.sync_adapter_after_startup_backfill);

    let raw_only = IndexerRunMode::new(BackfillAdapterSyncMode::RawOnly, false);
    assert!(!raw_only.live_poll_adapter_sync_enabled);
    assert!(!raw_only.live_poll_adapter_sync_after_normalized_replay_catchup);
    assert_eq!(
        raw_only.bootstrap_watch_scope,
        RuntimeWatchScope::ManifestDeclaredOnly
    );
}

#[test]
fn live_tailer_watches_the_active_watched_chain_in_every_adapter_sync_mode() {
    for adapter_sync_mode in [
        BackfillAdapterSyncMode::Auto,
        BackfillAdapterSyncMode::Inline,
        BackfillAdapterSyncMode::RawOnly,
    ] {
        for normalized_replay_catchup_requested in [false, true] {
            let run_mode =
                IndexerRunMode::new(adapter_sync_mode, normalized_replay_catchup_requested);
            assert_eq!(
                run_mode.live_watch_scope,
                RuntimeWatchScope::ActiveWatchedChain,
                "live tailer must watch discovered targets under {adapter_sync_mode:?}"
            );
            assert!(
                run_mode.discovery_refresh_enabled,
                "live tailer must admit newly discovered targets under {adapter_sync_mode:?}"
            );
        }
    }
}

#[test]
fn bootstrap_watch_scope_stays_narrow_while_the_live_tailer_stays_wide() {
    let auto = IndexerRunMode::new(BackfillAdapterSyncMode::Auto, true);
    assert_ne!(auto.bootstrap_watch_scope, auto.live_watch_scope);
    assert_eq!(
        auto.bootstrap_watch_scope,
        RuntimeWatchScope::ManifestDeclaredOnly
    );
    assert_eq!(auto.live_watch_scope, RuntimeWatchScope::ActiveWatchedChain);
}

#[test]
fn adapter_owned_state_syncs_after_bootstrap_when_it_seeds_or_completes_discovery() {
    // `auto` + catch-up: bootstrap is raw-only and live-poll adapter sync is deferred to catch-up,
    // so the one-shot post-bootstrap sync is what materializes discovery edges before the widen.
    let auto_with_catchup = IndexerRunMode::new(BackfillAdapterSyncMode::Auto, true);
    assert!(auto_with_catchup.sync_adapter_after_startup_backfill);
    assert!(!auto_with_catchup.sync_adapter_before_startup_backfill);

    // `auto` without catch-up: live-poll adapter sync stays on and re-derives edges each poll, so
    // no separate post-bootstrap sync is needed.
    let auto_without_catchup = IndexerRunMode::new(BackfillAdapterSyncMode::Auto, false);
    assert!(!auto_without_catchup.sync_adapter_after_startup_backfill);

    // `inline`: a fresh chain has no retained-history proof until bootstrap facts commit, so the
    // broad absence-based sync runs once bootstrap drains, never before it.
    for normalized_replay_catchup_requested in [false, true] {
        let inline = IndexerRunMode::new(
            BackfillAdapterSyncMode::Inline,
            normalized_replay_catchup_requested,
        );
        assert!(!inline.sync_adapter_before_startup_backfill);
        assert!(inline.sync_adapter_after_startup_backfill);
    }

    // `raw-only`: adapter-owned state is intentionally never written at startup.
    for normalized_replay_catchup_requested in [false, true] {
        let raw_only = IndexerRunMode::new(
            BackfillAdapterSyncMode::RawOnly,
            normalized_replay_catchup_requested,
        );
        assert!(!raw_only.sync_adapter_before_startup_backfill);
        assert!(!raw_only.sync_adapter_after_startup_backfill);
    }
}

#[test]
fn only_broad_runtime_refresh_resyncs_adapter_owned_state_on_discovery_refresh() {
    let auto = IndexerRunMode::new(BackfillAdapterSyncMode::Auto, true);
    assert!(auto.discovery_refresh_enabled);
    assert!(!auto.broad_runtime_refresh_enabled);

    let inline = IndexerRunMode::new(BackfillAdapterSyncMode::Inline, false);
    assert!(inline.discovery_refresh_enabled);
    assert!(inline.broad_runtime_refresh_enabled);
}
