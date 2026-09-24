use std::sync::Arc;

use super::*;

fn remember(state: &mut State, source: &str, surface: &str) {
    state.restoring_state_key = Some(source.to_owned());
    state.remember_known_surface(surface.to_owned());
    state.restoring_state_key = None;
}

#[test]
fn replacing_surface_sources_preserves_other_sources_and_current_bindings() {
    let mut state = State::new(vec![], vec![]);
    remember(&mut state, "first", "ens:z");
    remember(&mut state, "first", "ens:a");
    remember(&mut state, "first", "ens:z");
    remember(&mut state, "second", "ens:z");
    remember(&mut state, "first", "ens:current");
    state.replace_v2_current_surface(None, Some("ens:current"));
    assert_eq!(state.restored_surface_counts.get("ens:z"), Some(&2));
    assert_eq!(state.restored_surface_counts.get("ens:a"), Some(&1));

    state.replace_restored_surface_source("first");
    state.prune_unbacked_surfaces();
    assert!(!state.known_surfaces.contains("ens:a"));
    assert!(state.known_surfaces.contains("ens:z"));
    assert!(state.known_surfaces.contains("ens:current"));
    assert_eq!(state.restored_surface_counts.get("ens:z"), Some(&1));

    state.replace_restored_surface_source("second");
    state.replace_v2_current_surface(Some("ens:current"), None);
    state.prune_unbacked_surfaces();
    assert!(state.known_surfaces.is_empty());
    assert!(state.restored_surface_counts.is_empty());
}

#[test]
fn speculative_surface_changes_do_not_mutate_the_committed_state() {
    let mut committed = State::new(vec![], vec![]);
    remember(&mut committed, "source", "ens:original");
    let mut candidate = committed.clone();
    assert!(Arc::ptr_eq(
        committed.restored_surface_sources.get("source").unwrap(),
        candidate.restored_surface_sources.get("source").unwrap(),
    ));

    remember(&mut candidate, "source", "ens:new");
    assert!(!committed.known_surfaces.contains("ens:new"));
    assert_eq!(
        committed.restored_surface_counts.get("ens:original"),
        Some(&1)
    );
    assert_eq!(
        candidate.restored_surface_counts.get("ens:original"),
        Some(&1)
    );
    assert_eq!(candidate.restored_surface_counts.get("ens:new"), Some(&1));

    candidate.replace_restored_surface_source("source");
    candidate.prune_unbacked_surfaces();
    assert!(candidate.known_surfaces.is_empty());
    assert!(committed.known_surfaces.contains("ens:original"));
    assert_eq!(
        committed.restored_surface_sources["source"].as_ref(),
        ["ens:original"]
    );
}
