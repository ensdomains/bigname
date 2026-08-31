use super::State;
use crate::schema_v2::model::PriorEventInput;
use imbl::{ordmap::OrdMap, ordset::OrdSet};
use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;
const ROOT: &str = "0x0000000000000000000000000000000000000042";
const CHILD: &str = "0x0000000000000000000000000000000000000043";
const THIRD: &str = "0x0000000000000000000000000000000000000044";
const NEST_ROOT: &str = "0x0000000000000000000000000000000000000050";
const NEST: &str = "0x0000000000000000000000000000000000000051";
const NAMESPACE: &str = "ens";
#[path = "state_v2_expiry_tests.rs"]
mod expiry_tests;
#[path = "state_v2_pointer_tests.rs"]
mod pointer_tests;
#[test]
fn v2_named_expiry_retains_registration_state() {
    let mut state = anchored_state();
    install_token(&mut state, ROOT, "0x01", b"alpha", 10);
    state.link_v2_resource(
        ROOT,
        "0x01",
        "resource".to_owned(),
        Uuid::from_u128(9),
        None,
    );
    state.refresh_dirty_v2_names(9);
    let retired = state
        .refresh_dirty_v2_names(10)
        .into_iter()
        .next()
        .expect("named registration retires");
    assert!(retired.previous.is_some());
    assert!(retired.registration.is_some());
    let retained = state
        .v2_token(ROOT, "0x01")
        .expect("expired token remains retained");
    assert!(retained.name.is_none());
    assert!(retained.registration.is_some());
}
#[test]
fn v2_dirty_refresh_deduplicates_one_token_and_isolates_irrelevant_tokens() {
    let mut state = anchored_state();
    install_token(&mut state, ROOT, "0x01", b"alpha", 100);
    install_token(&mut state, ROOT, "0x02", b"beta", 100);
    state.refresh_dirty_v2_names(1);
    super::reset_v2_refresh_visits();
    state.set_v2_resolver(ROOT, "0x01", Some("0xresolver".to_owned()));
    state.set_v2_resolver(ROOT, "0x01", Some("0xresolver".to_owned()));
    let transitions = state.refresh_dirty_v2_names(2);
    assert!(transitions.is_empty() && state.v2_token(ROOT, "0x02").is_some());
    assert_eq!(super::v2_refresh_visits(), 1);
    assert_v2_indexes_are_derived(&state);
}
#[test]
fn v2_targeted_invalidation_matches_the_former_full_walk() {
    assert_targeted_refresh_matches_full_walk(reserved_state(), |state| {
        state.attach_v2_unbound_resource(
            ROOT,
            "0x01",
            "0x99".to_owned(),
            Uuid::from_u128(99),
            Some(Uuid::from_u128(100)),
        );
    });
    assert_targeted_refresh_matches_full_walk(reserved_resource_state(), |state| {
        install_token(state, ROOT, "0x01", b"alpha", 200);
    });
    assert_targeted_refresh_matches_full_walk(nested_state(100), |state| {
        state.set_v2_parent_claim(CHILD, None, b"sub");
    });
    assert_targeted_refresh_matches_full_walk(nested_state(100), |state| {
        state.release_v2_token(ROOT, "0x01");
    });
    // Both defensive replacement branches are constructible through replace_v2_registration:
    // relabeling the same live token replaces its old subregistry-bearing state, while minting a
    // different token for the same live label displaces the old token.
    assert_targeted_refresh_matches_full_walk(nested_state(100), |state| {
        install_token(state, ROOT, "0x01", b"other", 100);
    });
    assert_targeted_refresh_matches_full_walk(nested_state(100), |state| {
        install_token(state, ROOT, "0x09", b"sub", 100);
    });
}
#[test]
fn v2_duplicate_surface_refresh_is_visit_set_invariant() {
    // Two registries anchored to the same suffix each hold a registered, resource-linked token
    // labeled "alpha", so both compute the same logical name. Dirtying only one co-holder must
    // elect the same active resource as a full walk.
    assert_targeted_refresh_matches_full_walk(duplicate_surface_state(), |state| {
        state.set_v2_resolver(ROOT, "0x01", Some("0xresolver".to_owned()));
    });
    // Releasing one co-holder must hand the surface's active resource to the survivor.
    assert_targeted_refresh_matches_full_walk(duplicate_surface_state(), |state| {
        state.release_v2_token(CHILD, "0x01");
    });
}
#[test]
fn v2_contested_surface_release_keeps_the_max_key_holder_resource() {
    // Three registries anchored to the same suffix co-hold "alpha", giving the surface three
    // asserting holders with distinct `emitter:token_id` keys. The election rule is absolute: the
    // greatest token key among registered, resource-linked holders wins. Releasing the middle-key
    // holder must leave the max-key holder's resource active — a min-key election would hand the
    // surface to the smallest survivor instead.
    let mut state = contested_surface_state(&[ROOT, CHILD, THIRD]);
    state.refresh_dirty_v2_names(1);
    let name = state
        .v2_token(THIRD, "0x01")
        .and_then(|token| token.name)
        .expect("max-key co-holder names the contested surface");
    state
        .release_v2_token(CHILD, "0x01")
        .expect("middle-key co-holder release");
    state.refresh_dirty_v2_names(2);
    assert_eq!(
        state.name_link_by_namehash(NAMESPACE, &name.namehash),
        Some((name.logical_name_id, Some(Uuid::from_u128(3))))
    );
    assert_v2_indexes_are_derived(&state);
}
#[test]
fn v2_changed_away_winner_hands_the_surface_to_the_surviving_holder() {
    // A registry anchored at "eth" holds "alpha" via one resource, contested by a claim-path
    // registry (anchored at the namespace root, its "eth" token claimed as the nested registry's
    // parent) whose greater-key "alpha" holder owns the election. Dropping the parent claim
    // changes the winner's computed name away from the surface; the refresh that visits only the
    // departing holder must re-elect the survivor's resource rather than leave the surface with
    // no active resource.
    let mut state = claim_path_contested_state();
    state.refresh_dirty_v2_names(1);
    let name = state
        .v2_token(NEST, "0x01")
        .and_then(|token| token.name)
        .expect("claim-path co-holder names the contested surface");
    assert_eq!(
        state.name_link_by_namehash(NAMESPACE, &name.namehash),
        Some((name.logical_name_id.clone(), Some(Uuid::from_u128(2))))
    );
    state.set_v2_parent_claim(NEST, None, b"eth");
    let transitions = state.refresh_dirty_v2_names(2);
    assert!(transitions.iter().any(|transition| {
        transition.registry == ROOT
            && transition.previous == transition.current
            && transition
                .current
                .as_ref()
                .is_some_and(|current| current.logical_name_id == name.logical_name_id)
    }));
    assert_eq!(
        state.name_link_by_namehash(NAMESPACE, &name.namehash),
        Some((name.logical_name_id, Some(Uuid::from_u128(1))))
    );
    assert_v2_indexes_are_derived(&state);
}

#[test]
fn v2_contested_surface_expiry_reasserts_the_surviving_holder() {
    let mut state = claim_path_contested_state();
    state.set_v2_expiry(NEST_ROOT, "0x01", 2);
    state.refresh_dirty_v2_names(1);
    let survivor = state
        .v2_token(ROOT, "0x01")
        .and_then(|token| token.name)
        .expect("surviving holder names the contested surface");

    let transitions = state.refresh_dirty_v2_names(2);

    assert!(transitions.iter().any(|transition| {
        transition.registry == ROOT
            && transition.previous == transition.current
            && transition.current.as_ref() == Some(&survivor)
    }));
}
fn assert_targeted_refresh_matches_full_walk(mut baseline: State, mutate: impl Fn(&mut State)) {
    baseline.refresh_dirty_v2_names(1);
    let mut targeted = baseline.clone();
    let mut full_walk = baseline;
    mutate(&mut targeted);
    mutate(&mut full_walk);
    let targeted_transitions = targeted.refresh_dirty_v2_names(2);
    let full_walk_transitions = full_walk.refresh_all_v2_names(2);
    assert_eq!(targeted_transitions, full_walk_transitions);
    assert_eq!(targeted, full_walk);
    assert_v2_indexes_are_derived(&targeted);
}
#[test]
fn v2_dirty_drain_emits_transitions_in_ascending_token_key_order() {
    let mut state = anchored_state();
    install_token(&mut state, ROOT, "0x02", b"beta", 100);
    install_token(&mut state, ROOT, "0x01", b"alpha", 100);

    let transitions = state.refresh_dirty_v2_names(1);
    let keys = transitions
        .iter()
        .map(|transition| format!("{}:{}", transition.registry, transition.token_id))
        .collect::<Vec<_>>();
    assert_eq!(keys, [format!("{ROOT}:0x01"), format!("{ROOT}:0x02")]);
}
#[test]
fn v2_expiry_crossing_refreshes_descendants_without_a_token_event() {
    let mut state = nested_state(10);
    state.refresh_dirty_v2_names(9);
    let child_before = state
        .v2_token(CHILD, "0x02")
        .and_then(|token| token.name)
        .expect("child is named before the parent label expires");
    super::reset_v2_refresh_visits();
    let transitions = state.refresh_dirty_v2_names(10);
    assert_eq!(super::v2_refresh_visits(), 2);
    assert!(transitions.iter().any(|transition| {
        transition.registry == CHILD
            && transition.previous.as_ref() == Some(&child_before)
            && transition.current.is_none()
    }));
    assert!(
        state
            .v2_token(CHILD, "0x02")
            .is_some_and(|token| token.name.is_none())
    );
    assert_v2_indexes_are_derived(&state);
}
#[test]
fn v2_restore_consumes_crossed_expiries_before_retaining_the_boundary_clock() {
    let mut state = nested_state(10);
    state.link_v2_resource(CHILD, "0x02", "0x99".to_owned(), Uuid::from_u128(99), None);
    state.refresh_dirty_v2_names(9);
    let child_before = state
        .v2_token(CHILD, "0x02")
        .and_then(|token| token.name)
        .expect("child is named before the parent label expires");

    state.finish_prior_event_restore(Some(10));

    assert_eq!(state.latest_v2_timestamp, Some(10));
    assert!(
        state
            .v2_token(CHILD, "0x02")
            .is_some_and(|token| token.name.is_none())
    );
    assert!(
        !state
            .active_resources
            .contains_key(&child_before.logical_name_id)
    );
    assert_v2_indexes_are_derived(&state);
}
#[test]
fn v2_reverse_indexes_resolve_hits_misses_regeneration_and_release() {
    let mut state = anchored_state();
    install_token(&mut state, ROOT, "0x01", b"alpha", 100);
    for ordinal in 2..130 {
        install_token(
            &mut state,
            ROOT,
            &format!("0x{ordinal:02x}"),
            format!("unrelated-{ordinal}").as_bytes(),
            100,
        );
    }
    state.refresh_dirty_v2_names(1);
    state.link_v2_resource(
        ROOT,
        "0x01",
        "0x99".to_owned(),
        Uuid::from_u128(99),
        Some(Uuid::from_u128(100)),
    );
    let logical_name_id = name_id(&state, ROOT, "0x01");
    super::reset_v2_lookup_visits();
    assert!(has_upstream(&state, "0x99"));
    assert!(has_name(
        &state,
        "0x01",
        &logical_name_id.to_ascii_uppercase()
    ));
    assert!(!has_upstream(&state, "0xmissing"));
    assert_eq!(super::v2_lookup_visits(), 2);
    state
        .regenerate_v2_token(ROOT, "0x01", "0x02")
        .expect("token regeneration");
    assert!(!has_name(&state, "0x01", &logical_name_id));
    assert!(has_name(&state, "0x02", &logical_name_id));
    assert!(has_upstream(&state, "0x99"));
    state.release_v2_token(ROOT, "0x02").expect("token release");
    assert!(!has_upstream(&state, "0x99") && !has_name(&state, "0x02", &logical_name_id));
    assert_v2_indexes_are_derived(&state);
}
#[test]
fn v2_reserved_resource_is_indexed_without_an_active_binding_and_survives_claim() {
    let mut state = reserved_resource_state();
    let reserved = state.v2_token(ROOT, "0x01").expect("reserved token state");
    let name = reserved.name.as_ref().expect("reserved name facts");
    assert_v2_indexes_are_derived(&state);
    assert!(has_upstream(&state, "0x99"));
    assert_eq!(
        state.name_link_by_namehash(NAMESPACE, &name.namehash),
        Some((name.logical_name_id.clone(), None))
    );
    install_token(&mut state, ROOT, "0x01", b"alpha", 200);
    state.refresh_dirty_v2_names(3);
    let claimed = state.v2_token(ROOT, "0x01").expect("claimed token state");
    assert_eq!(claimed.resource_id, reserved.resource_id);
    assert_eq!(claimed.token_lineage_id, reserved.token_lineage_id);
    assert_eq!(
        state.name_link_by_namehash(NAMESPACE, &name.namehash),
        Some((name.logical_name_id.clone(), Some(Uuid::from_u128(99))))
    );
    assert_v2_indexes_are_derived(&state);
}
#[test]
fn v2_restore_rebuilds_indexes_and_reorg_histories_remove_then_restore_mappings() {
    let retained = retained_token_events(100);
    let restored = State::new(retained.clone(), anchors());
    let logical_name_id = name_id(&restored, ROOT, "0x01");
    assert!(has_upstream(&restored, "0x99") && has_name(&restored, "0x01", &logical_name_id));
    assert_v2_indexes_are_derived(&restored);
    let removed_history = State::new(Vec::new(), anchors());
    assert!(!has_upstream(&removed_history, "0x99"));
    let restored_again = State::new(retained, anchors());
    assert_eq!(restored_again, restored);
    assert_v2_indexes_are_derived(&restored_again);
}
fn retained_token_events(expiry: u64) -> Vec<PriorEventInput> {
    vec![
        prior_event(
            "registration",
            "RegistrationGranted",
            Some(format!("{ROOT}:-:0x01:-:LabelRegistered")),
            None,
            json!({
                "source_event":"LabelRegistered",
                "token_id":"0x01",
                "raw_label_hex":alloy_primitives::hex::encode(b"alpha"),
                "expiry":expiry,
            }),
        ),
        prior_event(
            "resource",
            "TokenResourceLinked",
            Some(format!("{ROOT}:-:0x01:-:TokenResource")),
            Some(Uuid::from_u128(99)),
            json!({
                "source_event":"TokenResource",
                "token_id":"0x01",
                "upstream_resource":"0x99",
                "token_lineage_id":Uuid::from_u128(100).to_string(),
            }),
        ),
    ]
}

#[test]
fn production_shaped_v2_refresh_visits_only_the_dirty_token() {
    const TOKENS: usize = 4_096;
    let mut state = anchored_state();
    for ordinal in 0..TOKENS {
        install_token(
            &mut state,
            ROOT,
            &format!("0x{ordinal:064x}"),
            format!("name-{ordinal}").as_bytes(),
            10_000,
        );
    }
    super::reset_v2_refresh_visits();
    state.refresh_dirty_v2_names(1);
    let full_walk = super::v2_refresh_visits();
    assert_eq!(full_walk, TOKENS);
    super::reset_v2_refresh_visits();
    state.set_v2_resolver(
        ROOT,
        &format!("0x{:064x}", TOKENS / 2),
        Some("0xresolver".to_owned()),
    );
    state.refresh_dirty_v2_names(2);
    let targeted = super::v2_refresh_visits();
    eprintln!(
        "ENSv2 refresh perf trace: tokens={TOKENS}, before_full_walk={full_walk}, dirty=1, after_visited={targeted}"
    );
    assert_eq!(targeted, 1);
}
fn anchored_state() -> State {
    State::new(Vec::new(), anchors())
}
fn has_upstream(state: &State, upstream_resource: &str) -> bool {
    state
        .v2_token_by_upstream_resource(ROOT, upstream_resource)
        .is_ok_and(|token| token.is_some())
}
fn has_name(state: &State, token_id: &str, logical_name_id: &str) -> bool {
    state
        .v2_token_for_logical_name(token_id, logical_name_id)
        .is_ok_and(|token| token.is_some())
}
fn name_id(state: &State, registry: &str, token_id: &str) -> String {
    state
        .v2_token(registry, token_id)
        .and_then(|token| token.name)
        .expect("anchored token name")
        .logical_name_id
}
fn anchors() -> Vec<(String, String, Vec<String>)> {
    vec![(
        ROOT.to_owned(),
        NAMESPACE.to_owned(),
        vec!["eth".to_owned()],
    )]
}
fn duplicate_surface_state() -> State {
    contested_surface_state(&[ROOT, CHILD])
}
/// One registered, resource-linked "alpha" holder per registry, all anchored to the same suffix,
/// so every holder computes the same surface. Holder `n` (in registry order) carries resource
/// `Uuid::from_u128(n + 1)`.
fn contested_surface_state(registries: &[&str]) -> State {
    let mut state = State::new(
        Vec::new(),
        registries
            .iter()
            .map(|address| {
                (
                    (*address).to_owned(),
                    NAMESPACE.to_owned(),
                    vec!["eth".to_owned()],
                )
            })
            .collect(),
    );
    for (ordinal, registry) in registries.iter().enumerate() {
        install_token(&mut state, registry, "0x01", b"alpha", 100);
        state.link_v2_resource(
            registry,
            "0x01",
            format!("0x{:02x}", 0xaa + ordinal),
            Uuid::from_u128(ordinal as u128 + 1),
            None,
        );
    }
    state
}
fn claim_path_contested_state() -> State {
    let mut state = State::new(
        Vec::new(),
        vec![
            (
                ROOT.to_owned(),
                NAMESPACE.to_owned(),
                vec!["eth".to_owned()],
            ),
            (NEST_ROOT.to_owned(), NAMESPACE.to_owned(), Vec::new()),
        ],
    );
    install_token(&mut state, ROOT, "0x01", b"alpha", 100);
    state.link_v2_resource(ROOT, "0x01", "0xaa".to_owned(), Uuid::from_u128(1), None);
    install_token(&mut state, NEST_ROOT, "0x01", b"eth", 100);
    state.set_v2_subregistry(NEST_ROOT, "0x01", Some(NEST.to_owned()));
    state.set_v2_parent_claim(NEST, Some(NEST_ROOT.to_owned()), b"eth");
    install_token(&mut state, NEST, "0x01", b"alpha", 100);
    state.link_v2_resource(NEST, "0x01", "0xbb".to_owned(), Uuid::from_u128(2), None);
    state
}
fn nested_state(parent_expiry: u64) -> State {
    let mut state = anchored_state();
    install_token(&mut state, ROOT, "0x01", b"sub", parent_expiry);
    state.set_v2_subregistry(ROOT, "0x01", Some(CHILD.to_owned()));
    state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), b"sub");
    install_token(&mut state, CHILD, "0x02", b"leaf", 100);
    state
}
fn reserved_state() -> State {
    let mut state = anchored_state();
    state.replace_v2_registration(
        ROOT,
        "0x01",
        Uuid::from_u128(1),
        NAMESPACE,
        b"alpha",
        100,
        None,
    );
    state.refresh_dirty_v2_names(1);
    state
}
fn reserved_resource_state() -> State {
    let mut state = reserved_state();
    state.attach_v2_unbound_resource(
        ROOT,
        "0x01",
        "0x99".to_owned(),
        Uuid::from_u128(99),
        Some(Uuid::from_u128(100)),
    );
    state.refresh_dirty_v2_names(2);
    state
}
fn install_token(state: &mut State, emitter: &str, token_id: &str, raw_label: &[u8], expiry: u64) {
    state.replace_v2_registration(
        emitter,
        token_id,
        Uuid::from_u128(1),
        NAMESPACE,
        raw_label,
        expiry,
        Some(json!({"registrant":"0xowner", "expiry":expiry})),
    );
}
fn prior_event(
    retained_state_key: &str,
    event_kind: &str,
    state_scope: Option<String>,
    resource_id: Option<Uuid>,
    after_state: serde_json::Value,
) -> PriorEventInput {
    PriorEventInput {
        retained_state_key: retained_state_key.to_owned(),
        chain_id: "1".to_owned(),
        namespace: NAMESPACE.to_owned(),
        logical_name_id: None,
        resource_id,
        event_kind: event_kind.to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        manifest_version: 1,
        source_manifest_id: Some(1),
        state_scope,
        block_timestamp: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
        after_state,
    }
}
fn assert_v2_indexes_are_derived(state: &State) {
    let mut upstream = OrdMap::<(String, String), OrdSet<String>>::new();
    let mut names = OrdMap::<(String, String), OrdSet<String>>::new();
    let mut current_names = OrdMap::<String, OrdSet<String>>::new();
    let mut expiries = OrdSet::new();
    let mut resolver_tokens = OrdMap::<(String, String), OrdSet<String>>::new();
    let mut resolver_aliases = OrdMap::<(String, String), OrdSet<(String, String)>>::new();
    let mut subregistry_tokens = OrdMap::<(String, String), OrdSet<String>>::new();
    for (token_key, token) in &state.v2_tokens {
        let (emitter, token_id) = token_key
            .rsplit_once(':')
            .expect("retained ENSv2 token key");
        if let Some(upstream_resource) = token.upstream_resource.as_ref() {
            upstream
                .entry((emitter.to_owned(), upstream_resource.clone()))
                .or_default()
                .insert(token_key.clone());
        }
        for logical_name_id in super::v2_index::token_name_ids(token) {
            names
                .entry((token_id.to_owned(), logical_name_id))
                .or_default()
                .insert(token_key.clone());
        }
        if let Some(name) = token.name.as_ref() {
            current_names
                .entry(name.logical_name_id.clone())
                .or_default()
                .insert(token_key.clone());
        }
        if let Some(expiry) = token.expiry {
            expiries.insert((expiry, token_key.clone()));
        }
        if token.resolver.is_some() {
            resolver_tokens
                .entry((
                    emitter.to_owned(),
                    super::v2_pointers::resolver_observation_id(token_id),
                ))
                .or_default()
                .insert(token_id.to_owned());
        }
        if token.subregistry.is_some() {
            subregistry_tokens
                .entry((
                    emitter.to_owned(),
                    super::v2_pointers::resolver_observation_id(token_id),
                ))
                .or_default()
                .insert(token_id.to_owned());
        }
        for alias in &token.resolver_discovery_aliases {
            resolver_aliases
                .entry((
                    emitter.to_owned(),
                    super::v2_pointers::resolver_observation_id(alias),
                ))
                .or_default()
                .insert((token_id.to_owned(), alias.clone()));
        }
    }
    assert_eq!(state.v2_token_by_upstream_resource_index, upstream);
    assert_eq!(state.v2_token_by_name_index, names);
    assert_eq!(state.v2_tokens_by_current_name_index, current_names);
    assert_eq!(state.v2_expiries, expiries);
    assert_eq!(state.v2_resolver_tokens_by_observation, resolver_tokens);
    assert_eq!(state.v2_resolver_aliases_by_observation, resolver_aliases);
    assert_eq!(
        state.v2_subregistry_tokens_by_observation,
        subregistry_tokens
    );
}
