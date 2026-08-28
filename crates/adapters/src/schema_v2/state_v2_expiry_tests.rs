use super::*;

#[test] #[rustfmt::skip]
fn v2_expiry_live_predicate_retires_at_the_exact_boundary() {
    let live = super::super::topology::v2_expiry_is_live; assert!(live(Some(10), 9)); assert!(!live(Some(10), 10)); assert!(!live(Some(10), -1)); assert!(!live(None, 9));
}

#[test] #[rustfmt::skip]
fn v2_direct_leaf_expiry_does_not_expire_its_suffix_anchor() {
    let mut state = anchored_state(); install_token(&mut state, ROOT, "0x01", b"alpha", 10); assert_eq!(state.refresh_dirty_v2_names(9).len(), 1); assert!(state.v2_registry_suffix(ROOT, NAMESPACE, 10).is_some());
    let retired = state.refresh_dirty_v2_names(10); assert_eq!(retired.len(), 1); assert_eq!(retired[0].expiry, Some(10)); assert!(retired[0].previous.is_some());
    assert!(retired[0].previous_shadow.is_none()); assert!(retired[0].current.is_none()); assert!(retired[0].current_shadow.is_none());
}

#[test] #[rustfmt::skip]
fn v2_expiry_crossing_ignores_many_unrelated_future_entries() {
    let mut state = anchored_state(); install_token(&mut state, ROOT, "0x01", b"crossing", 10);
    for ordinal in 2..=512 { install_token(&mut state, ROOT, &format!("0x{ordinal:064x}"), format!("future-{ordinal}").as_bytes(), 10_000); }
    state.refresh_dirty_v2_names(9); super::super::reset_v2_refresh_visits();
    let transitions = state.refresh_dirty_v2_names(10); assert_eq!(super::super::v2_refresh_visits(), 1); assert_eq!(transitions.len(), 1); assert_eq!(transitions[0].token_id, "0x01");
}

#[test] #[rustfmt::skip]
fn v2_detached_resource_expiry_emits_a_bindingless_release() {
    let mut state = State::new(Vec::new(), Vec::new()); state.replace_v2_registration(ROOT, "0x01", Uuid::from_u128(1), NAMESPACE, b"reserved", 10, None); state.attach_v2_unbound_resource(ROOT, "0x01", "resource".to_owned(), Uuid::from_u128(9), None);
    state.set_v2_resolver(ROOT, "0x01", Some("0xresolver".to_owned())); state.set_v2_subregistry(ROOT, "0x01", Some(CHILD.to_owned()));
    state.refresh_dirty_v2_names(9); let transition = state.refresh_dirty_v2_names(10).into_iter().next().expect("reservation retires");
    let interpreted = crate::schema_v2::protocol::v2_boundary_expiration(transition, 10).expect("retirement materializes");
    assert!(interpreted.binding_closures.is_empty());
    assert_eq!(interpreted.events.iter().map(|event| event.event_kind.as_str()).collect::<Vec<_>>(), ["RegistrationReleased", "ResolverChanged", "SubregistryChanged"]);
    assert!(interpreted.events.iter().all(|event| event.logical_name_id.is_none() && event.resource_id == Some(Uuid::from_u128(9))));
    assert_eq!(interpreted.events[0].explicit_before.as_ref().expect("before")["status"], "reserved");
    let retained = state.v2_token(ROOT, "0x01").expect("latent reservation");
    assert_eq!(retained.resolver.as_deref(), Some("0xresolver")); assert_eq!(retained.subregistry.as_deref(), Some(CHILD));
    let mut formerly_named = nested_state(100); formerly_named.link_v2_resource(CHILD, "0x02", "resource".to_owned(), Uuid::from_u128(10), None); formerly_named.set_v2_expiry(CHILD, "0x02", 10);
    formerly_named.refresh_dirty_v2_names(9); formerly_named.set_v2_parent_claim(CHILD, None, b"sub"); formerly_named.refresh_dirty_v2_names(9);
    let detached = formerly_named.refresh_dirty_v2_names(10).into_iter().next().expect("detached named resource retires at own expiry");
    assert!(detached.previous.is_none()); assert_eq!(detached.resource_id, Some(Uuid::from_u128(10))); assert!(formerly_named.v2_token(CHILD, "0x02").is_some_and(|token| token.expiry_retirement_emitted)); let emitted = crate::schema_v2::protocol::v2_boundary_expiration(detached, 10).expect("expiry event"); assert_eq!(emitted.events[0].after_state["source_event"], "RegistryPathExpired");
    formerly_named.set_v2_expiry(CHILD, "0x02", 20); assert!(formerly_named.refresh_dirty_v2_names(11).is_empty()); assert!(formerly_named.refresh_dirty_v2_names(20).into_iter().any(|transition| transition.registry == CHILD));
    let mut ancestor = nested_state(10); ancestor.link_v2_resource(CHILD, "0x02", "resource".to_owned(), Uuid::from_u128(11), None); ancestor.set_v2_expiry(CHILD, "0x02", 20); ancestor.refresh_dirty_v2_names(9); assert!(ancestor.refresh_dirty_v2_names(10).iter().any(|transition| transition.registry == CHILD)); assert!(ancestor.refresh_dirty_v2_names(20).iter().all(|transition| transition.registry != CHILD)); ancestor.set_v2_expiry(CHILD, "0x02", 30); assert!(ancestor.refresh_dirty_v2_names(21).is_empty()); assert!(ancestor.refresh_dirty_v2_names(30).iter().all(|transition| transition.registry != CHILD));
}

#[test] #[rustfmt::skip]
fn v2_shadow_expiry_emits_a_registration_release_without_a_binding() {
    let mut state = anchored_state(); state.replace_v2_registration(ROOT, "0x01", Uuid::from_u128(1), NAMESPACE, &[0xff], 10, None);
    state.refresh_dirty_v2_names(9); let transition = state.refresh_dirty_v2_names(10).into_iter().next().expect("shadow retires");
    assert!(transition.previous.is_none()); assert!(transition.previous_shadow.is_some());
    let interpreted = crate::schema_v2::protocol::v2_boundary_expiration(transition, 10).expect("retirement materializes");
    assert!(interpreted.binding_closures.is_empty()); assert_eq!(interpreted.events.len(), 1);
    assert_eq!(interpreted.events[0].event_kind, "RegistrationReleased");
}

#[test] #[rustfmt::skip]
fn v2_same_token_renewal_requeues_expiry_and_revives_the_surface() {
    let mut state = anchored_state(); install_token(&mut state, ROOT, "0x01", b"alpha", 10);
    state.refresh_dirty_v2_names(9); state.refresh_dirty_v2_names(10);
    assert!(state.v2_token(ROOT, "0x01").is_some_and(|token| token.name.is_none()));
    state.set_v2_expiry(ROOT, "0x01", 20); let revived = state.refresh_dirty_v2_names(11);
    assert_eq!(revived.len(), 1); assert!(revived[0].current.is_some());
    assert!(state.v2_expiries.contains(&(20, format!("{ROOT}:0x01"))));
    assert!(!state.v2_expiries.contains(&(10, format!("{ROOT}:0x01"))));
}

#[test] #[rustfmt::skip]
fn v2_version_bumped_replacement_does_not_copy_latent_pointers() {
    let mut state = anchored_state(); install_token(&mut state, ROOT, "0x01", b"alpha", 10);
    state.set_v2_resolver(ROOT, "0x01", Some("0xresolver".to_owned())); state.set_v2_subregistry(ROOT, "0x01", Some(CHILD.to_owned())); state.refresh_dirty_v2_names(10);
    install_token(&mut state, ROOT, "0x02", b"alpha", 20); let replacement = state.v2_token(ROOT, "0x02").expect("replacement");
    assert!(replacement.resolver.is_none()); assert!(replacement.subregistry.is_none());
}

#[test] #[rustfmt::skip]
fn v2_restore_keeps_latent_pointers_for_projection_only_expiry_events() {
    let scope = Some(format!("{ROOT}:-:0x01:-:RegistryPathExpired")); let mut retained = retained_token_events(100);
    retained.extend([
        prior_event("resolver-set", "ResolverChanged", scope.clone(), Some(Uuid::from_u128(99)), json!({"source_event":"ResolverUpdated","token_id":"0x01","resolver":"0xresolver"})),
        prior_event("subregistry-set", "SubregistryChanged", scope.clone(), Some(Uuid::from_u128(99)), json!({"source_event":"SubregistryUpdated","token_id":"0x01","subregistry":CHILD})),
        prior_event("resolver-expired", "ResolverChanged", scope.clone(), Some(Uuid::from_u128(99)), json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","token_id":"0x01","resolver":null})),
        prior_event("subregistry-expired", "SubregistryChanged", scope, Some(Uuid::from_u128(99)), json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","token_id":"0x01","subregistry":null})),
    ]);
    let restored = State::new(retained, anchors()); let token = restored.v2_token(ROOT, "0x01").expect("retained token");
    assert_eq!(token.resolver.as_deref(), Some("0xresolver")); assert_eq!(token.subregistry.as_deref(), Some(CHILD));
}

#[test] #[rustfmt::skip]
fn v2_restore_applies_raw_pointer_null_events_destructively() {
    let scope = Some(format!("{ROOT}:-:0x01:-:ResolverUpdated")); let mut retained = retained_token_events(100);
    retained.extend([
        prior_event("resolver-set", "ResolverChanged", scope.clone(), Some(Uuid::from_u128(99)), json!({"source_event":"ResolverUpdated","token_id":"0x01","resolver":"0xresolver"})),
        prior_event("resolver-clear", "ResolverChanged", scope, Some(Uuid::from_u128(99)), json!({"source_event":"ResolverUpdated","token_id":"0x01","resolver":null})),
    ]);
    assert!(State::new(retained, anchors()).v2_token(ROOT, "0x01").is_some_and(|token| token.resolver.is_none()));
}
