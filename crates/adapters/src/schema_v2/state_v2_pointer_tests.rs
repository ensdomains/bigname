use super::*;

#[test]
fn v2_subregistry_holder_lookup_is_constant_per_versioned_token() {
    const PREFIX: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mut state = anchored_state();
    let mut token_ids = Vec::new();
    for version in 0..64_u32 {
        let token_id = format!("0x{PREFIX}{version:08x}");
        let label = format!("label-{version}");
        install_token(&mut state, ROOT, &token_id, label.as_bytes(), 100);
        token_ids.push(token_id);
    }
    state.set_v2_subregistry(
        ROOT,
        token_ids.last().expect("at least one token ID"),
        Some(CHILD.to_owned()),
    );
    super::super::v2_pointers::reset_v2_subregistry_lookup_visits();
    for token_id in &token_ids {
        assert_eq!(
            state.v2_subregistry_reassertion_target(ROOT, token_id),
            Some(CHILD.to_owned())
        );
    }
    assert_eq!(
        super::super::v2_pointers::v2_subregistry_lookup_visits(),
        64,
        "each observation_key lookup must inspect at most one resident index entry"
    );
    assert_v2_indexes_are_derived(&state);
}

#[test]
fn v2_subregistry_holder_lookup_counts_the_index_members_it_examines() {
    const FIRST: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";
    const SECOND: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000002";
    let mut state = anchored_state();
    install_token(&mut state, ROOT, FIRST, b"first", 100);
    install_token(&mut state, ROOT, SECOND, b"second", 100);
    state.set_v2_subregistry(ROOT, FIRST, Some(CHILD.to_owned()));
    state.set_v2_subregistry(ROOT, SECOND, Some("0xother-child".to_owned()));

    super::super::v2_pointers::reset_v2_subregistry_lookup_visits();
    assert_eq!(
        state.v2_subregistry_reassertion_target(ROOT, FIRST),
        Some("0xother-child".to_owned()),
        "the greatest fixed-width token ID deterministically supplies the survivor target"
    );
    assert_eq!(
        super::super::v2_pointers::v2_subregistry_lookup_visits(),
        2,
        "the work counter must measure the resident index members, not query calls"
    );
}

#[test]
fn v2_resolver_observation_index_moves_and_queries_only_candidate_keys() {
    const OLD: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";
    const OTHER_OLD: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000002";
    const ALIAS: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000002";
    const NEW: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000001";
    const OTHER_NEW: &str = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccc00000001";
    let mut state = anchored_state();
    install_token(&mut state, ROOT, OLD, b"alpha", 100);
    state.set_v2_resolver(ROOT, OLD, Some("0xresolver".to_owned()));
    install_token(&mut state, ROOT, OTHER_OLD, b"beta", 100);
    state.set_v2_resolver(ROOT, OTHER_OLD, Some("0xother".to_owned()));
    install_token(&mut state, ROOT, NEW, b"gamma", 100);
    state.set_v2_resolver(ROOT, NEW, Some("0xdisplaced".to_owned()));
    assert_eq!(
        state.live_v2_resolver_tokens_sharing(
            ROOT,
            &std::collections::BTreeSet::from([ALIAS.into()])
        ),
        std::collections::BTreeSet::from([OLD.into(), OTHER_OLD.into()]),
    );
    state.regenerate_v2_token(ROOT, OLD, NEW).unwrap();
    state
        .regenerate_v2_token(ROOT, OTHER_OLD, OTHER_NEW)
        .unwrap();
    assert_eq!(
        state.live_v2_resolver_tokens_sharing(
            ROOT,
            &std::collections::BTreeSet::from([ALIAS.into()]),
        ),
        std::collections::BTreeSet::from([OLD.into(), OTHER_OLD.into()]),
    );
    assert_eq!(
        state
            .live_v2_resolver_tokens_sharing(ROOT, &std::collections::BTreeSet::from([NEW.into()])),
        std::collections::BTreeSet::from([NEW.into()]),
    );
    state.set_v2_resolver(ROOT, NEW, None);
    assert_eq!(
        state.live_v2_resolver_tokens_sharing(
            ROOT,
            &std::collections::BTreeSet::from([ALIAS.into()]),
        ),
        std::collections::BTreeSet::from([OTHER_OLD.into()]),
    );
    state.release_v2_token(ROOT, OTHER_NEW);
    assert!(state.v2_resolver_tokens_by_observation.is_empty());
    assert!(state.v2_resolver_aliases_by_observation.is_empty());
}

#[test]
fn v2_resolver_alias_index_excludes_resolverless_successors() {
    const RESOLVED_OLD: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";
    const RESOLVERLESS_OLD: &str =
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000002";
    const CANDIDATE: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000003";
    const RESOLVED_NEW: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000001";
    const RESOLVERLESS_NEW: &str =
        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccc00000001";
    let mut state = anchored_state();
    install_token(&mut state, ROOT, RESOLVED_OLD, b"alpha", 100);
    state.set_v2_resolver(ROOT, RESOLVED_OLD, Some("0xresolver".to_owned()));
    install_token(&mut state, ROOT, RESOLVERLESS_OLD, b"beta", 100);
    state
        .regenerate_v2_token(ROOT, RESOLVED_OLD, RESOLVED_NEW)
        .unwrap();
    state
        .regenerate_v2_token(ROOT, RESOLVERLESS_OLD, RESOLVERLESS_NEW)
        .unwrap();

    assert_eq!(
        state.live_v2_resolver_tokens_sharing(
            ROOT,
            &std::collections::BTreeSet::from([CANDIDATE.into()]),
        ),
        std::collections::BTreeSet::from([RESOLVED_OLD.into()]),
    );
    state.set_v2_resolver(ROOT, RESOLVED_NEW, None);
    assert!(
        state
            .live_v2_resolver_tokens_sharing(
                ROOT,
                &std::collections::BTreeSet::from([CANDIDATE.into()]),
            )
            .is_empty()
    );
}

#[test]
fn v2_restore_displacement_removes_the_displaced_resolver_index_entry() {
    const RESTORED: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000001";
    const DISPLACED: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb00000001";
    let mut state = anchored_state();
    install_token(&mut state, ROOT, RESTORED, b"alpha", 100);
    install_token(&mut state, ROOT, DISPLACED, b"alpha", 100);
    state.attach_v2_unbound_resource(
        ROOT,
        RESTORED,
        "0x99".to_owned(),
        Uuid::from_u128(99),
        Some(Uuid::from_u128(100)),
    );
    state.set_v2_resolver(ROOT, RESTORED, Some("0xrestored".to_owned()));
    state.set_v2_resolver(ROOT, DISPLACED, Some("0xdisplaced".to_owned()));

    state.restore_v2_registration(
        ROOT,
        RESTORED,
        Some(Uuid::from_u128(1)),
        NAMESPACE,
        b"alpha",
        200,
        Some(json!({"registrant":"0xowner", "expiry":200})),
    );

    assert!(state.v2_token(ROOT, DISPLACED).is_none());
    assert_v2_indexes_are_derived(&state);
}
