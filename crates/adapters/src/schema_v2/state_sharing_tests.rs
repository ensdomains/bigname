use uuid::Uuid;

use super::*;

fn registrar(state: &mut State, namehash: &str) {
    state.observe_v1_registrar(
        "ens",
        namehash,
        format!("ens:{namehash}"),
        true,
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        "ens_v1_registrar_l1".to_owned(),
        Some(1),
        Some("label".to_owned()),
        Some(100),
        Some("owner".to_owned()),
        None,
        false,
        true,
    );
}

#[test]
fn registrar_snapshots_share_until_mutation_and_reactivation_shares_the_new_value() {
    let mut committed = State::new(vec![], vec![]);
    registrar(&mut committed, "first");
    registrar(&mut committed, "neighbor");
    assert!(Arc::ptr_eq(
        &committed.v1_names["ens:first"],
        &committed.v1_registrars["ens:first"]
    ));
    let mut candidate = committed.clone();

    let (before, after) = candidate
        .transfer_v1_registrar_owner("ens", "first", "new-owner".to_owned())
        .unwrap();
    assert_eq!(before.owner.as_deref(), Some("owner"));
    assert_eq!(after.owner.as_deref(), Some("new-owner"));
    assert_eq!(
        committed.v1_registrars["ens:first"].owner.as_deref(),
        Some("owner")
    );
    assert_eq!(
        candidate.v1_names["ens:first"].owner.as_deref(),
        Some("owner")
    );
    assert!(!Arc::ptr_eq(
        &candidate.v1_names["ens:first"],
        &candidate.v1_registrars["ens:first"]
    ));
    // Copying a tree leaf for the changed entry does not copy its neighbor's payload.
    assert!(Arc::ptr_eq(
        &committed.v1_registrars["ens:neighbor"],
        &candidate.v1_registrars["ens:neighbor"]
    ));

    candidate
        .reactivate_v1_registrar_for_owner("ens", "first", "new-owner", 1)
        .unwrap();
    assert!(Arc::ptr_eq(
        &candidate.v1_names["ens:first"],
        &candidate.v1_registrars["ens:first"]
    ));
    assert_eq!(
        committed.v1_names["ens:first"].owner.as_deref(),
        Some("owner")
    );
}

#[test]
fn registry_fallback_changes_keep_current_and_committed_snapshots_independent() {
    let mut committed = State::new(vec![], vec![]);
    committed.observe_v1_registry(
        "ens",
        "node",
        "ens:node".to_owned(),
        true,
        Uuid::from_u128(3),
        "ens_v1_registry_l1".to_owned(),
        Some("owner".to_owned()),
        None,
        None,
    );
    assert!(Arc::ptr_eq(
        &committed.v1_names["ens:node"],
        &committed.v1_registry_authorities["ens:node"]
    ));
    let mut candidate = committed.clone();
    candidate.sync_registry_surface_from_registrar(
        "ens",
        "node",
        "ens:renamed",
        true,
        Some("label"),
    );
    assert_eq!(
        committed.v1_registry_authorities["ens:node"].logical_name_id,
        "ens:node"
    );
    assert_eq!(candidate.v1_names["ens:node"].logical_name_id, "ens:node");
    assert_eq!(
        candidate.v1_registry_authorities["ens:node"].logical_name_id,
        "ens:renamed"
    );

    let changed = candidate.v1_registry_authorities["ens:node"]
        .as_ref()
        .clone();
    candidate.activate_v1_authority("ens", "node", Some(changed));
    assert!(Arc::ptr_eq(
        &candidate.v1_names["ens:node"],
        &candidate.v1_registry_authorities["ens:node"]
    ));
    assert_eq!(committed.v1_names["ens:node"].logical_name_id, "ens:node");
}

#[test]
fn marking_an_already_known_surface_preserves_payload_sharing() {
    let mut state = State::new(vec![], vec![]);
    registrar(&mut state, "node");
    let committed = state.clone();
    state.bind_v1_active_surface("ens", "node");
    assert!(Arc::ptr_eq(
        &state.v1_names["ens:node"],
        &state.v1_registrars["ens:node"]
    ));
    assert!(Arc::ptr_eq(
        &state.v1_names["ens:node"],
        &committed.v1_names["ens:node"]
    ));
}
