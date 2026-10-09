//! A registry is named by its mount path, the parent token that points at it, when its parent
//! claim does not point back. `ROOT` is the anchored `eth` registry, `CHILD` the registry under
//! test and `THIRD` a second user registry.
use super::*;

const UNANCHORED: &str = "0x0000000000000000000000000000000000000060";

fn path(labels: &[&str]) -> String {
    let labels = labels
        .iter()
        .map(|label| (*label).to_owned())
        .collect::<Vec<_>>();
    format!(
        "{NAMESPACE}:{}",
        crate::schema_v2::common::namehash(&labels)
    )
}

fn mount(state: &mut State, token_id: &str, label: &[u8], expiry: u64, registry: &str) {
    install_token(state, ROOT, token_id, label, expiry);
    state.set_v2_subregistry(ROOT, token_id, Some(registry.to_owned()));
}

/// `CHILD` mounted at `m.eth` with a `child` token and no parent claim.
fn mounted_state() -> State {
    let mut state = anchored_state();
    mount(&mut state, "0x01", b"m", 100, CHILD);
    install_token(&mut state, CHILD, "0x02", b"child", 100);
    state
}

/// `mounted_state` with a claim that names a label `ROOT` holds no token for.
fn unattached_claim_state() -> State {
    let mut state = mounted_state();
    state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), b"m");
    state.refresh_dirty_v2_names(1);
    state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), b"unattached");
    state.refresh_dirty_v2_names(1);
    state
}

fn moves(
    transitions: &[super::super::V2NameTransition],
    registry: &str,
    token_id: &str,
) -> Vec<(Option<String>, Option<String>)> {
    let id = |name: &Option<super::super::V2NameState>| {
        name.as_ref().map(|name| name.logical_name_id.clone())
    };
    transitions
        .iter()
        .filter(|transition| transition.registry == registry && transition.token_id == token_id)
        .map(|transition| (id(&transition.previous), id(&transition.current)))
        .collect()
}

fn current_name(state: &State, registry: &str, token_id: &str) -> Option<String> {
    state
        .v2_token(registry, token_id)
        .and_then(|token| token.name)
        .map(|name| name.logical_name_id)
}

#[test]
fn v2_a_mounted_registry_with_no_claim_is_named() {
    let mut state = mounted_state();
    state.refresh_dirty_v2_names(1);
    assert_eq!(
        current_name(&state, CHILD, "0x02"),
        Some(path(&["child", "m", "eth"]))
    );
    assert_v2_indexes_are_derived(&state);
}

#[test]
fn v2_mount_path_names_a_registry_whose_claim_has_no_token() {
    let mut state = mounted_state();
    state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), b"m");
    state.refresh_dirty_v2_names(1);
    let named = path(&["child", "m", "eth"]);
    assert_eq!(current_name(&state, CHILD, "0x02"), Some(named.clone()));

    state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), b"unattached");
    assert!(state.refresh_dirty_v2_names(2).is_empty());
    assert_eq!(current_name(&state, CHILD, "0x02"), Some(named.clone()));

    state.set_v2_parent_claim(CHILD, None, b"");
    assert!(state.refresh_dirty_v2_names(3).is_empty());
    assert_eq!(current_name(&state, CHILD, "0x02"), Some(named));
}

#[test]
fn v2_mount_path_wins_over_a_claim_whose_token_points_elsewhere() {
    let mut state = mounted_state();
    state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), b"m");
    mount(&mut state, "0x03", b"other", 100, THIRD);
    state.refresh_dirty_v2_names(1);

    state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), b"other");
    assert!(state.refresh_dirty_v2_names(2).is_empty());
    assert_eq!(
        current_name(&state, CHILD, "0x02"),
        Some(path(&["child", "m", "eth"]))
    );
}

#[test]
fn v2_claim_made_valid_moves_the_name_from_the_mount_path() {
    let mut state = unattached_claim_state();
    mount(&mut state, "0x03", b"unattached", 100, CHILD);
    let transitions = state.refresh_dirty_v2_names(2);
    assert_eq!(
        moves(&transitions, CHILD, "0x02"),
        [(
            Some(path(&["child", "m", "eth"])),
            Some(path(&["child", "unattached", "eth"]))
        )]
    );
}

#[test]
fn v2_a_claim_that_points_back_chooses_the_path_even_through_a_nameless_parent() {
    // `CHILD` is mounted at `m.eth` and claims `c` in `THIRD`, whose `c` token points back.
    // While `THIRD` has no name the claim names nothing, and the mount is no fallback.
    let mut state = mounted_state();
    install_token(&mut state, THIRD, "0x03", b"c", 100);
    state.set_v2_subregistry(THIRD, "0x03", Some(CHILD.to_owned()));
    state.set_v2_parent_claim(CHILD, Some(THIRD.to_owned()), b"c");
    state.refresh_dirty_v2_names(1);
    assert_eq!(current_name(&state, CHILD, "0x02"), None);

    // Once `THIRD` is `t.eth` the claimed path is the name, though `child.m.eth` is shorter.
    mount(&mut state, "0x04", b"t", 100, THIRD);
    state.refresh_dirty_v2_names(2);
    assert_eq!(
        current_name(&state, CHILD, "0x02"),
        Some(path(&["child", "c", "t", "eth"]))
    );
    assert_v2_indexes_are_derived(&state);
}

#[test]
fn v2_two_mount_paths_of_equal_length_pick_the_smaller_label() {
    // The smaller token id holds the larger label, so a pick by token key would name `b`.
    let labels: [(&str, &[u8]); 2] = [("0x0b", b"a"), ("0x0a", b"b")];
    for order in [[0, 1], [1, 0]] {
        for claim in [None, Some(b"unattached".as_slice())] {
            let mut state = anchored_state();
            for index in order {
                let (token_id, label) = labels[index];
                mount(&mut state, token_id, label, 100, CHILD);
            }
            if let Some(label) = claim {
                state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), label);
            }
            install_token(&mut state, CHILD, "0x02", b"child", 100);
            // A parent with no suffix is not a mount path, whatever its label.
            install_token(&mut state, UNANCHORED, "0x09", b"0", 100);
            state.set_v2_subregistry(UNANCHORED, "0x09", Some(CHILD.to_owned()));
            state.refresh_dirty_v2_names(1);
            assert_eq!(
                current_name(&state, CHILD, "0x02"),
                Some(path(&["child", "a", "eth"])),
                "mount order {order:?}, claim {claim:?}"
            );
            assert_v2_indexes_are_derived(&state);
        }
    }
}

#[test]
fn v2_a_mount_under_an_unanchored_parent_names_nothing() {
    let mut state = anchored_state();
    install_token(&mut state, UNANCHORED, "0x09", b"q", 100);
    state.set_v2_subregistry(UNANCHORED, "0x09", Some(CHILD.to_owned()));
    install_token(&mut state, CHILD, "0x02", b"child", 100);
    state.refresh_dirty_v2_names(1);
    assert_eq!(current_name(&state, CHILD, "0x02"), None);
}

#[test]
fn v2_unmount_with_a_stale_claim_still_releases() {
    let mut state = mounted_state();
    state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), b"m");
    state.refresh_dirty_v2_names(1);

    state.set_v2_subregistry(ROOT, "0x01", None);
    let transitions = state.refresh_dirty_v2_names(2);
    assert_eq!(
        moves(&transitions, CHILD, "0x02"),
        [(Some(path(&["child", "m", "eth"])), None)]
    );
}

#[test]
fn v2_restored_mount_with_an_invalid_claim_moves_nothing() {
    let mut state = unattached_claim_state();
    assert_eq!(
        current_name(&state, CHILD, "0x02"),
        Some(path(&["child", "m", "eth"]))
    );

    state.set_v2_subregistry(ROOT, "0x01", None);
    state.set_v2_subregistry(ROOT, "0x01", Some(CHILD.to_owned()));
    super::super::reset_v2_refresh_visits();
    assert!(state.refresh_dirty_v2_names(2).is_empty());
    assert_eq!(super::super::v2_refresh_visits(), 1);
}

#[test]
fn v2_mount_fallback_reaches_nested_registries() {
    let leaf = path(&["leaf", "c", "m", "eth"]);
    // The outer claim is invalid, then the inner one.
    for (outer, inner) in [(b"unattached".as_slice(), b"c".as_slice()), (b"m", b"zzz")] {
        let mut state = mounted_state();
        state.set_v2_parent_claim(CHILD, Some(ROOT.to_owned()), outer);
        install_token(&mut state, CHILD, "0x03", b"c", 100);
        state.set_v2_subregistry(CHILD, "0x03", Some(THIRD.to_owned()));
        state.set_v2_parent_claim(THIRD, Some(CHILD.to_owned()), inner);
        install_token(&mut state, THIRD, "0x04", b"leaf", 100);
        state.refresh_dirty_v2_names(1);
        assert_eq!(current_name(&state, THIRD, "0x04"), Some(leaf.clone()));

        state.set_v2_subregistry(ROOT, "0x01", None);
        let transitions = state.refresh_dirty_v2_names(2);
        assert_eq!(
            moves(&transitions, CHILD, "0x02"),
            [(Some(path(&["child", "m", "eth"])), None)]
        );
        assert_eq!(
            moves(&transitions, THIRD, "0x04"),
            [(Some(leaf.clone()), None)]
        );
    }
}

#[test]
fn v2_mount_walk_ignores_a_mount_under_its_own_descendant() {
    let mut state = unattached_claim_state();
    install_token(&mut state, CHILD, "0x03", b"a", 100);
    state.set_v2_subregistry(CHILD, "0x03", Some(THIRD.to_owned()));
    install_token(&mut state, THIRD, "0x04", b"a", 100);
    state.set_v2_subregistry(THIRD, "0x04", Some(CHILD.to_owned()));
    // A registry that points one of its own labels at itself.
    install_token(&mut state, CHILD, "0x05", b"self", 100);
    state.set_v2_subregistry(CHILD, "0x05", Some(CHILD.to_owned()));
    state.refresh_dirty_v2_names(2);

    assert_eq!(
        current_name(&state, CHILD, "0x02"),
        Some(path(&["child", "m", "eth"]))
    );
    assert_eq!(
        current_name(&state, THIRD, "0x04"),
        Some(path(&["a", "a", "m", "eth"]))
    );
    assert_eq!(
        current_name(&state, CHILD, "0x05"),
        Some(path(&["self", "m", "eth"]))
    );
}

#[test]
fn v2_an_expired_mount_moves_the_name_to_the_next_mount_path() {
    let mut state = anchored_state();
    mount(&mut state, "0x0a", b"a", 10, CHILD);
    mount(&mut state, "0x0b", b"b", 100, CHILD);
    install_token(&mut state, CHILD, "0x02", b"child", 100);
    state.refresh_dirty_v2_names(9);
    assert_eq!(
        current_name(&state, CHILD, "0x02"),
        Some(path(&["child", "a", "eth"]))
    );

    let transitions = state.refresh_dirty_v2_names(10);
    assert_eq!(
        moves(&transitions, CHILD, "0x02"),
        [(
            Some(path(&["child", "a", "eth"])),
            Some(path(&["child", "b", "eth"]))
        )]
    );
}

#[test]
fn preferred_path_takes_fewest_labels_then_compares_from_the_anchor_downward() {
    let path = |namespace: &str, labels: &[&str]| {
        (
            namespace.to_owned(),
            labels
                .iter()
                .map(|label| label.as_bytes().to_vec())
                .collect::<Vec<_>>(),
        )
    };
    use super::super::topology::preferred_path as pick;
    assert_eq!(pick(Vec::new()), None);
    // Labels are stored leaf first. A path with fewer labels comes first, whatever its
    // labels. At equal length `b.a.eth` comes before `a.b.eth` because `a` is nearer the
    // anchor.
    assert_eq!(
        pick([path("ens", &["a", "a", "eth"]), path("ens", &["z", "eth"])]),
        Some(path("ens", &["z", "eth"]))
    );
    let deep = path("ens", &["b", "a", "eth"]);
    let shallow = path("ens", &["a", "b", "eth"]);
    let prefix = path("ens", &["a", "eth"]);
    for order in [
        [deep.clone(), shallow.clone(), prefix.clone()],
        [shallow.clone(), prefix.clone(), deep.clone()],
    ] {
        assert_eq!(pick(order), Some(prefix.clone()));
    }
    assert_eq!(pick([shallow.clone(), deep.clone()]), Some(deep.clone()));
    // Raw bytes, not text order: an uppercase label sorts before a lowercase one.
    assert_eq!(
        pick([path("ens", &["a", "eth"]), path("ens", &["Z", "eth"])]),
        Some(path("ens", &["Z", "eth"]))
    );
    // Equal labels are one path. Another namespace breaks the tie the same way in any order.
    assert_eq!(pick([deep.clone(), deep.clone()]), Some(deep.clone()));
    let other = path("basenames", &["b", "a", "eth"]);
    for order in [[deep.clone(), other.clone()], [other.clone(), deep.clone()]] {
        assert_eq!(pick(order), Some(other.clone()));
    }
}

#[test]
fn v2_two_mount_paths_compare_non_ascii_labels_by_their_bytes() {
    // `é` is 0xC3 0xA9 and `z` is 0x7A, so `z` is the smaller label.
    let mut state = anchored_state();
    mount(&mut state, "0x0a", "é".as_bytes(), 100, CHILD);
    mount(&mut state, "0x0b", b"z", 100, CHILD);
    install_token(&mut state, CHILD, "0x02", b"child", 100);
    state.refresh_dirty_v2_names(1);
    assert_eq!(
        current_name(&state, CHILD, "0x02"),
        Some(path(&["child", "z", "eth"]))
    );
}

fn registry(index: usize) -> String {
    format!("0x{:040x}", 0x1000 + index)
}

fn walk_steps(state: &State, registry: &str) -> (Option<Vec<Vec<u8>>>, usize) {
    use super::super::topology::V2_WALK_STEPS;
    V2_WALK_STEPS.set(0);
    let suffix = state.v2_registry_raw_suffix(registry, NAMESPACE, 1);
    (suffix, V2_WALK_STEPS.get())
}

#[test]
fn v2_mount_walk_enters_each_registry_once_under_nested_fan_out() {
    // Eight unclaimed registries in a chain. Each is mounted under three tokens of the one
    // above it, and the first under three tokens of the anchored registry.
    const REGISTRIES: usize = 8;
    const MOUNTS: usize = 3;
    let mut state = anchored_state();
    for index in 0..REGISTRIES {
        let parent = if index == 0 {
            ROOT.to_owned()
        } else {
            registry(index - 1)
        };
        for mount in 0..MOUNTS {
            let token_id = format!("0x{:02x}", 0x10 + mount);
            let label = [b'a' + mount as u8];
            install_token(&mut state, &parent, &token_id, &label, 100);
            state.set_v2_subregistry(&parent, &token_id, Some(registry(index)));
        }
    }
    let (suffix, steps) = walk_steps(&state, &registry(REGISTRIES - 1));
    let mut expected = vec![b"a".to_vec(); REGISTRIES];
    expected.push(b"eth".to_vec());
    assert_eq!(suffix, Some(expected));
    // The walk enters the eight registries and the anchored one, each once.
    assert_eq!(
        steps,
        REGISTRIES + 1,
        "{steps} walk steps for {REGISTRIES} registries with {MOUNTS} mounts each"
    );
}

#[test]
fn v2_mount_walk_gives_one_answer_in_a_cycle_whichever_mount_comes_first() {
    // `X` and `Y` mount each other and each has a mount under the anchored registry. `X` is
    // `z.eth` and `Y` is `b.eth`, their shortest paths. `TARGET` is mounted under both at
    // equal length, so the smaller label next to the anchor decides: `t.b.eth`. The order
    // the registries and their tokens sort in must not change any answer.
    for (x, y) in [(registry(1), registry(2)), (registry(2), registry(1))] {
        let target = registry(9);
        let mut state = anchored_state();
        mount(&mut state, "0x01", b"z", 100, &x);
        mount(&mut state, "0x02", b"b", 100, &y);
        for (parent, token_id, label, child) in [
            (&y, "0x03", b"a", &x),
            (&x, "0x04", b"y", &y),
            (&y, "0x05", b"t", &target),
            (&x, "0x06", b"u", &target),
        ] {
            install_token(&mut state, parent, token_id, label, 100);
            state.set_v2_subregistry(parent, token_id, Some(child.clone()));
        }
        let labels = |labels: &[&str]| {
            Some(
                labels
                    .iter()
                    .map(|label| label.as_bytes().to_vec())
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(walk_steps(&state, &x).0, labels(&["z", "eth"]));
        assert_eq!(walk_steps(&state, &y).0, labels(&["b", "eth"]));
        assert_eq!(
            walk_steps(&state, &target).0,
            labels(&["t", "b", "eth"]),
            "X at {x}, Y at {y}"
        );
    }
}

/// The eight by three ladder of `v2_mount_walk_enters_each_registry_once_under_nested_fan_out`.
fn ladder(state: &mut State, registries: usize, mounts: usize) {
    for index in 0..registries {
        let parent = if index == 0 {
            ROOT.to_owned()
        } else {
            registry(index - 1)
        };
        for mount in 0..mounts {
            let token_id = format!("0x{:02x}", 0x10 + mount);
            let label = [b'a' + mount as u8];
            install_token(state, &parent, &token_id, &label, 100);
            state.set_v2_subregistry(&parent, &token_id, Some(registry(index)));
        }
    }
}

#[test]
fn v2_mount_walk_stays_linear_when_a_cycle_is_reachable() {
    const REGISTRIES: usize = 8;
    const MOUNTS: usize = 3;
    let mut expected = vec![b"a".to_vec(); REGISTRIES];
    expected.push(b"eth".to_vec());
    // The registry mounted under the anchor points one of its own labels at itself.
    let mut own_label = anchored_state();
    ladder(&mut own_label, REGISTRIES, MOUNTS);
    install_token(&mut own_label, &registry(0), "0x20", b"self", 100);
    own_label.set_v2_subregistry(&registry(0), "0x20", Some(registry(0)));
    // The same registry and another one mount each other.
    let mut two_cycle = anchored_state();
    ladder(&mut two_cycle, REGISTRIES, MOUNTS);
    let other = registry(50);
    install_token(&mut two_cycle, &registry(0), "0x20", b"x", 100);
    two_cycle.set_v2_subregistry(&registry(0), "0x20", Some(other.clone()));
    install_token(&mut two_cycle, &other, "0x21", b"y", 100);
    two_cycle.set_v2_subregistry(&other, "0x21", Some(registry(0)));
    // Each registry above the target is entered once: the ladder, the anchored registry and,
    // for the two-cycle, the other registry.
    for (shape, state, closure) in [
        ("own label", own_label, REGISTRIES + 1),
        ("two-cycle", two_cycle, REGISTRIES + 2),
    ] {
        let (suffix, steps) = walk_steps(&state, &registry(REGISTRIES - 1));
        assert_eq!(suffix, Some(expected.clone()), "{shape}");
        assert_eq!(steps, closure, "{shape}: {steps} walk steps");
    }
}

#[test]
fn v2_a_shorter_mount_path_beats_a_longer_one_with_smaller_labels() {
    // `CHILD` is mounted at `z.eth` and at `a.a.eth`. The path with fewer labels is the name,
    // although `a` sorts before `z`.
    for shorter_first in [true, false] {
        let mut state = anchored_state();
        let shorter = |state: &mut State| mount(state, "0x01", b"z", 100, CHILD);
        let longer = |state: &mut State| {
            mount(state, "0x02", b"a", 100, THIRD);
            install_token(state, THIRD, "0x03", b"a", 100);
            state.set_v2_subregistry(THIRD, "0x03", Some(CHILD.to_owned()));
        };
        if shorter_first {
            shorter(&mut state);
            longer(&mut state);
        } else {
            longer(&mut state);
            shorter(&mut state);
        }
        install_token(&mut state, CHILD, "0x02", b"child", 100);
        state.refresh_dirty_v2_names(1);
        assert_eq!(
            current_name(&state, CHILD, "0x02"),
            Some(path(&["child", "z", "eth"])),
            "shorter first: {shorter_first}"
        );
    }
}
