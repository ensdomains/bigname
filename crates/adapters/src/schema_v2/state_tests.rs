use imbl::ordset::OrdSet;
use uuid::Uuid;

use super::{State, V1Release, v1_registration_is_live};

const NAMESPACE: &str = "test";

#[test]
fn v1_release_order_matches_naive_scan_after_expiry_updates_and_removals() {
    let mut state = State::new(Vec::new(), Vec::new());
    observe_registrar(&mut state, "zeta", Some(100));
    observe_registrar(&mut state, "alpha", Some(100));
    observe_registrar(&mut state, "middle", Some(50));
    observe_registrar(&mut state, "live", Some(500));
    observe_registrar(&mut state, "updated", Some(50));
    observe_registrar(&mut state, "updated", Some(500));
    observe_registrar(&mut state, "removed", Some(50));
    state.restore_v1_registration_release(NAMESPACE, "removed");

    assert_expiry_index_is_derived(&state);
    let timestamp = 100 + super::ENS_GRACE_PERIOD_SECS + 1;
    let expected = naive_due_keys(&state, timestamp);
    let actual = release_keys(state.settle_v1_releases(timestamp));

    assert_eq!(expected, vec!["test:alpha", "test:middle", "test:zeta"]);
    assert_eq!(actual, expected);
    assert_expiry_index_is_derived(&state);
}

#[test]
fn v1_expiry_index_matches_naive_scan_over_generated_mutations() {
    let mut state = State::new(Vec::new(), Vec::new());
    let mut sequence = Sequence(0xd1ff_3e12_395a_11ce);

    for step in 0..1_024 {
        let namehash = format!("name-{:02}", sequence.next() % 48);
        match sequence.next() % 5 {
            0..=2 => {
                let expiry = match sequence.next() % 8 {
                    0 => None,
                    1 => Some(i64::MAX),
                    _ => Some((sequence.next() % 4_000) as i64 - 2_000),
                };
                observe_registrar(&mut state, &namehash, expiry);
            }
            3 => state.restore_v1_registration_release(NAMESPACE, &namehash),
            _ => {
                let timestamp =
                    super::ENS_GRACE_PERIOD_SECS + (sequence.next() % 6_000) as i64 - 3_000;
                assert_release_sequence(&mut state, timestamp, step);
            }
        }
        assert_expiry_index_is_derived(&state);
    }

    assert_release_sequence(&mut state, i64::MAX, 1_024);
    assert_expiry_index_is_derived(&state);
}

#[test]
fn v1_release_occurs_one_second_after_the_grace_boundary() {
    let expiry = 100;
    let boundary = expiry + super::ENS_GRACE_PERIOD_SECS;
    let mut state = State::new(Vec::new(), Vec::new());
    observe_registrar(&mut state, "boundary", Some(expiry));

    assert!(state.settle_v1_releases(boundary - 1).is_empty());
    assert!(state.settle_v1_releases(boundary).is_empty());
    assert_eq!(
        release_keys(state.settle_v1_releases(boundary + 1)),
        vec!["test:boundary"]
    );
}

fn assert_release_sequence(state: &mut State, timestamp: i64, step: usize) {
    let expected = naive_due_keys(state, timestamp);
    let actual = release_keys(state.settle_v1_releases(timestamp));
    assert_eq!(actual, expected, "release sequence differed at step {step}");
}

// Test-only copy of the pre-index full scan, including its registrar-key iteration order.
fn naive_due_keys(state: &State, timestamp: i64) -> Vec<String> {
    state
        .v1_registrars
        .iter()
        .filter_map(|(key, registrar)| {
            let expiry = registrar.expiry?;
            (!v1_registration_is_live(Some(expiry), timestamp)).then(|| key.clone())
        })
        .collect()
}

fn release_keys(releases: Vec<V1Release>) -> Vec<String> {
    releases
        .into_iter()
        .map(|release| format!("{NAMESPACE}:{}", release.namehash))
        .collect()
}

fn assert_expiry_index_is_derived(state: &State) {
    let expected = state
        .v1_registrars
        .iter()
        .filter_map(|(key, registrar)| registrar.expiry.map(|expiry| (expiry, key.clone())))
        .collect::<OrdSet<_>>();
    assert_eq!(state.v1_expiries, expected);
}

fn observe_registrar(state: &mut State, namehash: &str, expiry: Option<i64>) {
    state.observe_v1_registrar(
        NAMESPACE,
        namehash,
        format!("{NAMESPACE}:{namehash}"),
        true,
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        "ens_v1_registrar_l1".to_owned(),
        Some(1),
        None,
        expiry,
        Some("0x0000000000000000000000000000000000000001".to_owned()),
        Some(format!("registrar:{namehash}")),
        false,
        true,
    );
}

struct Sequence(u64);

impl Sequence {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}
