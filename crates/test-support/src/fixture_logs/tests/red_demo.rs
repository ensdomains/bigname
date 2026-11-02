//! Draft-only: each test fails on purpose to show the check failing by run.

use super::*;

#[test]
fn red_demo_no_topic() {
    let batch = registry_batch(vec![log(
        1,
        0,
        REGISTRY,
        vec![topic("RegistryNewOwner(bytes32,bytes32,address)")],
    )]);
    assert_selectable(&batch, &Outcome::default());
}

#[test]
fn red_demo_role_mismatch() {
    let batch = registry_batch(vec![log(
        1,
        0,
        REGISTRAR,
        vec![topic("NewOwner(bytes32,bytes32,address)")],
    )]);
    assert_selectable(&batch, &Outcome::default());
}

#[test]
fn red_demo_stale_allowance() {
    let _guard = allow_unmatched(&[("Undeclared(address)", "a stale reason")]);
}

#[test]
fn red_demo_drift() {
    let batch = registry_batch(vec![log(
        1,
        0,
        REGISTRY,
        vec![topic("Undeclared(address)")],
    )]);
    assert_matches_catalog(&batch, &[Some(true)]);
}
