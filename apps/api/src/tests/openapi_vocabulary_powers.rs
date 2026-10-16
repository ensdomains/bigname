//! Every power the interpreters emit, read from their source tables and arms, is a
//! `PermissionPower` value. A served power outside the enum makes a schema-checking client
//! reject the whole page that carries it. This file is a child of `v2` for module visibility.

use std::collections::BTreeSet;

use super::permission_values::tests::code_powers;
use crate::tests::openapi_contract::document;

#[test]
fn every_emitted_power_is_a_permission_power_value() {
    let declared: BTreeSet<&str> = document()["components"]["schemas"]["PermissionPower"]["enum"]
        .as_array()
        .expect("PermissionPower is an enum")
        .iter()
        .map(|value| value.as_str().expect("enum member is a string"))
        .collect();
    let emitted = code_powers();
    assert!(
        emitted.contains("wrapper_control") && emitted.contains("registry_control"),
        "the producer readers must reach the standard approvals arms, got {emitted:?}"
    );
    let missing: Vec<&String> = emitted
        .iter()
        .filter(|power| !declared.contains(power.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "powers the code can serve that PermissionPower does not list: {missing:?}"
    );
}
