//! The resolver block of an `ens_v1` name whose selected resource has no admitted pointer of its
//! own: it reads the node's registry pointer (F4) unless a withholding rule, an unsupported
//! selection or a missing selected resource keeps it out.
use super::*;
use crate::families::position::Position;

const NAME: &str = "ens:0x03fe";
const LEASE: &str = "lease";
const REGISTRY_ONLY: &str = "registry-only";
const A: &str = "0x00000000000000000000000000000000000000a1";
const B: &str = "0x00000000000000000000000000000000000000b2";
const REGISTRY: &str = "ens_v1_registry_l1";

fn pointer(resource: Option<&str>, resolver: &str, block: i64, name: Option<&str>) -> PointerRow {
    PointerRow {
        resource_id: resource.map(str::to_owned),
        resolver_address: Some(resolver.to_owned()),
        position: Position {
            block_number: block,
            transaction_index: Some(0),
            log_index: Some(0),
            event_identity: format!("event-{block}"),
        },
        source_family: REGISTRY.to_owned(),
        logical_name_id: name.map(str::to_owned),
        normalized_event_id: Some(block),
    }
}

fn scope(resource: Option<&'static str>, arm: &'static str) -> ResolverScope<'static> {
    ResolverScope {
        name: NAME,
        chain_id: "ethereum-mainnet",
        resource,
        arm: Some(arm),
        admits: true,
        withholds: false,
        unresolvable: false,
    }
}

/// `(address, latest_event_kind, source family)` of the block.
fn block(
    scope: &ResolverScope<'_>,
    own: Option<&PointerRow>,
    node: Option<&PointerRow>,
    serving: Option<&Serving>,
) -> (Value, Value, Option<String>) {
    let (block, family) = resolver_block(scope, own, node, serving);
    (
        block["address"].clone(),
        block["latest_event_kind"].clone(),
        family,
    )
}

fn served(address: &str) -> (Value, Value, Option<String>) {
    (
        Value::from(address),
        Value::from("ResolverChanged"),
        Some(REGISTRY.to_owned()),
    )
}

fn none() -> (Value, Value, Option<String>) {
    (Value::Null, Value::Null, None)
}

#[test]
fn a_registry_pointer_on_another_resource_is_served() {
    let node = pointer(Some(LEASE), A, 10, Some(NAME));
    assert_eq!(
        block(&scope(Some(REGISTRY_ONLY), "ens_v1"), None, Some(&node), None),
        served(A)
    );
    let node = pointer(Some(REGISTRY_ONLY), A, 10, Some(NAME));
    assert_eq!(
        block(&scope(Some(LEASE), "ens_v1"), None, Some(&node), None),
        served(A)
    );
}

#[test]
fn an_unnamed_pointer_on_the_selected_resource_reads_the_registry_pointer() {
    let own = pointer(Some(LEASE), A, 10, None);
    let node = pointer(Some(LEASE), A, 10, None);
    assert_eq!(
        block(&scope(Some(LEASE), "ens_v1"), Some(&own), Some(&node), None),
        served(A)
    );
}

#[test]
fn another_names_pointer_on_the_selected_resource_reads_the_registry_pointer() {
    let own = pointer(Some(LEASE), B, 11, Some("ens:0xother"));
    let node = pointer(Some(REGISTRY_ONLY), A, 10, Some(NAME));
    assert_eq!(
        block(&scope(Some(LEASE), "ens_v1"), Some(&own), Some(&node), None),
        served(A)
    );
}

#[test]
fn an_admitted_pointer_keeps_the_registry_read_through() {
    let node = pointer(Some(REGISTRY_ONLY), A, 12, Some(NAME));
    let agreeing = pointer(Some(LEASE), A, 10, Some(NAME));
    let mut expected = served(A);
    assert_eq!(
        block(&scope(Some(LEASE), "ens_v1"), Some(&agreeing), Some(&node), None),
        expected
    );
    let stale = pointer(Some(LEASE), B, 10, Some(NAME));
    assert_eq!(
        block(&scope(Some(LEASE), "ens_v1"), Some(&stale), Some(&node), None),
        expected
    );
    let cleared = pointer(Some(LEASE), "", 11, Some(NAME));
    assert_eq!(
        block(&scope(Some(LEASE), "ens_v1"), Some(&cleared), Some(&node), None),
        expected
    );
    // With no registry pointer the admitted pointer is served as it is.
    expected.0 = Value::from(B);
    assert_eq!(
        block(&scope(Some(LEASE), "ens_v1"), Some(&stale), None, None),
        expected
    );
}

#[test]
fn a_cleared_registry_pointer_composes_a_clear() {
    let node = pointer(Some(LEASE), "", 10, None);
    assert_eq!(
        block(&scope(Some(REGISTRY_ONLY), "ens_v1"), None, Some(&node), None),
        (
            Value::Null,
            Value::from("ResolverChanged"),
            Some(REGISTRY.to_owned()),
        )
    );
}

#[test]
fn a_node_without_a_registry_pointer_composes_nothing() {
    assert_eq!(
        block(&scope(Some(REGISTRY_ONLY), "ens_v1"), None, None, None),
        none()
    );
}

#[test]
fn a_withheld_or_unresolvable_name_does_not_serve_the_registry_pointer() {
    let node = pointer(Some(LEASE), A, 10, Some(NAME));
    let mut withheld = scope(Some(REGISTRY_ONLY), "ens_v1");
    withheld.withholds = true;
    let kept = (
        Value::Null,
        Value::from("ResolverChanged"),
        Some(REGISTRY.to_owned()),
    );
    assert_eq!(block(&withheld, None, Some(&node), None), kept);
    let mut unresolvable = scope(Some(REGISTRY_ONLY), "ens_v1");
    unresolvable.unresolvable = true;
    assert_eq!(block(&unresolvable, None, Some(&node), None), kept);
}

#[test]
fn an_unsupported_selection_admits_no_registry_pointer() {
    let node = pointer(Some(LEASE), A, 10, Some(NAME));
    let mut unsupported = scope(Some(REGISTRY_ONLY), "ens_v1");
    unsupported.admits = false;
    assert_eq!(block(&unsupported, None, Some(&node), None), none());
}

#[test]
fn a_name_without_a_selected_resource_is_unchanged() {
    // The ownerless registry profile, and a `.eth` name the registrar lease alone holds while
    // its registry owner is zero, select no resource: only their serving pointer is served.
    let node = pointer(Some(LEASE), A, 10, Some(NAME));
    assert_eq!(
        block(&scope(None, "ens_v1"), None, Some(&node), None),
        none()
    );
    let serving = Serving {
        pointer: pointer(Some(LEASE), B, 9, Some(NAME)),
        basis: "retained_registry_resolver_pointer",
        owner_getter_reason: None,
    };
    assert_eq!(
        block(&scope(None, "ens_v1"), None, Some(&node), Some(&serving)),
        served(B)
    );
}

#[test]
fn another_arm_does_not_read_the_registry_pointer() {
    let node = pointer(Some(LEASE), A, 10, Some(NAME));
    for arm in ["ens_v2", "basenames"] {
        assert_eq!(
            block(&scope(Some(REGISTRY_ONLY), arm), None, Some(&node), None),
            none(),
            "{arm}"
        );
    }
}

#[test]
fn a_resourceless_registry_pointer_is_admitted_as_before() {
    let node = pointer(None, A, 10, Some(NAME));
    assert_eq!(
        block(&scope(Some(REGISTRY_ONLY), "ens_v1"), None, Some(&node), None),
        served(A)
    );
}
