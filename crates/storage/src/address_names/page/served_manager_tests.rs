//! `attach_served_managers` joins each registry-child entry to the one `served_manager` its
//! composed rows carry, and refuses a child whose rows disagree.
use serde_json::{Value, json};
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::*;

const CHILD: &str = "ens:0x01";
const SELLER: &str = "0x00000000000000000000000000000000000000d1";
const BUYER: &str = "0x00000000000000000000000000000000000000d2";

fn row(relation: &str, manager: &str) -> Value {
    json!({"logical_name_id": CHILD, "relation": relation, "registry_child": true,
           "served_owner": BUYER, "served_manager": manager})
}

fn entry(surface_binding_id: Option<Uuid>) -> AddressNameCurrentEntry {
    AddressNameCurrentEntry {
        address: BUYER.to_owned(),
        logical_name_id: CHILD.to_owned(),
        namespace: "ens".to_owned(),
        canonical_display_name: "child.eth".to_owned(),
        normalized_name: "child.eth".to_owned(),
        namehash: "0x01".to_owned(),
        surface_binding_id,
        resource_id: Uuid::nil(),
        token_lineage_id: None,
        binding_kind: None,
        relations: Vec::new(),
        provenance: json!({}),
        coverage: json!({}),
        chain_positions: json!({}),
        canonicality_summary: json!({}),
        manifest_version: 1,
        last_recomputed_at: OffsetDateTime::UNIX_EPOCH,
        served_owner: Some(BUYER.to_owned()),
        served_manager: None,
        served_authority: None,
        served_lifecycle_shadow: false,
    }
}

fn attach(rows: &Value, entries: &mut [AddressNameCurrentEntry]) {
    let names = json!([]);
    attach_served_managers(
        entries,
        RowSource::Composed {
            rows,
            names: &names,
            parent: None,
        },
    );
}

#[test]
fn a_registry_child_takes_the_one_manager_its_rows_carry() {
    let rows = json!([
        row("token_holder", SELLER),
        row("effective_controller", SELLER)
    ]);
    let mut entries = [entry(None), entry(Some(Uuid::nil()))];
    attach(&rows, &mut entries);
    assert_eq!(entries[0].served_manager.as_deref(), Some(SELLER));
    assert_eq!(entries[1].served_manager, None, "a named row is untouched");
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "carries two served managers")]
fn a_registry_child_whose_rows_disagree_on_its_manager_is_refused() {
    let rows = json!([
        row("token_holder", SELLER),
        row("effective_controller", BUYER)
    ]);
    attach(&rows, &mut [entry(None)]);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "has no served manager")]
fn a_registry_child_without_a_composed_manager_is_refused() {
    attach(&json!([]), &mut [entry(None)]);
}
