//! The family fidelity cases of the step 2 review: unnamed resource pointers (F5), links only
//! from the resolver that emits them (F7), the registry owner group and the observation that
//! follows a name's current binding (F2c), the raw grant authority kind (F2a), and reverse claims
//! per resolver with their raw input (F12). Every case undoes its last block byte for byte and
//! equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{Fixture, uuid};
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e2";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const R2: &str = "0x00000000000000000000000000000000000000a2";
const STRANGER: &str = "0x00000000000000000000000000000000000000a9";
const OWNER_A: &str = "0x00000000000000000000000000000000000000b1";
const OWNER_B: &str = "0x00000000000000000000000000000000000000b2";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

fn columns(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

// An unnamed ResolverChanged on a resource (the unbound ENSv2 TLD root) and an unnamed version
// change are the resource's own facts; an empty resolver is a clear and never the non-zero
// pointer.
#[tokio::test]
async fn unnamed_resource_pointers_and_boundaries_are_kept() -> Result<()> {
    let fixture = Fixture::new("families_fidelity_pointer", 20).await?;
    let tld = uuid(1);
    let v2 = "ens_v2_registry_l1";
    fixture
        .write(
            10,
            1,
            "ResolverChanged",
            v2,
            None,
            Some(&tld),
            json!({"node": node(1), "resolver": R1}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "ResolverChanged",
            v2,
            None,
            Some(&tld),
            json!({"node": node(1), "resolver": ""}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "RecordVersionChanged",
            "ens_v2_resolver_l1",
            None,
            Some(&tld),
            json!({"node": node(1), "resolver": R1, "version": "2"}),
            R1,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_resource_pointer").await?;
    assert_eq!(
        rows.len(),
        1,
        "the unnamed pointer writes the resource's row"
    );
    assert_eq!(
        columns(
            &rows[0],
            &[
                "resolver_address",
                "nonzero_resolver_address",
                "boundary_kind",
                "namehash"
            ]
        ),
        json!({"resolver_address": "", "nonzero_resolver_address": R1,
               "boundary_kind": "RecordVersionChanged", "namehash": node(1)}),
        "the empty pointer is the current clear and leaves the non-zero group at R1"
    );
    assert_eq!(rows[0]["nonzero_position"]["block_number"], json!(10));
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// A Linked event belongs to the resolver that emitted it only when its payload names that
// resolver (resolvers/collections/links.sql:16-17); a stranger contract naming another resolver
// writes nothing.
#[tokio::test]
async fn a_link_counts_only_from_the_resolver_it_names() -> Result<()> {
    let fixture = Fixture::new("families_fidelity_link", 20).await?;
    let linked = |resolver: &str, record_id: &str| {
        json!({"node": node(1), "resolver": resolver, "resolver_record_id": record_id,
               "storage_model": "resolver_record_id", "source_event": "Linked"})
    };
    let v2 = "ens_v2_resolver_l1";
    fixture
        .write(
            10,
            1,
            "ResolverRecordLinked",
            v2,
            None,
            None,
            linked(R1, "7"),
            R1,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "ResolverRecordLinked",
            v2,
            None,
            None,
            linked(R1, "9"),
            STRANGER,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let links = fixture.rows("project_resolver_link").await?;
    assert_eq!(
        links
            .iter()
            .map(|row| columns(row, &["resolver_address", "record_id", "block_number"]))
            .collect::<Vec<_>>(),
        vec![json!({"resolver_address": R1, "record_id": "7", "block_number": 10})],
        "the stranger's link names R1 but was not emitted by it"
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// Both registry producers set the owner group, which keeps its own event position and resource;
// a named AuthorityTransferred reaches the name's current binding, and the row follows the name
// when a later binding replaces it.
#[tokio::test]
async fn the_owner_group_and_a_named_observation_follow_their_own_events() -> Result<()> {
    let fixture = Fixture::new("families_fidelity_registry", 20).await?;
    let (first, second) = (uuid(1), uuid(2));
    let v1 = "ens_v1_registry_l1";
    fixture
        .binding(&uuid(101), &name(1), &first, "ens_v1", 10, 1, Some(12))
        .await?;
    fixture
        .write(
            10,
            2,
            "AuthorityTransferred",
            v1,
            Some(&name(1)),
            Some(&first),
            json!({"node": node(1), "owner": OWNER_A, "owner_getter": OWNER_A}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "SubregistryChanged",
            v1,
            None,
            None,
            json!({"node": node(9), "child_node": node(1), "owner": OWNER_B,
                   "owner_getter": OWNER_B, "labelhash": node(99)}),
            REGISTRY,
        )
        .await?;
    fixture
        .binding(&uuid(102), &name(1), &second, "ens_v1", 12, 1, None)
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let nodes = fixture.rows("project_registry_node_state").await?;
    assert_eq!(
        columns(
            &nodes[0],
            &["owner", "owner_event_kind", "owner_resource_id"]
        ),
        json!({"owner": OWNER_B, "owner_event_kind": "SubregistryChanged",
               "owner_resource_id": null})
    );
    assert_eq!(nodes[0]["owner_position"]["block_number"], json!(11));
    let target = |rows: &[Value]| {
        rows.iter()
            .find(|row| row["observation_identity"] == json!(name(1)))
            .map(|row| {
                columns(
                    row,
                    &["attributed_via", "resource_id", "target_resource_id"],
                )
            })
    };
    assert_eq!(
        target(&fixture.rows("project_registry_binding_observation").await?),
        Some(json!({"attributed_via": "name", "resource_id": first, "target_resource_id": first}))
    );

    fixture.apply(12, FamilyMode::Normal).await;
    assert_eq!(
        target(&fixture.rows("project_registry_binding_observation").await?),
        Some(json!({"attributed_via": "name", "resource_id": first, "target_resource_id": second})),
        "the name's new binding is the resource its observation reaches"
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// A grant that carries no authority kind keeps none: the served name block reports null and the
// admission reads default it.
#[tokio::test]
async fn a_grant_without_an_authority_kind_keeps_none() -> Result<()> {
    let fixture = Fixture::new("families_fidelity_kind", 20).await?;
    let lease = uuid(3);
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&name(3)),
            Some(&lease),
            json!({"namehash": node(3), "registrant": OWNER_A, "authority_key": "k"}),
            REGISTRAR,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    let events = fixture.rows("project_lifecycle_event").await?;
    assert_eq!(
        columns(&events[0], &["authority_kind", "authority_key"]),
        json!({"authority_kind": null, "authority_key": "k"})
    );
    let states = fixture.rows("project_lifecycle_key_state").await?;
    assert_eq!(
        columns(
            &states[0]["last_grant"],
            &["authority_kind", "authority_key"]
        ),
        json!({"authority_kind": null, "authority_key": "k"})
    );
    fixture.assert_rebuild_equal(10).await?;
    fixture.cleanup().await
}

/// The claim a ReverseClaimed tuple reads today (primary_names.rs `node_claim`), from the family
/// rows: the reverse node's latest pointer (F4), that resolver's row of the node (F12) and the
/// claim event's normalization with its raw input.
async fn selected_claim(fixture: &Fixture, reverse_node: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
                    'resolver', pointer.resolver_address,
                    'normalized_name', normalization.normalized_name,
                    'status', normalization.status,
                    'raw_name', normalization.raw_name,
                    'claim_raw_name', claim.raw_name)
         FROM project_registry_pointer pointer
         LEFT JOIN project_reverse_node_claim claim
           ON claim.namespace = pointer.namespace AND claim.reverse_node = pointer.node
          AND claim.resolver_address = pointer.resolver_address
         LEFT JOIN project_claim_normalization normalization
           ON normalization.chain_id = pointer.chain_id
          AND normalization.claim_event_identity = claim.event_identity
         WHERE pointer.node = $1",
    )
    .bind(reverse_node)
    .fetch_one(&fixture.pool)
    .await?)
}

// A reverse node's claim follows its resolver pointer: moving from A to B and back to A reads
// A's claim again, and a version change at B while the node points at A leaves A's claim alone.
#[tokio::test]
async fn a_reverse_claim_follows_the_pointer_back_to_an_earlier_resolver() -> Result<()> {
    let fixture = Fixture::new("families_fidelity_reverse", 20).await?;
    let reverse = node(77);
    let v1 = "ens_v1_resolver_l1";
    let pointer = |resolver: &str| json!({"node": reverse, "resolver": resolver});
    let claim = |resolver: &str, raw: &str| {
        json!({"node": reverse, "resolver": resolver, "record_key": "name", "record_family": "text",
               "source_event": "NameChanged", "raw_name": raw})
    };
    fixture
        .write(
            9,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            None,
            pointer(R1),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "RecordChanged",
            v1,
            None,
            None,
            claim(R1, "Alice.eth"),
            R1,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            None,
            pointer(R2),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "RecordChanged",
            v1,
            None,
            None,
            claim(R2, "bob.eth"),
            R2,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    assert_eq!(
        selected_claim(&fixture, &reverse).await?,
        json!({"resolver": R2, "normalized_name": "bob.eth", "status": "success",
               "raw_name": "bob.eth", "claim_raw_name": "bob.eth"})
    );

    fixture
        .write(
            13,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            None,
            pointer(R1),
            REGISTRY,
        )
        .await?;
    // B's version change while the node points at A.
    fixture
        .write(
            14,
            1,
            "RecordVersionChanged",
            v1,
            None,
            None,
            json!({"node": reverse, "resolver": R2, "version": "3"}),
            R2,
        )
        .await?;
    fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(
        selected_claim(&fixture, &reverse).await?,
        json!({"resolver": R1, "normalized_name": "alice.eth", "status": "success",
               "raw_name": "Alice.eth", "claim_raw_name": "Alice.eth"}),
        "A's claim and its original payload survive B's writes"
    );
    let claims = fixture.rows("project_reverse_node_claim").await?;
    assert_eq!(claims.len(), 2, "one row per resolver of the node");
    fixture.assert_undo_restores(14).await?;
    fixture.assert_rebuild_equal(14).await?;
    fixture.cleanup().await
}

// A ReverseChanged is kept by its after-state tuple; the tuple it moves away from keeps its own
// latest event (primary_names.rs `latest_reverse`).
#[tokio::test]
async fn a_reverse_change_leaves_its_before_tuple_as_it_was() -> Result<()> {
    let fixture = Fixture::new("families_fidelity_tuple", 20).await?;
    let tuple = |address: &str| json!({"address": address, "coin_type": "60", "namespace": "ens"});
    let reverse_family = "ens_v1_reverse_l1";
    let mut first = tuple(OWNER_A);
    first["reverse_node"] = json!(node(1));
    first["source_event"] = json!("ReverseClaimed");
    fixture
        .write(
            10,
            1,
            "ReverseChanged",
            reverse_family,
            None,
            None,
            first,
            REGISTRY,
        )
        .await?;
    let mut moved = tuple(OWNER_B);
    moved["reverse_node"] = json!(node(2));
    moved["source_event"] = json!("ReverseClaimed");
    fixture
        .event(
            families_support::Event::new(
                "ReverseChanged:11:1",
                11,
                1,
                "ReverseChanged",
                reverse_family,
            )
            .before(tuple(OWNER_A))
            .after(moved),
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let rows = fixture.rows("project_reverse_tuple").await?;
    let by_address = |address: &str| {
        rows.iter()
            .find(|row| row["address"] == json!(address))
            .map(|row| columns(row, &["reverse_node", "block_number"]))
    };
    assert_eq!(
        by_address(OWNER_A),
        Some(json!({"reverse_node": node(1), "block_number": 10})),
        "the before tuple keeps its own latest ReverseChanged"
    );
    assert_eq!(
        by_address(OWNER_B),
        Some(json!({"reverse_node": node(2), "block_number": 11}))
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}
