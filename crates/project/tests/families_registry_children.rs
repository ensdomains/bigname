//! F13 for ENSv1 registry children with no name surface: a node an `ens_v1_registry_l1`
//! NewOwner created is indexed under its `<namespace>:<node>` id for its registry owner, a
//! registry Transfer adds the new owner, a surface the child gains later adds the ordinary rows
//! beside them, and undo and rebuild derive the same rows. A registrar transfer of the child's
//! lease with no name surface indexes its recipient under the same id.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{Fixture, uuid};
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// The (address, relation) index rows of `logical_name_id`, sorted.
async fn indexed(fixture: &Fixture, logical_name_id: &str) -> Result<Vec<Value>> {
    let mut rows: Vec<Value> = fixture
        .rows("project_address_name_index")
        .await?
        .into_iter()
        .filter(|row| row["logical_name_id"] == json!(logical_name_id))
        .map(|row| json!({"address": row["address"], "relation": row["relation"]}))
        .collect();
    rows.sort_by_key(Value::to_string);
    Ok(rows)
}

/// The index rows of a child's controllers: a name with no token lineage is owned by its
/// controller, so each is indexed under both relations.
fn controllers(addresses: &[&str]) -> Vec<Value> {
    let mut rows: Vec<Value> = addresses
        .iter()
        .flat_map(|address| {
            ["token_holder", "effective_controller"]
                .map(|relation| json!({"address": address, "relation": relation}))
        })
        .collect();
    rows.sort_by_key(Value::to_string);
    rows
}

#[tokio::test]
async fn a_surface_less_registry_child_indexes_its_registry_owner() -> Result<()> {
    let fixture = Fixture::new("families_registry_children", 20).await?;
    let v1 = "ens_v1_registry_l1";
    let child = name(9);
    // setSubnodeOwner(parent 1, labelhash 99, alice): the child edge and the child node's owner,
    // with no name.
    fixture
        .write(
            10,
            1,
            "SubregistryChanged",
            v1,
            None,
            None,
            json!({"source_event": "NewOwner", "node": node(1), "child_node": node(9),
                   "labelhash": node(99), "owner": ALICE, "owner_getter": ALICE,
                   "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    // Another registry's NewOwner and a Transfer of a node no NewOwner created index nothing.
    fixture
        .write(
            10,
            2,
            "AuthorityTransferred",
            v1,
            None,
            None,
            json!({"source_event": "Transfer", "node": node(7), "owner": CAROL,
                   "owner_getter": CAROL, "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    assert_eq!(
        indexed(&fixture, &child).await?,
        controllers(&[ALICE]),
        "the NewOwner owner of a surface-less child is its effective controller candidate"
    );
    assert_eq!(indexed(&fixture, &name(7)).await?, Vec::<Value>::new());

    // setOwner(child, bob): only the node's F2c row and owner events change.
    fixture
        .write(
            11,
            1,
            "AuthorityTransferred",
            v1,
            None,
            None,
            json!({"source_event": "Transfer", "node": node(9), "owner": BOB,
                   "owner_getter": BOB, "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    assert_eq!(
        indexed(&fixture, &child).await?,
        controllers(&[ALICE, BOB]),
        "the transfer indexes the new owner; the NewOwner owner stays a candidate, which the \
         read removes because the node's owner is now bob"
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;

    // A transfer to the zero owner indexes no zero address.
    fixture
        .write(
            12,
            1,
            "AuthorityTransferred",
            v1,
            None,
            None,
            json!({"source_event": "Transfer", "node": node(9), "owner": ZERO,
                   "owner_getter": ZERO, "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    assert_eq!(indexed(&fixture, &child).await?, controllers(&[ALICE]));
    fixture.assert_undo_restores(12).await?;

    // The child gains a surface: a registration under the same id adds the ordinary relations
    // beside the registry rows.
    fixture
        .write(
            13,
            1,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&child),
            Some(&uuid(5)),
            json!({"namehash": node(9), "registrant": CAROL}),
            REGISTRY,
        )
        .await?;
    fixture.apply(13, FamilyMode::Normal).await?;
    assert_eq!(
        indexed(&fixture, &child).await?,
        controllers(&[ALICE, CAROL])
    );
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

/// The fold row of `logical_name_id` as (token holder, its block), if any.
async fn fold_token_holder(fixture: &Fixture, logical_name_id: &str) -> Result<Option<Value>> {
    Ok(fixture
        .rows("project_address_name_fold")
        .await?
        .into_iter()
        .find(|row| row["logical_name_id"] == json!(logical_name_id))
        .map(|row| {
            json!([
                row["token_holder"],
                row["token_holder_position"]["block_number"]
            ])
        }))
}

/// A NewOwner under node 1 creating node 9 for alice, and an unnamed `.eth` lease granted to
/// alice in the same block, applied at 10.
async fn surface_less_lease(fixture: &Fixture, lease: &str) -> Result<()> {
    fixture
        .write(
            10,
            1,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"source_event": "NewOwner", "node": node(1), "child_node": node(9),
                   "labelhash": node(99), "owner": ALICE, "owner_getter": ALICE,
                   "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            None,
            Some(lease),
            json!({"namehash": node(9), "registrant": ALICE, "surface_known": false}),
            REGISTRY,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    Ok(())
}

// A BaseRegistrar Transfer without `reclaim` moves the token and leaves the registry record with
// the seller, so the buyer is only reachable through the lease's transfer. The adapter emits it
// unnamed when no surface carries the name; the fold keys it by the node's `<namespace>:<node>`
// id, the id the name keeps once a surface names it.
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L174 @ ens_v1@91c966f)
#[tokio::test]
async fn an_unnamed_lease_transfer_indexes_its_recipient_under_the_node_id() -> Result<()> {
    let fixture = Fixture::new("families_registry_children_lease", 20).await?;
    let lease = uuid(5);
    let child = name(9);
    surface_less_lease(&fixture, &lease).await?;
    assert_eq!(indexed(&fixture, &child).await?, controllers(&[ALICE]));
    fixture
        .write(
            11,
            1,
            "TokenControlTransferred",
            "ens_v1_registrar_l1",
            None,
            Some(&lease),
            json!({"source_event": "Transfer", "namehash": node(9), "to": BOB}),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    assert_eq!(
        fold_token_holder(&fixture, &child).await?,
        Some(json!([BOB, 11])),
        "the unnamed transfer is the node id's token holder"
    );
    assert_eq!(
        indexed(&fixture, &child).await?,
        controllers(&[ALICE, BOB]),
        "the buyer is indexed beside the seller, who keeps the registry record"
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;

    // The name gains a surface: the fold keeps the transfer it already holds, and the ordinary
    // rows of the name are the same addresses.
    fixture
        .binding(&uuid(105), &child, &lease, "ens_v1", 12, 1, None)
        .await?;
    fixture
        .write(
            12,
            1,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            Some(&child),
            Some(&lease),
            json!({"authority_kind": "registrar"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    assert_eq!(
        fold_token_holder(&fixture, &child).await?,
        Some(json!([BOB, 11]))
    );
    assert_eq!(indexed(&fixture, &child).await?, controllers(&[ALICE, BOB]));
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// A NameWrapper transfer carries the node, not a registrar namehash, so a wrapped token with no
// name surface still writes no fold row.
#[tokio::test]
async fn an_unnamed_wrapper_transfer_writes_no_fold_row() -> Result<()> {
    let fixture = Fixture::new("families_registry_children_wrapper", 20).await?;
    let child = name(9);
    surface_less_lease(&fixture, &uuid(5)).await?;
    fixture
        .write(
            11,
            1,
            "TokenControlTransferred",
            "ens_v1_wrapper_l1",
            None,
            Some(&uuid(6)),
            json!({"source_event": "TransferSingle", "node": node(9), "to": CAROL}),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    assert_eq!(fold_token_holder(&fixture, &child).await?, None);
    assert_eq!(indexed(&fixture, &child).await?, controllers(&[ALICE]));
    fixture.cleanup().await
}
