//! F13 and F14 through the family loop: the per-name controller fold with its token holder and
//! registrant, the (address, name, relation) index derived from it, and the inverse record
//! indexes derived from node and record-id `addr` values. Undo restores the index rows with
//! their base rows, and a rebuild derives the same rows.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{Fixture, uuid};
use serde_json::{Value, json};

const R1: &str = "0x00000000000000000000000000000000000000a1";
const ALICE: &str = "0x00000000000000000000000000000000000000AA";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
const DAVE: &str = "0x00000000000000000000000000000000000000dd";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

fn pick(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

fn index(rows: &[Value], names: &[&str]) -> Vec<Value> {
    rows.iter().map(|row| pick(row, names)).collect()
}

#[tokio::test]
async fn names_fold_their_controller_and_index_every_relation() -> Result<()> {
    let fixture = Fixture::new("families_addresses_names", 20).await?;
    let resource = uuid(1);
    let (one, v1) = (name(1), "ens_v1_registry_l1");
    let permission = |subject: &str, powers: Value| json!({"subject": subject, "scope": {"kind": "resource"}, "effective_powers": powers});
    let events = [
        (10, 1, "AuthorityTransferred", json!({"owner": ALICE})),
        (10, 2, "RegistrationGranted", json!({"registrant": CAROL})),
        (
            11,
            1,
            "PermissionChanged",
            permission(BOB, json!(["resource_control"])),
        ),
        // A revoke from someone other than the controller changes nothing.
        (11, 2, "PermissionChanged", permission(ALICE, json!([]))),
        // A grant scoped to something other than the resource is not a controller event.
        (
            11,
            3,
            "PermissionChanged",
            json!({"subject": DAVE, "scope": {"kind": "registry"},
                   "effective_powers": ["resource_control"]}),
        ),
        (12, 1, "TokenControlTransferred", json!({"to": DAVE})),
        (
            12,
            2,
            "PermissionChanged",
            permission(BOB, json!(["set_resolver"])),
        ),
    ];
    for (block, log, kind, after) in events {
        fixture
            .write(block, log, kind, v1, Some(&one), Some(&resource), after, R1)
            .await?;
    }
    fixture.apply(12, FamilyMode::Normal).await;

    let folds = fixture.rows("project_address_name_fold").await?;
    assert_eq!(folds.len(), 1);
    assert_eq!(
        pick(
            &folds[0],
            &[
                "controller",
                "controller_action",
                "controller_subject",
                "token_holder",
                "registrant"
            ]
        ),
        json!({"controller": null, "controller_action": "revoke", "controller_subject": BOB,
               "token_holder": DAVE, "registrant": CAROL})
    );
    assert_eq!(folds[0]["controller_position"]["log_index"], json!(2));
    assert_eq!(folds[0]["controller_position"]["block_number"], json!(12));
    assert_eq!(
        index(
            &fixture.rows("project_address_name_index").await?,
            &["address", "relation"]
        ),
        vec![
            json!({"address": CAROL, "relation": "registrant"}),
            json!({"address": DAVE, "relation": "effective_controller"}),
            json!({"address": DAVE, "relation": "token_holder"}),
        ],
        "the revoked controller falls back to the token holder"
    );

    fixture.assert_undo_restores(12).await?;
    // Block 11's grant still stands after undoing 12, so Bob is the controller there.
    fixture.apply(11, FamilyMode::Rebuild).await;
    let controllers = fixture.rows("project_address_name_index").await?;
    assert!(
        controllers.contains(&json!({"address": BOB, "logical_name_id": one,
                                          "relation": "effective_controller"}))
    );
    fixture.apply(12, FamilyMode::Normal).await;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn a_masked_owner_word_clears_the_controller() -> Result<()> {
    let fixture = Fixture::new("families_addresses_masked", 20).await?;
    let resource = uuid(2);
    let v1 = "ens_v1_registry_l1";
    fixture
        .write(
            10,
            1,
            "AuthorityTransferred",
            v1,
            Some(&name(2)),
            Some(&resource),
            json!({"owner": ALICE}),
            R1,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "AuthorityTransferred",
            v1,
            Some(&name(2)),
            Some(&resource),
            json!({"owner": BOB, "owner_word_unmasked": true}),
            R1,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let folds = fixture.rows("project_address_name_fold").await?;
    assert_eq!(
        folds[0]["controller"],
        json!("0x0000000000000000000000000000000000000000")
    );
    assert_eq!(
        fixture.rows("project_address_name_index").await?,
        Vec::<Value>::new()
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn addr_values_index_their_address_until_a_version_change() -> Result<()> {
    let fixture = Fixture::new("families_addresses_records", 20).await?;
    let addr = |node_hex: String, coin: &str, value: Value| {
        json!({"node": node_hex, "resolver": R1, "record_key": format!("addr:{coin}"),
               "record_family": "addr", "selector_key": coin, "value": value,
               "source_event": "AddressChanged"})
    };
    let v1 = "ens_v1_resolver_l1";
    fixture
        .write(
            10,
            1,
            "RecordChanged",
            v1,
            None,
            None,
            addr(node(1), "60", json!(ALICE)),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "RecordChanged",
            v1,
            None,
            None,
            addr(node(1), "2147483658", json!({"bytes": BOB})),
            R1,
        )
        .await?;
    // A zero address, a non-EVM value and a text record are not indexed.
    fixture
        .write(
            10,
            3,
            "RecordChanged",
            v1,
            None,
            None,
            addr(
                node(2),
                "60",
                json!("0x0000000000000000000000000000000000000000"),
            ),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            4,
            "RecordChanged",
            v1,
            None,
            None,
            addr(node(2), "0", json!("0x00aa")),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            5,
            "RecordChanged",
            "ens_v2_resolver_l1",
            None,
            None,
            json!({"resolver": R1, "resolver_record_id": "7",
                      "storage_model": "resolver_record_id", "record_key": "addr:60",
                      "record_family": "addr", "selector_key": "60", "value": CAROL,
                      "source_event": "AddressUpdated"}),
            R1,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    let lower = ALICE.to_lowercase();
    assert_eq!(
        index(
            &fixture.rows("project_address_record_node_index").await?,
            &["address", "coin_type", "resolver_address", "node"]
        ),
        vec![
            json!({"address": lower, "coin_type": "60", "resolver_address": R1, "node": node(1)}),
            json!({"address": BOB, "coin_type": "2147483658", "resolver_address": R1,
                   "node": node(1)}),
        ]
    );
    assert_eq!(
        index(
            &fixture.rows("project_address_record_id_index").await?,
            &["address", "coin_type", "record_id"]
        ),
        vec![json!({"address": CAROL, "coin_type": "60", "record_id": "7"})]
    );

    // A version change at node 1 drops its values from the index; undo brings them back.
    fixture
        .write(
            11,
            1,
            "RecordVersionChanged",
            v1,
            None,
            None,
            json!({"node": node(1), "resolver": R1, "version": "1"}),
            R1,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    assert_eq!(
        fixture.rows("project_address_record_node_index").await?,
        Vec::<Value>::new()
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}
