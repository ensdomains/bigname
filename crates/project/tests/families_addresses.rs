//! F13 and F14 through the family loop: the per-name controller fold with its token holder, the
//! controller candidates, the candidate-complete (address, name, relation) index derived from
//! them and F2a, and the inverse record indexes derived from node and record-id `addr` values. Undo restores the index rows with
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
               "token_holder": DAVE, "registrant": DAVE}),
        "the registrant is the latest retained F2a row that reports one: the transfer at 12"
    );
    assert_eq!(folds[0]["controller_position"]["log_index"], json!(2));
    assert_eq!(folds[0]["controller_position"]["block_number"], json!(12));
    let candidates = fixture.rows("project_address_controller_candidate").await?;
    let mut candidates = index(
        &candidates,
        &["block_number", "log_index", "action", "subject"],
    );
    candidates.sort_by_key(|row| (row["block_number"].as_i64(), row["log_index"].as_i64()));
    assert_eq!(
        candidates,
        vec![
            json!({"block_number": 10, "log_index": 1, "action": "set", "subject": ALICE.to_lowercase()}),
            json!({"block_number": 11, "log_index": 1, "action": "set", "subject": BOB}),
            json!({"block_number": 11, "log_index": 2, "action": "revoke", "subject": ALICE.to_lowercase()}),
            json!({"block_number": 12, "log_index": 2, "action": "revoke", "subject": BOB}),
        ],
        "every controller event stays a candidate with its own position"
    );
    let mut relations = index(
        &fixture.rows("project_address_name_index").await?,
        &["address", "relation"],
    );
    relations.sort_by_key(Value::to_string);
    let mut expected = vec![
        json!({"address": ALICE.to_lowercase(), "relation": "effective_controller"}),
        json!({"address": BOB, "relation": "effective_controller"}),
    ];
    for address in [CAROL, DAVE] {
        for relation in ["registrant", "token_holder", "effective_controller"] {
            expected.push(json!({"address": address, "relation": relation}));
        }
    }
    expected.sort_by_key(Value::to_string);
    assert_eq!(
        relations, expected,
        "the index holds every address a relation can take; the read-time fold and masks \
         narrow it (the registrant and the transfer recipient come from F2a)"
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
        index(
            &fixture.rows("project_address_name_index").await?,
            &["address", "relation"]
        ),
        vec![json!({"address": ALICE.to_lowercase(), "relation": "effective_controller"})],
        "the earlier transfer stays a candidate; the masked owner word never indexes an address"
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn addr_values_index_their_address_past_a_version_change() -> Result<()> {
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
    let node_one = vec![
        json!({"address": lower, "coin_type": "60", "resolver_address": R1, "node": node(1)}),
        json!({"address": BOB, "coin_type": "2147483658", "resolver_address": R1,
               "node": node(1)}),
    ];
    assert_eq!(
        index(
            &fixture.rows("project_address_record_node_index").await?,
            &["address", "coin_type", "resolver_address", "node"]
        ),
        node_one
    );
    assert_eq!(
        index(
            &fixture.rows("project_address_record_id_index").await?,
            &["address", "coin_type", "record_id"]
        ),
        vec![json!({"address": CAROL, "coin_type": "60", "record_id": "7"})]
    );

    // A version change at node 1 keeps its values in the index: a later link can keep a value
    // below the version served (record_inventory.rs, the combined boundary), so readers apply the
    // boundary and the index stays a superset.
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
        index(
            &fixture.rows("project_address_record_node_index").await?,
            &["address", "coin_type", "resolver_address", "node"]
        ),
        node_one
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// The served value of a coin-60 pair is its AddressChanged half (record_inventory.rs,
// `ranked_records`): when the two halves carry different addresses, only that one is indexed. A
// value carried as address_bytes_hex is indexed like a value payload, and a value ordered after
// the version change only by its event identity (two synthesised events of one block) is kept.
#[tokio::test]
async fn the_index_holds_the_served_half_of_a_pair_and_hex_payloads() -> Result<()> {
    let fixture = Fixture::new("families_addresses_pair", 20).await?;
    let v1 = "ens_v1_resolver_l1";
    let record = |node_hex: String, source: &str, payload: Value| {
        let mut after = json!({"node": node_hex, "resolver": R1, "record_key": "addr:60",
                               "record_family": "addr", "selector_key": "60",
                               "source_event": source});
        for (field, value) in payload.as_object().cloned().unwrap_or_default() {
            after[field] = value;
        }
        after
    };
    fixture
        .write(
            10,
            4,
            "RecordChanged",
            v1,
            None,
            None,
            record(
                node(1),
                "AddressChanged",
                json!({"value": {"bytes": ALICE}}),
            ),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            5,
            "RecordChanged",
            v1,
            None,
            None,
            record(node(1), "AddrChanged", json!({"value": BOB})),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            7,
            "RecordChanged",
            v1,
            None,
            None,
            record(
                node(2),
                "AddressChanged",
                json!({"address_bytes_hex": CAROL}),
            ),
            R1,
        )
        .await?;
    // Two synthesised events of block 11 at node 3: the version change sorts before the value
    // only by identity.
    fixture
        .event(
            families_support::Event::new("11:a-version", 11, 0, "RecordVersionChanged", v1)
                .synthesised()
                .after(json!({"node": node(3), "resolver": R1, "version": "1"}))
                .raw(json!({"emitting_address": R1})),
        )
        .await?;
    fixture
        .event(
            families_support::Event::new("11:b-value", 11, 0, "RecordChanged", v1)
                .synthesised()
                .after(record(node(3), "AddressChanged", json!({"value": DAVE})))
                .raw(json!({"emitting_address": R1})),
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let mut rows = index(
        &fixture.rows("project_address_record_node_index").await?,
        &["address", "node"],
    );
    rows.sort_by_key(Value::to_string);
    let mut expected = vec![
        json!({"address": ALICE.to_lowercase(), "node": node(1)}),
        json!({"address": CAROL, "node": node(2)}),
        json!({"address": DAVE, "node": node(3)}),
    ];
    expected.sort_by_key(Value::to_string);
    assert_eq!(
        rows, expected,
        "the AddrChanged half's own address is not indexed"
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// An unnamed registrar grant is named when a binding candidate of its lease arrives a block
// later; its registrant then enters the name's index, since the registrant is read from F2a.
#[tokio::test]
async fn a_grant_named_later_puts_its_registrant_in_the_index() -> Result<()> {
    let fixture = Fixture::new("families_addresses_decoded", 20).await?;
    let lease = uuid(5);
    let registrar = "ens_v1_registrar_l1";
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            registrar,
            None,
            Some(&lease),
            json!({"namehash": node(4), "registrant": CAROL}),
            R1,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    assert_eq!(
        fixture.rows("project_address_name_index").await?,
        Vec::<Value>::new(),
        "an unnamed grant indexes nothing"
    );
    fixture
        .binding(&uuid(105), &name(4), &lease, "ens_v1", 11, 1, None)
        .await?;
    fixture
        .write(
            11,
            1,
            "SurfaceBound",
            registrar,
            Some(&name(4)),
            Some(&lease),
            json!({"authority_kind": "registrar"}),
            R1,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let mut rows = index(
        &fixture.rows("project_address_name_index").await?,
        &["address", "logical_name_id", "relation"],
    );
    rows.sort_by_key(Value::to_string);
    let mut expected: Vec<Value> = ["registrant", "token_holder", "effective_controller"]
        .into_iter()
        .map(|relation| json!({"address": CAROL, "logical_name_id": name(4), "relation": relation}))
        .collect();
    expected.sort_by_key(Value::to_string);
    assert_eq!(rows, expected);
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// A named write keeps the name it was written under on its index row, so the inverse read finds a
// value whose node is not the name's namehash by the name, as the forward inventory does.
#[tokio::test]
async fn a_named_value_indexes_the_name_it_was_written_under() -> Result<()> {
    let fixture = Fixture::new("families_addresses_named_node", 20).await?;
    let name = format!("ens:{}", node(7));
    fixture
        .write(
            10,
            1,
            "RecordChanged",
            "ens_v1_resolver_l1",
            Some(&name),
            None,
            json!({"node": node(8), "resolver": R1, "record_key": "addr:60",
                   "record_family": "addr", "selector_key": "60", "value": DAVE,
                   "source_event": "AddressChanged"}),
            R1,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    assert_eq!(
        index(
            &fixture.rows("project_address_record_node_index").await?,
            &["address", "coin_type", "node", "logical_name_id"]
        ),
        vec![json!({"address": DAVE, "coin_type": "60", "node": node(8),
                    "logical_name_id": name})]
    );
    fixture.assert_undo_restores(10).await?;
    fixture.assert_rebuild_equal(10).await?;
    fixture.cleanup().await
}
